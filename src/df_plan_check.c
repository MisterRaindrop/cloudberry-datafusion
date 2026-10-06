/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * df_plan_check.c
 *	  Decide which slices of a plan DataFusion can run.
 *
 * A slice qualifies when every plan node and expression in it is on a
 * deliberately small allow list.  Anything else keeps the slice on the
 * PostgreSQL executor, with a one-line reason that EXPLAIN shows.  The list
 * grows milestone by milestone; today it covers a Seq Scan with a filter
 * over a heap, AO, AOCS or PAX table, under an optional aggregate, over boolean, integer and
 * floating-point columns.  The aggregate may be single-stage, or the partial
 * (first) stage whose transition states a Finalize Aggregate combines in
 * another slice.  The slice may end in the Motion it sends through; that
 * Motion keeps running on PostgreSQL and sends DataFusion's rows one at a
 * time.
 *
 * Instead of a scan, the aggregate may read the rows a Motion receives from
 * another slice (M7a): the Motion keeps running on PostgreSQL, and the
 * aggregate may then be the combining stage (Finalize Aggregate) of count,
 * sum, min and max, whose transition states are plain values.
 *
 * src/df_plan_check.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "mb/pg_wchar.h"
#include "executor/execUtils.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "nodes/bitmapset.h"
#include "optimizer/optimizer.h"
#include "optimizer/walkers.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "df_executor.h"

/*
 * From utils/pg_locale.h, which needs ICU's headers when the server was
 * built with ICU.
 */
extern bool lc_collate_is_c(Oid collation);
extern bool lc_ctype_is_c(Oid collation);

typedef struct DfCheckContext
{
	PlannedStmt *stmt;
	bool		allow_aggref;	/* inside an Agg node's targetlist or qual */
	bool		agg_input;		/* checking the child of an Agg node */
	bool		join_input;		/* checking an input of a join */
	Bitmapset  *batches;		/* Motions carrying batches (df_batch_motions) */
	bool		batch_sender;	/* checking the child of a batch-sending Motion */
	bool		partial_states; /* this Agg may output DataFusion avg states */
	bool		final_states;	/* this Agg may read DataFusion avg states */
	bool		agg_to_batches; /* this Agg's results go into a batch Motion */
	bool		locale_dependent;	/* the verdict depends on this node's locale */
	Plan	   *node;			/* the node whose expressions are checked */
	Plan	   *top;			/* where a Limit or Sort may be: the top of
								 * the slice, or below its Limit (S1) */
	Plan	   *output;			/* the node whose targetlist is the slice's
								 * output: below any Limit and Sort */
	Bitmapset  *sort_keys;		/* its columns the slice's Sort orders by */
	bool		tails_ok;		/* PostgreSQL may finish output columns (P1):
								 * the slice does not send batches */
	DfTails		tails;			/* the columns it finishes */
	bool		failed;
	char	   *reason;
	size_t		reasonlen;
} DfCheckContext;

static void df_reject(DfCheckContext *cxt, const char *fmt,...) pg_attribute_printf(2, 3);
static bool df_check_expr(Node *node, DfCheckContext *cxt);
static void df_check_plan(Plan *plan, DfCheckContext *cxt, Bitmapset *needed,
						  bool root_is_sender);
static void df_check_plan_node(Plan *plan, DfCheckContext *cxt, Bitmapset *needed,
							   bool root_is_sender);

/* Record the first reason a slice does not qualify. */
static void
df_reject(DfCheckContext *cxt, const char *fmt,...)
{
	va_list		args;

	if (cxt->failed)
		return;
	cxt->failed = true;
	va_start(args, fmt);
	vsnprintf(cxt->reason, cxt->reasonlen, fmt, args);
	va_end(args);
}

static bool
df_type_supported(Oid type)
{
	switch (type)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case DATEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return true;
		case TEXTOID:
		case VARCHAROID:
			/* Arrow's strings are UTF-8. */
			return GetDatabaseEncoding() == PG_UTF8;
		case NUMERICOID:
			/* of a known precision and scale: df_numeric_ps */
			return true;
		default:
			return false;
	}
}

/*
 * Date and time types travel as PostgreSQL stores them: days or
 * microseconds from 2000-01-01 (time: from midnight), infinities included,
 * so comparing two values of one type compares the integers.
 */
static bool
df_type_is_datetime(Oid type)
{
	return type == DATEOID || type == TIMEOID ||
		type == TIMESTAMPOID || type == TIMESTAMPTZOID;
}

/*
 * text and varchar travel as their bytes, which DataFusion compares byte by
 * byte: equality agrees with PostgreSQL under a deterministic collation,
 * ordering (and min/max) only under the C collation.
 */
static bool
df_type_is_string(Oid type)
{
	return type == TEXTOID || type == VARCHAROID;
}

/*
 * Why comparing strings under 'collation' as 'name' does stays on
 * PostgreSQL, or NULL.  Whether the database's default collation is C is
 * each node's own: the coordinator and the segments can differ (e.g. C and
 * C.UTF-8), so such a verdict is marked as depending on the node.
 */
static const char *
df_string_compare_problem(DfCheckContext *cxt, const char *name, Oid collation)
{
	/* LIKE compares bytes too, and PostgreSQL rejects it otherwise */
	bool		equality = strcmp(name, "=") == 0 || strcmp(name, "<>") == 0 ||
		strcmp(name, "~~") == 0 || strcmp(name, "!~~") == 0;

	if (!equality && collation == DEFAULT_COLLATION_OID)
		cxt->locale_dependent = true;
	if (!OidIsValid(collation))
		return "without a collation";
	if (equality ? !get_collation_isdeterministic(collation) : !lc_collate_is_c(collation))
		return equality ? "under a nondeterministic collation" : "under a collation other than C";
	return NULL;
}

static const DfStringFunc df_string_funcs[] = {
	{F_LENGTH_TEXT, "char_length", DF_COLL_ANY},
	{F_CHAR_LENGTH_TEXT, "char_length", DF_COLL_ANY},
	{F_CHARACTER_LENGTH_TEXT, "char_length", DF_COLL_ANY},
	{F_OCTET_LENGTH_TEXT, "octet_length", DF_COLL_ANY},
	{F_SUBSTR_TEXT_INT4_INT4, "substr", DF_COLL_ANY},
	{F_SUBSTRING_TEXT_INT4_INT4, "substr", DF_COLL_ANY},
	{F_SUBSTR_TEXT_INT4, "substr", DF_COLL_ANY},
	{F_SUBSTRING_TEXT_INT4, "substr", DF_COLL_ANY},
	{F_TEXTCAT, "textcat", DF_COLL_ANY},
	{F_CONCAT, "concat", DF_COLL_ANY},
	{F_BTRIM_TEXT_TEXT, "btrim", DF_COLL_ANY},
	{F_BTRIM_TEXT, "btrim", DF_COLL_ANY},
	{F_LTRIM_TEXT_TEXT, "ltrim", DF_COLL_ANY},
	{F_LTRIM_TEXT, "ltrim", DF_COLL_ANY},
	{F_RTRIM_TEXT_TEXT, "rtrim", DF_COLL_ANY},
	{F_RTRIM_TEXT, "rtrim", DF_COLL_ANY},
	{F_LEFT, "left", DF_COLL_ANY},
	{F_RIGHT, "right", DF_COLL_ANY},
	{F_REVERSE, "reverse", DF_COLL_ANY},
	{F_REPEAT, "repeat", DF_COLL_ANY},
	{F_LPAD_TEXT_INT4_TEXT, "lpad", DF_COLL_ANY},
	{F_RPAD_TEXT_INT4_TEXT, "rpad", DF_COLL_ANY},
	{F_STRPOS, "strpos", DF_COLL_DETERMINISTIC},
	{F_POSITION_TEXT_TEXT, "strpos", DF_COLL_DETERMINISTIC},
	{F_REPLACE, "replace", DF_COLL_DETERMINISTIC},
	{F_SPLIT_PART, "split_part", DF_COLL_DETERMINISTIC},
	{F_STARTS_WITH, "starts_with", DF_COLL_DETERMINISTIC},
	{F_LOWER_TEXT, "lower", DF_COLL_CTYPE_C},
	{F_UPPER_TEXT, "upper", DF_COLL_CTYPE_C},
};

static const struct
{
	Oid			funcid;
	const char *kind;
}			df_casts[] =
{
	/* exact, or rounding as C converts */
	{F_INT4_INT2, "widen"}, {F_INT8_INT2, "widen"}, {F_INT8_INT4, "widen"},
	{F_FLOAT8_INT2, "widen"}, {F_FLOAT8_INT4, "widen"}, {F_FLOAT8_INT8, "widen"},
	{F_FLOAT4_INT2, "widen"}, {F_FLOAT4_INT4, "widen"}, {F_FLOAT4_INT8, "widen"},
	{F_FLOAT8_FLOAT4, "widen"},
	/* PostgreSQL's range checks and rounding */
	{F_INT2_INT4, "int"}, {F_INT2_INT8, "int"}, {F_INT4_INT8, "int"},
	{F_INT2_FLOAT8, "float_int"}, {F_INT4_FLOAT8, "float_int"}, {F_INT8_FLOAT8, "float_int"},
	{F_INT2_FLOAT4, "float_int"}, {F_INT4_FLOAT4, "float_int"}, {F_INT8_FLOAT4, "float_int"},
	{F_FLOAT4_FLOAT8, "float8_float4"},
	{F_TIMESTAMP_DATE, "date_timestamp"}, {F_DATE_TIMESTAMP, "timestamp_date"},
	{F_INT2_NUMERIC, "numeric_int"}, {F_INT4_NUMERIC, "numeric_int"}, {F_INT8_NUMERIC, "numeric_int"},
	{F_FLOAT8_NUMERIC, "numeric_float8"},
	{F_NUMERIC_NUMERIC_INT4, "numeric_typmod"},
};

const char *
df_cast_kind(Oid funcid)
{
	int			i;

	for (i = 0; i < lengthof(df_casts); i++)
		if (df_casts[i].funcid == funcid)
			return df_casts[i].kind;
	return NULL;
}

const DfStringFunc *
df_string_func(Oid funcid)
{
	int			i;

	for (i = 0; i < lengthof(df_string_funcs); i++)
		if (df_string_funcs[i].funcid == funcid)
			return &df_string_funcs[i];
	return NULL;
}

/*
 * Why calling string function 'f' under 'collation' stays on PostgreSQL,
 * or NULL.  Case mapping under the default collation depends on the
 * node's LC_CTYPE, so such a verdict is marked as depending on the node.
 */
static const char *
df_string_func_problem(DfCheckContext *cxt, const DfStringFunc *f, Oid collation)
{
	switch (f->rule)
	{
		case DF_COLL_ANY:
			return NULL;
		case DF_COLL_DETERMINISTIC:
			if (!OidIsValid(collation) || !get_collation_isdeterministic(collation))
				return "under a nondeterministic collation";
			return NULL;
		case DF_COLL_CTYPE_C:
			if (collation == DEFAULT_COLLATION_OID)
				cxt->locale_dependent = true;
			if (!OidIsValid(collation) || !lc_ctype_is_c(collation))
				return "under a collation other than C";
			return NULL;
	}
	return NULL;
}

/*
 * Precision and scale of numeric expression 'expr' of plan node 'ctx', if
 * DataFusion can carry it as Decimal128(38, scale): a column of a declared
 * numeric(p, s) with p <= 38, found through the references of the nodes
 * above it; a constant; min/max of such, and sum of such or of int8 (at
 * most 38 digits, the scale of its argument).  A partial avg's sum counts
 * as such a sum.  Anything else (an avg's result, arithmetic) has none.
 */
bool
df_numeric_ps(Plan *ctx, Node *expr, int *precision, int *scale)
{
	if (expr == NULL || exprType(expr) == InvalidOid)
		return false;
	switch (nodeTag(expr))
	{
		case T_Var:
			{
				Var		   *var = (Var *) expr;
				Plan	   *child;
				TargetEntry *tle;

				/*
				 * A reference is followed whatever its type: one to a partial
				 * sum or avg has PostgreSQL's state type (bytea).
				 */
				if (var->varno != OUTER_VAR && var->varno != INNER_VAR)
					return var->vartype == NUMERICOID &&
						df_numeric_typmod(var->vartypmod, precision, scale);
				child = var->varno == OUTER_VAR ? outerPlan(ctx) : innerPlan(ctx);
				if (child == NULL)
					return false;
				tle = get_tle_by_resno(child->targetlist, var->varattno);
				return tle != NULL && df_numeric_ps(child, (Node *) tle->expr, precision, scale);
			}
		case T_Const:
			if (((Const *) expr)->consttype != NUMERICOID)
				return false;
			if (((Const *) expr)->constisnull)
			{
				*precision = 1;
				*scale = 0;
				return true;
			}
			return df_numeric_const_ps(((Const *) expr)->constvalue, precision, scale);
		case T_OpExpr:
			{
				/* numeric + - * (N3): the result scale and digits of numeric.c */
				OpExpr	   *op = (OpExpr *) expr;
				char	   *name = get_opname(op->opno);
				int			lp,
							ls,
							rp,
							rs;

				if (op->opresulttype != NUMERICOID || list_length(op->args) != 2 ||
					name == NULL || op->opno >= FirstGenbkiObjectId ||
					exprType(linitial(op->args)) != NUMERICOID ||
					exprType(lsecond(op->args)) != NUMERICOID ||
					!df_numeric_ps(ctx, linitial(op->args), &lp, &ls) ||
					!df_numeric_ps(ctx, lsecond(op->args), &rp, &rs))
					return false;
				if (strcmp(name, "+") == 0 || strcmp(name, "-") == 0)
				{
					*scale = Max(ls, rs);
					*precision = Max(lp - ls, rp - rs) + *scale + 1;
				}
				else if (strcmp(name, "*") == 0)
				{
					*scale = ls + rs;	/* mul_var's rscale */
					*precision = lp + rp;
				}
				else
					return false;	/* / and %: the scale depends on the values */
				return *precision <= DF_NUMERIC_MAX_EXPR_PRECISION;
			}
		case T_CaseExpr:
		case T_CoalesceExpr:
			{
				/* E1: branches of one scale (checked), the widest precision */
				List	   *results = NIL;
				ListCell   *lc;
				bool		found = false;

				if (IsA(expr, CaseExpr))
				{
					foreach(lc, ((CaseExpr *) expr)->args)
						results = lappend(results, lfirst_node(CaseWhen, lc)->result);
					results = lappend(results, ((CaseExpr *) expr)->defresult);
				}
				else
					results = ((CoalesceExpr *) expr)->args;
				*precision = 1;
				*scale = 0;
				foreach(lc, results)
				{
					Node	   *e = lfirst(lc);
					int			p,
								s;

					if (IsA(e, Const) && ((Const *) e)->constisnull)
						continue;
					if (!df_numeric_ps(ctx, e, &p, &s) || (found && s != *scale))
						return false;
					*precision = Max(*precision, p);
					*scale = s;
					found = true;
				}
				return true;
			}
		case T_NullIfExpr:
			return df_numeric_ps(ctx, linitial(((NullIfExpr *) expr)->args), precision, scale);
		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) expr;

				/* integers made numeric, scale 0 */
				*scale = 0;
				switch (fe->funcid)
				{
					case F_NUMERIC_INT2:
						*precision = 5;
						return true;
					case F_NUMERIC_INT4:
						*precision = 10;
						return true;
					case F_NUMERIC_INT8:
						*precision = 19;
						return true;
					case F_NUMERIC_NUMERIC_INT4:
						{
							/* coerced to numeric(p, s) (E2) */
							Node	   *tm = lsecond(fe->args);
							int			ap,
										as;

							return IsA(tm, Const) && !((Const *) tm)->constisnull &&
								df_numeric_ps(ctx, linitial(fe->args), &ap, &as) &&
								df_numeric_typmod(DatumGetInt32(((Const *) tm)->constvalue),
												  precision, scale);
						}
					default:
						return false;
				}
			}
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) expr;
				char	   *name = get_func_name(agg->aggfnoid);
				Node	   *arg;
				int			p,
							s;

				if (name == NULL || agg->aggfnoid >= FirstGenbkiObjectId ||
					list_length(agg->args) != 1)
					return false;
				arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
				/* arguments refer to the Agg's child (OUTER_VAR): 'ctx' is the Agg */
				if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
				{
					/* the argument is the partial result below, of the same scale */
					if (!df_numeric_ps(ctx, arg, &p, &s))
						return false;
				}
				else if (exprType(arg) == INT8OID && strcmp(name, "sum") == 0)
					p = 19, s = 0;
				else if (exprType(arg) == INT8OID || exprType(arg) == INT4OID ||
						 exprType(arg) == INT2OID)
				{
					/* a partial avg of integers: its numeric sum */
					if (strcmp(name, "avg") != 0 || agg->aggsplit != AGGSPLIT_INITIAL_SERIAL)
						return false;
					p = 19, s = 0;
				}
				else if (!df_numeric_ps(ctx, arg, &p, &s))
					return false;
				if (strcmp(name, "min") == 0 || strcmp(name, "max") == 0)
				{
					*precision = p;
					*scale = s;
					return true;
				}
				if (strcmp(name, "sum") == 0 ||
					(strcmp(name, "avg") == 0 && agg->aggsplit == AGGSPLIT_INITIAL_SERIAL))
				{
					*precision = DF_NUMERIC_MAX_PRECISION;
					*scale = s;
					return true;
				}
				return false;
			}
		default:
			return false;
	}
}

/*
 * Can 'l' and 'r' be compared for equality as PostgreSQL does (E1: IN
 * lists, IS DISTINCT FROM, NULLIF)?  The rules of = (N2, X1).  Rejects if
 * not.
 */
static void
df_check_equality(DfCheckContext *cxt, const char *what, Node *l, Node *r, Oid collation)
{
	Oid			lt = exprType(l);
	Oid			rt = exprType(r);
	int			lp,
				ls,
				rp,
				rs,
				t;
	const char *problem;

	/* integers of two widths, or floats, compare in the wider one */
	if (!df_type_supported(lt) || (lt != rt && !OidIsValid(df_join_key_type(lt, rt))))
		df_reject(cxt, "%s on %s and %s", what, format_type_be(lt), format_type_be(rt));
	else if (lt == NUMERICOID)
	{
		if (!df_numeric_ps(cxt->node, l, &lp, &ls) || !df_numeric_ps(cxt->node, r, &rp, &rs))
			df_reject(cxt, "%s on numeric of unknown precision", what);
		else if (t = Max(ls, rs), lp - ls + t > DF_NUMERIC_MAX_EXPR_PRECISION ||
				 rp - rs + t > DF_NUMERIC_MAX_EXPR_PRECISION)
			df_reject(cxt, "%s on numeric beyond %d digits at a common scale",
					  what, DF_NUMERIC_MAX_EXPR_PRECISION);
	}
	else if (df_type_is_string(lt) &&
			 (problem = df_string_compare_problem(cxt, "=", collation)) != NULL)
		df_reject(cxt, "%s on %s %s", what, format_type_be(lt), problem);
}

/*
 * Do the numeric expressions in 'exprs' all have scale 'scale' (the first
 * one's when 'scale' < 0)?  A numeric value carries its display scale, so
 * a CASE or COALESCE whose branches have different scales returns values
 * displayed differently from row to row, which one Decimal256 column of
 * one scale cannot.  A NULL constant fits any scale.
 */
static bool
df_numeric_same_scale(Plan *ctx, List *exprs, int scale)
{
	ListCell   *lc;

	foreach(lc, exprs)
	{
		Node	   *e = lfirst(lc);
		int			p,
					s;

		if (IsA(e, Const) && ((Const *) e)->constisnull)
			continue;
		if (!df_numeric_ps(ctx, e, &p, &s) || (scale >= 0 && s != scale))
			return false;
		scale = s;
	}
	return true;
}

/* Is 'node' a constant LIKE pattern whose last backslash escapes nothing? */
static bool
df_like_pattern_ends_in_escape(Node *node)
{
	text	   *t;
	const char *p;
	int			len,
				i;

	if (!IsA(node, Const) || ((Const *) node)->constisnull)
		return false;
	t = DatumGetTextPP(((Const *) node)->constvalue);
	p = VARDATA_ANY(t);
	len = VARSIZE_ANY_EXHDR(t);
	for (i = 0; i < len; i++)
	{
		if (p[i] == '\\')
		{
			if (i + 1 == len)
				return true;
			i++;				/* the escaped byte */
		}
	}
	return false;
}

static bool
df_name_in(const char *name, const char *const *list)
{
	for (; *list; list++)
		if (strcmp(name, *list) == 0)
			return true;
	return false;
}

static const char *
df_plan_name(Plan *plan)
{
	switch (nodeTag(plan))
	{
		case T_Result:
			return "Result";
		case T_ProjectSet:
			return "ProjectSet";
		case T_Append:
			return "Append";
		case T_MergeAppend:
			return "Merge Append";
		case T_SeqScan:
			return "Seq Scan";
		case T_IndexScan:
			return "Index Scan";
		case T_IndexOnlyScan:
			return "Index Only Scan";
		case T_BitmapHeapScan:
			return "Bitmap Heap Scan";
		case T_FunctionScan:
			return "Function Scan";
		case T_ValuesScan:
			return "Values Scan";
		case T_SubqueryScan:
			return "Subquery Scan";
		case T_CteScan:
			return "CTE Scan";
		case T_ForeignScan:
			return "Foreign Scan";
		case T_CustomScan:
			return "Custom Scan";
		case T_NestLoop:
			return "Nested Loop";
		case T_MergeJoin:
			return "Merge Join";
		case T_HashJoin:
			return "Hash Join";
		case T_Material:
			return "Materialize";
		case T_Sort:
			return "Sort";
		case T_IncrementalSort:
			return "Incremental Sort";
		case T_Group:
			return "Group";
		case T_Agg:
			return "Aggregate";
		case T_WindowAgg:
			return "WindowAgg";
		case T_Unique:
			return "Unique";
		case T_Hash:
			return "Hash";
		case T_SetOp:
			return "SetOp";
		case T_Limit:
			return "Limit";
		case T_ModifyTable:
			return "ModifyTable";
		case T_ShareInputScan:
			return "Shared Scan";
		case T_Motion:
			switch (((Motion *) plan)->motionType)
			{
				case MOTIONTYPE_GATHER:
				case MOTIONTYPE_GATHER_SINGLE:
					return "Gather Motion";
				case MOTIONTYPE_HASH:
					return "Redistribute Motion";
				case MOTIONTYPE_BROADCAST:
				case MOTIONTYPE_BROADCAST_WORKERS:
					return "Broadcast Motion";
				case MOTIONTYPE_EXPLICIT:
					return "Explicit Redistribute Motion";
				default:
					return "Motion";
			}
		default:
			return NULL;
	}
}

static void
df_reject_plan(DfCheckContext *cxt, Plan *plan)
{
	const char *name = df_plan_name(plan);

	if (name)
		df_reject(cxt, "%s is not supported", name);
	else
		df_reject(cxt, "plan node type %d is not supported", (int) nodeTag(plan));
}

static bool
df_check_expr_list(List *exprs, DfCheckContext *cxt)
{
	return df_check_expr((Node *) exprs, cxt);
}

/* Expression walker: returns true to stop at the first unsupported node. */
static bool
df_check_expr(Node *node, DfCheckContext *cxt)
{
	static const char *const operators[] =
	{"=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "~~", "!~~", "||", NULL};
	static const char *const aggregates[] =
	{"count", "sum", "min", "max", "avg", NULL};

	if (node == NULL)
		return false;
	if (cxt->failed)
		return true;

	switch (nodeTag(node))
	{
		case T_List:
		case T_TargetEntry:
		case T_BoolExpr:
			break;

		case T_Var:
			{
				Var		   *var = (Var *) node;

				if (var->varattno == 0)
					df_reject(cxt, "whole-row reference");
				else if (var->varattno < 0)
				{
					/*
					 * Cloudberry adds ctid and gp_segment_id to updatable
					 * cursors so WHERE CURRENT OF can find the row.
					 */
					char	   *attname = NULL;

					if (!IS_SPECIAL_VARNO(var->varno))
						attname = get_attname(rt_fetch(var->varno, cxt->stmt->rtable)->relid,
											  var->varattno, true);
					df_reject(cxt, "system column %s", attname ? attname : "reference");
				}
				else if (!df_type_supported(var->vartype))
					df_reject(cxt, "column of type %s", format_type_be(var->vartype));
				else if (var->vartype == NUMERICOID)
				{
					int			p,
								s;

					if (!df_numeric_ps(cxt->node, node, &p, &s))
						df_reject(cxt, "numeric column without a precision of at most %d",
								  DF_NUMERIC_MAX_PRECISION);
				}
				return cxt->failed;
			}

		case T_Const:
			{
				Const	   *c = (Const *) node;

				int			p,
							s;

				if (!df_type_supported(c->consttype))
					df_reject(cxt, "constant of type %s", format_type_be(c->consttype));
				else if (c->consttype == NUMERICOID && !df_numeric_ps(cxt->node, node, &p, &s))
					df_reject(cxt, "numeric constant of more than %d digits or infinite",
							  DF_NUMERIC_MAX_PRECISION);
				return cxt->failed;
			}

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				char	   *name = get_opname(op->opno);
				ListCell   *lc;

				if (list_length(op->args) != 2)
					df_reject(cxt, "unary operator %s", name ? name : "?");
				else if (op->opno >= FirstGenbkiObjectId)
					df_reject(cxt, "user-defined operator %s", name ? name : "?");
				else if (name == NULL || !df_name_in(name, operators))
					df_reject(cxt, "operator %s", name ? name : "?");
				else if (!df_type_supported(op->opresulttype))
					df_reject(cxt, "operator %s returning %s", name,
							  format_type_be(op->opresulttype));
				foreach(lc, op->args)
				{
					Oid			argtype = exprType(lfirst(lc));

					if (!df_type_supported(argtype))
						df_reject(cxt, "operator %s on %s", name ? name : "?",
								  format_type_be(argtype));
				}
				if (!cxt->failed)
				{
					Oid			ltype = exprType(linitial(op->args));
					Oid			rtype = exprType(lsecond(op->args));

					/*
					 * Arithmetic on dates and times checks for overflow and
					 * infinities, and comparing two types converts one.
					 */
					if (strcmp(name, "||") == 0)
					{
						/* text || text only, not text || anynonarray */
						if (get_opcode(op->opno) != F_TEXTCAT)
							df_reject(cxt, "operator || on %s and %s",
									  format_type_be(ltype), format_type_be(rtype));
					}
					else if ((df_type_is_datetime(ltype) || df_type_is_datetime(rtype)) &&
							 (op->opresulttype != BOOLOID || ltype != rtype))
						df_reject(cxt, "operator %s on %s and %s", name,
								  format_type_be(ltype), format_type_be(rtype));
					else if (ltype == NUMERICOID || rtype == NUMERICOID)
					{
						int			lp,
									ls,
									rp,
									rs,
									t;

						/* comparisons at the larger scale (N2), + - * (N3) */
						if (ltype != rtype)
							df_reject(cxt, "operator %s on %s and %s", name,
									  format_type_be(ltype), format_type_be(rtype));
						else if (op->opresulttype != BOOLOID)
						{
							if (!df_numeric_ps(cxt->node, node, &lp, &ls))
								df_reject(cxt, "operator %s on numeric: %s", name,
										  strcmp(name, "/") == 0 || strcmp(name, "%") == 0 ?
										  "its scale depends on the values" :
										  "of unknown precision or more than 76 digits");
						}
						else if (!df_numeric_ps(cxt->node, linitial(op->args), &lp, &ls) ||
								 !df_numeric_ps(cxt->node, lsecond(op->args), &rp, &rs))
							df_reject(cxt, "operator %s on numeric of unknown precision", name);
						else if (t = Max(ls, rs), lp - ls + t > DF_NUMERIC_MAX_EXPR_PRECISION ||
								 rp - rs + t > DF_NUMERIC_MAX_EXPR_PRECISION)
							df_reject(cxt, "operator %s on numeric beyond %d digits at a common scale",
									  name, DF_NUMERIC_MAX_EXPR_PRECISION);
					}
					else if (df_type_is_string(ltype) || df_type_is_string(rtype))
					{
						const char *problem;

						if (op->opresulttype != BOOLOID || ltype != rtype)
							df_reject(cxt, "operator %s on %s and %s", name,
									  format_type_be(ltype), format_type_be(rtype));
						else if ((problem = df_string_compare_problem(cxt, name, op->inputcollid)) != NULL)
							df_reject(cxt, "operator %s on %s %s", name,
									  format_type_be(ltype), problem);
						else if ((strcmp(name, "~~") == 0 || strcmp(name, "!~~") == 0) &&
								 df_like_pattern_ends_in_escape(lsecond(op->args)))
							/* PostgreSQL may raise an error, depending on the rows */
							df_reject(cxt, "LIKE pattern ending with an escape character");
					}
				}
				if (cxt->failed)
					return true;
				break;
			}

		case T_NullTest:
			if (((NullTest *) node)->argisrow)
			{
				df_reject(cxt, "row-valued IS NULL");
				return true;
			}
			break;

		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) node;
				char	   *name = get_func_name(agg->aggfnoid);
				int			np,
							ns;

				if (!cxt->allow_aggref)
					df_reject(cxt, "aggregate outside an Aggregate node");
				else if (agg->aggfnoid >= FirstGenbkiObjectId)
					df_reject(cxt, "user-defined aggregate %s", name ? name : "?");
				else if (name == NULL || !df_name_in(name, aggregates))
					df_reject(cxt, "aggregate %s", name ? name : "?");
				else if (agg->aggdistinct != NIL || agg->aggorder != NIL)
					df_reject(cxt, "DISTINCT or ORDER BY inside aggregate %s", name);
				else if (agg->aggfilter != NULL)
					df_reject(cxt, "FILTER clause on aggregate %s", name);
				else if (agg->aggsplit != AGGSPLIT_SIMPLE &&
						 agg->aggsplit != AGGSPLIT_INITIAL_SERIAL &&
						 agg->aggsplit != AGGSPLIT_FINAL_DESERIAL)
					df_reject(cxt, "combining stage of aggregate %s", name);
				else if ((df_agg_state(agg) == DF_AGG_SUM_NUMERIC ||
						  df_agg_state(agg) == DF_AGG_AVG_NUMERIC) &&
						 agg->aggsplit != AGGSPLIT_FINAL_DESERIAL &&
						 !(df_numeric_ps(cxt->node,
										 (Node *) linitial_node(TargetEntry, agg->args)->expr,
										 &np, &ns) && np <= DF_NUMERIC_MAX_SUM_PRECISION))
					/* the sum must stay within Decimal128's 38 digits */
					df_reject(cxt, "aggregate %s of numeric of unknown precision or more than %d digits",
							  name, DF_NUMERIC_MAX_SUM_PRECISION);
				else if (agg->aggtype == NUMERICOID && df_agg_state(agg) == DF_AGG_PLAIN &&
						 !df_numeric_ps(cxt->node, node, &np, &ns))
					df_reject(cxt, "aggregate %s returning numeric of unknown precision", name);
				else if ((df_agg_state(agg) == DF_AGG_AVG_INT ||
						  df_agg_state(agg) == DF_AGG_AVG_NUMERIC) &&
						 agg->aggsplit != AGGSPLIT_INITIAL_SERIAL && cxt->agg_to_batches)
					/* avg = sum / count is computed where tuples are made */
					df_reject(cxt, "aggregate avg returning numeric into a batch Motion");
				else if (df_agg_state(agg) != DF_AGG_PLAIN && agg->aggsplit != AGGSPLIT_SIMPLE)
				{
					/*
					 * M7d, N1: through a batch Motion, a split avg or
					 * sum(int8) passes DataFusion's state (sum, and count
					 * for avg) instead of PostgreSQL's array or serialized
					 * state.
					 */
					bool		allowed = agg->aggsplit == AGGSPLIT_INITIAL_SERIAL ?
						cxt->partial_states : cxt->final_states;

					if (!allowed)
						df_reject(cxt, "%s aggregate %s without batch Motions",
								  agg->aggsplit == AGGSPLIT_INITIAL_SERIAL ? "partial" : "combining",
								  name);
					else if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
					{
						/* its argument is the state column of the Motion below */
						if (list_length(agg->args) != 1 ||
							!IsA(linitial_node(TargetEntry, agg->args)->expr, Var))
							df_reject(cxt, "combining aggregate %s over an expression", name);
						return cxt->failed;
					}
				}
				else if (strcmp(name, "avg") == 0 && agg->aggsplit != AGGSPLIT_SIMPLE)
					df_reject(cxt, "split aggregate avg of %s",
							  format_type_be(list_length(agg->aggargtypes) == 1 ?
											 linitial_oid(agg->aggargtypes) : InvalidOid));
				else if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL &&
						 agg->aggtranstype != agg->aggtype)
					/*
					 * Combining count, sum and min/max adds up or compares
					 * plain values of the result type; other transition
					 * states (avg's array, serialized states) stay on
					 * PostgreSQL.
					 */
					df_reject(cxt, "combining stage of aggregate %s", name);
				else if (agg->aggsplit == AGGSPLIT_INITIAL_SERIAL &&
						 agg->aggtype == BYTEAOID)
					df_reject(cxt, "partial aggregate %s with a serialized transition state", name);
				else if (!df_type_supported(agg->aggtype) && df_agg_state(agg) == DF_AGG_PLAIN)
					df_reject(cxt, "aggregate %s returning %s", name,
							  format_type_be(agg->aggtype));
				else if (strcmp(name, "sum") == 0 && agg->aggtype == FLOAT4OID)
					/* PostgreSQL adds real values in single precision. */
					df_reject(cxt, "sum of real values");
				else if (df_type_is_string(agg->aggtype) &&
						 df_string_compare_problem(cxt, name, agg->inputcollid) != NULL)
					df_reject(cxt, "aggregate %s of %s %s", name, format_type_be(agg->aggtype),
							  df_string_compare_problem(cxt, name, agg->inputcollid));
				if (cxt->failed)
					return true;
				break;
			}

		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;
				const DfStringFunc *f = df_string_func(fe->funcid);
				char	   *name = get_func_name(fe->funcid);
				const char *problem;
				ListCell   *lc;
				int			np,
							ns;

				if (fe->funcresulttype == NUMERICOID && df_numeric_ps(cxt->node, node, &np, &ns))
				{
					/* an integer made numeric (N3), numeric(p, s) (E2) */
					if (fe->funcid == F_NUMERIC_NUMERIC_INT4)
					{
						df_check_expr(linitial(fe->args), cxt);
						return cxt->failed;
					}
					break;
				}
				if (df_cast_kind(fe->funcid) != NULL && fe->funcid != F_NUMERIC_NUMERIC_INT4)
				{
					Node	   *arg = linitial(fe->args);

					/* E2: a numeric argument needs a precision */
					if (exprType(arg) == NUMERICOID && !df_numeric_ps(cxt->node, arg, &np, &ns))
					{
						df_reject(cxt, "cast of numeric of unknown precision");
						return true;
					}
					break;
				}
				if (f == NULL || fe->funcretset || fe->funcvariadic)
				{
					df_reject(cxt, "function %s()", name ? name : "?");
					return true;
				}
				if ((problem = df_string_func_problem(cxt, f, fe->inputcollid)) != NULL)
				{
					df_reject(cxt, "function %s() %s", name, problem);
					return true;
				}
				/* concat() takes "any": only text arguments here */
				foreach(lc, fe->args)
				{
					Oid			argtype = exprType(lfirst(lc));

					if (fe->funcid == F_CONCAT && !df_type_is_string(argtype))
					{
						df_reject(cxt, "function concat() of %s", format_type_be(argtype));
						return true;
					}
				}
				break;
			}

		case T_SubPlan:
		case T_AlternativeSubPlan:
			df_reject(cxt, "subquery");
			return true;

		case T_Param:
			df_reject(cxt, "query parameter");
			return true;

		case T_CaseExpr:
			{
				/* E1: DataFusion evaluates a branch only on the rows reaching it */
				CaseExpr   *ce = (CaseExpr *) node;
				List	   *results = NIL;
				ListCell   *lc;

				if (!df_type_supported(ce->casetype))
				{
					df_reject(cxt, "CASE returning %s", format_type_be(ce->casetype));
					return true;
				}
				if (ce->arg != NULL && exprType((Node *) ce->arg) == NUMERICOID)
				{
					df_reject(cxt, "CASE on a numeric value");
					return true;
				}
				foreach(lc, ce->args)
					results = lappend(results, lfirst_node(CaseWhen, lc)->result);
				results = lappend(results, ce->defresult);
				if (ce->casetype == NUMERICOID && !df_numeric_same_scale(cxt->node, results, -1))
				{
					df_reject(cxt, "CASE returning numeric of different scales");
					return true;
				}
				break;
			}

		case T_CaseWhen:
		case T_CaseTestExpr:
			break;

		case T_CoalesceExpr:
			{
				CoalesceExpr *co = (CoalesceExpr *) node;

				if (!df_type_supported(co->coalescetype))
				{
					df_reject(cxt, "COALESCE of %s", format_type_be(co->coalescetype));
					return true;
				}
				if (co->coalescetype == NUMERICOID &&
					!df_numeric_same_scale(cxt->node, co->args, -1))
				{
					df_reject(cxt, "COALESCE of numeric of different scales");
					return true;
				}
				break;
			}

		case T_NullIfExpr:
			{
				NullIfExpr *ni = (NullIfExpr *) node;

				if (list_length(ni->args) != 2)
				{
					df_reject(cxt, "NULLIF");
					return true;
				}
				df_check_equality(cxt, "NULLIF", linitial(ni->args), lsecond(ni->args),
								  ni->inputcollid);
				if (cxt->failed)
					return true;
				break;
			}

		case T_ScalarArrayOpExpr:
			{
				/* E1: x IN (constants), x NOT IN (constants) */
				ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) node;
				char	   *name = get_opname(sa->opno);
				Node	   *left = linitial(sa->args);
				Const	   *arr = lsecond(sa->args);
				Datum	   *elems;
				bool	   *nulls;
				int			nelems,
							i;
				int16		elmlen;
				bool		elmbyval;
				char		elmalign;
				Oid			elemtype;

				if (name == NULL || sa->opno >= FirstGenbkiObjectId ||
					!((sa->useOr && strcmp(name, "=") == 0) ||
					  (!sa->useOr && strcmp(name, "<>") == 0)))
				{
					df_reject(cxt, "%s %s list", name ? name : "?", sa->useOr ? "ANY" : "ALL");
					return true;
				}
				if (!IsA(arr, Const) || arr->constisnull)
				{
					df_reject(cxt, "IN list that is not a constant");
					return true;
				}
				elemtype = ARR_ELEMTYPE(DatumGetArrayTypeP(arr->constvalue));
				get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(DatumGetArrayTypeP(arr->constvalue), elemtype,
								  elmlen, elmbyval, elmalign, &elems, &nulls, &nelems);
				for (i = 0; i < nelems && !cxt->failed; i++)
				{
					Const	   *c = makeConst(elemtype, -1, sa->inputcollid, elmlen,
											  elems[i], nulls[i], elmbyval);

					df_check_equality(cxt, sa->useOr ? "IN list" : "NOT IN list", left,
									  (Node *) c, sa->inputcollid);
				}
				if (!cxt->failed)
					df_check_expr(left, cxt);
				return cxt->failed;
			}

		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * varchar is read as text, the same bytes; COLLATE keeps the
				 * type and the comparison above checks the collation.
				 */
				if (df_type_is_string(r->resulttype) &&
					(r->resulttype == exprType((Node *) r->arg) ||
					 (r->resulttype == TEXTOID && exprType((Node *) r->arg) == VARCHAROID)))
					break;
				df_reject(cxt, "type cast");
				return true;
			}

		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
			df_reject(cxt, "type cast");
			return true;

		case T_DistinctExpr:
			{
				DistinctExpr *de = (DistinctExpr *) node;

				if (list_length(de->args) != 2)
				{
					df_reject(cxt, "IS DISTINCT FROM");
					return true;
				}
				df_check_equality(cxt, "IS DISTINCT FROM", linitial(de->args),
								  lsecond(de->args), de->inputcollid);
				if (cxt->failed)
					return true;
				break;
			}

		default:
			df_reject(cxt, "expression node type %d", (int) nodeTag(node));
			return true;
	}

	return expression_tree_walker(node, df_check_expr, cxt);
}

/* Collect the child targetlist positions an upper node refers to. */
static bool
df_collect_outer_refs(Node *node, Bitmapset **refs)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var) && ((Var *) node)->varno == OUTER_VAR)
	{
		*refs = bms_add_member(*refs, ((Var *) node)->varattno);
		return false;
	}
	return expression_tree_walker(node, df_collect_outer_refs, refs);
}

/* The same for a join's inner child (INNER_VAR). */
static bool
df_collect_inner_refs(Node *node, Bitmapset **refs)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var) && ((Var *) node)->varno == INNER_VAR)
	{
		*refs = bms_add_member(*refs, ((Var *) node)->varattno);
		return false;
	}
	return expression_tree_walker(node, df_collect_inner_refs, refs);
}

/* The type both sides of a join key are compared in, or InvalidOid. */
Oid
df_join_key_type(Oid a, Oid b)
{
	static const Oid ints[] = {INT2OID, INT4OID, INT8OID};
	static const Oid floats[] = {FLOAT4OID, FLOAT8OID};
	int			ia = -1,
				ib = -1,
				i;

	if (a == b)
		return a;
	for (i = 0; i < lengthof(ints); i++)
	{
		if (ints[i] == a)
			ia = i;
		if (ints[i] == b)
			ib = i;
	}
	if (ia >= 0 && ib >= 0)
		return ints[Max(ia, ib)];
	ia = ib = -1;
	for (i = 0; i < lengthof(floats); i++)
	{
		if (floats[i] == a)
			ia = i;
		if (floats[i] == b)
			ib = i;
	}
	if (ia >= 0 && ib >= 0)
		return floats[Max(ia, ib)];
	return InvalidOid;
}

/*
 * Are output columns 'attnos' of 'plan' plain columns all the way down?
 * Expressions on the side an outer join fills with NULLs must be computed
 * before it does (x IS NULL is false for a row, true once nulled), which
 * DataFusion, evaluating them above the join, would not do.
 */
static bool
df_plain_outputs(Plan *plan, Bitmapset *attnos)
{
	Bitmapset  *outer = NULL;
	Bitmapset  *inner = NULL;
	ListCell   *lc;

	if (plan == NULL)
		return false;
	if (IsA(plan, Motion))
		return true;
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var		   *var = (Var *) tle->expr;

		if (!bms_is_member(tle->resno, attnos))
			continue;
		if (!IsA(var, Var))
			return false;
		if (var->varno == OUTER_VAR)
			outer = bms_add_member(outer, var->varattno);
		else if (var->varno == INNER_VAR)
			inner = bms_add_member(inner, var->varattno);
		else if (!(IsA(plan, SeqScan) && var->varno == ((Scan *) plan)->scanrelid))
			return false;
	}
	if (IsA(plan, SeqScan))
		return true;
	if (IsA(plan, Hash))
		return inner == NULL && df_plain_outputs(outerPlan(plan), outer);
	if (IsA(plan, HashJoin))
		return (outer == NULL || df_plain_outputs(outerPlan(plan), outer)) &&
			(inner == NULL || df_plain_outputs(innerPlan(plan), inner));
	return false;
}

/*
 * Bytes a Hash node's table is estimated to take: the planner's rows and
 * width, plus a per-row allowance for DataFusion's hash table.
 */
static double
df_hash_estimate(Plan *hash)
{
	return hash->plan_rows * (hash->plan_width + 48.0);
}

/*
 * Bytes the executor would give a Hash node's table: like a hashed Agg's
 * budget, min(operatorMemKB, work_mem) * hash_mem_multiplier.
 */
double
df_hash_budget(Plan *hash)
{
	double		kb = work_mem;

	if (hash->operatorMemKB > 0 && hash->operatorMemKB < kb)
		kb = hash->operatorMemKB;
	return kb * hash_mem_multiplier * 1024.0;
}

/*
 * Check the targetlist entries at the positions in 'needed', or all of them
 * if 'needed' is NULL.  A scan often emits every column of the table (a
 * "physical" targetlist) even when the node above uses only a few; columns
 * nobody reads do not matter.
 */
/* ---------------------------------------------------------------------
 * P1: output columns PostgreSQL finishes over DataFusion's values
 * ---------------------------------------------------------------------
 */

/* Could DataFusion compute 'node' whole?  Leaves no trace of a failure. */
static bool
df_expr_ok(Node *node, DfCheckContext *cxt)
{
	bool		ok;

	Assert(!cxt->failed);
	df_check_expr(node, cxt);
	ok = !cxt->failed;
	cxt->failed = false;
	cxt->reason[0] = '\0';
	return ok;
}

static void df_check_tail(Node *node, DfCheckContext *cxt);

static bool
df_check_tail_walker(Node *node, DfCheckContext *cxt)
{
	df_check_tail(node, cxt);
	return cxt->failed;
}

/*
 * Part of an output column PostgreSQL evaluates: what DataFusion cannot
 * compute is evaluated by PostgreSQL's expression machinery over the
 * values of the largest subexpressions DataFusion can compute (the
 * leaves), so it may hold any expression that needs no more than those
 * values: no subplans, parameters, window functions or set-returning
 * functions, and no CaseTestExpr to split from its CASE.
 */
static void
df_check_tail(Node *node, DfCheckContext *cxt)
{
	if (node == NULL || cxt->failed)
		return;
	if (IsA(node, List))
	{
		ListCell   *lc;

		foreach(lc, (List *) node)
			df_check_tail(lfirst(lc), cxt);
		return;
	}
	if (IsA(node, CaseWhen))
	{
		/* part of its CASE, never an expression of its own */
		df_check_tail((Node *) ((CaseWhen *) node)->expr, cxt);
		df_check_tail((Node *) ((CaseWhen *) node)->result, cxt);
		return;
	}
	if (df_expr_ok(node, cxt))
	{
		cxt->tails.leaves = lappend(cxt->tails.leaves, node);
		return;
	}
	switch (nodeTag(node))
	{
		case T_Const:
			return;
		case T_OpExpr:
			if (((OpExpr *) node)->opretset)
			{
				df_reject(cxt, "set-returning operator in an output column");
				return;
			}
			break;
		case T_FuncExpr:
			if (((FuncExpr *) node)->funcretset)
			{
				df_reject(cxt, "set-returning function in an output column");
				return;
			}
			break;
		case T_CaseExpr:
			if (((CaseExpr *) node)->arg != NULL)
			{
				df_check_expr(node, cxt);	/* DataFusion's reason */
				return;
			}
			break;
		case T_BoolExpr:
		case T_CoalesceExpr:
		case T_NullIfExpr:
		case T_MinMaxExpr:
		case T_NullTest:
		case T_BooleanTest:
		case T_RelabelType:
		case T_CoerceViaIO:
		case T_ScalarArrayOpExpr:
		case T_DistinctExpr:
			break;
		default:
			/* a column, an aggregate, ...: DataFusion's reason */
			df_check_expr(node, cxt);
			if (!cxt->failed)
				df_reject(cxt, "expression node %d in an output column", (int) nodeTag(node));
			return;
	}
	(void) expression_tree_walker(node, df_check_tail_walker, cxt);
}

static void
df_check_targetlist(List *targetlist, Bitmapset *needed, DfCheckContext *cxt)
{
	ListCell   *lc;
	bool		output = cxt->tails_ok && cxt->node == cxt->output;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (cxt->failed)
			return;
		if (needed != NULL && !bms_is_member(tle->resno, needed))
			continue;
		if (!output || bms_is_member(tle->resno, cxt->sort_keys) ||
			df_expr_ok((Node *) tle->expr, cxt))
			df_check_expr((Node *) tle->expr, cxt);
		else
		{
			/* P1: PostgreSQL finishes it */
			df_check_tail((Node *) tle->expr, cxt);
			cxt->tails.tles = lappend(cxt->tails.tles, tle);
		}
	}
}

/*
 * Table access methods whose scans DataFusion can read.  The main thread
 * reads them through the table AM interface (df_exec.c), so any AM whose
 * slots carry the columns works; these are the ones tested.
 */
static bool
df_am_supported(Oid relam)
{
	static const char *const ams[] = {"heap", "ao_row", "ao_column", "pax", NULL};
	char	   *name;

	if (relam == HEAP_TABLE_AM_OID)
		return true;
	name = get_am_name(relam);
	return name != NULL && df_name_in(name, ams);
}

/* Access method of a relation, read from its pg_class row. */
static Oid
df_relation_am(Oid relid)
{
	HeapTuple	tup;
	Oid			relam;

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	relam = ((Form_pg_class) GETSTRUCT(tup))->relam;
	ReleaseSysCache(tup);
	return relam;
}

/* ---------------------------------------------------------------------
 * S1: Sort and Limit at the top of a slice
 * ---------------------------------------------------------------------
 */

/*
 * The value of a Limit's OFFSET or COUNT 'expr', if it is a constant (or
 * folds to one) and not negative: -1 for none (no expression, or NULL as
 * in LIMIT ALL).  A negative value is PostgreSQL's error to raise.
 */
bool
df_limit_value(Node *expr, int64 *value)
{
	Node	   *folded;
	Const	   *c;

	*value = -1;
	if (expr == NULL)
		return true;
	folded = eval_const_expressions(NULL, copyObject(expr));
	if (!IsA(folded, Const) || ((Const *) folded)->consttype != INT8OID)
		return false;
	c = (Const *) folded;
	if (c->constisnull)
		return true;
	*value = DatumGetInt64(c->constvalue);
	return *value >= 0;
}

/*
 * Does 'sortop' order values of 'type' as its default btree operator class
 * does, ascending (<) or descending (>)?  Those orders are what DataFusion
 * sorts by (strings under a C collation, floats through pg_float_key).
 */
bool
df_sort_direction(Oid sortop, Oid type, bool *desc)
{
	Oid			opfamily;
	Oid			opcintype;
	int16		strategy;
	Oid			opclass = GetDefaultOpClass(type, BTREE_AM_OID);

	if (!OidIsValid(opclass) ||
		!get_ordering_op_properties(sortop, &opfamily, &opcintype, &strategy) ||
		opfamily != get_opclass_family(opclass))
		return false;
	*desc = strategy == BTGreaterStrategyNumber;
	return true;
}

/*
 * Does 'plan' pass its child's rows through unchanged: no filter, and
 * output column i is the child's column i, as Sort and Limit build them?
 * set_dummy_tlist_references keeps a constant output of the child as the
 * constant itself.
 */
bool
df_passes_through(Plan *plan)
{
	ListCell   *lc;
	Plan	   *child = outerPlan(plan);

	if (plan->qual != NIL || child == NULL ||
		list_length(plan->targetlist) != list_length(child->targetlist))
		return false;
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var		   *var = (Var *) tle->expr;

		if (IsA(var, Const) &&
			equal(var, get_tle_by_resno(child->targetlist, tle->resno)->expr))
			continue;
		if (!IsA(var, Var) || var->varno != OUTER_VAR || var->varattno != tle->resno)
			return false;
	}
	return true;
}

/*
 * Is the output of the slice part 'top' (a Sort, or a Limit over one) in
 * the order a sorted Motion's receiver merges by?  The planner builds
 * them so; this guards the assumption.
 */
static bool
df_sorted_for(Plan *top, Motion *motion)
{
	Sort	   *sort;
	int			i;

	if (IsA(top, Limit))
		top = outerPlan(top);
	if (!IsA(top, Sort) || motion->numSortCols > ((Sort *) top)->numCols)
		return false;
	sort = (Sort *) top;
	for (i = 0; i < motion->numSortCols; i++)
	{
		TargetEntry *tle = get_tle_by_resno(motion->plan.targetlist, motion->sortColIdx[i]);
		Var		   *var = tle ? (Var *) tle->expr : NULL;

		if (var == NULL || !IsA(var, Var) || var->varno != OUTER_VAR ||
			var->varattno != sort->sortColIdx[i] ||
			motion->sortOperators[i] != sort->sortOperators[i] ||
			motion->nullsFirst[i] != sort->nullsFirst[i])
			return false;
	}
	return true;
}

static void
df_check_plan(Plan *plan, DfCheckContext *cxt, Bitmapset *needed,
			  bool root_is_sender)
{
	Plan	   *saved = cxt->node;

	cxt->node = plan;
	df_check_plan_node(plan, cxt, needed, root_is_sender);
	cxt->node = saved;
}

static void
df_check_plan_node(Plan *plan, DfCheckContext *cxt, Bitmapset *needed,
				   bool root_is_sender)
{
	if (plan == NULL || cxt->failed)
		return;

	if (plan->initPlan != NIL)
	{
		df_reject(cxt, "init plan");
		return;
	}

	switch (nodeTag(plan))
	{
		case T_SeqScan:
			{
				Scan	   *scan = (Scan *) plan;
				RangeTblEntry *rte = rt_fetch(scan->scanrelid, cxt->stmt->rtable);
				Oid			relam;

				if (rte->rtekind != RTE_RELATION || rte->tablesample != NULL)
				{
					df_reject(cxt, "Seq Scan over something other than a plain table");
					return;
				}
				relam = df_relation_am(rte->relid);
				if (!df_am_supported(relam))
				{
					char	   *amname = get_am_name(relam);

					df_reject(cxt, "table %s uses access method %s",
							  get_rel_name(rte->relid), amname ? amname : "?");
					return;
				}
				cxt->allow_aggref = false;
				df_check_targetlist(plan->targetlist, needed, cxt);
				df_check_expr_list(plan->qual, cxt);
				return;
			}

		case T_Agg:
			{
				Agg		   *agg = (Agg *) plan;
				Plan	   *child = outerPlan(plan);
				Bitmapset  *child_needed = NULL;
				int			i;

				if (agg->aggstrategy != AGG_PLAIN && agg->aggstrategy != AGG_HASHED)
				{
					df_reject(cxt, "sorted or mixed aggregation");
					return;
				}
				if (cxt->join_input)
				{
					df_reject(cxt, "aggregate below a join");
					return;
				}
				/*
				 * Single-stage, or the first stage of a split aggregate: its
				 * output is the transition state, which for the supported
				 * aggregates is a plain value (count, sum and min/max of the
				 * supported types).  Or the combining stage, reading those
				 * states straight from the Motion that gathers them; other
				 * combining stages stay on PostgreSQL.
				 */
				if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
				{
					if (child == NULL || !IsA(child, Motion))
					{
						df_reject(cxt, "combining stage of a multi-stage aggregation not above a Motion");
						return;
					}
				}
				else if (agg->aggsplit != AGGSPLIT_SIMPLE &&
						 agg->aggsplit != AGGSPLIT_INITIAL_SERIAL)
				{
					df_reject(cxt, "combining stage of a multi-stage aggregation");
					return;
				}
				if (child != NULL && IsA(child, Agg))
				{
					/* e.g. DISTINCT aggregates: an aggregate over a grouping */
					df_reject(cxt, "aggregate over another aggregate");
					return;
				}
				for (i = 0; i < agg->numCols && child != NULL; i++)
				{
					TargetEntry *tle = get_tle_by_resno(child->targetlist,
														agg->grpColIdx[i]);

					if (tle && !df_type_supported(exprType((Node *) tle->expr)))
					{
						df_reject(cxt, "GROUP BY key of type %s",
								  format_type_be(exprType((Node *) tle->expr)));
						return;
					}
					if (tle && exprType((Node *) tle->expr) == NUMERICOID)
					{
						int			p,
									s;

						if (!df_numeric_ps(child, (Node *) tle->expr, &p, &s))
						{
							df_reject(cxt, "GROUP BY key of numeric of unknown precision");
							return;
						}
					}
					if (tle && df_type_is_string(exprType((Node *) tle->expr)) &&
						df_string_compare_problem(cxt, "=", agg->grpCollations[i]) != NULL)
					{
						df_reject(cxt, "GROUP BY key of type %s %s",
								  format_type_be(exprType((Node *) tle->expr)),
								  df_string_compare_problem(cxt, "=", agg->grpCollations[i]));
						return;
					}
				}
				cxt->partial_states = agg->aggsplit == AGGSPLIT_INITIAL_SERIAL && cxt->batch_sender;
				cxt->agg_to_batches = cxt->batch_sender;
				cxt->final_states = agg->aggsplit == AGGSPLIT_FINAL_DESERIAL && child != NULL &&
					IsA(child, Motion) &&
					bms_is_member(((Motion *) child)->motionID, cxt->batches);
				cxt->batch_sender = false;
				cxt->allow_aggref = true;
				df_check_targetlist(plan->targetlist, needed, cxt);
				df_check_expr_list(plan->qual, cxt);
				cxt->allow_aggref = false;
				cxt->partial_states = false;
				cxt->final_states = false;
				cxt->agg_to_batches = false;

				/* The child must produce what the aggregate reads. */
				df_collect_outer_refs((Node *) plan->targetlist, &child_needed);
				df_collect_outer_refs((Node *) plan->qual, &child_needed);
				for (i = 0; i < agg->numCols; i++)
					child_needed = bms_add_member(child_needed, agg->grpColIdx[i]);
				/* count(*) alone reads no column; keep the set non-NULL. */
				child_needed = bms_add_member(child_needed, 0);
				cxt->agg_input = true;
				df_check_plan(child, cxt, child_needed, false);
				cxt->agg_input = false;
				return;
			}

		case T_HashJoin:
			{
				HashJoin   *hj = (HashJoin *) plan;
				Join	   *join = &hj->join;
				Plan	   *outer = outerPlan(plan);
				Plan	   *inner = innerPlan(plan);
				Bitmapset  *outer_needed = NULL;
				Bitmapset  *inner_needed = NULL;
				ListCell   *lc;

				/*
				 * An equi-join, DataFusion's hash table on PostgreSQL's Hash
				 * side: inner, outer, semi or anti (J4).  NOT IN anti joins
				 * (LASJ) and IS NOT DISTINCT FROM joins (hashqualclauses)
				 * treat NULLs their own way and stay on PostgreSQL.
				 */
				if (join->jointype != JOIN_INNER && join->jointype != JOIN_LEFT &&
					join->jointype != JOIN_RIGHT && join->jointype != JOIN_FULL &&
					join->jointype != JOIN_SEMI && join->jointype != JOIN_ANTI &&
					join->jointype != JOIN_RIGHT_ANTI)
				{
					df_reject(cxt, "%s", join->jointype == JOIN_LASJ_NOTIN ?
							  "NOT IN anti join" : "this kind of join");
					return;
				}
				if (hj->hashqualclauses != NIL)
				{
					df_reject(cxt, "IS NOT DISTINCT FROM join");
					return;
				}
				if (inner == NULL || !IsA(inner, Hash) || outer == NULL)
				{
					df_reject(cxt, "Hash Join without a Hash node");
					return;
				}
				foreach(lc, hj->hashclauses)
				{
					OpExpr	   *op = lfirst(lc);
					char	   *name;

					if (!IsA(op, OpExpr) || list_length(op->args) != 2 ||
						(name = get_opname(op->opno)) == NULL || strcmp(name, "=") != 0)
					{
						df_reject(cxt, "Hash Join condition other than =");
						return;
					}
					if (!OidIsValid(df_join_key_type(exprType(linitial(op->args)),
													 exprType(lsecond(op->args)))))
					{
						df_reject(cxt, "Hash Join on %s = %s",
								  format_type_be(exprType(linitial(op->args))),
								  format_type_be(exprType(lsecond(op->args))));
						return;
					}
				}
				if (df_hash_estimate(inner) > df_hash_budget(inner))
				{
					df_reject(cxt, "Hash Join build side of about %.0f kB exceeds its %.0f kB",
							  df_hash_estimate(inner) / 1024, df_hash_budget(inner) / 1024);
					return;
				}
				cxt->allow_aggref = false;
				df_check_targetlist(plan->targetlist, needed, cxt);
				df_check_expr_list(hj->hashclauses, cxt);
				df_check_expr_list(join->joinqual, cxt);
				df_check_expr_list(plan->qual, cxt);
				if (cxt->failed)
					return;

				/* What each side must produce. */
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);

					if (needed == NULL || bms_is_member(tle->resno, needed))
					{
						df_collect_outer_refs((Node *) tle->expr, &outer_needed);
						df_collect_inner_refs((Node *) tle->expr, &inner_needed);
					}
				}
				df_collect_outer_refs((Node *) hj->hashclauses, &outer_needed);
				df_collect_inner_refs((Node *) hj->hashclauses, &inner_needed);
				df_collect_outer_refs((Node *) join->joinqual, &outer_needed);
				df_collect_inner_refs((Node *) join->joinqual, &inner_needed);
				df_collect_outer_refs((Node *) plan->qual, &outer_needed);
				df_collect_inner_refs((Node *) plan->qual, &inner_needed);

				/* Above an outer join, the nulled side's columns must be plain. */
				{
					Bitmapset  *above_outer = NULL;
					Bitmapset  *above_inner = NULL;

					foreach(lc, plan->targetlist)
					{
						TargetEntry *tle = lfirst_node(TargetEntry, lc);

						if (needed == NULL || bms_is_member(tle->resno, needed))
						{
							df_collect_outer_refs((Node *) tle->expr, &above_outer);
							df_collect_inner_refs((Node *) tle->expr, &above_inner);
						}
					}
					df_collect_outer_refs((Node *) plan->qual, &above_outer);
					df_collect_inner_refs((Node *) plan->qual, &above_inner);
					if (((join->jointype == JOIN_LEFT || join->jointype == JOIN_FULL) &&
						 !df_plain_outputs(inner, above_inner)) ||
						((join->jointype == JOIN_RIGHT || join->jointype == JOIN_FULL) &&
						 !df_plain_outputs(outer, above_outer)))
					{
						df_reject(cxt, "expression on the nullable side of an outer join");
						return;
					}
				}
				outer_needed = bms_add_member(outer_needed, 0);
				inner_needed = bms_add_member(inner_needed, 0);
				cxt->agg_input = false;
				cxt->join_input = true;
				df_check_plan(inner, cxt, inner_needed, false);
				cxt->join_input = true;
				df_check_plan(outer, cxt, outer_needed, false);
				cxt->join_input = false;
				return;
			}

		case T_Hash:
			{
				Bitmapset  *child_needed = NULL;
				ListCell   *lc;

				if (plan->qual != NIL)
				{
					df_reject(cxt, "filter on a Hash node");
					return;
				}
				df_check_targetlist(plan->targetlist, needed, cxt);
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);

					if (needed == NULL || bms_is_member(tle->resno, needed))
						df_collect_outer_refs((Node *) tle->expr, &child_needed);
				}
				child_needed = bms_add_member(child_needed, 0);
				df_check_plan(outerPlan(plan), cxt, child_needed, false);
				return;
			}

		case T_Limit:
			{
				Limit	   *limit = (Limit *) plan;
				int64		v;

				if (plan != cxt->top)
				{
					df_reject(cxt, "Limit below the top of the slice");
					return;
				}
				if (limit->limitOption != LIMIT_OPTION_COUNT)
				{
					df_reject(cxt, "LIMIT WITH TIES");
					return;
				}
				if (!df_limit_value(limit->limitCount, &v) ||
					!df_limit_value(limit->limitOffset, &v))
				{
					df_reject(cxt, "LIMIT or OFFSET that is not a constant of at least 0");
					return;
				}
				if (!df_passes_through(plan))
				{
					df_reject(cxt, "Limit that computes columns");
					return;
				}
				/* a Sort may be below it */
				cxt->top = outerPlan(plan);
				cxt->output = outerPlan(plan);
				df_check_plan(outerPlan(plan), cxt, needed, false);
				return;
			}

		case T_Sort:
			{
				Sort	   *sort = (Sort *) plan;
				Plan	   *child = outerPlan(plan);
				Bitmapset  *child_needed = needed ? bms_copy(needed) : NULL;
				int			i;

				if (plan != cxt->top)
				{
					df_reject(cxt, "Sort below the top of the slice");
					return;
				}
				cxt->top = NULL;
				if (!df_passes_through(plan))
				{
					df_reject(cxt, "Sort that computes columns");
					return;
				}
				for (i = 0; i < sort->numCols; i++)
				{
					TargetEntry *tle = get_tle_by_resno(child->targetlist, sort->sortColIdx[i]);
					Oid			type;
					bool		desc;

					if (tle == NULL)
					{
						df_reject(cxt, "sort key outside the target list");
						return;
					}
					type = exprType((Node *) tle->expr);
					if (!df_type_supported(type))
					{
						df_reject(cxt, "sort key of type %s", format_type_be(type));
						return;
					}
					if (!df_sort_direction(sort->sortOperators[i], type, &desc))
					{
						df_reject(cxt, "sort key ordered by operator %s",
								  get_opname(sort->sortOperators[i]));
						return;
					}
					if (df_type_is_string(type) &&
						df_string_compare_problem(cxt, "<", sort->collations[i]) != NULL)
					{
						df_reject(cxt, "sort key of type %s %s", format_type_be(type),
								  df_string_compare_problem(cxt, "<", sort->collations[i]));
						return;
					}
					if (IsA(tle->expr, Aggref) &&
						(df_agg_state((Aggref *) tle->expr) == DF_AGG_AVG_INT ||
						 df_agg_state((Aggref *) tle->expr) == DF_AGG_AVG_NUMERIC))
					{
						/* the C side divides it, after DataFusion */
						df_reject(cxt, "sort key avg returning numeric");
						return;
					}
					if (child_needed)
						child_needed = bms_add_member(child_needed, sort->sortColIdx[i]);
					/* sorted in DataFusion, so computed there */
					cxt->sort_keys = bms_add_member(cxt->sort_keys, sort->sortColIdx[i]);
				}
				cxt->output = child;
				df_check_plan(child, cxt, child_needed, false);
				return;
			}

		case T_Motion:
			{
				Motion	   *motion = (Motion *) plan;

				if (!root_is_sender)
				{
					ListCell   *lc;

					/*
					 * A receiving Motion stays on PostgreSQL; the main thread
					 * pulls the rows it receives and hands them to DataFusion
					 * in batches.  Worth it only below an aggregate.  The
					 * Motion's targetlist is evaluated by the sending slice;
					 * here only the types of the columns read matter.
					 */
					if (!cxt->agg_input && !cxt->join_input)
					{
						df_reject(cxt, "receives rows from a %s with nothing to compute on them",
								  df_plan_name(plan));
						return;
					}
					if (motion->sendSorted)
					{
						df_reject(cxt, "receives rows from a sorted %s", df_plan_name(plan));
						return;
					}
					if (plan->qual != NIL)
					{
						df_reject(cxt, "filter on a %s", df_plan_name(plan));
						return;
					}
					foreach(lc, plan->targetlist)
					{
						TargetEntry *tle = lfirst_node(TargetEntry, lc);
						Oid			type = exprType((Node *) tle->expr);

						bool		state = bms_is_member(motion->motionID, cxt->batches) &&
							df_motion_state_columns(motion, tle->resno) > 0;
						int			p,
									s;

						if (bms_is_member(tle->resno, needed) && !state &&
							(!df_type_supported(type) ||
							 (type == NUMERICOID &&
							  !df_numeric_ps(plan, (Node *) tle->expr, &p, &s))))
						{
							df_reject(cxt, "receives a column of type %s", format_type_be(type));
							return;
						}
					}
					return;
				}

				/*
				 * The sending Motion stays on PostgreSQL and pulls DataFusion's
				 * rows.  A sorted send needs them sorted: a Sort at the top of
				 * the slice (S1).  The remaining types belong to parallel or
				 * DML plans.
				 */
				if (motion->sendSorted &&
					(outerPlan(plan) == NULL || !df_sorted_for(outerPlan(plan), motion)))
				{
					df_reject(cxt, "sorted %s without a Sort below it", df_plan_name(plan));
					return;
				}
				if (motion->motionType != MOTIONTYPE_GATHER &&
					motion->motionType != MOTIONTYPE_GATHER_SINGLE &&
					motion->motionType != MOTIONTYPE_HASH &&
					motion->motionType != MOTIONTYPE_BROADCAST)
				{
					df_reject(cxt, "%s is not supported", df_plan_name(plan));
					return;
				}
				cxt->agg_input = false;
				cxt->batch_sender = bms_is_member(motion->motionID, cxt->batches);
				df_check_plan(outerPlan(plan), cxt, NULL, false);
				cxt->batch_sender = false;
				return;
			}

		default:
			df_reject_plan(cxt, plan);
			return;
	}
}

/*
 * Can DataFusion run the slice whose top plan node is 'root'?  If not, the
 * reason is written to 'reason'.  'root_is_sender' says that 'root' is the
 * Motion this slice sends through (every slice but the top one); a Motion
 * anywhere else in the slice is a receiver.
 */
static bool
df_check_slice_b(PlannedStmt *stmt, Plan *root, bool root_is_sender,
				 Bitmapset *batches, DfTails *tails, char *reason, size_t reasonlen,
				 bool *locale_dependent)
{
	DfCheckContext cxt;

	memset(&cxt, 0, sizeof(cxt));
	cxt.stmt = stmt;
	cxt.batches = batches;
	cxt.failed = false;
	cxt.reason = reason;
	cxt.reasonlen = reasonlen;
	reason[0] = '\0';

	if (stmt->commandType != CMD_SELECT)
		df_reject(&cxt, "only SELECT is supported");
	else if (stmt->intoClause != NULL)
		df_reject(&cxt, "SELECT INTO and CREATE TABLE AS are not supported");
	else if (stmt->rowMarks != NIL)
		df_reject(&cxt, "row locking clauses are not supported");
	else if (stmt->hasModifyingCTE)
		df_reject(&cxt, "data-modifying WITH is not supported");
	else
	{
		cxt.top = root_is_sender ? outerPlan(root) : root;
		cxt.output = cxt.top;
		cxt.tails_ok = !(root_is_sender &&
						 bms_is_member(((Motion *) root)->motionID, batches));
		df_check_plan(root, &cxt, NULL, root_is_sender);
	}

	if (locale_dependent)
		*locale_dependent = cxt.locale_dependent;
	if (tails)
		*tails = cxt.tails;
	return !cxt.failed;
}

bool
df_check_slice(PlannedStmt *stmt, Plan *root, bool root_is_sender,
			   DfTails *tails, char *reason, size_t reasonlen)
{
	return df_check_slice_b(stmt, root, root_is_sender, df_batch_motions(stmt),
							tails, reason, reasonlen, NULL);
}

/*
 * The top plan node of the slice this process executes: the Motion it sends
 * through, or the plan's top node for the top slice.  On the coordinator
 * that is the tree its executor runs: the dispatcher replaces
 * plannedstmt->planTree by a copy with parameters folded, for the
 * segments, after the executor was set up on the original.
 */
Plan *
df_local_slice_root(QueryDesc *queryDesc, int *slice_index, bool *is_sender)
{
	int			idx = LocallyExecutingSliceIndex(queryDesc->estate);
	Motion	   *sender = findSenderMotion(queryDesc->plannedstmt, idx);

	*slice_index = idx;
	*is_sender = (sender != NULL);
	if (sender)
		return (Plan *) sender;
	return queryDesc->planstate ? queryDesc->planstate->plan : queryDesc->plannedstmt->planTree;
}

/* ---------------------------------------------------------------------
 * M7b: which Gather Motions carry Arrow batches
 * ---------------------------------------------------------------------
 */
bool		df_motion_batches = false;

/*
 * Would the executor hook run slice 'index' in DataFusion, if the Motions
 * in 'batches' carry batches?  The same check and translation it applies,
 * from the plan alone.  A slice whose verdict depends on the node's locale
 * counts as not running there: every node must find the same batch
 * Motions, and its Motions carry tuples whichever way each node decides.
 */
static bool
df_slice_runs_in_datafusion(PlannedStmt *stmt, int index, Bitmapset *batches)
{
	char		reason[256];
	DfSliceSpec spec;
	DfTails		tails;
	Motion	   *sender;
	Plan	   *root;
	Plan	   *compute;
	bool		locale_dependent = false;

	if (index < 0 || index >= stmt->numSlices)
		return false;
	sender = findSenderMotion(stmt, index);
	root = sender ? (Plan *) sender : stmt->planTree;
	compute = sender ? outerPlan(root) : root;
	if (compute != NULL &&
		df_check_slice_b(stmt, root, sender != NULL, batches, &tails, reason, sizeof(reason),
						 &locale_dependent) &&
		!locale_dependent &&
		df_translate_slice(compute, &tails, &spec, reason, sizeof(reason)))
		return true;
	if (locale_dependent)
		snprintf(reason, sizeof(reason), "its verdict depends on the node's default collation");
	elog(DEBUG2, "datafusion: with these batch Motions slice %d cannot run: %s",
		 index, compute ? reason : "no plan");
	return false;
}

/*
 * Distribution key 'i' of a Redistribute Motion, if DataFusion can hash it
 * as cdbhash() does: a plain column of a supported type, hashed by that
 * type's own non-legacy hash function.  Sets the 0-based output column and
 * the type tag (which also names the hash, see df_core::cdbhash).
 */
bool
df_motion_hash_key(Motion *motion, int i, int *column, const char **tag)
{
	static const struct
	{
		Oid			type;
		const char *func;
	}			hashes[] =
	{
		{BOOLOID, "hashchar"}, {INT2OID, "hashint2"}, {INT4OID, "hashint4"},
		{INT8OID, "hashint8"}, {FLOAT4OID, "hashfloat4"}, {FLOAT8OID, "hashfloat8"},
		{DATEOID, "hashint4"}, {TIMEOID, "time_hash"},
		{TIMESTAMPOID, "timestamp_hash"}, {TIMESTAMPTZOID, "timestamp_hash"},
#ifndef WORDS_BIGENDIAN
		/* cdbhash passes the default collation: hash_any of the bytes */
		{TEXTOID, "hashtext"}, {VARCHAROID, "hashtext"},
#endif
	};
	Node	   *expr = (Node *) list_nth(motion->hashExprs, i);
	Var		   *var;
	char	   *func;
	int			k;

	while (IsA(expr, RelabelType))
		expr = (Node *) ((RelabelType *) expr)->arg;
	if (!IsA(expr, Var) || ((Var *) expr)->varattno <= 0)
		return false;
	var = (Var *) expr;
	func = get_func_name(motion->hashFuncs[i]);
	if (func == NULL || motion->hashFuncs[i] >= FirstGenbkiObjectId)
		return false;
	for (k = 0; k < lengthof(hashes); k++)
	{
		if (hashes[k].type == var->vartype && strcmp(hashes[k].func, func) == 0)
		{
			*column = var->varattno - 1;
			*tag = df_type_tag(var->vartype);
			return true;
		}
	}
	return false;
}

/*
 * Could 'motion' carry batches at all: a plain Gather or Broadcast, or a
 * Redistribute whose keys DataFusion hashes as cdbhash() does.
 */
static bool
df_motion_batchable(Motion *motion)
{
	if (motion->sendSorted)
		return false;
	if (motion->motionType == MOTIONTYPE_HASH)
	{
		int			n = list_length(motion->hashExprs);
		int			i;

		if (n == 0)
			return false;		/* random distribution */
		for (i = 0; i < n; i++)
		{
			int			column;
			const char *tag;

			if (!df_motion_hash_key(motion, i, &column, &tag))
				return false;
		}
		return true;
	}
	return motion->motionType == MOTIONTYPE_GATHER ||
		motion->motionType == MOTIONTYPE_BROADCAST;
}

static void
df_list_motions(Plan *plan, List **motions)
{
	if (plan == NULL)
		return;
	if (IsA(plan, Motion))
		*motions = lappend(*motions, plan);
	df_list_motions(outerPlan(plan), motions);
	df_list_motions(innerPlan(plan), motions);
}

/*
 * The Motions of 'stmt' that carry Arrow IPC batches instead of tuples:
 * those whose sending and receiving slices both run in DataFusion.  A slice
 * may itself need its Motions to carry batches (a split avg passes
 * DataFusion's state, M7d), so this is the largest set for which that
 * holds: start from every Motion that could, and drop those with a slice
 * that cannot run given the rest, until none is dropped.  Every process of
 * the query computes it from the plan and the synchronized settings alone,
 * so the senders and the receivers agree.  Should they not (a slice falling
 * back for a reason outside the plan), the chunks carry a type of their
 * own: PostgreSQL's receiver rejects it with an error, and ours rejects
 * tuple chunks, so nothing is ever misread.
 */
Bitmapset *
df_batch_motions(PlannedStmt *stmt)
{
	List	   *motions = NIL;
	Bitmapset  *batches = NULL;
	ListCell   *lc;
	bool		changed;

	if (!df_motion_batches || df_mode == DF_MODE_OFF)
		return NULL;
	df_list_motions(stmt->planTree, &motions);
	foreach(lc, motions)
	{
		Motion	   *m = (Motion *) lfirst(lc);

		if (m->motionID > 0 && m->motionID < stmt->numSlices && df_motion_batchable(m))
			batches = bms_add_member(batches, m->motionID);
	}
	do
	{
		changed = false;
		foreach(lc, motions)
		{
			Motion	   *m = (Motion *) lfirst(lc);

			if (bms_is_member(m->motionID, batches) &&
				!(df_slice_runs_in_datafusion(stmt, m->motionID, batches) &&
				  df_slice_runs_in_datafusion(stmt, stmt->slices[m->motionID].parentIndex,
											  batches)))
			{
				batches = bms_del_member(batches, m->motionID);
				changed = true;
			}
		}
	} while (changed);
	return batches;
}

/* Does 'motion' carry Arrow IPC batches instead of tuples? */
bool
df_motion_sends_batches(PlannedStmt *stmt, Motion *motion)
{
	return bms_is_member(motion->motionID, df_batch_motions(stmt));
}

/*
 * Columns of DataFusion state that column 'resno' of 'motion' carries
 * through a batch Motion: 2 for a partial avg (sum, count), 1 for a partial
 * sum(int8), 0 for an ordinary column.
 */
int
df_motion_state_columns(Motion *motion, AttrNumber resno)
{
	TargetEntry *tle = get_tle_by_resno(motion->plan.targetlist, resno);
	Plan	   *child = outerPlan(motion);
	Node	   *expr;

	if (tle == NULL || child == NULL)
		return 0;
	expr = (Node *) tle->expr;
	if (IsA(expr, Var) && ((Var *) expr)->varno == OUTER_VAR)
	{
		tle = get_tle_by_resno(child->targetlist, ((Var *) expr)->varattno);
		if (tle == NULL)
			return 0;
		expr = (Node *) tle->expr;
	}
	if (!IsA(expr, Aggref) || ((Aggref *) expr)->aggsplit != AGGSPLIT_INITIAL_SERIAL)
		return 0;
	return df_agg_state_ncols(df_agg_state((Aggref *) expr));
}

/* Which state DataFusion keeps for 'agg'. */
DfAggState
df_agg_state(Aggref *agg)
{
	char	   *name = get_func_name(agg->aggfnoid);
	Oid			argtype = list_length(agg->aggargtypes) == 1 ?
		linitial_oid(agg->aggargtypes) : InvalidOid;

	if (name == NULL || agg->aggfnoid >= FirstGenbkiObjectId)
		return DF_AGG_PLAIN;
	if (strcmp(name, "avg") == 0)
	{
		if (argtype == FLOAT4OID || argtype == FLOAT8OID)
			return DF_AGG_AVG_FLOAT;
		if (argtype == INT2OID || argtype == INT4OID || argtype == INT8OID)
			return DF_AGG_AVG_INT;
	}
	if (strcmp(name, "sum") == 0 && argtype == INT8OID)
		return DF_AGG_SUM_INT8;
	if (argtype == NUMERICOID && strcmp(name, "sum") == 0)
		return DF_AGG_SUM_NUMERIC;
	if (argtype == NUMERICOID && strcmp(name, "avg") == 0)
		return DF_AGG_AVG_NUMERIC;
	return DF_AGG_PLAIN;
}

/* Columns its state takes in a batch stream (0 for a plain aggregate). */
int
df_agg_state_ncols(DfAggState state)
{
	switch (state)
	{
		case DF_AGG_AVG_FLOAT:
		case DF_AGG_AVG_INT:
		case DF_AGG_AVG_NUMERIC:
			return 2;
		case DF_AGG_SUM_INT8:
		case DF_AGG_SUM_NUMERIC:
			return 1;
		default:
			return 0;
	}
}

/*
 * 0-based position in the batch stream of column 'resno' of 'motion': a
 * partial avg's state takes two columns (sum, then count).
 */
int
df_motion_stream_column(Motion *motion, AttrNumber resno)
{
	int			pos = 0;
	AttrNumber	r;

	for (r = 1; r < resno; r++)
		pos += Max(df_motion_state_columns(motion, r), 1);
	return pos;
}

/*
 * What both ends of a batch Motion must agree on, hashed (FNV-1a): the
 * Motion and the types of the columns it carries.  The sender puts it at
 * the start of each stream and the receiver checks it.
 */
uint64
df_motion_signature(Motion *motion)
{
	uint64		h = UINT64CONST(14695981039346656037);
	ListCell   *lc;

#define DF_MIX(v) (h = (h ^ (uint64) (v)) * UINT64CONST(1099511628211))
	DF_MIX(motion->motionID);
	DF_MIX(list_length(motion->plan.targetlist));
	foreach(lc, motion->plan.targetlist)
		DF_MIX(exprType((Node *) lfirst_node(TargetEntry, lc)->expr));
#undef DF_MIX
	return h;
}

/* ---------------------------------------------------------------------
 * EXPLAIN: one line per slice
 * ---------------------------------------------------------------------
 */
typedef struct DfSliceRoot
{
	int			index;
	Plan	   *root;
	bool		is_sender;		/* root is the Motion this slice sends through */
} DfSliceRoot;

typedef struct DfCollectContext
{
	DfSliceRoot *roots;
	int			nroots;
	int			maxroots;
} DfCollectContext;

static void
df_add_root(DfCollectContext *cxt, int index, Plan *root, bool is_sender)
{
	if (cxt->nroots == cxt->maxroots)
	{
		cxt->maxroots *= 2;
		cxt->roots = repalloc(cxt->roots, sizeof(DfSliceRoot) * cxt->maxroots);
	}
	cxt->roots[cxt->nroots].index = index;
	cxt->roots[cxt->nroots].root = root;
	cxt->roots[cxt->nroots].is_sender = is_sender;
	cxt->nroots++;
}

static bool
df_collect_motions(Node *node, DfCollectContext *cxt)
{
	if (node == NULL)
		return false;
	if (IsA(node, Motion))
		df_add_root(cxt, ((Motion *) node)->motionID, (Plan *) node, true);
	return plan_tree_walker(node, df_collect_motions, cxt, true);
}

static int
df_root_cmp(const void *a, const void *b)
{
	return ((const DfSliceRoot *) a)->index - ((const DfSliceRoot *) b)->index;
}

/* Append "DataFusion: slice N ..." lines for every slice of 'stmt'. */
void
df_explain_slices(PlannedStmt *stmt, StringInfo out)
{
	DfCollectContext cxt;
	int			root_index = 0;
	int			i;

	for (i = 0; i < stmt->numSlices; i++)
	{
		if (stmt->slices[i].parentIndex < 0)
		{
			root_index = stmt->slices[i].sliceIndex;
			break;
		}
	}

	cxt.maxroots = 8;
	cxt.nroots = 0;
	cxt.roots = palloc(sizeof(DfSliceRoot) * cxt.maxroots);
	df_add_root(&cxt, root_index, stmt->planTree, false);
	df_collect_motions((Node *) stmt->planTree, &cxt);
	qsort(cxt.roots, cxt.nroots, sizeof(DfSliceRoot), df_root_cmp);

	for (i = 0; i < cxt.nroots; i++)
	{
		char		reason[256];

		if (df_check_slice(stmt, cxt.roots[i].root, cxt.roots[i].is_sender, NULL,
						   reason, sizeof(reason)))
			appendStringInfo(out, "DataFusion: slice %d eligible%s\n", cxt.roots[i].index,
							 cxt.roots[i].is_sender &&
							 df_motion_sends_batches(stmt, (Motion *) cxt.roots[i].root) ?
							 ", sends Arrow batches" : "");
		else
			appendStringInfo(out, "DataFusion: slice %d not eligible: %s\n",
							 cxt.roots[i].index, reason);
	}
	pfree(cxt.roots);
}
