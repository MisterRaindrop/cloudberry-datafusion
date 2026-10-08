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
#include "parser/scansup.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/timestamp.h"
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
	bool		inner_agg;		/* checking an Agg below a join or another
								 * Agg, whose values others read (A1) */
	bool		projection_input;	/* checking the plan below a Subquery
									 * Scan or a Result */
	bool		rowid_ok;		/* checking a scan's or join's targetlist,
								 * where a RowIdExpr may be (RI1) */
	Bitmapset  *batches;		/* Motions carrying batches (df_batch_motions) */
	bool		batch_sender;	/* checking the child of a batch-sending Motion */
	bool		partial_states; /* this Agg may output DataFusion avg states */
	bool		final_states;	/* this Agg may read DataFusion avg states */
	bool		agg_to_batches; /* this Agg's results go into a batch Motion */
	bool		locale_dependent;	/* the verdict depends on this node's locale */
	Node	   *mixed_scales_ok;	/* a sum's argument whose branches may have
									 * different scales (MS1) */
	Node	   *numeric_param_ok;	/* a numeric parameter compared with a
									 * numeric of known scale (IP1) */
	Bitmapset  *init_params;	/* PARAM_EXEC ids init plans set (IP1) */
	bool		init_params_found;
	Plan	   *node;			/* the node whose expressions are checked */
	Plan	   *top;			/* where a Limit or Sort may be: the top of
								 * the slice, or below its Limit (S1) */
	Plan	   *output;			/* the node whose targetlist is the slice's
								 * output: below any Limit and Sort */
	Bitmapset  *sort_keys;		/* its columns the slice's Sort orders by */
	bool		tails_ok;		/* PostgreSQL may finish output columns (P1):
								 * the slice does not send batches */
	bool		order_free;		/* set for a child: no one reads the order of
								 * its rows (D2) */
	bool		node_order_free;	/* that, for the node being checked */
	Plan	   *skipped_sort;	/* the Sort below such a sorted Agg, which
								 * DataFusion leaves out (D2) */
	DfTails		tails;			/* the columns it finishes */
	bool		failed;
	char	   *reason;
	size_t		reasonlen;
} DfCheckContext;

static void df_reject(DfCheckContext *cxt, const char *fmt,...) pg_attribute_printf(2, 3);
static bool df_check_expr(Node *node, DfCheckContext *cxt);
static bool df_check_resort_limit(DfCheckContext *cxt, Motion *motion, Limit *limit);
static bool df_name_in(const char *name, const char *const *list);
static void df_check_plan(Plan *plan, DfCheckContext *cxt, Bitmapset *needed,
						  bool root_is_sender);
static void df_check_plan_node(Plan *plan, DfCheckContext *cxt, Bitmapset *needed,
							   bool root_is_sender);

/* Add the parameters the init plans of 'plan' and below it set. */
static void
df_collect_init_params(PlannedStmt *stmt, Plan *plan, Bitmapset **params)
{
	ListCell   *lc;

	if (plan == NULL)
		return;
	foreach(lc, plan->initPlan)
	{
		SubPlan    *sp = lfirst_node(SubPlan, lc);
		ListCell   *lp;

		foreach(lp, sp->setParam)
			*params = bms_add_member(*params, lfirst_int(lp));
	}
	df_collect_init_params(stmt, outerPlan(plan), params);
	df_collect_init_params(stmt, innerPlan(plan), params);
	if (IsA(plan, SubqueryScan))
		df_collect_init_params(stmt, ((SubqueryScan *) plan)->subplan, params);
	if (IsA(plan, Append))
		foreach(lc, ((Append *) plan)->appendplans)
			df_collect_init_params(stmt, lfirst(lc), params);
}

/*
 * The PARAM_EXEC ids that init plans of the statement set (IP1), whose
 * values are known when a slice starts: the coordinator computes them
 * before dispatching (preprocess_initplans) and the segments receive them.
 */
static Bitmapset *
df_init_params(DfCheckContext *cxt)
{
	ListCell   *lc;

	if (!cxt->init_params_found)
	{
		df_collect_init_params(cxt->stmt, cxt->stmt->planTree, &cxt->init_params);
		foreach(lc, cxt->stmt->subplans)
			df_collect_init_params(cxt->stmt, lfirst(lc), &cxt->init_params);
		cxt->init_params_found = true;
	}
	return cxt->init_params;
}

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
		case BPCHAROID:
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
 * Is 'op' a date compared with a timestamp constant (DT1), such as the
 * folded "d < date '1994-01-01' + interval '1' year"?  Sets 'date_arg'
 * and the date comparison that answers it: 'cmp' on the date integers
 * with 'value'.  PostgreSQL compares d * USECS_PER_DAY with the timestamp,
 * dates past the timestamp range after every finite timestamp
 * (date_cmp_timestamp_internal).  Those and the infinities lie on either
 * side of the finite date the constant maps to, so comparing integers
 * answers the same.  An = or <> on a constant within a day comes out
 * constant (< or >= the earliest date).
 */
bool
df_date_timestamp_cmp(OpExpr *op, Node **date_arg, const char **cmp, int32 *value)
{
	static const char *const names[][2] = {
		{"<", ">"}, {"<=", ">="}, {">", "<"}, {">=", "<="}, {"=", "="}, {"<>", "<>"}
	};
	Node	   *l,
			   *r;
	Const	   *c;
	char	   *name;
	Timestamp	t;
	int64		floor_day;
	bool		exact;
	int			i;

	if (list_length(op->args) != 2 || op->opresulttype != BOOLOID)
		return false;
	l = linitial(op->args);
	r = lsecond(op->args);
	if (exprType(l) == DATEOID && exprType(r) == TIMESTAMPOID && IsA(r, Const))
		c = (Const *) r;
	else if (exprType(l) == TIMESTAMPOID && exprType(r) == DATEOID && IsA(l, Const))
	{
		c = (Const *) l;
		l = r;
	}
	else
		return false;
	if (c->constisnull || (name = get_opname(op->opno)) == NULL)
		return false;
	for (i = 0; i < lengthof(names) && strcmp(names[i][0], name) != 0; i++)
		;
	if (i == lengthof(names))
		return false;
	/* the constant on the left: t < d is d > t */
	*cmp = names[i][(Node *) c == linitial(op->args) ? 1 : 0];
	*date_arg = l;

	t = DatumGetTimestamp(c->constvalue);
	if (TIMESTAMP_IS_NOBEGIN(t) || TIMESTAMP_IS_NOEND(t))
	{
		*value = TIMESTAMP_IS_NOBEGIN(t) ? DATEVAL_NOBEGIN : DATEVAL_NOEND;
		return true;
	}
	floor_day = t / USECS_PER_DAY - (t % USECS_PER_DAY < 0 ? 1 : 0);
	exact = t % USECS_PER_DAY == 0;
	if (strcmp(*cmp, "<") == 0 || strcmp(*cmp, ">=") == 0)
		*value = (int32) (floor_day + (exact ? 0 : 1));
	else if (strcmp(*cmp, "<=") == 0 || strcmp(*cmp, ">") == 0 || exact)
		*value = (int32) floor_day;
	else
	{
		/* = within a day: never; <> always (NULL on NULL) */
		*cmp = strcmp(*cmp, "=") == 0 ? "<" : ">=";
		*value = DATEVAL_NOBEGIN;
	}
	return true;
}

/*
 * text and varchar travel as their bytes, which DataFusion compares byte by
 * byte: equality agrees with PostgreSQL under a deterministic collation,
 * ordering (and min/max) only under the C collation.  character (B1) as
 * stored, blank-padded; it compares, hashes, groups and sorts without its
 * trailing blanks (df_core::pgstr::bpchar_key), as bpcharcmp does.
 */
static bool
df_type_is_string(Oid type)
{
	return type == TEXTOID || type == VARCHAROID || type == BPCHAROID;
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

/*
 * B1: functions of character DataFusion runs: the cast to text (and
 * varchar), which drops trailing blanks ("bpkey"), and the lengths
 * ("char_length" of that; "octet_length" of the padded value).
 */
const char *
df_bpchar_func(Oid funcid)
{
	switch (funcid)
	{
		case F_TEXT_BPCHAR:
			return "bpkey";
		case F_LENGTH_BPCHAR:
		case F_CHAR_LENGTH_BPCHAR:
		case F_CHARACTER_LENGTH_BPCHAR:
			return "char_length";
		case F_OCTET_LENGTH_BPCHAR:
			return "octet_length";
		default:
			return NULL;
	}
}

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
 * The field of extract(field from date) that DataFusion computes (DT2:
 * df_core::pgdate), or NULL: year, quarter, month or day, each a numeric
 * of scale 0.
 */
const char *
df_extract_field(FuncExpr *fe)
{
	static const char *const fields[] = {"year", "quarter", "month", "day", NULL};
	Const	   *c;
	char	   *lower;
	int			i;

	if (fe->funcid != F_EXTRACT_TEXT_DATE || list_length(fe->args) != 2 ||
		!IsA(linitial(fe->args), Const))
		return NULL;
	c = linitial_node(Const, fe->args);
	if (c->constisnull)
		return NULL;
	lower = downcase_truncate_identifier(VARDATA_ANY(DatumGetTextPP(c->constvalue)),
										 VARSIZE_ANY_EXHDR(DatumGetTextPP(c->constvalue)),
										 false);
	for (i = 0; fields[i] != NULL; i++)
		if (strcmp(lower, fields[i]) == 0)
			return fields[i];
	return NULL;
}

/*
 * Can numeric expression 'expr' of plan node 'ctx' be infinite (DT2)?  The
 * year of an infinite date is, and so are min/max, CASE and COALESCE of
 * one, through references.  DataFusion holds an infinity as a value beyond
 * 76 digits that sorts and groups as PostgreSQL does, but arithmetic,
 * rescaling, casts and sums would take it for a number.
 */
static bool
df_numeric_may_be_infinite(Plan *ctx, Node *expr)
{
	ListCell   *lc;

	if (expr == NULL || exprType(expr) != NUMERICOID)
		return false;
	switch (nodeTag(expr))
	{
		case T_Var:
			{
				Var		   *var = (Var *) expr;
				Plan	   *child;
				TargetEntry *tle;

				if (IsA(ctx, SubqueryScan) && var->varno == ((Scan *) ctx)->scanrelid)
					child = ((SubqueryScan *) ctx)->subplan;
				else if (var->varno == OUTER_VAR || var->varno == INNER_VAR)
					child = var->varno == OUTER_VAR ? outerPlan(ctx) : innerPlan(ctx);
				else
					return false;
				tle = child ? get_tle_by_resno(child->targetlist, var->varattno) : NULL;
				return tle != NULL && df_numeric_may_be_infinite(child, (Node *) tle->expr);
			}
		case T_FuncExpr:
			{
				const char *f = df_extract_field((FuncExpr *) expr);

				return f != NULL && strcmp(f, "year") == 0;
			}
		case T_CaseExpr:
			foreach(lc, ((CaseExpr *) expr)->args)
				if (df_numeric_may_be_infinite(ctx, (Node *) lfirst_node(CaseWhen, lc)->result))
					return true;
			return df_numeric_may_be_infinite(ctx, (Node *) ((CaseExpr *) expr)->defresult);
		case T_CoalesceExpr:
			foreach(lc, ((CoalesceExpr *) expr)->args)
				if (df_numeric_may_be_infinite(ctx, lfirst(lc)))
					return true;
			return false;
		case T_NullIfExpr:
			return df_numeric_may_be_infinite(ctx, linitial(((NullIfExpr *) expr)->args));
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) expr;
				char	   *name = get_func_name(agg->aggfnoid);

				return name != NULL && list_length(agg->args) == 1 &&
					(strcmp(name, "min") == 0 || strcmp(name, "max") == 0) &&
					df_numeric_may_be_infinite(ctx, (Node *) linitial_node(TargetEntry, agg->args)->expr);
			}
		default:
			return false;
	}
}

/*
 * Would comparing numeric 'l' and 'r' at their common scale rescale one
 * that may be infinite (DT2)?
 */
static bool
df_rescales_infinity(Plan *ctx, Node *l, Node *r)
{
	int			lp,
				ls,
				rp,
				rs;

	if (!df_numeric_ps(ctx, l, &lp, &ls) || !df_numeric_ps(ctx, r, &rp, &rs))
		return false;
	return (ls < rs && df_numeric_may_be_infinite(ctx, l)) ||
		(rs < ls && df_numeric_may_be_infinite(ctx, r));
}

/*
 * Is numeric 'expr' of plan node 'ctx' a CASE or COALESCE whose results
 * have scales of their own, not all the same (MS1)?  Each row's value then
 * has the display scale of its branch.  Sets the largest scale and the
 * precision at it.
 */
bool
df_numeric_mixed(Plan *ctx, Node *expr, int *precision, int *scale)
{
	List	   *results = NIL;
	ListCell   *lc;
	int			first = -1,
				digits = 0;
	bool		mixed = false;

	if (expr == NULL || exprType(expr) != NUMERICOID)
		return false;
	if (IsA(expr, CaseExpr))
	{
		foreach(lc, ((CaseExpr *) expr)->args)
			results = lappend(results, lfirst_node(CaseWhen, lc)->result);
		results = lappend(results, ((CaseExpr *) expr)->defresult);
	}
	else if (IsA(expr, CoalesceExpr))
		results = ((CoalesceExpr *) expr)->args;
	else
		return false;
	*scale = 0;
	foreach(lc, results)
	{
		Node	   *e = lfirst(lc);
		int			p,
					s;

		if (e == NULL || (IsA(e, Const) && ((Const *) e)->constisnull))
			continue;
		if (!df_numeric_ps(ctx, e, &p, &s) || df_numeric_may_be_infinite(ctx, e))
			return false;
		if (first >= 0 && s != first)
			mixed = true;
		if (first < 0)
			first = s;
		*scale = Max(*scale, s);
		digits = Max(digits, p - s);
	}
	*precision = digits + *scale;
	return mixed && *precision <= DF_NUMERIC_MAX_EXPR_PRECISION;
}

/*
 * Is 'agg', of Agg node 'ctx', a sum of such a CASE or COALESCE (MS1)?  A
 * combining sum is if the partial sum below it is.  PostgreSQL's sum takes
 * the largest display scale of the values it adds up, which DataFusion
 * keeps beside the sum.
 */
static bool
df_sum_mixed(Plan *ctx, Aggref *agg)
{
	char	   *name = get_func_name(agg->aggfnoid);
	Node	   *arg;

	if (name == NULL || strcmp(name, "sum") != 0 || agg->aggfnoid >= FirstGenbkiObjectId ||
		list_length(agg->args) != 1 || list_length(agg->aggargtypes) != 1 ||
		linitial_oid(agg->aggargtypes) != NUMERICOID || agg->aggdistinct != NIL)
		return false;
	arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
	if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
	{
		/* the partial sum, through the Motion and any Sort between */
		Plan	   *child = ctx;

		while (IsA(arg, Var) && ((Var *) arg)->varno == OUTER_VAR &&
			   (child = outerPlan(child)) != NULL)
		{
			TargetEntry *tle = get_tle_by_resno(child->targetlist, ((Var *) arg)->varattno);

			if (tle == NULL)
				return false;
			arg = (Node *) tle->expr;
		}
		return child != NULL && IsA(arg, Aggref) &&
			((Aggref *) arg)->aggsplit == AGGSPLIT_INITIAL_SERIAL &&
			df_sum_mixed(child, (Aggref *) arg);
	}
	{
		int			p,
					s;

		return df_numeric_mixed(ctx, arg, &p, &s);
	}
}

/*
 * The scale of MS1's sum that numeric 'expr' of node 'ctx' is, or refers
 * to through the nodes below: the largest of its argument's branches.
 */
bool
df_mixed_sum_scale(Plan *ctx, Node *expr, int *scale)
{
	int			p;

	while (IsA(expr, Var) &&
		   (((Var *) expr)->varno == OUTER_VAR || ((Var *) expr)->varno == INNER_VAR ||
			(IsA(ctx, SubqueryScan) && ((Var *) expr)->varno == ((Scan *) ctx)->scanrelid)))
	{
		Plan	   *child = IsA(ctx, SubqueryScan) && ((Var *) expr)->varno == ((Scan *) ctx)->scanrelid ?
			((SubqueryScan *) ctx)->subplan :
			((Var *) expr)->varno == OUTER_VAR ? outerPlan(ctx) : innerPlan(ctx);
		TargetEntry *tle = child ? get_tle_by_resno(child->targetlist,
													((Var *) expr)->varattno) : NULL;

		if (tle == NULL)
			return false;
		ctx = child;
		expr = (Node *) tle->expr;
	}
	if (!IsA(expr, Aggref) || df_agg_state_at(ctx, (Aggref *) expr) != DF_AGG_SUM_NUMERIC_MIXED)
		return false;
	if (((Aggref *) expr)->aggsplit == AGGSPLIT_FINAL_DESERIAL)
		/* its argument is the partial sum below */
		return df_mixed_sum_scale(ctx, (Node *) linitial_node(TargetEntry,
															  ((Aggref *) expr)->args)->expr,
								  scale);
	return df_numeric_mixed(ctx, (Node *) linitial_node(TargetEntry, ((Aggref *) expr)->args)->expr,
							&p, scale);
}

/* df_agg_state of 'agg' of Agg node 'ctx', telling apart MS1's sums. */
DfAggState
df_agg_state_at(Plan *ctx, Aggref *agg)
{
	DfAggState	state = df_agg_state(agg);

	if (state == DF_AGG_SUM_NUMERIC && df_sum_mixed(ctx, agg))
		return DF_AGG_SUM_NUMERIC_MIXED;
	return state;
}

/*
 * Is 'op' a comparison of a numeric of known scale (at most 38 digits, not
 * infinite) with a numeric init plan parameter (IP1)?  Sets the other side
 * and whether the parameter is on the left.  The parameter's value is
 * rounded to that side's scale when the slice starts.
 */
bool
df_numeric_param_cmp(Plan *ctx, OpExpr *op, Node **other, bool *param_left)
{
	static const char *const cmps[] = {"<", "<=", ">", ">=", "=", "<>", NULL};
	Node	   *l,
			   *r,
			   *x;
	char	   *name;
	int			p,
				s;

	if (list_length(op->args) != 2 || op->opresulttype != BOOLOID ||
		(name = get_opname(op->opno)) == NULL || !df_name_in(name, cmps))
		return false;
	l = linitial(op->args);
	r = lsecond(op->args);
	if (exprType(l) != NUMERICOID || exprType(r) != NUMERICOID ||
		IsA(l, Param) == IsA(r, Param) ||
		((Param *) (IsA(l, Param) ? l : r))->paramkind != PARAM_EXEC)
		return false;
	x = IsA(l, Param) ? r : l;
	if (!df_numeric_ps(ctx, x, &p, &s) || p > DF_NUMERIC_MAX_PRECISION ||
		df_numeric_may_be_infinite(ctx, x))
		return false;
	if (other)
		*other = x;
	if (param_left)
		*param_left = IsA(l, Param);
	return true;
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
				if (IsA(ctx, SubqueryScan) && var->varno == ((Scan *) ctx)->scanrelid)
				{
					/* a column of the subquery: its expression there */
					Plan	   *sub = ((SubqueryScan *) ctx)->subplan;

					tle = get_tle_by_resno(sub->targetlist, var->varattno);
					return tle != NULL && df_numeric_ps(sub, (Node *) tle->expr, precision, scale);
				}
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
				if (df_extract_field(fe) != NULL)
				{
					/* DT2: years run from -4714 to 5874897 */
					*precision = strcmp(df_extract_field(fe), "year") == 0 ? 7 : 2;
					return true;
				}
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
 * D1: what keeps DataFusion from telling the arguments of aggregate 'agg'
 * apart as its DISTINCT does, by equality, or NULL.  Strings need a
 * deterministic collation.  Of -0 and 0, equal, PostgreSQL keeps one, and
 * which one changes a sum's sign of zero: sum and avg of distinct floats
 * stay on PostgreSQL (count, min and max are not affected).
 */
static const char *
df_distinct_problem(DfCheckContext *cxt, Aggref *agg)
{
	Node	   *arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
	Oid			type = exprType(arg);
	char	   *name = get_func_name(agg->aggfnoid);

	if (df_type_is_string(type))
		return df_string_compare_problem(cxt, "=", agg->inputcollid);
	if ((type == FLOAT4OID || type == FLOAT8OID) &&
		(strcmp(name, "sum") == 0 || strcmp(name, "avg") == 0))
		return "of floats";
	return NULL;
}

/* Add the Aggrefs of 'node' to 'aggs'. */
bool
df_collect_aggrefs(Node *node, List **aggs)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		*aggs = lappend(*aggs, node);
		return false;
	}
	return expression_tree_walker(node, df_collect_aggrefs, aggs);
}

/*
 * The kept side's hash key of semi join 'hj' that inner column 'var' equals
 * exactly (a hash clause outer = var of a type whose equal values are the
 * same: integers, dates and times, bool), or NULL.
 */
Node *
df_semi_key_for(HashJoin *hj, Var *var)
{
	ListCell   *lc;

	foreach(lc, hj->hashclauses)
	{
		OpExpr	   *op = lfirst(lc);
		Node	   *outer = linitial(op->args);
		Node	   *inner = lsecond(op->args);
		Oid			type = exprType(inner);

		if (equal(inner, var) && exprType(outer) == type &&
			(type == INT2OID || type == INT4OID || type == INT8OID || type == BOOLOID ||
			 df_type_is_datetime(type)))
			return outer;
	}
	return NULL;
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
		else if (df_rescales_infinity(cxt->node, l, r))
			df_reject(cxt, "%s rescaling a numeric that may be infinite", what);
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

		case T_RowIdExpr:
			/*
			 * RI1: a number unique to each row of the node whose targetlist
			 * it is in, which DataFusion adds as a column of that node's
			 * rows (df_core::rowid)
			 */
			if (!cxt->rowid_ok)
				df_reject(cxt, "RowIdExpr outside a scan's or join's output");
			return cxt->failed;

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
					Node	   *date_arg;
					const char *date_cmp;
					int32		date_value;

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
							 (op->opresulttype != BOOLOID || ltype != rtype) &&
							 !df_date_timestamp_cmp(op, &date_arg, &date_cmp, &date_value))
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
							if (df_numeric_may_be_infinite(cxt->node, linitial(op->args)) ||
								df_numeric_may_be_infinite(cxt->node, lsecond(op->args)))
								df_reject(cxt, "operator %s on a numeric that may be infinite", name);
							else if (!df_numeric_ps(cxt->node, node, &lp, &ls))
								df_reject(cxt, "operator %s on numeric: %s", name,
										  strcmp(name, "/") == 0 || strcmp(name, "%") == 0 ?
										  "its scale depends on the values" :
										  "of unknown precision or more than 76 digits");
						}
						else if (df_numeric_param_cmp(cxt->node, op, NULL, NULL))
							/* IP1: the parameter, rounded to the other side's scale */
							cxt->numeric_param_ok = IsA(linitial(op->args), Param) ?
								linitial(op->args) : lsecond(op->args);
						else if (!df_numeric_ps(cxt->node, linitial(op->args), &lp, &ls) ||
								 !df_numeric_ps(cxt->node, lsecond(op->args), &rp, &rs))
							df_reject(cxt, "operator %s on numeric of unknown precision", name);
						else if (t = Max(ls, rs), lp - ls + t > DF_NUMERIC_MAX_EXPR_PRECISION ||
								 rp - rs + t > DF_NUMERIC_MAX_EXPR_PRECISION)
							df_reject(cxt, "operator %s on numeric beyond %d digits at a common scale",
									  name, DF_NUMERIC_MAX_EXPR_PRECISION);
						else if (df_rescales_infinity(cxt->node, linitial(op->args), lsecond(op->args)))
							df_reject(cxt, "operator %s rescaling a numeric that may be infinite", name);
					}
					else if (df_type_is_string(ltype) || df_type_is_string(rtype))
					{
						const char *problem;

						/* character LIKE text: bpcharlike, on the padded value */
						bool		bplike = ltype == BPCHAROID && rtype == TEXTOID &&
							(strcmp(name, "~~") == 0 || strcmp(name, "!~~") == 0);

						if (op->opresulttype != BOOLOID || (ltype != rtype && !bplike))
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
				else if (agg->aggorder != NIL)
					df_reject(cxt, "ORDER BY inside aggregate %s", name);
				else if (agg->aggdistinct != NIL && agg->aggsplit != AGGSPLIT_FINAL_DESERIAL &&
						 df_distinct_problem(cxt, agg) != NULL)
					df_reject(cxt, "DISTINCT inside aggregate %s %s", name,
							  df_distinct_problem(cxt, agg));
				else if (agg->aggfilter != NULL)
					df_reject(cxt, "FILTER clause on aggregate %s", name);
				else if (agg->aggsplit != AGGSPLIT_SIMPLE &&
						 agg->aggsplit != AGGSPLIT_INITIAL_SERIAL &&
						 agg->aggsplit != AGGSPLIT_FINAL_DESERIAL)
					df_reject(cxt, "combining stage of aggregate %s", name);
				else if (cxt->inner_agg &&
						 (df_agg_state(agg) == DF_AGG_AVG_INT ||
						  df_agg_state(agg) == DF_AGG_AVG_NUMERIC ||
						  df_agg_state_at(cxt->node, agg) == DF_AGG_SUM_NUMERIC_MIXED))
					/* A1: finished where tuples are made, which those above cannot read */
					df_reject(cxt, "aggregate %s returning numeric below a join or another aggregate",
							  name);
				else if ((df_agg_state(agg) == DF_AGG_SUM_NUMERIC ||
						  df_agg_state(agg) == DF_AGG_AVG_NUMERIC) &&
						 agg->aggsplit != AGGSPLIT_FINAL_DESERIAL &&
						 !((df_numeric_ps(cxt->node,
										  (Node *) linitial_node(TargetEntry, agg->args)->expr,
										  &np, &ns) ||
							(df_agg_state_at(cxt->node, agg) == DF_AGG_SUM_NUMERIC_MIXED &&
							 df_numeric_mixed(cxt->node,
											  (Node *) linitial_node(TargetEntry, agg->args)->expr,
											  &np, &ns))) &&
						   np <= DF_NUMERIC_MAX_SUM_PRECISION))
					/* the sum must stay within Decimal128's 38 digits */
					df_reject(cxt, "aggregate %s of numeric of unknown precision or more than %d digits",
							  name, DF_NUMERIC_MAX_SUM_PRECISION);
				else if ((df_agg_state(agg) == DF_AGG_SUM_NUMERIC ||
						  df_agg_state(agg) == DF_AGG_AVG_NUMERIC) &&
						 df_numeric_may_be_infinite(cxt->node,
													(Node *) linitial_node(TargetEntry, agg->args)->expr))
					/* DT2: the sum would take an infinity for a number */
					df_reject(cxt, "aggregate %s of a numeric that may be infinite", name);
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
				else if (agg->aggtype == BPCHAROID)
					/* bpchar_larger compares without trailing blanks, returns the padded value */
					df_reject(cxt, "aggregate %s of character", name);
				else if (df_type_is_string(agg->aggtype) &&
						 df_string_compare_problem(cxt, name, agg->inputcollid) != NULL)
					df_reject(cxt, "aggregate %s of %s %s", name, format_type_be(agg->aggtype),
							  df_string_compare_problem(cxt, name, agg->inputcollid));
				if (cxt->failed)
					return true;
				/* MS1: the branches of a sum's argument may differ in scale */
				if (df_agg_state_at(cxt->node, agg) == DF_AGG_SUM_NUMERIC_MIXED)
					cxt->mixed_scales_ok = (Node *) linitial_node(TargetEntry, agg->args)->expr;
				/* the arguments; aggdistinct holds sort clauses, checked above */
				return df_check_expr((Node *) agg->args, cxt);
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

				foreach(lc, fe->args)
				{
					if (df_numeric_may_be_infinite(cxt->node, lfirst(lc)))
					{
						df_reject(cxt, "function %s() of a numeric that may be infinite",
								  name ? name : "?");
						return true;
					}
				}
				if (df_extract_field(fe) != NULL)
				{
					/* DT2: the date (the field is a constant) */
					df_check_expr(lsecond(fe->args), cxt);
					return cxt->failed;
				}
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
				if (df_bpchar_func(fe->funcid) != NULL)
					break;		/* B1: character to text, lengths */
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
			{
				/*
				 * IP1: the value of an init plan, known before the slice runs
				 * (computed on the coordinator and dispatched), as a constant
				 */
				Param	   *param = (Param *) node;

				if (param->paramkind != PARAM_EXEC ||
					!bms_is_member(param->paramid, df_init_params(cxt)))
					df_reject(cxt, "query parameter");
				else if (!df_type_supported(param->paramtype))
					df_reject(cxt, "query parameter of type %s", format_type_be(param->paramtype));
				else if (param->paramtype == NUMERICOID && node != cxt->numeric_param_ok)
					df_reject(cxt, "numeric query parameter other than compared with a numeric of known scale");
				return cxt->failed;
			}

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
				if (ce->casetype == NUMERICOID && node != cxt->mixed_scales_ok &&
					!df_numeric_same_scale(cxt->node, results, -1))
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
				if (co->coalescetype == NUMERICOID && node != cxt->mixed_scales_ok &&
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

/* Columns of scan relation 'relid' that an expression reads. */
typedef struct DfScanRefs
{
	Index		relid;
	Bitmapset  *refs;
} DfScanRefs;

static bool
df_collect_scan_refs(Node *node, DfScanRefs *c)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var) && ((Var *) node)->varno == c->relid)
	{
		c->refs = bms_add_member(c->refs, ((Var *) node)->varattno);
		return false;
	}
	return expression_tree_walker(node, df_collect_scan_refs, c);
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
	if (IsA(plan, Result))
	{
		/* R1: its child's columns */
		Bitmapset  *sub = NULL;

		foreach(lc, plan->targetlist)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);

			if (!bms_is_member(tle->resno, attnos))
				continue;
			if (!IsA(tle->expr, Var) || ((Var *) tle->expr)->varno != OUTER_VAR)
				return false;
			sub = bms_add_member(sub, ((Var *) tle->expr)->varattno);
		}
		return df_plain_outputs(outerPlan(plan), sub);
	}
	if (IsA(plan, SubqueryScan))
	{
		/* SQ1: its plan's columns */
		Bitmapset  *sub = NULL;

		foreach(lc, plan->targetlist)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);

			if (!bms_is_member(tle->resno, attnos))
				continue;
			if (!IsA(tle->expr, Var) ||
				((Var *) tle->expr)->varno != ((Scan *) plan)->scanrelid)
				return false;
			sub = bms_add_member(sub, ((Var *) tle->expr)->varattno);
		}
		return df_plain_outputs(((SubqueryScan *) plan)->subplan, sub);
	}
	if (IsA(plan, Agg))
	{
		/* A1: its groups and calls are columns of its node, below the join */
		foreach(lc, plan->targetlist)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);

			if (bms_is_member(tle->resno, attnos) && !IsA(tle->expr, Aggref) &&
				!(IsA(tle->expr, Var) && ((Var *) tle->expr)->varno == OUTER_VAR))
				return false;
		}
		return true;
	}
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

/* Column of Agg 'agg' that outputs its child's column 'attno', or 0. */
static AttrNumber
df_agg_output_of(Agg *agg, AttrNumber attno)
{
	ListCell   *lc;

	foreach(lc, agg->plan.targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (IsA(tle->expr, Var) && ((Var *) tle->expr)->varno == OUTER_VAR &&
			((Var *) tle->expr)->varattno == attno)
			return tle->resno;
	}
	return 0;
}

/*
 * D4: below sorted Motion 'motion', a Limit over a GroupAggregate over a
 * Sort, as the planner pushes a LIMIT down (TPC-H Q18): the Limit keeps
 * the first rows in the GroupAggregate's order, the Sort's.  May the slice
 * run it hashed, sort its output by the Sort's keys and then limit it?
 * That order is the GroupAggregate's when the Sort's keys are grouping
 * columns covering all of them (one row per group, so no ties), each an
 * output column; the Motion merges by a prefix of them.  Records it.
 */
static bool
df_check_resort_limit(DfCheckContext *cxt, Motion *motion, Limit *limit)
{
	Agg		   *agg = (Agg *) outerPlan(limit);
	Sort	   *sort;
	Bitmapset  *groups = NULL;
	Bitmapset  *keys = NULL;
	int			i,
				k;

	if (agg == NULL || !IsA(agg, Agg) || agg->aggstrategy != AGG_SORTED ||
		outerPlan(agg) == NULL || !IsA(outerPlan(agg), Sort) ||
		!df_passes_through((Plan *) limit) || motion->numSortCols == 0)
		return false;
	sort = (Sort *) outerPlan(agg);
	if (sort->numCols < motion->numSortCols)
		return false;
	for (i = 0; i < sort->numCols; i++)
	{
		AttrNumber	col = df_agg_output_of(agg, sort->sortColIdx[i]);
		TargetEntry *tle = col > 0 ? get_tle_by_resno(agg->plan.targetlist, col) : NULL;
		Oid			type;
		bool		desc;

		for (k = 0; k < agg->numCols && agg->grpColIdx[k] != sort->sortColIdx[i]; k++)
			;
		if (tle == NULL || k == agg->numCols)
			return false;
		groups = bms_add_member(groups, k);
		type = exprType((Node *) tle->expr);
		if (!df_type_supported(type) || !df_sort_direction(sort->sortOperators[i], type, &desc))
			return false;
		if (df_type_is_string(type) &&
			df_string_compare_problem(cxt, "<", sort->collations[i]) != NULL)
		{
			df_reject(cxt, "sort key of type %s %s", format_type_be(type),
					  df_string_compare_problem(cxt, "<", sort->collations[i]));
			return false;
		}
		if (i < motion->numSortCols)
		{
			/* the Motion's key i is the same column, the same way */
			TargetEntry *mtle = get_tle_by_resno(motion->plan.targetlist, motion->sortColIdx[i]);

			if (mtle == NULL || !IsA(mtle->expr, Var) ||
				((Var *) mtle->expr)->varno != OUTER_VAR ||
				((Var *) mtle->expr)->varattno != col ||
				motion->sortOperators[i] != sort->sortOperators[i] ||
				motion->nullsFirst[i] != sort->nullsFirst[i])
				return false;
		}
		keys = bms_add_member(keys, col);
	}
	if (bms_num_members(groups) != agg->numCols)
		return false;
	cxt->sort_keys = bms_union(cxt->sort_keys, keys);
	cxt->tails.resort = motion;
	cxt->tails.resort_by = sort;
	return true;
}

/*
 * D3: may the slice below sorted Motion 'motion' run its GroupAggregate
 * hashed and sort the result by the Motion's keys?  The keys must be the
 * Agg's grouping columns, in its default order.  Records it.
 */
static bool
df_check_resort(DfCheckContext *cxt, Motion *motion)
{
	Agg		   *agg = (Agg *) outerPlan(motion);
	int			i,
				k;

	if (agg != NULL && IsA(agg, Limit))
		return df_check_resort_limit(cxt, motion, (Limit *) agg);
	if (agg == NULL || !IsA(agg, Agg) || agg->aggstrategy != AGG_SORTED ||
		outerPlan(agg) == NULL || !IsA(outerPlan(agg), Sort) || motion->numSortCols == 0)
		return false;
	for (i = 0; i < motion->numSortCols; i++)
	{
		TargetEntry *mtle = get_tle_by_resno(motion->plan.targetlist, motion->sortColIdx[i]);
		Var		   *var = mtle ? (Var *) mtle->expr : NULL;
		TargetEntry *atle;
		Var		   *grp;
		Oid			type;
		bool		desc;

		if (var == NULL || !IsA(var, Var) || var->varno != OUTER_VAR)
			return false;
		atle = get_tle_by_resno(agg->plan.targetlist, var->varattno);
		grp = atle ? (Var *) atle->expr : NULL;
		if (grp == NULL || !IsA(grp, Var) || grp->varno != OUTER_VAR)
			return false;
		for (k = 0; k < agg->numCols && agg->grpColIdx[k] != grp->varattno; k++)
			;
		if (k == agg->numCols)
			return false;
		type = exprType((Node *) grp);
		if (!df_type_supported(type) || !df_sort_direction(motion->sortOperators[i], type, &desc))
			return false;
		if (df_type_is_string(type) &&
			df_string_compare_problem(cxt, "<", motion->collations[i]) != NULL)
		{
			df_reject(cxt, "sort key of type %s %s", format_type_be(type),
					  df_string_compare_problem(cxt, "<", motion->collations[i]));
			return false;
		}
		cxt->sort_keys = bms_add_member(cxt->sort_keys, var->varattno);
	}
	cxt->tails.resort = motion;
	return true;
}

static void
df_check_plan(Plan *plan, DfCheckContext *cxt, Bitmapset *needed,
			  bool root_is_sender)
{
	Plan	   *saved = cxt->node;

	cxt->node = plan;
	cxt->node_order_free = cxt->order_free;
	cxt->order_free = false;
	df_check_plan_node(plan, cxt, needed, root_is_sender);
	cxt->node = saved;
}

static void
df_check_plan_node(Plan *plan, DfCheckContext *cxt, Bitmapset *needed,
				   bool root_is_sender)
{
	if (plan == NULL || cxt->failed)
		return;

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
				cxt->rowid_ok = true;
				df_check_targetlist(plan->targetlist, needed, cxt);
				cxt->rowid_ok = false;
				df_check_expr_list(plan->qual, cxt);
				return;
			}

		case T_Agg:
			{
				Agg		   *agg = (Agg *) plan;
				Plan	   *child = outerPlan(plan);
				Bitmapset  *child_needed = NULL;
				bool		inner;
				int			i;

				if (agg->aggstrategy == AGG_SORTED && cxt->node_order_free &&
					child != NULL && IsA(child, Sort))
				{
					/*
					 * D2: a GroupAggregate whose order no one reads (below an
					 * unsorted Motion, a Sort, or another Agg) runs hashed,
					 * without the Sort below it.
					 */
					cxt->skipped_sort = child;
				}
				else if (agg->aggstrategy == AGG_SORTED && cxt->node_order_free &&
						 child != NULL && IsA(child, Motion) && ((Motion *) child)->sendSorted)
				{
					/*
					 * D5: one over a sorted Motion's merge, as a Finalize
					 * GroupAggregate (TPC-H Q5), runs hashed and reads the
					 * Motion unmerged.
					 */
				}
				else if (agg->aggstrategy != AGG_PLAIN && agg->aggstrategy != AGG_HASHED)
				{
					df_reject(cxt, "sorted or mixed aggregation");
					return;
				}
				/*
				 * A1: an Agg below a join or another Agg runs as a node of
				 * the plan, which those above read by name: not a partial
				 * stage, whose state only a batch Motion carries.
				 */
				inner = cxt->join_input || cxt->agg_input || cxt->projection_input;
				if (inner && agg->aggsplit == AGGSPLIT_INITIAL_SERIAL)
				{
					df_reject(cxt, "partial aggregate below a join or another aggregate");
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
					if (child == NULL || !IsA(df_below_sort(child), Motion))
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
				cxt->agg_to_batches = cxt->batch_sender && !inner;
				cxt->final_states = agg->aggsplit == AGGSPLIT_FINAL_DESERIAL && child != NULL &&
					IsA(df_below_sort(child), Motion) &&
					bms_is_member(((Motion *) df_below_sort(child))->motionID, cxt->batches);
				cxt->batch_sender = false;
				cxt->allow_aggref = true;
				cxt->inner_agg = inner;
				/* A1: an Agg below computes all its calls (df_emit_node) */
				df_check_targetlist(plan->targetlist, inner ? NULL : needed, cxt);
				df_check_expr_list(plan->qual, cxt);
				cxt->inner_agg = false;
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
				cxt->order_free = true; /* hashed: the order of its input is not read */
				df_check_plan(child, cxt, child_needed, false);
				cxt->agg_input = false;
				return;
			}

		case T_Result:
			{
				/*
				 * R1: a projection and filter over its child's rows, as GPORCA
				 * puts above aggregates and joins.  Not one without a child or
				 * with a one-time filter.  An Agg below it is not the slice's
				 * top one (A1).
				 */
				Bitmapset  *child_needed = NULL;
				bool		saved = cxt->projection_input;
				ListCell   *lc;

				if (outerPlan(plan) == NULL || innerPlan(plan) != NULL ||
					((Result *) plan)->resconstantqual != NULL)
				{
					df_reject(cxt, "Result without a child or with a one-time filter");
					return;
				}
				cxt->allow_aggref = false;
				df_check_targetlist(plan->targetlist, needed, cxt);
				df_check_expr_list(plan->qual, cxt);
				if (cxt->failed)
					return;
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);

					if (needed == NULL || bms_is_member(tle->resno, needed))
						df_collect_outer_refs((Node *) tle->expr, &child_needed);
				}
				df_collect_outer_refs((Node *) plan->qual, &child_needed);
				child_needed = bms_add_member(child_needed, 0);
				cxt->order_free = cxt->node_order_free; /* its child's order */
				cxt->projection_input = true;
				df_check_plan(outerPlan(plan), cxt, child_needed, false);
				cxt->projection_input = saved;
				return;
			}

		case T_SubqueryScan:
			{
				/*
				 * SQ1: a subquery's rows, as its plan makes them.  Its
				 * columns and filter read its plan's columns (Vars of its
				 * scanrelid).  An Agg below it is not the slice's top one,
				 * which those above read by name (A1).
				 */
				SubqueryScan *sq = (SubqueryScan *) plan;
				DfScanRefs	refs = {sq->scan.scanrelid, NULL};
				bool		saved = cxt->projection_input;
				ListCell   *lc;

				cxt->allow_aggref = false;
				df_check_targetlist(plan->targetlist, needed, cxt);
				df_check_expr_list(plan->qual, cxt);
				if (cxt->failed)
					return;
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);

					if (needed == NULL || bms_is_member(tle->resno, needed))
						df_collect_scan_refs((Node *) tle->expr, &refs);
				}
				df_collect_scan_refs((Node *) plan->qual, &refs);
				refs.refs = bms_add_member(refs.refs, 0);
				cxt->order_free = cxt->node_order_free; /* its plan's order */
				cxt->projection_input = true;
				df_check_plan(sq->subplan, cxt, refs.refs, false);
				cxt->projection_input = saved;
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
				cxt->rowid_ok = true;
				df_check_targetlist(plan->targetlist, needed, cxt);
				cxt->rowid_ok = false;
				df_check_expr_list(hj->hashclauses, cxt);
				df_check_expr_list(join->joinqual, cxt);
				df_check_expr_list(plan->qual, cxt);
				if (cxt->failed)
					return;

				/*
				 * A semi or anti join keeps one side's rows, and DataFusion's
				 * outputs that side alone.  PostgreSQL's semi join may still
				 * read the other side's columns (the planner picks any member
				 * of an equivalence class): one equal to the kept side's
				 * hash key by an exact equality reads that key instead.
				 */
				if (join->jointype == JOIN_SEMI || join->jointype == JOIN_ANTI ||
					join->jointype == JOIN_RIGHT_ANTI)
				{
					List	   *vars = NIL;
					int			dropped = join->jointype == JOIN_RIGHT_ANTI ? OUTER_VAR : INNER_VAR;

					foreach(lc, plan->targetlist)
					{
						TargetEntry *tle = lfirst_node(TargetEntry, lc);

						if (needed == NULL || bms_is_member(tle->resno, needed))
							vars = list_concat(vars, pull_var_clause((Node *) tle->expr, 0));
					}
					vars = list_concat(vars, pull_var_clause((Node *) plan->qual, 0));
					foreach(lc, vars)
					{
						Var		   *var = lfirst_node(Var, lc);

						if (var->varno == dropped &&
							(join->jointype != JOIN_SEMI || df_semi_key_for(hj, var) == NULL))
						{
							df_reject(cxt, "column of the side a semi or anti join drops");
							return;
						}
					}
				}

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
				/* D4: the GroupAggregate below, in the order the slice sorts by */
				cxt->order_free = cxt->node_order_free && cxt->tails.resort_by != NULL;
				df_check_plan(outerPlan(plan), cxt, needed, false);
				return;
			}

		case T_Sort:
			{
				Sort	   *sort = (Sort *) plan;
				Plan	   *child = outerPlan(plan);
				Bitmapset  *child_needed = needed ? bms_copy(needed) : NULL;
				int			i;

				if (plan == cxt->skipped_sort)
				{
					/* D2: below a GroupAggregate DataFusion runs hashed */
					cxt->skipped_sort = NULL;
					if (!df_passes_through(plan))
					{
						df_reject(cxt, "Sort that computes columns");
						return;
					}
					cxt->order_free = true;
					df_check_plan(child, cxt, needed, false);
					return;
				}
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
				cxt->order_free = true; /* sorted here */
				df_check_plan(child, cxt, child_needed, false);
				return;
			}

		case T_Motion:
			{
				Motion	   *motion = (Motion *) plan;
				bool		sorted;

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
					if (motion->sendSorted && !cxt->node_order_free)
					{
						/* D5: unmerged where no one reads the order */
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
				 * DML plans.  Batches through it are not merged (D5): their
				 * order does not matter.
				 */
				sorted = motion->sendSorted && !bms_is_member(motion->motionID, cxt->batches);
				if (sorted &&
					(outerPlan(plan) == NULL || !df_sorted_for(outerPlan(plan), motion)))
				{
					/*
					 * D3: a GroupAggregate below sorts by its groups, which
					 * are unique: the slice runs it hashed and sorts its
					 * output by the Motion's keys, the same order.
					 */
					if (!df_check_resort(cxt, motion))
					{
						if (!cxt->failed)
							df_reject(cxt, "sorted %s without a Sort below it", df_plan_name(plan));
						return;
					}
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
				cxt->order_free = !sorted || cxt->tails.resort != NULL;
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
 * T2: subtrees DataFusion runs below PostgreSQL's nodes
 * ---------------------------------------------------------------------
 */

/*
 * Can DataFusion take over subtree 'plan', and is it worth starting for
 * it?  Not at a Hash node, which its Hash Join runs through
 * MultiExecProcNode rather than ExecProcNode (its child can be taken
 * over); not for rows it would only pass on: a Motion's, or a table's
 * without a filter.
 */
static bool
df_attach_worth(Plan *plan)
{
	if (IsA(plan, Motion) || IsA(plan, Hash))
		return false;
	if (IsA(plan, SeqScan) && plan->qual == NIL)
		return false;
	return true;
}

/*
 * 'locale_free': skip subtrees whose verdict depends on the node's locale
 * and look below them instead, for subtrees every node runs alike.
 */
static void
df_find_attach(PlannedStmt *stmt, Plan *plan, Bitmapset *batches, bool locale_free,
			   List **points)
{
	char		reason[256];
	DfTails		tails;
	bool		locale_dependent = false;
	ListCell   *lc;

	if (plan == NULL || IsA(plan, Motion))
		return;
	if (df_attach_worth(plan) &&
		df_check_slice_b(stmt, plan, false, batches, &tails, reason, sizeof(reason),
						 &locale_dependent) &&
		!(locale_free && locale_dependent))
	{
		DfAttach   *a = palloc0(sizeof(DfAttach));

		a->plan = plan;
		a->tails = tails;
		a->locale_dependent = locale_dependent;
		*points = lappend(*points, a);
		return;
	}

	/*
	 * Below the PostgreSQL node, in the children it runs once: DataFusion's
	 * stream cannot be rescanned or marked, so not a Nested Loop's inner
	 * side, a Merge Join's, or anything else that may run again.
	 */
	switch (nodeTag(plan))
	{
		case T_Result:
		case T_Sort:
		case T_IncrementalSort:
		case T_Limit:
		case T_Unique:
		case T_Material:
		case T_ProjectSet:
		case T_WindowAgg:
		case T_Agg:
		case T_Hash:
		case T_NestLoop:
		case T_MergeJoin:
			df_find_attach(stmt, outerPlan(plan), batches, locale_free, points);
			return;
		case T_HashJoin:
			df_find_attach(stmt, outerPlan(plan), batches, locale_free, points);
			df_find_attach(stmt, innerPlan(plan), batches, locale_free, points);
			return;
		case T_SubqueryScan:
			df_find_attach(stmt, ((SubqueryScan *) plan)->subplan, batches, locale_free, points);
			return;
		case T_Append:
			foreach(lc, ((Append *) plan)->appendplans)
				df_find_attach(stmt, lfirst(lc), batches, locale_free, points);
			return;
		default:
			return;
	}
}

/*
 * The subtrees of the slice computed by 'compute' (below its sending
 * Motion, if any) that DataFusion runs when the slice as a whole cannot:
 * the highest ones that can, below the PostgreSQL nodes above them.  NIL
 * if none.  Their rows go to PostgreSQL's nodes as tuples, so a slice run
 * this way sends no batches.
 */
List *
df_slice_attach_points(PlannedStmt *stmt, Plan *compute, Bitmapset *batches)
{
	List	   *points = NIL;

	df_find_attach(stmt, compute, batches, false, &points);
	return points;
}

/* Is 'target' a node of 'plan' within its slice? */
static bool
df_plan_contains(Plan *plan, Plan *target)
{
	if (plan == NULL)
		return false;
	if (plan == target)
		return true;
	if (IsA(plan, Motion))
		return false;
	if (IsA(plan, SubqueryScan) && df_plan_contains(((SubqueryScan *) plan)->subplan, target))
		return true;
	if (IsA(plan, Append))
	{
		ListCell   *lc;

		foreach(lc, ((Append *) plan)->appendplans)
			if (df_plan_contains(lfirst(lc), target))
				return true;
	}
	return df_plan_contains(outerPlan(plan), target) ||
		df_plan_contains(innerPlan(plan), target);
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
 * Would slice 'index' read the batches of receiving Motion 'motion' in
 * DataFusion?  As a whole, or in a subtree DataFusion runs below
 * PostgreSQL's nodes (T2) holding the Motion.  That subtree must not
 * depend on the node's locale, as every node must agree; a node may still
 * run more of the slice in DataFusion (such as a Sort above it by the
 * default collation, where that is C), which reads the Motion too.
 */
static bool
df_slice_reads_batches(PlannedStmt *stmt, int index, Motion *motion, Bitmapset *batches)
{
	Motion	   *sender;
	Plan	   *compute;
	List	   *points = NIL;
	ListCell   *lc;

	if (df_slice_runs_in_datafusion(stmt, index, batches))
		return true;
	if (index < 0 || index >= stmt->numSlices)
		return false;
	sender = findSenderMotion(stmt, index);
	compute = sender ? outerPlan((Plan *) sender) : stmt->planTree;
	df_find_attach(stmt, compute, batches, true, &points);
	foreach(lc, points)
	{
		DfAttach   *a = lfirst(lc);
		DfSliceSpec spec;
		char		reason[256];

		if (df_plan_contains(a->plan, (Plan *) motion))
			return df_translate_slice(a->plan, &a->tails, &spec, reason, sizeof(reason));
	}
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
		/* the bytes without trailing blanks */
		{BPCHAROID, "hashbpchar"},
		/* NBASE digits in memory order; of a known scale (below) */
		{NUMERICOID, "hash_numeric"},
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
			if (var->vartype == NUMERICOID)
			{
				/* the column's scale places the digits (df_core::cdbhash) */
				Plan	   *child = outerPlan(motion);
				TargetEntry *tle = child ? get_tle_by_resno(child->targetlist, var->varattno) : NULL;
				int			p,
							sc;

				if (tle == NULL || !df_numeric_ps(child, (Node *) tle->expr, &p, &sc))
					return false;
				if (sc != 0)
					*tag = psprintf("numeric:%d", sc);
			}
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
	/* D5: a sorted Gather's receiver reading batches merges nothing */
	if (motion->sendSorted && motion->motionType != MOTIONTYPE_GATHER &&
		motion->motionType != MOTIONTYPE_GATHER_SINGLE)
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

/*
 * The Motions of the plan tree 'plan', also below a Subquery Scan and an
 * Append.  Those of init plans are not listed: their top slice has no
 * sending Motion to find it by (df_slice_runs_in_datafusion).
 */
static void
df_list_motions(Plan *plan, List **motions)
{
	ListCell   *lc;

	if (plan == NULL)
		return;
	if (IsA(plan, Motion))
		*motions = lappend(*motions, plan);
	df_list_motions(outerPlan(plan), motions);
	df_list_motions(innerPlan(plan), motions);
	if (IsA(plan, SubqueryScan))
		df_list_motions(((SubqueryScan *) plan)->subplan, motions);
	if (IsA(plan, Append))
		foreach(lc, ((Append *) plan)->appendplans)
			df_list_motions(lfirst(lc), motions);
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
				  df_slice_reads_batches(stmt, stmt->slices[m->motionID].parentIndex,
										 m, batches)))
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
	return df_agg_state_ncols(df_agg_state_at(child, (Aggref *) expr));
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
		case DF_AGG_SUM_NUMERIC_MIXED:
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
		{
			Plan	   *compute = cxt.roots[i].is_sender ?
				outerPlan(cxt.roots[i].root) : cxt.roots[i].root;
			List	   *points = df_slice_attach_points(stmt, compute, df_batch_motions(stmt));
			ListCell   *lc;

			if (points == NIL)
				appendStringInfo(out, "DataFusion: slice %d not eligible: %s\n",
								 cxt.roots[i].index, reason);
			else
			{
				/* T2: what runs below PostgreSQL's nodes, and why they stay */
				appendStringInfo(out, "DataFusion: slice %d eligible below the top:",
								 cxt.roots[i].index);
				foreach(lc, points)
				{
					Plan	   *p = ((DfAttach *) lfirst(lc))->plan;

					appendStringInfo(out, "%s %s", foreach_current_index(lc) > 0 ? "," : "",
									 df_plan_name(p) ? df_plan_name(p) : "?");
				}
				appendStringInfo(out, " (above: %s)\n", reason);
			}
		}
	}
	pfree(cxt.roots);
}
