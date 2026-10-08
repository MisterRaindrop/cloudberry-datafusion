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
 * df_translate.c
 *	  Describe an eligible slice to the Rust side as a JSON plan.
 *
 * The slice is a Seq Scan, optionally under one aggregate, or an aggregate
 * over a receiving Motion.  The scan contributes the columns DataFusion
 * needs and the filter; the aggregate its grouping keys, aggregate calls
 * and HAVING; the slice's top node the output columns, in targetlist order,
 * each with its PostgreSQL type.  The JSON format is documented in
 * rust/df_core/src/query.rs.
 *
 * Over a Motion, the input columns are the Motion's output columns, by
 * position: what the sending slice computed is already in them.  A
 * combining (Finalize) aggregate's single argument refers to such a column,
 * holding the partial aggregates' transition states; count's are added up
 * ("count_merge", 0 without rows), sum's added, min's and max's compared.
 *
 * Through a batch Motion a split avg passes DataFusion's state instead of
 * PostgreSQL's array (M7d): the partial stage outputs sum(x) and count(x),
 * two columns of the stream, and the combining stage divides their sums
 * ("avg_merge").  Input columns from a Motion are therefore numbered by
 * their position in the stream (df_motion_stream_column).
 *
 * A Sort and a Limit at the top of the slice (S1) pass the rows of the
 * rest through: they become the spec's "sort", by output column, and
 * "limit".
 *
 * df_check_slice has already rejected anything this file cannot express,
 * so the translator reports a failure only as a safety net, and the slice
 * then stays on the PostgreSQL executor.
 *
 * src/df_translate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/parallel.h"
#include "catalog/pg_type_d.h"
#include "cdb/cdbvars.h"
#include "executor/nodeSubplan.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

#include "df_executor.h"

typedef enum DfLevel
{
	DF_LEVEL_SCAN,				/* expressions of node b->ctx, over its inputs */
	DF_LEVEL_AGG				/* expressions over the aggregate's output */
} DfLevel;

/* An input being described: its leaf and the columns read so far. */
typedef struct DfInputDesc
{
	Plan	   *leaf;			/* Seq Scan or receiving Motion */
	List	   *attnos;			/* int: table column, or Motion stream
								 * position + 1, of each input column */
	List	   *types;			/* oid: type of each */
	List	   *scales;			/* int: scale of each numeric one */
} DfInputDesc;

/* A1: an Agg below a join or another Agg, a named node of the plan. */
typedef struct DfNamedAgg
{
	Agg		   *agg;
	List	   *aggrefs;		/* its distinct aggregate calls */
	List	   *aggfns;			/* the function of each (df_emit_agg_calls) */
} DfNamedAgg;

typedef struct DfBuilder
{
	Plan	   *ctx;			/* the node whose expressions are emitted */
	Agg		   *agg;			/* the Agg of DF_LEVEL_AGG */
	bool		agg_named;		/* read from above: its columns by name */
	List	   *named;			/* DfNamedAgg, of the Aggs below the top */
	List	   *inputs;			/* DfInputDesc, in input order */
	List	   *aggrefs;		/* distinct aggregate calls, in first-use order */
	List	   *aggfns;			/* the function of each: NULL for the
								 * Aggref's own, or "sum" / "count" for the
								 * parts of a partial avg's state */
	Node	   *case_arg;		/* the value of the CASE being emitted (E1) */
	bool		join_sides;		/* emitting a join's keys or filter, which
								 * read both sides as they are */
	int		   *out_col;		/* output column of each value
								 * (df_emit_outputs) */
	bool		failed;
	char	   *reason;
	size_t		reasonlen;
} DfBuilder;

static void df_emit(DfBuilder *b, StringInfo out, Node *node, DfLevel level);
static void df_emit_numeric_const(DfBuilder *b, StringInfo out, Const *c, int scale);
static void df_emit_output_of(DfBuilder *b, StringInfo out, Plan *child, AttrNumber resno);
static Plan *df_level_ctx(DfBuilder *b, DfLevel level);
static void df_emit_node(DfBuilder *b, StringInfo out, Plan *plan);
static void df_emit_agg_calls(DfBuilder *b, StringInfo aggs, Agg *aggnode, List *aggrefs,
							  List *aggfns);
static DfNamedAgg *df_named_of(DfBuilder *b, Agg *agg);
static void df_emit_named_call(DfBuilder *b, StringInfo out, Aggref *call);
static void df_emit_ungrouped(DfBuilder *b, StringInfo out, Var *var);
static int	df_agg_ref(DfBuilder *b, Aggref *agg, const char *fn);
static bool df_numbers_rows(Plan *plan);

static void
df_fail(DfBuilder *b, const char *what)
{
	if (!b->failed)
		snprintf(b->reason, b->reasonlen, "cannot translate %s", what);
	b->failed = true;
}

const char *
df_type_tag(Oid type)
{
	switch (type)
	{
		case BOOLOID:
			return "bool";
		case INT2OID:
			return "int2";
		case INT4OID:
			return "int4";
		case INT8OID:
			return "int8";
		case FLOAT4OID:
			return "float4";
		case FLOAT8OID:
			return "float8";
		case DATEOID:
			return "date";
		case TIMEOID:
			return "time";
		case TIMESTAMPOID:
			return "timestamp";
		case TIMESTAMPTZOID:
			return "timestamptz";
		case TIDOID:
			return "tid";
		case TEXTOID:
		case VARCHAROID:
			return "text";
		case BPCHAROID:
			return "bpchar";
		case NUMERICOID:
			/* of scale 0; see df_tag for others */
			return "numeric";
		default:
			return NULL;
	}
}

/* The type tag of 'type', for numeric of scale 'scale'. */
static const char *
df_tag(Oid type, int scale)
{
	if (type == NUMERICOID && scale != 0)
		return psprintf("numeric:%d", scale);
	return df_type_tag(type);
}

/*
 * The scale of numeric expression 'expr' of node 'ctx' (0 if none), also
 * of a reference to a partial sum or avg below, whose type is PostgreSQL's
 * state (bytea) but whose stream column is DataFusion's numeric sum.
 */
static int
df_scale_of(Plan *ctx, Node *expr)
{
	int			p,
				s;

	if (!df_numeric_ps(ctx, expr, &p, &s) && !df_numeric_mixed(ctx, expr, &p, &s) &&
		!df_mixed_sum_scale(ctx, expr, &s))
		return 0;
	return s;
}

static void
df_emit_float(StringInfo out, double v, bool single)
{
	if (isnan(v))
		appendStringInfoString(out, "\"NaN\"");
	else if (isinf(v))
		appendStringInfoString(out, v > 0 ? "\"Infinity\"" : "\"-Infinity\"");
	else
		appendStringInfo(out, single ? "%.9g" : "%.17g", v);
}

/* 'len' bytes of UTF-8 as a JSON string. */
static void
df_emit_json_string(StringInfo out, const char *s, int len)
{
	int			i;

	appendStringInfoChar(out, '"');
	for (i = 0; i < len; i++)
	{
		unsigned char ch = (unsigned char) s[i];

		if (ch == '"' || ch == '\\')
		{
			appendStringInfoChar(out, '\\');
			appendStringInfoChar(out, ch);
		}
		else if (ch < 0x20)
			appendStringInfo(out, "\\u%04x", ch);
		else
			appendStringInfoChar(out, ch);
	}
	appendStringInfoChar(out, '"');
}

static void
df_emit_const(DfBuilder *b, StringInfo out, Const *c)
{
	const char *tag = df_type_tag(c->consttype);
	int			p,
				s;

	if (c->consttype == NUMERICOID)
	{
		if (c->constisnull)
			s = 0;
		else if (!df_numeric_const_ps(c->constvalue, &p, &s))
		{
			df_fail(b, "a numeric constant");
			return;
		}
		df_emit_numeric_const(b, out, c, s);
		return;
	}
	if (tag == NULL)
	{
		df_fail(b, "a constant");
		return;
	}
	appendStringInfo(out, "{\"lit\":{\"type\":\"%s\"", tag);
	if (c->constisnull)
		appendStringInfoString(out, ",\"null\":true}}");
	else
	{
		appendStringInfoString(out, ",\"value\":");
		switch (c->consttype)
		{
			case BOOLOID:
				appendStringInfoString(out, DatumGetBool(c->constvalue) ? "true" : "false");
				break;
			case INT2OID:
				appendStringInfo(out, "%d", (int) DatumGetInt16(c->constvalue));
				break;
			case INT4OID:
			case DATEOID:
				appendStringInfo(out, "%d", DatumGetInt32(c->constvalue));
				break;
			case INT8OID:
			case TIMEOID:
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
				appendStringInfo(out, INT64_FORMAT, DatumGetInt64(c->constvalue));
				break;
			case FLOAT4OID:
				df_emit_float(out, DatumGetFloat4(c->constvalue), true);
				break;
			case FLOAT8OID:
				df_emit_float(out, DatumGetFloat8(c->constvalue), false);
				break;
			case TEXTOID:
			case VARCHAROID:
			case BPCHAROID:
				{
					text	   *t = DatumGetTextPP(c->constvalue);

					df_emit_json_string(out, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
				}
				break;
		}
		appendStringInfoString(out, "}}");
	}
}

/* numeric constant 'c' as a literal of scale 'scale' (it fits: checked). */
static void
df_emit_numeric_const(DfBuilder *b, StringInfo out, Const *c, int scale)
{
	int128		v;
	char		digits[48];
	int			n = 0;
	bool		neg;

	appendStringInfo(out, "{\"lit\":{\"type\":\"%s\"", df_tag(NUMERICOID, scale));
	if (c->constisnull)
	{
		appendStringInfoString(out, ",\"null\":true}}");
		return;
	}
	if (df_numeric_value(c->constvalue, scale, &v) != DF_NUMERIC_FITS)
	{
		df_fail(b, "a numeric constant");
		return;
	}
	if (v == DF_NUMERIC_NAN)
	{
		/* NaN of the Decimal256 representation is the Rust side's to make */
		appendStringInfoString(out, ",\"value\":\"NaN\"}}");
		return;
	}
	/* the integer, as a JSON string (beyond what JSON numbers carry) */
	neg = v < 0;
	do
	{
		int			d = (int) (v % 10);

		digits[n++] = (char) ('0' + (d < 0 ? -d : d));
		v /= 10;
	} while (v != 0);
	appendStringInfoString(out, ",\"value\":\"");
	if (neg)
		appendStringInfoChar(out, '-');
	while (n > 0)
		appendStringInfoChar(out, digits[--n]);
	appendStringInfoString(out, "\"}}");
}

/* IP1: init plan values for the slice being translated to start it */
ParamExecData *df_param_values = NULL;
ExprContext *df_param_econtext = NULL;

/*
 * The value of init plan parameter 'param': dispatched, computed before
 * dispatching, or computed now on the coordinator, as PostgreSQL would when
 * the parameter is first read.  NULL when translating only to check (EXPLAIN,
 * batch Motions), where it does not matter.
 */
static Const *
df_param_const(Param *param)
{
	int16		len;
	bool		byval;
	Datum		value = (Datum) 0;
	bool		isnull = true;

	get_typlenbyval(param->paramtype, &len, &byval);
	if (df_param_values != NULL)
	{
		ParamExecData *prm = &df_param_values[param->paramid];

		if (prm->execPlan != NULL)
			ExecSetParamPlan(prm->execPlan, df_param_econtext, NULL);
		value = prm->value;
		isnull = prm->isnull;
	}
	return makeConst(param->paramtype, param->paramtypmod, param->paramcollid, len,
					 value, isnull, byval);
}

static Datum
df_numeric_from(const char *s)
{
	return DirectFunctionCall3(numeric_in, CStringGetDatum(s), ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(-1));
}

static int
df_numeric_cmp(Datum a, Datum b)
{
	return DatumGetInt32(DirectFunctionCall2(numeric_cmp, a, b));
}

/*
 * IP1: numeric 'x' (of known scale s and at most p digits) compared by 'op'
 * with an init plan parameter, as x compared with a literal of scale s:
 * the value rounded up for < and >=, down for <= and >; = and <> with a
 * value finer than s are constant.  A value beyond any x becomes the
 * infinity on its side, NaN stays NaN, so NaN x compare as in PostgreSQL.
 */
static void
df_emit_numeric_param_cmp(DfBuilder *b, StringInfo out, OpExpr *op, DfLevel level)
{
	static const char *const swapped[][2] = {
		{"<", ">"}, {"<=", ">="}, {">", "<"}, {">=", "<="}, {"=", "="}, {"<>", "<>"}
	};
	Plan	   *ctx = df_level_ctx(b, level);
	Node	   *x;
	bool		param_left;
	const char *name = get_opname(op->opno);
	const char *special = NULL;
	Const	   *c;
	int			p,
				s,
				i;

	(void) df_numeric_param_cmp(ctx, op, &x, &param_left);
	(void) df_numeric_ps(ctx, x, &p, &s);
	for (i = 0; param_left && strcmp(swapped[i][0], name) != 0; i++)
		;
	if (param_left)
		name = swapped[i][1];
	c = df_param_const((Param *) (param_left ? linitial(op->args) : lsecond(op->args)));
	if (!c->constisnull)
	{
		Datum		v = c->constvalue;
		Numeric		n = DatumGetNumeric(v);

		if (numeric_is_nan(n))
			special = "NaN";
		else if (numeric_is_inf(n))
			special = df_numeric_cmp(v, df_numeric_from("0")) > 0 ? "Infinity" : "-Infinity";
		else
		{
			Datum		q = DirectFunctionCall2(numeric_trunc, v, Int32GetDatum(s));
			Datum		ulp = df_numeric_from(psprintf("1e-%d", s));
			Datum		bound = df_numeric_from(psprintf("1e%d", p - s));
			bool		exact = df_numeric_cmp(q, v) == 0;
			int			sign = df_numeric_cmp(v, df_numeric_from("0"));

			if (exact)
				v = q;
			else if (strcmp(name, "<") == 0 || strcmp(name, ">=") == 0)
				v = sign > 0 ? DirectFunctionCall2(numeric_add, q, ulp) : q;	/* up */
			else if (strcmp(name, "<=") == 0 || strcmp(name, ">") == 0)
				v = sign < 0 ? DirectFunctionCall2(numeric_sub, q, ulp) : q;	/* down */
			else
			{
				/* = never, <> always (NULL for a NULL x) */
				name = strcmp(name, "=") == 0 ? "<" : ">=";
				special = "-Infinity";
			}
			/* |x| < 10^(p - s): beyond, the infinity on that side answers alike */
			if (special == NULL && df_numeric_cmp(v, bound) >= 0)
				special = "Infinity";
			else if (special == NULL &&
					 df_numeric_cmp(v, DirectFunctionCall1(numeric_uminus, bound)) <= 0)
				special = "-Infinity";
			c = makeConst(NUMERICOID, -1, InvalidOid, -1, v, false, false);
		}
	}
	appendStringInfo(out, "{\"op\":\"%s\",\"type\":\"bool\",\"args\":[", name);
	df_emit(b, out, x, level);
	appendStringInfoChar(out, ',');
	if (special != NULL)
		appendStringInfo(out, "{\"lit\":{\"type\":\"%s\",\"value\":\"%s\"}}",
						 df_tag(NUMERICOID, s), special);
	else
		df_emit_numeric_const(b, out, c, s);
	appendStringInfoString(out, "]}");
}

static void df_emit(DfBuilder *b, StringInfo out, Node *node, DfLevel level);

/*
 * numeric expression 'e' of node 'ctx' at scale 'scale' (not below its
 * own): constants as literals of that scale, others multiplied by a power
 * of ten.
 */
static void
df_emit_numeric_at(DfBuilder *b, StringInfo out, Node *e, int scale, DfLevel level, Plan *ctx)
{
	int			own;

	if (IsA(e, Const))
	{
		df_emit_numeric_const(b, out, (Const *) e, scale);
		return;
	}
	own = df_scale_of(ctx, e);
	if (own == scale)
	{
		df_emit(b, out, e, level);
		return;
	}
	appendStringInfoString(out, "{\"rescale\":");
	df_emit(b, out, e, level);
	appendStringInfo(out, ",\"by\":%d,\"type\":\"%s\"}", scale - own, df_tag(NUMERICOID, scale));
}

/* The context of expressions at 'level'. */
static Plan *
df_level_ctx(DfBuilder *b, DfLevel level)
{
	return level == DF_LEVEL_AGG ? (Plan *) b->agg : b->ctx;
}

/* The scale numeric 'l' and 'r' are compared at: the larger one. */
static int
df_common_scale(Plan *ctx, Node *l, Node *r)
{
	return Max(df_scale_of(ctx, l), df_scale_of(ctx, r));
}

/*
 * An operand of a comparison: a float as PostgreSQL compares it, -0 as 0
 * and every NaN as one, above all numbers (df_core::pgfloat); a character
 * value without its trailing blanks (B1).
 */
static void
df_emit_compared(DfBuilder *b, StringInfo out, Node *e, DfLevel level)
{
	Oid			type = exprType(e);
	const char *key = type == FLOAT4OID || type == FLOAT8OID ? "floatkey" :
		type == BPCHAROID ? "bpkey" : NULL;

	if (key == NULL)
	{
		df_emit(b, out, e, level);
		return;
	}
	appendStringInfo(out, "{\"%s\":", key);
	df_emit(b, out, e, level);
	appendStringInfoChar(out, '}');
}

/*
 * A grouping key, DISTINCT argument: character ones marked, which group by
 * their value without trailing blanks (df_core::query::build_node).
 */
static void
df_emit_grouped(DfBuilder *b, StringInfo out, Plan *child, AttrNumber resno)
{
	TargetEntry *tle = get_tle_by_resno(child->targetlist, resno);
	bool		bp = tle != NULL && exprType((Node *) tle->expr) == BPCHAROID;

	if (bp)
		appendStringInfoString(out, "{\"bpchar\":");
	df_emit_output_of(b, out, child, resno);
	if (bp)
		appendStringInfoChar(out, '}');
}

/* "[l, r]", numeric operands at their common scale (E1). */
static void
df_emit_operands(DfBuilder *b, StringInfo out, Node *l, Node *r, DfLevel level)
{
	appendStringInfoChar(out, '[');
	if (exprType(l) == NUMERICOID)
	{
		Plan	   *ctx = df_level_ctx(b, level);
		int			t = df_common_scale(ctx, l, r);

		df_emit_numeric_at(b, out, l, t, level, ctx);
		appendStringInfoChar(out, ',');
		df_emit_numeric_at(b, out, r, t, level, ctx);
	}
	else
	{
		df_emit_compared(b, out, l, level);
		appendStringInfoChar(out, ',');
		df_emit_compared(b, out, r, level);
	}
	appendStringInfoChar(out, ']');
}

/* A result of a CASE or COALESCE of 'type' (numeric: at 'scale'). */
static void
df_emit_result(DfBuilder *b, StringInfo out, Node *e, Oid type, int scale, DfLevel level)
{
	if (type == NUMERICOID)
		df_emit_numeric_at(b, out, e, scale, level, df_level_ctx(b, level));
	else
		df_emit(b, out, e, level);
}

/* Add an input reading 'leaf'; returns its index. */
static int
df_add_input(DfBuilder *b, Plan *leaf)
{
	DfInputDesc *in = palloc0(sizeof(DfInputDesc));

	in->leaf = leaf;
	b->inputs = lappend(b->inputs, in);
	return list_length(b->inputs) - 1;
}

/* Column of input 'j' for 'attno', adding it if new; its index there. */
static int
df_input_column(DfBuilder *b, int j, AttrNumber attno, Oid type, int scale)
{
	DfInputDesc *in = list_nth(b->inputs, j);
	ListCell   *lc;
	int			i = 0;

	foreach(lc, in->attnos)
	{
		if (lfirst_int(lc) == attno)
			return i;
		i++;
	}
	in->attnos = lappend_int(in->attnos, attno);
	in->types = lappend_oid(in->types, type);
	in->scales = lappend_int(in->scales, scale);
	return i;
}

/* Append a reference to that column of input 'j'. */
static void
df_emit_column(DfBuilder *b, StringInfo out, int j, AttrNumber attno, Oid type, int scale)
{
	int			k = df_input_column(b, j, attno, type, scale);

	if (j == 0)
		appendStringInfo(out, "{\"col\":%d}", k);
	else
		appendStringInfo(out, "{\"col\":%d,\"input\":%d}", k, j);
}

/* The input reading 'leaf', or -1. */
static int
df_input_of(DfBuilder *b, Plan *leaf)
{
	ListCell   *lc;
	int			j = 0;

	foreach(lc, b->inputs)
	{
		if (((DfInputDesc *) lfirst(lc))->leaf == leaf)
			return j;
		j++;
	}
	return -1;
}

/* A1: the named node of Agg 'agg', or NULL. */
static DfNamedAgg *
df_named_of(DfBuilder *b, Agg *agg)
{
	ListCell   *lc;

	foreach(lc, b->named)
		if (((DfNamedAgg *) lfirst(lc))->agg == agg)
			return lfirst(lc);
	return NULL;
}

/*
 * A1: call 'call' of b->agg, an Agg below the top: by its name from above,
 * by its index in its own HAVING.
 */
static void
df_emit_named_call(DfBuilder *b, StringInfo out, Aggref *call)
{
	List	   *calls = df_named_of(b, b->agg)->aggrefs;
	int			k = list_length(calls);
	ListCell   *lc;

	foreach(lc, calls)
		if (equal(lfirst(lc), call))
			k = foreach_current_index(lc);
	if (k == list_length(calls))
		df_fail(b, "an aggregate call");
	else if (b->agg_named)
		appendStringInfo(out, "{\"name\":\"q%d_a%d\"}", b->agg->plan.plan_node_id, k);
	else
		appendStringInfo(out, "{\"agg\":%d}", k);
}

/*
 * RI1: column 'var' of an Agg's child, as the call any(var) of the Agg
 * (df_emit_agg_calls).
 */
static Aggref *
df_any_call(Var *var)
{
	Aggref	   *call = makeNode(Aggref);

	call->aggfnoid = InvalidOid;
	call->aggtype = var->vartype;
	call->aggsplit = AGGSPLIT_SIMPLE;
	call->aggargtypes = list_make1_oid(var->vartype);
	call->args = list_make1(makeTargetEntry((Expr *) copyObject(var), 1, NULL, false));
	call->location = -1;
	return call;
}

/*
 * RI1: a column of b->agg's child it does not group by.  The planner puts
 * one there only where the groups determine it: a column of a table
 * grouped by its key, or of a row whose copies a grouping by RowIdExpr
 * drops.  Any value of the group is it.
 */
static void
df_emit_ungrouped(DfBuilder *b, StringInfo out, Var *var)
{
	if (df_named_of(b, b->agg) != NULL)
		df_emit_named_call(b, out, df_any_call(var));
	else
		appendStringInfo(out, "{\"agg\":%d}", df_agg_ref(b, df_any_call(var), "any"));
}

/* RI1: the columns of an Agg's child it does not group by, outside calls. */
typedef struct DfUngrouped
{
	Agg		   *agg;
	List	   *vars;
} DfUngrouped;

static bool
df_collect_ungrouped(Node *node, DfUngrouped *c)
{
	if (node == NULL || IsA(node, Aggref))
		return false;
	if (IsA(node, Var) && ((Var *) node)->varno == OUTER_VAR)
	{
		int			k;

		for (k = 0; k < c->agg->numCols; k++)
			if (c->agg->grpColIdx[k] == ((Var *) node)->varattno)
				return false;
		c->vars = lappend(c->vars, node);
		return false;
	}
	return expression_tree_walker(node, df_collect_ungrouped, c);
}


/*
 * Output column 'resno' of plan node 'child', as an expression over the
 * inputs: the Motion's column itself, or the child's targetlist entry
 * emitted in the child's own terms, down to the scanned columns.
 */
static void
df_emit_output_of(DfBuilder *b, StringInfo out, Plan *child, AttrNumber resno)
{
	TargetEntry *tle = child ? get_tle_by_resno(child->targetlist, resno) : NULL;
	Plan	   *saved = b->ctx;

	if (tle == NULL)
		df_fail(b, "a reference to a child's output");
	else if (IsA(child, Agg))
	{
		/*
		 * A1, D2: an Agg below, its groups and calls by their names
		 * (df_emit_node)
		 */
		Agg		   *agg = b->agg;
		bool		named = b->agg_named;

		b->agg = (Agg *) child;
		b->agg_named = true;
		if (df_named_of(b, b->agg) == NULL)
			df_fail(b, "a reference to an aggregate");
		else
			df_emit(b, out, (Node *) tle->expr, DF_LEVEL_AGG);
		b->agg = agg;
		b->agg_named = named;
	}
	else if (IsA(child, Motion))
		df_emit_column(b, out, df_input_of(b, child),
					   df_motion_stream_column((Motion *) child, resno) + 1,
					   exprType((Node *) tle->expr),
					   df_scale_of(child, (Node *) tle->expr));
	else
	{
		b->ctx = child;
		df_emit(b, out, (Node *) tle->expr, DF_LEVEL_SCAN);
		b->ctx = saved;
	}
}

/* Index of aggregate call ('agg', 'fn'), adding it if new. */
static int
df_agg_ref(DfBuilder *b, Aggref *agg, const char *fn)
{
	ListCell   *la,
			   *lf;
	int			i = 0;

	forboth(la, b->aggrefs, lf, b->aggfns)
	{
		const char *f = lfirst(lf);

		if (equal(lfirst(la), agg) &&
			((f == NULL && fn == NULL) || (f && fn && strcmp(f, fn) == 0)))
			return i;
		i++;
	}
	b->aggrefs = lappend(b->aggrefs, agg);
	b->aggfns = lappend(b->aggfns, (void *) fn);
	return i;
}

/* The DataFusion state a partial aggregate of Agg 'ctx' sends instead of its own. */
static DfAggState
df_partial_state(Plan *ctx, Node *node)
{
	if (!IsA(node, Aggref) || ((Aggref *) node)->aggsplit != AGGSPLIT_INITIAL_SERIAL)
		return DF_AGG_PLAIN;
	return df_agg_state_at(ctx, (Aggref *) node);
}

/* Is 'node' a sum of Agg 'ctx' that keeps its display scale (MS1)? */
static bool
df_is_mixed_sum(Plan *ctx, Node *node)
{
	return IsA(node, Aggref) && IsA(ctx, Agg) &&
		df_agg_state_at(ctx, (Aggref *) node) == DF_AGG_SUM_NUMERIC_MIXED;
}

/* 'when' result 'then' */
static CaseWhen *
df_case_when(Expr *when, Expr *then)
{
	CaseWhen   *w = makeNode(CaseWhen);

	w->expr = when;
	w->result = then;
	w->location = -1;
	return w;
}

/* 'e' IS [NOT] NULL */
static Expr *
df_null_test(Expr *e, NullTestType type)
{
	NullTest   *nt = makeNode(NullTest);

	nt->arg = e;
	nt->nulltesttype = type;
	nt->argisrow = false;
	nt->location = -1;
	return (Expr *) nt;
}

/* An int4 constant, or NULL. */
static Expr *
df_int4_const(int v, bool isnull)
{
	return (Expr *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
							  Int32GetDatum(v), isnull, true);
}

/*
 * The display scale of each row of MS1's CASE or COALESCE 'arg' of node
 * 'ctx': its branch's scale, NULL where the value is NULL, which sum
 * leaves out.  The same branches are taken: the CASE keeps its tests.
 */
static Expr *
df_mixed_scale_expr(Plan *ctx, Node *arg)
{
	CaseExpr   *out = makeNode(CaseExpr);
	ListCell   *lc;
	int			p,
				s;

	out->casetype = INT4OID;
	out->casecollid = InvalidOid;
	out->location = -1;
	if (IsA(arg, CaseExpr))
	{
		CaseExpr   *ce = (CaseExpr *) arg;
		List	   *results = NIL;

		out->arg = ce->arg;
		foreach(lc, ce->args)
			results = lappend(results, lfirst_node(CaseWhen, lc)->result);
		results = lappend(results, ce->defresult);
		foreach(lc, results)
		{
			Expr	   *r = lfirst(lc);
			Expr	   *scale;

			if (r == NULL || (IsA(r, Const) && ((Const *) r)->constisnull))
				scale = df_int4_const(0, true);
			else
			{
				CaseExpr   *sc = makeNode(CaseExpr);

				(void) df_numeric_ps(ctx, (Node *) r, &p, &s);
				sc->casetype = INT4OID;
				sc->args = list_make1(df_case_when(df_null_test(r, IS_NULL),
												   df_int4_const(0, true)));
				sc->defresult = df_int4_const(s, false);
				sc->location = -1;
				scale = (Expr *) sc;
			}
			if (foreach_current_index(lc) < list_length(ce->args))
				out->args = lappend(out->args,
									df_case_when(list_nth_node(CaseWhen, ce->args,
															   foreach_current_index(lc))->expr,
												 scale));
			else
				out->defresult = scale;
		}
	}
	else
	{
		/* COALESCE: the scale of the first argument that is not NULL */
		foreach(lc, ((CoalesceExpr *) arg)->args)
		{
			Expr	   *a = lfirst(lc);

			if (IsA(a, Const) && ((Const *) a)->constisnull)
				continue;
			(void) df_numeric_ps(ctx, (Node *) a, &p, &s);
			out->args = lappend(out->args, df_case_when(df_null_test(a, IS_NOT_NULL),
														df_int4_const(s, false)));
		}
		out->defresult = df_int4_const(0, true);
	}
	return (Expr *) out;
}

static void
df_emit_list(DfBuilder *b, StringInfo out, List *items, DfLevel level)
{
	ListCell   *lc;
	bool		first = true;

	appendStringInfoChar(out, '[');
	foreach(lc, items)
	{
		if (!first)
			appendStringInfoChar(out, ',');
		first = false;
		df_emit(b, out, lfirst(lc), level);
	}
	appendStringInfoChar(out, ']');
}

/* A qual list is an implicit AND. */
static void
df_emit_qual(DfBuilder *b, StringInfo out, List *qual, DfLevel level)
{
	if (qual == NIL)
		appendStringInfoString(out, "null");
	else if (list_length(qual) == 1)
		df_emit(b, out, linitial(qual), level);
	else
	{
		appendStringInfoString(out, "{\"and\":");
		df_emit_list(b, out, qual, level);
		appendStringInfoChar(out, '}');
	}
}

static void
df_emit(DfBuilder *b, StringInfo out, Node *node, DfLevel level)
{
	if (b->failed)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				if (level == DF_LEVEL_SCAN && var->varno == INNER_VAR && !b->join_sides &&
					IsA(b->ctx, HashJoin) &&
					((HashJoin *) b->ctx)->join.jointype == JOIN_SEMI &&
					df_semi_key_for((HashJoin *) b->ctx, var) != NULL)
				{
					/* the equal key of the side a semi join keeps */
					df_emit(b, out, df_semi_key_for((HashJoin *) b->ctx, var), level);
					return;
				}
				if (level == DF_LEVEL_SCAN && var->varno == OUTER_VAR)
					df_emit_output_of(b, out, outerPlan(b->ctx), var->varattno);
				else if (level == DF_LEVEL_SCAN && var->varno == INNER_VAR)
					df_emit_output_of(b, out, innerPlan(b->ctx), var->varattno);
				else if (level == DF_LEVEL_SCAN && IsA(b->ctx, SubqueryScan) &&
						 ((Scan *) b->ctx)->scanrelid == var->varno)
					/* SQ1: a column of the subquery's plan */
					df_emit_output_of(b, out, ((SubqueryScan *) b->ctx)->subplan, var->varattno);
				else if (level == DF_LEVEL_SCAN && IsA(b->ctx, SeqScan) &&
						 ((Scan *) b->ctx)->scanrelid == var->varno &&
						 df_input_of(b, b->ctx) >= 0)
					df_emit_column(b, out, df_input_of(b, b->ctx),
								   var->varattno, var->vartype,
								   df_scale_of(b->ctx, (Node *) var));
				else if (level == DF_LEVEL_AGG && var->varno == OUTER_VAR)
				{
					int			k;

					for (k = 0; k < b->agg->numCols; k++)
						if (b->agg->grpColIdx[k] == var->varattno)
							break;
					if (k < b->agg->numCols && b->agg_named)
						appendStringInfo(out, "{\"name\":\"q%d_g%d\"}",
										 b->agg->plan.plan_node_id, k);
					else if (k < b->agg->numCols)
						appendStringInfo(out, "{\"group\":%d}", k);
					else
						df_emit_ungrouped(b, out, var);
				}
				else
					df_fail(b, "a column reference");
				return;
			}

		case T_RowIdExpr:
			/* RI1: the column its node's rows were numbered in */
			if (level != DF_LEVEL_SCAN || !df_numbers_rows(b->ctx))
			{
				df_fail(b, "a RowIdExpr");
				return;
			}
			appendStringInfo(out, "{\"name\":\"rowid%d\"}", b->ctx->plan_node_id);
			return;

		case T_Param:
			/* IP1: an init plan's value (numeric only compared: above) */
			if (((Param *) node)->paramtype == NUMERICOID)
			{
				df_fail(b, "a numeric parameter");
				return;
			}
			df_emit(b, out, (Node *) df_param_const((Param *) node), level);
			return;

		case T_Const:
			df_emit_const(b, out, (Const *) node);
			return;

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				char	   *name = get_opname(op->opno);
				const char *tag = df_type_tag(op->opresulttype);
				Node	   *date_arg;
				const char *date_cmp;
				int32		date_value;

				if (name == NULL || tag == NULL)
				{
					df_fail(b, "an operator");
					return;
				}
				if (df_numeric_param_cmp(df_level_ctx(b, level), op, NULL, NULL))
				{
					df_emit_numeric_param_cmp(b, out, op, level);
					return;
				}
				if (df_date_timestamp_cmp(op, &date_arg, &date_cmp, &date_value))
				{
					/* a date against a timestamp constant, as dates (DT1) */
					appendStringInfo(out, "{\"op\":\"%s\",\"type\":\"bool\",\"args\":[", date_cmp);
					df_emit(b, out, date_arg, level);
					appendStringInfo(out, ",{\"lit\":{\"type\":\"date\",\"value\":%d}}]}", date_value);
					return;
				}
				if (op->opresulttype == NUMERICOID)
				{
					/*
					 * numeric + - * (N3): operands of + and - at the result
					 * scale, those of * at their own (whose sum it is).
					 */
					Plan	   *ctx = level == DF_LEVEL_AGG ? (Plan *) b->agg : b->ctx;
					int			rs = df_scale_of(ctx, node);
					ListCell   *la;

					appendStringInfo(out, "{\"op\":\"%s\",\"type\":\"%s\",\"args\":[",
									 name, df_tag(NUMERICOID, rs));
					foreach(la, op->args)
					{
						if (foreach_current_index(la) > 0)
							appendStringInfoChar(out, ',');
						if (strcmp(name, "*") == 0)
							df_emit(b, out, lfirst(la), level);
						else
							df_emit_numeric_at(b, out, lfirst(la), rs, level, ctx);
					}
					appendStringInfoString(out, "]}");
					return;
				}
				appendStringInfo(out, "{\"op\":\"%s\",\"type\":\"%s\",\"args\":", name, tag);
				if (exprType(linitial(op->args)) == NUMERICOID)
				{
					/* compared at the larger scale (N2) */
					Plan	   *ctx = level == DF_LEVEL_AGG ? (Plan *) b->agg : b->ctx;
					Node	   *l = linitial(op->args);
					Node	   *r = lsecond(op->args);
					int			t;
					int			lp,
								ls = 0,
								rp,
								rs = 0;

					if (IsA(l, Const))
						(void) df_numeric_const_ps(((Const *) l)->constvalue, &lp, &ls);
					else
						ls = df_scale_of(ctx, l);
					if (IsA(r, Const))
						(void) df_numeric_const_ps(((Const *) r)->constvalue, &rp, &rs);
					else
						rs = df_scale_of(ctx, r);
					t = Max(ls, rs);
					appendStringInfoChar(out, '[');
					df_emit_numeric_at(b, out, l, t, level, ctx);
					appendStringInfoChar(out, ',');
					df_emit_numeric_at(b, out, r, t, level, ctx);
					appendStringInfoString(out, "]}");
					return;
				}
				if (op->opresulttype == BOOLOID && list_length(op->args) == 2 &&
					strcmp(name, "~~") != 0 && strcmp(name, "!~~") != 0)
				{
					/* a comparison (LIKE sees character values padded: bpcharlike) */
					df_emit_operands(b, out, linitial(op->args), lsecond(op->args), level);
					appendStringInfoChar(out, '}');
					return;
				}
				df_emit_list(b, out, op->args, level);
				appendStringInfoChar(out, '}');
				return;
			}

		case T_BoolExpr:
			{
				BoolExpr   *be = (BoolExpr *) node;

				if (be->boolop == NOT_EXPR)
				{
					appendStringInfoString(out, "{\"not\":");
					df_emit(b, out, linitial(be->args), level);
				}
				else
				{
					appendStringInfoString(out, be->boolop == AND_EXPR ? "{\"and\":" : "{\"or\":");
					df_emit_list(b, out, be->args, level);
				}
				appendStringInfoChar(out, '}');
				return;
			}

		case T_CaseExpr:
			{
				/* E1: {"case": [{"when": c, "then": r}, ...], "else": e} */
				CaseExpr   *ce = (CaseExpr *) node;
				int			scale = df_scale_of(df_level_ctx(b, level), node);
				Node	   *saved = b->case_arg;
				ListCell   *lc;

				b->case_arg = (Node *) ce->arg;
				appendStringInfoString(out, "{\"case\":[");
				foreach(lc, ce->args)
				{
					CaseWhen   *w = lfirst_node(CaseWhen, lc);

					if (foreach_current_index(lc) > 0)
						appendStringInfoChar(out, ',');
					appendStringInfoString(out, "{\"when\":");
					df_emit(b, out, (Node *) w->expr, level);
					appendStringInfoString(out, ",\"then\":");
					df_emit_result(b, out, (Node *) w->result, ce->casetype, scale, level);
					appendStringInfoChar(out, '}');
				}
				b->case_arg = saved;
				appendStringInfoString(out, "],\"else\":");
				df_emit_result(b, out, (Node *) ce->defresult, ce->casetype, scale, level);
				appendStringInfoChar(out, '}');
				return;
			}

		case T_CaseTestExpr:
			/* the value of a simple CASE (CASE x WHEN ...) */
			if (b->case_arg == NULL)
			{
				df_fail(b, "a CASE value outside a CASE");
				return;
			}
			df_emit(b, out, b->case_arg, level);
			return;

		case T_CoalesceExpr:
			{
				/*
				 * E1: as CASE WHEN a IS NOT NULL THEN a ... ELSE last END,
				 * so that later arguments only run where the earlier ones
				 * are NULL, as in PostgreSQL.
				 */
				CoalesceExpr *co = (CoalesceExpr *) node;
				int			scale = df_scale_of(df_level_ctx(b, level), node);
				int			n = list_length(co->args);
				ListCell   *lc;

				appendStringInfoString(out, "{\"case\":[");
				foreach(lc, co->args)
				{
					if (foreach_current_index(lc) == n - 1)
						break;
					if (foreach_current_index(lc) > 0)
						appendStringInfoChar(out, ',');
					appendStringInfoString(out, "{\"when\":{\"isnotnull\":");
					df_emit(b, out, lfirst(lc), level);
					appendStringInfoString(out, "},\"then\":");
					df_emit_result(b, out, lfirst(lc), co->coalescetype, scale, level);
					appendStringInfoChar(out, '}');
				}
				appendStringInfoString(out, "],\"else\":");
				df_emit_result(b, out, llast(co->args), co->coalescetype, scale, level);
				appendStringInfoChar(out, '}');
				return;
			}

		case T_NullIfExpr:
			{
				/* E1: CASE WHEN a = b THEN NULL ELSE a END */
				NullIfExpr *ni = (NullIfExpr *) node;
				OpExpr	   *eq = makeNode(OpExpr);
				Oid			type = exprType(linitial(ni->args));
				int			scale = df_scale_of(df_level_ctx(b, level), linitial(ni->args));

				*eq = *(OpExpr *) ni;
				eq->xpr.type = T_OpExpr;
				eq->opresulttype = BOOLOID;
				appendStringInfoString(out, "{\"case\":[{\"when\":");
				df_emit(b, out, (Node *) eq, level);
				appendStringInfoString(out, ",\"then\":");
				df_emit_result(b, out,
							   (Node *) makeNullConst(type, -1, ni->inputcollid),
							   type, scale, level);
				appendStringInfoString(out, "}],\"else\":");
				df_emit_result(b, out, linitial(ni->args), type, scale, level);
				appendStringInfoChar(out, '}');
				return;
			}

		case T_DistinctExpr:
			{
				/* E1: {"distinct": [a, b]}; IS NOT DISTINCT FROM is NOT of it */
				DistinctExpr *de = (DistinctExpr *) node;

				appendStringInfoString(out, "{\"distinct\":");
				df_emit_operands(b, out, linitial(de->args), lsecond(de->args), level);
				appendStringInfoChar(out, '}');
				return;
			}

		case T_ScalarArrayOpExpr:
			{
				/* E1: {"inlist": x, "values": [...], "negated": bool} */
				ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) node;
				Node	   *left = linitial(sa->args);
				Const	   *arr = lsecond(sa->args);
				ArrayType  *a = DatumGetArrayTypeP(arr->constvalue);
				Oid			elemtype = ARR_ELEMTYPE(a);
				Plan	   *ctx = df_level_ctx(b, level);
				Datum	   *elems;
				bool	   *nulls;
				int			nelems,
							i,
							scale;
				int16		elmlen;
				bool		elmbyval;
				char		elmalign;

				get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(a, elemtype, elmlen, elmbyval, elmalign,
								  &elems, &nulls, &nelems);
				/* numeric: the column and every element at the largest scale */
				scale = df_scale_of(ctx, left);
				for (i = 0; i < nelems && elemtype == NUMERICOID; i++)
				{
					int			p,
								s;

					if (!nulls[i] && df_numeric_const_ps(elems[i], &p, &s))
						scale = Max(scale, s);
				}
				appendStringInfoString(out, "{\"inlist\":");
				if (elemtype == NUMERICOID)
					df_emit_numeric_at(b, out, left, scale, level, ctx);
				else
					df_emit_compared(b, out, left, level);
				appendStringInfoString(out, ",\"values\":[");
				for (i = 0; i < nelems; i++)
				{
					Const	   *c = makeConst(elemtype, -1, sa->inputcollid, elmlen,
											  elems[i], nulls[i], elmbyval);

					if (i > 0)
						appendStringInfoChar(out, ',');
					if (elemtype == NUMERICOID)
						df_emit_numeric_const(b, out, c, scale);
					else
						df_emit_compared(b, out, (Node *) c, level);
				}
				appendStringInfo(out, "],\"negated\":%s}", sa->useOr ? "false" : "true");
				return;
			}

		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				appendStringInfoString(out, nt->nulltesttype == IS_NULL ?
									   "{\"isnull\":" : "{\"isnotnull\":");
				df_emit(b, out, (Node *) nt->arg, level);
				appendStringInfoChar(out, '}');
				return;
			}

		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;
				const DfStringFunc *f = df_string_func(fe->funcid);
				const char *tag = df_type_tag(fe->funcresulttype);

				if (df_bpchar_func(fe->funcid) != NULL)
				{
					/* B1: character to text drops trailing blanks; lengths */
					const char *fn = df_bpchar_func(fe->funcid);

					if (strcmp(fn, "bpkey") == 0)
						appendStringInfoString(out, "{\"bpkey\":");
					else
						appendStringInfo(out, "{\"call\":\"%s\",\"type\":\"int4\",\"args\":[%s",
										 fn, strcmp(fn, "char_length") == 0 ? "{\"bpkey\":" : "");
					df_emit(b, out, linitial(fe->args), level);
					appendStringInfoString(out, strcmp(fn, "bpkey") == 0 ? "}" :
										   strcmp(fn, "char_length") == 0 ? "}]}" : "]}");
					return;
				}
				if (df_extract_field(fe) != NULL)
				{
					/* DT2: df_core::pgdate */
					appendStringInfoString(out, "{\"extract\":");
					df_emit(b, out, lsecond(fe->args), level);
					appendStringInfo(out, ",\"field\":\"%s\"}", df_extract_field(fe));
					return;
				}
				if (df_cast_kind(fe->funcid) != NULL && strcmp(df_cast_kind(fe->funcid), "widen") == 0)
				{
					/* exact, or rounding as C does (E2) */
					appendStringInfoString(out, "{\"cast\":");
					df_emit(b, out, linitial(fe->args), level);
					appendStringInfo(out, ",\"type\":\"%s\"}", tag);
					return;
				}
				if (df_cast_kind(fe->funcid) != NULL)
				{
					/* PostgreSQL's rounding and errors (E2): df_core::pgcast */
					Plan	   *ctx = df_level_ctx(b, level);
					Node	   *arg = linitial(fe->args);
					int			rs = fe->funcresulttype == NUMERICOID ? df_scale_of(ctx, node) : 0;
					int			p = 0,
								s;

					if (fe->funcresulttype == NUMERICOID)
						(void) df_numeric_ps(ctx, node, &p, &s);
					appendStringInfoString(out, "{\"pgcast\":");
					df_emit(b, out, arg, level);
					appendStringInfo(out, ",\"kind\":\"%s\",\"from\":\"%s\",\"type\":\"%s\",\"precision\":%d}",
									 df_cast_kind(fe->funcid),
									 df_tag(exprType(arg), df_scale_of(ctx, arg)),
									 df_tag(fe->funcresulttype, rs), p);
					return;
				}
				if (fe->funcid == F_NUMERIC_INT2 || fe->funcid == F_NUMERIC_INT4 ||
					fe->funcid == F_NUMERIC_INT8)
				{
					/* an integer made numeric, exactly (N3) */
					appendStringInfoString(out, "{\"cast\":");
					df_emit(b, out, linitial(fe->args), level);
					appendStringInfoString(out, ",\"type\":\"numeric\"}");
					return;
				}

				if (f == NULL || tag == NULL)
				{
					df_fail(b, "a function call");
					return;
				}
				appendStringInfo(out, "{\"call\":\"%s\",\"type\":\"%s\",\"args\":", f->name, tag);
				df_emit_list(b, out, fe->args, level);
				appendStringInfoChar(out, '}');
				return;
			}

		case T_RelabelType:
			/* varchar read as text, or COLLATE (checked by the planner hook) */
			df_emit(b, out, (Node *) ((RelabelType *) node)->arg, level);
			return;

		case T_Aggref:
			if (level != DF_LEVEL_AGG)
			{
				df_fail(b, "an aggregate outside the aggregate node");
				return;
			}
			if (df_agg_state((Aggref *) node) == DF_AGG_AVG_INT)
			{
				/* only a whole output column (df_emit_outputs) */
				df_fail(b, "an avg returning numeric inside an expression");
				return;
			}
			if (df_named_of(b, b->agg) != NULL)
			{
				/* A1: a call of an Agg below the top (df_emit_node) */
				df_emit_named_call(b, out, (Aggref *) node);
				return;
			}
			appendStringInfo(out, "{\"agg\":%d}", df_agg_ref(b, (Aggref *) node, NULL));
			return;

		default:
			df_fail(b, "an expression");
			return;
	}
}

/* A join key, cast to 'type' if it has another (numeric: to 'scale'). */
static void
df_emit_key(DfBuilder *b, StringInfo out, Node *key, Oid type, int scale)
{
	const char *tag = df_type_tag(type);

	if (tag == NULL)
	{
		df_fail(b, "a join key");
		return;
	}
	if (type == NUMERICOID)
	{
		df_emit_numeric_at(b, out, key, scale, DF_LEVEL_SCAN, b->ctx);
		return;
	}
	if (exprType(key) == type)
	{
		df_emit_compared(b, out, key, DF_LEVEL_SCAN);
		return;
	}
	appendStringInfoString(out, "{\"cast\":");
	df_emit(b, out, key, DF_LEVEL_SCAN);
	appendStringInfo(out, ",\"type\":\"%s\"}", tag);
}

/*
 * The plan node 'plan' of the slice, below its aggregate, as a node of the
 * spec.  Inputs are added in the order they are met: a join's Hash side
 * first, which is the order they are fed in.
 */
static void
df_emit_plan_node(DfBuilder *b, StringInfo out, Plan *plan)
{
	if (b->failed)
		return;
	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_Motion:
			{
				int			j = df_add_input(b, plan);

				if (plan->qual == NIL)
				{
					appendStringInfo(out, "{\"input\":%d}", j);
					return;
				}
				appendStringInfo(out, "{\"filter\":{\"input\":{\"input\":%d},\"pred\":", j);
				b->ctx = plan;
				df_emit_qual(b, out, plan->qual, DF_LEVEL_SCAN);
				appendStringInfoString(out, "}}");
				return;
			}

		case T_Hash:
		case T_Material:
			df_emit_node(b, out, outerPlan(plan));
			return;

		case T_NestLoop:
			{
				/*
				 * NL1: DataFusion's nested loop join collects its left side,
				 * PostgreSQL's inner one, fed first, and streams the other;
				 * the join types turn around as for a Hash Join.  No keys,
				 * the join filter decides.
				 */
				Join	   *join = (Join *) plan;
				const char *type;

				switch (join->jointype)
				{
					case JOIN_INNER:
						type = "inner";
						break;
					case JOIN_LEFT:
						type = "right";
						break;
					case JOIN_SEMI:
						type = "rightsemi";
						break;
					case JOIN_ANTI:
						type = "rightanti";
						break;
					default:
						df_fail(b, "this kind of Nested Loop");
						return;
				}
				if (plan->qual != NIL)
					appendStringInfoString(out, "{\"filter\":{\"input\":");
				appendStringInfo(out, "{\"join\":{\"type\":\"%s\",\"left\":", type);
				df_emit_node(b, out, innerPlan(plan));
				appendStringInfoString(out, ",\"right\":");
				df_emit_node(b, out, outerPlan(plan));
				appendStringInfoString(out, ",\"on\":[],\"filter\":");
				b->ctx = plan;
				b->join_sides = true;
				df_emit_qual(b, out, join->joinqual, DF_LEVEL_SCAN);
				b->join_sides = false;
				appendStringInfoString(out, "}}");
				if (plan->qual != NIL)
				{
					appendStringInfoString(out, ",\"pred\":");
					b->ctx = plan;
					df_emit_qual(b, out, plan->qual, DF_LEVEL_SCAN);
					appendStringInfoString(out, "}}");
				}
				return;
			}

		case T_Result:
			/* R1: its child's rows, its filter over them */
			if (plan->qual == NIL)
			{
				df_emit_node(b, out, outerPlan(plan));
				return;
			}
			appendStringInfoString(out, "{\"filter\":{\"input\":");
			df_emit_node(b, out, outerPlan(plan));
			appendStringInfoString(out, ",\"pred\":");
			b->ctx = plan;
			df_emit_qual(b, out, plan->qual, DF_LEVEL_SCAN);
			appendStringInfoString(out, "}}");
			return;

		case T_SubqueryScan:
			/* SQ1: its plan's rows, its filter over them */
			if (plan->qual == NIL)
			{
				df_emit_node(b, out, ((SubqueryScan *) plan)->subplan);
				return;
			}
			appendStringInfoString(out, "{\"filter\":{\"input\":");
			df_emit_node(b, out, ((SubqueryScan *) plan)->subplan);
			appendStringInfoString(out, ",\"pred\":");
			b->ctx = plan;
			df_emit_qual(b, out, plan->qual, DF_LEVEL_SCAN);
			appendStringInfoString(out, "}}");
			return;

		case T_Sort:
			/* below a GroupAggregate run hashed (D2) */
			df_emit_node(b, out, outerPlan(plan));
			return;

		case T_Agg:
			{
				/*
				 * A1, D2: an Agg below a join or another Agg (df_check_slice),
				 * its groups named q<plan_node_id>_g<i> and its calls
				 * q<plan_node_id>_a<k>.
				 */
				Agg		   *agg = (Agg *) plan;
				DfNamedAgg *na = palloc0(sizeof(DfNamedAgg));
				List	   *calls = NIL;
				DfUngrouped ungrouped;
				Agg		   *saved = b->agg;
				bool		named = b->agg_named;
				ListCell   *lc;
				int			i;

				na->agg = agg;
				df_collect_aggrefs((Node *) plan->targetlist, &calls);
				df_collect_aggrefs((Node *) plan->qual, &calls);
				foreach(lc, calls)
					if (!list_member(na->aggrefs, lfirst(lc)))
					{
						na->aggrefs = lappend(na->aggrefs, lfirst(lc));
						na->aggfns = lappend(na->aggfns, NULL);
					}
				/* RI1: the columns it does not group by, as any() */
				ungrouped.agg = agg;
				ungrouped.vars = NIL;
				df_collect_ungrouped((Node *) plan->targetlist, &ungrouped);
				df_collect_ungrouped((Node *) plan->qual, &ungrouped);
				foreach(lc, ungrouped.vars)
				{
					Aggref	   *call = df_any_call(lfirst(lc));

					if (!list_member(na->aggrefs, call))
					{
						na->aggrefs = lappend(na->aggrefs, call);
						na->aggfns = lappend(na->aggfns, "any");
					}
				}
				appendStringInfoString(out, "{\"aggregate\":{\"input\":");
				df_emit_node(b, out, outerPlan(plan));
				appendStringInfoString(out, ",\"group\":[");
				for (i = 0; i < agg->numCols; i++)
				{
					if (i > 0)
						appendStringInfoChar(out, ',');
					df_emit_grouped(b, out, outerPlan(plan), agg->grpColIdx[i]);
				}
				appendStringInfoString(out, "],\"aggs\":[");
				df_emit_agg_calls(b, out, agg, na->aggrefs, na->aggfns);
				b->named = lappend(b->named, na);
				appendStringInfoString(out, "],\"having\":");
				b->agg = agg;
				b->agg_named = false;
				df_emit_qual(b, out, plan->qual, DF_LEVEL_AGG);
				b->agg = saved;
				b->agg_named = named;
				appendStringInfo(out, ",\"name\":\"q%d\"}}", plan->plan_node_id);
				return;
			}

		case T_HashJoin:
			{
				HashJoin   *hj = (HashJoin *) plan;
				ListCell   *lc;
				bool		first = true;
				const char *type;

				/*
				 * DataFusion builds its hash table on the left: PostgreSQL's
				 * Hash side, its inner.  PostgreSQL's outer is DataFusion's
				 * right, so the join type turns around.  Its hash clauses read
				 * outer = inner.
				 */
				switch (hj->join.jointype)
				{
					case JOIN_INNER:
						type = "inner";
						break;
					case JOIN_LEFT:
						type = "right";
						break;
					case JOIN_RIGHT:
						type = "left";
						break;
					case JOIN_FULL:
						type = "full";
						break;
					case JOIN_SEMI:
						type = "rightsemi";
						break;
					case JOIN_ANTI:
						type = "rightanti";
						break;
					case JOIN_RIGHT_ANTI:
						type = "leftanti";
						break;
					case JOIN_LASJ_NOTIN:
						/* NJ1: below */
						type = "leftanti";
						break;
					default:
						df_fail(b, "this kind of join");
						return;
				}
				if (plan->qual != NIL)
					appendStringInfoString(out, "{\"filter\":{\"input\":");
				if (hj->join.jointype == JOIN_LASJ_NOTIN)
				{
					/*
					 * NJ1: NOT IN.  PostgreSQL returns nothing if the inner
					 * side has a NULL key, and drops an outer row with a NULL
					 * key unless the inner side is empty: DataFusion's
					 * null-aware anti join, which keeps its left side and
					 * builds its table there.  So the outer side is the left
					 * one, fed first, and the keys read outer, inner.
					 */
					OpExpr	   *op = linitial_node(OpExpr, hj->hashclauses);
					Node	   *outer = linitial(op->args);
					Node	   *inner = lsecond(op->args);
					Oid			ktype = df_join_key_type(exprType(inner), exprType(outer));
					int			scale = ktype != NUMERICOID ? 0 :
						Max(df_scale_of(plan, inner), df_scale_of(plan, outer));

					appendStringInfoString(out, "{\"join\":{\"type\":\"leftanti\",\"null_aware\":true,\"left\":");
					df_emit_node(b, out, outerPlan(plan));
					appendStringInfoString(out, ",\"right\":");
					df_emit_node(b, out, innerPlan(plan));
					appendStringInfoString(out, ",\"on\":[[");
					b->ctx = plan;
					b->join_sides = true;
					df_emit_key(b, out, outer, ktype, scale);
					appendStringInfoChar(out, ',');
					df_emit_key(b, out, inner, ktype, scale);
					b->join_sides = false;
					appendStringInfoString(out, "]],\"filter\":null}}");
					if (plan->qual != NIL)
					{
						appendStringInfoString(out, ",\"pred\":");
						b->ctx = plan;
						df_emit_qual(b, out, plan->qual, DF_LEVEL_SCAN);
						appendStringInfoString(out, "}}");
					}
					return;
				}
				appendStringInfo(out, "{\"join\":{\"type\":\"%s\",\"left\":", type);
				df_emit_node(b, out, innerPlan(plan));
				appendStringInfoString(out, ",\"right\":");
				df_emit_node(b, out, outerPlan(plan));
				appendStringInfoString(out, ",\"on\":[");
				b->ctx = plan;
				b->join_sides = true;
				foreach(lc, hj->hashclauses)
				{
					OpExpr	   *op = lfirst_node(OpExpr, lc);
					Node	   *inner = lsecond(op->args);
					Node	   *outer = linitial(op->args);
					Oid			type = df_join_key_type(exprType(inner), exprType(outer));
					int			scale = type != NUMERICOID ? 0 :
						Max(df_scale_of(b->ctx, inner), df_scale_of(b->ctx, outer));

					/* Keys of two types are compared in the wider one. */
					appendStringInfoString(out, first ? "[" : ",[");
					first = false;
					df_emit_key(b, out, inner, type, scale);
					appendStringInfoChar(out, ',');
					df_emit_key(b, out, outer, type, scale);
					appendStringInfoChar(out, ']');
				}
				appendStringInfoString(out, "],\"filter\":");
				b->ctx = plan;
				df_emit_qual(b, out, hj->join.joinqual, DF_LEVEL_SCAN);
				b->join_sides = false;
				appendStringInfoString(out, "}}");
				if (plan->qual != NIL)
				{
					appendStringInfoString(out, ",\"pred\":");
					b->ctx = plan;
					df_emit_qual(b, out, plan->qual, DF_LEVEL_SCAN);
					appendStringInfoString(out, "}}");
				}
				return;
			}

		default:
			df_fail(b, "this plan node");
			return;
	}
}

/* Is there a RowIdExpr in 'node'? */
static bool
df_has_rowid_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, RowIdExpr))
		return true;
	return expression_tree_walker(node, df_has_rowid_walker, context);
}

/*
 * RI1: does plan node 'plan' number its rows (a RowIdExpr in its
 * targetlist, of a scan or join: df_check_slice)?  Its rows get a column
 * rowid<plan_node_id>.
 */
static bool
df_numbers_rows(Plan *plan)
{
	return (IsA(plan, SeqScan) || IsA(plan, HashJoin)) &&
		df_has_rowid_walker((Node *) plan->targetlist, NULL);
}

/*
 * RI1: the number RowIdExpr counts up from in this process, as
 * execExpr.c has it: unique to the segment (and parallel worker).
 */
static int64
df_rowid_base(void)
{
	if (TotalParallelWorkerNumberOfSlice > 0)
	{
		int			bits = pg_leftmost_one_pos32(TotalParallelWorkerNumberOfSlice) + 1;
		int64		base = ((int64) GpIdentity.dbid) << (48 + bits);

		if (IsParallelWorkerOfSlice())
			base |= ((int64) ParallelWorkerNumberOfSlice) << 48;
		return base;
	}
	return ((int64) GpIdentity.dbid) << 48;
}

/*
 * The plan node 'plan' of the slice, below its aggregate, as a node of the
 * spec, its rows numbered if it has a RowIdExpr (RI1).
 */
static void
df_emit_node(DfBuilder *b, StringInfo out, Plan *plan)
{
	if (b->failed || !df_numbers_rows(plan))
	{
		df_emit_plan_node(b, out, plan);
		return;
	}
	appendStringInfoString(out, "{\"rowid\":{\"input\":");
	df_emit_plan_node(b, out, plan);
	appendStringInfo(out, ",\"name\":\"rowid%d\",\"base\":" INT64_FORMAT "}}",
					 plan->plan_node_id, df_rowid_base());
}

/*
 * The aggregate calls 'aggrefs' of Agg 'aggnode', with the function of
 * each in 'aggfns' (NULL for the call's own; NIL: all NULL), as the "aggs"
 * of its node.  Arguments are expressions of the Agg over its child.
 */
static void
df_emit_agg_calls(DfBuilder *b, StringInfo aggs, Agg *aggnode, List *aggrefs, List *aggfns)
{
	ListCell   *lc;

	foreach(lc, aggrefs)
	{
		Aggref	   *agg = lfirst_node(Aggref, lc);
		const char *fn = aggfns ? list_nth(aggfns, foreach_current_index(lc)) : NULL;
		char	   *name = get_func_name(agg->aggfnoid);
		bool		combine = (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL);

		if (foreach_current_index(lc) > 0)
			appendStringInfoChar(aggs, ',');
		if (fn != NULL && strcmp(fn, "any") == 0)
		{
			/* RI1: a column the groups determine (df_emit_ungrouped) */
			b->ctx = (Plan *) aggnode;
			appendStringInfoString(aggs, "{\"fn\":\"any\",\"arg\":");
			df_emit(b, aggs, (Node *) linitial_node(TargetEntry, agg->args)->expr, DF_LEVEL_SCAN);
			appendStringInfoChar(aggs, '}');
			continue;
		}
		if (combine && df_agg_state(agg) != DF_AGG_AVG_FLOAT &&
			df_agg_state(agg) != DF_AGG_PLAIN)
		{
			/* sum(int8), avg(int): the stream's numeric sum and count columns */
			Node	   *arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
			Plan	   *child = df_below_sort(outerPlan(aggnode));
			bool		count = fn != NULL && strcmp(fn, "merge_count") == 0;
			bool		mixed = fn != NULL && strcmp(fn, "mixed_scale") == 0;
			int			pos;

			if (!IsA(child, Motion) || !IsA(arg, Var))
			{
				df_fail(b, "a combining aggregate");
				return;
			}
			pos = df_motion_stream_column((Motion *) child, ((Var *) arg)->varattno);
			appendStringInfo(aggs, "{\"fn\":\"%s\",\"arg\":",
							 count ? "sum" : mixed ? "max" : "sum_decimal");
			df_emit_column(b, aggs, df_input_of(b, child), pos + (count || mixed ? 2 : 1),
						   count ? INT8OID : mixed ? INT4OID : NUMERICOID,
						   count || mixed ? 0 : df_scale_of((Plan *) aggnode, arg));
			appendStringInfoChar(aggs, '}');
			continue;
		}
		if (!combine && fn != NULL && strcmp(fn, "mixed_scale") == 0)
		{
			/* MS1: the largest display scale of the values added up */
			b->ctx = (Plan *) aggnode;
			appendStringInfoString(aggs, "{\"fn\":\"max\",\"arg\":");
			df_emit(b, aggs,
					(Node *) df_mixed_scale_expr((Plan *) aggnode,
												 (Node *) linitial_node(TargetEntry, agg->args)->expr),
					DF_LEVEL_SCAN);
			appendStringInfoChar(aggs, '}');
			continue;
		}
		if (!combine && fn == NULL && df_agg_state(agg) == DF_AGG_SUM_INT8)
			fn = "sum_numeric";	/* exact, and numeric as PostgreSQL's */
		if (!combine && fn == NULL && df_agg_state(agg) == DF_AGG_SUM_NUMERIC)
			fn = "sum_decimal";	/* exact, NaN if any is */
		if (combine && strcmp(name, "avg") == 0)
		{
			/* DataFusion's avg state: the stream's sum and count columns */
			Node	   *arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
			int			pos;

			Plan	   *child = df_below_sort(outerPlan(aggnode));

			if (!IsA(child, Motion) || !IsA(arg, Var))
			{
				df_fail(b, "a combining avg");
				return;
			}
			pos = df_motion_stream_column((Motion *) child, ((Var *) arg)->varattno);
			appendStringInfoString(aggs, "{\"fn\":\"avg_merge\",\"arg\":");
			df_emit_column(b, aggs, df_input_of(b, child), pos + 1, FLOAT8OID, 0);
			appendStringInfoString(aggs, ",\"arg2\":");
			df_emit_column(b, aggs, df_input_of(b, child), pos + 2, INT8OID, 0);
			appendStringInfoChar(aggs, '}');
			continue;
		}
		appendStringInfo(aggs, "{\"fn\":\"%s%s\",\"arg\":", fn ? fn : name,
						 !fn && combine && strcmp(name, "count") == 0 ? "_merge" : "");

		/*
		 * A combining aggregate keeps aggstar from the original call
		 * (count(*)), but its one argument is the partial state.
		 */
		if ((agg->aggstar && !combine) || agg->args == NIL)
			appendStringInfoString(aggs, "null");
		else
		{
			/* arguments are expressions of the aggregate over its child */
			Node	   *arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
			bool		bp = agg->aggdistinct != NIL && !combine && exprType(arg) == BPCHAROID;

			b->ctx = (Plan *) aggnode;
			if (bp)
				appendStringInfoString(aggs, "{\"bpchar\":");	/* told apart without blanks */
			df_emit(b, aggs, arg, DF_LEVEL_SCAN);
			if (bp)
				appendStringInfoChar(aggs, '}');
		}
		/*
		 * D1: over distinct arguments (min and max are the same without).
		 * A combining stage keeps the DISTINCT of the call, but adds up
		 * the partial results: the planner splits it only where each
		 * segment sees all of a value.
		 */
		if (agg->aggdistinct != NIL && !combine &&
			strcmp(name, "min") != 0 && strcmp(name, "max") != 0)
			appendStringInfoString(aggs, ",\"distinct\":true");
		appendStringInfoChar(aggs, '}');
	}
}

/*
 * The output columns of the spec: the values of 'values', expressions at
 * 'level' over the inputs of node 'ctx'.
 */
static void
df_emit_outputs(DfBuilder *b, StringInfo out, List *values, DfLevel level,
				DfSliceSpec *spec, Plan *ctx)
{
	ListCell   *lc;
	int			i = 0;

	spec->nout = 0;
	foreach(lc, values)
	{
		Node	   *expr = (Node *) lfirst(lc);

		if (level == DF_LEVEL_AGG && df_partial_state(ctx, expr) != DF_AGG_PLAIN)
			spec->nout += df_agg_state_ncols(df_partial_state(ctx, expr));
		else if (level == DF_LEVEL_AGG && IsA(expr, Aggref) &&
				 (df_agg_state((Aggref *) expr) == DF_AGG_AVG_INT ||
				  df_agg_state((Aggref *) expr) == DF_AGG_AVG_NUMERIC ||
				  df_is_mixed_sum(ctx, expr)))
			spec->nout += 2;	/* numeric sum and count, or display scale */
		else
			spec->nout++;
	}
	b->out_col = palloc0(sizeof(int) * (list_length(values) + 1));
	spec->out_types = palloc(sizeof(Oid) * Max(spec->nout, 1));
	spec->out_kinds = palloc0(sizeof(uint8) * Max(spec->nout, 1));
	spec->out_scales = palloc0(sizeof(int16) * Max(spec->nout, 1));
	appendStringInfoChar(out, '[');
	foreach(lc, values)
	{
		Expr	   *value = (Expr *) lfirst(lc);
		Oid			type = exprType((Node *) value);
		const char *tag = df_type_tag(type);
		DfAggState	state = level == DF_LEVEL_AGG ?
			df_partial_state(ctx, (Node *) value) : DF_AGG_PLAIN;

		b->out_col[foreach_current_index(lc)] = i;
		if (state != DF_AGG_PLAIN)
		{
			/* DataFusion's state: sum (float8 or numeric), then count (M7d, N1, N2) */
			Aggref	   *agg = (Aggref *) value;
			bool		fsum = state == DF_AGG_AVG_FLOAT;
			bool		dsum = state == DF_AGG_SUM_NUMERIC || state == DF_AGG_AVG_NUMERIC ||
				state == DF_AGG_SUM_NUMERIC_MIXED;
			int			scale = df_scale_of(ctx, (Node *) agg);

			spec->out_scales[i] = (int16) scale;
			spec->out_types[i++] = fsum ? FLOAT8OID : NUMERICOID;
			appendStringInfo(out, "%s{\"expr\":{\"agg\":%d},\"type\":\"%s\"}",
							 i > 1 ? "," : "",
							 df_agg_ref(b, agg, fsum ? "sum" : dsum ? "sum_decimal" : "sum_numeric"),
							 fsum ? "float8" : df_tag(NUMERICOID, scale));
			if (state == DF_AGG_SUM_NUMERIC_MIXED)
			{
				/* MS1: the largest display scale added up */
				spec->out_types[i++] = INT4OID;
				appendStringInfo(out, ",{\"expr\":{\"agg\":%d},\"type\":\"int4\"}",
								 df_agg_ref(b, agg, "mixed_scale"));
			}
			else if (df_agg_state_ncols(state) == 2)
			{
				spec->out_types[i++] = INT8OID;
				appendStringInfo(out, ",{\"expr\":{\"agg\":%d},\"type\":\"int8\"}",
								 df_agg_ref(b, agg, "count"));
			}
			continue;
		}
		if (level == DF_LEVEL_AGG && df_is_mixed_sum(ctx, (Node *) value))
		{
			/*
			 * MS1: the sum at the largest scale of its argument, and the
			 * largest display scale of the values added up, which the C side
			 * gives the result as PostgreSQL's sum does
			 */
			Aggref	   *agg = (Aggref *) value;
			int			scale = df_scale_of(ctx, (Node *) agg);

			spec->out_kinds[i] = DF_OUT_NUMERIC_MIXED;
			spec->out_scales[i] = (int16) scale;
			spec->out_types[i++] = NUMERICOID;
			spec->out_kinds[i] = DF_OUT_PART;
			spec->out_types[i++] = INT4OID;
			appendStringInfo(out, "%s{\"expr\":{\"agg\":%d},\"type\":\"%s\"},"
							 "{\"expr\":{\"agg\":%d},\"type\":\"int4\"}",
							 i > 2 ? "," : "",
							 df_agg_ref(b, agg, "sum_decimal"), df_tag(NUMERICOID, scale),
							 df_agg_ref(b, agg, "mixed_scale"));
			continue;
		}
		if (level == DF_LEVEL_AGG && IsA(value, Aggref) &&
			(df_agg_state((Aggref *) value) == DF_AGG_AVG_INT ||
			 df_agg_state((Aggref *) value) == DF_AGG_AVG_NUMERIC))
		{
			/* avg = numeric sum / count, divided on the C side (N1, N2) */
			Aggref	   *agg = (Aggref *) value;
			bool		combine = agg->aggsplit == AGGSPLIT_FINAL_DESERIAL;
			bool		dsum = df_agg_state(agg) == DF_AGG_AVG_NUMERIC;
			Node	   *arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
			int			scale = df_scale_of(ctx, arg);	/* arguments refer to the Agg's child */

			spec->out_kinds[i] = DF_OUT_NUMERIC_AVG;
			spec->out_scales[i] = (int16) scale;
			spec->out_types[i++] = NUMERICOID;
			spec->out_kinds[i] = DF_OUT_PART;
			spec->out_types[i++] = INT8OID;
			appendStringInfo(out, "%s{\"expr\":{\"agg\":%d},\"type\":\"%s\"},"
							 "{\"expr\":{\"agg\":%d},\"type\":\"int8\"}",
							 i > 2 ? "," : "",
							 df_agg_ref(b, agg, combine ? "merge_sum" : dsum ? "sum_decimal" : "sum_numeric"),
							 df_tag(NUMERICOID, scale),
							 df_agg_ref(b, agg, combine ? "merge_count" : "count"));
			continue;
		}
		if (tag == NULL)
		{
			df_fail(b, "an output column");
			return;
		}
		if (type == NUMERICOID)
		{
			spec->out_scales[i] = (int16) df_scale_of(ctx, (Node *) value);
			tag = df_tag(type, spec->out_scales[i]);
		}
		spec->out_types[i++] = type;
		if (i > 1)
			appendStringInfoChar(out, ',');
		appendStringInfoString(out, "{\"expr\":");
		df_emit(b, out, (Node *) value, level);
		appendStringInfo(out, ",\"type\":\"%s\"}", tag);
	}
	appendStringInfoChar(out, ']');
	/* the arrays above were sized by the first loop */
	if (!b->failed && i != spec->nout)
		elog(ERROR, "datafusion: %d output columns where %d were counted", i, spec->nout);
}

/*
 * Rows per batch: about DF_BATCH_BYTES of the widest row the planner
 * expects anywhere in the slice, at most DF_BATCH_MAX_ROWS.  DataFusion's
 * operators allocate in proportion to a batch's bytes, and a partition's
 * share of the memory budget is sized for batches of about this size.
 */
#define DF_BATCH_BYTES		(256 * 1024)
#define DF_BATCH_MIN_ROWS	16
#define DF_BATCH_MAX_ROWS	8192

static int
df_slice_width(Plan *plan)
{
	int			width;

	if (plan == NULL)
		return 0;
	width = plan->plan_width;
	if (IsA(plan, Motion))
		return width;			/* another slice below */
	return Max(width, Max(df_slice_width(outerPlan(plan)), df_slice_width(innerPlan(plan))));
}

/* P1: building the expression PostgreSQL finishes an output column with */
typedef struct DfTailContext
{
	const DfTails *tails;
	List	  **values;
} DfTailContext;

/* 'node' with each leaf DataFusion computes replaced by a Var of its value */
static Node *
df_tail_mutator(Node *node, DfTailContext *c)
{
	if (node == NULL)
		return NULL;
	if (list_member_ptr(c->tails->leaves, node))
	{
		int			v = list_length(*c->values);

		*c->values = lappend(*c->values, node);
		return (Node *) makeVar(OUTER_VAR, v + 1, exprType(node), exprTypmod(node),
								exprCollation(node), 0);
	}
	return expression_tree_mutator(node, df_tail_mutator, c);
}

/*
 * The values DataFusion computes for the targetlist of 'body', and how each
 * of its columns is made of them (spec->col_value, col_tail).
 */
static List *
df_output_values(Plan *body, const DfTails *tails, DfSliceSpec *spec)
{
	List	   *values = NIL;
	ListCell   *lc;
	DfTailContext c = {tails, &values};

	spec->ncols = list_length(body->targetlist);
	spec->col_value = palloc(sizeof(int) * Max(spec->ncols, 1));
	spec->col_tail = NULL;
	foreach(lc, body->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		int			k = foreach_current_index(lc);

		if (tails != NULL && list_member_ptr(tails->tles, tle))
		{
			if (spec->col_tail == NULL)
				spec->col_tail = palloc0(sizeof(Expr *) * spec->ncols);
			spec->col_value[k] = -1;
			spec->col_tail[k] = (Expr *) df_tail_mutator((Node *) tle->expr, &c);
		}
		else
		{
			spec->col_value[k] = list_length(values);
			values = lappend(values, tle->expr);
		}
	}
	spec->nvalues = list_length(values);
	spec->value_types = palloc(sizeof(Oid) * Max(spec->nvalues, 1));
	foreach(lc, values)
		spec->value_types[foreach_current_index(lc)] = exprType(lfirst(lc));
	return values;
}

/*
 * S1: the slice's Sort keys, as output columns of the spec, and its Limit:
 * ,"sort":[{"col": c, "desc": b, "nulls_first": b}, ...],"limit":{...}
 */
static void
df_emit_sort_limit(DfBuilder *b, StringInfo out, Sort *sort, Limit *limit,
				   Plan *body, DfSliceSpec *spec)
{
	int			i;

	if (sort != NULL)
	{
		appendStringInfoString(out, ",\"sort\":[");
		for (i = 0; i < sort->numCols; i++)
		{
			AttrNumber	k = sort->sortColIdx[i];
			TargetEntry *tle = get_tle_by_resno(body->targetlist, k);
			bool		desc;
			int			c;

			if (tle == NULL || k > list_length(body->targetlist) ||
				!df_sort_direction(sort->sortOperators[i], exprType((Node *) tle->expr), &desc))
			{
				df_fail(b, "a sort key");
				return;
			}
			if (spec->col_value[k - 1] < 0)
			{
				df_fail(b, "a sort key PostgreSQL computes");
				return;
			}
			c = b->out_col[spec->col_value[k - 1]];
			if (spec->out_kinds[c] != DF_OUT_PLAIN ||
				spec->out_types[c] != exprType((Node *) tle->expr))
			{
				/* e.g. avg returning numeric, divided after DataFusion */
				df_fail(b, "a sort key computed outside DataFusion");
				return;
			}
			appendStringInfo(out, "%s{\"col\":%d,\"desc\":%s,\"nulls_first\":%s}",
							 i > 0 ? "," : "", c, desc ? "true" : "false",
							 sort->nullsFirst[i] ? "true" : "false");
		}
		appendStringInfoChar(out, ']');
	}
	if (limit != NULL)
	{
		int64		count,
					offset;

		if (!df_limit_value(limit->limitCount, &count) ||
			!df_limit_value(limit->limitOffset, &offset))
		{
			df_fail(b, "a LIMIT or OFFSET");
			return;
		}
		appendStringInfo(out, ",\"limit\":{\"skip\":" INT64_FORMAT, Max(offset, 0));
		if (count >= 0)
			appendStringInfo(out, ",\"fetch\":" INT64_FORMAT, count);
		appendStringInfoChar(out, '}');
	}
}

/*
 * D4: the order of Sort 'sort' below 'body', a GroupAggregate run hashed
 * under a Limit, as the spec's "sort" by output columns (the Limit applies
 * after it).
 */
static void
df_emit_resort_by(DfBuilder *b, StringInfo out, Sort *sort, Plan *body, DfSliceSpec *spec)
{
	int			i;

	appendStringInfoString(out, ",\"sort\":[");
	for (i = 0; i < sort->numCols; i++)
	{
		TargetEntry *tle = NULL;
		ListCell   *lc;
		bool		desc;
		int			c;

		foreach(lc, body->targetlist)
		{
			TargetEntry *t = lfirst_node(TargetEntry, lc);

			if (IsA(t->expr, Var) && ((Var *) t->expr)->varno == OUTER_VAR &&
				((Var *) t->expr)->varattno == sort->sortColIdx[i])
				tle = t;
		}
		if (tle == NULL || spec->col_value[tle->resno - 1] < 0 ||
			!df_sort_direction(sort->sortOperators[i], exprType((Node *) tle->expr), &desc))
		{
			df_fail(b, "a sort key of the GroupAggregate");
			return;
		}
		c = b->out_col[spec->col_value[tle->resno - 1]];
		appendStringInfo(out, "%s{\"col\":%d,\"desc\":%s,\"nulls_first\":%s}",
						 i > 0 ? "," : "", c, desc ? "true" : "false",
						 sort->nullsFirst[i] ? "true" : "false");
	}
	appendStringInfoChar(out, ']');
}

/*
 * D3: the order of sorted Motion 'motion' above 'body', a GroupAggregate
 * run hashed, as the spec's "sort" by output columns.
 */
static void
df_emit_resort(DfBuilder *b, StringInfo out, Motion *motion, Plan *body, DfSliceSpec *spec)
{
	int			i;

	appendStringInfoString(out, ",\"sort\":[");
	for (i = 0; i < motion->numSortCols; i++)
	{
		TargetEntry *mtle = get_tle_by_resno(motion->plan.targetlist, motion->sortColIdx[i]);
		AttrNumber	k = mtle && IsA(mtle->expr, Var) ? ((Var *) mtle->expr)->varattno : 0;
		TargetEntry *tle = k > 0 ? get_tle_by_resno(body->targetlist, k) : NULL;
		bool		desc;
		int			c;

		if (tle == NULL || spec->col_value[k - 1] < 0 ||
			!df_sort_direction(motion->sortOperators[i], exprType((Node *) tle->expr), &desc))
		{
			df_fail(b, "a sort key of the Motion");
			return;
		}
		c = b->out_col[spec->col_value[k - 1]];
		appendStringInfo(out, "%s{\"col\":%d,\"desc\":%s,\"nulls_first\":%s}",
						 i > 0 ? "," : "", c, desc ? "true" : "false",
						 motion->nullsFirst[i] ? "true" : "false");
	}
	appendStringInfoChar(out, ']');
}

/*
 * Build the JSON plan for the slice whose top node is 'root'.  Returns
 * false, with a reason, if something cannot be expressed.
 */
bool
df_translate_slice(Plan *root, const DfTails *tails, DfSliceSpec *spec,
				   char *reason, size_t reasonlen)
{
	DfBuilder	b;
	StringInfoData node,
				group,
				having,
				outputs,
				sortlimit,
				json;
	ListCell   *lc;
	int			i;
	Limit	   *limit = NULL;
	Sort	   *sort = NULL;
	List	   *values;

	/* S1: a Limit and a Sort at the top, over the rows of the rest */
	if (IsA(root, Limit))
	{
		limit = (Limit *) root;
		root = outerPlan(root);
	}
	if (root != NULL && IsA(root, Sort))
	{
		sort = (Sort *) root;
		root = outerPlan(root);
	}
	if (root == NULL || (limit && !df_passes_through((Plan *) limit)) ||
		(sort && !df_passes_through((Plan *) sort)))
	{
		snprintf(reason, reasonlen, "cannot translate a Limit or Sort");
		return false;
	}

	memset(&b, 0, sizeof(b));
	b.reason = reason;
	b.reasonlen = reasonlen;
	memset(spec, 0, sizeof(*spec));
	initStringInfo(&group);
	initStringInfo(&having);
	initStringInfo(&outputs);

	initStringInfo(&node);
	if (IsA(root, Agg))
	{
		b.agg = (Agg *) root;
		df_emit_node(&b, &node, outerPlan(root));
		appendStringInfoChar(&group, '[');
		for (i = 0; i < b.agg->numCols; i++)
		{
			if (i > 0)
				appendStringInfoChar(&group, ',');
			df_emit_grouped(&b, &group, outerPlan(root), b.agg->grpColIdx[i]);
		}
		appendStringInfoChar(&group, ']');
		df_emit_qual(&b, &having, root->qual, DF_LEVEL_AGG);
		values = df_output_values(root, tails, spec);
		df_emit_outputs(&b, &outputs, values, DF_LEVEL_AGG, spec, root);
	}
	else
	{
		df_emit_node(&b, &node, root);
		b.ctx = root;
		values = df_output_values(root, tails, spec);
		df_emit_outputs(&b, &outputs, values, DF_LEVEL_SCAN, spec, root);
	}
	initStringInfo(&sortlimit);
	if (!b.failed)
		df_emit_sort_limit(&b, &sortlimit, sort, limit, root, spec);
	if (!b.failed && tails != NULL && tails->resort != NULL)
	{
		if (sort != NULL || (limit != NULL && tails->resort_by == NULL))
			df_fail(&b, "a Sort below a sorted Motion's GroupAggregate");
		else if (tails->resort_by != NULL)
			df_emit_resort_by(&b, &sortlimit, tails->resort_by, root, spec);
		else
			df_emit_resort(&b, &sortlimit, tails->resort, root, spec);
	}
	if (b.failed)
		return false;

	/*
	 * Aggregate calls were collected while emitting the above.  Emit them
	 * before writing the scan columns: their arguments may add columns.
	 */
	{
		StringInfoData aggs;

		initStringInfo(&aggs);
		df_emit_agg_calls(&b, &aggs, b.agg, b.aggrefs, b.aggfns);
		if (b.failed)
			return false;

		/* inputs */
		initStringInfo(&json);
		appendStringInfoString(&json, "{\"inputs\":[");
		i = 0;
		foreach(lc, b.inputs)
		{
			DfInputDesc *in = lfirst(lc);
			ListCell   *lt,
					   *ls;
			int			k = 0;

			appendStringInfoString(&json, i++ > 0 ? ",{\"columns\":[" : "{\"columns\":[");
			forboth(lt, in->types, ls, in->scales)
				appendStringInfo(&json, "%s{\"type\":\"%s\"}", k++ > 0 ? "," : "",
								 df_tag(lfirst_oid(lt), lfirst_int(ls)));
			appendStringInfoChar(&json, ']');
			if (IsA(in->leaf, Motion))
			{
				/* The stream column behind each input column (M7b batches). */
				appendStringInfoString(&json, ",\"motion_columns\":[");
				k = 0;
				foreach(lt, in->attnos)
					appendStringInfo(&json, "%s%d", k++ > 0 ? "," : "", lfirst_int(lt) - 1);
				appendStringInfoChar(&json, ']');
			}
			appendStringInfoChar(&json, '}');
		}

		/* the plan, under the aggregate if there is one */
		if (b.agg)
		{
			StringInfoData a;

			initStringInfo(&a);
			appendStringInfo(&a, "{\"aggregate\":{\"input\":%s,\"group\":%s,"
							 "\"aggs\":[%s],\"having\":%s}}",
							 node.data, group.data, aggs.data, having.data);
			node = a;
		}
		spec->batch_rows = Min(DF_BATCH_MAX_ROWS,
							   Max(DF_BATCH_MIN_ROWS, DF_BATCH_BYTES / Max(df_slice_width(root), 1)));
		appendStringInfo(&json, "],\"plan\":%s,\"output\":%s%s,\"batch_rows\":%d}",
						 node.data, outputs.data, sortlimit.data, spec->batch_rows);
	}

	spec->json = json.data;
	spec->ninputs = list_length(b.inputs);
	spec->inputs = palloc0(sizeof(DfSliceInput) * spec->ninputs);
	i = 0;
	foreach(lc, b.inputs)
	{
		DfInputDesc *in = lfirst(lc);
		DfSliceInput *si = &spec->inputs[i++];
		ListCell   *la,
				   *lt;
		int			k = 0;

		si->leaf = in->leaf;
		si->ncols = list_length(in->attnos);
		si->attnos = palloc(sizeof(AttrNumber) * Max(si->ncols, 1));
		si->types = palloc(sizeof(Oid) * Max(si->ncols, 1));
		si->scales = palloc(sizeof(int16) * Max(si->ncols, 1));
		forboth(la, in->attnos, lt, in->types)
		{
			si->attnos[k] = (AttrNumber) lfirst_int(la);
			si->types[k] = lfirst_oid(lt);
			si->scales[k] = (int16) list_nth_int(in->scales, k);
			k++;
		}
	}
	return true;
}
