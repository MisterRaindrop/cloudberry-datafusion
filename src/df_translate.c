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

#include "catalog/pg_type_d.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

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

typedef struct DfBuilder
{
	Plan	   *ctx;			/* the node whose expressions are emitted */
	Agg		   *agg;
	List	   *inputs;			/* DfInputDesc, in input order */
	List	   *aggrefs;		/* distinct aggregate calls, in first-use order */
	List	   *aggfns;			/* the function of each: NULL for the
								 * Aggref's own, or "sum" / "count" for the
								 * parts of a partial avg's state */
	bool		failed;
	char	   *reason;
	size_t		reasonlen;
} DfBuilder;

static void df_emit(DfBuilder *b, StringInfo out, Node *node, DfLevel level);
static void df_emit_numeric_const(DfBuilder *b, StringInfo out, Const *c, int scale);

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
		case TEXTOID:
		case VARCHAROID:
			return "text";
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

	if (!df_numeric_ps(ctx, expr, &p, &s))
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

/* The DataFusion state a partial aggregate sends instead of its own. */
static DfAggState
df_partial_state(Node *node)
{
	if (!IsA(node, Aggref) || ((Aggref *) node)->aggsplit != AGGSPLIT_INITIAL_SERIAL)
		return DF_AGG_PLAIN;
	return df_agg_state((Aggref *) node);
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

				if (level == DF_LEVEL_SCAN && var->varno == OUTER_VAR)
					df_emit_output_of(b, out, outerPlan(b->ctx), var->varattno);
				else if (level == DF_LEVEL_SCAN && var->varno == INNER_VAR)
					df_emit_output_of(b, out, innerPlan(b->ctx), var->varattno);
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
					if (k < b->agg->numCols)
						appendStringInfo(out, "{\"group\":%d}", k);
					else
						df_fail(b, "an ungrouped column above an aggregate");
				}
				else
					df_fail(b, "a column reference");
				return;
			}

		case T_Const:
			df_emit_const(b, out, (Const *) node);
			return;

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				char	   *name = get_opname(op->opno);
				const char *tag = df_type_tag(op->opresulttype);

				if (name == NULL || tag == NULL)
				{
					df_fail(b, "an operator");
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
		df_emit(b, out, key, DF_LEVEL_SCAN);
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
df_emit_node(DfBuilder *b, StringInfo out, Plan *plan)
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
			df_emit_node(b, out, outerPlan(plan));
			return;

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
					default:
						df_fail(b, "this kind of join");
						return;
				}
				if (plan->qual != NIL)
					appendStringInfoString(out, "{\"filter\":{\"input\":");
				appendStringInfo(out, "{\"join\":{\"type\":\"%s\",\"left\":", type);
				df_emit_node(b, out, innerPlan(plan));
				appendStringInfoString(out, ",\"right\":");
				df_emit_node(b, out, outerPlan(plan));
				appendStringInfoString(out, ",\"on\":[");
				b->ctx = plan;
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

static void
df_emit_outputs(DfBuilder *b, StringInfo out, List *tlist, DfLevel level,
				DfSliceSpec *spec, Plan *ctx)
{
	ListCell   *lc;
	int			i = 0;

	spec->nout = 0;
	foreach(lc, tlist)
	{
		Node	   *expr = (Node *) lfirst_node(TargetEntry, lc)->expr;

		if (level == DF_LEVEL_AGG && df_partial_state(expr) != DF_AGG_PLAIN)
			spec->nout += df_agg_state_ncols(df_partial_state(expr));
		else if (level == DF_LEVEL_AGG && IsA(expr, Aggref) &&
				 (df_agg_state((Aggref *) expr) == DF_AGG_AVG_INT ||
				  df_agg_state((Aggref *) expr) == DF_AGG_AVG_NUMERIC))
			spec->nout += 2;	/* numeric sum and count */
		else
			spec->nout++;
	}
	spec->out_types = palloc(sizeof(Oid) * Max(spec->nout, 1));
	spec->out_kinds = palloc0(sizeof(uint8) * Max(spec->nout, 1));
	spec->out_scales = palloc0(sizeof(int16) * Max(spec->nout, 1));
	appendStringInfoChar(out, '[');
	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Oid			type = exprType((Node *) tle->expr);
		const char *tag = df_type_tag(type);
		DfAggState	state = level == DF_LEVEL_AGG ?
			df_partial_state((Node *) tle->expr) : DF_AGG_PLAIN;

		if (state != DF_AGG_PLAIN)
		{
			/* DataFusion's state: sum (float8 or numeric), then count (M7d, N1, N2) */
			Aggref	   *agg = (Aggref *) tle->expr;
			bool		fsum = state == DF_AGG_AVG_FLOAT;
			bool		dsum = state == DF_AGG_SUM_NUMERIC || state == DF_AGG_AVG_NUMERIC;
			int			scale = df_scale_of(ctx, (Node *) agg);

			spec->out_scales[i] = (int16) scale;
			spec->out_types[i++] = fsum ? FLOAT8OID : NUMERICOID;
			appendStringInfo(out, "%s{\"expr\":{\"agg\":%d},\"type\":\"%s\"}",
							 i > 1 ? "," : "",
							 df_agg_ref(b, agg, fsum ? "sum" : dsum ? "sum_decimal" : "sum_numeric"),
							 fsum ? "float8" : df_tag(NUMERICOID, scale));
			if (df_agg_state_ncols(state) == 2)
			{
				spec->out_types[i++] = INT8OID;
				appendStringInfo(out, ",{\"expr\":{\"agg\":%d},\"type\":\"int8\"}",
								 df_agg_ref(b, agg, "count"));
			}
			continue;
		}
		if (level == DF_LEVEL_AGG && IsA(tle->expr, Aggref) &&
			(df_agg_state((Aggref *) tle->expr) == DF_AGG_AVG_INT ||
			 df_agg_state((Aggref *) tle->expr) == DF_AGG_AVG_NUMERIC))
		{
			/* avg = numeric sum / count, divided on the C side (N1, N2) */
			Aggref	   *agg = (Aggref *) tle->expr;
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
			spec->out_scales[i] = (int16) df_scale_of(ctx, (Node *) tle->expr);
			tag = df_tag(type, spec->out_scales[i]);
		}
		spec->out_types[i++] = type;
		if (i > 1)
			appendStringInfoChar(out, ',');
		appendStringInfoString(out, "{\"expr\":");
		df_emit(b, out, (Node *) tle->expr, level);
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

/*
 * Build the JSON plan for the slice whose top node is 'root'.  Returns
 * false, with a reason, if something cannot be expressed.
 */
bool
df_translate_slice(Plan *root, DfSliceSpec *spec, char *reason, size_t reasonlen)
{
	DfBuilder	b;
	StringInfoData node,
				group,
				having,
				outputs,
				json;
	ListCell   *lc;
	int			i;

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
			df_emit_output_of(&b, &group, outerPlan(root), b.agg->grpColIdx[i]);
		}
		appendStringInfoChar(&group, ']');
		df_emit_qual(&b, &having, root->qual, DF_LEVEL_AGG);
		df_emit_outputs(&b, &outputs, root->targetlist, DF_LEVEL_AGG, spec, root);
	}
	else
	{
		df_emit_node(&b, &node, root);
		b.ctx = root;
		df_emit_outputs(&b, &outputs, root->targetlist, DF_LEVEL_SCAN, spec, root);
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
		ListCell   *lf;

		i = 0;
		forboth(lc, b.aggrefs, lf, b.aggfns)
		{
			Aggref	   *agg = lfirst_node(Aggref, lc);
			const char *fn = lfirst(lf);
			char	   *name = get_func_name(agg->aggfnoid);
			bool		combine = (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL);

			if (i++ > 0)
				appendStringInfoChar(&aggs, ',');
			if (combine && df_agg_state(agg) != DF_AGG_AVG_FLOAT &&
				df_agg_state(agg) != DF_AGG_PLAIN)
			{
				/* sum(int8), avg(int): the stream's numeric sum and count columns */
				Node	   *arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
				Plan	   *child = outerPlan(b.agg);
				bool		count = fn != NULL && strcmp(fn, "merge_count") == 0;
				int			pos;

				if (!IsA(child, Motion) || !IsA(arg, Var))
				{
					df_fail(&b, "a combining aggregate");
					break;
				}
				pos = df_motion_stream_column((Motion *) child, ((Var *) arg)->varattno);
				appendStringInfo(&aggs, "{\"fn\":\"%s\",\"arg\":", count ? "sum" : "sum_decimal");
				df_emit_column(&b, &aggs, df_input_of(&b, child), pos + (count ? 2 : 1),
							   count ? INT8OID : NUMERICOID,
							   count ? 0 : df_scale_of((Plan *) b.agg, arg));
				appendStringInfoChar(&aggs, '}');
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

				Plan	   *child = outerPlan(b.agg);

				if (!IsA(child, Motion) || !IsA(arg, Var))
				{
					df_fail(&b, "a combining avg");
					break;
				}
				pos = df_motion_stream_column((Motion *) child, ((Var *) arg)->varattno);
				appendStringInfoString(&aggs, "{\"fn\":\"avg_merge\",\"arg\":");
				df_emit_column(&b, &aggs, df_input_of(&b, child), pos + 1, FLOAT8OID, 0);
				appendStringInfoString(&aggs, ",\"arg2\":");
				df_emit_column(&b, &aggs, df_input_of(&b, child), pos + 2, INT8OID, 0);
				appendStringInfoChar(&aggs, '}');
				continue;
			}
			appendStringInfo(&aggs, "{\"fn\":\"%s%s\",\"arg\":", fn ? fn : name,
							 !fn && combine && strcmp(name, "count") == 0 ? "_merge" : "");

			/*
			 * A combining aggregate keeps aggstar from the original call
			 * (count(*)), but its one argument is the partial state.
			 */
			if ((agg->aggstar && !combine) || agg->args == NIL)
				appendStringInfoString(&aggs, "null");
			else
			{
				/* arguments are expressions of the aggregate over its child */
				b.ctx = (Plan *) b.agg;
				df_emit(&b, &aggs,
						(Node *) linitial_node(TargetEntry, agg->args)->expr,
						DF_LEVEL_SCAN);
			}
			appendStringInfoChar(&aggs, '}');
		}
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
		appendStringInfo(&json, "],\"plan\":%s,\"output\":%s,\"batch_rows\":%d}",
						 node.data, outputs.data, spec->batch_rows);
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
