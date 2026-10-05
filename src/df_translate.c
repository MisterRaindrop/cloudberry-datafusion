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
		default:
			return NULL;
	}
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

static void
df_emit_const(DfBuilder *b, StringInfo out, Const *c)
{
	const char *tag = df_type_tag(c->consttype);

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
		}
		appendStringInfoString(out, "}}");
	}
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
df_input_column(DfBuilder *b, int j, AttrNumber attno, Oid type)
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
	return i;
}

/* Append a reference to that column of input 'j'. */
static void
df_emit_column(DfBuilder *b, StringInfo out, int j, AttrNumber attno, Oid type)
{
	int			k = df_input_column(b, j, attno, type);

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
					   exprType((Node *) tle->expr));
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

/* Is 'node' a partial avg, whose state goes out as sum and count? */
static bool
df_is_partial_avg(Node *node)
{
	char	   *name;

	if (!IsA(node, Aggref) || ((Aggref *) node)->aggsplit != AGGSPLIT_INITIAL_SERIAL)
		return false;
	name = get_func_name(((Aggref *) node)->aggfnoid);
	return name != NULL && strcmp(name, "avg") == 0;
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
								   var->varattno, var->vartype);
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

		case T_Aggref:
			if (level != DF_LEVEL_AGG)
			{
				df_fail(b, "an aggregate outside the aggregate node");
				return;
			}
			appendStringInfo(out, "{\"agg\":%d}", df_agg_ref(b, (Aggref *) node, NULL));
			return;

		default:
			df_fail(b, "an expression");
			return;
	}
}

/* A join key, cast to 'type' if it has another. */
static void
df_emit_key(DfBuilder *b, StringInfo out, Node *key, Oid type)
{
	const char *tag = df_type_tag(type);

	if (tag == NULL)
	{
		df_fail(b, "a join key");
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

					/* Keys of two types are compared in the wider one. */
					appendStringInfoString(out, first ? "[" : ",[");
					first = false;
					df_emit_key(b, out, inner, type);
					appendStringInfoChar(out, ',');
					df_emit_key(b, out, outer, type);
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
				DfSliceSpec *spec)
{
	ListCell   *lc;
	int			i = 0;

	spec->nout = list_length(tlist);
	foreach(lc, tlist)
		if (df_is_partial_avg((Node *) lfirst_node(TargetEntry, lc)->expr))
			spec->nout++;
	spec->out_types = palloc(sizeof(Oid) * Max(spec->nout, 1));
	appendStringInfoChar(out, '[');
	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Oid			type = exprType((Node *) tle->expr);
		const char *tag = df_type_tag(type);

		if (level == DF_LEVEL_AGG && df_is_partial_avg((Node *) tle->expr))
		{
			/* DataFusion's avg state: sum, then count (M7d) */
			Aggref	   *agg = (Aggref *) tle->expr;

			spec->out_types[i++] = FLOAT8OID;
			spec->out_types[i++] = INT8OID;
			appendStringInfo(out, "%s{\"expr\":{\"agg\":%d},\"type\":\"float8\"},"
							 "{\"expr\":{\"agg\":%d},\"type\":\"int8\"}",
							 i > 2 ? "," : "",
							 df_agg_ref(b, agg, "sum"), df_agg_ref(b, agg, "count"));
			continue;
		}
		if (tag == NULL)
		{
			df_fail(b, "an output column");
			return;
		}
		spec->out_types[i++] = type;
		if (i > 1)
			appendStringInfoChar(out, ',');
		appendStringInfoString(out, "{\"expr\":");
		df_emit(b, out, (Node *) tle->expr, level);
		appendStringInfo(out, ",\"type\":\"%s\"}", tag);
	}
	appendStringInfoChar(out, ']');
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
		df_emit_outputs(&b, &outputs, root->targetlist, DF_LEVEL_AGG, spec);
	}
	else
	{
		df_emit_node(&b, &node, root);
		b.ctx = root;
		df_emit_outputs(&b, &outputs, root->targetlist, DF_LEVEL_SCAN, spec);
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
				df_emit_column(&b, &aggs, df_input_of(&b, child), pos + 1, FLOAT8OID);
				appendStringInfoString(&aggs, ",\"arg2\":");
				df_emit_column(&b, &aggs, df_input_of(&b, child), pos + 2, INT8OID);
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
			ListCell   *lt;
			int			k = 0;

			appendStringInfoString(&json, i++ > 0 ? ",{\"columns\":[" : "{\"columns\":[");
			foreach(lt, in->types)
				appendStringInfo(&json, "%s{\"type\":\"%s\"}", k++ > 0 ? "," : "",
								 df_type_tag(lfirst_oid(lt)));
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
		appendStringInfo(&json, "],\"plan\":%s,\"output\":%s}", node.data, outputs.data);
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
		forboth(la, in->attnos, lt, in->types)
		{
			si->attnos[k] = (AttrNumber) lfirst_int(la);
			si->types[k] = lfirst_oid(lt);
			k++;
		}
	}
	return true;
}
