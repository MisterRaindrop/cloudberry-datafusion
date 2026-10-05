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
	DF_LEVEL_SCAN,				/* expressions over the scanned table */
	DF_LEVEL_AGG,				/* expressions over the aggregate's output */
	DF_LEVEL_AGGARG				/* aggregate arguments: over the scan */
} DfLevel;

typedef struct DfBuilder
{
	Index		scanrelid;		/* 0 when the input is a Motion */
	Plan	   *scan;			/* the Seq Scan or the receiving Motion */
	Agg		   *agg;
	List	   *attnos;			/* int: table column (or Motion output
								 * column) of each input column */
	List	   *types;			/* oid: type of each scan column */
	List	   *aggrefs;		/* distinct Aggrefs, in first-use order */
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
				appendStringInfo(out, "%d", DatumGetInt32(c->constvalue));
				break;
			case INT8OID:
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

/* Index of the input column for column 'attno', adding it if new. */
static int
df_input_column(DfBuilder *b, AttrNumber attno, Oid type)
{
	ListCell   *lc;
	int			i = 0;

	foreach(lc, b->attnos)
	{
		if (lfirst_int(lc) == attno)
			return i;
		i++;
	}
	b->attnos = lappend_int(b->attnos, attno);
	b->types = lappend_oid(b->types, type);
	return i;
}

static int
df_scan_column(DfBuilder *b, Var *var)
{
	return df_input_column(b, var->varattno, var->vartype);
}

/*
 * Output column 'resno' of the aggregate's child, as an input expression:
 * the scan's targetlist entry, or the Motion's column itself.
 */
static void
df_emit_child_column(DfBuilder *b, StringInfo out, AttrNumber resno)
{
	TargetEntry *tle = get_tle_by_resno(b->scan->targetlist, resno);

	if (tle == NULL)
		df_fail(b, "a reference to the child's output");
	else if (IsA(b->scan, Motion))
		appendStringInfo(out, "{\"col\":%d}",
						 df_input_column(b, resno, exprType((Node *) tle->expr)));
	else
		df_emit(b, out, (Node *) tle->expr, DF_LEVEL_SCAN);
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

				if (level == DF_LEVEL_SCAN && var->varno == b->scanrelid)
					appendStringInfo(out, "{\"col\":%d}", df_scan_column(b, var));
				else if (level == DF_LEVEL_AGGARG && var->varno == OUTER_VAR)
					df_emit_child_column(b, out, var->varattno);
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
			{
				ListCell   *lc;
				int			i = 0;

				if (level != DF_LEVEL_AGG)
				{
					df_fail(b, "an aggregate outside the aggregate node");
					return;
				}
				foreach(lc, b->aggrefs)
				{
					if (equal(lfirst(lc), node))
						break;
					i++;
				}
				if (lc == NULL)
					b->aggrefs = lappend(b->aggrefs, node);
				appendStringInfo(out, "{\"agg\":%d}", i);
				return;
			}

		default:
			df_fail(b, "an expression");
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
	spec->out_types = palloc(sizeof(Oid) * Max(spec->nout, 1));
	appendStringInfoChar(out, '[');
	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Oid			type = exprType((Node *) tle->expr);
		const char *tag = df_type_tag(type);

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
	StringInfoData filter,
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
	initStringInfo(&filter);
	initStringInfo(&group);
	initStringInfo(&having);
	initStringInfo(&outputs);

	if (IsA(root, Agg))
	{
		b.agg = (Agg *) root;
		b.scan = outerPlan(root);
	}
	else
		b.scan = root;
	if (b.scan != NULL && IsA(b.scan, SeqScan))
		b.scanrelid = ((Scan *) b.scan)->scanrelid;
	else if (!(b.agg && b.scan != NULL && IsA(b.scan, Motion)))
	{
		snprintf(reason, reasonlen, "cannot translate this slice shape");
		return false;
	}

	df_emit_qual(&b, &filter, b.scan->qual, DF_LEVEL_SCAN);
	if (b.agg)
	{
		appendStringInfoChar(&group, '[');
		for (i = 0; i < b.agg->numCols; i++)
		{
			if (i > 0)
				appendStringInfoChar(&group, ',');
			df_emit_child_column(&b, &group, b.agg->grpColIdx[i]);
		}
		appendStringInfoChar(&group, ']');
		df_emit_qual(&b, &having, root->qual, DF_LEVEL_AGG);
		df_emit_outputs(&b, &outputs, root->targetlist, DF_LEVEL_AGG, spec);
	}
	else
		df_emit_outputs(&b, &outputs, root->targetlist, DF_LEVEL_SCAN, spec);

	/*
	 * Aggregate calls were collected while emitting the above.  Emit them
	 * before writing the scan columns: their arguments may add columns.
	 */
	{
		StringInfoData aggs;

		initStringInfo(&aggs);
		i = 0;
		foreach(lc, b.aggrefs)
		{
			Aggref	   *agg = lfirst_node(Aggref, lc);
			char	   *name = get_func_name(agg->aggfnoid);
			bool		combine = (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL);

			if (i++ > 0)
				appendStringInfoChar(&aggs, ',');
			appendStringInfo(&aggs, "{\"fn\":\"%s%s\",\"arg\":", name,
							 combine && strcmp(name, "count") == 0 ? "_merge" : "");

			/*
			 * A combining aggregate keeps aggstar from the original call
			 * (count(*)), but its one argument is the partial state.
			 */
			if ((agg->aggstar && !combine) || agg->args == NIL)
				appendStringInfoString(&aggs, "null");
			else
				df_emit(&b, &aggs,
						(Node *) linitial_node(TargetEntry, agg->args)->expr,
						DF_LEVEL_AGGARG);
			appendStringInfoChar(&aggs, '}');
		}
		if (b.failed)
			return false;

		initStringInfo(&json);
		appendStringInfoString(&json, "{\"scan\":{\"columns\":[");
		i = 0;
		foreach(lc, b.types)
		{
			if (i++ > 0)
				appendStringInfoChar(&json, ',');
			appendStringInfo(&json, "{\"type\":\"%s\"}", df_type_tag(lfirst_oid(lc)));
		}
		appendStringInfoChar(&json, ']');
		if (IsA(b.scan, Motion))
		{
			/* The Motion column behind each input column (M7b batches). */
			appendStringInfoString(&json, ",\"motion_columns\":[");
			i = 0;
			foreach(lc, b.attnos)
				appendStringInfo(&json, "%s%d", i++ > 0 ? "," : "", lfirst_int(lc) - 1);
			appendStringInfoChar(&json, ']');
		}
		appendStringInfo(&json, "},\"filter\":%s,\"aggregate\":", filter.data);
		if (b.agg)
			appendStringInfo(&json, "{\"group\":%s,\"aggs\":[%s]},\"having\":%s",
							 group.data, aggs.data, having.data);
		else
			appendStringInfoString(&json, "null,\"having\":null");
		appendStringInfo(&json, ",\"output\":%s}", outputs.data);
	}

	spec->json = json.data;
	spec->scanrelid = b.scanrelid;
	spec->nscan = list_length(b.attnos);
	spec->scan_attnos = palloc(sizeof(AttrNumber) * Max(spec->nscan, 1));
	spec->scan_types = palloc(sizeof(Oid) * Max(spec->nscan, 1));
	i = 0;
	foreach(lc, b.attnos)
		spec->scan_attnos[i++] = (AttrNumber) lfirst_int(lc);
	i = 0;
	foreach(lc, b.types)
		spec->scan_types[i++] = lfirst_oid(lc);
	return true;
}
