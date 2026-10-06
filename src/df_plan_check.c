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
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "nodes/bitmapset.h"
#include "optimizer/walkers.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "df_executor.h"

/*
 * From utils/pg_locale.h, which needs ICU's headers when the server was
 * built with ICU.
 */
extern bool lc_collate_is_c(Oid collation);

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
	bool		locale_dependent;	/* the verdict depends on this node's locale */
	bool		failed;
	char	   *reason;
	size_t		reasonlen;
} DfCheckContext;

static void df_reject(DfCheckContext *cxt, const char *fmt,...) pg_attribute_printf(2, 3);
static bool df_check_expr(Node *node, DfCheckContext *cxt);
static void df_check_plan(Plan *plan, DfCheckContext *cxt, Bitmapset *needed,
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
	{"=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "~~", "!~~", NULL};
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
				return cxt->failed;
			}

		case T_Const:
			{
				Const	   *c = (Const *) node;

				if (!df_type_supported(c->consttype))
					df_reject(cxt, "constant of type %s", format_type_be(c->consttype));
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
					if ((df_type_is_datetime(ltype) || df_type_is_datetime(rtype)) &&
						(op->opresulttype != BOOLOID || ltype != rtype))
						df_reject(cxt, "operator %s on %s and %s", name,
								  format_type_be(ltype), format_type_be(rtype));
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
				else if (strcmp(name, "avg") == 0 && agg->aggsplit != AGGSPLIT_SIMPLE)
				{
					/*
					 * M7d: through a batch Motion, a split avg of float4 or
					 * float8 passes DataFusion's state (sum and count)
					 * instead of PostgreSQL's array.
					 */
					Oid			argtype = list_length(agg->aggargtypes) == 1 ?
						linitial_oid(agg->aggargtypes) : InvalidOid;
					bool		allowed = agg->aggsplit == AGGSPLIT_INITIAL_SERIAL ?
						cxt->partial_states : cxt->final_states;

					if (argtype != FLOAT4OID && argtype != FLOAT8OID)
						df_reject(cxt, "split aggregate avg of %s", format_type_be(argtype));
					else if (!allowed)
						df_reject(cxt, "%s aggregate avg without batch Motions",
								  agg->aggsplit == AGGSPLIT_INITIAL_SERIAL ? "partial" : "combining");
					else if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
					{
						/* its argument is the state column of the Motion below */
						if (list_length(agg->args) != 1 ||
							!IsA(linitial_node(TargetEntry, agg->args)->expr, Var))
							df_reject(cxt, "combining aggregate avg over an expression");
						return cxt->failed;
					}
				}
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
				else if (!df_type_supported(agg->aggtype) &&
						 !(strcmp(name, "avg") == 0 && agg->aggsplit == AGGSPLIT_INITIAL_SERIAL))
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
				char	   *name = get_func_name(((FuncExpr *) node)->funcid);

				df_reject(cxt, "function %s()", name ? name : "?");
				return true;
			}

		case T_SubPlan:
		case T_AlternativeSubPlan:
			df_reject(cxt, "subquery");
			return true;

		case T_Param:
			df_reject(cxt, "query parameter");
			return true;

		case T_CaseExpr:
			df_reject(cxt, "CASE expression");
			return true;

		case T_ScalarArrayOpExpr:
			df_reject(cxt, "IN or ANY list");
			return true;

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
			df_reject(cxt, "IS DISTINCT FROM");
			return true;

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
static void
df_check_targetlist(List *targetlist, Bitmapset *needed, DfCheckContext *cxt)
{
	ListCell   *lc;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (needed == NULL || bms_is_member(tle->resno, needed))
			df_check_expr((Node *) tle->expr, cxt);
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

static void
df_check_plan(Plan *plan, DfCheckContext *cxt, Bitmapset *needed,
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

						if (bms_is_member(tle->resno, needed) && !df_type_supported(type) &&
							!(bms_is_member(motion->motionID, cxt->batches) &&
							  df_motion_state_column(motion, tle->resno)))
						{
							df_reject(cxt, "receives a column of type %s", format_type_be(type));
							return;
						}
					}
					return;
				}

				/*
				 * The sending Motion stays on PostgreSQL and pulls DataFusion's
				 * rows.  A sorted send needs sorted input, and the remaining
				 * types belong to parallel or DML plans.
				 */
				if (motion->sendSorted)
				{
					df_reject(cxt, "sorted %s", df_plan_name(plan));
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
				 Bitmapset *batches, char *reason, size_t reasonlen,
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
		df_check_plan(root, &cxt, NULL, root_is_sender);

	if (locale_dependent)
		*locale_dependent = cxt.locale_dependent;
	return !cxt.failed;
}

bool
df_check_slice(PlannedStmt *stmt, Plan *root, bool root_is_sender,
			   char *reason, size_t reasonlen)
{
	return df_check_slice_b(stmt, root, root_is_sender, df_batch_motions(stmt),
							reason, reasonlen, NULL);
}

/*
 * The top plan node of the slice this process executes: the Motion it sends
 * through, or the plan's top node for the top slice.
 */
Plan *
df_local_slice_root(QueryDesc *queryDesc, int *slice_index, bool *is_sender)
{
	int			idx = LocallyExecutingSliceIndex(queryDesc->estate);
	Motion	   *sender = findSenderMotion(queryDesc->plannedstmt, idx);

	*slice_index = idx;
	*is_sender = (sender != NULL);
	return sender ? (Plan *) sender : queryDesc->plannedstmt->planTree;
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
		df_check_slice_b(stmt, root, sender != NULL, batches, reason, sizeof(reason),
						 &locale_dependent) &&
		!locale_dependent &&
		df_translate_slice(compute, &spec, reason, sizeof(reason)))
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
 * Does column 'resno' of 'motion' carry a partial avg's state?  Through a
 * batch Motion that state is DataFusion's, two columns in the stream (M7d).
 */
bool
df_motion_state_column(Motion *motion, AttrNumber resno)
{
	TargetEntry *tle = get_tle_by_resno(motion->plan.targetlist, resno);
	Plan	   *child = outerPlan(motion);
	Node	   *expr;
	char	   *name;

	if (tle == NULL || child == NULL)
		return false;
	expr = (Node *) tle->expr;
	if (IsA(expr, Var) && ((Var *) expr)->varno == OUTER_VAR)
	{
		tle = get_tle_by_resno(child->targetlist, ((Var *) expr)->varattno);
		if (tle == NULL)
			return false;
		expr = (Node *) tle->expr;
	}
	if (!IsA(expr, Aggref) || ((Aggref *) expr)->aggsplit != AGGSPLIT_INITIAL_SERIAL)
		return false;
	name = get_func_name(((Aggref *) expr)->aggfnoid);
	return name != NULL && strcmp(name, "avg") == 0;
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
		pos += df_motion_state_column(motion, r) ? 2 : 1;
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

		if (df_check_slice(stmt, cxt.roots[i].root, cxt.roots[i].is_sender,
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
