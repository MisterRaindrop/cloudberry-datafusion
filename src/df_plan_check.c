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
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "executor/execUtils.h"
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

typedef struct DfCheckContext
{
	PlannedStmt *stmt;
	bool		allow_aggref;	/* inside an Agg node's targetlist or qual */
	bool		agg_input;		/* checking the child of an Agg node */
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
			return true;
		default:
			return false;
	}
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
	{"=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", NULL};
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
				else if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL &&
						 (strcmp(name, "avg") == 0 || agg->aggtranstype != agg->aggtype))
					/*
					 * Combining count, sum and min/max adds up or compares
					 * plain values of the result type; other transition
					 * states (avg's array, serialized states) stay on
					 * PostgreSQL.
					 */
					df_reject(cxt, "combining stage of aggregate %s", name);
				else if (agg->aggsplit == AGGSPLIT_INITIAL_SERIAL &&
						 strcmp(name, "avg") == 0)
					/* its transition state is an array */
					df_reject(cxt, "partial aggregate avg");
				else if (agg->aggsplit == AGGSPLIT_INITIAL_SERIAL &&
						 agg->aggtype == BYTEAOID)
					df_reject(cxt, "partial aggregate %s with a serialized transition state", name);
				else if (!df_type_supported(agg->aggtype))
					df_reject(cxt, "aggregate %s returning %s", name,
							  format_type_be(agg->aggtype));
				else if (strcmp(name, "sum") == 0 && agg->aggtype == FLOAT4OID)
					/* PostgreSQL adds real values in single precision. */
					df_reject(cxt, "sum of real values");
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
				}
				cxt->allow_aggref = true;
				df_check_targetlist(plan->targetlist, needed, cxt);
				df_check_expr_list(plan->qual, cxt);
				cxt->allow_aggref = false;

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
					if (!cxt->agg_input)
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

						if (bms_is_member(tle->resno, needed) && !df_type_supported(type))
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
				df_check_plan(outerPlan(plan), cxt, NULL, false);
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
bool
df_check_slice(PlannedStmt *stmt, Plan *root, bool root_is_sender,
			   char *reason, size_t reasonlen)
{
	DfCheckContext cxt;

	cxt.stmt = stmt;
	cxt.allow_aggref = false;
	cxt.agg_input = false;
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

	return !cxt.failed;
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
			appendStringInfo(out, "DataFusion: slice %d eligible\n", cxt.roots[i].index);
		else
			appendStringInfo(out, "DataFusion: slice %d not eligible: %s\n",
							 cxt.roots[i].index, reason);
	}
	pfree(cxt.roots);
}
