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
 * vx_hooks.c
 *	  Executor and EXPLAIN hooks: where DataFusion takes over a slice.
 *
 * ExecutorStart runs the standard start, then asks vx_check_slice whether
 * this process's slice qualifies.  A qualifying slice is recorded against
 * its QueryDesc, in the query's memory context with a reset callback, so
 * the record disappears with the query however it ends.  ExecutorRun then
 * routes recorded slices to DataFusion.
 *
 * A taken-over slice runs in DataFusion (vx_exec.c): its top PlanState's
 * ExecProcNode is replaced, so the rest of the executor is unchanged.
 * vec_debug_takeovers() counts the ExecutorRun calls that reached a
 * taken-over slice.
 *
 * With vec.mode = explain or on, text-format EXPLAIN ends with one
 * line per slice saying whether DataFusion can run it, and why not.
 *
 * src/vx_hooks.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/explain.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"

#include "vx_executor.h"

PG_FUNCTION_INFO_V1(vec_debug_takeovers);

int			vx_mode = VX_MODE_OFF;

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExplainOneQuery_hook_type prev_ExplainOneQuery = NULL;

/* A slice DataFusion runs, recorded at ExecutorStart. */
typedef struct VxTakeover
{
	QueryDesc  *queryDesc;
	int			slice_index;
	struct VxTakeover *next;
	MemoryContextCallback forget;
} VxTakeover;

static VxTakeover *vx_takeovers = NULL;

/* Number of ExecutorRun calls routed to DataFusion in this backend. */
static int64 vx_takeover_runs = 0;

static void
vx_takeover_forget(void *arg)
{
	VxTakeover **link;

	for (link = &vx_takeovers; *link != NULL; link = &(*link)->next)
	{
		if (*link == (VxTakeover *) arg)
		{
			*link = (*link)->next;
			return;
		}
	}
}

static void
vx_takeover_record(QueryDesc *queryDesc, int slice_index)
{
	MemoryContext cxt = queryDesc->estate->es_query_cxt;
	VxTakeover *t = MemoryContextAllocZero(cxt, sizeof(VxTakeover));

	t->queryDesc = queryDesc;
	t->slice_index = slice_index;
	t->forget.func = vx_takeover_forget;
	t->forget.arg = t;
	MemoryContextRegisterResetCallback(cxt, &t->forget);

	t->next = vx_takeovers;
	vx_takeovers = t;
}

static VxTakeover *
vx_takeover_find(QueryDesc *queryDesc)
{
	VxTakeover *t;

	for (t = vx_takeovers; t != NULL; t = t->next)
		if (t->queryDesc == queryDesc)
			return t;
	return NULL;
}

static bool vx_attach_points(QueryDesc *queryDesc, PlanState *top, int slice_index,
							 List *points, bool record);

/*
 * T2: run in DataFusion the subtrees below the PostgreSQL nodes at the top
 * of slice 'slice_index' (whose top PlanState is 'top') that can run there.
 * Returns whether any does.
 */
static bool
vx_attach_below(QueryDesc *queryDesc, PlanState *top, int slice_index)
{
	PlannedStmt *stmt = queryDesc->plannedstmt;

	return vx_attach_points(queryDesc, top, slice_index,
							vx_slice_attach_points(stmt, top->plan, vx_batch_motions(stmt)),
							true);
}

/*
 * T2: below the Shared Scan producers that PostgreSQL runs first in slice
 * 'slice_index', which DataFusion runs from its top PlanState 'top'
 * (vx_exec_prerun): their inputs.
 */
static void
vx_attach_producers(QueryDesc *queryDesc, PlanState *top, int slice_index)
{
	PlannedStmt *stmt = queryDesc->plannedstmt;
	List	   *points = NIL;
	ListCell   *lc;

	foreach(lc, vx_slice_producers(top->plan))
		points = list_concat(points, vx_slice_attach_points(stmt, lfirst(lc),
															vx_batch_motions(stmt)));
	if (points != NIL)
		vx_attach_points(queryDesc, top, slice_index, points, false);
}

static bool
vx_attach_points(QueryDesc *queryDesc, PlanState *top, int slice_index, List *points,
				 bool record)
{
	ListCell   *lc;
	int			attached = 0;

	foreach(lc, points)
	{
		VxAttach   *a = lfirst(lc);
		PlanState  *ps = vx_exec_find_state(top, a->plan);
		char		reason[256];

		if (ps != NULL && vx_exec_attach(queryDesc, ps, NULL, &a->tails, reason, sizeof(reason)))
			attached++;
		else
			elog(DEBUG1, "vec_executor: a subtree of slice %d stays on the PostgreSQL executor: %s",
				 slice_index, ps ? reason : "no executor state");
	}
	if (attached > 0)
	{
		elog(DEBUG1, "vec_executor: slice %d runs %d subtrees below its top", slice_index, attached);
		if (record)
			vx_takeover_record(queryDesc, slice_index);
	}
	return attached > 0;
}

static void
vx_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	/* before the plan is dispatched */
	vx_sync_cluster_collation();
	vx_batch_motions_forget();
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	/*
	 * Backward and mark/restore scans need a rewindable result, which the
	 * DataFusion stream is not.
	 */
	if (vx_mode == VX_MODE_ON &&
		(eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0 &&
		queryDesc->estate != NULL && queryDesc->planstate != NULL)
	{
		char		reason[256];
		int			slice_index;
		bool		is_sender;
		VxTails		tails;
		Plan	   *root = vx_local_slice_root(queryDesc, &slice_index, &is_sender);

		/*
		 * The top slice is attached at its top node.  Any other slice starts
		 * with the Motion it sends through: that Motion stays on PostgreSQL,
		 * and DataFusion runs the subtree it pulls rows from.
		 */
		PlanState  *attach = queryDesc->planstate;
		MotionState *send = NULL;

		if (is_sender && IsA(attach, MotionState))
		{
			/* M7b: a Gather to another DataFusion slice carries batches. */
			if (vx_motion_sends_batches(queryDesc->plannedstmt, (Motion *) root))
				send = (MotionState *) attach;
			attach = outerPlanState(attach);
		}
		else if (is_sender)
			attach = NULL;

		if (vx_check_slice(queryDesc->plannedstmt, root, is_sender, &tails,
						   reason, sizeof(reason)) &&
			attach != NULL &&
			vx_exec_attach(queryDesc, attach, send, &tails, reason, sizeof(reason)))
		{
			vx_takeover_record(queryDesc, slice_index);
			vx_attach_producers(queryDesc, attach, slice_index);
		}
		else if (send == NULL && attach != NULL &&
				 vx_attach_below(queryDesc, attach, slice_index))
			 /* T2: subtrees below PostgreSQL's nodes */ ;
		else if (send != NULL)
			/* the receiver expects batches; tuples would only fail there */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("vec_executor: slice %d cannot send the batches its receiver expects: %s",
							slice_index, reason)));
		else
			elog(DEBUG1, "vec_executor: slice %d stays on the PostgreSQL executor: %s",
				 slice_index, reason);
	}
}

static void
vx_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
			   uint64 count, bool execute_once)
{
	VxTakeover *t = vx_takeover_find(queryDesc);

	if (t != NULL)
	{
		vx_takeover_runs++;
		elog(DEBUG1, "vec_executor: running slice %d", t->slice_index);
	}

	if (prev_ExecutorRun)
		prev_ExecutorRun(queryDesc, direction, count, execute_once);
	else
		standard_ExecutorRun(queryDesc, direction, count, execute_once);
}

/*
 * Same as the standard ExplainOneQuery path for a planned statement
 * (explain.c), plus the per-slice DataFusion lines at the end.  The lines
 * are added for text format only: in the structured formats ExplainOnePlan
 * has already closed its group, so nothing can be appended validly.
 */
static void
vx_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into,
				   ExplainState *es, const char *queryString,
				   ParamListInfo params, QueryEnvironment *queryEnv)
{
	PlannedStmt *plan;
	instr_time	planstart,
				planduration;
	BufferUsage bufusage_start,
				bufusage;
	uint64		runs_before;

	vx_sync_cluster_collation();
	vx_batch_motions_forget();
	if (prev_ExplainOneQuery)
	{
		prev_ExplainOneQuery(query, cursorOptions, into, es, queryString,
							 params, queryEnv);
		return;
	}

	if (es->buffers)
		bufusage_start = pgBufferUsage;
	INSTR_TIME_SET_CURRENT(planstart);

	plan = pg_plan_query(query, queryString, cursorOptions, params);

	INSTR_TIME_SET_CURRENT(planduration);
	INSTR_TIME_SUBTRACT(planduration, planstart);

	if (into != NULL)
		plan->intoClause = copyObject(into);

	if (es->buffers)
	{
		memset(&bufusage, 0, sizeof(BufferUsage));
		BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
	}

	runs_before = vx_runs_completed;
	ExplainOnePlan(plan, into, es, queryString, params, queryEnv,
				   &planduration, (es->buffers ? &bufusage : NULL),
				   cursorOptions);

	if (vx_mode != VX_MODE_OFF && es->format == EXPLAIN_FORMAT_TEXT)
	{
		vx_explain_slices(plan, es->str);
		if (es->analyze && vx_runs_completed != runs_before)
			appendStringInfo(es->str,
							 "Vectorized: " UINT64_FORMAT " partitions, memory limit "
							 UINT64_FORMAT " kB, peak " UINT64_FORMAT " kB, spilled "
							 UINT64_FORMAT " kB in " UINT64_FORMAT " files\n",
							 vx_last_run.partitions,
							 vx_last_run.memory_limit / 1024,
							 (vx_last_run.memory_peak + 1023) / 1024,
							 (vx_last_run.spilled_bytes + 1023) / 1024,
							 vx_last_run.spill_count);
		if (es->analyze && vx_runs_completed != runs_before && vx_last_run_pax)
			appendStringInfo(es->str,
							 "Vectorized PAX direct read: skipped " INT64_FORMAT
							 " of " INT64_FORMAT " micro-partitions and " INT64_FORMAT
							 " of " INT64_FORMAT " groups, decode peak " UINT64_FORMAT " kB\n",
							 vx_last_pax_scan.files_skipped, vx_last_pax_scan.files,
							 vx_last_pax_scan.groups_skipped, vx_last_pax_scan.groups,
							 (vx_last_run.pax_decode_peak + 1023) / 1024);
	}
}

void
vx_install_hooks(void)
{
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = vx_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = vx_ExecutorRun;
	prev_ExplainOneQuery = ExplainOneQuery_hook;
	ExplainOneQuery_hook = vx_ExplainOneQuery;
}

/* vec_debug_takeovers() returns bigint */
Datum
vec_debug_takeovers(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(vx_takeover_runs);
}
