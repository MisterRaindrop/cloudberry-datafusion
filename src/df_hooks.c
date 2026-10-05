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
 * df_hooks.c
 *	  Executor and EXPLAIN hooks: where DataFusion takes over a slice.
 *
 * ExecutorStart runs the standard start, then asks df_check_slice whether
 * this process's slice qualifies.  A qualifying slice is recorded against
 * its QueryDesc, in the query's memory context with a reset callback, so
 * the record disappears with the query however it ends.  ExecutorRun then
 * routes recorded slices to DataFusion.
 *
 * A taken-over slice runs in DataFusion (df_exec.c): its top PlanState's
 * ExecProcNode is replaced, so the rest of the executor is unchanged.
 * datafusion_debug_takeovers() counts the ExecutorRun calls that reached a
 * taken-over slice.
 *
 * With datafusion.mode = explain or on, text-format EXPLAIN ends with one
 * line per slice saying whether DataFusion can run it, and why not.
 *
 * src/df_hooks.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/explain.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"

#include "df_executor.h"

PG_FUNCTION_INFO_V1(datafusion_debug_takeovers);

int			df_mode = DF_MODE_OFF;

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExplainOneQuery_hook_type prev_ExplainOneQuery = NULL;

/* A slice DataFusion runs, recorded at ExecutorStart. */
typedef struct DfTakeover
{
	QueryDesc  *queryDesc;
	int			slice_index;
	struct DfTakeover *next;
	MemoryContextCallback forget;
} DfTakeover;

static DfTakeover *df_takeovers = NULL;

/* Number of ExecutorRun calls routed to DataFusion in this backend. */
static int64 df_takeover_runs = 0;

static void
df_takeover_forget(void *arg)
{
	DfTakeover **link;

	for (link = &df_takeovers; *link != NULL; link = &(*link)->next)
	{
		if (*link == (DfTakeover *) arg)
		{
			*link = (*link)->next;
			return;
		}
	}
}

static void
df_takeover_record(QueryDesc *queryDesc, int slice_index)
{
	MemoryContext cxt = queryDesc->estate->es_query_cxt;
	DfTakeover *t = MemoryContextAllocZero(cxt, sizeof(DfTakeover));

	t->queryDesc = queryDesc;
	t->slice_index = slice_index;
	t->forget.func = df_takeover_forget;
	t->forget.arg = t;
	MemoryContextRegisterResetCallback(cxt, &t->forget);

	t->next = df_takeovers;
	df_takeovers = t;
}

static DfTakeover *
df_takeover_find(QueryDesc *queryDesc)
{
	DfTakeover *t;

	for (t = df_takeovers; t != NULL; t = t->next)
		if (t->queryDesc == queryDesc)
			return t;
	return NULL;
}

static void
df_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	/*
	 * Backward and mark/restore scans need a rewindable result, which the
	 * DataFusion stream is not.
	 */
	if (df_mode == DF_MODE_ON &&
		(eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0 &&
		queryDesc->estate != NULL && queryDesc->planstate != NULL)
	{
		char		reason[256];
		int			slice_index;
		bool		is_sender;
		Plan	   *root = df_local_slice_root(queryDesc, &slice_index, &is_sender);

		/*
		 * The top slice is attached at its top node.  Any other slice starts
		 * with the Motion it sends through: that Motion stays on PostgreSQL,
		 * and DataFusion runs the subtree it pulls rows from.
		 */
		PlanState  *attach = queryDesc->planstate;

		if (is_sender)
			attach = IsA(attach, MotionState) ? outerPlanState(attach) : NULL;

		if (df_check_slice(queryDesc->plannedstmt, root, is_sender,
						   reason, sizeof(reason)) &&
			attach != NULL &&
			df_exec_attach(queryDesc, attach, reason, sizeof(reason)))
			df_takeover_record(queryDesc, slice_index);
		else
			elog(DEBUG1, "datafusion: slice %d stays on the PostgreSQL executor: %s",
				 slice_index, reason);
	}
}

static void
df_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
			   uint64 count, bool execute_once)
{
	DfTakeover *t = df_takeover_find(queryDesc);

	if (t != NULL)
	{
		df_takeover_runs++;
		elog(DEBUG1, "datafusion: running slice %d", t->slice_index);
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
df_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into,
				   ExplainState *es, const char *queryString,
				   ParamListInfo params, QueryEnvironment *queryEnv)
{
	PlannedStmt *plan;
	instr_time	planstart,
				planduration;
	BufferUsage bufusage_start,
				bufusage;
	uint64		runs_before;

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

	runs_before = df_runs_completed;
	ExplainOnePlan(plan, into, es, queryString, params, queryEnv,
				   &planduration, (es->buffers ? &bufusage : NULL),
				   cursorOptions);

	if (df_mode != DF_MODE_OFF && es->format == EXPLAIN_FORMAT_TEXT)
	{
		df_explain_slices(plan, es->str);
		if (es->analyze && df_runs_completed != runs_before)
			appendStringInfo(es->str,
							 "DataFusion: " UINT64_FORMAT " partitions, memory limit "
							 UINT64_FORMAT " kB, peak " UINT64_FORMAT " kB, spilled "
							 UINT64_FORMAT " kB in " UINT64_FORMAT " files\n",
							 df_last_run.partitions,
							 df_last_run.memory_limit / 1024,
							 (df_last_run.memory_peak + 1023) / 1024,
							 (df_last_run.spilled_bytes + 1023) / 1024,
							 df_last_run.spill_count);
	}
}

void
df_install_hooks(void)
{
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = df_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = df_ExecutorRun;
	prev_ExplainOneQuery = ExplainOneQuery_hook;
	ExplainOneQuery_hook = df_ExplainOneQuery;
}

/* datafusion_debug_takeovers() returns bigint */
Datum
datafusion_debug_takeovers(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(df_takeover_runs);
}
