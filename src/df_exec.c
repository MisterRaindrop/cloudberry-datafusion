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
 * df_exec.c
 *	  Run a slice in DataFusion behind PostgreSQL's executor interface.
 *
 * The slice's top PlanState keeps its place in the executor; only its
 * ExecProcNode function is replaced.  standard_ExecutorRun therefore still
 * starts and stops the DestReceiver, honours FETCH counts, keeps
 * es_processed and runs the MPP cleanup on error, and EXPLAIN ANALYZE
 * counts the rows of the top node.  Each call returns one row:
 *
 *   - the main thread scans the heap table with the query's snapshot and
 *     packs the needed columns into batches of DF_BATCH_ROWS rows;
 *   - batches go to DataFusion through a bounded queue, which filters and
 *     aggregates them on the runtime's threads;
 *   - result batches come back and are handed out one row at a time.
 *
 * The scan descriptor is installed in the Seq Scan node, so ExecEndSeqScan
 * closes it.  The DataFusion query is released by a reset callback on the
 * query's memory context, so it goes away however the query ends; an error
 * needs no PG_TRY here.
 *
 * src/df_exec.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tableam.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "df_executor.h"

/* Rows per input batch; matches df_core::query::BATCH_ROWS. */
#define DF_BATCH_ROWS		8192

/* Wait for results this long when there is nothing else to do. */
#define DF_POLL_IDLE_MS		10

/* Wait this long when the input queue was full. */
#define DF_POLL_FULL_MS		1

typedef struct DfExec
{
	PlanState  *root;			/* the slice's top node; its ExecProcNode is ours */
	SeqScanState *scan;			/* the scan whose table we read */
	EState	   *estate;
	DfSliceSpec spec;
	int			maxattno;		/* highest table column we read */

	DfQuery    *query;			/* NULL until the first row is requested */
	TableScanDesc scandesc;

	/* input batch being assembled or offered */
	char	  **invalues;
	uint8	  **innulls;
	DfColumn   *incols;
	int			batch_rows;
	bool		batch_ready;	/* a filled batch waits to be pushed */
	bool		scan_ended;
	bool		input_done;

	/* output batch being handed out */
	TupleTableSlot *outslot;
	DfColumn   *outcols;
	uint32		out_nrows;
	uint32		out_row;
	bool		done;

	struct DfExec *next;
	MemoryContextCallback release;
} DfExec;

static DfExec *df_execs = NULL;

static int
df_type_width(Oid type)
{
	switch (type)
	{
		case BOOLOID:
			return 1;
		case INT2OID:
			return 2;
		case INT4OID:
		case FLOAT4OID:
			return 4;
		default:
			return 8;
	}
}

static void
df_exec_release(void *arg)
{
	DfExec	   *x = (DfExec *) arg;
	DfExec	  **link;

	if (x->query != NULL)
	{
		df_ffi_query_free(x->query);
		x->query = NULL;
	}
	for (link = &df_execs; *link != NULL; link = &(*link)->next)
	{
		if (*link == x)
		{
			*link = x->next;
			break;
		}
	}
}

static DfExec *
df_exec_lookup(PlanState *pstate)
{
	DfExec	   *x;

	for (x = df_execs; x != NULL; x = x->next)
		if (x->root == pstate)
			return x;
	elog(ERROR, "datafusion: no execution state for this plan node");
	return NULL;				/* keep compiler quiet */
}

static void
df_raise_query(int32 status, const char *sqlstate, const char *msg)
{
	if (status == DF_PANIC)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("datafusion panicked: %s", msg)));
	ereport(ERROR,
			(errcode(MAKE_SQLSTATE(sqlstate[0], sqlstate[1], sqlstate[2],
								   sqlstate[3], sqlstate[4])),
			 errmsg("%s", msg)));
}

static void
df_exec_begin(DfExec *x)
{
	char		buf[DF_MSG_BUFLEN];
	char		sqlstate[6] = "XX000";
	int			workers;
	int32		status;
	Relation	rel = x->scan->ss.ss_currentRelation;

	workers = df_runtime_ensure();
	status = df_ffi_query_start(x->spec.json, (uint32_t) workers, &x->query,
								sqlstate, buf, sizeof(buf));
	if (status != DF_OK)
	{
		x->query = NULL;
		df_raise_query(status, sqlstate, buf);
	}

	x->scandesc = table_beginscan(rel, x->estate->es_snapshot, 0, NULL);
	x->scan->ss.ss_currentScanDesc = x->scandesc;	/* ExecEndSeqScan closes it */
}

/* Read up to DF_BATCH_ROWS rows of the needed columns. */
static void
df_exec_fill(DfExec *x)
{
	TupleTableSlot *slot = x->scan->ss.ss_ScanTupleSlot;
	int			n = 0;
	int			c;

	while (n < DF_BATCH_ROWS)
	{
		if (!table_scan_getnextslot(x->scandesc, ForwardScanDirection, slot))
		{
			x->scan_ended = true;
			break;
		}
		if (x->maxattno > 0)
			slot_getsomeattrs(slot, x->maxattno);
		for (c = 0; c < x->spec.nscan; c++)
		{
			int			att = x->spec.scan_attnos[c] - 1;
			bool		isnull = slot->tts_isnull[att];
			Datum		d = slot->tts_values[att];
			char	   *dst = x->invalues[c];

			x->innulls[c][n] = isnull ? 1 : 0;
			switch (x->spec.scan_types[c])
			{
				case BOOLOID:
					((uint8 *) dst)[n] = isnull ? 0 : (DatumGetBool(d) ? 1 : 0);
					break;
				case INT2OID:
					((int16 *) dst)[n] = isnull ? 0 : DatumGetInt16(d);
					break;
				case INT4OID:
					((int32 *) dst)[n] = isnull ? 0 : DatumGetInt32(d);
					break;
				case INT8OID:
					((int64 *) dst)[n] = isnull ? 0 : DatumGetInt64(d);
					break;
				case FLOAT4OID:
					((float4 *) dst)[n] = isnull ? 0 : DatumGetFloat4(d);
					break;
				case FLOAT8OID:
					((float8 *) dst)[n] = isnull ? 0 : DatumGetFloat8(d);
					break;
			}
		}
		n++;
	}
	x->batch_rows = n;
	x->batch_ready = (n > 0);
	if (x->scan_ended && !x->batch_ready)
	{
		df_ffi_query_finish_input(x->query);
		x->input_done = true;
	}
}

static TupleTableSlot *
df_exec_emit(DfExec *x)
{
	TupleTableSlot *slot = x->outslot;
	uint32		r = x->out_row++;
	int			c;

	ExecClearTuple(slot);
	for (c = 0; c < x->spec.nout; c++)
	{
		const void *v = x->outcols[c].values;
		bool		isnull = x->outcols[c].nulls[r] != 0;

		slot->tts_isnull[c] = isnull;
		if (isnull)
		{
			slot->tts_values[c] = (Datum) 0;
			continue;
		}
		switch (x->spec.out_types[c])
		{
			case BOOLOID:
				slot->tts_values[c] = BoolGetDatum(((const uint8 *) v)[r] != 0);
				break;
			case INT2OID:
				slot->tts_values[c] = Int16GetDatum(((const int16 *) v)[r]);
				break;
			case INT4OID:
				slot->tts_values[c] = Int32GetDatum(((const int32 *) v)[r]);
				break;
			case INT8OID:
				slot->tts_values[c] = Int64GetDatum(((const int64 *) v)[r]);
				break;
			case FLOAT4OID:
				slot->tts_values[c] = Float4GetDatum(((const float4 *) v)[r]);
				break;
			case FLOAT8OID:
				slot->tts_values[c] = Float8GetDatum(((const float8 *) v)[r]);
				break;
		}
	}
	return ExecStoreVirtualTuple(slot);
}

static TupleTableSlot *
df_exec_next(DfExec *x)
{
	char		buf[DF_MSG_BUFLEN];
	char		sqlstate[6] = "XX000";

	if (x->query == NULL && !x->done)
		df_exec_begin(x);

	for (;;)
	{
		bool		pushed = false;
		bool		full = false;
		uint32		nrows = 0;
		int32		status;

		if (x->out_row < x->out_nrows)
			return df_exec_emit(x);
		if (x->done)
			return ExecClearTuple(x->outslot);

		if (!x->input_done)
		{
			if (!x->batch_ready)
				df_exec_fill(x);
			if (x->batch_ready)
			{
				status = df_ffi_query_push(x->query, x->incols, (uint32_t) x->spec.nscan,
										   (uint32_t) x->batch_rows, sqlstate,
										   buf, sizeof(buf));
				if (status == DF_OK)
				{
					pushed = true;
					x->batch_ready = false;
					if (x->scan_ended)
					{
						df_ffi_query_finish_input(x->query);
						x->input_done = true;
					}
				}
				else if (status == DF_PENDING)
					full = true;
				else
					df_raise_query(status, sqlstate, buf);
			}
		}

		status = df_ffi_query_poll(x->query,
								   pushed ? 0 : (full ? DF_POLL_FULL_MS : DF_POLL_IDLE_MS),
								   &nrows, sqlstate, buf, sizeof(buf));
		if (status == DF_OK)
		{
			int			c;

			for (c = 0; c < x->spec.nout; c++)
				if (df_ffi_query_column(x->query, (uint32_t) c, &x->outcols[c]) != DF_OK)
					elog(ERROR, "datafusion: result column %d is missing", c);
			x->out_nrows = nrows;
			x->out_row = 0;
		}
		else if (status == DF_DONE)
			x->done = true;
		else if (status != DF_PENDING)
			df_raise_query(status, sqlstate, buf);

		CHECK_FOR_INTERRUPTS();
	}
}

static TupleTableSlot *
df_exec_proc_node(PlanState *pstate)
{
	return df_exec_next(df_exec_lookup(pstate));
}

/*
 * Prepare to run the slice whose top PlanState is 'root' in DataFusion.
 * Returns false, with a reason, if the slice cannot be translated; it then
 * stays on the PostgreSQL executor.
 */
bool
df_exec_attach(QueryDesc *queryDesc, PlanState *root, char *reason, size_t reasonlen)
{
	EState	   *estate = queryDesc->estate;
	MemoryContext oldcxt;
	DfExec	   *x;
	PlanState  *scanps;
	int			c;

	scanps = IsA(root, AggState) ? outerPlanState(root) : root;
	if (scanps == NULL || !IsA(scanps, SeqScanState))
	{
		snprintf(reason, reasonlen, "unexpected executor state for this slice");
		return false;
	}

	oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);
	x = palloc0(sizeof(DfExec));
	if (!df_translate_slice(root->plan, &x->spec, reason, reasonlen))
	{
		MemoryContextSwitchTo(oldcxt);
		pfree(x);
		return false;
	}
	x->root = root;
	x->scan = (SeqScanState *) scanps;
	x->estate = estate;

	x->invalues = palloc0(sizeof(char *) * Max(x->spec.nscan, 1));
	x->innulls = palloc0(sizeof(uint8 *) * Max(x->spec.nscan, 1));
	x->incols = palloc0(sizeof(DfColumn) * Max(x->spec.nscan, 1));
	for (c = 0; c < x->spec.nscan; c++)
	{
		x->invalues[c] = palloc(DF_BATCH_ROWS * df_type_width(x->spec.scan_types[c]));
		x->innulls[c] = palloc(DF_BATCH_ROWS);
		x->incols[c].values = x->invalues[c];
		x->incols[c].nulls = x->innulls[c];
		x->maxattno = Max(x->maxattno, x->spec.scan_attnos[c]);
	}
	x->outcols = palloc0(sizeof(DfColumn) * Max(x->spec.nout, 1));
	x->outslot = ExecInitExtraTupleSlot(estate, ExecGetResultType(root), &TTSOpsVirtual);

	x->release.func = df_exec_release;
	x->release.arg = x;
	MemoryContextRegisterResetCallback(estate->es_query_cxt, &x->release);
	x->next = df_execs;
	df_execs = x;
	MemoryContextSwitchTo(oldcxt);

	elog(DEBUG1, "datafusion plan: %s", x->spec.json);
	ExecSetExecProcNode(root, df_exec_proc_node);
	return true;
}
