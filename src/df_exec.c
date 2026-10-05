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
 *   - the main thread scans the table (heap, AO, AOCS or PAX) through the
 *     table AM interface with the query's snapshot, or pulls the rows a
 *     receiving Motion gets from another slice (M7a), and packs the needed
 *     columns into batches of DF_BATCH_ROWS rows;
 *   - batches go to DataFusion through a bounded queue, which filters and
 *     aggregates them on the runtime's threads;
 *   - result batches come back and are handed out one row at a time.
 *
 * Between two DataFusion slices a Gather Motion carries Arrow IPC batches
 * instead of tuples (M7b, datafusion.motion_batches; df_motion_sends_batches
 * decides).  The sending slice then runs in place of the Motion node: the
 * workers encode the results, and the main thread cuts the bytes into
 * tuple chunks of a type of its own and sends them, then end-of-stream.
 * The receiving slice takes the chunks straight from the interconnect,
 * hands their bytes to the workers to decode, and keeps the motion layer's
 * end-of-stream accounting the way cdbmotion.c does for an unordered
 * receiver.  Each stream starts with DF_BATCH_MAGIC and the Motion's
 * signature, which the receiver checks.
 *
 * The scan descriptor is installed in the Seq Scan node, so ExecEndSeqScan
 * closes it.  The DataFusion query is released by a reset callback on the
 * query's memory context, so it goes away however the query ends; an error
 * needs no PG_TRY here.
 *
 * Memory follows Cloudberry's rules (M4):
 *
 *   - the slice's operator budget is what a hashed Agg node would get from
 *     the executor, min(operatorMemKB, work_mem) * hash_mem_multiplier, or
 *     work_mem for a plain aggregate or a scan; it becomes the DataFusion
 *     pool's limit, and
 *     hash aggregation spills to this backend's temporary directory instead
 *     of growing past it;
 *   - everything Rust allocates is leased from the vmem tracker between
 *     waits (df_vmem_sync), so per-query and segment memory limits apply;
 *   - the peak and the spill volume go into the top node's Instrumentation
 *     and, for EXPLAIN ANALYZE, onto a DataFusion line.
 *
 * src/df_exec.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tableam.h"
#include "cdb/cdbinterconnect.h"
#include "cdb/cdbmotion.h"
#include "cdb/ml_ipc.h"
#include "cdb/tupchunk.h"
#include "cdb/tupchunklist.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "df_executor.h"

/* Rows per input batch; matches df_core::query::BATCH_ROWS. */
#define DF_BATCH_ROWS		8192

/* Wait for results this long when there is nothing else to do. */
#define DF_POLL_IDLE_MS		10

/* Wait this long when the input queue was full. */
#define DF_POLL_FULL_MS		1

/*
 * Tuple-chunk type of Arrow IPC bytes (M7b), outside PostgreSQL's
 * TupleChunkType range so that its receiver rejects them.
 */
#define DF_CHUNK_TYPE		0x4446

/* What each batch stream starts with, followed by the Motion's signature. */
#define DF_BATCH_MAGIC		"DFARROW1"
#define DF_BATCH_HEAD		16

/* The vmem lease runs ahead of Rust's heap by budget/8, at least this much. */
#define DF_MIN_HEADROOM		(1024 * 1024)

uint64		df_runs_completed = 0;
DfQueryStats df_last_run;
bool		df_pax_direct_read = false;
uint64		df_pax_direct_scans = 0;
bool		df_last_run_pax = false;
DfPaxScanInfo df_last_pax_scan;

typedef struct DfExec
{
	PlanState  *root;			/* the slice's top node below any sending Motion */
	PlanState  *procnode;		/* the node whose ExecProcNode is ours: root,
								 * or the Motion we send batches through */
	SeqScanState *scan;			/* the scan whose table we read, or NULL */
	MotionState *motion;		/* else the receiving Motion we read */

	/* M7b batch Motions */
	MotionState *send;			/* the Gather Motion we send batches through */
	bool		ipc_input;		/* 'motion' delivers batches */
	uint8		batch_head[DF_BATCH_HEAD];	/* magic + signature */
	bool		head_sent;
	bool		stopped;		/* the receiver asked us to stop */
	MemoryContext chunkcxt;		/* chunks being sent */
	TupleChunkListItem rx_items;	/* chunks received, not yet released */
	int16		rx_route;
	DfSlice    *rx_slices;		/* their payloads, past each stream's head */
	int			rx_nslices;
	int			rx_maxslices;
	bool		rx_eos;			/* the chunks end the route's stream */
	int			rx_nroutes;
	int		   *rx_head_seen;	/* per route: head bytes checked */
	bool	   *rx_ended;		/* per route: end-of-stream received */
	EState	   *estate;
	DfSliceSpec spec;
	int			maxattno;		/* highest table column we read */
	int64		memory_limit;	/* operator memory budget, bytes */
	int64		headroom;		/* vmem lease ahead of the heap, bytes */

	DfQuery    *query;			/* NULL until the first row is requested */
	bool		pax_direct;		/* reads PAX directly; pax_info is valid */
	DfPaxScanInfo pax_info;
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
		df_vmem_trim();
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
		if (x->procnode == pstate)
			return x;
	elog(ERROR, "datafusion: no execution state for this plan node");
	return NULL;				/* keep compiler quiet */
}

static void
df_raise_query(int32 status, const char *sqlstate, char *msg)
{
	char	   *detail = strchr(msg, '\n');

	if (status == DF_PANIC)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("datafusion panicked: %s", msg)));
	if (detail)
		*detail++ = '\0';
	ereport(ERROR,
			(errcode(MAKE_SQLSTATE(sqlstate[0], sqlstate[1], sqlstate[2],
								   sqlstate[3], sqlstate[4])),
			 errmsg("%s", msg),
			 detail ? errdetail("%s", detail) : 0));
}

/*
 * Operator memory budget of the slice, in bytes: what the executor would
 * give its hashed Agg node (see hash_agg_set_limits in nodeAgg.c), or
 * work_mem for a plain aggregate or a scan alone.
 */
static int64
df_slice_memory(PlanState *root)
{
	double		kb = work_mem;

	/*
	 * Only a hashed aggregate holds a hash table that the executor would
	 * limit.  A plain aggregate, like a scan, holds just the batches in
	 * flight; the resource queue rates it a light operator (100 kB), far
	 * too little for those, and DataFusion's repartitioning would spill
	 * them to disk.
	 */
	if (IsA(root, AggState) &&
		((Agg *) root->plan)->aggstrategy == AGG_HASHED)
	{
		uint64		op = PlanStateOperatorMemKB(root);

		if (op < kb)
			kb = op;
		kb *= hash_mem_multiplier;
	}
	return (int64) (kb * 1024.0);
}

/* The query produced all its rows: keep its figures. */
static void
df_exec_finished(DfExec *x)
{
	DfQueryStats s;

	df_ffi_query_stats(x->query, &s);
	df_last_run = s;
	df_last_run_pax = x->pax_direct;
	if (x->pax_direct)
		df_last_pax_scan = x->pax_info;
	df_runs_completed++;

	if (x->root->instrument)
	{
		x->root->instrument->workmemused = (double) s.memory_peak;
		if (s.spilled_bytes > 0)
			x->root->instrument->workmemwanted = (double) (s.memory_peak + s.spilled_bytes);
	}
}

static uint32_t
df_query_flags(DfExec *x)
{
	return (x->ipc_input ? DF_QUERY_IPC_INPUT : 0) |
		(x->send ? DF_QUERY_IPC_OUTPUT : 0);
}

/* ---------------------------------------------------------------------
 * M7b: sending batches through a Gather Motion
 * ---------------------------------------------------------------------
 */

/*
 * Send 'len' bytes of the batch stream to the receiver (route 0 of a
 * Gather), cut into tuple chunks of DF_CHUNK_TYPE.  Sets x->stopped if the
 * receiver asked the senders to stop.
 */
static void
df_send_bytes(DfExec *x, const uint8 *data, size_t len)
{
	Motion	   *motion = (Motion *) x->send->ps.plan;
	size_t		maxdata = Gp_max_tuple_chunk_size - TUPLE_CHUNK_HEADER_SIZE;
	TupleChunkListItem first = NULL;
	TupleChunkListItem last = NULL;
	MemoryContext oldcxt = MemoryContextSwitchTo(x->chunkcxt);

	while (len > 0)
	{
		size_t		n = Min(len, maxdata);
		TupleChunkListItem item;

		item = palloc(offsetof(TupleChunkListItemData, chunk_data) +
					  TUPLE_CHUNK_HEADER_SIZE + n);
		item->p_next = NULL;
		item->inplace = NULL;
		item->chunk_length = TUPLE_CHUNK_HEADER_SIZE + n;
		SetChunkDataSize(item->chunk_data, n);
		SetChunkType(item->chunk_data, DF_CHUNK_TYPE);
		memcpy(item->chunk_data + TUPLE_CHUNK_HEADER_SIZE, data, n);
		if (last)
			last->p_next = item;
		else
			first = item;
		last = item;
		data += n;
		len -= n;
	}
	MemoryContextSwitchTo(oldcxt);

	/* The interconnect copies the chunks into its own buffers. */
	if (first != NULL &&
		!CurrentMotionIPCLayer->SendTupleChunkToAMS(x->estate->interconnect_context,
													motion->motionID, 0, first))
		x->stopped = true;
	MemoryContextReset(x->chunkcxt);
}

/* ---------------------------------------------------------------------
 * M7b: receiving batches from a Gather Motion
 * ---------------------------------------------------------------------
 */

/* Free the received chunks and give their receive buffer back. */
static void
df_rx_release(DfExec *x, int16 motion_id)
{
	TupleChunkListItem item = x->rx_items;

	while (item != NULL)
	{
		TupleChunkListItem next = item->p_next;

		pfree(item);
		item = next;
	}
	x->rx_items = NULL;
	x->rx_nslices = 0;
	CurrentMotionIPCLayer->DirectPutRxBuffer(x->estate->interconnect_context,
											 motion_id, x->rx_route);
}

/*
 * Receive the next chunks, from whichever sender has some, and note their
 * payloads in x->rx_slices, checking the head of each stream.
 */
static void
df_rx_take(DfExec *x, int16 motion_id, MotionNodeEntry *entry)
{
	MotionLayerState *ml = (MotionLayerState *) x->estate->motionlayer_context;
	TupleChunkListItem item;
	int16		route = ANY_ROUTE;
	MemoryContext oldcxt;

	if (x->estate->interconnect_context == NULL)
		ereport(ERROR, (errmsg("Interconnect is down unexpectedly.")));
	if (x->rx_head_seen == NULL)
	{
		x->rx_nroutes = entry->num_senders;
		x->rx_head_seen = MemoryContextAllocZero(x->estate->es_query_cxt,
												 sizeof(int) * Max(x->rx_nroutes, 1));
		x->rx_ended = MemoryContextAllocZero(x->estate->es_query_cxt,
											 sizeof(bool) * Max(x->rx_nroutes, 1));
	}

	/* As execMotionUnsortedReceiver and processIncomingChunks do. */
	x->estate->active_recv_id = motion_id;
	oldcxt = MemoryContextSwitchTo(ml->motion_layer_mctx);
	x->rx_items = CurrentMotionIPCLayer->RecvTupleChunkFromAny(x->estate->interconnect_context,
															   motion_id, &route);
	MemoryContextSwitchTo(oldcxt);
	x->rx_route = route;
	x->rx_nslices = 0;
	x->rx_eos = false;
	if (route < 0 || route >= x->rx_nroutes)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("datafusion: chunks from unexpected route %d of Motion %d",
						route, motion_id)));

	for (item = x->rx_items; item != NULL; item = item->p_next)
	{
		TupleChunkType type;
		uint8	   *data;
		size_t		len;
		int		   *seen = &x->rx_head_seen[route];

		if (item->chunk_length < TUPLE_CHUNK_HEADER_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("datafusion: tuple chunk of %u bytes from route %d of Motion %d",
							item->chunk_length, route, motion_id)));
		GetChunkType(item, &type);
		if (x->rx_eos)
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("datafusion: data after end-of-stream from route %d of Motion %d",
							route, motion_id)));
		if (type == TC_END_OF_STREAM)
		{
			x->rx_eos = true;
			continue;
		}
		if (type != DF_CHUNK_TYPE)
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("datafusion: Motion %d received tuples where DataFusion batches were expected",
							motion_id),
					 errdetail("The sending slice on route %d did not run in DataFusion.", route)));

		data = (uint8 *) GetChunkDataPtr(item) + TUPLE_CHUNK_HEADER_SIZE;
		len = item->chunk_length - TUPLE_CHUNK_HEADER_SIZE;
		if (*seen < DF_BATCH_HEAD)
		{
			size_t		k = Min(len, (size_t) (DF_BATCH_HEAD - *seen));

			if (memcmp(data, x->batch_head + *seen, k) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						 errmsg("datafusion: Motion %d received batches of another plan",
								motion_id),
						 errdetail("The stream from route %d does not start with this Motion's signature.",
								   route)));
			*seen += k;
			data += k;
			len -= k;
		}
		if (len == 0)
			continue;
		if (x->rx_nslices == x->rx_maxslices)
		{
			x->rx_maxslices = Max(16, x->rx_maxslices * 2);
			x->rx_slices = x->rx_slices ?
				repalloc(x->rx_slices, sizeof(DfSlice) * x->rx_maxslices) :
				MemoryContextAlloc(x->estate->es_query_cxt, sizeof(DfSlice) * x->rx_maxslices);
		}
		x->rx_slices[x->rx_nslices].data = data;
		x->rx_slices[x->rx_nslices].len = len;
		x->rx_nslices++;
	}
}

/*
 * Batch-Motion input: hand the received bytes to DataFusion, receiving more
 * when they are taken.  Sets *pushed when bytes were taken, *full when the
 * queue was full.
 */
static void
df_exec_receive(DfExec *x, bool *pushed, bool *full)
{
	char		buf[DF_MSG_BUFLEN];
	char		sqlstate[6] = "XX000";
	int16		motion_id = ((Motion *) x->motion->ps.plan)->motionID;
	MotionLayerState *ml = (MotionLayerState *) x->estate->motionlayer_context;
	MotionNodeEntry *entry = &ml->mnEntries[motion_id - 1];

	if (x->rx_items == NULL)
	{
		if (!entry->moreNetWork)
		{
			/* every sender has ended its stream */
			x->estate->active_recv_id = -1;
			df_ffi_query_finish_input(x->query);
			x->input_done = true;
			return;
		}
		df_rx_take(x, motion_id, entry);
		if (x->rx_items == NULL)
			return;
	}

	if (x->rx_nslices > 0)
	{
		int32		status = df_ffi_query_push_ipc(x->query, x->rx_route, x->rx_slices,
												   (uint32_t) x->rx_nslices,
												   sqlstate, buf, sizeof(buf));

		if (status == DF_PENDING)
		{
			/*
			 * Still full after a short wait inside: keep the chunks and offer
			 * them again after polling (without waiting there).
			 */
			*pushed = true;
			return;
		}
		if (status != DF_OK)
			df_raise_query(status, sqlstate, buf);
		*pushed = true;
	}

	/* The bytes are copied: account for end-of-stream, then release. */
	if (x->rx_eos)
	{
		if (x->rx_ended[x->rx_route])
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("datafusion: second end-of-stream from route %d of Motion %d",
							x->rx_route, motion_id)));
		x->rx_ended[x->rx_route] = true;
		entry->num_stream_ends_recvd++;
		if (entry->num_stream_ends_recvd == entry->num_senders)
			entry->moreNetWork = false;
		CurrentMotionIPCLayer->DeregisterReadInterest(x->estate->interconnect_context,
													  motion_id, x->rx_route,
													  "end of stream");
	}
	df_rx_release(x, motion_id);
}

/*
 * Experimental: start the query on PAX micro-partitions that DataFusion's
 * partitions decode themselves.  Returns false when the direct reader does
 * not apply; the caller then reads through the table AM.
 */
static bool
df_exec_begin_pax(DfExec *x, int workers)
{
	Relation	rel = x->scan->ss.ss_currentRelation;
	const DfPaxReader *reader;
	char		buf[DF_MSG_BUFLEN];
	char		sqlstate[6] = "XX000";
	int		   *cols;
	int		   *widths;
	void	   *scan;
	int32		status;
	int			c;

	if (!df_pax_direct_read || x->scan->ss.ss_currentScanDesc != NULL)
		return false;
	{
		char	   *amname = get_am_name(rel->rd_rel->relam);

		if (amname == NULL || strcmp(amname, "pax") != 0)
			return false;
	}
	reader = df_pax_reader_get();
	if (reader == NULL)
		return false;

	cols = palloc(sizeof(int) * Max(x->spec.nscan, 1));
	widths = palloc(sizeof(int) * Max(x->spec.nscan, 1));
	for (c = 0; c < x->spec.nscan; c++)
	{
		cols[c] = x->spec.scan_attnos[c] - 1;
		widths[c] = df_type_width(x->spec.scan_types[c]);
	}
	/*
	 * The scan's qual only lets PAX skip micro-partitions and groups by
	 * their min/max statistics; DataFusion still filters every row.
	 */
	scan = reader->begin(rel, x->estate->es_snapshot, x->scan->ss.ps.plan->qual,
						 cols, widths, x->spec.nscan, buf, sizeof(buf));
	if (scan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("datafusion: %s", buf)));
	reader->info(scan, &x->pax_info);
	x->pax_direct = true;

	/* From here on the query owns the scan. */
	status = df_ffi_query_start_pax(x->spec.json, (uint32_t) workers,
									(uint64_t) x->memory_limit, df_spill_dir(),
									scan, (uint32_t) reader->nblocks(scan),
									reader->read, reader->end, df_query_flags(x),
									&x->query, sqlstate, buf, sizeof(buf));
	if (status != DF_OK)
	{
		x->query = NULL;
		df_raise_query(status, sqlstate, buf);
	}
	x->input_done = true;		/* nothing to push from the main thread */
	df_pax_direct_scans++;
	return true;
}

static void
df_exec_begin(DfExec *x)
{
	char		buf[DF_MSG_BUFLEN];
	char		sqlstate[6] = "XX000";
	int			workers;
	int32		status;
	Relation	rel;

	workers = df_runtime_ensure();
	if (x->scan && df_exec_begin_pax(x, workers))
	{
		df_vmem_sync(x->headroom);
		return;
	}
	status = df_ffi_query_start(x->spec.json, (uint32_t) workers,
								(uint64_t) x->memory_limit, df_spill_dir(),
								df_query_flags(x), &x->query, sqlstate, buf, sizeof(buf));
	if (status != DF_OK)
	{
		x->query = NULL;
		df_raise_query(status, sqlstate, buf);
	}
	df_vmem_sync(x->headroom);
	if (x->motion)
		return;					/* the Motion is ready to receive */

	/*
	 * In Cloudberry's parallel mode several QEs of one segment share the
	 * scan: before the first row is requested, ExecutePlan has already begun
	 * a parallel scan on the Seq Scan node (GpInsertParallelDSMHash), which
	 * hands each QE its own part of the table.  Use it.
	 *
	 * Otherwise begin the scan the way the Seq Scan node would.  Column
	 * stores (AOCS, PAX) take the node's PlanState to read only the columns
	 * its targetlist and filter use; PAX also skips micro-partitions whose
	 * min/max statistics rule the filter out.  DataFusion still applies the
	 * whole filter to every row it receives.
	 */
	rel = x->scan->ss.ss_currentRelation;
	if (x->scan->ss.ss_currentScanDesc != NULL)
		x->scandesc = x->scan->ss.ss_currentScanDesc;
	else if (rel->rd_tableam->scan_begin_extractcolumns)
		x->scandesc = table_beginscan_es(rel, x->estate->es_snapshot, 0, NULL,
										 NULL, &x->scan->ss.ps);
	else
		x->scandesc = table_beginscan(rel, x->estate->es_snapshot, 0, NULL);
	x->scan->ss.ss_currentScanDesc = x->scandesc;	/* ExecEndSeqScan closes it */
}

/* Read up to DF_BATCH_ROWS rows of the needed columns. */
static void
df_exec_fill(DfExec *x)
{
	TupleTableSlot *slot = x->scan ? x->scan->ss.ss_ScanTupleSlot : NULL;
	int			n = 0;
	int			c;

	while (n < DF_BATCH_ROWS)
	{
		if (x->motion)
		{
			/* The Motion's own receive path, with its instrumentation. */
			slot = ExecProcNode(&x->motion->ps);
			if (TupIsNull(slot))
			{
				x->scan_ended = true;
				break;
			}
		}
		else if (!table_scan_getnextslot(x->scandesc, ForwardScanDirection, slot))
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

		if (!x->input_done && x->ipc_input)
			df_exec_receive(x, &pushed, &full);
		else if (!x->input_done)
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
		else if (status == DF_BYTES)
		{
			const uint8 *data;
			size_t		len;

			/* Our results, for the Motion's receiver. */
			if (!x->head_sent)
			{
				df_send_bytes(x, x->batch_head, DF_BATCH_HEAD);
				x->head_sent = true;
			}
			df_ffi_query_bytes(x->query, &data, &len);
			if (!x->stopped)
				df_send_bytes(x, data, len);
			if (x->stopped)
			{
				/*
				 * The receiver needs no more rows: stop as the Motion would,
				 * without end-of-stream, and let the workers go.
				 */
				x->send->stopRequested = true;
				x->done = true;
				df_ffi_query_free(x->query);
				x->query = NULL;
				df_vmem_trim();
			}
		}
		else if (status == DF_DONE)
		{
			x->done = true;
			df_exec_finished(x);
			if (x->send)
			{
				SendEndOfStream(x->estate->motionlayer_context,
								x->estate->interconnect_context,
								((Motion *) x->send->ps.plan)->motionID);
				x->send->sentEndOfStream = true;
			}
		}
		else if (status != DF_PENDING)
			df_raise_query(status, sqlstate, buf);

		df_vmem_sync(x->headroom);
		CHECK_FOR_INTERRUPTS();
	}
}

static TupleTableSlot *
df_exec_proc_node(PlanState *pstate)
{
	return df_exec_next(df_exec_lookup(pstate));
}

/* The head of a batch stream through 'motion': magic, then signature. */
static void
df_batch_head(uint8 *head, Motion *motion)
{
	uint64		sig = df_motion_signature(motion);

	memcpy(head, DF_BATCH_MAGIC, 8);
	memcpy(head + 8, &sig, 8);
}

/*
 * Prepare to run the slice whose top PlanState is 'root' in DataFusion.
 * 'send', if not NULL, is the Gather Motion above it that is to carry
 * Arrow batches; the slice then runs in its place.  Returns false, with a
 * reason, if the slice cannot be translated; it then stays on the
 * PostgreSQL executor.
 */
bool
df_exec_attach(QueryDesc *queryDesc, PlanState *root, MotionState *send,
			   char *reason, size_t reasonlen)
{
	EState	   *estate = queryDesc->estate;
	MemoryContext oldcxt;
	DfExec	   *x;
	PlanState  *scanps;
	int			c;

	scanps = IsA(root, AggState) ? outerPlanState(root) : root;
	if (scanps == NULL ||
		!(IsA(scanps, SeqScanState) || (IsA(scanps, MotionState) && scanps != root)))
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
	x->procnode = send ? &send->ps : root;
	if (IsA(scanps, MotionState))
	{
		x->motion = (MotionState *) scanps;
		x->ipc_input = df_motion_sends_batches(queryDesc->plannedstmt,
											   (Motion *) scanps->plan);
		if (x->ipc_input)
			df_batch_head(x->batch_head, (Motion *) scanps->plan);
	}
	else
		x->scan = (SeqScanState *) scanps;
	if (send)
	{
		x->send = send;
		df_batch_head(x->batch_head, (Motion *) send->ps.plan);
		x->chunkcxt = AllocSetContextCreate(estate->es_query_cxt, "datafusion chunks",
											ALLOCSET_DEFAULT_SIZES);
	}
	x->estate = estate;
	x->memory_limit = df_slice_memory(root);
	x->headroom = Max(x->memory_limit / 8, DF_MIN_HEADROOM);

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

	elog(DEBUG1, "datafusion plan: %s%s%s", x->spec.json,
		 x->ipc_input ? " (batches in)" : "", x->send ? " (batches out)" : "");
	ExecSetExecProcNode(x->procnode, df_exec_proc_node);
	return true;
}
