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
#include "cdb/cdbgang.h"
#include "cdb/cdbinterconnect.h"
#include "cdb/cdbmotion.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/ml_ipc.h"
#include "cdb/tupchunk.h"
#include "cdb/tupchunklist.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/snapmgr.h"

#include "df_executor.h"

/* Rows per input batch; matches df_core::query::BATCH_ROWS. */
#define DF_BATCH_ROWS		8192
/*
 * A batch also ends once one of its string columns holds this many bytes,
 * for rows much wider than the planner expected.
 */
#define DF_BATCH_STRING_BYTES	(4 * 1024 * 1024)

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
bool		df_pax_direct_read = true;
int			df_pax_max_decoders = 0;
int			df_slice_partitions = 0;
uint64		df_pax_direct_scans = 0;
bool		df_last_run_pax = false;
DfPaxScanInfo df_last_pax_scan;

/* PS1: a column read from PostgreSQL's partial state */
#define DF_PART_NONE	0
#define DF_PART_SUM		1
#define DF_PART_COUNT	2

#define DF_STATE_NUMERIC	1	/* numeric_avg_serialize */
#define DF_STATE_INT8		2	/* int8_avg_serialize */
#define DF_STATE_INT_ARRAY	3	/* int4_avg_accum's int8[] {count, sum} */

/* One input of the slice: a table scanned, or a Motion received from. */
typedef struct DfInput
{
	int			index;			/* in the plan's inputs */
	DfSliceInput *spec;			/* the columns read */
	SeqScanState *scan;			/* the scan whose table we read, or NULL */
	MotionState *motion;		/* else the receiving Motion we read */
	PlanState  *reader;			/* else the Shared Scan reader we pull */
	TableScanDesc scandesc;
	int			maxattno;		/* highest column read */

	/*
	 * PS1: from a Motion of tuples, the tuple column behind each column read
	 * (its stream column counts a state as DataFusion's columns), and for a
	 * partial aggregate's state, which part and which of PostgreSQL's
	 * formats; NULL without states.
	 */
	AttrNumber *tupatt;
	uint8	   *statepart;		/* DF_PART_* */
	uint8	   *stateformat;	/* DF_STATE_* */
	MemoryContext statecxt;

	/* batch being assembled or offered */
	char	  **invalues;		/* string columns: their bytes */
	uint8	  **innulls;
	int32	  **inoffsets;		/* string columns: DF_BATCH_ROWS + 1 offsets */
	Size	   *incap;			/* string columns: bytes allocated */
	DfColumn   *incols;
	int			batch_rows;
	bool		batch_ready;	/* a filled batch waits to be pushed */
	bool		ended;			/* no more rows to read */
	bool		done;			/* DataFusion told so */

	/* M7b: 'motion' delivers batches */
	bool		ipc;
	uint8		rx_head[DF_BATCH_HEAD];	/* expected head of each stream */
	TupleChunkListItem rx_items;	/* chunks received, not yet released */
	int16		rx_route;
	DfSlice    *rx_slices;		/* their payloads, past each stream's head */
	int			rx_nslices;
	int			rx_maxslices;
	bool		rx_eos;			/* the chunks end the route's stream */
	int			rx_nroutes;
	int		   *rx_head_seen;	/* per route: head bytes checked */
	bool	   *rx_ended;		/* per route: end-of-stream received */
} DfInput;

typedef struct DfExec
{
	PlanState  *root;			/* the slice's top node below any sending Motion */
	PlanState  *procnode;		/* the node whose ExecProcNode is ours: root,
								 * or the Motion we send batches through */
	int			ninputs;
	DfInput    *inputs;			/* fed in this order, each to its end */
	int			nprerun;
	PlanState **prerun;			/* Shared Scan producers PostgreSQL runs
								 * first, as their Sequences would */

	/* M7b batch Motions */
	MotionState *send;			/* the Motion we send batches through */
	int			send_nroutes;	/* its receivers */
	bool	   *head_sent_to;	/* per route: stream head sent */
	uint8		tx_head[DF_BATCH_HEAD];	/* head of the streams we send */
	bool		stopped;		/* the receiver asked us to stop */
	MemoryContext chunkcxt;		/* chunks being sent */
	EState	   *estate;
	DfSliceSpec spec;
	int64		memory_limit;	/* operator memory budget, bytes */
	int64		headroom;		/* vmem lease ahead of the heap, bytes */
	int			slices_together;	/* slices of the plan running with ours */
	int			host_segments;	/* primary segments on this host */

	DfQuery    *query;			/* NULL until the first row is requested */
	bool		pax_direct;		/* reads PAX directly; pax_info is valid */
	DfPaxScanInfo pax_info;
	bool		input_done;		/* every input is done */

	/* output batch being handed out */
	TupleTableSlot *outslot;
	TupleTableSlot *valslot;	/* P1: DataFusion's values, if PostgreSQL
								 * finishes some columns; else outslot */
	ExprState **tail_states;	/* per column: what finishes it, or NULL */
	ExprContext *tailcxt;
	MemoryContext rowcxt;		/* the strings of the row handed out */
	DfColumn   *outcols;
	uint32		out_nrows;
	uint32		out_row;
	bool		done;

	struct DfExec *next;
	MemoryContextCallback release;
} DfExec;

static DfExec *df_execs = NULL;

static bool
df_type_is_string(Oid type)
{
	return type == TEXTOID || type == VARCHAROID || type == BPCHAROID;
}

/* Bytes per value of a fixed-width type. */
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
		case DATEOID:
			return 4;
		case NUMERICOID:
			return DF_NUMERIC_BYTES;	/* Decimal256 */
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
 * give its hashed Agg node (see hash_agg_set_limits in nodeAgg.c) and its
 * Hash nodes, or work_mem for a plain aggregate or a scan alone.
 */
/* Bytes for the hash tables of the slice's nodes from 'ps' down. */
static double
df_hash_memory(PlanState *ps)
{
	double		bytes = 0;

	if (ps == NULL || IsA(ps, MotionState))
		return 0;
	if (IsA(ps, AggState) && ((Agg *) ps->plan)->aggstrategy == AGG_HASHED)
	{
		double		kb = work_mem;
		uint64		op = PlanStateOperatorMemKB(ps);

		if (op < kb)
			kb = op;
		bytes += kb * hash_mem_multiplier * 1024.0;
	}
	else if (IsA(ps, HashState))
		bytes += df_hash_budget(ps->plan);
	else if (IsA(ps, SortState))
		bytes += PlanStateOperatorMemKB(ps) * 1024.0;	/* as nodeSort.c (S1) */
	else if (IsA(ps, SequenceState))
	{
		/* its last input; PostgreSQL runs the others (df_exec_prerun) */
		SequenceState *ss = (SequenceState *) ps;

		return df_hash_memory(ss->subplans[ss->numSubplans - 1]);
	}
	else if (IsA(ps, AppendState))
	{
		AppendState *as = (AppendState *) ps;
		int			i;

		for (i = 0; i < as->as_nplans; i++)
			bytes += df_hash_memory(as->appendplans[i]);
	}
	return bytes + df_hash_memory(outerPlanState(ps)) + df_hash_memory(innerPlanState(ps));
}

static int64
df_slice_memory(PlanState *root)
{
	double		bytes = df_hash_memory(root);

	/*
	 * Only hashed aggregates, Hash Join tables and sorts are limited by
	 * the executor; their budgets add up.  Without them, as for a plain
	 * aggregate or a scan, only the batches in flight need room: the
	 * resource queue rates a plain Agg a light operator (100 kB), far too
	 * little for those, and DataFusion's repartitioning would spill them.
	 */
	if (bytes <= 0)
		bytes = work_mem * 1024.0;
	return (int64) bytes;
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
	return x->send ? DF_QUERY_IPC_OUTPUT : 0;
}

/* Bit j set: input j arrives as batches from a Motion. */
static uint64
df_ipc_inputs(DfExec *x)
{
	uint64		mask = 0;
	int			j;

	for (j = 0; j < x->ninputs; j++)
		if (x->inputs[j].ipc)
			mask |= UINT64CONST(1) << j;
	return mask;
}

/* ---------------------------------------------------------------------
 * M7b: sending batches through a Gather Motion
 * ---------------------------------------------------------------------
 */

/*
 * Send 'len' bytes of the batch stream for receiver 'route' (0 for a
 * Gather; a Broadcast's one stream goes to every receiver), cut into tuple
 * chunks of DF_CHUNK_TYPE.  Sets x->stopped if the receiver asked the
 * senders to stop.
 */
static void
df_send_bytes(DfExec *x, int route, const uint8 *data, size_t len)
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
	if (motion->motionType == MOTIONTYPE_BROADCAST)
		route = BROADCAST_SEGIDX;
	if (first != NULL &&
		!CurrentMotionIPCLayer->SendTupleChunkToAMS(x->estate->interconnect_context,
													motion->motionID, (int16) route, first))
		x->stopped = true;
	MemoryContextReset(x->chunkcxt);
}

/* ---------------------------------------------------------------------
 * M7b: receiving batches from a Gather Motion
 * ---------------------------------------------------------------------
 */

/* Free the received chunks and give their receive buffer back. */
static void
df_rx_release(DfExec *x, DfInput *in, int16 motion_id)
{
	TupleChunkListItem item = in->rx_items;

	while (item != NULL)
	{
		TupleChunkListItem next = item->p_next;

		pfree(item);
		item = next;
	}
	in->rx_items = NULL;
	in->rx_nslices = 0;
	CurrentMotionIPCLayer->DirectPutRxBuffer(x->estate->interconnect_context,
											 motion_id, in->rx_route);
}

/*
 * Receive the next chunks, from whichever sender has some, and note their
 * payloads in in->rx_slices, checking the head of each stream.
 */
static void
df_rx_take(DfExec *x, DfInput *in, int16 motion_id, MotionNodeEntry *entry)
{
	MotionLayerState *ml = (MotionLayerState *) x->estate->motionlayer_context;
	TupleChunkListItem item;
	int16		route = ANY_ROUTE;
	MemoryContext oldcxt;

	if (x->estate->interconnect_context == NULL)
		ereport(ERROR, (errmsg("Interconnect is down unexpectedly.")));
	if (in->rx_head_seen == NULL)
	{
		in->rx_nroutes = entry->num_senders;
		in->rx_head_seen = MemoryContextAllocZero(x->estate->es_query_cxt,
												 sizeof(int) * Max(in->rx_nroutes, 1));
		in->rx_ended = MemoryContextAllocZero(x->estate->es_query_cxt,
											 sizeof(bool) * Max(in->rx_nroutes, 1));
	}

	/* As execMotionUnsortedReceiver and processIncomingChunks do. */
	x->estate->active_recv_id = motion_id;
	oldcxt = MemoryContextSwitchTo(ml->motion_layer_mctx);
	in->rx_items = CurrentMotionIPCLayer->RecvTupleChunkFromAny(x->estate->interconnect_context,
															   motion_id, &route);
	MemoryContextSwitchTo(oldcxt);
	in->rx_route = route;
	in->rx_nslices = 0;
	in->rx_eos = false;
	if (route < 0 || route >= in->rx_nroutes)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("datafusion: chunks from unexpected route %d of Motion %d",
						route, motion_id)));

	for (item = in->rx_items; item != NULL; item = item->p_next)
	{
		TupleChunkType type;
		uint8	   *data;
		size_t		len;
		int		   *seen = &in->rx_head_seen[route];

		if (item->chunk_length < TUPLE_CHUNK_HEADER_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("datafusion: tuple chunk of %u bytes from route %d of Motion %d",
							item->chunk_length, route, motion_id)));
		GetChunkType(item, &type);
		if (in->rx_eos)
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("datafusion: data after end-of-stream from route %d of Motion %d",
							route, motion_id)));
		if (type == TC_END_OF_STREAM)
		{
			in->rx_eos = true;
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

			if (memcmp(data, in->rx_head + *seen, k) != 0)
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
		if (in->rx_nslices == in->rx_maxslices)
		{
			in->rx_maxslices = Max(16, in->rx_maxslices * 2);
			in->rx_slices = in->rx_slices ?
				repalloc(in->rx_slices, sizeof(DfSlice) * in->rx_maxslices) :
				MemoryContextAlloc(x->estate->es_query_cxt, sizeof(DfSlice) * in->rx_maxslices);
		}
		in->rx_slices[in->rx_nslices].data = data;
		in->rx_slices[in->rx_nslices].len = len;
		in->rx_nslices++;
	}
}

/*
 * Batch-Motion input: hand the received bytes to DataFusion, receiving more
 * when they are taken.  Sets *pushed when chunks were taken, *full when the
 * queue was full.
 */
static void
df_exec_receive(DfExec *x, DfInput *in, bool *pushed, bool *full)
{
	char		buf[DF_MSG_BUFLEN];
	char		sqlstate[6] = "XX000";
	int16		motion_id = ((Motion *) in->motion->ps.plan)->motionID;
	MotionLayerState *ml = (MotionLayerState *) x->estate->motionlayer_context;
	MotionNodeEntry *entry = &ml->mnEntries[motion_id - 1];

	if (in->rx_items == NULL)
	{
		if (!entry->moreNetWork)
		{
			/* every sender has ended its stream */
			x->estate->active_recv_id = -1;
			df_ffi_query_finish_input_at(x->query, (uint32_t) in->index);
			in->done = true;
			return;
		}
		df_rx_take(x, in, motion_id, entry);
		if (in->rx_items == NULL)
			return;
	}

	if (in->rx_nslices > 0)
	{
		int32		status = df_ffi_query_push_ipc(x->query, (uint32_t) in->index,
												   in->rx_route, in->rx_slices,
												   (uint32_t) in->rx_nslices,
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
	if (in->rx_eos)
	{
		if (in->rx_ended[in->rx_route])
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("datafusion: second end-of-stream from route %d of Motion %d",
							in->rx_route, motion_id)));
		in->rx_ended[in->rx_route] = true;
		entry->num_stream_ends_recvd++;
		if (entry->num_stream_ends_recvd == entry->num_senders)
			entry->moreNetWork = false;
		CurrentMotionIPCLayer->DeregisterReadInterest(x->estate->interconnect_context,
													  motion_id, in->rx_route,
													  "end of stream");
	}
	df_rx_release(x, in, motion_id);

	/*
	 * Taken, even without bytes for DataFusion: a stream's head alone (the
	 * first packet of each sender, as its first chunk of data fills a
	 * packet of its own) or its end.  The caller must come back for more
	 * at once, not wait for results: while it waits, nothing here takes
	 * the senders' packets or acknowledges them, and they stall.
	 */
	*pushed = true;
}

/* Does input 'in' scan a PAX table the direct reader may read? */
static bool
df_input_scans_pax(DfInput *in)
{
	char	   *amname;

	if (in->scan == NULL || in->scan->ss.ss_currentScanDesc != NULL)
		return false;			/* a Motion, or Cloudberry's parallel mode */
	amname = get_am_name(in->scan->ss.ss_currentRelation->rd_rel->relam);
	return amname != NULL && strcmp(amname, "pax") == 0;
}

/*
 * Begin a direct read of input 'in' if it scans a PAX table
 * (C3: any input of the slice, the others read through the table AM or a
 * Motion): DataFusion's partitions decode its micro-partitions themselves.
 * Returns false when the direct reader does not apply; 'pax' then stays
 * empty.  The scan's min/max skipping is added to the slice's.
 */
static bool
df_pax_begin_input(DfExec *x, DfInput *in, const DfPaxReader *reader, DfPaxInput *pax)
{
	Relation	rel;
	char		buf[DF_MSG_BUFLEN];
	DfPaxScanInfo info;
	int		   *cols;
	int		   *widths;
	void	   *scan;
	int			c;

	if (!df_input_scans_pax(in))
		return false;
	rel = in->scan->ss.ss_currentRelation;
	/*
	 * The reader hands out fixed-width values, strings and numerics, of the
	 * table's own columns (not ctid).
	 */
	for (c = 0; c < in->spec->ncols; c++)
		if (in->spec->attnos[c] <= 0)
			return false;

	cols = palloc(sizeof(int) * Max(in->spec->ncols, 1));
	widths = palloc(sizeof(int) * Max(in->spec->ncols, 1));
	for (c = 0; c < in->spec->ncols; c++)
	{
		cols[c] = in->spec->attnos[c] - 1;
		/* strings and numerics as their bytes (C1, C2) */
		widths[c] = df_type_is_string(in->spec->types[c]) ||
			in->spec->types[c] == NUMERICOID ? -1 :
			df_type_width(in->spec->types[c]);
	}
	/*
	 * The scan's qual only lets PAX skip micro-partitions and groups by
	 * their min/max statistics; DataFusion still filters every row.
	 */
	scan = reader->begin(rel, x->estate->es_snapshot, in->scan->ss.ps.plan->qual,
						 cols, widths, in->spec->ncols, buf, sizeof(buf));
	pfree(cols);
	pfree(widths);
	if (scan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("datafusion: %s", buf)));
	reader->info(scan, &info);
	x->pax_info.files += info.files;
	x->pax_info.files_skipped += info.files_skipped;
	x->pax_info.groups += info.groups;
	x->pax_info.groups_skipped += info.groups_skipped;
	x->pax_direct = true;
	pax->scan = scan;
	pax->nblocks = (uint32_t) reader->nblocks(scan);
	in->done = true;			/* nothing to push from the main thread */
	df_pax_direct_scans++;
	return true;
}

/* The top slice of slice 'index''s tree (parentIndex -1). */
static int
df_slice_tree_root(PlannedStmt *stmt, int index)
{
	int			steps = 0;

	while (stmt->slices[index].parentIndex >= 0 &&
		   stmt->slices[index].parentIndex < stmt->numSlices &&
		   steps++ < stmt->numSlices)
		index = stmt->slices[index].parentIndex;
	return index;
}

/*
 * The slices of 'stmt' that run while slice 'index' does: those of its
 * slice tree.  An init plan's slices form a tree of their own, run before
 * the main plan's.
 */
static int
df_slices_together(PlannedStmt *stmt, int index)
{
	int			root;
	int			n = 0;
	int			i;

	if (stmt == NULL || index < 0 || index >= stmt->numSlices)
		return 1;
	root = df_slice_tree_root(stmt, index);
	for (i = 0; i < stmt->numSlices; i++)
		if (df_slice_tree_root(stmt, i) == root)
			n++;
	return Max(n, 1);
}

/*
 * Primaries on this host, from gp_segment_configuration, the coordinator's
 * among them: a QE gets the count from the coordinator as it starts
 * (gpqeid), the coordinator looks its own host up.  At least 1.  Counts the
 * roles in effect, so a host that took over its mirrors' primaries counts
 * them.
 */
static int
df_host_segments(void)
{
	int			n = host_primary_segment_count;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbComponentDatabaseInfo *qd = cdbcomponent_getComponentInfo(MASTER_CONTENT_ID);

		n = qd ? qd->hostPrimaryCount : 0;
	}
	return Max(n, 1);
}

/*
 * Partitions of a query, out of 'workers' threads, while 'slices' slices of
 * the plan run together in each of 'segments' primaries on the host:
 * datafusion.slice_partitions, else 8 * workers / (slices * segments), at
 * least 1, at most 'workers'.  Every slice that took a partition per thread
 * made the slices of a plan of many crowd the host's cores: TPC-DS at scale
 * factor 1, 3 segments on 10 cores, took 100 s with 10 partitions, 89 s
 * with 1 and 88 s with 2; plans of more than 10 slices ran fastest with 1,
 * of 5 to 10 with 2, of fewer with any (4 primaries there, with the
 * coordinator's).  Counting them keeps a host of one segment busy and one
 * of many from crowding more.
 */
static int
df_query_partitions(int workers, int slices, int segments)
{
	int			n = df_slice_partitions > 0 ? df_slice_partitions :
		8 * workers / (Max(slices, 1) * Max(segments, 1));

	return Max(Min(n, workers), 1);
}

static void
df_exec_begin(DfExec *x)
{
	char		buf[DF_MSG_BUFLEN];
	char		sqlstate[6] = "XX000";
	int			workers;
	int32		status;
	int			j;
	const DfPaxReader *reader = NULL;
	DfPaxInput *pax = NULL;

	workers = df_runtime_ensure();
	if (df_pax_direct_read)
	{
		/* loaded only once a PAX table is read: pax.so is loaded by then */
		for (j = 0; j < x->ninputs && reader == NULL; j++)
			if (df_input_scans_pax(&x->inputs[j]))
				reader = df_pax_reader_get();
	}
	if (reader != NULL)
	{
		pax = palloc0(sizeof(DfPaxInput) * x->ninputs);
		for (j = 0; j < x->ninputs; j++)
		{
			PG_TRY();
			{
				df_pax_begin_input(x, &x->inputs[j], reader, &pax[j]);
			}
			PG_CATCH();
			{
				int			k;

				/* the scans begun so far are not the query's yet */
				for (k = 0; k < j; k++)
					if (pax[k].scan != NULL)
						reader->end(pax[k].scan);
				PG_RE_THROW();
			}
			PG_END_TRY();
		}
	}
	/* From here on the query owns the PAX scans, even if this fails. */
	status = df_ffi_query_start(x->spec.json,
								(uint32_t) df_query_partitions(workers, x->slices_together,
															  x->host_segments),
								(uint64_t) x->memory_limit, df_spill_dir(),
								df_query_flags(x), (uint32_t) x->ninputs, df_ipc_inputs(x),
								pax, reader ? reader->read : NULL,
								reader ? reader->end : NULL,
								reader ? reader->block_groups : NULL,
								(uint32_t) df_pax_max_decoders,
								&x->query, sqlstate, buf, sizeof(buf));
	if (status != DF_OK)
	{
		x->query = NULL;
		df_raise_query(status, sqlstate, buf);
	}
	df_vmem_sync(x->headroom);

	for (j = 0; j < x->ninputs; j++)
	{
		DfInput    *in = &x->inputs[j];
		Relation	rel;

		if (in->scan == NULL || in->done)
			continue;			/* a Motion is ready to receive, or PAX read directly */

		/*
		 * In Cloudberry's parallel mode several QEs of one segment share the
		 * scan: before the first row is requested, ExecutePlan has already
		 * begun a parallel scan on the Seq Scan node (GpInsertParallelDSMHash),
		 * which hands each QE its own part of the table.  Use it.
		 *
		 * Otherwise begin the scan the way the Seq Scan node would.  Column
		 * stores (AOCS, PAX) take the node's PlanState to read only the
		 * columns its targetlist and filter use; PAX also skips
		 * micro-partitions whose min/max statistics rule the filter out.
		 * DataFusion still applies the whole filter to every row it receives.
		 */
		rel = in->scan->ss.ss_currentRelation;
		if (in->scan->ss.ss_currentScanDesc != NULL)
			in->scandesc = in->scan->ss.ss_currentScanDesc;
		else if (rel->rd_tableam->scan_begin_extractcolumns)
			in->scandesc = table_beginscan_es(rel, x->estate->es_snapshot, 0, NULL,
											  NULL, &in->scan->ss.ps);
		else
			in->scandesc = table_beginscan(rel, x->estate->es_snapshot, 0, NULL);
		in->scan->ss.ss_currentScanDesc = in->scandesc; /* ExecEndSeqScan closes it */
	}
}

/*
 * Read up to the slice's rows per batch (at most DF_BATCH_ROWS) of the
 * needed columns of input 'in'.
 */
/*
 * PS1: a NumericVar as numericvar_serialize wrote it at 'p' (int32 ndigits,
 * weight, sign and display scale, int16 digits), as a numeric: numeric_recv
 * reads the same fields as int16.  Returns the bytes read.
 */
static int
df_recv_numericvar(const char *p, int len, Datum *out)
{
	StringInfoData wire;
	int32		f[4];
	int			i;

	if (len < 16)
		elog(ERROR, "datafusion: short partial aggregate state");
	for (i = 0; i < 4; i++)
		f[i] = (int32) pg_ntoh32(*(const uint32 *) (p + 4 * i));
	if (f[0] < 0 || len < 16 + 2 * f[0])
		elog(ERROR, "datafusion: short partial aggregate state");
	initStringInfo(&wire);
	for (i = 0; i < 4; i++)
		pq_sendint16(&wire, (int16) f[i]);
	pq_sendbytes(&wire, p + 16, 2 * f[0]);
	*out = DirectFunctionCall3(numeric_recv, PointerGetDatum(&wire),
							   ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
	return 16 + 2 * f[0];
}

/*
 * PS1: part 'part' of PostgreSQL's partial state 'd' of format 'format': its
 * sum, NULL without values and NaN after a NaN, or its count of values.
 */
static Datum
df_state_part(Datum d, uint8 format, uint8 part, bool *isnull)
{
	int64		count;
	Datum		sum = (Datum) 0;

	*isnull = false;
	if (format == DF_STATE_INT_ARRAY)
	{
		ArrayType  *a = DatumGetArrayTypeP(d);
		Datum	   *elems;
		int			n;

		deconstruct_array(a, INT8OID, sizeof(int64), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE,
						  &elems, NULL, &n);
		if (n != 2)
			elog(ERROR, "datafusion: unexpected avg state");
		count = DatumGetInt64(elems[0]);
		sum = NumericGetDatum(int64_to_numeric(DatumGetInt64(elems[1])));
	}
	else
	{
		bytea	   *b = DatumGetByteaPP(d);
		const char *p = VARDATA_ANY(b);
		int			len = VARSIZE_ANY_EXHDR(b);
		int			used;

		if (len < 8)
			elog(ERROR, "datafusion: short partial aggregate state");
		count = (int64) pg_ntoh64(*(const uint64 *) p);
		used = 8 + df_recv_numericvar(p + 8, len - 8, &sum);
		if (format == DF_STATE_NUMERIC)
		{
			int64		nans,
						pinfs,
						ninfs;

			/* maxScale (int32) and maxScaleCount (int64): an inverse transition's */
			if (len < used + 12 + 24)
				elog(ERROR, "datafusion: short partial aggregate state");
			nans = (int64) pg_ntoh64(*(const uint64 *) (p + used + 12));
			pinfs = (int64) pg_ntoh64(*(const uint64 *) (p + used + 20));
			ninfs = (int64) pg_ntoh64(*(const uint64 *) (p + used + 28));
			if (pinfs > 0 || ninfs > 0)
				elog(ERROR, "datafusion: infinite numeric in a partial aggregate state");
			if (nans > 0)
				sum = DirectFunctionCall3(numeric_in, CStringGetDatum("NaN"),
										  ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
			/* the count of values but NaNs; with one, the result is NaN */
			if (count == 0 && nans > 0)
				count = nans;
		}
	}
	if (part == DF_PART_COUNT)
		return Int64GetDatum(count);
	*isnull = (count == 0);
	return sum;
}

static void
df_exec_fill(DfExec *x, DfInput *in)
{
	TupleTableSlot *slot = in->scan ? in->scan->ss.ss_ScanTupleSlot : NULL;
	int			n = 0;
	int			c;
	bool		strings_full = false;

	while (n < x->spec.batch_rows && !strings_full)
	{
		if (in->motion || in->reader)
		{
			/*
			 * The Motion's own receive path, with its instrumentation; a
			 * Shared Scan reader's, which waits for its producer.
			 */
			slot = ExecProcNode(in->motion ? &in->motion->ps : in->reader);
			if (TupIsNull(slot))
			{
				in->ended = true;
				break;
			}
		}
		else if (!table_scan_getnextslot(in->scandesc, ForwardScanDirection, slot))
		{
			in->ended = true;
			break;
		}
		if (in->maxattno > 0)
			slot_getsomeattrs(slot, in->maxattno);
		for (c = 0; c < in->spec->ncols; c++)
		{
			int			att = (in->tupatt ? in->tupatt[c] : in->spec->attnos[c]) - 1;
			bool		isnull;
			Datum		d;
			char	   *dst = in->invalues[c];

			if (att < 0)
				/* TID1: ctid or gp_segment_id */
				d = slot_getsysattr(slot, att + 1, &isnull);
			else
			{
				isnull = slot->tts_isnull[att];
				d = slot->tts_values[att];
			}
			if (in->statepart && in->statepart[c] != DF_PART_NONE)
			{
				/* PS1: a part of PostgreSQL's partial state */
				MemoryContext oldcxt = MemoryContextSwitchTo(in->statecxt);

				MemoryContextReset(in->statecxt);
				if (isnull)
				{
					/*
					 * No values: a partial aggregate without groups over
					 * no rows keeps its NULL initial state, which the
					 * strict serial function leaves NULL
					 */
					isnull = in->statepart[c] == DF_PART_SUM;
					d = Int64GetDatum(0);
				}
				else
					d = df_state_part(d, in->stateformat[c], in->statepart[c], &isnull);
				MemoryContextSwitchTo(oldcxt);
			}

			in->innulls[c][n] = isnull ? 1 : 0;
			switch (in->spec->types[c])
			{
				case BOOLOID:
					((uint8 *) dst)[n] = isnull ? 0 : (DatumGetBool(d) ? 1 : 0);
					break;
				case INT2OID:
					((int16 *) dst)[n] = isnull ? 0 : DatumGetInt16(d);
					break;
				case INT4OID:
				case DATEOID:
					if (in->spec->scales[c] == DF_SCALE_DSCALE)
						/* DN1: the display scale of the numeric value */
						((int32 *) dst)[n] = isnull ? 0 : df_numeric_dscale(d);
					else
						((int32 *) dst)[n] = isnull ? 0 : DatumGetInt32(d);
					break;
				case INT8OID:
				case TIMEOID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					((int64 *) dst)[n] = isnull ? 0 : DatumGetInt64(d);
					break;
				case TIDOID:
					/* TID1: block << 16 | offset, ordered as tids are */
					((int64 *) dst)[n] = isnull ? 0 :
						((int64) ItemPointerGetBlockNumberNoCheck(DatumGetItemPointer(d)) << 16) |
						ItemPointerGetOffsetNumberNoCheck(DatumGetItemPointer(d));
					break;
				case FLOAT4OID:
					((float4 *) dst)[n] = isnull ? 0 : DatumGetFloat4(d);
					break;
				case FLOAT8OID:
					((float8 *) dst)[n] = isnull ? 0 : DatumGetFloat8(d);
					break;
				case NUMERICOID:
					{
						uint8	   *out = (uint8 *) dst + (size_t) n * DF_NUMERIC_BYTES;

						/*
						 * An infinity can only be a year of an infinite date
						 * (DT2), which the planner hook allows
						 */
						if (isnull)
							df_numeric_store(0, out);
						else if (df_numeric_value_wide(d, in->spec->scales[c], out) ==
								 DF_NUMERIC_TOO_LONG)
							elog(ERROR, "datafusion: numeric value beyond numeric(%d, %d)",
								 DF_NUMERIC_MAX_EXPR_PRECISION, in->spec->scales[c]);
					}
					break;
				case TEXTOID:
				case VARCHAROID:
				case BPCHAROID:
					{
						int32	   *off = in->inoffsets[c];
						Size		end = off[n];

						if (!isnull)
						{
							struct varlena *v = (struct varlena *) DatumGetPointer(d);
							struct varlena *p = pg_detoast_datum_packed(v);
							Size		len = VARSIZE_ANY_EXHDR(p);

							if (end + len > in->incap[c])
							{
								in->incap[c] = Max(in->incap[c] * 2, end + len);
								in->invalues[c] = repalloc_huge(in->invalues[c], in->incap[c]);
							}
							memcpy(in->invalues[c] + end, VARDATA_ANY(p), len);
							if (p != v)
								pfree(p);
							end += len;
						}
						off[n + 1] = (int32) end;
						if (end >= DF_BATCH_STRING_BYTES)
							strings_full = true;
					}
					break;
			}
		}
		n++;
	}
	for (c = 0; c < in->spec->ncols; c++)
		in->incols[c].values = in->invalues[c];	/* moved if it grew */
	in->batch_rows = n;
	in->batch_ready = (n > 0);
	if (in->ended && !in->batch_ready)
	{
		df_ffi_query_finish_input_at(x->query, (uint32_t) in->index);
		in->done = true;
	}
}

/*
 * Feed input 'in' one step: push the batch it has ready, reading the next
 * one if needed.  Sets *pushed or *full as df_exec_receive does.
 */
static void
df_exec_feed(DfExec *x, DfInput *in, bool *pushed, bool *full)
{
	char		buf[DF_MSG_BUFLEN];
	char		sqlstate[6] = "XX000";
	int32		status;

	if (in->ipc)
	{
		df_exec_receive(x, in, pushed, full);
		return;
	}
	if (!in->batch_ready)
		df_exec_fill(x, in);
	if (!in->batch_ready)
		return;
	status = df_ffi_query_push(x->query, (uint32_t) in->index, in->incols,
							   (uint32_t) in->spec->ncols, (uint32_t) in->batch_rows,
							   sqlstate, buf, sizeof(buf));
	if (status == DF_OK)
	{
		*pushed = true;
		in->batch_ready = false;
		if (in->ended)
		{
			df_ffi_query_finish_input_at(x->query, (uint32_t) in->index);
			in->done = true;
		}
	}
	else if (status == DF_PENDING)
		*full = true;
	else
		df_raise_query(status, sqlstate, buf);
}

/* Row 'r' of a Decimal256 column. */
static const uint8 *
df_numeric_at(const void *values, uint32 r)
{
	return (const uint8 *) values + (size_t) r * DF_NUMERIC_BYTES;
}

/*
 * PS1: numeric 'num' as numericvar_serialize writes a NumericVar: ndigits,
 * weight, sign and display scale as int32, then the base-10000 digits.
 * numeric_send gives the same fields as int16.
 */
static void
df_send_numericvar(StringInfo buf, Datum num)
{
	bytea	   *wire = DatumGetByteaPP(DirectFunctionCall1(numeric_send, num));
	const char *p = VARDATA_ANY(wire);
	int16		f[4];
	int			i;

	for (i = 0; i < 4; i++)
		f[i] = (int16) pg_ntoh16(*(const uint16 *) (p + 2 * i));
	for (i = 0; i < 4; i++)
		pq_sendint32(buf, f[i]);
	pq_sendbytes(buf, p + 8, 2 * f[0]);
}

/*
 * PS1: the state numeric_avg_serialize writes for 'count' values (NaNs
 * among them) adding up to 'sum', each of display scale 'scale': PostgreSQL
 * counts those of the largest scale for an inverse transition, and sums
 * all but the NaNs, which make the result NaN.
 */
static Datum
df_numeric_state(Datum sum, bool sumnull, int scale, int64 count)
{
	StringInfoData buf;
	bool		nan = !sumnull && numeric_is_nan(DatumGetNumeric(sum));

	pq_begintypsend(&buf);
	pq_sendint64(&buf, count);
	df_send_numericvar(&buf, sumnull || nan || count == 0 ?
					   NumericGetDatum(int64_to_numeric(0)) : sum);
	pq_sendint32(&buf, count == 0 ? 0 : scale);	/* maxScale */
	pq_sendint64(&buf, count == 0 ? 0 : count - (nan ? 1 : 0));	/* maxScaleCount */
	pq_sendint64(&buf, nan ? 1 : 0);	/* NaNcount */
	pq_sendint64(&buf, 0);		/* pInfcount: infinities stay on PostgreSQL */
	pq_sendint64(&buf, 0);		/* nInfcount */
	return PointerGetDatum(pq_endtypsend(&buf));
}

/* PS1: the state int8_avg_serialize writes: the count, the sum (an int128). */
static Datum
df_int8_state(Datum sum, bool sumnull, int64 count)
{
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, count);
	df_send_numericvar(&buf, sumnull || count == 0 ?
					   NumericGetDatum(int64_to_numeric(0)) : sum);
	return PointerGetDatum(pq_endtypsend(&buf));
}

static TupleTableSlot *
df_exec_emit(DfExec *x)
{
	TupleTableSlot *slot = x->valslot ? x->valslot : x->outslot;
	uint32		r = x->out_row++;
	int			c,
				k = 0;			/* the tuple's column */

	ExecClearTuple(slot);
	MemoryContextReset(x->rowcxt);
	for (c = 0; c < x->spec.nout; c++)
	{
		const void *v = x->outcols[c].values;
		bool		isnull = x->outcols[c].nulls[r] != 0;
		uint8		kind = x->spec.out_kinds ? x->spec.out_kinds[c] : DF_OUT_PLAIN;
		MemoryContext oldcxt;

		if (kind == DF_OUT_PART)
			continue;
		if (kind == DF_OUT_NUMERIC_MIXED)
		{
			/*
			 * MS1: a sum at the largest scale of its argument's branches,
			 * shown at the largest scale of the values added up, as
			 * PostgreSQL's sum does; the digits past it are 0
			 */
			int			scale = x->spec.out_scales[c];
			int			shown = x->outcols[c + 1].nulls[r] ? scale :
				((const int32 *) x->outcols[c + 1].values)[r];

			slot->tts_isnull[k] = isnull;
			slot->tts_values[k] = (Datum) 0;
			if (!isnull)
			{
				oldcxt = MemoryContextSwitchTo(x->rowcxt);
				slot->tts_values[k] = df_numeric_datum(df_numeric_at(v, r), scale);
				if (shown < scale)
					slot->tts_values[k] = DirectFunctionCall2(numeric_round, slot->tts_values[k],
															  Int32GetDatum(shown));
				MemoryContextSwitchTo(oldcxt);
			}
			k++;
			continue;
		}
		if (kind == DF_OUT_STATE_NUMERIC || kind == DF_OUT_STATE_INT8 ||
			kind == DF_OUT_STATE_INT_ARRAY)
		{
			/*
			 * PS1: PostgreSQL's state of a partial sum or avg, for the
			 * combining aggregate of the slice that reads these tuples.  A
			 * partial aggregate's state is never NULL: it has a count of 0.
			 * PostgreSQL shares one state among aggregates of one transition
			 * function and argument (sum and avg of a column) and combines
			 * it from one of their columns: the states agree, count and all.
			 */
			int64		count = x->outcols[c + 1].nulls[r] ? 0 :
				((const int64 *) x->outcols[c + 1].values)[r];
			Datum		sum = (Datum) 0;

			oldcxt = MemoryContextSwitchTo(x->rowcxt);
			if (!isnull)
				sum = df_numeric_datum(df_numeric_at(v, r), x->spec.out_scales[c]);
			if (kind == DF_OUT_STATE_NUMERIC)
				slot->tts_values[k] = df_numeric_state(sum, isnull, x->spec.out_scales[c], count);
			else if (kind == DF_OUT_STATE_INT_ARRAY)
			{
				/* int4_avg_accum's Int8TransTypeData: {count, sum} */
				Datum		elems[2];

				elems[0] = Int64GetDatum(count);
				elems[1] = isnull || count == 0 ? Int64GetDatum(0) :
					DirectFunctionCall1(numeric_int8, sum);
				slot->tts_values[k] = PointerGetDatum(construct_array(elems, 2, INT8OID, sizeof(int64),
																	  FLOAT8PASSBYVAL, TYPALIGN_DOUBLE));
			}
			else
				slot->tts_values[k] = df_int8_state(sum, isnull, count);
			MemoryContextSwitchTo(oldcxt);
			slot->tts_isnull[k] = false;
			k++;
			continue;
		}
		if (kind == DF_OUT_NUMERIC_AVG)
		{
			/* avg(int) = numeric_div(sum, count), as int8_avg and numeric_poly_avg */
			int64		count = x->outcols[c + 1].nulls[r] ? 0 :
				((const int64 *) x->outcols[c + 1].values)[r];

			slot->tts_isnull[k] = isnull || count == 0;
			slot->tts_values[k] = (Datum) 0;
			if (!slot->tts_isnull[k])
			{
				oldcxt = MemoryContextSwitchTo(x->rowcxt);
				slot->tts_values[k] =
					DirectFunctionCall2(numeric_div,
										df_numeric_datum(df_numeric_at(v, r),
														 x->spec.out_scales[c]),
										NumericGetDatum(int64_to_numeric(count)));
				MemoryContextSwitchTo(oldcxt);
			}
			k++;
			continue;
		}
		slot->tts_isnull[k] = isnull;
		if (isnull)
		{
			slot->tts_values[k++] = (Datum) 0;
			continue;
		}
		switch (x->spec.out_types[c])
		{
			case BOOLOID:
				slot->tts_values[k] = BoolGetDatum(((const uint8 *) v)[r] != 0);
				break;
			case INT2OID:
				slot->tts_values[k] = Int16GetDatum(((const int16 *) v)[r]);
				break;
			case INT4OID:
			case DATEOID:
				slot->tts_values[k] = Int32GetDatum(((const int32 *) v)[r]);
				break;
			case INT8OID:
			case TIMEOID:
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
				slot->tts_values[k] = Int64GetDatum(((const int64 *) v)[r]);
				break;
			case TIDOID:
				{
					/* TID1 */
					int64		t = ((const int64 *) v)[r];
					ItemPointer ip;

					oldcxt = MemoryContextSwitchTo(x->rowcxt);
					ip = palloc(sizeof(ItemPointerData));
					MemoryContextSwitchTo(oldcxt);
					ItemPointerSet(ip, (BlockNumber) (t >> 16), (OffsetNumber) (t & 0xFFFF));
					slot->tts_values[k] = ItemPointerGetDatum(ip);
				}
				break;
			case FLOAT4OID:
				slot->tts_values[k] = Float4GetDatum(((const float4 *) v)[r]);
				break;
			case FLOAT8OID:
				slot->tts_values[k] = Float8GetDatum(((const float8 *) v)[r]);
				break;
			case TEXTOID:
			case VARCHAROID:
			case BPCHAROID:
				{
					const int32 *off = x->outcols[c].offsets;

					oldcxt = MemoryContextSwitchTo(x->rowcxt);
					slot->tts_values[k] =
						PointerGetDatum(cstring_to_text_with_len((const char *) v + off[r],
																 off[r + 1] - off[r]));
					MemoryContextSwitchTo(oldcxt);
				}
				break;
			case NUMERICOID:
				oldcxt = MemoryContextSwitchTo(x->rowcxt);
				slot->tts_values[k] = df_numeric_datum(df_numeric_at(v, r),
													   x->spec.out_scales[c]);
				MemoryContextSwitchTo(oldcxt);
				break;
		}
		k++;
	}
	ExecStoreVirtualTuple(slot);
	if (x->valslot == NULL)
		return slot;

	/* P1: the columns PostgreSQL finishes, over the values */
	ExecClearTuple(x->outslot);
	ResetExprContext(x->tailcxt);
	x->tailcxt->ecxt_outertuple = x->valslot;
	for (k = 0; k < x->spec.ncols; k++)
	{
		int			v = x->spec.col_value[k];

		if (v >= 0)
		{
			x->outslot->tts_values[k] = slot->tts_values[v];
			x->outslot->tts_isnull[k] = slot->tts_isnull[v];
		}
		else
			x->outslot->tts_values[k] =
				ExecEvalExprSwitchContext(x->tail_states[k], x->tailcxt,
										  &x->outslot->tts_isnull[k]);
	}
	return ExecStoreVirtualTuple(x->outslot);
}

/*
 * Run the Shared Scan producers of the slice's Sequences to their end, as
 * ExecSequence does before reading its last input (completeSubplan): each
 * writes the store its readers share, and readers of this slice, pulled as
 * inputs, find it complete.
 */
static void
df_exec_prerun(DfExec *x)
{
	int			i;

	for (i = 0; i < x->nprerun; i++)
	{
		while (!TupIsNull(ExecProcNode(x->prerun[i])))
			CHECK_FOR_INTERRUPTS();
		CHECK_FOR_INTERRUPTS();
	}
}

static TupleTableSlot *
df_exec_next(DfExec *x)
{
	char		buf[DF_MSG_BUFLEN];
	char		sqlstate[6] = "XX000";

	if (x->query == NULL && !x->done)
	{
		df_exec_prerun(x);
		df_exec_begin(x);
	}

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

		/*
		 * Feed the inputs one after the other, each to its end, in the order
		 * the translator gave them: a join's build side first, as
		 * PostgreSQL's Hash Join reads it, which also keeps its
		 * interconnect deadlock-free (prefetch_inner).
		 */
		if (!x->input_done)
		{
			int			j;

			/* SP1: a subquery's rows first, which DataFusion reads whole first */
			for (j = 0; j < x->ninputs && (x->inputs[j].done || !x->inputs[j].spec->set); j++)
				;
			if (j == x->ninputs)
				for (j = 0; j < x->ninputs && x->inputs[j].done; j++)
					;
			if (j == x->ninputs)
				x->input_done = true;
			else
			{
				df_exec_feed(x, &x->inputs[j], &pushed, &full);

				/*
				 * An input that has just ended is progress too: go on to the
				 * next one at once.  Waiting for results first would wait in
				 * vain, as long as DataFusion needs that next input to
				 * produce any (a join's probe side after its build side).
				 */
				if (x->inputs[j].done)
					pushed = true;
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
			int32		route;

			/* Our results, for one of the Motion's receivers. */
			df_ffi_query_bytes(x->query, &route, &data, &len);
			if (route < 0)
				route = 0;		/* a Gather's only receiver */
			if (route >= x->send_nroutes)
				elog(ERROR, "datafusion: route %d of a Motion with %d receivers",
					 route, x->send_nroutes);
			if (!x->head_sent_to[route])
			{
				df_send_bytes(x, route, x->tx_head, DF_BATCH_HEAD);
				x->head_sent_to[route] = true;
			}
			if (!x->stopped)
				df_send_bytes(x, route, data, len);
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
df_batch_head(uint8 *head, PlannedStmt *stmt, Motion *motion)
{
	uint64		sig = df_motion_signature(stmt, motion);

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
/*
 * The PlanState of 'leaf' below 'ps' in this slice: not below a Motion,
 * which belongs to another slice.
 */
static PlanState *
df_find_state(PlanState *ps, Plan *leaf)
{
	PlanState  *found;

	if (ps == NULL)
		return NULL;
	if (ps->plan == leaf)
		return ps;
	if (ps->subPlan != NIL)
	{
		/* SP1: a subquery's rows, pulled from the top of its plan */
		ListCell   *lc;

		foreach(lc, ps->subPlan)
		{
			SubPlanState *sps = lfirst_node(SubPlanState, lc);

			if (sps->planstate != NULL && sps->planstate->plan == leaf)
				return sps->planstate;
			/* SC1: the Motion below a correlated one's aggregate */
			if (sps->planstate != NULL &&
				(found = df_find_state(sps->planstate, leaf)) != NULL)
				return found;
		}
	}
	if (IsA(ps, MotionState))
		return NULL;
	if (IsA(ps, SubqueryScanState))
		return df_find_state(((SubqueryScanState *) ps)->subplan, leaf);
	if (IsA(ps, AppendState))
	{
		AppendState *as = (AppendState *) ps;
		int			i;

		for (i = 0; i < as->as_nplans; i++)
			if ((found = df_find_state(as->appendplans[i], leaf)) != NULL)
				return found;
		return NULL;
	}
	if (IsA(ps, MergeAppendState))
	{
		/* UA1: run as an Append where nothing reads its order */
		MergeAppendState *ms = (MergeAppendState *) ps;
		int			i;

		for (i = 0; i < ms->ms_nplans; i++)
			if ((found = df_find_state(ms->mergeplans[i], leaf)) != NULL)
				return found;
		return NULL;
	}
	if (IsA(ps, SequenceState))
	{
		SequenceState *ss = (SequenceState *) ps;
		int			i;

		for (i = 0; i < ss->numSubplans; i++)
			if ((found = df_find_state(ss->subplans[i], leaf)) != NULL)
				return found;
		return NULL;
	}
	found = df_find_state(outerPlanState(ps), leaf);
	return found ? found : df_find_state(innerPlanState(ps), leaf);
}

/* The PlanState of 'plan' below 'ps' in this slice, or NULL (T2). */
PlanState *
df_exec_find_state(PlanState *ps, Plan *plan)
{
	return df_find_state(ps, plan);
}

/*
 * PS1: map the columns input 'in' reads from Motion of tuples 'motion' to
 * its tuple's columns.  The translator numbers them as stream columns, a
 * partial aggregate's state as DataFusion's (M7d), sum then count: here
 * that state is PostgreSQL's, in one column.
 */
static void
df_input_states(DfInput *in, Motion *motion)
{
	int			ncols = list_length(motion->plan.targetlist);
	int			c;

	for (c = 1; c <= ncols; c++)
		if (df_motion_state_columns(motion, c) > 0)
			break;
	if (c > ncols)
		return;					/* no states: stream and tuple columns agree */
	in->tupatt = palloc0(sizeof(AttrNumber) * Max(in->spec->ncols, 1));
	in->statepart = palloc0(sizeof(uint8) * Max(in->spec->ncols, 1));
	in->stateformat = palloc0(sizeof(uint8) * Max(in->spec->ncols, 1));
	in->statecxt = AllocSetContextCreate(CurrentMemoryContext, "datafusion states",
										 ALLOCSET_SMALL_SIZES);
	for (c = 0; c < in->spec->ncols; c++)
	{
		int			pos = in->spec->attnos[c] - 1;
		int			start = 0;
		AttrNumber	r;

		if (pos < 0)
		{
			in->tupatt[c] = in->spec->attnos[c];	/* a system column */
			continue;
		}
		for (r = 1; r <= ncols; r++)
		{
			int			width = df_motion_state_columns(motion, r);

			if (pos < start + Max(width, 1))
			{
				in->tupatt[c] = r;
				if (width > 0)
				{
					Plan	   *agg = NULL;
					Aggref	   *partial = df_motion_column_partial(motion, r, &agg);
					DfAggState	state = df_agg_state_at(agg, partial);

					in->statepart[c] = pos == start ? DF_PART_SUM : DF_PART_COUNT;
					in->stateformat[c] =
						state == DF_AGG_SUM_NUMERIC || state == DF_AGG_AVG_NUMERIC ?
						DF_STATE_NUMERIC :
						state == DF_AGG_AVG_INT && linitial_oid(partial->aggargtypes) != INT8OID ?
						DF_STATE_INT_ARRAY : DF_STATE_INT8;
				}
				break;
			}
			start += Max(width, 1);
		}
		if (r > ncols)
			elog(ERROR, "datafusion: stream column %d beyond Motion %d", pos, motion->motionID);
	}
}

bool
df_exec_attach(QueryDesc *queryDesc, PlanState *root, MotionState *send,
			   const DfTails *tails, char *reason, size_t reasonlen)
{
	EState	   *estate = queryDesc->estate;
	MemoryContext oldcxt;
	DfExec	   *x;
	volatile bool ok = false;
	int			c,
				j;

	oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);
	x = palloc0(sizeof(DfExec));
	/* IP1: init plan values become constants of the plan */
	df_param_values = estate->es_param_exec_vals;
	df_param_econtext = GetPerTupleExprContext(estate);
	df_numeric_stmt = queryDesc->plannedstmt;
	PG_TRY();
	{
		ok = df_translate_slice(root->plan, tails, send == NULL, &x->spec, reason, reasonlen);
	}
	PG_FINALLY();
	{
		df_param_values = NULL;
		df_param_econtext = NULL;
		df_numeric_stmt = NULL;
	}
	PG_END_TRY();
	if (!ok)
	{
		MemoryContextSwitchTo(oldcxt);
		pfree(x);
		return false;
	}

	/*
	 * Each input's executor state: a Seq Scan, a Motion receiving, or a
	 * Shared Scan reader.
	 */
	x->ninputs = x->spec.ninputs;
	x->inputs = palloc0(sizeof(DfInput) * Max(x->ninputs, 1));
	for (j = 0; j < x->ninputs; j++)
	{
		DfInput    *in = &x->inputs[j];
		PlanState  *ps = df_find_state(root, x->spec.inputs[j].leaf);

		in->index = j;
		in->spec = &x->spec.inputs[j];
		if (ps != NULL && IsA(ps, SeqScanState))
			in->scan = (SeqScanState *) ps;
		else if (ps != NULL && IsA(ps, MotionState) && ps != root)
		{
			in->motion = (MotionState *) ps;
			in->ipc = df_motion_sends_batches(queryDesc->plannedstmt, (Motion *) ps->plan);
			if (in->ipc)
				df_batch_head(in->rx_head, queryDesc->plannedstmt, (Motion *) ps->plan);
		}
		else if (ps != NULL && IsA(ps, ShareInputScanState))
			in->reader = ps;
		else if (ps != NULL && x->spec.inputs[j].set)
			in->reader = ps;	/* SP1: the top of a subquery's plan */
		else
		{
			MemoryContextSwitchTo(oldcxt);
			snprintf(reason, reasonlen, "unexpected executor state for this slice");
			return false;
		}
		if (in->motion != NULL && !in->ipc)
		{
			/* its states' scales may come from a Shared Scan's producer */
			df_numeric_stmt = queryDesc->plannedstmt;
			PG_TRY();
			{
				df_input_states(in, (Motion *) in->motion->ps.plan);
			}
			PG_FINALLY();
			{
				df_numeric_stmt = NULL;
			}
			PG_END_TRY();
		}
		in->invalues = palloc0(sizeof(char *) * Max(in->spec->ncols, 1));
		in->innulls = palloc0(sizeof(uint8 *) * Max(in->spec->ncols, 1));
		in->inoffsets = palloc0(sizeof(int32 *) * Max(in->spec->ncols, 1));
		in->incap = palloc0(sizeof(Size) * Max(in->spec->ncols, 1));
		in->incols = palloc0(sizeof(DfColumn) * Max(in->spec->ncols, 1));
		for (c = 0; c < in->spec->ncols; c++)
		{
			if (df_type_is_string(in->spec->types[c]))
			{
				in->incap[c] = DF_BATCH_ROWS * 16;
				in->invalues[c] = palloc(in->incap[c]);
				in->inoffsets[c] = palloc(sizeof(int32) * (DF_BATCH_ROWS + 1));
				in->inoffsets[c][0] = 0;
			}
			else
				in->invalues[c] = palloc(DF_BATCH_ROWS * df_type_width(in->spec->types[c]));
			in->innulls[c] = palloc(DF_BATCH_ROWS);
			in->incols[c].values = in->invalues[c];
			in->incols[c].nulls = in->innulls[c];
			in->incols[c].offsets = in->inoffsets[c];
			in->maxattno = Max(in->maxattno,
							   in->tupatt ? in->tupatt[c] : in->spec->attnos[c]);
		}
	}
	{
		List	   *producers = df_slice_producers(root->plan);
		ListCell   *lc;

		x->nprerun = list_length(producers);
		x->prerun = palloc0(sizeof(PlanState *) * Max(x->nprerun, 1));
		foreach(lc, producers)
		{
			PlanState  *ps = df_find_state(root, lfirst(lc));

			if (ps == NULL)
			{
				MemoryContextSwitchTo(oldcxt);
				snprintf(reason, reasonlen, "no executor state for a Shared Scan producer");
				return false;
			}
			x->prerun[foreach_current_index(lc)] = ps;
		}
	}
	x->root = root;
	x->procnode = send ? &send->ps : root;
	if (send)
	{
		Motion	   *motion = (Motion *) send->ps.plan;
		StringInfoData json;
		AttrNumber	r;
		int			ntrim = 0;

		x->send = send;
		df_batch_head(x->tx_head, queryDesc->plannedstmt, motion);
		x->chunkcxt = AllocSetContextCreate(estate->es_query_cxt, "datafusion chunks",
											ALLOCSET_DEFAULT_SIZES);
		initStringInfo(&json);
		appendBinaryStringInfo(&json, x->spec.json, strlen(x->spec.json) - 1);

		/* CP1: character(n) values go without their trailing blanks */
		for (r = 1; r <= list_length(motion->plan.targetlist); r++)
			if (df_motion_bpchar_pad(queryDesc->plannedstmt, motion, r) > 0)
				appendStringInfo(&json, "%s%d", ntrim++ > 0 ? "," : ",\"trim\":[",
								 df_motion_stream_column(motion, r));
		if (ntrim > 0)
			appendStringInfoChar(&json, ']');
		x->send_nroutes = 1;
		if (motion->motionType == MOTIONTYPE_HASH)
		{
			/* Route rows as the Motion's cdbhash would (nodeMotion.c). */
			int			i;

			x->send_nroutes = send->numHashSegments * Max(send->parallel_workers, 1);
			appendStringInfoString(&json, ",\"route\":{\"kind\":\"hash\",\"keys\":[");
			for (i = 0; i < list_length(motion->hashExprs); i++)
			{
				int			column;
				const char *tag;

				if (!df_motion_hash_key(motion, i, &column, &tag))
					elog(ERROR, "datafusion: unsupported distribution key in Motion %d",
						 motion->motionID);
				appendStringInfo(&json, "%s{\"col\":%d,\"hash\":\"%s\"}",
								 i > 0 ? "," : "",
								 df_motion_stream_column(motion, column + 1), tag);
			}
			appendStringInfo(&json, "],\"segments\":%d,\"workers\":%d}",
							 send->numHashSegments, Max(send->parallel_workers, 1));
		}
		appendStringInfoChar(&json, '}');
		x->spec.json = json.data;
		x->head_sent_to = palloc0(sizeof(bool) * x->send_nroutes);
	}
	x->estate = estate;
	x->memory_limit = df_slice_memory(root);
	x->headroom = Max(x->memory_limit / 8, DF_MIN_HEADROOM);
	x->slices_together = df_slices_together(queryDesc->plannedstmt,
											LocallyExecutingSliceIndex(estate));
	x->host_segments = df_host_segments();

	x->outcols = palloc0(sizeof(DfColumn) * Max(x->spec.nout, 1));
	x->outslot = ExecInitExtraTupleSlot(estate, ExecGetResultType(root), &TTSOpsVirtual);
	if (x->spec.col_tail != NULL)
	{
		/* P1: DataFusion's values, and what finishes the other columns */
		TupleDesc	desc = CreateTemplateTupleDesc(x->spec.nvalues);

		for (c = 0; c < x->spec.nvalues; c++)
			TupleDescInitEntry(desc, c + 1, NULL, x->spec.value_types[c], -1, 0);
		x->valslot = ExecInitExtraTupleSlot(estate, desc, &TTSOpsVirtual);
		x->tailcxt = CreateExprContext(estate);
		x->tail_states = palloc0(sizeof(ExprState *) * x->spec.ncols);
		for (c = 0; c < x->spec.ncols; c++)
			if (x->spec.col_tail[c] != NULL)
				x->tail_states[c] = ExecInitExpr(x->spec.col_tail[c], NULL);
	}
	x->rowcxt = AllocSetContextCreate(estate->es_query_cxt, "datafusion row",
									  ALLOCSET_SMALL_SIZES);

	x->release.func = df_exec_release;
	x->release.arg = x;
	MemoryContextRegisterResetCallback(estate->es_query_cxt, &x->release);
	x->next = df_execs;
	df_execs = x;
	MemoryContextSwitchTo(oldcxt);

	elog(DEBUG1, "datafusion plan: %s%s%s", x->spec.json,
		 df_ipc_inputs(x) ? " (batches in)" : "", x->send ? " (batches out)" : "");
	ExecSetExecProcNode(x->procnode, df_exec_proc_node);
	return true;
}
