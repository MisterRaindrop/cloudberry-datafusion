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
 * vec_engine.h
 *	  The ABI between the extension (the host) and an execution engine.
 *
 * An engine hands the host one table of functions, VecEngine, from
 * vec_engine_entry.  Every function in it follows one contract:
 *   - it never calls into PostgreSQL;
 *   - it never lets a panic or an exception escape;
 *   - it returns a VEC_* status, and on VEC_ERROR or VEC_PANIC fills the
 *     caller's VecError: a SQLSTATE and a message, which may be followed
 *     by a newline and a detail.
 * The host raises any ereport only after the call has returned, so a
 * longjmp never crosses an engine frame.
 *
 * Plans reach the engine as the IR of docs/ir.md, version VEC_IR_VERSION.
 *
 * An engine is a library of its own, $libdir/vec_engine_<name>.so, which
 * exports vec_engine_entry and nothing else.  A backend opens the one
 * datafusion.engine names (RTLD_LOCAL) when it first needs it, and keeps it.
 * The host blocks every signal while it opens the library and around
 * runtime_init and query_start, so that threads started there begin with
 * all of them blocked; a thread started at any other time must block them
 * itself.  Only the backend's own thread then gets the signals PostgreSQL
 * acts on.
 *
 * The DataFusion engine (engines/datafusion/df_ffi) implements this; keep
 * the two in sync.
 *
 * include/vec_engine.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_ENGINE_H
#define VEC_ENGINE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Version of this ABI: host and engine must agree exactly. */
#define VEC_ENGINE_ABI	2

/* Version of the plan IR (docs/ir.md): the "ir" member of every plan. */
#define VEC_IR_VERSION	1

#define VEC_OK			0
#define VEC_ERROR		1
#define VEC_PANIC		2
#define VEC_PENDING		3		/* still running, or a queue is full */
#define VEC_CANCELLED	4		/* task_wait: stopped by cancel */
#define VEC_DONE		5		/* query_poll: all rows produced */
#define VEC_BYTES		6		/* query_poll: IPC bytes available */

#define VEC_MSG_LEN		1024

typedef struct VecError
{
	char		sqlstate[6];	/* five characters and a NUL */
	char		message[VEC_MSG_LEN];
} VecError;

/* Query flags */
#define VEC_QUERY_IPC_OUTPUT 2	/* results leave as Arrow IPC streams */

/* A query, and a debug task; owned by the host until freed. */
typedef struct VecQuery VecQuery;
typedef struct VecTask VecTask;

/*
 * Rows cross the boundary as Arrow arrays, by the Arrow C Data Interface
 * (https://arrow.apache.org/docs/format/CDataInterface.html): a batch is a
 * struct array with one child per column.  Each column's layout follows
 * from its IR type (docs/ir.md, "Types"): bool as Boolean, int2/4/8 as
 * Int16/32/64, float4/8 as Float32/64, date as Int32 and time, timestamp,
 * timestamptz and tid as Int64 (PostgreSQL's units and epoch, not Arrow's
 * date and time types), text and bpchar as Utf8, numeric as Decimal256(76,
 * scale).
 *
 * A batch the host pushes is lent for the call only: the engine copies
 * what it needs and never calls its release callback.  A batch the engine
 * returns belongs to the host, which releases it.
 */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema
{
	const char *format;
	const char *name;
	const char *metadata;
	int64_t		flags;
	int64_t		n_children;
	struct ArrowSchema **children;
	struct ArrowSchema *dictionary;
	void		(*release) (struct ArrowSchema *);
	void	   *private_data;
};

struct ArrowArray
{
	int64_t		length;
	int64_t		null_count;
	int64_t		offset;
	int64_t		n_buffers;
	int64_t		n_children;
	const void **buffers;
	struct ArrowArray **children;
	struct ArrowArray *dictionary;
	void		(*release) (struct ArrowArray *);
	void	   *private_data;
};

#endif							/* ARROW_C_DATA_INTERFACE */

/* A piece of a byte stream. */
typedef struct VecSlice
{
	const uint8_t *data;
	size_t		len;
} VecSlice;

/*
 * Direct PAX reads (experimental; see patches/pax).  The engine calls these
 * on its own threads; none of them enters PostgreSQL.  A column as PAX's
 * reader hands it out: fixed-width values, or the bytes of strings or
 * numerics and nrows + 1 offsets.
 */
typedef struct VecPaxColumn
{
	const void *values;
	const uint8_t *nulls;
	const int32_t *offsets;
} VecPaxColumn;

typedef int (*VecPaxEmit) (void *ctx, uint32_t nrows, const VecPaxColumn *cols);
typedef void (*VecPaxAccount) (void *ctx, int64_t delta);
typedef int (*VecPaxRead) (void *scan, int index, int first, int count,
						   VecPaxEmit emit, VecPaxAccount account, void *ctx,
						   char *err, size_t errlen);
typedef void (*VecPaxEnd) (void *scan);
typedef int (*VecPaxGroups) (void *scan, int index, char *err, size_t errlen);

/* An input read directly from PAX: its scan and blocks, or a NULL scan. */
typedef struct VecPaxInput
{
	void	   *scan;
	uint32_t	nblocks;
} VecPaxInput;

/* What query_start needs to start one slice's plan. */
typedef struct VecQueryArgs
{
	const char *ir;				/* the plan, NUL-terminated */
	/* one struct schema per input: the batches query_push takes */
	const struct ArrowSchema *input_schemas;
	uint32_t	partitions;		/* parallel partitions to run it in */
	uint64_t	memory_limit;	/* operator memory budget, bytes */
	const char *spill_dir;
	uint32_t	flags;			/* VEC_QUERY_* */
	uint32_t	ninputs;
	uint64_t	ipc_inputs;		/* bit j: input j arrives as IPC streams */

	/*
	 * NULL, or one per input: the inputs the engine reads from PAX itself
	 * with pax_read, pax_end and pax_groups, decoding at most
	 * pax_max_readers groups at once (0: no limit).  The query owns those
	 * scans from query_start on, even if it fails.
	 */
	const VecPaxInput *pax_inputs;
	VecPaxRead	pax_read;
	VecPaxEnd	pax_end;
	VecPaxGroups pax_groups;
	uint32_t	pax_max_readers;
} VecQueryArgs;

/* Memory and spill figures of a query, for EXPLAIN ANALYZE. */
typedef struct VecQueryStats
{
	uint64_t	partitions;		/* partitions the plan ran with */
	uint64_t	memory_limit;	/* operator memory budget, bytes */
	uint64_t	memory_peak;	/* peak reserved from the pool, bytes */
	uint64_t	spilled_bytes;
	uint64_t	spill_count;
	uint64_t	pax_decode_peak;	/* peak bytes PAX's reader held */
} VecQueryStats;

/* What the host gives the engine. */
typedef struct VecHost
{
	uint32_t	abi;			/* VEC_ENGINE_ABI */
} VecHost;

/* Functions for the extension's tests; an engine may leave them out. */
typedef struct VecEngineDebug
{
	/* panics (throws) on purpose: must come back as VEC_PANIC */
	int32_t		(*panic) (VecError *err);

	/*
	 * Tasks that spin for 'seconds' on the engine's threads, the first one
	 * panicking instead with panic_in_worker; task_wait gives VEC_PENDING
	 * while they run, then VEC_OK with a result text in err->message,
	 * VEC_CANCELLED, VEC_ERROR or VEC_PANIC.
	 */
	int32_t		(*spin_start) (double seconds, uint32_t ntasks, bool panic_in_worker,
							   VecTask **out_task, VecError *err);
	int32_t		(*task_wait) (VecTask *task, uint32_t timeout_ms, VecError *err);
	void		(*task_cancel) (VecTask *task);
	void		(*task_free) (VecTask *task);
	uint64_t	(*active_tasks) (void);

	/*
	 * Routes of the rows of 'keys' by the engine's cdbhash, to check it
	 * against Cloudberry's: kinds[i] is the hash of key column i (0 bool,
	 * 1 int2, 2 int4, 3 int8, 4 float4, 5 float8, 6 text, 7 character,
	 * 8 + s numeric of scale s, 85 tid), laid out as its IR type.  Writes
	 * keys->length routes to 'out'.  VEC_ERROR for an unknown kind.
	 */
	int32_t		(*cdbhash_routes) (const int32_t *kinds, const struct ArrowSchema *schema,
								   const struct ArrowArray *keys,
								   int32_t segments, int32_t workers, uint32_t *out);
} VecEngineDebug;

typedef struct VecEngine
{
	uint32_t	abi;			/* VEC_ENGINE_ABI */
	const char *name;			/* "datafusion" */
	const char *version;		/* "datafusion 50.0.0" */

	/*
	 * The engine's threads for this backend, 'workers' of them (0: one per
	 * CPU), if not running yet.  The host blocks every signal around this
	 * call so that the threads start with all of them blocked.
	 */
	int32_t		(*runtime_init) (uint32_t workers, uint32_t *out_workers, VecError *err);
	/* Stop them, waiting at most timeout_ms for running work. */
	void		(*runtime_shutdown) (uint32_t timeout_ms);
	/* Live bytes the engine has allocated in this process, on any thread. */
	int64_t		(*heap_bytes) (void);
	/* The case maps of LC_CTYPE: pairs of code points, flattened. */
	void		(*set_case_maps) (const uint32_t *upper, size_t nupper,
								  const uint32_t *lower, size_t nlower);

	/*
	 * Start a query.  *out_schema gets the struct schema of its result
	 * batches, which the host releases.  Called with every signal blocked.
	 */
	int32_t		(*query_start) (const VecQueryArgs *args, struct ArrowSchema *out_schema,
								VecQuery **out_query, VecError *err);

	/*
	 * Offer a batch of input 'input' (lent: copied, never released), or
	 * Arrow IPC bytes from Motion route 'route' as nparts consecutive
	 * pieces (copied).  VEC_OK: taken.  VEC_PENDING: the queue is full and
	 * nothing was taken; poll, then offer again.
	 */
	int32_t		(*query_push) (VecQuery *query, uint32_t input,
							   const struct ArrowArray *batch, VecError *err);
	int32_t		(*query_push_ipc) (VecQuery *query, uint32_t input, int32_t route,
								   const VecSlice *parts, uint32_t nparts, VecError *err);
	/* Input 'input' has no more rows. */
	void		(*query_finish_input) (VecQuery *query, uint32_t input);

	/*
	 * Wait up to timeout_ms for results.  VEC_OK: a batch of *nrows rows,
	 * taken with query_result.  VEC_BYTES: *nrows bytes of the result
	 * stream, read with query_bytes (VEC_QUERY_IPC_OUTPUT).  VEC_PENDING,
	 * VEC_DONE, or VEC_ERROR / VEC_PANIC.
	 */
	int32_t		(*query_poll) (VecQuery *query, uint32_t timeout_ms, uint32_t *nrows,
							   VecError *err);
	/*
	 * Move the batch of the last VEC_OK poll into *out: the host's from
	 * then on, to release once read.  VEC_ERROR without one.
	 */
	int32_t		(*query_result) (VecQuery *query, struct ArrowArray *out);
	/* The bytes of the last VEC_BYTES poll and their route (-1: the only one). */
	void		(*query_bytes) (VecQuery *query, int32_t *route,
								const uint8_t **data, size_t *len);
	void		(*query_stats) (VecQuery *query, VecQueryStats *out);
	/* Stop the query and release it; never blocks. */
	void		(*query_free) (VecQuery *query);

	const VecEngineDebug *debug;	/* NULL without the test functions */
} VecEngine;

/*
 * The one symbol an engine exports: its table for host_abi, or, if it does
 * not speak host_abi, a table of the version it does.  abi comes first in
 * every version, and a host reads nothing else from a table of another one.
 * NULL if the engine cannot make its table.
 */
typedef const VecEngine *(*VecEngineEntry) (uint32_t host_abi, const VecHost *host);
extern const VecEngine *vec_engine_entry(uint32_t host_abi, const VecHost *host);

#endif							/* VEC_ENGINE_H */
