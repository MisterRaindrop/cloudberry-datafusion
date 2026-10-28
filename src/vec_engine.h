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
 * The DataFusion engine (rust/df_ffi) implements this; keep the two in
 * sync.
 *
 * src/vec_engine.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_ENGINE_H
#define VEC_ENGINE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Version of this ABI: host and engine must agree exactly. */
#define VEC_ENGINE_ABI	1

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
 * A column of pushed or returned rows: native values (one byte per bool),
 * or for a string or numeric column its bytes with 'offsets' giving row r
 * as bytes offsets[r] to offsets[r + 1] ('offsets' is NULL for the
 * others); and one byte per row that is 1 for NULL.
 */
typedef struct VecColumn
{
	const void *values;
	const uint8_t *nulls;
	const int32_t *offsets;
} VecColumn;

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
	 * Routes of nrows rows by the engine's cdbhash, to check it against
	 * Cloudberry's: kinds[i] is the hash of key column cols[i] (0 bool,
	 * 1 int2, 2 int4, 3 int8, 4 float4, 5 float8, 6 text, 7 character,
	 * 8 + s numeric of scale s, 85 tid).  VEC_ERROR for an unknown kind.
	 */
	int32_t		(*cdbhash_routes) (const int32_t *kinds, const VecColumn *cols,
								   uint32_t nkeys, uint32_t nrows,
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

	int32_t		(*query_start) (const VecQueryArgs *args, VecQuery **out_query,
								VecError *err);

	/*
	 * Offer nrows rows of input 'input' in ncols columns, or Arrow IPC
	 * bytes from Motion route 'route' as nparts consecutive pieces.  Both
	 * copy what they take.  VEC_OK: taken.  VEC_PENDING: the queue is full
	 * and nothing was taken; poll, then offer again.
	 */
	int32_t		(*query_push) (VecQuery *query, uint32_t input, const VecColumn *cols,
							   uint32_t ncols, uint32_t nrows, VecError *err);
	int32_t		(*query_push_ipc) (VecQuery *query, uint32_t input, int32_t route,
								   const VecSlice *parts, uint32_t nparts, VecError *err);
	/* Input 'input' has no more rows. */
	void		(*query_finish_input) (VecQuery *query, uint32_t input);

	/*
	 * Wait up to timeout_ms for results.  VEC_OK: *nrows rows, read with
	 * query_column until the next poll.  VEC_BYTES: *nrows bytes of the
	 * result stream, read with query_bytes (VEC_QUERY_IPC_OUTPUT).
	 * VEC_PENDING, VEC_DONE, or VEC_ERROR / VEC_PANIC.
	 */
	int32_t		(*query_poll) (VecQuery *query, uint32_t timeout_ms, uint32_t *nrows,
							   VecError *err);
	int32_t		(*query_column) (VecQuery *query, uint32_t col, VecColumn *out);
	/* The bytes of the last VEC_BYTES poll and their route (-1: the only one). */
	void		(*query_bytes) (VecQuery *query, int32_t *route,
								const uint8_t **data, size_t *len);
	void		(*query_stats) (VecQuery *query, VecQueryStats *out);
	/* Stop the query and release it; never blocks. */
	void		(*query_free) (VecQuery *query);

	const VecEngineDebug *debug;	/* NULL without the test functions */
} VecEngine;

/*
 * The one symbol an engine exports: its table, or NULL if it does not speak
 * host_abi.
 */
typedef const VecEngine *(*VecEngineEntry) (uint32_t host_abi, const VecHost *host);
extern const VecEngine *vec_engine_entry(uint32_t host_abi, const VecHost *host);

#endif							/* VEC_ENGINE_H */
