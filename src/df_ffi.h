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
 * df_ffi.h
 *	  C declarations for the Rust static library (rust/df_ffi).
 *
 * Every function here follows one contract:
 *   - it never calls into PostgreSQL;
 *   - it never lets a Rust panic escape;
 *   - it returns DF_OK, DF_ERROR or DF_PANIC, and writes either its result
 *     or the error text into the caller's buffer as a NUL-terminated string.
 * The caller raises any ereport only after the call has returned, so a
 * longjmp never crosses a Rust frame.
 *
 * Keep in sync with rust/df_ffi/src/lib.rs.
 *
 * src/df_ffi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DF_FFI_H
#define DF_FFI_H

#include <stddef.h>
#include <stdint.h>

#define DF_OK		0
#define DF_ERROR	1
#define DF_PANIC	2
#define DF_PENDING	3			/* df_ffi_task_wait: still running */
#define DF_CANCELLED 4			/* df_ffi_task_wait: stopped by cancel */
#define DF_DONE		5			/* df_ffi_query_poll: all rows produced */
#define DF_BYTES	6			/* df_ffi_query_poll: IPC bytes available */

/* Query flags (M7b) */
#define DF_QUERY_IPC_OUTPUT 2	/* results: Arrow IPC streams */

#define DF_MSG_BUFLEN 1024

#include <stdbool.h>

extern int32_t df_ffi_version(char *buf, size_t buflen);
extern int32_t df_ffi_debug_panic(char *buf, size_t buflen);

/*
 * Per-backend runtime.  The caller must block all signals around
 * df_ffi_runtime_init so that the runtime's threads start with every signal
 * blocked.  workers == 0 means one thread per CPU.
 */
extern int32_t df_ffi_runtime_init(uint32_t workers, uint32_t *out_workers,
								   char *buf, size_t buflen);
extern void df_ffi_runtime_shutdown(uint32_t timeout_ms);

/* A running group of tasks; owned by the caller until df_ffi_task_free. */
typedef struct DfTask DfTask;

extern int32_t df_ffi_debug_spin_start(double seconds, uint32_t ntasks,
									   bool panic_in_worker, DfTask **out_task,
									   char *buf, size_t buflen);
extern int32_t df_ffi_task_wait(DfTask *task, uint32_t timeout_ms,
								char *buf, size_t buflen);
extern void df_ffi_task_cancel(DfTask *task);
extern void df_ffi_task_free(DfTask *task);
extern uint64_t df_ffi_debug_active_tasks(void);

/*
 * Queries (M3).  Column buffers hold native values (one byte per bool) and
 * one byte per row that is 1 for NULL.  Failures carry a SQLSTATE: the
 * 'sqlstate' arguments take a 6-byte buffer.
 */
typedef struct DfQuery DfQuery;

/*
 * Native values, or for a string column its bytes with 'offsets' giving
 * row r as bytes offsets[r] to offsets[r + 1]; one byte per row that is 1
 * for NULL.  'offsets' is NULL for other columns.
 */
typedef struct DfColumn
{
	const void *values;
	const uint8_t *nulls;
	const int32_t *offsets;
} DfColumn;

/*
 * A column as the PAX reader hands it out: fixed-width values, or a
 * string's bytes and nrows + 1 offsets (DatafusionPaxColumn).
 */
typedef struct DfPaxColumn
{
	const void *values;
	const uint8_t *nulls;
	const int32_t *offsets;
} DfPaxColumn;

/* The message may be followed by a newline and an error detail. */
/* Bit j of 'ipc_inputs': input j arrives as Arrow IPC streams from a Motion. */
extern int32_t df_ffi_query_start(const char *spec, uint32_t partitions,
								  uint64_t memory_limit, const char *spill_dir,
								  uint32_t flags, uint32_t ninputs, uint64_t ipc_inputs,
								  DfQuery **out_query,
								  char *sqlstate, char *buf, size_t buflen);
/* PAX blocks read on the workers (experimental); see patches/pax. */
typedef int (*DfPaxEmit) (void *ctx, uint32_t nrows, const DfPaxColumn *cols);
typedef void (*DfPaxAccount) (void *ctx, int64_t delta);
typedef int (*DfPaxRead) (void *scan, int index, int first, int count,
						  DfPaxEmit emit, DfPaxAccount account, void *ctx,
						  char *err, size_t errlen);
typedef void (*DfPaxEnd) (void *scan);
typedef int (*DfPaxGroups) (void *scan, int index, char *err, size_t errlen);

/* Takes ownership of 'scan' (released with 'end'), even on failure. */
extern int32_t df_ffi_query_start_pax(const char *spec, uint32_t partitions,
									  uint64_t memory_limit, const char *spill_dir,
									  void *scan, uint32_t nblocks,
									  DfPaxRead read, DfPaxEnd end,
									  DfPaxGroups groups, uint32_t flags,
									  DfQuery **out_query, char *sqlstate,
									  char *buf, size_t buflen);
extern int32_t df_ffi_query_push(DfQuery *query, uint32_t input, const DfColumn *cols,
								 uint32_t ncols, uint32_t nrows,
								 char *sqlstate, char *buf, size_t buflen);
typedef struct DfSlice
{
	const uint8_t *data;
	size_t		len;
} DfSlice;

/* Copies the bytes; DF_PENDING when the queue is full (nothing taken). */
extern int32_t df_ffi_query_push_ipc(DfQuery *query, uint32_t input, int32_t route,
									 const DfSlice *parts, uint32_t nparts,
									 char *sqlstate, char *buf, size_t buflen);
/*
 * The bytes of the last DF_BYTES poll, valid until the next poll, and their
 * route (a Redistribute Motion's receiver), or -1 for the only stream.
 */
extern void df_ffi_query_bytes(DfQuery *query, int32_t *route,
							   const uint8_t **data, size_t *len);

/* Routes by the Rust transcription of cdbhash, for checking it. */
extern int32_t df_ffi_cdbhash_routes(const int32_t *kinds, const DfColumn *cols,
									 uint32_t nkeys, uint32_t nrows,
									 int32_t segments, int32_t workers,
									 uint32_t *out);
extern void df_ffi_query_finish_input(DfQuery *query);
extern void df_ffi_query_finish_input_at(DfQuery *query, uint32_t input);
extern int32_t df_ffi_query_poll(DfQuery *query, uint32_t timeout_ms,
								 uint32_t *nrows, char *sqlstate,
								 char *buf, size_t buflen);
extern int32_t df_ffi_query_column(DfQuery *query, uint32_t col, DfColumn *out);
extern void df_ffi_query_free(DfQuery *query);

typedef struct DfQueryStats
{
	uint64_t	partitions;		/* partitions the plan ran with */
	uint64_t	memory_limit;	/* operator memory budget, bytes */
	uint64_t	memory_peak;	/* peak reserved from the pool, bytes */
	uint64_t	spilled_bytes;
	uint64_t	spill_count;
	uint64_t	pax_decode_peak;	/* peak bytes PAX's reader held */
} DfQueryStats;

extern void df_ffi_query_stats(DfQuery *query, DfQueryStats *out);

/* Live bytes allocated by Rust code in this process, on any thread. */
extern int64_t df_ffi_heap_bytes(void);

#endif							/* DF_FFI_H */
