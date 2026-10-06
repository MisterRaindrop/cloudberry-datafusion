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
 * df_executor.h
 *	  Declarations shared by the C sources of datafusion_executor.
 *
 * src/df_executor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DF_EXECUTOR_H
#define DF_EXECUTOR_H

#include "df_ffi.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/* GUC datafusion.mode */
typedef enum DfMode
{
	DF_MODE_OFF,				/* hooks pass everything through */
	DF_MODE_EXPLAIN,			/* EXPLAIN reports eligibility; nothing runs */
	DF_MODE_ON					/* eligible slices run in DataFusion */
} DfMode;

extern int	df_mode;

/* GUC datafusion.worker_threads */
extern int	df_worker_threads;

/* df_init.c */
extern void df_raise(int32 status, const char *msg) pg_attribute_noreturn();

/* One input of a slice: the rows of a Seq Scan or of a receiving Motion. */
typedef struct DfSliceInput
{
	Plan	   *leaf;			/* the Seq Scan or Motion node */
	int			ncols;			/* columns read */
	AttrNumber *attnos;			/* table columns, or Motion stream
								 * positions + 1 */
	Oid		   *types;
} DfSliceInput;

/* What df_translate_slice produces for one slice. */
typedef struct DfSliceSpec
{
	char	   *json;			/* plan for the Rust side */
	int			ninputs;		/* in the order they are fed */
	DfSliceInput *inputs;
	int			nout;			/* output columns, in targetlist order */
	Oid		   *out_types;
	uint8	   *out_kinds;		/* DfOutKind of each */
	int			batch_rows;		/* rows per batch, from the widest row */
} DfSliceSpec;

/*
 * Aggregates whose state DataFusion keeps itself.  Split through a batch
 * Motion, the partial stage sends that state (one or two columns) instead
 * of PostgreSQL's; numeric results are built on the C side with
 * PostgreSQL's own functions.
 */
typedef enum DfAggState
{
	DF_AGG_PLAIN,				/* the result type is the state */
	DF_AGG_AVG_FLOAT,			/* avg(float4/8): float8 sum, int8 count */
	DF_AGG_AVG_INT,				/* avg(int2/4/8): numeric sum, int8 count */
	DF_AGG_SUM_INT8				/* sum(int8): numeric sum */
} DfAggState;

extern DfAggState df_agg_state(Aggref *agg);
extern int	df_agg_state_ncols(DfAggState state);

/* How an output column of the slice becomes a value of its tuple. */
typedef enum DfOutKind
{
	DF_OUT_PLAIN,				/* one column, one value */
	DF_OUT_NUMERIC_AVG,			/* numeric sum and the next column's count:
								 * avg = sum / count (numeric_div) */
	DF_OUT_PART					/* consumed with the column before */
} DfOutKind;

/* String functions DataFusion runs (df_core::pgstr), by pg_proc OID. */
typedef enum DfCollRule
{
	DF_COLL_ANY,				/* the collation does not matter */
	DF_COLL_DETERMINISTIC,		/* substring searches: PostgreSQL rejects others */
	DF_COLL_CTYPE_C				/* case mapping: ASCII only under C */
} DfCollRule;

typedef struct DfStringFunc
{
	Oid			funcid;
	const char *name;			/* as df_core::pgstr knows it */
	DfCollRule	rule;
} DfStringFunc;

extern const DfStringFunc *df_string_func(Oid funcid);

/* df_exec.c */
extern bool df_exec_attach(QueryDesc *queryDesc, PlanState *root,
						   MotionState *send, char *reason, size_t reasonlen);
extern uint64 df_runs_completed;	/* slices DataFusion ran to the end */
extern DfQueryStats df_last_run;	/* figures of the latest one */

/* df_translate.c */
extern const char *df_type_tag(Oid type);
extern bool df_translate_slice(Plan *root, DfSliceSpec *spec,
							   char *reason, size_t reasonlen);

/*
 * df_paxload.c: the experimental direct PAX reader, datafusion_pax.so, built
 * from patches/pax.  DfPaxReader mirrors DatafusionPaxScanApi in the patch's
 * access/datafusion_scan_api.h, version DF_PAX_SCAN_API_VERSION; keep the
 * two identical.
 */
#define DF_PAX_SCAN_API_VERSION 2

/* What the scan's min/max skipping did (DatafusionPaxScanInfo). */
typedef struct DfPaxScanInfo
{
	int64		files;			/* micro-partitions visible to the snapshot */
	int64		files_skipped;	/* skipped by their statistics */
	int64		groups;			/* groups examined in the remaining files */
	int64		groups_skipped; /* skipped by their statistics */
} DfPaxScanInfo;

typedef void *(*DfPaxBegin) (Relation rel, Snapshot snapshot, List *qual,
							 const int *cols, const int *widths, int ncols,
							 char *err, size_t errlen);
typedef int (*DfPaxNBlocks) (void *scan);
typedef void (*DfPaxInfo) (void *scan, DfPaxScanInfo *out);

typedef struct DfPaxReader
{
	uint32		version;
	const char *pax_build_id;	/* the pax.so it was built against */
	DfPaxBegin	begin;
	DfPaxNBlocks nblocks;
	DfPaxInfo	info;
	DfPaxRead	read;
	DfPaxEnd	end;
} DfPaxReader;

extern bool df_pax_direct_read;	/* GUC datafusion.pax_direct_read */
extern const DfPaxReader *df_pax_reader_get(void);
extern uint64 df_pax_direct_scans;	/* scans read through it */
extern bool df_last_run_pax;	/* df_last_run read PAX directly */
extern DfPaxScanInfo df_last_pax_scan;	/* and skipped this */

/* M7b: Gather Motions between DataFusion slices carry Arrow IPC batches */
extern bool df_motion_batches;	/* GUC datafusion.motion_batches */
extern bool df_motion_sends_batches(PlannedStmt *stmt, Motion *motion);
extern Bitmapset *df_batch_motions(PlannedStmt *stmt);
extern int	df_motion_state_columns(Motion *motion, AttrNumber resno);
extern int	df_motion_stream_column(Motion *motion, AttrNumber resno);
extern uint64 df_motion_signature(Motion *motion);
extern bool df_motion_hash_key(Motion *motion, int i, int *column, const char **tag);

/* df_hooks.c */
extern void df_install_hooks(void);

/* df_plan_check.c */
extern bool df_check_slice(PlannedStmt *stmt, Plan *root, bool root_is_sender,
						   char *reason, size_t reasonlen);
extern Plan *df_local_slice_root(QueryDesc *queryDesc, int *slice_index,
								 bool *is_sender);
extern void df_explain_slices(PlannedStmt *stmt, StringInfo out);
extern double df_hash_budget(Plan *hash);
extern Oid	df_join_key_type(Oid a, Oid b);

/* df_runtime.c */
extern int	df_runtime_ensure(void);
extern const char *df_spill_dir(void);
extern void df_vmem_sync(int64 headroom);
extern void df_vmem_trim(void);
extern int64 df_vmem_leased_bytes(void);
extern int32 df_task_wait_interruptible(DfTask *task, char *buf, size_t buflen);

#endif							/* DF_EXECUTOR_H */
