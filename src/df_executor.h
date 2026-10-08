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
	int16	   *scales;			/* of numeric columns (Decimal128) */
} DfSliceInput;

/*
 * Output columns PostgreSQL finishes (P1): the slice's output columns that
 * DataFusion cannot compute whole, and the subexpressions of them that it
 * computes.  PostgreSQL evaluates the rest of each such column over those
 * values, row by row, as its own projection would (numeric division, any
 * function).  df_check_slice finds them; df_translate_slice uses them.
 */
typedef struct DfTails
{
	List	   *tles;			/* TargetEntry of each such column */
	List	   *leaves;			/* Node: subexpressions DataFusion computes */
	Motion	   *resort;			/* a sorted Motion whose GroupAggregate below
								 * runs hashed: the slice sorts its output by
								 * the Motion's keys (D3) */
	Sort	   *resort_by;		/* or, with a Limit between them, by this
								 * Sort's keys, the GroupAggregate's order
								 * (D4) */
} DfTails;

/*
 * T2: a subtree of a slice DataFusion runs below PostgreSQL's nodes, when
 * the slice as a whole cannot run (df_slice_attach_points).
 */
typedef struct DfAttach
{
	Plan	   *plan;			/* its top node */
	DfTails		tails;			/* as df_check_slice found them */
	bool		locale_dependent;	/* its verdict depends on the node */
} DfAttach;

/* What df_translate_slice produces for one slice. */
typedef struct DfSliceSpec
{
	char	   *json;			/* plan for the Rust side */
	int			ninputs;		/* in the order they are fed */
	DfSliceInput *inputs;
	int			nout;			/* output columns, in targetlist order */
	Oid		   *out_types;
	uint8	   *out_kinds;		/* DfOutKind of each */
	int16	   *out_scales;		/* of numeric ones */
	int			batch_rows;		/* rows per batch, from the widest row */

	/*
	 * The tuple's columns (P1).  The output columns above are values: one
	 * per value, two for DF_OUT_NUMERIC_AVG and DF_OUT_NUMERIC_MIXED.
	 * Column k of the tuple is value col_value[k], or, if that is -1,
	 * col_tail[k] evaluated over the values (Vars of OUTER_VAR, attno =
	 * value + 1).  col_tail is NULL without such columns.
	 */
	int			ncols;
	int		   *col_value;
	Expr	  **col_tail;
	int			nvalues;
	Oid		   *value_types;
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
	DF_AGG_SUM_INT8,			/* sum(int8): numeric sum */
	DF_AGG_SUM_NUMERIC,			/* sum(numeric(p <= 28, s)): numeric sum */
	DF_AGG_AVG_NUMERIC,			/* avg(numeric(p <= 28, s)): numeric sum, count */
	DF_AGG_SUM_NUMERIC_MIXED	/* sum of a CASE or COALESCE whose branches
								 * differ in scale (MS1): numeric sum at the
								 * largest, int4 largest scale added up */
} DfAggState;

/* sum and avg of numeric(p, s): p digits added up must stay within 76 */
#define DF_NUMERIC_MAX_SUM_PRECISION 66

extern DfAggState df_agg_state(Aggref *agg);
extern DfAggState df_agg_state_at(Plan *ctx, Aggref *agg);
extern bool df_collect_aggrefs(Node *node, List **aggs);
extern int	df_agg_state_ncols(DfAggState state);

/*
 * df_numeric.c: numeric values of a fixed scale as Decimal256 (N2, N3).
 * Columns and constants are read with up to 38 digits (as int128), and
 * expressions keep up to 76; a sum adds up values of up to 66.
 */
#define DF_NUMERIC_MAX_PRECISION 38
#define DF_NUMERIC_MAX_EXPR_PRECISION 76
#define DF_NUMERIC_NAN	((int128) (~(uint128) 0 >> 1))	/* above 10^38 */
/* the infinities (DT2): below every value, above every one but NaN */
#define DF_NUMERIC_PINF	(DF_NUMERIC_NAN - 1)
#define DF_NUMERIC_NINF	(-DF_NUMERIC_NAN - 1)
#define DF_NUMERIC_BYTES 32		/* a Decimal256 value, little-endian */

typedef enum DfNumericFit
{
	DF_NUMERIC_FITS,
	DF_NUMERIC_INFINITE,
	DF_NUMERIC_TOO_LONG			/* more digits than 38 at that scale */
} DfNumericFit;

extern bool df_numeric_typmod(int32 typmod, int *precision, int *scale);
extern DfNumericFit df_numeric_value(Datum d, int scale, int128 *out);
extern bool df_numeric_const_ps(Datum d, int *precision, int *scale);
extern void df_numeric_store(int128 v, uint8 *dst);
extern Datum df_numeric_datum(const uint8 *src, int scale);

/* How an output column of the slice becomes a value of its tuple. */
typedef enum DfOutKind
{
	DF_OUT_PLAIN,				/* one column, one value */
	DF_OUT_NUMERIC_AVG,			/* numeric sum and the next column's count:
								 * avg = sum / count (numeric_div) */
	DF_OUT_NUMERIC_MIXED,		/* numeric sum and the next column's display
								 * scale (MS1) */
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
extern const char *df_bpchar_func(Oid funcid);
extern bool df_numeric_ps(Plan *ctx, Node *expr, int *precision, int *scale);
extern bool df_numeric_mixed(Plan *ctx, Node *expr, int *precision, int *scale);
extern bool df_mixed_sum_scale(Plan *ctx, Node *expr, int *scale);
extern const char *df_extract_field(FuncExpr *fe);

/*
 * Casts DataFusion runs (E2), by pg_proc OID: "widen" ones are exact or
 * round as C does (DataFusion's cast); the others are df_core::pgcast's,
 * with PostgreSQL's rounding and errors.
 */
extern const char *df_cast_kind(Oid funcid);

/* df_exec.c */
extern bool df_exec_attach(QueryDesc *queryDesc, PlanState *root,
						   MotionState *send, const DfTails *tails,
						   char *reason, size_t reasonlen);
extern PlanState *df_exec_find_state(PlanState *ps, Plan *plan);
extern uint64 df_runs_completed;	/* slices DataFusion ran to the end */
extern DfQueryStats df_last_run;	/* figures of the latest one */

/* df_translate.c */
extern const char *df_type_tag(Oid type);
extern bool df_translate_slice(Plan *root, const DfTails *tails, DfSliceSpec *spec,
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
						   DfTails *tails, char *reason, size_t reasonlen);
extern List *df_slice_attach_points(PlannedStmt *stmt, Plan *compute, Bitmapset *batches);
extern Plan *df_local_slice_root(QueryDesc *queryDesc, int *slice_index,
								 bool *is_sender);
extern void df_explain_slices(PlannedStmt *stmt, StringInfo out);
extern double df_hash_budget(Plan *hash);
extern Oid	df_join_key_type(Oid a, Oid b);
extern bool df_limit_value(Node *expr, int64 *value);
extern bool df_sort_direction(Oid sortop, Oid type, bool *desc);
extern bool df_passes_through(Plan *plan);
extern Node *df_semi_key_for(HashJoin *hj, Var *var);
extern bool df_numeric_param_cmp(Plan *ctx, OpExpr *op, Node **other, bool *param_left);

/* IP1: init plan values for the translation of a slice that starts, or NULL */
extern ParamExecData *df_param_values;
extern ExprContext *df_param_econtext;
extern bool df_date_timestamp_cmp(OpExpr *op, Node **date_arg, const char **cmp,
								  int32 *value);

/* 'plan', or the child of it if it is a Sort (one D2 leaves out) */
static inline Plan *
df_below_sort(Plan *plan)
{
	return plan != NULL && IsA(plan, Sort) ? outerPlan(plan) : plan;
}

/* df_runtime.c */
extern int	df_runtime_ensure(void);
extern const char *df_spill_dir(void);
extern void df_vmem_sync(int64 headroom);
extern void df_vmem_trim(void);
extern int64 df_vmem_leased_bytes(void);
extern int32 df_task_wait_interruptible(DfTask *task, char *buf, size_t buflen);

#endif							/* DF_EXECUTOR_H */
