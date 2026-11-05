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
 * vx_executor.h
 *	  Declarations shared by the C sources of vec_executor.
 *
 * src/vx_executor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VX_EXECUTOR_H
#define VX_EXECUTOR_H

#include <signal.h>

#include "vec_engine.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/* GUC vec.mode */
typedef enum VxMode
{
	VX_MODE_OFF,				/* hooks pass everything through */
	VX_MODE_EXPLAIN,			/* EXPLAIN reports eligibility; nothing runs */
	VX_MODE_ON					/* eligible slices run in DataFusion */
} VxMode;

extern int	vx_mode;

/* vec.join_estimates: how far a hash join's build estimate is trusted */
typedef enum VxJoinEstimates
{
	VX_JOIN_ESTIMATES_SPILL,	/* hash joins spill: no size to check */
	VX_JOIN_ESTIMATES_TRUSTED,
	VX_JOIN_ESTIMATES_BOUNDED,
	VX_JOIN_ESTIMATES_STRICT
} VxJoinEstimates;
extern int	vx_join_estimates;

/* GUC vec.worker_threads */
extern int	vx_worker_threads;

/* GUC vec.engine */
extern char *vx_engine_name;
/* GUC vec.engine_options, passed to the engine as it is */
extern char *vx_engine_options;

/* GUCs vec.debug_features_off, vec.debug_result_offset */
extern int	vx_debug_features_off;
extern int	vx_debug_result_offset;

/* vx_init.c */
extern const VecEngine *vx_engine;	/* NULL until vx_engine_load */
extern const VecEngine *vx_engine_load(void);
extern uint64 vx_engine_features(void);
extern bool vx_engine_numeric(void);
extern const char *vx_feature_name(uint64 feature);
extern const VecEngineDebug *vx_engine_debug(void);
extern void vx_raise(int32 status, const char *msg) pg_attribute_noreturn();

/* A VecError of no message yet, an internal error if the engine sets none. */
#define VX_ERROR_INIT(e) \
	(memcpy((e).sqlstate, "XX000", 6), (e).message[0] = '\0')

/* One input of a slice: the rows of a Seq Scan or of a receiving Motion. */
typedef struct VxSliceInput
{
	Plan	   *leaf;			/* the Seq Scan or Motion node */
	int			ncols;			/* columns read */
	AttrNumber *attnos;			/* table columns, or Motion stream
								 * positions + 1 */
	Oid		   *types;
	int16	   *scales;			/* of numeric columns (Decimal128) */
	int16	   *digits;			/* N6: of a numeric column read narrow, as
								 * Decimal128(digits, scale) of 16 bytes; 0
								 * for Decimal256 of 32 */
	bool		set;			/* SP1: a subquery's rows, read whole first */
} VxSliceInput;

/*
 * Output columns PostgreSQL finishes (P1): the slice's output columns that
 * DataFusion cannot compute whole, and the subexpressions of them that it
 * computes.  PostgreSQL evaluates the rest of each such column over those
 * values, row by row, as its own projection would (numeric division, any
 * function).  vx_check_slice finds them; vx_translate_slice uses them.
 */
typedef struct VxTails
{
	List	   *tles;			/* TargetEntry of each such column */
	List	   *leaves;			/* Node: subexpressions DataFusion computes */
	Motion	   *resort;			/* a sorted Motion whose GroupAggregate below
								 * runs hashed: the slice sorts its output by
								 * the Motion's keys (D3) */
	Plan	   *resort_by;		/* or, with a Limit between them, by the
								 * keys of this Sort or merging Motion below
								 * the GroupAggregate, its order (D4); or so
								 * at the top of the slice, with no Motion
								 * (D7) */
	Plan	   *window_order;	/* or by the keys of this Sort or merging
								 * Motion below the WindowAgg at the top of
								 * the slice, the order its rows leave in
								 * (WIN1) */
} VxTails;

/*
 * T2: a subtree of a slice DataFusion runs below PostgreSQL's nodes, when
 * the slice as a whole cannot run (vx_slice_attach_points).
 */
typedef struct VxAttach
{
	Plan	   *plan;			/* its top node */
	VxTails		tails;			/* as vx_check_slice found them */
	bool		locale_dependent;	/* its verdict depends on the node */
} VxAttach;

/* What vx_translate_slice produces for one slice. */
typedef struct VxSliceSpec
{
	char	   *json;			/* plan for the Rust side */
	int			ninputs;		/* in the order they are fed */
	VxSliceInput *inputs;
	int			nout;			/* output columns, in targetlist order */
	Oid		   *out_types;
	uint8	   *out_kinds;		/* VxOutKind of each */
	int16	   *out_scales;		/* of numeric ones */
	int			batch_rows;		/* rows per batch, from the widest row */

	/*
	 * The tuple's columns (P1).  The output columns above are values: one
	 * per value, two for VX_OUT_NUMERIC_AVG, VX_OUT_NUMERIC_MIXED and the
	 * states of two columns.
	 * Column k of the tuple is value col_value[k], or, if that is -1,
	 * col_tail[k] evaluated over the values (Vars of OUTER_VAR, attno =
	 * value + 1).  col_tail is NULL without such columns.
	 */
	int			ncols;
	int		   *col_value;
	Expr	  **col_tail;
	int			nvalues;
	Oid		   *value_types;
} VxSliceSpec;

/*
 * Aggregates whose state DataFusion keeps itself.  Split through a batch
 * Motion, the partial stage sends that state (one or two columns) instead
 * of PostgreSQL's; numeric results are built on the C side with
 * PostgreSQL's own functions.
 */
typedef enum VxAggState
{
	VX_AGG_PLAIN,				/* the result type is the state */
	VX_AGG_AVG_FLOAT,			/* avg(float4/8): float8 sum, int8 count */
	VX_AGG_AVG_INT,				/* avg(int2/4/8): numeric sum, int8 count */
	VX_AGG_SUM_INT8,			/* sum(int8): numeric sum */
	VX_AGG_SUM_NUMERIC,			/* sum(numeric(p <= 28, s)): numeric sum */
	VX_AGG_AVG_NUMERIC,			/* avg(numeric(p <= 28, s)): numeric sum, count */
	VX_AGG_SUM_NUMERIC_MIXED,	/* sum of a CASE or COALESCE whose branches
								 * differ in scale (MS1): numeric sum at the
								 * largest, int4 largest scale added up */
	VX_AGG_STDDEV				/* split stddev or variance of int2/4 (STD2):
								 * numeric sum, int8 count, numeric sum of
								 * squares */
} VxAggState;

/* sum and avg of numeric(p, s): p digits added up must stay within 76 */
#define VX_NUMERIC_MAX_SUM_PRECISION 66

extern VxAggState vx_agg_state(Aggref *agg);
/* STD1: the largest display scale of a stddev or variance of int2/int4 */
#define VX_STDDEV_MAX_SCALE 44
extern bool vx_stddev_agg(Aggref *agg, bool *sample, bool *variance);
extern VxAggState vx_agg_state_at(Plan *ctx, Aggref *agg);
extern bool vx_collect_aggrefs(Node *node, List **aggs);
extern int	vx_avg_scale(int s);
extern int	vx_agg_state_ncols(VxAggState state);

/*
 * vx_numeric.c: numeric values of a fixed scale as Decimal256 (N2, N3).
 * Columns and constants are read with up to 38 digits (as int128), values
 * received as tuples with up to 76, and expressions keep up to 76; a sum
 * adds up values of up to 66.
 */
#define VX_NUMERIC_MAX_PRECISION 38
#define VX_NUMERIC_MAX_EXPR_PRECISION 76
#define VX_NUMERIC_NAN	((int128) (~(uint128) 0 >> 1))	/* above 10^38 */
/* the infinities (DT2): below every value, above every one but NaN */
#define VX_NUMERIC_PINF	(VX_NUMERIC_NAN - 1)
#define VX_NUMERIC_NINF	(-VX_NUMERIC_NAN - 1)
#define VX_NUMERIC_BYTES 32		/* a Decimal256 value, little-endian */
#define VX_NARROW_BYTES 16		/* N6: a Decimal128 value, little-endian */

typedef enum VxNumericFit
{
	VX_NUMERIC_FITS,
	VX_NUMERIC_INFINITE,
	VX_NUMERIC_TOO_LONG			/* more digits than 38 at that scale */
} VxNumericFit;

extern bool vx_numeric_typmod(int32 typmod, int *precision, int *scale);
extern VxNumericFit vx_numeric_value(Datum d, int scale, int128 *out);
extern VxNumericFit vx_numeric_value_wide(Datum d, int scale, uint8 *dst);
extern bool vx_numeric_const_ps(Datum d, int *precision, int *scale);
extern void vx_numeric_store(int128 v, uint8 *dst);
extern Datum vx_numeric_datum(const uint8 *src, int scale);

/* How an output column of the slice becomes a value of its tuple. */
typedef enum VxOutKind
{
	VX_OUT_PLAIN,				/* one column, one value */
	VX_OUT_NUMERIC_AVG,			/* numeric sum and the next column's count:
								 * avg = sum / count (numeric_div) */
	VX_OUT_NUMERIC_MIXED,		/* numeric sum and the next column's display
								 * scale (MS1) */
	VX_OUT_PART,				/* consumed with the column before */
	/*
	 * PS1: a partial aggregate's state, from DataFusion's (M7d) and the
	 * count of its values, as PostgreSQL's serial function writes it, for
	 * tuples
	 */
	VX_OUT_STATE_NUMERIC,		/* numeric sum and the next column's count:
								 * numeric_avg_serialize's bytea */
	VX_OUT_STATE_INT8,			/* numeric sum and the next column's count:
								 * int8_avg_serialize's bytea */
	VX_OUT_STATE_INT_ARRAY		/* numeric sum and the next column's count:
								 * int4_avg_accum's int8[] {count, sum} */
} VxOutKind;

/* String functions DataFusion runs (df_core::pgstr), by pg_proc OID. */
typedef enum VxCollRule
{
	VX_COLL_ANY,				/* the collation does not matter */
	VX_COLL_DETERMINISTIC,		/* substring searches: PostgreSQL rejects others */
	VX_COLL_CTYPE_C				/* case mapping: ASCII only under C */
} VxCollRule;

typedef struct VxStringFunc
{
	Oid			funcid;
	const char *name;			/* as df_core::pgstr knows it */
	VxCollRule	rule;
} VxStringFunc;

extern const VxStringFunc *vx_string_func(Oid funcid);
extern const char *vx_bpchar_func(Oid funcid);
extern const char *vx_collation_locale(Oid collation);
extern PlannedStmt *vx_numeric_stmt;
extern Agg *vx_motion_consumer_agg(Plan *top);
extern bool vx_numeric_ps(Plan *ctx, Node *expr, int *precision, int *scale);
extern int	vx_bpchar_length(Plan *ctx, Node *expr);
extern bool vx_numeric_range(Plan *ctx, Node *expr, int *precision, int *scale, bool *dyn);
extern CaseExpr *vx_case_searched(CaseExpr *ce);
extern bool vx_numeric_dyn(Plan *ctx, Node *expr, int *precision, int *scale);
extern Plan *vx_subplan_rows(PlannedStmt *stmt, SubPlan *sp);
extern const char *vx_subplan_in_set(PlannedStmt *stmt, SubPlan *sp, Node **arg);
extern const char *vx_subplan_scalar(PlannedStmt *stmt, SubPlan *sp, Plan **top,
									 TargetEntry **tle);

/*
 * SC1: a correlated scalar subquery, an aggregate of rows equal to the
 * outer row on some keys, as that aggregate grouped by those keys and
 * joined to the outer rows (vx_subplan_correlated)
 */
typedef struct VxCorrelated
{
	SubPlan    *sp;
	Agg		   *agg;			/* the aggregate, grouped by the keys */
	Result	   *top;			/* over it, the node its columns are read in */
	List	   *outer;			/* the outer row's side of each key */
	List	   *inner;			/* the subquery's side, a Var over 'top' */
	Node	   *value;			/* the subquery's value, over 'top' */
} VxCorrelated;
extern const char *vx_subplan_correlated(PlannedStmt *stmt, SubPlan *sp,
										 VxCorrelated **corr);
/* AVG1: does it depend on an avg returning numeric (of an output column)? */
extern bool vx_numeric_varying(Plan *ctx, Node *expr);
extern bool vx_output_varying(Plan *ctx, Node *expr);
/*
 * DN1: the scale of an input column that holds the display scales of a
 * numeric column of the same input attribute (vx_numeric_dscale).
 */
#define VX_SCALE_DSCALE (-1)
extern int	vx_numeric_dscale(Datum d);
extern bool vx_numeric_mixed(Plan *ctx, Node *expr, int *precision, int *scale);
extern bool vx_mixed_sum_scale(Plan *ctx, Node *expr, int *scale);
extern const char *vx_extract_field(FuncExpr *fe);

/*
 * Casts DataFusion runs (E2), by pg_proc OID: "widen" ones are exact or
 * round as C does (DataFusion's cast); the others are df_core::pgcast's,
 * with PostgreSQL's rounding and errors.
 */
extern const char *vx_cast_kind(Oid funcid);

/* vx_exec.c */
extern bool vx_exec_attach(QueryDesc *queryDesc, PlanState *root,
						   MotionState *send, const VxTails *tails,
						   char *reason, size_t reasonlen);
extern PlanState *vx_exec_find_state(PlanState *ps, Plan *plan);
extern uint64 vx_runs_completed;	/* slices DataFusion ran to the end */
extern VecQueryStats vx_last_run;	/* figures of the latest one */

/* vx_translate.c */
extern const char *vx_type_tag(Oid type);
extern bool vx_translate_slice(Plan *root, const VxTails *tails, bool tuples, VxSliceSpec *spec,
							   char *reason, size_t reasonlen);

/*
 * vx_paxload.c: the experimental direct PAX reader, vec_pax.so, built
 * from patches/pax.  VxPaxReader mirrors PaxDirectScanApi in the patch's
 * access/pax_direct_api.h, version VX_PAX_SCAN_API_VERSION; keep the
 * two identical.
 */
#define VX_PAX_SCAN_API_VERSION 10

/* What the scan's min/max skipping did (PaxDirectScanInfo). */
typedef struct VxPaxScanInfo
{
	int64		files;			/* micro-partitions visible to the snapshot */
	int64		files_skipped;	/* skipped by their statistics */
	int64		groups;			/* groups examined in the remaining files */
	int64		groups_skipped; /* skipped by their statistics */
} VxPaxScanInfo;

typedef void *(*VxPaxBegin) (Relation rel, Snapshot snapshot, List *qual,
							 const int *cols, const int *widths,
							 const int *scales, int ncols,
							 char *err, size_t errlen);
typedef int (*VxPaxNBlocks) (void *scan);

/* What the decoded-column cache did in this backend (PaxDirectCacheStats). */
typedef struct VxPaxCacheStats
{
	int64		hits;			/* groups read from the cache */
	int64		fills;			/* entries written */
	int64		bad;			/* entries that did not fit, removed */
	int64		evicted;		/* micro-partitions removed for the limit */
	int64		refused;		/* groups left out: the cache was full */
	int64		unusable;		/* scans whose cache directory was unusable */
} VxPaxCacheStats;

typedef void (*VxPaxCacheConfig) (void *scan, const char *dir, int64 limit);
typedef void (*VxPaxCacheStatsFn) (VxPaxCacheStats *out);
typedef void (*VxPaxInfo) (void *scan, VxPaxScanInfo *out);

typedef struct VxPaxReader
{
	uint32		version;
	const char *pax_build_id;	/* the pax.so it was built against */
	VxPaxBegin	begin;
	VxPaxNBlocks nblocks;
	VxPaxInfo	info;
	VecPaxRead	read;
	VecPaxEnd	end;
	VecPaxGroups block_groups;
	VecPaxLate	late;
	VecPaxReadRows read_rows;
	VxPaxCacheConfig cache_config;
	VxPaxCacheStatsFn cache_stats;
} VxPaxReader;

extern bool vx_pax_direct_read;	/* GUC vec.pax_direct_read */
extern int	vx_pax_max_decoders;	/* GUC vec.pax_max_decoders */
extern char *vx_pax_decode_cache;	/* GUC vec.pax_decode_cache */
extern int	vx_pax_decode_cache_mb; /* GUC vec.pax_decode_cache_size */
extern int	vx_slice_partitions;	/* GUC vec.slice_partitions */
extern const VxPaxReader *vx_pax_reader_get(void);
extern const VxPaxReader *vx_pax_reader_loaded(void);
extern uint64 vx_pax_direct_scans;	/* scans read through it */
extern bool vx_last_run_pax;	/* vx_last_run read PAX directly */
extern VxPaxScanInfo vx_last_pax_scan;	/* and skipped this */

/* M7b: Gather Motions between DataFusion slices carry Arrow IPC batches */
extern bool vx_motion_batches;	/* GUC vec.motion_batches */
extern bool vx_cluster_collation_c;	/* GUC vec.cluster_collation_c */
extern void vx_sync_cluster_collation(void);
extern bool vx_motion_sends_batches(PlannedStmt *stmt, Motion *motion);
extern Bitmapset *vx_batch_motions(PlannedStmt *stmt);
extern int	vx_motion_state_columns(Motion *motion, AttrNumber resno);
extern Aggref *vx_motion_column_partial(Motion *motion, AttrNumber resno, Plan **agg);
extern AttrNumber vx_output_of(Plan *plan, AttrNumber attno);

/*
 * GS1: the grouping of an Agg with grouping sets (ROLLUP, CUBE, GROUPING
 * SETS), its own and its chain's.  Without them, one set of all columns.
 */
typedef struct VxAggGroups
{
	int			ncols;
	AttrNumber *cols;			/* the child columns grouped by: the Agg's,
								 * then its chain's not among them */
	Oid		   *collations;
	int			nsets;			/* grouping sets, 0 without them */
	Bitmapset **sets;			/* each set's child columns, in the order
								 * of their GROUPINGSET_ID() (nodeAgg.c) */
	int		   *group_ids;		/* each set's GROUP_ID() */
} VxAggGroups;

extern void vx_agg_groups(Agg *agg, VxAggGroups *g);
extern int	vx_agg_group_index(Agg *agg, AttrNumber attno);
extern int	vx_grouping_value(List *cols, Bitmapset *set);
extern bool vx_motion_pg_state(Motion *motion, AttrNumber resno);
extern Aggref *vx_partial_path(Plan **ctx, Node *arg, Motion **motion, AttrNumber *resno);
extern bool vx_nestloop_swapped(Plan *plan);
extern int	vx_motion_stream_column(Motion *motion, AttrNumber resno);
extern int	vx_motion_bpchar_pad(PlannedStmt *stmt, Motion *motion, AttrNumber resno);
extern int	vx_motion_stream_pad(PlannedStmt *stmt, Motion *motion, int pos);
extern int	vx_motion_numeric_digits(PlannedStmt *stmt, Motion *motion, AttrNumber resno);
extern int	vx_motion_stream_digits(PlannedStmt *stmt, Motion *motion, int pos);
extern uint64 vx_motion_signature(PlannedStmt *stmt, Motion *motion);
extern bool vx_motion_hash_key(Motion *motion, int i, int *column, const char **tag);

/* vx_hooks.c */
extern void vx_install_hooks(void);

/* vx_regex.c: RX1, the regular expressions of regexp_replace DataFusion runs */
typedef struct VxRegexSeg
{
	StringInfoData re;			/* in PostgreSQL's syntax, of the subset taken */
	bool		capture;		/* the contents of a capturing group */
} VxRegexSeg;

extern List *vx_regex_segments(const char *pattern, const char **problem);
extern const char *vx_regexp_replace_problem(FuncExpr *fe, List **segments,
											  char **replacement);

/* vx_rewrite.c: SQ1, filters of correlated aggregate subqueries */
extern bool vx_subquery_key_filter;	/* GUC vec.subquery_key_filter */
extern void vx_install_planner_hook(void);

/* vx_plan_check.c */
extern bool vx_check_slice(PlannedStmt *stmt, Plan *root, bool root_is_sender,
						   VxTails *tails, char *reason, size_t reasonlen);
extern List *vx_slice_attach_points(PlannedStmt *stmt, Plan *compute, Bitmapset *batches);
extern List *vx_slice_producers(Plan *plan);
extern void vx_batch_motions_forget(void);
extern Plan *vx_local_slice_root(QueryDesc *queryDesc, int *slice_index,
								 bool *is_sender);
extern void vx_explain_slices(PlannedStmt *stmt, StringInfo out);
extern double vx_hash_budget(Plan *hash);
extern Oid	vx_join_key_type(Oid a, Oid b);
extern bool vx_limit_value(Node *expr, int64 *value);
extern bool vx_sort_direction(Oid sortop, Oid type, bool *desc);
extern bool vx_passes_through(Plan *plan);
/* WIN1: window functions */
extern List *vx_window_funcs(WindowAgg *wa);
extern Aggref *vx_window_aggref(WindowFunc *wf);
extern bool vx_window_avg(WindowFunc *wf);
extern const char *vx_window_frame(WindowAgg *wa);
extern List *vx_window_run_condition(WindowAgg *wa);
extern Plan *vx_window_order_input(Plan *input);
extern bool vx_window_order(Plan *plan, AttrNumber col, Oid *sortop, bool *nulls_first,
							Oid *collation);
extern Node *vx_semi_key_for(HashJoin *hj, Var *var);
extern bool vx_numeric_param_cmp(Plan *ctx, OpExpr *op, Node **other, bool *param_left);
extern bool vx_numcmp_needed(Plan *ctx, OpExpr *op);
extern List *vx_append_plans(Plan *plan);
extern int	vx_round_scale(FuncExpr *fe);
extern bool vx_null_equal_join(HashJoin *hj);
extern int	vx_one_time_value(Node *qual, bool evaluate);
extern bool vx_empty_result(Plan *plan);
extern int	vx_setop_group(SetOp *setop, AttrNumber attno);
extern int	vx_unique_group(Unique *uq, AttrNumber attno);

/* IP1: init plan values for the translation of a slice that starts, or NULL */
extern ParamExecData *vx_param_values;
extern ExprContext *vx_param_econtext;
extern bool vx_date_timestamp_cmp(OpExpr *op, Node **date_arg, const char **cmp,
								  int32 *value);

/* 'plan', or the child of it if it is a Sort (one D2 leaves out) */
static inline Plan *
vx_below_sort(Plan *plan)
{
	return plan != NULL && IsA(plan, Sort) ? outerPlan(plan) : plan;
}

/*
 * vx_arrow.c: a column of a result batch, read row by row.  Rows count
 * from the batch's first; 'bit' is where it starts in the validity bitmap,
 * and in a bool's bitmap of values.
 */
typedef struct VxOutCol
{
	const uint8 *valid;			/* 1: not NULL; NULL without NULLs */
	int64		bit;
	const void *values;			/* the first row's value, a bool's bitmap,
								 * or a string's bytes */
	const int32 *offsets;		/* a string's: the first row's */
} VxOutCol;

static inline bool
vx_out_isnull(const VxOutCol *c, uint32 r)
{
	int64		b = c->bit + r;

	return c->valid != NULL && (c->valid[b >> 3] & (1 << (b & 7))) == 0;
}

static inline bool
vx_out_bool(const VxOutCol *c, uint32 r)
{
	int64		b = c->bit + r;

	return (((const uint8 *) c->values)[b >> 3] & (1 << (b & 7))) != 0;
}

extern const char *vx_arrow_format(Oid type, int scale);
extern void vx_arrow_struct_schema(struct ArrowSchema *s, int n, const char *const *formats);
extern void vx_arrow_struct_array(struct ArrowArray *a, int n);
extern void vx_arrow_set_column(struct ArrowArray *col, int64 length, int64 null_count,
								const uint8 *valid, const void *values, const int32 *offsets);
extern void vx_arrow_out_column(VxOutCol *out, const struct ArrowArray *batch, int c,
								int width);

/* vx_runtime.c */
extern void vx_signals_block(sigset_t *saved);
extern void vx_signals_restore(const sigset_t *saved);
extern int	vx_runtime_ensure(void);
extern void vx_case_maps_ensure(void);
extern bool vx_default_case_maps(void);
extern bool vx_ctype_is_c(Oid collation);
extern const char *vx_spill_dir(void);
extern void vx_vmem_sync(int64 headroom);
extern void vx_vmem_trim(void);
extern int64 vx_vmem_leased_bytes(void);
extern int32 vx_task_wait_interruptible(VecTask *task, VecError *err);

#endif							/* VX_EXECUTOR_H */
