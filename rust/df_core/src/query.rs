// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! One slice executed by DataFusion.
//!
//! The backend's main thread scans the table and pushes column batches in;
//! DataFusion filters and aggregates them on the runtime; the main thread
//! polls the result batches and turns their rows into tuples.  Both
//! directions go through bounded channels, so a slow consumer slows the
//! producer instead of piling up memory.
//!
//! The plan arrives as JSON built by the C side (src/df_translate.c), as a
//! tree over one or more inputs:
//!
//! ```text
//! { "inputs":  [ { "columns": [ {"type": "int4"}, ... ],
//!                  "motion_columns": [k, ...] }, ... ],   (motion_columns:
//!                input from a Motion, the stream column behind each column)
//!   "plan":    <node>,
//!   "output":  [ {"expr": <expr>, "type": "int8"}, ... ],
//!   "route":   ... }                                   (as below)
//!
//! <node> := {"input": j}
//!         | {"filter": {"input": <node>, "pred": <expr>}}
//!         | {"aggregate": {"input": <node>, "group": [<expr>...],
//!                          "aggs": [...], "having": <expr> | null}}
//!         | {"join": {"type": "inner" | "left" | "right" | "full" | "leftsemi"
//!                           | "rightsemi" | "leftanti" | "rightanti",
//!                     "left": <node>, "right": <node>,
//!                     "on": [[<left expr>, <right expr>], ...],
//!                     "filter": <expr> | null}}
//!           (an equi-join; DataFusion builds its hash table on the left,
//!           NULL keys match nothing)
//! ```
//!
//! {"col": k, "input": j} is column k of input j ("input" defaults to 0);
//! every input column has a name of its own (`input_column`), which it
//! keeps through any node.  The single-input form below is still accepted:
//!
//! ```text
//! { "scan":      { "columns": [ {"type": "int4"}, ... ],
//!                  "motion_columns": [k, ...] },   (input from a Motion: the
//!                  0-based Motion column behind each input column)
//!   "filter":    <expr> | null,
//!   "aggregate": { "group": [<expr>...], "aggs": [ {"fn": "sum", "arg": <expr>} ... ] } | null,
//!                fn: count, sum, min, max, avg, count_merge (adds up partial
//!                counts, 0 without rows: PostgreSQL's combining count), or
//!                avg_merge with "arg2" (sum of arg / sum of arg2, NULL when
//!                that is 0: combines avg states of sum and count)
//!   "having":    <expr> | null,
//!   "output":    [ {"expr": <expr>, "type": "int8"}, ... ],
//!   "route":     { "kind": "hash", "keys": [ {"col": k, "hash": "int4"}, ... ],
//!                  "segments": n, "workers": w } }   (optional, IPC output
//!                through a Redistribute Motion: one stream per route)
//!
//! <expr> := {"col": i} | {"group": i} | {"agg": i}
//!         | {"lit": {"type": t, "value": v}} | {"lit": {"type": t, "null": true}}
//!         | {"op": "+", "type": t, "args": [<expr>, <expr>]}
//!         | {"and": [...]} | {"or": [...]} | {"not": <expr>}
//!         | {"isnull": <expr>} | {"isnotnull": <expr>}
//!         | {"cast": <expr>, "type": t}   (a widening one: int2/4/8, float4/8)
//! ```

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray, StringArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::compute::{cast, take};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Schema, SchemaRef,
};
use datafusion::arrow::ipc::reader::StreamDecoder;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::ScalarValue;
use datafusion::datasource::provider_as_source;
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::{binary_expr, when, Expr, LogicalPlanBuilder, Operator};
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{col, lit, SessionConfig, SessionContext};
use futures::StreamExt;
use serde_json::Value;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::cdbhash::{self, KeyHash};
use crate::memory::TrackingPool;
use crate::pgfunc::{to_pg_error, ArithOp, PgArith, PgError};
use crate::runtime;

/// Rows per input batch the main thread pushes.
pub const BATCH_ROWS: usize = 8192;

/// Batches buffered in each direction.
const CHANNEL_DEPTH: usize = 4;

/// Depth of the queue of received IPC bytes: the interconnect hands them
/// over a chunk (a few kB) at a time.
const IPC_CHANNEL_DEPTH: usize = 64;

/// How long `push_ipc` waits for room in a full queue.
const IPC_PUSH_WAIT: Duration = Duration::from_millis(1);

/// Operator memory each partition of a grouped aggregate needs at least.
/// The pool is shared fairly among the operators that can spill, two per
/// partition (partial and final); a final aggregate's first allocation
/// alone is a few hundred kB.  A small budget therefore runs on fewer
/// partitions instead of starving all of them.  Plans without a grouped
/// aggregate hold only a few accumulators per partition and are not capped;
/// Cloudberry gives such operators little memory (a plain Agg gets 100 kB
/// from the resource queue), which would otherwise force one partition.
const MIN_PARTITION_MEMORY: usize = 2 << 20;

/// The PostgreSQL types a slice can carry so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    // Kept as PostgreSQL stores them, in its own epoch: days (date) or
    // microseconds (time, timestamp, timestamptz).  Only compared.
    Date,
    Time,
    Timestamp,
    Timestamptz,
    // text and varchar: UTF-8 bytes, compared byte by byte.
    Text,
}

impl PgType {
    fn parse(name: &str) -> Result<PgType, String> {
        Ok(match name {
            "bool" => PgType::Bool,
            "int2" => PgType::Int2,
            "int4" => PgType::Int4,
            "int8" => PgType::Int8,
            "float4" => PgType::Float4,
            "float8" => PgType::Float8,
            "date" => PgType::Date,
            "time" => PgType::Time,
            "timestamp" => PgType::Timestamp,
            "timestamptz" => PgType::Timestamptz,
            "text" => PgType::Text,
            other => return Err(format!("unsupported type {other}")),
        })
    }

    /// The type whose values this one's are laid out as.
    pub fn storage(self) -> PgType {
        match self {
            PgType::Date => PgType::Int4,
            PgType::Time | PgType::Timestamp | PgType::Timestamptz => PgType::Int8,
            t => t,
        }
    }

    /// The distribution hash of a column of this type: hashint4 for date,
    /// and time_hash and timestamp_hash are hashint8.
    pub fn key_hash(self) -> KeyHash {
        match self.storage() {
            PgType::Text => KeyHash::Text,
            PgType::Bool => KeyHash::Bool,
            PgType::Int2 => KeyHash::Int2,
            PgType::Int4 => KeyHash::Int4,
            PgType::Int8 => KeyHash::Int8,
            PgType::Float4 => KeyHash::Float4,
            PgType::Float8 => KeyHash::Float8,
            _ => unreachable!(),
        }
    }

    pub fn arrow(self) -> DataType {
        match self.storage() {
            PgType::Bool => DataType::Boolean,
            PgType::Int2 => DataType::Int16,
            PgType::Int4 => DataType::Int32,
            PgType::Int8 => DataType::Int64,
            PgType::Float4 => DataType::Float32,
            PgType::Float8 => DataType::Float64,
            PgType::Text => DataType::Utf8,
            _ => unreachable!(),
        }
    }
}

/// Column data as the C side lays it out: native values (one byte per bool)
/// and one byte per row that is 1 for NULL.  A string column's `values` are
/// its bytes, row r being `offsets[r]..offsets[r + 1]`; `offsets` is null
/// for the other types.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawColumn {
    pub values: *const u8,
    pub nulls: *const u8,
    pub offsets: *const i32,
}

impl RawColumn {
    pub fn fixed(values: *const u8, nulls: *const u8) -> RawColumn {
        RawColumn { values, nulls, offsets: std::ptr::null() }
    }
}

/// A column as the PAX reader hands it out (fixed-width types only).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PaxColumn {
    pub values: *const u8,
    pub nulls: *const u8,
}

/// Converted output column whose buffers stay valid until the next poll.
struct OutColumn {
    _array: ArrayRef,
    offsets: *const i32,
    bools: Option<Vec<u8>>,
    nulls: Vec<u8>,
    values: *const u8,
}

pub enum Poll {
    Batch(usize),
    /// Arrow IPC stream bytes of the results (`output_bytes`).
    Bytes(usize),
    Pending,
    Done,
    Failed(PgError),
    Panicked(String),
}

// ---------------------------------------------------------------------------
// Plan construction
// ---------------------------------------------------------------------------

fn field<'a>(v: &'a Value, key: &str) -> Result<&'a Value, String> {
    v.get(key).ok_or_else(|| format!("plan spec: missing \"{key}\""))
}

fn index(v: &Value) -> Result<usize, String> {
    v.as_u64().map(|i| i as usize).ok_or_else(|| "plan spec: bad index".to_string())
}

fn float_value(v: &Value) -> Result<f64, String> {
    match v {
        Value::Number(n) => n.as_f64().ok_or_else(|| "bad float".to_string()),
        Value::String(s) => match s.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(format!("bad float {s}")),
        },
        _ => Err("bad float".to_string()),
    }
}

fn literal(v: &Value) -> Result<Expr, String> {
    let ty = PgType::parse(field(v, "type")?.as_str().unwrap_or(""))?;
    let null = v.get("null").and_then(Value::as_bool).unwrap_or(false);
    let val = v.get("value");
    let int = || -> Result<i64, String> {
        val.and_then(Value::as_i64).ok_or_else(|| "bad integer literal".to_string())
    };
    let sv = match ty.storage() {
        PgType::Bool => ScalarValue::Boolean(if null { None } else { val.and_then(Value::as_bool) }),
        PgType::Int2 => ScalarValue::Int16(if null { None } else { Some(int()? as i16) }),
        PgType::Int4 => ScalarValue::Int32(if null { None } else { Some(int()? as i32) }),
        PgType::Int8 => ScalarValue::Int64(if null { None } else { Some(int()?) }),
        PgType::Float4 => ScalarValue::Float32(if null {
            None
        } else {
            Some(float_value(val.ok_or("bad float literal")?)? as f32)
        }),
        PgType::Float8 => ScalarValue::Float64(if null {
            None
        } else {
            Some(float_value(val.ok_or("bad float literal")?)?)
        }),
        PgType::Text => ScalarValue::Utf8(if null {
            None
        } else {
            Some(val.and_then(Value::as_str).ok_or("bad text literal")?.to_string())
        }),
        _ => unreachable!(),
    };
    Ok(lit(sv))
}

fn comparison(name: &str) -> Option<Operator> {
    Some(match name {
        "=" => Operator::Eq,
        "<>" => Operator::NotEq,
        "<" => Operator::Lt,
        "<=" => Operator::LtEq,
        ">" => Operator::Gt,
        ">=" => Operator::GtEq,
        _ => return None,
    })
}

/// The name of column `k` of input `j`.
fn input_column(j: usize, k: usize) -> String {
    if j == 0 {
        format!("c{k}")
    } else {
        format!("c{j}_{k}")
    }
}

fn expr(v: &Value) -> Result<Expr, String> {
    if let Some(i) = v.get("col") {
        let j = match v.get("input") {
            Some(j) => index(j)?,
            None => 0,
        };
        return Ok(col(input_column(j, index(i)?)));
    }
    if let Some(i) = v.get("group") {
        return Ok(col(format!("g{}", index(i)?)));
    }
    if let Some(i) = v.get("agg") {
        return Ok(col(format!("a{}", index(i)?)));
    }
    if let Some(l) = v.get("lit") {
        return literal(l);
    }
    if let Some(op) = v.get("op") {
        let name = op.as_str().unwrap_or("");
        let args = field(v, "args")?.as_array().ok_or("plan spec: bad args")?;
        if args.len() != 2 {
            return Err(format!("operator {name} with {} arguments", args.len()));
        }
        let (a, b) = (expr(&args[0])?, expr(&args[1])?);
        if let Some(cmp) = comparison(name) {
            return Ok(binary_expr(a, cmp, b));
        }
        if let Some(arith) = ArithOp::from_pg(name) {
            // PostgreSQL computes a cross-type operator in its result type.
            let ty = PgType::parse(field(v, "type")?.as_str().unwrap_or(""))?.arrow();
            return Ok(PgArith::udf(arith).call(vec![
                Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(a), ty.clone())),
                Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(b), ty)),
            ]));
        }
        return Err(format!("unsupported operator {name}"));
    }
    if let Some(list) = v.get("and").or_else(|| v.get("or")) {
        let is_and = v.get("and").is_some();
        let items = list.as_array().ok_or("plan spec: bad bool list")?;
        let mut iter = items.iter();
        let mut acc = expr(iter.next().ok_or("plan spec: empty bool list")?)?;
        for item in iter {
            let e = expr(item)?;
            acc = if may_fail(item) {
                // PostgreSQL evaluates AND/OR arguments left to right and
                // skips the rest once the result is decided, so a guard
                // such as `b <> 0 AND a / b > 1` never divides by zero.
                // DataFusion evaluates both sides for the whole batch, so
                // an argument that can raise an error only runs on the
                // rows the earlier ones left undecided: CASE evaluates
                // each branch only for the rows that reach it.
                if is_and {
                    when(acc.clone().is_false(), lit(false)).otherwise(acc.and(e))
                } else {
                    when(acc.clone().is_true(), lit(true)).otherwise(acc.or(e))
                }
                .map_err(|e| e.to_string())?
            } else if is_and {
                acc.and(e)
            } else {
                acc.or(e)
            };
        }
        return Ok(acc);
    }
    if let Some(e) = v.get("not") {
        return Ok(Expr::Not(Box::new(expr(e)?)));
    }
    if let Some(e) = v.get("isnull") {
        return Ok(expr(e)?.is_null());
    }
    if let Some(e) = v.get("isnotnull") {
        return Ok(expr(e)?.is_not_null());
    }
    if let Some(e) = v.get("cast") {
        let ty = PgType::parse(field(v, "type")?.as_str().unwrap_or(""))?.arrow();
        return Ok(Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(expr(e)?), ty)));
    }
    Err(format!("plan spec: unknown expression {v}"))
}

/// Can evaluating this expression raise an error?  Only the PostgreSQL
/// arithmetic operators can (overflow, division by zero).
fn may_fail(v: &Value) -> bool {
    match v {
        Value::Object(map) => {
            if let Some(op) = map.get("op").and_then(Value::as_str) {
                if ArithOp::from_pg(op).is_some() {
                    return true;
                }
            }
            map.values().any(may_fail)
        }
        Value::Array(items) => items.iter().any(may_fail),
        _ => false,
    }
}

fn aggregate(v: &Value) -> Result<Expr, String> {
    let name = field(v, "fn")?.as_str().unwrap_or("");
    let arg = match v.get("arg") {
        Some(a) if !a.is_null() => Some(expr(a)?),
        _ => None,
    };
    let need = |a: Option<Expr>| a.ok_or_else(|| format!("aggregate {name} needs an argument"));
    Ok(match name {
        "count" => count(arg.unwrap_or_else(|| lit(1i64))),
        // 0 instead of NULL without rows: see the projection after the
        // aggregate in Query::start_with.
        "count_merge" => sum(need(arg)?),
        // The sum of the sums; the counts are a second aggregate (`aggregate_extra`).
        "avg_merge" => sum(need(arg)?),
        "sum" => sum(need(arg)?),
        "min" => min(need(arg)?),
        "max" => max(need(arg)?),
        "avg" => avg(need(arg)?),
        other => return Err(format!("unsupported aggregate {other}")),
    })
}

/// The second aggregate an aggregate spec needs, if any: avg_merge's sum of
/// counts.
fn aggregate_extra(v: &Value) -> Result<Option<Expr>, String> {
    if field(v, "fn")?.as_str() != Some("avg_merge") {
        return Ok(None);
    }
    Ok(Some(sum(expr(field(v, "arg2")?)?)))
}

/// DataFusion's default session, minus expression simplification.
///
/// The simplifier rewrites `CASE WHEN x IS FALSE THEN false ELSE x AND y
/// END` back into `x AND y`, which evaluates `y` on every row and undoes
/// the guard that keeps PostgreSQL from dividing by zero (see `expr`).  The
/// plan comes from PostgreSQL's planner, which has already folded constants
/// and simplified expressions, so little is lost.
fn session_state(config: SessionConfig, pool: Arc<TrackingPool>, spill_dir: &str) -> Result<SessionState, DataFusionError> {
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_disk_manager_builder(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(vec![spill_dir.into()])),
        )
        .build_arc()?;
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .build();
    let rules: Vec<_> = state
        .optimizers()
        .iter()
        .filter(|r| r.name() != "simplify_expressions")
        .cloned()
        .collect();
    Ok(SessionStateBuilder::new_from_existing(state)
        .with_optimizer_rules(rules)
        .build())
}

/// Single-use stream fed by the main thread.
struct ChannelPartition {
    schema: SchemaRef,
    rx: Mutex<Option<mpsc::Receiver<Result<RecordBatch, DataFusionError>>>>,
}

impl fmt::Debug for ChannelPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ChannelPartition")
    }
}

impl PartitionStream for ChannelPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let rx = self.rx.lock().ok().and_then(|mut g| g.take());
        let stream = futures::stream::unfold(rx, |rx| async move {
            let mut rx = rx?;
            let item = rx.recv().await?;
            Some((item, Some(rx)))
        });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

// ---------------------------------------------------------------------------
// PAX micro-partitions read on the workers (experimental)
// ---------------------------------------------------------------------------

/// Called by the PAX reader once per group of visible rows.
pub type PaxEmitFn = unsafe extern "C" fn(ctx: *mut c_void, nrows: u32, cols: *const PaxColumn) -> i32;
/// Called by the PAX reader with the change in bytes it holds.
pub type PaxAccountFn = unsafe extern "C" fn(ctx: *mut c_void, delta: i64);
/// read_block of the PAX scan interface (patches/pax): decode one block,
/// emitting its groups and accounting for the memory it holds.
pub type PaxReadFn = unsafe extern "C" fn(
    scan: *mut c_void,
    index: i32,
    emit: PaxEmitFn,
    account: PaxAccountFn,
    ctx: *mut c_void,
    err: *mut c_char,
    errlen: usize,
) -> i32;
/// end of the PAX scan interface: free the scan.
pub type PaxEndFn = unsafe extern "C" fn(scan: *mut c_void);

/// A PAX scan listed on the main thread.  Partitions take blocks from it
/// one at a time; it is freed when the last partition is done with it.
pub struct PaxScan {
    scan: *mut c_void,
    nblocks: usize,
    read: PaxReadFn,
    end: PaxEndFn,
    next: AtomicUsize,
    memory: Arc<PaxMemory>,
}

/// Memory the PAX reader reports holding for one scan, all partitions
/// together.
#[derive(Debug, Default)]
pub struct PaxMemory {
    held: AtomicI64,
    peak: AtomicI64,
}

impl PaxMemory {
    fn add(&self, delta: i64) {
        crate::memory::external_add(delta);
        let now = self.held.fetch_add(delta, Ordering::Relaxed) + delta;
        self.peak.fetch_max(now, Ordering::Relaxed);
    }

    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed).max(0) as u64
    }
}

// SAFETY: the C++ scan object holds no PostgreSQL state and its read
// function is safe to call from several threads for different blocks.
unsafe impl Send for PaxScan {}
unsafe impl Sync for PaxScan {}

impl PaxScan {
    /// Take ownership of a scan; `end` runs when this is dropped.
    pub fn new(scan: *mut c_void, nblocks: usize, read: PaxReadFn, end: PaxEndFn) -> Self {
        PaxScan { scan, nblocks, read, end, next: AtomicUsize::new(0), memory: Arc::default() }
    }
}

impl Drop for PaxScan {
    fn drop(&mut self) {
        // SAFETY: no partition is reading any more (they hold an Arc).
        unsafe { (self.end)(self.scan) }
    }
}

struct EmitContext {
    memory: Arc<PaxMemory>,
    schema: SchemaRef,
    types: Vec<PgType>,
    batches: Vec<RecordBatch>,
    error: Option<String>,
}

unsafe extern "C" fn pax_emit(ctx: *mut c_void, nrows: u32, cols: *const PaxColumn) -> i32 {
    let ctx = &mut *(ctx as *mut EmitContext);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let n = nrows as usize;
        let arrays = ctx
            .types
            .iter()
            .enumerate()
            .map(|(i, ty)| {
                let c = *cols.add(i);
                build_array(*ty, RawColumn::fixed(c.values, c.nulls), n)
            })
            .collect::<Result<Vec<ArrayRef>, PgError>>()?;
        make_batch(&ctx.schema, arrays, n)
    }));
    match result {
        Ok(Ok(batch)) => {
            ctx.batches.push(batch);
            0
        }
        Ok(Err(e)) => {
            ctx.error = Some(e.message);
            1
        }
        Err(_) => {
            ctx.error = Some("panic while building a batch".into());
            1
        }
    }
}

unsafe extern "C" fn pax_account(ctx: *mut c_void, delta: i64) {
    let ctx = &*(ctx as *const EmitContext);
    ctx.memory.add(delta);
}

fn make_batch(schema: &SchemaRef, arrays: Vec<ArrayRef>, nrows: usize) -> Result<RecordBatch, PgError> {
    if arrays.is_empty() {
        RecordBatch::try_new_with_options(
            schema.clone(),
            arrays,
            &datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(nrows)),
        )
    } else {
        RecordBatch::try_new(schema.clone(), arrays)
    }
    .map_err(|e| PgError::internal(e.to_string()))
}

/// One of the partitions reading a PAX scan.
struct PaxPartition {
    schema: SchemaRef,
    types: Vec<PgType>,
    scan: Arc<PaxScan>,
}

impl fmt::Debug for PaxPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PaxPartition")
    }
}

impl PartitionStream for PaxPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        let types = self.types.clone();
        let scan = self.scan.clone();
        let state = (scan, std::collections::VecDeque::<RecordBatch>::new(), false);
        let stream = futures::stream::unfold(state, move |(scan, mut queue, failed)| {
            let schema = schema.clone();
            let types = types.clone();
            async move {
                loop {
                    if let Some(batch) = queue.pop_front() {
                        return Some((Ok(batch), (scan, queue, failed)));
                    }
                    if failed {
                        return None;
                    }
                    let index = scan.next.fetch_add(1, Ordering::Relaxed);
                    if index >= scan.nblocks {
                        return None;
                    }
                    let mut ctx = EmitContext { memory: scan.memory.clone(), schema: schema.clone(), types: types.clone(), batches: Vec::new(), error: None };
                    let mut err = vec![0 as c_char; 1024];
                    // SAFETY: see PaxScan; ctx outlives the call.
                    let rc = unsafe {
                        (scan.read)(
                            scan.scan,
                            index as i32,
                            pax_emit,
                            pax_account,
                            &mut ctx as *mut EmitContext as *mut c_void,
                            err.as_mut_ptr(),
                            err.len(),
                        )
                    };
                    if rc != 0 {
                        let msg = ctx.error.take().unwrap_or_else(|| {
                            unsafe { std::ffi::CStr::from_ptr(err.as_ptr()) }.to_string_lossy().into_owned()
                        });
                        let e = DataFusionError::External(Box::new(PgError::internal(msg)));
                        return Some((Err(e), (scan, queue, true)));
                    }
                    queue.extend(ctx.batches);
                    // Let other tasks run between blocks.
                    tokio::task::yield_now().await;
                }
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Where a query's rows come from.
pub enum Source {
    /// Batches the main thread pushes (any table AM).
    Pushed,
    /// Arrow IPC streams the main thread pushes as it receives them from a
    /// Motion, one stream per sending route (`push_ipc`).
    Ipc,
    /// PAX micro-partitions read by the partitions themselves.
    Pax(PaxScan),
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

/// The single-input spec form (one "scan", then "filter", "aggregate",
/// "having") as the tree form.
fn normalize_spec(mut spec: Value) -> Result<Value, String> {
    if spec.get("plan").is_some() {
        return Ok(spec);
    }
    let obj = spec.as_object_mut().ok_or("plan spec: not an object")?;
    let scan = obj.remove("scan").ok_or("plan spec: no scan")?;
    let mut node = serde_json::json!({"input": 0});
    if let Some(f) = obj.remove("filter").filter(|v| !v.is_null()) {
        node = serde_json::json!({"filter": {"input": node, "pred": f}});
    }
    if let Some(mut a) = obj.remove("aggregate").filter(|v| !v.is_null()) {
        let having = obj.remove("having").unwrap_or(Value::Null);
        let am = a.as_object_mut().ok_or("plan spec: bad aggregate")?;
        am.insert("input".into(), node);
        am.insert("having".into(), having);
        node = serde_json::json!({ "aggregate": a });
    } else {
        obj.remove("having");
    }
    obj.insert("inputs".into(), Value::Array(vec![scan]));
    obj.insert("plan".into(), node);
    Ok(spec)
}

/// Does the plan group rows in an aggregate (which caps the partitions)?
fn has_grouped_aggregate(node: &Value) -> bool {
    if let Some(a) = node.get("aggregate") {
        let grouped = a.get("group").and_then(Value::as_array).map_or(false, |g| !g.is_empty());
        return grouped || a.get("input").map_or(false, has_grouped_aggregate);
    }
    if let Some(f) = node.get("filter") {
        return f.get("input").map_or(false, has_grouped_aggregate);
    }
    if let Some(j) = node.get("join") {
        return j.get("left").map_or(false, has_grouped_aggregate)
            || j.get("right").map_or(false, has_grouped_aggregate);
    }
    false
}

/// The logical plan of a node of the spec.  `tables` holds each input's
/// table, taken by the node that reads it.
fn build_node(node: &Value, tables: &mut Vec<Option<StreamingTable>>) -> Result<LogicalPlanBuilder, String> {
    let df = |e: DataFusionError| e.to_string();
    if let Some(j) = node.get("input") {
        let j = index(j)?;
        let table = tables
            .get_mut(j)
            .and_then(Option::take)
            .ok_or_else(|| format!("plan spec: input {j} missing or read twice"))?;
        return LogicalPlanBuilder::scan(format!("t{j}"), provider_as_source(Arc::new(table)), None).map_err(df);
    }
    if let Some(f) = node.get("filter") {
        let b = build_node(field(f, "input")?, tables)?;
        return b.filter(expr(field(f, "pred")?)?).map_err(df);
    }
    if let Some(a) = node.get("aggregate") {
        let mut b = build_node(field(a, "input")?, tables)?;
        let groups = field(a, "group")?.as_array().cloned().unwrap_or_default();
        let aggs = field(a, "aggs")?.as_array().cloned().unwrap_or_default();
        let group_exprs = groups
            .iter()
            .enumerate()
            .map(|(i, g)| expr(g).map(|e| e.alias(format!("g{i}"))))
            .collect::<Result<Vec<_>, _>>()?;
        let mut agg_exprs = Vec::with_capacity(aggs.len());
        for (i, g) in aggs.iter().enumerate() {
            agg_exprs.push(aggregate(g)?.alias(format!("a{i}")));
            if let Some(e) = aggregate_extra(g)? {
                agg_exprs.push(e.alias(format!("a{i}_n")));
            }
        }
        let ngroups = group_exprs.len();
        b = b.aggregate(group_exprs, agg_exprs).map_err(df)?;
        // A combined count is 0, not NULL, when no partial count arrived;
        // a combined avg divides the sums, NULL without values (as
        // PostgreSQL's float8_avg).
        let kind = |a: &Value| a.get("fn").and_then(Value::as_str).map(str::to_owned);
        if aggs.iter().any(|a| matches!(kind(a).as_deref(), Some("count_merge" | "avg_merge"))) {
            let mut cols: Vec<Expr> = (0..ngroups).map(|i| col(format!("g{i}"))).collect();
            for (i, a) in aggs.iter().enumerate() {
                let c = col(format!("a{i}"));
                cols.push(match kind(a).as_deref() {
                    Some("count_merge") => {
                        when(c.clone().is_null(), lit(0i64)).otherwise(c).map_err(df)?.alias(format!("a{i}"))
                    }
                    Some("avg_merge") => {
                        let n = col(format!("a{i}_n"));
                        let nf = Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(n.clone()), DataType::Float64));
                        when(n.clone().is_null().or(n.eq(lit(0i64))), lit(ScalarValue::Float64(None)))
                            .otherwise(binary_expr(c, Operator::Divide, nf))
                            .map_err(df)?
                            .alias(format!("a{i}"))
                    }
                    _ => c,
                });
            }
            b = b.project(cols).map_err(df)?;
        }
        if let Some(h) = a.get("having").filter(|v| !v.is_null()) {
            b = b.filter(expr(h)?).map_err(df)?;
        }
        return Ok(b);
    }
    if let Some(j) = node.get("join") {
        use datafusion::logical_expr::JoinType;
        let join_type = match field(j, "type")?.as_str().unwrap_or("") {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" => JoinType::Full,
            "leftsemi" => JoinType::LeftSemi,
            "rightsemi" => JoinType::RightSemi,
            "leftanti" => JoinType::LeftAnti,
            "rightanti" => JoinType::RightAnti,
            other => return Err(format!("plan spec: unsupported join type {other}")),
        };
        let left = build_node(field(j, "left")?, tables)?;
        let right = build_node(field(j, "right")?, tables)?.build().map_err(df)?;
        let mut lkeys = Vec::new();
        let mut rkeys = Vec::new();
        for pair in field(j, "on")?.as_array().ok_or("plan spec: bad join keys")? {
            let pair = pair.as_array().filter(|p| p.len() == 2).ok_or("plan spec: bad join key")?;
            lkeys.push(expr(&pair[0])?);
            rkeys.push(expr(&pair[1])?);
        }
        let filter = match j.get("filter").filter(|v| !v.is_null()) {
            Some(f) => Some(expr(f)?),
            None => None,
        };
        return left
            .join_with_expr_keys(right, join_type, (lkeys, rkeys), filter)
            .map_err(df);
    }
    Err(format!("plan spec: unknown node {node}"))
}

/// What the plan's task hands the main thread.
enum Out {
    Batch(RecordBatch),
    /// Results encoded as Arrow IPC stream bytes (`ipc_output`), for a
    /// route, or NO_ROUTE for the only stream.
    Bytes(i32, Vec<u8>),
}

/// Out::Bytes of the single stream of a Motion without routing.
pub const NO_ROUTE: i32 = -1;

/// How result rows are spread over a Redistribute Motion's receivers.
struct HashRoute {
    keys: Vec<(usize, KeyHash)>,
    segments: i32,
    workers: i32,
}

impl HashRoute {
    fn parse(v: &Value) -> Result<HashRoute, String> {
        if field(v, "kind")?.as_str() != Some("hash") {
            return Err(format!("plan spec: unknown route {v}"));
        }
        let keys = field(v, "keys")?
            .as_array()
            .ok_or("plan spec: bad route keys")?
            .iter()
            .map(|k| {
                let c = index(field(k, "col")?)?;
                let h = field(k, "hash")?.as_str().and_then(KeyHash::parse).ok_or("plan spec: bad key hash")?;
                Ok((c, h))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let int = |name: &str| field(v, name).ok().and_then(Value::as_i64).map(|n| n as i32);
        let segments = int("segments").filter(|n| *n > 0).ok_or("plan spec: bad route segments")?;
        let workers = int("workers").unwrap_or(1).max(1);
        if keys.is_empty() {
            return Err("plan spec: route without keys".into());
        }
        Ok(HashRoute { keys, segments, workers })
    }
}

pub struct Query {
    handle: Handle,
    /// Per input: Source::Pushed batches.
    in_tx: Vec<Option<mpsc::Sender<Result<RecordBatch, DataFusionError>>>>,
    /// Per input: Source::Ipc stream bytes, by route, for its decoding task.
    ipc_tx: Vec<Option<mpsc::Sender<(i32, Vec<u8>)>>>,
    decoders: Vec<JoinHandle<()>>,
    in_schemas: Vec<SchemaRef>,
    in_types: Vec<Vec<PgType>>,
    out_rx: mpsc::Receiver<Result<Out, PgError>>,
    out_types: Vec<PgType>,
    task: Option<JoinHandle<()>>,
    current: Vec<OutColumn>,
    current_bytes: Vec<u8>,
    current_route: i32,
    pool: Arc<TrackingPool>,
    physical: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
    partitions: usize,
    pax_memory: Option<Arc<PaxMemory>>,
}

/// Memory and spill figures of a query, for EXPLAIN ANALYZE.
#[derive(Debug, Default, Clone, Copy)]
pub struct QueryStats {
    pub partitions: u64,
    pub memory_limit: u64,
    pub memory_peak: u64,
    pub spilled_bytes: u64,
    pub spill_count: u64,
    /// Peak bytes the PAX reader reported holding (direct PAX scans).
    pub pax_decode_peak: u64,
}

fn add_spills(plan: &Arc<dyn ExecutionPlan>, stats: &mut QueryStats) {
    if let Some(m) = plan.metrics() {
        stats.spilled_bytes += m.spilled_bytes().unwrap_or(0) as u64;
        stats.spill_count += m.spill_count().unwrap_or(0) as u64;
    }
    for child in plan.children() {
        add_spills(child, stats);
    }
}

impl Query {
    /// Build the plan described by `spec` and start running it with
    /// `partitions` partitions, an operator memory budget of `memory_limit`
    /// bytes, and spill files under `spill_dir`.
    pub fn start(spec: &str, partitions: usize, memory_limit: usize, spill_dir: &str) -> Result<Query, PgError> {
        Self::start_with(spec, partitions, memory_limit, spill_dir, Source::Pushed, false)
    }

    /// Like `start`, reading from `source`.  With `ipc_output`, results come
    /// back as one Arrow IPC stream (Poll::Bytes) instead of batches; the
    /// stream always holds the schema and the end marker, even without rows.
    pub fn start_with(
        spec: &str,
        partitions: usize,
        memory_limit: usize,
        spill_dir: &str,
        source: Source,
        ipc_output: bool,
    ) -> Result<Query, PgError> {
        Self::start_multi(spec, partitions, memory_limit, spill_dir, vec![source], ipc_output)
    }

    /// Like `start_with`, with one source per input of the plan.
    pub fn start_multi(
        spec: &str,
        partitions: usize,
        memory_limit: usize,
        spill_dir: &str,
        sources: Vec<Source>,
        ipc_output: bool,
    ) -> Result<Query, PgError> {
        let handle = runtime::handle()
            .ok_or_else(|| PgError::internal("the DataFusion runtime is not running"))?;
        let spec: Value = serde_json::from_str(spec)
            .map_err(|e| PgError::internal(format!("bad plan spec: {e}")))?;
        let internal = |e: String| PgError::internal(format!("cannot build DataFusion plan: {e}"));
        let df = |e: DataFusionError| internal(e.to_string());

        let spec = normalize_spec(spec).map_err(internal)?;
        let inputs = field(&spec, "inputs").map_err(internal)?.as_array().cloned().unwrap_or_default();
        if inputs.len() != sources.len() || inputs.is_empty() {
            return Err(internal(format!("{} inputs, {} sources", inputs.len(), sources.len())));
        }
        let mut in_types: Vec<Vec<PgType>> = Vec::with_capacity(inputs.len());
        let mut in_schemas: Vec<SchemaRef> = Vec::with_capacity(inputs.len());
        for (j, inp) in inputs.iter().enumerate() {
            let types: Vec<PgType> = field(inp, "columns")
                .map_err(internal)?
                .as_array()
                .ok_or_else(|| internal("bad input columns".into()))?
                .iter()
                .map(|c| PgType::parse(c.get("type").and_then(Value::as_str).unwrap_or("")))
                .collect::<Result<_, _>>()
                .map_err(internal)?;
            in_schemas.push(Arc::new(Schema::new(
                types
                    .iter()
                    .enumerate()
                    .map(|(i, t)| Field::new(input_column(j, i), t.arrow(), true))
                    .collect::<Vec<_>>(),
            )));
            in_types.push(types);
        }

        let plan_node = field(&spec, "plan").map_err(internal)?.clone();
        let partitions = if has_grouped_aggregate(&plan_node) {
            partitions.max(1).min((memory_limit / MIN_PARTITION_MEMORY).max(1))
        } else {
            partitions.max(1)
        };
        let pax_memory = sources.iter().find_map(|s| match s {
            Source::Pax(scan) => Some(scan.memory.clone()),
            _ => None,
        });
        let mut in_tx = Vec::with_capacity(sources.len());
        let mut ipc_tx = Vec::with_capacity(sources.len());
        let mut decoders = Vec::new();
        let mut tables = Vec::with_capacity(sources.len());
        for (j, source) in sources.into_iter().enumerate() {
            let schema = in_schemas[j].clone();
            let (tx, itx, table) = match source {
                Source::Ipc => {
                    let positions: Vec<usize> = field(&inputs[j], "motion_columns")
                        .map_err(internal)?
                        .as_array()
                        .ok_or_else(|| internal("bad motion columns".into()))?
                        .iter()
                        .map(|v| v.as_u64().map(|k| k as usize).ok_or_else(|| internal("bad motion column".into())))
                        .collect::<Result<_, _>>()?;
                    let (in_tx, in_rx) = mpsc::channel(CHANNEL_DEPTH);
                    let (tx, rx) = mpsc::channel(IPC_CHANNEL_DEPTH);
                    decoders.push(handle.spawn(decode_ipc(rx, in_tx, schema.clone(), positions)));
                    let partition = ChannelPartition { schema: schema.clone(), rx: Mutex::new(Some(in_rx)) };
                    (None, Some(tx), StreamingTable::try_new(schema.clone(), vec![Arc::new(partition)]).map_err(df)?)
                }
                Source::Pushed => {
                    let (in_tx, in_rx) = mpsc::channel(CHANNEL_DEPTH);
                    let partition = ChannelPartition { schema: schema.clone(), rx: Mutex::new(Some(in_rx)) };
                    (Some(in_tx), None, StreamingTable::try_new(schema.clone(), vec![Arc::new(partition)]).map_err(df)?)
                }
                Source::Pax(scan) => {
                    let scan = Arc::new(scan);
                    let parts: Vec<Arc<dyn PartitionStream>> = (0..partitions)
                        .map(|_| {
                            Arc::new(PaxPartition { schema: schema.clone(), types: in_types[j].clone(), scan: scan.clone() })
                                as Arc<dyn PartitionStream>
                        })
                        .collect();
                    (None, None, StreamingTable::try_new(schema.clone(), parts).map_err(df)?)
                }
            };
            in_tx.push(tx);
            ipc_tx.push(itx);
            tables.push(Some(table));
        }

        let b = build_node(&plan_node, &mut tables).map_err(internal)?;
        let outputs = field(&spec, "output").map_err(internal)?.as_array().cloned().unwrap_or_default();
        let mut out_types = Vec::with_capacity(outputs.len());
        let mut out_exprs = Vec::with_capacity(outputs.len());
        for (i, o) in outputs.iter().enumerate() {
            let ty = PgType::parse(o.get("type").and_then(Value::as_str).unwrap_or("")).map_err(internal)?;
            let e = expr(field(o, "expr").map_err(internal)?).map_err(internal)?;
            out_exprs.push(
                Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(e), ty.arrow()))
                    .alias(format!("o{i}")),
            );
            out_types.push(ty);
        }
        let plan = b.project(out_exprs).map_err(df)?.build().map_err(df)?;
        let route = match spec.get("route").filter(|v| !v.is_null()) {
            Some(r) if ipc_output => Some(HashRoute::parse(r).map_err(internal)?),
            Some(_) => return Err(internal("a route needs IPC output".into())),
            None => None,
        };
        if let Some(r) = &route {
            if r.keys.iter().any(|(c, h)| out_types.get(*c).map(|t| t.key_hash()) != Some(*h)) {
                return Err(internal("route key does not match its output column".into()));
            }
        }

        // Rows per batch, from the widest row the planner expects (the C
        // side pushes batches of the same size).
        let batch_rows = spec
            .get("batch_rows")
            .and_then(Value::as_u64)
            .map(|n| (n as usize).clamp(1, BATCH_ROWS))
            .unwrap_or(BATCH_ROWS);
        let config = SessionConfig::new()
            .with_target_partitions(partitions)
            .with_batch_size(batch_rows);
        let pool = Arc::new(TrackingPool::new(memory_limit.max(1)));
        let ctx = SessionContext::new_with_state(session_state(config, pool.clone(), spill_dir).map_err(df)?);
        let (out_tx, out_rx) = mpsc::channel(CHANNEL_DEPTH);
        let physical_slot: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>> = Arc::new(Mutex::new(None));
        let slot = physical_slot.clone();

        let task = handle.spawn(async move {
            let run = async {
                let state = ctx.state();
                let physical = state.create_physical_plan(&plan).await?;
                if let Ok(mut s) = slot.lock() {
                    *s = Some(physical.clone());
                }
                let schema = physical.schema();
                let mut stream = execute_stream(physical, ctx.task_ctx())?;
                if let Some(route) = route {
                    // One stream per receiving route, opened on its first
                    // rows; a route without rows gets end-of-stream only.
                    let mut writers: HashMap<u32, StreamWriter<Vec<u8>>> = HashMap::new();
                    while let Some(batch) = stream.next().await {
                        let batch = batch?;
                        let n = batch.num_rows();
                        if n == 0 {
                            continue;
                        }
                        let keys: Vec<(KeyHash, ArrayRef)> =
                            route.keys.iter().map(|(c, h)| (*h, batch.column(*c).clone())).collect();
                        let routes = cdbhash::routes(&keys, n, route.segments, route.workers);
                        let mut rows: std::collections::BTreeMap<u32, Vec<u32>> = Default::default();
                        for (i, r) in routes.iter().enumerate() {
                            rows.entry(*r).or_default().push(i as u32);
                        }
                        for (r, idx) in rows {
                            let idx = UInt32Array::from(idx);
                            let cols = batch
                                .columns()
                                .iter()
                                .map(|c| take(c.as_ref(), &idx, None))
                                .collect::<Result<Vec<_>, _>>()?;
                            let part = RecordBatch::try_new(schema.clone(), cols)?;
                            if !writers.contains_key(&r) {
                                writers.insert(r, StreamWriter::try_new(Vec::new(), &schema)?);
                            }
                            let w = writers.get_mut(&r).expect("writer just inserted");
                            w.write(&part)?;
                            let bytes = std::mem::take(w.get_mut());
                            if out_tx.send(Ok(Out::Bytes(r as i32, bytes))).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    for (r, mut w) in writers {
                        w.finish()?;
                        let bytes = std::mem::take(w.get_mut());
                        if out_tx.send(Ok(Out::Bytes(r as i32, bytes))).await.is_err() {
                            return Ok(());
                        }
                    }
                    return Ok(());
                }
                if ipc_output {
                    // Encode here, on a worker; the main thread only copies
                    // the bytes into the interconnect.
                    let mut w = StreamWriter::try_new(Vec::new(), &schema)?;
                    while let Some(batch) = stream.next().await {
                        let batch = batch?;
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        w.write(&batch)?;
                        let bytes = std::mem::take(w.get_mut());
                        if out_tx.send(Ok(Out::Bytes(NO_ROUTE, bytes))).await.is_err() {
                            return Ok(());
                        }
                    }
                    w.finish()?;
                    let bytes = std::mem::take(w.get_mut());
                    let _ = out_tx.send(Ok(Out::Bytes(NO_ROUTE, bytes))).await;
                    return Ok(());
                }
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() > 0 && out_tx.send(Ok(Out::Batch(batch))).await.is_err() {
                        return Ok(()); // the main thread is gone
                    }
                }
                Ok::<(), DataFusionError>(())
            };
            if let Err(e) = run.await {
                let _ = out_tx.send(Err(to_pg_error(&e))).await;
            }
        });

        Ok(Query {
            handle,
            in_tx,
            ipc_tx,
            decoders,
            in_schemas,
            in_types,
            out_rx,
            out_types,
            task: Some(task),
            current: Vec::new(),
            current_bytes: Vec::new(),
            current_route: NO_ROUTE,
            pool,
            physical: physical_slot,
            partitions,
            pax_memory,
        })
    }

    /// Memory and spill figures so far.
    pub fn stats(&self) -> QueryStats {
        let mut s = QueryStats {
            partitions: self.partitions as u64,
            memory_limit: self.pool.limit() as u64,
            memory_peak: self.pool.peak() as u64,
            pax_decode_peak: self.pax_memory.as_ref().map_or(0, |m| m.peak()),
            ..Default::default()
        };
        if let Ok(slot) = self.physical.lock() {
            if let Some(p) = slot.as_ref() {
                add_spills(p, &mut s);
            }
        }
        s
    }

    pub fn num_inputs(&self) -> usize {
        self.in_types.len()
    }

    pub fn num_output_columns(&self) -> usize {
        self.out_types.len()
    }

    /// Offer one batch.  Ok(false): the channel is full; nothing was taken.
    ///
    /// # Safety
    /// Each column must point to `nrows` values of its type and `nrows`
    /// null bytes.
    pub unsafe fn push(&mut self, cols: &[RawColumn], nrows: usize) -> Result<bool, PgError> {
        self.push_input(0, cols, nrows)
    }

    /// Like `push`, to input `input`.
    ///
    /// # Safety
    /// As for `push`.
    pub unsafe fn push_input(&mut self, input: usize, cols: &[RawColumn], nrows: usize) -> Result<bool, PgError> {
        let tx = self
            .in_tx
            .get(input)
            .and_then(Option::as_ref)
            .ok_or_else(|| PgError::internal("input not open"))?;
        if tx.capacity() == 0 {
            return Ok(false);
        }
        let types = &self.in_types[input];
        if cols.len() != types.len() {
            return Err(PgError::internal("wrong number of input columns"));
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
        for (c, ty) in cols.iter().zip(types) {
            arrays.push(build_array(*ty, *c, nrows)?);
        }
        let batch = make_batch(&self.in_schemas[input], arrays, nrows)?;
        match tx.try_send(Ok(batch)) {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            // The plan stopped reading (finished early or failed); poll tells.
            Err(mpsc::error::TrySendError::Closed(_)) => Ok(true),
        }
    }

    /// Offer Arrow IPC stream bytes received from `route`, given as the
    /// consecutive pieces `parts` (Source::Ipc).  A full queue is waited on
    /// for up to IPC_PUSH_WAIT, waking as soon as the decoder makes room:
    /// the receiver has nothing else to do meanwhile, and waiting for
    /// output instead would sleep the whole time (an aggregate's results
    /// come only at the end).  Ok(false): still full; nothing was taken.
    pub fn push_ipc(&mut self, route: i32, parts: &[&[u8]]) -> Result<bool, PgError> {
        self.push_ipc_input(0, route, parts)
    }

    /// Like `push_ipc`, to input `input`.
    pub fn push_ipc_input(&mut self, input: usize, route: i32, parts: &[&[u8]]) -> Result<bool, PgError> {
        let tx = self
            .ipc_tx
            .get(input)
            .and_then(Option::as_ref)
            .ok_or_else(|| PgError::internal("not reading Arrow IPC input"))?;
        let permit = match tx.try_reserve() {
            Ok(p) => p,
            Err(mpsc::error::TrySendError::Full(())) => {
                match self.handle.block_on(async { tokio::time::timeout(IPC_PUSH_WAIT, tx.reserve()).await }) {
                    Ok(Ok(p)) => p,
                    Ok(Err(_)) => return Ok(true), // the decoder stopped; poll tells why
                    Err(_) => return Ok(false),
                }
            }
            // The decoder stopped (the plan finished early or failed).
            Err(mpsc::error::TrySendError::Closed(())) => return Ok(true),
        };
        let mut bytes = Vec::with_capacity(parts.iter().map(|p| p.len()).sum());
        for p in parts {
            bytes.extend_from_slice(p);
        }
        permit.send((route, bytes));
        Ok(true)
    }

    /// No more input, on any input.
    pub fn finish_input(&mut self) {
        for t in self.in_tx.iter_mut() {
            *t = None;
        }
        for t in self.ipc_tx.iter_mut() {
            *t = None;
        }
    }

    /// No more rows for input `input`.
    pub fn finish_input_at(&mut self, input: usize) {
        if let Some(t) = self.in_tx.get_mut(input) {
            *t = None;
        }
        if let Some(t) = self.ipc_tx.get_mut(input) {
            *t = None;
        }
    }

    /// The bytes returned by the last Poll::Bytes.
    pub fn output_bytes(&self) -> &[u8] {
        &self.current_bytes
    }

    /// The route of those bytes, or NO_ROUTE.
    pub fn output_route(&self) -> i32 {
        self.current_route
    }

    /// Wait up to `timeout` for the next result batch.
    pub fn poll(&mut self, timeout: Duration) -> Poll {
        self.current.clear();
        self.current_bytes = Vec::new();
        let next = if timeout.is_zero() {
            match self.out_rx.try_recv() {
                Ok(v) => Some(v),
                Err(mpsc::error::TryRecvError::Empty) => return Poll::Pending,
                Err(mpsc::error::TryRecvError::Disconnected) => None,
            }
        } else {
            let rx = &mut self.out_rx;
            match self.handle.block_on(async { tokio::time::timeout(timeout, rx.recv()).await }) {
                Ok(v) => v,
                Err(_) => return Poll::Pending,
            }
        };
        match next {
            Some(Ok(Out::Batch(batch))) => match self.convert(&batch) {
                Ok(()) => Poll::Batch(batch.num_rows()),
                Err(e) => Poll::Failed(e),
            },
            Some(Ok(Out::Bytes(route, bytes))) => {
                self.current_route = route;
                self.current_bytes = bytes;
                Poll::Bytes(self.current_bytes.len())
            }
            Some(Err(e)) => Poll::Failed(e),
            None => {
                // The task ended.  Make sure it did not die in a panic.
                match self.task.take().map(|t| self.handle.block_on(t)) {
                    Some(Err(e)) if e.is_panic() => {
                        let p = e.into_panic();
                        let msg = p
                            .downcast_ref::<&'static str>()
                            .map(|s| s.to_string())
                            .or_else(|| p.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "panic with a non-string payload".into());
                        Poll::Panicked(msg)
                    }
                    Some(Err(e)) => Poll::Failed(PgError::internal(e.to_string())),
                    _ => Poll::Done,
                }
            }
        }
    }

    fn convert(&mut self, batch: &RecordBatch) -> Result<(), PgError> {
        if batch.num_columns() != self.out_types.len() {
            return Err(PgError::internal("result batch has the wrong number of columns"));
        }
        for (i, ty) in self.out_types.iter().enumerate() {
            let mut array = batch.column(i).clone();
            if array.data_type() != &ty.arrow() {
                array = cast(&array, &ty.arrow()).map_err(|e| PgError::internal(e.to_string()))?;
            }
            let n = array.len();
            let nulls: Vec<u8> = (0..n).map(|r| array.is_null(r) as u8).collect();
            let mut offsets: *const i32 = std::ptr::null();
            let (bools, values) = match ty.storage() {
                PgType::Bool => {
                    let b: Vec<u8> = array.as_boolean().values().iter().map(|v| v as u8).collect();
                    let p = b.as_ptr();
                    (Some(b), p)
                }
                PgType::Int2 => (None, array.as_primitive::<Int16Type>().values().as_ptr() as *const u8),
                PgType::Int4 => (None, array.as_primitive::<Int32Type>().values().as_ptr() as *const u8),
                PgType::Int8 => (None, array.as_primitive::<Int64Type>().values().as_ptr() as *const u8),
                PgType::Float4 => (None, array.as_primitive::<Float32Type>().values().as_ptr() as *const u8),
                PgType::Float8 => (None, array.as_primitive::<Float64Type>().values().as_ptr() as *const u8),
                PgType::Text => {
                    let a = array.as_string::<i32>();
                    offsets = a.value_offsets().as_ptr();
                    (None, a.values().as_ptr())
                }
                _ => unreachable!(),
            };
            self.current.push(OutColumn { _array: array, offsets, bools, nulls, values });
        }
        Ok(())
    }

    /// Buffers of output column `i` of the current batch.
    pub fn output_column(&self, i: usize) -> Option<RawColumn> {
        self.current.get(i).map(|c| RawColumn {
            values: c.bools.as_ref().map(|b| b.as_ptr()).unwrap_or(c.values),
            nulls: c.nulls.as_ptr(),
            offsets: c.offsets,
        })
    }
}

impl Drop for Query {
    fn drop(&mut self) {
        self.finish_input();
        for t in self.decoders.drain(..) {
            t.abort();
        }
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

/// Decode the Arrow IPC streams arriving from a Motion, one per route, and
/// feed their rows to the plan: input column i is the stream's column
/// `positions[i]`.  A malformed stream fails the plan.
async fn decode_ipc(
    mut rx: mpsc::Receiver<(i32, Vec<u8>)>,
    tx: mpsc::Sender<Result<RecordBatch, DataFusionError>>,
    schema: SchemaRef,
    positions: Vec<usize>,
) {
    let mut decoders: HashMap<i32, StreamDecoder> = HashMap::new();
    let fail = |e: String| Err(DataFusionError::External(Box::new(PgError::internal(format!(
        "cannot decode the batches received from a Motion: {e}"
    )))));
    while let Some((route, bytes)) = rx.recv().await {
        let decoder = decoders.entry(route).or_insert_with(StreamDecoder::new);
        let mut buffer = Buffer::from_vec(bytes);
        while !buffer.is_empty() {
            let batch = match decoder.decode(&mut buffer) {
                Ok(Some(b)) => b,
                Ok(None) => continue,
                Err(e) => {
                    let _ = tx.send(fail(e.to_string())).await;
                    return;
                }
            };
            let n = batch.num_rows();
            let mut arrays = Vec::with_capacity(positions.len());
            for (i, &k) in positions.iter().enumerate() {
                match batch.columns().get(k) {
                    Some(a) if a.data_type() == schema.field(i).data_type() => arrays.push(a.clone()),
                    _ => {
                        let _ = tx.send(fail(format!("column {k} is missing or has another type"))).await;
                        return;
                    }
                }
            }
            let item = make_batch(&schema, arrays, n).map_err(|e| DataFusionError::External(Box::new(e)));
            if tx.send(item).await.is_err() {
                return; // the plan stopped reading
            }
        }
    }
    // Every route ended: each stream must be complete.
    for (_, mut d) in decoders {
        if let Err(e) = d.finish() {
            let _ = tx.send(fail(e.to_string())).await;
            return;
        }
    }
}

unsafe fn primitive<T: ArrowPrimitiveType>(c: RawColumn, n: usize, nulls: Option<NullBuffer>) -> ArrayRef {
    let values = std::slice::from_raw_parts(c.values as *const T::Native, n).to_vec();
    Arc::new(PrimitiveArray::<T>::new(ScalarBuffer::from(values), nulls))
}

/// An Arrow array of `n` values of `ty` from C buffers.  Strings are
/// checked to be UTF-8 (PostgreSQL has checked them on input; Arrow's
/// string kernels rely on it).
///
/// # Safety
/// `c` must point to `n` values of `ty` (for a string, `n + 1` offsets and
/// the bytes they cover) and `n` null bytes.
pub unsafe fn build_array(ty: PgType, c: RawColumn, n: usize) -> Result<ArrayRef, PgError> {
    let null_bytes = std::slice::from_raw_parts(c.nulls, n);
    let nulls = if null_bytes.iter().any(|b| *b != 0) {
        Some(NullBuffer::from(null_bytes.iter().map(|b| *b == 0).collect::<Vec<bool>>()))
    } else {
        None
    };
    Ok(match ty.storage() {
        PgType::Bool => {
            let v = std::slice::from_raw_parts(c.values, n);
            Arc::new(BooleanArray::new(BooleanBuffer::from_iter(v.iter().map(|b| *b != 0)), nulls))
        }
        PgType::Text => {
            let offsets = std::slice::from_raw_parts(c.offsets, n + 1);
            let bytes = std::slice::from_raw_parts(c.values, offsets[n] as usize);
            let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets.to_vec()));
            Arc::new(
                StringArray::try_new(offsets, Buffer::from_slice_ref(bytes), nulls)
                    .map_err(|e| PgError::new("22021", format!("invalid byte sequence for encoding \"UTF8\": {e}")))?,
            )
        }
        PgType::Int2 => primitive::<Int16Type>(c, n, nulls),
        PgType::Int4 => primitive::<Int32Type>(c, n, nulls),
        PgType::Int8 => primitive::<Int64Type>(c, n, nulls),
        PgType::Float4 => primitive::<Float32Type>(c, n, nulls),
        PgType::Float8 => primitive::<Float64Type>(c, n, nulls),
        _ => unreachable!(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(spec: &str, input: Vec<(Vec<i32>, Vec<u8>)>, nrows: usize) -> Result<Vec<Vec<Option<i64>>>, PgError> {
        run_with(spec, input, nrows, 64 << 20)
    }

    fn run_with(spec: &str, input: Vec<(Vec<i32>, Vec<u8>)>, nrows: usize, limit: usize) -> Result<Vec<Vec<Option<i64>>>, PgError> {
        runtime::init(2).unwrap();
        let dir = std::env::temp_dir();
        let mut q = Query::start(spec, 2, limit, dir.to_str().unwrap())?;
        let cols: Vec<RawColumn> = input
            .iter()
            .map(|(v, n)| RawColumn::fixed(v.as_ptr() as *const u8, n.as_ptr()))
            .collect();
        loop {
            if unsafe { q.push(&cols, nrows) }? {
                break;
            }
        }
        q.finish_input();
        let mut rows = Vec::new();
        loop {
            match q.poll(Duration::from_millis(50)) {
                Poll::Batch(n) => {
                    let ncol = q.num_output_columns();
                    for r in 0..n {
                        let mut row = Vec::new();
                        for c in 0..ncol {
                            let col = q.output_column(c).unwrap();
                            let null = unsafe { *col.nulls.add(r) } != 0;
                            row.push(if null { None } else { Some(unsafe { *(col.values as *const i64).add(r) }) });
                        }
                        rows.push(row);
                    }
                }
                Poll::Pending => {}
                Poll::Bytes(_) => panic!("unexpected IPC output"),
                Poll::Done => return Ok(rows),
                Poll::Failed(e) => return Err(e),
                Poll::Panicked(m) => panic!("{m}"),
            }
        }
    }

    #[test]
    fn filter_and_sum() {
        let spec = r#"{"scan":{"columns":[{"type":"int4"}]},
            "filter":{"op":">","type":"bool","args":[{"col":0},{"lit":{"type":"int4","value":1}}]},
            "aggregate":{"group":[],"aggs":[{"fn":"sum","arg":{"col":0}},{"fn":"count"}]},
            "having":null,
            "output":[{"expr":{"agg":0},"type":"int8"},{"expr":{"agg":1},"type":"int8"}]}"#;
        let rows = run(spec, vec![(vec![1, 2, 3, 0], vec![0, 0, 0, 1])], 4).unwrap();
        assert_eq!(rows, vec![vec![Some(5), Some(2)]]);
    }

    #[test]
    fn dates_compare_as_stored() {
        // Days from 2000-01-01; i32::MIN and i32::MAX are -infinity and
        // infinity, which compare below and above every date.
        let spec = r#"{"scan":{"columns":[{"type":"date"}]},
            "filter":{"op":"<","type":"bool","args":[{"col":0},{"lit":{"type":"date","value":%D%}}]},
            "aggregate":{"group":[],"aggs":[{"fn":"count","arg":{"col":0}}]},
            "having":null,
            "output":[{"expr":{"agg":0},"type":"int8"}]}"#;
        let input = || vec![(vec![i32::MIN, -730, 0, 9000, i32::MAX, 5], vec![0, 0, 0, 0, 0, 1])];
        let count = |d: i32| run(&spec.replace("%D%", &d.to_string()), input(), 6).unwrap();
        assert_eq!(count(0), vec![vec![Some(2)]]);
        assert_eq!(count(i32::MAX), vec![vec![Some(4)]]);
        assert_eq!(count(i32::MIN), vec![vec![Some(0)]]);
    }

    /// Push one text column (`None` = NULL) and return the output text column.
    fn run_text(spec: &str, values: &[Option<&str>]) -> Result<Vec<Option<String>>, PgError> {
        runtime::init(2).unwrap();
        let mut bytes = Vec::new();
        let mut offsets = vec![0i32];
        let mut nulls = Vec::new();
        for v in values {
            bytes.extend_from_slice(v.unwrap_or("").as_bytes());
            offsets.push(bytes.len() as i32);
            nulls.push(v.is_none() as u8);
        }
        let dir = std::env::temp_dir();
        let mut q = Query::start(spec, 2, 64 << 20, dir.to_str().unwrap())?;
        let col = RawColumn { values: bytes.as_ptr(), nulls: nulls.as_ptr(), offsets: offsets.as_ptr() };
        while !unsafe { q.push(&[col], values.len()) }? {}
        q.finish_input();
        let mut out = Vec::new();
        loop {
            match q.poll(Duration::from_millis(50)) {
                Poll::Batch(n) => {
                    let c = q.output_column(0).unwrap();
                    for r in 0..n {
                        if unsafe { *c.nulls.add(r) } != 0 {
                            out.push(None);
                            continue;
                        }
                        let (a, b) = unsafe { (*c.offsets.add(r) as usize, *c.offsets.add(r + 1) as usize) };
                        let s = unsafe { std::slice::from_raw_parts(c.values.add(a), b - a) };
                        out.push(Some(String::from_utf8(s.to_vec()).unwrap()));
                    }
                }
                Poll::Pending => {}
                Poll::Bytes(_) => panic!("unexpected IPC output"),
                Poll::Done => return Ok(out),
                Poll::Failed(e) => return Err(e),
                Poll::Panicked(m) => panic!("{m}"),
            }
        }
    }

    #[test]
    fn text_compares_bytes() {
        let spec = r#"{"scan":{"columns":[{"type":"text"}]},
            "filter":{"op":"<>","type":"bool","args":[{"col":0},{"lit":{"type":"text","value":"b\"\\"}}]},
            "aggregate":null,"having":null,
            "output":[{"expr":{"col":0},"type":"text"}]}"#;
        let input = [Some("a"), Some("b\"\\"), Some(""), None, Some("中文"), Some("b")];
        let mut got = run_text(spec, &input).unwrap();
        got.sort();
        let want: Vec<Option<String>> = vec![Some(""), Some("a"), Some("b"), Some("中文")]
            .into_iter().map(|s| s.map(String::from)).collect();
        assert_eq!(got, want);

        // min/max order bytes, as the C collation does.
        let spec = r#"{"scan":{"columns":[{"type":"text"}]},"filter":null,
            "aggregate":{"group":[],"aggs":[{"fn":"max","arg":{"col":0}}]},"having":null,
            "output":[{"expr":{"agg":0},"type":"text"}]}"#;
        assert_eq!(run_text(spec, &input).unwrap(), vec![Some("中文".to_string())]);
    }

    #[test]
    fn invalid_utf8_is_an_error() {
        let spec = r#"{"scan":{"columns":[{"type":"text"}]},"filter":null,"aggregate":null,"having":null,
            "output":[{"expr":{"col":0},"type":"text"}]}"#;
        runtime::init(2).unwrap();
        let dir = std::env::temp_dir();
        let mut q = Query::start(spec, 2, 64 << 20, dir.to_str().unwrap()).unwrap();
        let (bytes, offsets, nulls) = ([0xffu8, 0xfe], [0i32, 2], [0u8]);
        let col = RawColumn { values: bytes.as_ptr(), nulls: nulls.as_ptr(), offsets: offsets.as_ptr() };
        let e = unsafe { q.push(&[col], 1) }.err().expect("invalid UTF-8 accepted");
        assert_eq!(e.sqlstate, "22021");
    }

    #[test]
    fn count_merge_adds_partial_counts() {
        // Partial counts arrive as an int4 column here; HAVING sees the
        // combined count, and no rows give 0 where sum gives NULL.
        let spec = r#"{"scan":{"columns":[{"type":"int4"}]},
            "filter":{"op":">","type":"bool","args":[{"col":0},{"lit":{"type":"int4","value":%MIN%}}]},
            "aggregate":{"group":[],"aggs":[{"fn":"count_merge","arg":{"col":0}},{"fn":"sum","arg":{"col":0}}]},
            "having":{"op":">=","type":"bool","args":[{"agg":0},{"lit":{"type":"int8","value":0}}]},
            "output":[{"expr":{"agg":0},"type":"int8"},{"expr":{"agg":1},"type":"int8"}]}"#;
        let input = || vec![(vec![3, 4, 0, 5], vec![0, 0, 1, 0])];
        let rows = run(&spec.replace("%MIN%", "0"), input(), 4).unwrap();
        assert_eq!(rows, vec![vec![Some(12), Some(12)]]);
        let rows = run(&spec.replace("%MIN%", "100"), input(), 4).unwrap();
        assert_eq!(rows, vec![vec![Some(0), None]]);
    }

    /// Run `spec` over `input` with IPC output; returns the stream bytes.
    fn run_ipc_out(spec: &str, input: Vec<(Vec<i32>, Vec<u8>)>, nrows: usize) -> Vec<u8> {
        runtime::init(2).unwrap();
        let dir = std::env::temp_dir();
        let mut q = Query::start_with(spec, 2, 64 << 20, dir.to_str().unwrap(), Source::Pushed, true).unwrap();
        let cols: Vec<RawColumn> = input
            .iter()
            .map(|(v, n)| RawColumn::fixed(v.as_ptr() as *const u8, n.as_ptr()))
            .collect();
        if nrows > 0 {
            while !unsafe { q.push(&cols, nrows) }.unwrap() {}
        }
        q.finish_input();
        let mut out = Vec::new();
        loop {
            match q.poll(Duration::from_millis(50)) {
                Poll::Bytes(_) => out.extend_from_slice(q.output_bytes()),
                Poll::Pending => {}
                Poll::Done => return out,
                Poll::Batch(_) => panic!("batch instead of IPC bytes"),
                Poll::Failed(e) => panic!("{e:?}"),
                Poll::Panicked(m) => panic!("{m}"),
            }
        }
    }

    #[test]
    fn ipc_streams_carry_rows_between_queries() {
        // Two senders (routes) each send (a, a * 10) for their rows; one of
        // them has none.  The receiver reads only the second column.
        let send = r#"{"scan":{"columns":[{"type":"int4"}]},"filter":null,"aggregate":null,"having":null,
            "output":[{"expr":{"col":0},"type":"int4"},
                      {"expr":{"op":"*","type":"int8","args":[{"col":0},{"lit":{"type":"int8","value":10}}]},"type":"int8"}]}"#;
        let a = run_ipc_out(send, vec![(vec![1, 2, 3, 0], vec![0, 0, 0, 1])], 4);
        let b = run_ipc_out(send, vec![(vec![], vec![])], 0);
        let recv = r#"{"scan":{"columns":[{"type":"int8"}],"motion_columns":[1]},"filter":null,
            "aggregate":{"group":[],"aggs":[{"fn":"count_merge","arg":{"col":0}},{"fn":"sum","arg":{"col":0}},{"fn":"count"}]},
            "having":null,
            "output":[{"expr":{"agg":0},"type":"int8"},{"expr":{"agg":1},"type":"int8"},{"expr":{"agg":2},"type":"int8"}]}"#;
        let dir = std::env::temp_dir();
        let mut q = Query::start_with(recv, 2, 64 << 20, dir.to_str().unwrap(), Source::Ipc, false).unwrap();
        // Deliver route 0 in small pieces, interleaved with route 1.
        let (a1, a2) = a.split_at(a.len() / 3);
        for (route, part) in [(0, a1), (1, &b[..]), (0, a2)] {
            let pieces: Vec<&[u8]> = part.chunks(7).collect();
            while !q.push_ipc(route, &pieces).unwrap() {}
        }
        q.finish_input();
        let mut row = Vec::new();
        loop {
            match q.poll(Duration::from_millis(50)) {
                Poll::Batch(_) => {
                    for c in 0..3 {
                        let col = q.output_column(c).unwrap();
                        row.push(unsafe { *(col.values as *const i64) });
                    }
                }
                Poll::Pending => {}
                Poll::Done => break,
                Poll::Failed(e) => panic!("{e:?}"),
                Poll::Panicked(m) => panic!("{m}"),
                Poll::Bytes(_) => panic!("unexpected IPC output"),
            }
        }
        // 4 rows (one NULL): count_merge adds up the non-NULL values 10+20+30.
        assert_eq!(row, vec![60, 60, 4]);
    }

    #[test]
    fn hash_routed_streams_follow_cdbhash() {
        // Rows 0..999 (one NULL) routed by column 0 over 3 segments x 2
        // workers: every route's stream holds exactly the rows cdbhash
        // sends there.
        runtime::init(2).unwrap();
        let spec = r#"{"scan":{"columns":[{"type":"int4"}]},"filter":null,"aggregate":null,"having":null,
            "output":[{"expr":{"col":0},"type":"int4"}],
            "route":{"kind":"hash","keys":[{"col":0,"hash":"int4"}],"segments":3,"workers":2}}"#;
        let vals: Vec<i32> = (0..1000).collect();
        let nulls: Vec<u8> = vals.iter().map(|v| (*v == 500) as u8).collect();
        let dir = std::env::temp_dir();
        let mut q = Query::start_with(spec, 2, 64 << 20, dir.to_str().unwrap(), Source::Pushed, true).unwrap();
        let col = RawColumn::fixed(vals.as_ptr() as *const u8, nulls.as_ptr());
        while !unsafe { q.push(&[col], 1000) }.unwrap() {}
        q.finish_input();
        let mut streams: HashMap<i32, Vec<u8>> = HashMap::new();
        loop {
            match q.poll(Duration::from_millis(50)) {
                Poll::Bytes(_) => streams.entry(q.output_route()).or_default().extend_from_slice(q.output_bytes()),
                Poll::Pending => {}
                Poll::Done => break,
                Poll::Batch(_) => panic!("batch instead of IPC bytes"),
                Poll::Failed(e) => panic!("{e:?}"),
                Poll::Panicked(m) => panic!("{m}"),
            }
        }
        let mut total = 0;
        for (route, bytes) in streams {
            assert!((0..6).contains(&route), "route {route}");
            let mut d = StreamDecoder::new();
            let mut buf = Buffer::from_vec(bytes);
            while !buf.is_empty() {
                if let Some(b) = d.decode(&mut buf).unwrap() {
                    let a = b.column(0).clone();
                    let expect = cdbhash::routes(&[(KeyHash::Int4, a.clone())], b.num_rows(), 3, 2);
                    assert!(expect.iter().all(|r| *r as i32 == route));
                    total += b.num_rows();
                }
            }
            d.finish().unwrap();
        }
        assert_eq!(total, 1000);
    }

    #[test]
    fn truncated_ipc_stream_fails() {
        let send = r#"{"scan":{"columns":[{"type":"int4"}]},"filter":null,"aggregate":null,"having":null,
            "output":[{"expr":{"col":0},"type":"int4"}]}"#;
        let a = run_ipc_out(send, vec![(vec![1, 2, 3], vec![0, 0, 0])], 3);
        let recv = r#"{"scan":{"columns":[{"type":"int4"}],"motion_columns":[0]},"filter":null,
            "aggregate":{"group":[],"aggs":[{"fn":"count"}]},"having":null,
            "output":[{"expr":{"agg":0},"type":"int8"}]}"#;
        let dir = std::env::temp_dir();
        let mut q = Query::start_with(recv, 2, 64 << 20, dir.to_str().unwrap(), Source::Ipc, false).unwrap();
        while !q.push_ipc(0, &[&a[..a.len() - 20]]).unwrap() {}
        q.finish_input();
        loop {
            match q.poll(Duration::from_millis(50)) {
                Poll::Failed(e) => {
                    assert!(e.message.contains("cannot decode"), "{e:?}");
                    return;
                }
                Poll::Done => panic!("a truncated stream was accepted"),
                _ => {}
            }
        }
    }

    #[test]
    fn guarded_division_does_not_fail() {
        // a <> 0 AND 10 / a > 1.  Nine of the ten rows pass the guard, above
        // DataFusion's 20% pre-selection threshold, so without the CASE
        // rewrite the division would run on the a = 0 row too.
        let spec = r#"{"scan":{"columns":[{"type":"int4"}]},
            "filter":{"and":[
                {"op":"<>","type":"bool","args":[{"col":0},{"lit":{"type":"int4","value":0}}]},
                {"op":">","type":"bool","args":[
                    {"op":"/","type":"int4","args":[{"lit":{"type":"int4","value":10}},{"col":0}]},
                    {"lit":{"type":"int4","value":1}}]}]},
            "aggregate":{"group":[],"aggs":[{"fn":"count"}]},"having":null,
            "output":[{"expr":{"agg":0},"type":"int8"}]}"#;
        let rows = run(spec, vec![(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], vec![0; 10])], 10).unwrap();
        assert_eq!(rows, vec![vec![Some(5)]]); // a = 1..5
    }

    #[test]
    fn nested_guards_do_not_fail() {
        // a <> 0 AND 10 / a > 0 AND 100 / (a - 1) > 0: the third argument
        // must only run where the first two are not false (a > 1 here).
        let div = |n: i64, d: &str| format!(
            r#"{{"op":"/","type":"int4","args":[{{"lit":{{"type":"int4","value":{n}}}}},{d}]}}"#);
        let a = r#"{"col":0}"#;
        let a_minus_1 = r#"{"op":"-","type":"int4","args":[{"col":0},{"lit":{"type":"int4","value":1}}]}"#;
        let gt0 = |e: String| format!(r#"{{"op":">","type":"bool","args":[{e},{{"lit":{{"type":"int4","value":0}}}}]}}"#);
        let spec = format!(
            r#"{{"scan":{{"columns":[{{"type":"int4"}}]}},
                "filter":{{"and":[{{"op":"<>","type":"bool","args":[{a},{{"lit":{{"type":"int4","value":0}}}}]}},{},{}]}},
                "aggregate":{{"group":[],"aggs":[{{"fn":"count"}}]}},"having":null,
                "output":[{{"expr":{{"agg":0}},"type":"int8"}}]}}"#,
            gt0(div(10, a)), gt0(div(100, a_minus_1)));
        // a = 1 passes the first two (10/1 = 10 > 0) and divides by zero in
        // the third, exactly as PostgreSQL would; so leave it out.
        let rows = run(&spec, vec![(vec![0, 2, 3, 4, 5, 6, 7, 8, 9, 10], vec![0; 10])], 10).unwrap();
        assert_eq!(rows, vec![vec![Some(9)]]);
    }

    #[test]
    fn grouping_spills_within_a_small_budget() {
        // 200,000 distinct keys, grouped under a 1 MB budget.
        runtime::init(2).unwrap();
        let spec = r#"{"scan":{"columns":[{"type":"int4"}]},"filter":null,
            "aggregate":{"group":[{"col":0}],"aggs":[{"fn":"count"}]},"having":null,
            "output":[{"expr":{"group":0},"type":"int8"},{"expr":{"agg":0},"type":"int8"}]}"#;
        let dir = std::env::temp_dir();
        let mut q = Query::start(spec, 2, 1 << 20, dir.to_str().unwrap()).unwrap();
        let n = 200_000usize;
        let values: Vec<i32> = (0..n as i32).collect();
        let nulls = vec![0u8; n];
        let mut off = 0;
        while off < n {
            let len = BATCH_ROWS.min(n - off);
            let col = RawColumn::fixed(
                unsafe { (values.as_ptr() as *const u8).add(off * 4) },
                unsafe { nulls.as_ptr().add(off) },
            );
            if unsafe { q.push(&[col], len) }.unwrap() {
                off += len;
            } else {
                let _ = q.poll(Duration::from_millis(1));
            }
        }
        q.finish_input();
        let (mut groups, mut total) = (0usize, 0i64);
        loop {
            match q.poll(Duration::from_millis(50)) {
                Poll::Batch(rows) => {
                    let c = q.output_column(1).unwrap();
                    for r in 0..rows {
                        total += unsafe { *(c.values as *const i64).add(r) };
                    }
                    groups += rows;
                }
                Poll::Pending => {}
                Poll::Bytes(_) => panic!("unexpected IPC output"),
                Poll::Done => break,
                Poll::Failed(e) => panic!("{e:?}"),
                Poll::Panicked(m) => panic!("{m}"),
            }
        }
        assert_eq!(groups, n);
        assert_eq!(total, n as i64);
        // The limit makes the aggregate spill.  It is not a hard cap on the
        // pool: reservations DataFusion cannot refuse (merging the spilled
        // runs) may go past it, so the peak is not asserted.  The hard limit
        // is Cloudberry's vmem tracker, which the backend leases from for
        // the whole Rust heap.
        let s = q.stats();
        assert_eq!(s.memory_limit, 1 << 20);
        assert!(s.spill_count > 0 && s.spilled_bytes > 0, "expected spills: {s:?}");
    }

    /// A fake PAX reader: block i holds rows i*10 .. i*10+9 of one int4
    /// column, in two groups, with the row i*10+3 NULL.  It reports holding
    /// 1000 bytes while it reads a block.
    unsafe extern "C" fn fake_read(
        _scan: *mut c_void,
        index: i32,
        emit: PaxEmitFn,
        account: PaxAccountFn,
        ctx: *mut c_void,
        _err: *mut c_char,
        _errlen: usize,
    ) -> i32 {
        account(ctx, 1000);
        for g in 0..2 {
            let vals: Vec<i32> = (0..5).map(|r| index * 10 + g * 5 + r).collect();
            let nulls: Vec<u8> = vals.iter().map(|v| (v % 10 == 3) as u8).collect();
            let col = PaxColumn { values: vals.as_ptr() as *const u8, nulls: nulls.as_ptr() };
            if emit(ctx, 5, &col) != 0 {
                account(ctx, -1000);
                return -1;
            }
        }
        account(ctx, -1000);
        0
    }

    static FAKE_ENDED: AtomicUsize = AtomicUsize::new(0);
    unsafe extern "C" fn fake_end(_scan: *mut c_void) {
        FAKE_ENDED.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn pax_source_reads_all_blocks_once() {
        runtime::init(2).unwrap();
        let spec = r#"{"scan":{"columns":[{"type":"int4"}]},"filter":null,
            "aggregate":{"group":[],"aggs":[{"fn":"count","arg":{"col":0}},{"fn":"sum","arg":{"col":0}},{"fn":"count"}]},
            "having":null,
            "output":[{"expr":{"agg":0},"type":"int8"},{"expr":{"agg":1},"type":"int8"},{"expr":{"agg":2},"type":"int8"}]}"#;
        let before = FAKE_ENDED.load(Ordering::SeqCst);
        let scan = PaxScan::new(std::ptr::null_mut(), 50, fake_read, fake_end);
        let dir = std::env::temp_dir();
        let mut q = Query::start_with(spec, 4, 64 << 20, dir.to_str().unwrap(), Source::Pax(scan), false).unwrap();
        let mut row = Vec::new();
        loop {
            match q.poll(Duration::from_millis(50)) {
                Poll::Batch(n) => {
                    assert_eq!(n, 1);
                    for c in 0..3 {
                        let col = q.output_column(c).unwrap();
                        row.push(unsafe { *(col.values as *const i64) });
                    }
                }
                Poll::Pending => {}
                Poll::Done => break,
                Poll::Failed(e) => panic!("{e:?}"),
                Poll::Panicked(m) => panic!("{m}"),
                Poll::Bytes(_) => panic!("unexpected IPC output"),
            }
        }
        // 500 rows 0..499, 50 of them NULL (those ending in 3).
        let sum: i64 = (0..500).filter(|v| v % 10 != 3).sum();
        assert_eq!(row, vec![450, sum, 500]);
        // At least one block was held, at most one per partition at a time.
        let peak = q.stats().pax_decode_peak;
        assert!((1000..=4000).contains(&peak), "pax_decode_peak {peak}");
        drop(q);
        // The scan is released once the plan is gone.
        for _ in 0..100 {
            if FAKE_ENDED.load(Ordering::SeqCst) > before {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(FAKE_ENDED.load(Ordering::SeqCst), before + 1);
    }

    #[test]
    fn overflow_is_a_pg_error() {
        let spec = r#"{"scan":{"columns":[{"type":"int4"}]},"filter":null,"aggregate":null,"having":null,
            "output":[{"expr":{"op":"+","type":"int4","args":[{"col":0},{"lit":{"type":"int4","value":1}}]},"type":"int8"}]}"#;
        let err = run(spec, vec![(vec![i32::MAX], vec![0])], 1).unwrap_err();
        assert_eq!(err, PgError::new("22003", "integer out of range"));
    }
}

