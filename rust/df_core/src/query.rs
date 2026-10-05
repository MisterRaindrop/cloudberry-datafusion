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
//! The plan arrives as JSON built by the C side (src/df_translate.c):
//!
//! ```text
//! { "scan":      { "columns": [ {"type": "int4"}, ... ] },
//!   "filter":    <expr> | null,
//!   "aggregate": { "group": [<expr>...], "aggs": [ {"fn": "sum", "arg": <expr>} ... ] } | null,
//!   "having":    <expr> | null,
//!   "output":    [ {"expr": <expr>, "type": "int8"}, ... ] }
//!
//! <expr> := {"col": i} | {"group": i} | {"agg": i}
//!         | {"lit": {"type": t, "value": v}} | {"lit": {"type": t, "null": true}}
//!         | {"op": "+", "type": t, "args": [<expr>, <expr>]}
//!         | {"and": [...]} | {"or": [...]} | {"not": <expr>}
//!         | {"isnull": <expr>} | {"isnotnull": <expr>}
//! ```

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Schema, SchemaRef,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::ScalarValue;
use datafusion::datasource::provider_as_source;
use datafusion::error::DataFusionError;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
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

use crate::pgfunc::{to_pg_error, ArithOp, PgArith, PgError};
use crate::runtime;

/// Rows per input batch the main thread pushes.
pub const BATCH_ROWS: usize = 8192;

/// Batches buffered in each direction.
const CHANNEL_DEPTH: usize = 4;

/// The PostgreSQL types a slice can carry so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
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
            other => return Err(format!("unsupported type {other}")),
        })
    }

    pub fn arrow(self) -> DataType {
        match self {
            PgType::Bool => DataType::Boolean,
            PgType::Int2 => DataType::Int16,
            PgType::Int4 => DataType::Int32,
            PgType::Int8 => DataType::Int64,
            PgType::Float4 => DataType::Float32,
            PgType::Float8 => DataType::Float64,
        }
    }
}

/// Column data as the C side lays it out: native values (one byte per bool)
/// and one byte per row that is 1 for NULL.
#[derive(Clone, Copy)]
pub struct RawColumn {
    pub values: *const u8,
    pub nulls: *const u8,
}

/// Converted output column whose buffers stay valid until the next poll.
struct OutColumn {
    _array: ArrayRef,
    bools: Option<Vec<u8>>,
    nulls: Vec<u8>,
    values: *const u8,
}

pub enum Poll {
    Batch(usize),
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
    let sv = match ty {
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

fn expr(v: &Value) -> Result<Expr, String> {
    if let Some(i) = v.get("col") {
        return Ok(col(format!("c{}", index(i)?)));
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
        "sum" => sum(need(arg)?),
        "min" => min(need(arg)?),
        "max" => max(need(arg)?),
        "avg" => avg(need(arg)?),
        other => return Err(format!("unsupported aggregate {other}")),
    })
}

/// DataFusion's default session, minus expression simplification.
///
/// The simplifier rewrites `CASE WHEN x IS FALSE THEN false ELSE x AND y
/// END` back into `x AND y`, which evaluates `y` on every row and undoes
/// the guard that keeps PostgreSQL from dividing by zero (see `expr`).  The
/// plan comes from PostgreSQL's planner, which has already folded constants
/// and simplified expressions, so little is lost.
fn session_state(config: SessionConfig) -> SessionState {
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let rules: Vec<_> = state
        .optimizers()
        .iter()
        .filter(|r| r.name() != "simplify_expressions")
        .cloned()
        .collect();
    SessionStateBuilder::new_from_existing(state)
        .with_optimizer_rules(rules)
        .build()
}

/// Single-use stream fed by the main thread.
struct ChannelPartition {
    schema: SchemaRef,
    rx: Mutex<Option<mpsc::Receiver<RecordBatch>>>,
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
            let batch = rx.recv().await?;
            Some((Ok(batch), Some(rx)))
        });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

pub struct Query {
    handle: Handle,
    in_tx: Option<mpsc::Sender<RecordBatch>>,
    in_schema: SchemaRef,
    in_types: Vec<PgType>,
    out_rx: mpsc::Receiver<Result<RecordBatch, PgError>>,
    out_types: Vec<PgType>,
    task: Option<JoinHandle<()>>,
    current: Vec<OutColumn>,
}

impl Query {
    /// Build the plan described by `spec` and start running it.
    pub fn start(spec: &str, partitions: usize) -> Result<Query, PgError> {
        let handle = runtime::handle()
            .ok_or_else(|| PgError::internal("the DataFusion runtime is not running"))?;
        let spec: Value = serde_json::from_str(spec)
            .map_err(|e| PgError::internal(format!("bad plan spec: {e}")))?;
        let internal = |e: String| PgError::internal(format!("cannot build DataFusion plan: {e}"));
        let df = |e: DataFusionError| internal(e.to_string());

        let in_types: Vec<PgType> = field(&spec, "scan")
            .and_then(|s| field(s, "columns"))
            .map_err(internal)?
            .as_array()
            .ok_or_else(|| internal("bad scan columns".into()))?
            .iter()
            .map(|c| PgType::parse(c.get("type").and_then(Value::as_str).unwrap_or("")))
            .collect::<Result<_, _>>()
            .map_err(internal)?;
        let in_schema: SchemaRef = Arc::new(Schema::new(
            in_types
                .iter()
                .enumerate()
                .map(|(i, t)| Field::new(format!("c{i}"), t.arrow(), true))
                .collect::<Vec<_>>(),
        ));

        let (in_tx, in_rx) = mpsc::channel(CHANNEL_DEPTH);
        let partition = ChannelPartition { schema: in_schema.clone(), rx: Mutex::new(Some(in_rx)) };
        let table = StreamingTable::try_new(in_schema.clone(), vec![Arc::new(partition)]).map_err(df)?;

        let mut b = LogicalPlanBuilder::scan("t", provider_as_source(Arc::new(table)), None).map_err(df)?;
        if let Some(f) = spec.get("filter").filter(|v| !v.is_null()) {
            b = b.filter(expr(f).map_err(internal)?).map_err(df)?;
        }
        if let Some(a) = spec.get("aggregate").filter(|v| !v.is_null()) {
            let groups = field(a, "group").map_err(internal)?.as_array().cloned().unwrap_or_default();
            let aggs = field(a, "aggs").map_err(internal)?.as_array().cloned().unwrap_or_default();
            let group_exprs = groups
                .iter()
                .enumerate()
                .map(|(i, g)| expr(g).map(|e| e.alias(format!("g{i}"))))
                .collect::<Result<Vec<_>, _>>()
                .map_err(internal)?;
            let agg_exprs = aggs
                .iter()
                .enumerate()
                .map(|(i, g)| aggregate(g).map(|e| e.alias(format!("a{i}"))))
                .collect::<Result<Vec<_>, _>>()
                .map_err(internal)?;
            b = b.aggregate(group_exprs, agg_exprs).map_err(df)?;
            if let Some(h) = spec.get("having").filter(|v| !v.is_null()) {
                b = b.filter(expr(h).map_err(internal)?).map_err(df)?;
            }
        }
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

        let config = SessionConfig::new()
            .with_target_partitions(partitions.max(1))
            .with_batch_size(BATCH_ROWS);
        let ctx = SessionContext::new_with_state(session_state(config));
        let (out_tx, out_rx) = mpsc::channel(CHANNEL_DEPTH);

        let task = handle.spawn(async move {
            let run = async {
                let state = ctx.state();
                let physical = state.create_physical_plan(&plan).await?;
                let mut stream = execute_stream(physical, ctx.task_ctx())?;
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() > 0 && out_tx.send(Ok(batch)).await.is_err() {
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
            in_tx: Some(in_tx),
            in_schema,
            in_types,
            out_rx,
            out_types,
            task: Some(task),
            current: Vec::new(),
        })
    }

    pub fn num_input_columns(&self) -> usize {
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
        let tx = self.in_tx.as_ref().ok_or_else(|| PgError::internal("input already finished"))?;
        if tx.capacity() == 0 {
            return Ok(false);
        }
        if cols.len() != self.in_types.len() {
            return Err(PgError::internal("wrong number of input columns"));
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
        for (c, ty) in cols.iter().zip(&self.in_types) {
            arrays.push(build_array(*ty, *c, nrows));
        }
        let batch = if arrays.is_empty() {
            RecordBatch::try_new_with_options(
                self.in_schema.clone(),
                arrays,
                &datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(nrows)),
            )
        } else {
            RecordBatch::try_new(self.in_schema.clone(), arrays)
        }
        .map_err(|e| PgError::internal(e.to_string()))?;
        match tx.try_send(batch) {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            // The plan stopped reading (finished early or failed); poll tells.
            Err(mpsc::error::TrySendError::Closed(_)) => Ok(true),
        }
    }

    /// No more input.
    pub fn finish_input(&mut self) {
        self.in_tx = None;
    }

    /// Wait up to `timeout` for the next result batch.
    pub fn poll(&mut self, timeout: Duration) -> Poll {
        self.current.clear();
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
            Some(Ok(batch)) => match self.convert(&batch) {
                Ok(()) => Poll::Batch(batch.num_rows()),
                Err(e) => Poll::Failed(e),
            },
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
            let (bools, values) = match ty {
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
            };
            self.current.push(OutColumn { _array: array, bools, nulls, values });
        }
        Ok(())
    }

    /// Buffers of output column `i` of the current batch.
    pub fn output_column(&self, i: usize) -> Option<RawColumn> {
        self.current.get(i).map(|c| RawColumn {
            values: c.bools.as_ref().map(|b| b.as_ptr()).unwrap_or(c.values),
            nulls: c.nulls.as_ptr(),
        })
    }
}

impl Drop for Query {
    fn drop(&mut self) {
        self.in_tx = None;
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

unsafe fn primitive<T: ArrowPrimitiveType>(c: RawColumn, n: usize, nulls: Option<NullBuffer>) -> ArrayRef {
    let values = std::slice::from_raw_parts(c.values as *const T::Native, n).to_vec();
    Arc::new(PrimitiveArray::<T>::new(ScalarBuffer::from(values), nulls))
}

unsafe fn build_array(ty: PgType, c: RawColumn, n: usize) -> ArrayRef {
    let null_bytes = std::slice::from_raw_parts(c.nulls, n);
    let nulls = if null_bytes.iter().any(|b| *b != 0) {
        Some(NullBuffer::from(null_bytes.iter().map(|b| *b == 0).collect::<Vec<bool>>()))
    } else {
        None
    };
    match ty {
        PgType::Bool => {
            let v = std::slice::from_raw_parts(c.values, n);
            Arc::new(BooleanArray::new(BooleanBuffer::from_iter(v.iter().map(|b| *b != 0)), nulls))
        }
        PgType::Int2 => primitive::<Int16Type>(c, n, nulls),
        PgType::Int4 => primitive::<Int32Type>(c, n, nulls),
        PgType::Int8 => primitive::<Int64Type>(c, n, nulls),
        PgType::Float4 => primitive::<Float32Type>(c, n, nulls),
        PgType::Float8 => primitive::<Float64Type>(c, n, nulls),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(spec: &str, input: Vec<(Vec<i32>, Vec<u8>)>, nrows: usize) -> Result<Vec<Vec<Option<i64>>>, PgError> {
        runtime::init(2).unwrap();
        let mut q = Query::start(spec, 2)?;
        let cols: Vec<RawColumn> = input
            .iter()
            .map(|(v, n)| RawColumn { values: v.as_ptr() as *const u8, nulls: n.as_ptr() })
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
    fn overflow_is_a_pg_error() {
        let spec = r#"{"scan":{"columns":[{"type":"int4"}]},"filter":null,"aggregate":null,"having":null,
            "output":[{"expr":{"op":"+","type":"int4","args":[{"col":0},{"lit":{"type":"int4","value":1}}]},"type":"int8"}]}"#;
        let err = run(spec, vec![(vec![i32::MAX], vec![0])], 1).unwrap_err();
        assert_eq!(err, PgError::new("22003", "integer out of range"));
    }
}

