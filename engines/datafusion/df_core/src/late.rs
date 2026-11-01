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


//! LT1: the columns of a PAX input that only a Limit's rows need, read
//! after the Limit.  `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY
//! EventTime LIMIT 10` (ClickBench Q24) decoded all 105 columns of every
//! group, and copied all 28 strings of every row, to keep 10 rows: 9.7 s on
//! win-dev, against 1.6 s reading EventTime alone.  The reader now decodes
//! the columns the filter and the sort read, and hands out each row's
//! locator (block, group, position) in place of the others; above the
//! Limit, LateFetchExec reads those columns of the rows left, group by
//! group (read_rows).  The rows a slice's Limit keeps are few
//! (LATE_MAX_ROWS), so are the groups decoded again.

use std::ffi::{c_char, c_void};
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{new_empty_array, Array, ArrayRef, AsArray, UInt32Array};
use datafusion::arrow::compute::{concat, concat_batches, take};
use datafusion::arrow::datatypes::{Field, Int64Type, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::TryStreamExt;
use serde_json::Value;

use crate::pgfunc::PgError;
use crate::query::{build_array_rows, PaxColumn, PaxFetchFn, PaxScan, PgType, RawColumn};

/// Most rows a slice's Limit may keep (its count plus its offset) for the
/// columns only it needs to be read after it: each row may need a group of
/// its own decoded again.
pub const LATE_MAX_ROWS: u64 = 100;

/// The rows the slice's Limit keeps at most (its count plus its offset).
pub fn late_rows(spec: &Value) -> u64 {
    let limit = spec.get("limit");
    let n = |name: &str| limit.and_then(|l| l.get(name)).and_then(Value::as_u64);
    n("fetch").unwrap_or(u64::MAX).saturating_add(n("skip").unwrap_or(0))
}

/// The name of the locators' column above the scan.
pub const LATE_LOC: &str = "lt_loc";

/// Input column `k` of input `j`, when `v` is that column and nothing else.
fn plain_column(v: &Value, j: usize) -> Option<usize> {
    let m = v.as_object()?;
    let k = m.get("col")?.as_u64()? as usize;
    let input = m.get("input").map_or(Some(0), Value::as_u64)? as usize;
    (input == j && m.keys().all(|key| key == "col" || key == "input")).then_some(k)
}

fn columns_of(v: &Value, out: &mut Vec<usize>) {
    match v {
        Value::Object(m) => {
            if let Some(k) = m.get("col").and_then(Value::as_u64) {
                out.push(k as usize);
            }
            m.values().for_each(|x| columns_of(x, out));
        }
        Value::Array(a) => a.iter().for_each(|x| columns_of(x, out)),
        _ => {}
    }
}

/// The input and the columns of it to read after the Limit, ascending:
/// when the slice is a sort and a Limit of at most LATE_MAX_ROWS rows over
/// one input, filtered or not, the columns of that input that outputs are
/// and that nothing else reads (the filter, the sort keys, other outputs).
pub fn late_columns(spec: &Value) -> Option<(usize, Vec<usize>)> {
    spec.get("sort").filter(|v| !v.is_null())?;
    let limit = spec.get("limit").filter(|v| !v.is_null())?;
    limit.get("fetch")?.as_u64()?;
    if late_rows(spec) > LATE_MAX_ROWS {
        return None;
    }
    let node = spec.get("plan")?;
    let (j, pred) = match (node.get("input"), node.get("filter")) {
        (Some(j), None) => (j.as_u64()? as usize, None),
        (None, Some(f)) => (f.get("input")?.get("input")?.as_u64()? as usize, f.get("pred")),
        _ => return None,
    };
    let outputs = spec.get("output")?.as_array()?;
    let mut read = Vec::new();
    if let Some(p) = pred {
        columns_of(p, &mut read);
    }
    for k in spec.get("sort")?.as_array()? {
        match (k.get("expr"), k.get("col").and_then(Value::as_u64)) {
            (Some(e), _) => columns_of(e, &mut read),
            (None, Some(c)) => columns_of(outputs.get(c as usize)?.get("expr")?, &mut read),
            _ => return None,
        }
    }
    let mut candidates = Vec::new();
    for o in outputs {
        let e = o.get("expr")?;
        match plain_column(e, j) {
            Some(k) => candidates.push(k),
            None => columns_of(e, &mut read),
        }
    }
    let mut late: Vec<usize> = candidates.into_iter().filter(|k| !read.contains(k)).collect();
    late.sort_unstable();
    late.dedup();
    (!late.is_empty()).then_some((j, late))
}

/// Reads the scan's late columns for the rows of its input, by their
/// locators (column LATE_LOC), and puts them after the input's other
/// columns, as `names`.
pub struct LateFetchExec {
    input: Arc<dyn ExecutionPlan>,
    scan: Arc<PaxScan>,
    loc: usize,
    types: Vec<PgType>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for LateFetchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LateFetchExec")
    }
}

impl LateFetchExec {
    /// `types` and `names`: one per late column of the scan, in its order.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        scan: Arc<PaxScan>,
        types: Vec<PgType>,
        names: Vec<String>,
    ) -> Result<Self> {
        let input: Arc<dyn ExecutionPlan> =
            if input.properties().output_partitioning().partition_count() > 1 {
                Arc::new(CoalescePartitionsExec::new(input))
            } else {
                input
            };
        let in_schema = input.schema();
        let loc = in_schema.index_of(LATE_LOC)?;
        let mut fields: Vec<Field> = in_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != loc)
            .map(|(_, f)| f.as_ref().clone())
            .collect();
        for (t, n) in types.iter().zip(&names) {
            fields.push(Field::new(n, t.arrow(), true));
        }
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        Ok(LateFetchExec {
            input,
            scan,
            loc,
            types,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
            schema,
        })
    }
}

impl DisplayAs for LateFetchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LateFetchExec: columns={:?}", self.scan.late)
    }
}

impl ExecutionPlan for LateFetchExec {
    fn name(&self) -> &str {
        "LateFetchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.as_slice() {
            [c] => Ok(Arc::new(LateFetchExec {
                input: c.clone(),
                scan: self.scan.clone(),
                loc: self.loc,
                types: self.types.clone(),
                schema: self.schema.clone(),
                properties: self.properties.clone(),
            })),
            _ => internal_err!("LateFetchExec takes one input"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("LateFetchExec has one partition");
        }
        let input = self.input.clone();
        let scan = self.scan.clone();
        let types = self.types.clone();
        let schema = self.schema.clone();
        let loc = self.loc;
        let fut = async move {
            let in_schema = input.schema();
            let batches: Vec<RecordBatch> = input.execute(0, ctx)?.try_collect().await?;
            let batch = concat_batches(&in_schema, &batches)?;
            let locs: Vec<i64> = batch.column(loc).as_primitive::<Int64Type>().values().to_vec();
            let late = read_late(&scan, &locs, &types)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let mut cols: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != loc)
                .map(|(_, c)| c.clone())
                .collect();
            cols.extend(late);
            Ok(RecordBatch::try_new_with_options(
                schema,
                cols,
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )?)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(fut),
        )))
    }
}

/// What read_rows' emit builds: the late columns of the rows asked for,
/// chunk by chunk.
struct LateCtx<'a> {
    scan: &'a PaxScan,
    types: &'a [PgType],
    chunks: Vec<Vec<ArrayRef>>,
    rows: usize,
    error: Option<String>,
}

unsafe extern "C" fn late_emit(
    ctx: *mut c_void,
    nrows: u32,
    cols: *const PaxColumn,
    fetch: PaxFetchFn,
    chunk: *mut c_void,
) -> i32 {
    let ctx = &mut *(ctx as *mut LateCtx);
    let n = nrows as usize;
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<(), PgError> {
        for (p, &k) in ctx.scan.late.iter().enumerate() {
            let mut c = *cols.add(k);
            if c.values.is_null() {
                if fetch(chunk, k as i32, std::ptr::null(), nrows, &mut c) != 0 {
                    return Err(PgError::internal(String::new()));
                }
            }
            let raw = RawColumn {
                values: c.values,
                nulls: c.nulls,
                offsets: c.offsets,
            };
            ctx.chunks[p].push(build_array_rows(ctx.types[p], raw, n, None)?);
        }
        ctx.rows += n;
        Ok(())
    }));
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => {
            ctx.error = (!e.message.is_empty()).then_some(e.message);
            1
        }
        Err(_) => {
            ctx.error = Some("panic while reading late columns".into());
            1
        }
    }
}

unsafe extern "C" fn late_account(ctx: *mut c_void, delta: i64) {
    let ctx = &*(ctx as *const LateCtx);
    ctx.scan.shared.memory.add(delta);
}

/// Most groups read_late reads at once, each on a thread of its own.
const LATE_THREADS: usize = 16;

/// The late columns of the rows at `locs`, in their order: each group's
/// rows read at once, ascending, the groups in parallel (each costs a
/// decode of the whole group's late columns).
fn read_late(scan: &PaxScan, locs: &[i64], types: &[PgType]) -> Result<Vec<ArrayRef>, PgError> {
    let n = locs.len();
    let rows_fn = scan
        .rows_fn
        .ok_or_else(|| PgError::internal("the PAX reader cannot read rows"))?;
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by_key(|&i| locs[i]);
    // the groups to read (block << 16 | group, rows ascending), and where
    // each row lands among the rows read, group after group
    let mut groups: Vec<(i64, Vec<u32>)> = Vec::new();
    let mut at = vec![0u32; n];
    let mut read = 0usize;
    for &i in &order {
        let bg = locs[i] >> 24;
        let r = (locs[i] & 0xff_ffff) as u32;
        if groups.last().map(|(g, _)| *g) != Some(bg) {
            groups.push((bg, Vec::new()));
        }
        let rows = &mut groups.last_mut().unwrap().1;
        if rows.last() != Some(&r) {
            rows.push(r);
            read += 1;
        }
        at[i] = (read - 1) as u32;
    }

    let read_group = |bg: i64, rows: &[u32]| -> Result<Vec<Vec<ArrayRef>>, PgError> {
        let mut ctx = LateCtx {
            scan,
            types,
            chunks: vec![Vec::new(); types.len()],
            rows: 0,
            error: None,
        };
        let mut err = vec![0 as c_char; 1024];
        // SAFETY: see PaxScan; the scan has not ended (the caller holds
        // `ended`) and ctx outlives the call.
        let rc = unsafe {
            rows_fn(
                scan.raw(),
                (bg >> 16) as i32,
                (bg & 0xffff) as i32,
                rows.as_ptr(),
                rows.len() as u32,
                late_emit,
                late_account,
                &mut ctx as *mut LateCtx as *mut c_void,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        if rc != 0 {
            return Err(PgError::internal(ctx.error.take().unwrap_or_else(|| {
                unsafe { std::ffi::CStr::from_ptr(err.as_ptr()) }
                    .to_string_lossy()
                    .into_owned()
            })));
        }
        if ctx.rows != rows.len() {
            return Err(PgError::internal(format!(
                "read {} rows of a group, asked for {}",
                ctx.rows,
                rows.len()
            )));
        }
        Ok(ctx.chunks)
    };

    let ended = scan.ended();
    if *ended {
        return Err(PgError::internal("the PAX scan has ended"));
    }
    let results: Vec<Mutex<Option<Result<Vec<Vec<ArrayRef>>, PgError>>>> =
        groups.iter().map(|_| Mutex::new(None)).collect();
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..groups.len().min(LATE_THREADS) {
            s.spawn(|| loop {
                let g = next.fetch_add(1, Ordering::Relaxed);
                let Some((bg, rows)) = groups.get(g) else { break };
                let r = catch_unwind(AssertUnwindSafe(|| read_group(*bg, rows)))
                    .unwrap_or_else(|_| Err(PgError::internal("panic while reading late columns")));
                *results[g].lock().unwrap() = Some(r);
            });
        }
    });
    drop(ended);

    let mut pieces: Vec<Vec<ArrayRef>> = vec![Vec::new(); types.len()];
    for r in results {
        let chunks = r
            .into_inner()
            .unwrap()
            .unwrap_or_else(|| Err(PgError::internal("a group was not read")))?;
        for (p, c) in chunks.into_iter().enumerate() {
            pieces[p].extend(c);
        }
    }
    let positions = UInt32Array::from(at);
    let internal = |e: datafusion::arrow::error::ArrowError| PgError::internal(e.to_string());
    pieces
        .into_iter()
        .zip(types)
        .map(|(ps, t)| {
            let all = if ps.is_empty() {
                new_empty_array(&t.arrow())
            } else {
                let refs: Vec<&dyn Array> = ps.iter().map(|a| a.as_ref()).collect();
                concat(&refs).map_err(internal)?
            };
            take(all.as_ref(), &positions, None).map_err(internal)
        })
        .collect()
}
