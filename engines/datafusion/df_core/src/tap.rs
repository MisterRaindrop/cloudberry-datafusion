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

//! EA1: the rows each PostgreSQL plan node of a slice produced, for
//! EXPLAIN ANALYZE.  Under EXPLAIN ANALYZE the host marks the IR of each
//! node with its plan node ids (`"tap": [id, ...]`: several when nodes pass
//! their child's rows on as they are); the node's logical plan is
//! then wrapped in a TapNode, planned as a TapExec, which passes its input's
//! batches on unchanged and counts their rows, and notes when the first
//! batch and the end of the stream came.  `Query::node_stats` reads them
//! back.
//!
//! A TapNode lets projections through (`necessary_children_exprs`) but not
//! filters or limits, so that it counts what the PostgreSQL node it stands
//! for would; DataFusion's other rewrites across it are lost, so the plan
//! under EXPLAIN ANALYZE can run a little differently.  Plans without taps
//! are untouched.

use std::cmp::Ordering as CmpOrdering;
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, DFSchemaRef, Result};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use futures::Stream;

/// Nanoseconds since the first call in this process.
pub fn now_ns() -> u64 {
    static BASE: OnceLock<Instant> = OnceLock::new();
    BASE.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// What one tap saw, over all its partitions.
#[derive(Debug)]
pub struct TapStat {
    pub ids: Vec<i32>,
    rows: AtomicU64,
    first_ns: AtomicU64, // u64::MAX: no batch yet
    end_ns: AtomicU64,   // 0: not ended
}

impl TapStat {
    pub fn new(ids: Vec<i32>) -> Arc<Self> {
        Arc::new(TapStat {
            ids,
            rows: AtomicU64::new(0),
            first_ns: AtomicU64::new(u64::MAX),
            end_ns: AtomicU64::new(0),
        })
    }

    /// Rows, and nanoseconds (of `now_ns`) of the first batch and of the
    /// last partition's end; None for one that did not happen.
    pub fn read(&self) -> (u64, Option<u64>, Option<u64>) {
        let first = self.first_ns.load(Ordering::Relaxed);
        let end = self.end_ns.load(Ordering::Relaxed);
        (
            self.rows.load(Ordering::Relaxed),
            (first != u64::MAX).then_some(first),
            (end != 0).then_some(end),
        )
    }
}

/// The logical node of a tap: its input's rows, as they are.
#[derive(Debug, Clone)]
pub struct TapNode {
    pub input: LogicalPlan,
    pub stat: Arc<TapStat>,
}

impl PartialEq for TapNode {
    fn eq(&self, other: &Self) -> bool {
        self.stat.ids == other.stat.ids && self.input == other.input
    }
}
impl Eq for TapNode {}
impl Hash for TapNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.stat.ids.hash(state);
        self.input.hash(state);
    }
}
impl PartialOrd for TapNode {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        self.stat.ids.partial_cmp(&other.stat.ids)
    }
}

impl UserDefinedLogicalNodeCore for TapNode {
    fn name(&self) -> &str {
        "Tap"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // every column: a filter above stays above, as in PostgreSQL's plan
        self.input
            .schema()
            .columns()
            .into_iter()
            .map(|c| c.flat_name())
            .collect()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tap: nodes {:?}", self.stat.ids)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        match inputs.len() {
            1 => Ok(TapNode {
                input: inputs.remove(0),
                stat: self.stat.clone(),
            }),
            _ => internal_err!("a tap takes one input"),
        }
    }
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // the same columns of its input: projections go through
        Some(vec![output_columns.to_vec()])
    }
}

/// Plans TapNodes as TapExecs.
#[derive(Debug)]
struct TapPlanner;

#[async_trait]
impl ExtensionPlanner for TapPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(tap) = node.as_any().downcast_ref::<TapNode>() else {
            return Ok(None);
        };
        match physical_inputs {
            [input] => Ok(Some(Arc::new(TapExec::new(
                input.clone(),
                tap.stat.clone(),
            )))),
            _ => internal_err!("a tap takes one input"),
        }
    }
}

/// The default query planner, with TapPlanner.
#[derive(Debug)]
pub struct TapQueryPlanner;

#[async_trait]
impl QueryPlanner for TapQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(TapPlanner)])
            .create_physical_plan(logical_plan, session)
            .await
    }
}

/// Its input's batches, counted.
#[derive(Debug)]
pub struct TapExec {
    input: Arc<dyn ExecutionPlan>,
    pub stat: Arc<TapStat>,
}

impl TapExec {
    fn new(input: Arc<dyn ExecutionPlan>, stat: Arc<TapStat>) -> Self {
        TapExec { input, stat }
    }
}

impl DisplayAs for TapExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TapExec: nodes {:?}", self.stat.ids)
    }
}

impl ExecutionPlan for TapExec {
    fn name(&self) -> &str {
        "TapExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        // its input's: partitioning, ordering and all
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
            [c] => Ok(Arc::new(TapExec::new(c.clone(), self.stat.clone()))),
            _ => internal_err!("TapExec takes one input"),
        }
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, ctx)?;
        Ok(Box::pin(TapStream {
            schema: input.schema(),
            input,
            stat: self.stat.clone(),
            ended: false,
        }))
    }
}

struct TapStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    stat: Arc<TapStat>,
    ended: bool,
}

impl Stream for TapStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.input.as_mut().poll_next(cx);
        match &poll {
            Poll::Ready(Some(Ok(batch))) => {
                if batch.num_rows() > 0 {
                    self.stat.first_ns.fetch_min(now_ns(), Ordering::Relaxed);
                }
                self.stat
                    .rows
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            }
            Poll::Ready(None) if !self.ended => {
                self.ended = true;
                self.stat.end_ns.fetch_max(now_ns(), Ordering::Relaxed);
            }
            _ => {}
        }
        poll
    }
}

impl RecordBatchStream for TapStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// The taps of physical plan `plan`, each once.
pub fn collect(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Arc<TapStat>>) {
    if let Some(t) = plan.downcast_ref::<TapExec>() {
        if !out.iter().any(|s| Arc::ptr_eq(s, &t.stat)) {
            out.push(t.stat.clone());
        }
    }
    for c in plan.children() {
        collect(c, out);
    }
}
