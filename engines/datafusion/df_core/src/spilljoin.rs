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

//! A hash join that spills (SHJ).
//!
//! DataFusion's HashJoinExec holds its whole build side in memory and fails
//! the query when that outgrows the memory pool, where PostgreSQL's hash
//! join spills; the planner's estimate of a build side can be far off
//! (TPC-H Q9 at scale factor 10: 40 rows estimated, a million per segment).
//!
//! `SpillingHashJoinExec` wraps a partitioned HashJoinExec.  Each partition
//! collects its build side under a memory reservation; while it fits, the
//! rows go to an ordinary in-memory HashJoinExec, so a join that fits costs
//! what it did.  Past the reservation the build rows, and then the probe
//! rows, are split by their keys' hash into `FANOUT` buckets on disk, and
//! the buckets are joined one after the other; a bucket that still does not
//! fit is split again with another hash, up to `MAX_DEPTH` times, then
//! joined in memory if the pool has the room (rows of one key cannot be
//! split).  The in-memory join takes over the reservation its build side
//! was collected under (see `HeldPool`).  Rows of
//! equal keys share a bucket, so every join type that only matches equal
//! keys gives the same rows: inner, outer, semi, anti and mark joins, with
//! or without a join filter.  A null-aware anti join (NOT IN) depends on the
//! whole build side (a NULL anywhere empties the result) and is left alone.
//!
//! The build side is read to its end before the probe side, the order in
//! which the main thread feeds a slice's inputs.
//!
//! AJ1: unless the probe side joins or aggregates (`derived`) and reads
//! its own inputs (PAX scans the partitions read, nothing the main thread
//! feeds, no set another input fills; `reads_itself`).  Then a partition reads up to `PEEK_ROWS` probe
//! rows first; a probe side that ends within them is joined the other way
//! round, its rows the hash table and the build side streamed past it,
//! unless the build side ends within as many rows.  The planner's estimate
//! of a side can be far off: TPC-H Q18's `o_orderkey IN (... HAVING
//! sum(l_quantity) > 300)` is estimated at a third of the orders, against a
//! few thousand, and on three segments PostgreSQL hashed orders joined
//! with customer (50 million rows a segment) to probe them with the
//! lineitem rows of those few thousand orders.
//!
//! RC1: a partitioned join hash-partitions both of its sides, the probe
//! side too, however small the build side turns out (TPC-H at scale factor
//! 100 spent a fifth of Q5's, Q8's and Q17's time partitioning lineitem for
//! a few thousand parts).  Where nothing above relies on the join's rows
//! coming out partitioned by its keys, the build side is read first, below
//! the repartition, by all its partitions at once: one that ends within
//! `COLLECT_ROWS` rows and `COLLECT_BYTES` makes one table every probe
//! partition probes as it comes (DataFusion's CollectLeft).  A larger one
//! goes on as planned, the rows read so far ahead of the rest.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, UInt32Array};
use datafusion::arrow::compute::{concat_batches, take_record_batch};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::hash_utils::{create_hashes, RandomState};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::utils::memory::estimate_memory_size;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{internal_err, DataFusionError, NullEquality, Result};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::{SpillFile, TaskContext};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, Partitioning, PhysicalExpr, PhysicalExprRef,
};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, SpillMetrics,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::spill::{get_record_batch_memory_size, SpillManager};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::physical_plan::{
    replace_children_if_necessary, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, PlanProperties, SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};

/// Buckets a spilled build side is split into, at each depth.
const FANOUT: usize = 16;
/// Times a bucket that does not fit may be split again.
const MAX_DEPTH: u32 = 4;
/// Bytes per build row DataFusion's hash table takes besides the row: an
/// 8-byte link and two 16-byte slots of its map.
const HASH_ROW_BYTES: usize = 40;
/// Bytes of a bucket's rows gathered before they are written as one batch
/// (or the session's batch size in rows, whichever comes first).
const BUCKET_BATCH_BYTES: usize = 1 << 20;
/// AJ1: probe rows a partition reads before its build side, and the bytes
/// they may take at most.
const PEEK_ROWS: usize = 128 * 1024;
const PEEK_BYTES: usize = 32 << 20;
/// RC1: rows and bytes of a build side collected for all probe partitions,
/// at most.  DataFusion builds that table on one thread.
const COLLECT_ROWS: usize = 1 << 20;
const COLLECT_BYTES: usize = 64 << 20;

/// AJ1: whether `plan` joins or aggregates: the planner's estimates of
/// those go wrong, a table's rows (filtered) seldom by much.  Reading a scan
/// of a large table ahead decodes its first groups early, to be held while
/// the build side is read (TPC-H Q18 at scale factor 10, where the planner
/// chose well, took 15% longer peeking at lineitem and orders).
fn derived(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        found = matches!(
            node.name(),
            "HashJoinExec"
                | "SpillingHashJoinExec"
                | "SortMergeJoinExec"
                | "NestedLoopJoinExec"
                | "CrossJoinExec"
                | "AggregateExec"
        );
        Ok(if found { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
    });
    found
}

/// AJ1: whether every input under `plan` is read by the partitions
/// themselves: PAX scans (PaxPartition), empty relations, no set or scalar
/// subquery waiting for another input.  Reading such a side before the
/// other cannot wait on the main thread, which feeds its inputs in order.
/// `memory_leaves`: in-memory tables count too (the tests').
fn reads_itself(plan: &Arc<dyn ExecutionPlan>, memory_leaves: bool) -> bool {
    let mut ok = true;
    let _ = plan.apply(|node| {
        if let Some(s) = node.downcast_ref::<StreamingTableExec>() {
            ok = s.partitions().iter().all(|p| format!("{p:?}") == "PaxPartition");
        } else if node.children().is_empty() {
            ok = matches!(node.name(), "EmptyExec" | "PlaceholderRowExec")
                || (memory_leaves && node.name() == "DataSourceExec");
        }
        if ok {
            node.apply_expressions(&mut |e| {
                let text = e.to_string();
                ok = !text.contains("pg_in_set") && !text.contains("pg_scalar_subquery");
                Ok(if ok { TreeNodeRecursion::Continue } else { TreeNodeRecursion::Stop })
            })?;
        }
        Ok(if ok { TreeNodeRecursion::Continue } else { TreeNodeRecursion::Stop })
    });
    ok
}

/// Bytes DataFusion's hash join reserves to concatenate a build side's
/// batches into one: none for a single batch, which it keeps as it is; the
/// estimate of its `concat_build_batches` otherwise.
fn concat_bytes(batches: &[RecordBatch]) -> Result<usize> {
    if batches.len() <= 1 {
        return Ok(0);
    }
    let mut bytes = 0;
    for array in batches.iter().flat_map(|b| b.columns()) {
        bytes += match array.data_type() {
            DataType::Utf8View | DataType::BinaryView => {
                array.len() * 16 + array.nulls().map_or(0, |_| array.len().div_ceil(8))
            }
            _ => array.to_data().get_slice_memory_size()?,
        };
    }
    Ok(bytes)
}

/// Replaces each partitioned HashJoinExec (and one reading single
/// partitions) by a SpillingHashJoinExec.
#[derive(Debug)]
pub struct SpillHashJoins {
    /// AJ1 over in-memory tables too, joined or aggregated or not, for the
    /// tests
    memory_leaves: bool,
    /// RC1: rows a build side collected for all partitions may have (0:
    /// none is)
    collect_rows: usize,
}

impl Default for SpillHashJoins {
    fn default() -> Self {
        SpillHashJoins {
            memory_leaves: false,
            collect_rows: COLLECT_ROWS,
        }
    }
}

impl SpillHashJoins {
    /// `plan` with its joins replaced; `relied`: whether a node above
    /// relies on how `plan`'s rows are partitioned (RC1).
    fn replace(&self, plan: Arc<dyn ExecutionPlan>, relied: bool) -> Result<Arc<dyn ExecutionPlan>> {
        // A node relies on a child's partitioning when it requires one, or
        // when it passes it on to a node that relies on it; a repartition
        // or a merge makes its own.
        let passes = !matches!(
            plan.name(),
            "RepartitionExec" | "CoalescePartitionsExec" | "SortPreservingMergeExec"
        );
        let requirements = plan.input_distribution_requirements();
        let children = plan
            .children()
            .into_iter()
            .enumerate()
            .map(|(i, child)| {
                let relied = match requirements.child_distribution(i) {
                    None | Some(Distribution::UnspecifiedDistribution) => relied && passes,
                    Some(_) => true,
                };
                self.replace(child.clone(), relied)
            })
            .collect::<Result<Vec<_>>>()?;
        let plan = replace_children_if_necessary(plan, children)?;
        let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
            return Ok(plan);
        };
        // A collected build side is shared by every probe partition:
        // only one probe partition makes it a partition of its own.
        let own = match join.mode {
            PartitionMode::Partitioned => true,
            PartitionMode::CollectLeft => {
                join.right.output_partitioning().partition_count() == 1
            }
            PartitionMode::Auto => false,
        };
        if join.null_aware || !own {
            return Ok(plan);
        }
        let collect_rows = if relied { 0 } else { self.collect_rows };
        Ok(Arc::new(SpillingHashJoinExec::new(plan.clone(), self.memory_leaves, collect_rows)?))
    }
}

impl PhysicalOptimizerRule for SpillHashJoins {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace(plan, false)
    }

    fn name(&self) -> &str {
        "spill_hash_joins"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// RC1: the input below the hash repartition DataFusion put over a join's
/// side, if it did.
fn below_repartition(side: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(r) = side.downcast_ref::<RepartitionExec>() {
        return matches!(r.partitioning(), Partitioning::Hash(..)).then(|| r.input().clone());
    }
    if side.name() == "CoalesceBatchesExec" {
        return below_repartition(side.children()[0]);
    }
    None
}

fn same(a: &Arc<dyn ExecutionPlan>, b: &Arc<dyn ExecutionPlan>) -> bool {
    std::ptr::eq(Arc::as_ptr(a) as *const (), Arc::as_ptr(b) as *const ())
}

/// RC1: a partitioned join's sides below their repartitions, and how its
/// run goes once the build side is read.
struct Collect {
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<dyn ExecutionPlan>,
    /// the join's partitions, the probe side's
    partitions: usize,
    rows: usize,
    mode: tokio::sync::OnceCell<std::result::Result<Mode, Arc<DataFusionError>>>,
}

impl fmt::Debug for Collect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Collect({} rows)", self.rows)
    }
}

enum Mode {
    /// one table for all partitions, probed by each partition's probe rows
    Shared {
        join: Arc<dyn ExecutionPlan>,
        probes: Arc<StreamsExec>,
        ctx: Arc<TaskContext>,
    },
    /// the build side as planned, its rows read so far ahead of the rest
    Partitioned(Arc<dyn ExecutionPlan>),
}

impl Collect {
    fn of(join: &HashJoinExec, rows: usize) -> Option<Self> {
        let build = below_repartition(&join.left)?;
        let probe = below_repartition(&join.right)?;
        let partitions = join.properties().output_partitioning().partition_count();
        (rows > 0
            && join.mode == PartitionMode::Partitioned
            && partitions > 1
            && probe.output_partitioning().partition_count() == partitions)
            .then(|| Collect {
                build,
                probe,
                partitions,
                rows,
                mode: tokio::sync::OnceCell::new(),
            })
    }

    /// Read the build side, its partitions at once, until it ends or
    /// passes the limits or the pool's room.
    async fn start(
        &self,
        join: &HashJoinExec,
        spec: &JoinSpec,
        ctx: &Arc<TaskContext>,
    ) -> Result<Mode> {
        let reservation = Arc::new(Mutex::new(
            MemoryConsumer::new("SpillingHashJoin collected")
                .with_can_spill(true)
                .register(ctx.memory_pool()),
        ));
        let over = Arc::new(AtomicBool::new(false));
        let rows = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for p in 0..self.build.output_partitioning().partition_count() {
            let mut input = self.build.execute(p, ctx.clone())?;
            let (reservation, over, rows, limit) =
                (reservation.clone(), over.clone(), rows.clone(), self.rows);
            tasks.push(SpawnedTask::spawn(async move {
                let mut batches = Vec::new();
                while !over.load(Ordering::Relaxed) {
                    let Some(batch) = input.next().await else {
                        return Ok((batches, None));
                    };
                    let batch = batch?;
                    let n = batch.num_rows();
                    let size = get_record_batch_memory_size(&batch) + n * HASH_ROW_BYTES;
                    let fits = rows.fetch_add(n, Ordering::Relaxed) + n <= limit && {
                        let r = reservation.lock().unwrap();
                        r.size() + size <= COLLECT_BYTES && r.try_grow(size).is_ok()
                    };
                    batches.push(batch);
                    if !fits {
                        over.store(true, Ordering::Relaxed);
                    }
                }
                Ok::<_, DataFusionError>((batches, Some(input)))
            }));
        }
        let mut read = Vec::new();
        for task in tasks {
            read.push(task.join_unwind().await.map_err(|e| DataFusionError::External(Box::new(e)))??);
        }
        let reservation = Arc::into_inner(reservation).unwrap().into_inner().unwrap();
        if !over.load(Ordering::Relaxed) {
            let batches: Vec<RecordBatch> = read.iter().flat_map(|(b, _)| b.iter().cloned()).collect();
            let rows = batches.iter().map(|b| b.num_rows()).sum();
            let bytes: usize = batches.iter().map(get_record_batch_memory_size).sum();
            // the table and the concatenated copy, as `Run::collect` reserves
            let need = bytes + join_table_bytes(spec, ctx, rows)? + concat_bytes(&batches)?;
            let short = need.saturating_sub(reservation.size());
            if short == 0 || reservation.try_grow(short).is_ok() {
                let probes = Arc::new(StreamsExec::new(self.probe.schema(), self.partitions));
                let build = Box::pin(RecordBatchStreamAdapter::new(
                    spec.left_schema.clone(),
                    futures::stream::iter(batches.into_iter().map(Ok)),
                ));
                let join = HashJoinExec::try_new(
                    Arc::new(OneShotExec::new(build)),
                    probes.clone(),
                    spec.on.clone(),
                    spec.filter.clone(),
                    &spec.join_type,
                    spec.projection.clone(),
                    PartitionMode::CollectLeft,
                    spec.null_equality,
                    false,
                )?;
                return Ok(Mode::Shared {
                    join: Arc::new(join),
                    probes,
                    ctx: held_context(ctx, reservation),
                });
            }
        }
        // The rows read stay unreserved until the partitions collect them
        // again: within COLLECT_BYTES.
        drop(reservation);
        let replay = Arc::new(StreamsExec::new(self.build.schema(), read.len()));
        for (p, (batches, rest)) in read.into_iter().enumerate() {
            let schema = self.build.schema();
            let held = futures::stream::iter(batches.into_iter().map(Ok));
            let stream: SendableRecordBatchStream = match rest {
                Some(rest) => Box::pin(RecordBatchStreamAdapter::new(schema, held.chain(rest))),
                None => Box::pin(RecordBatchStreamAdapter::new(schema, held)),
            };
            replay.put(p, stream);
        }
        let replay: Arc<dyn ExecutionPlan> = replay;
        let left = join
            .left
            .clone()
            .transform_down(|node| {
                Ok(if same(&node, &self.build) {
                    Transformed::new(replay.clone(), true, TreeNodeRecursion::Jump)
                } else {
                    Transformed::no(node)
                })
            })
            .data()?;
        Ok(Mode::Partitioned(left))
    }
}

/// Bytes DataFusion's hash join reserves for the table of a build side
/// of `rows` rows: the estimate of its `new_join_hashmap`, with room for
/// the map's own fields; or, on one integer key, at most what an
/// ArrayMap over the keys' range takes, which it builds instead where
/// the range is small or the keys dense in it, and which may take more.
fn join_table_bytes(spec: &JoinSpec, ctx: &TaskContext, rows: usize) -> Result<usize> {
    let hash = if rows > u32::MAX as usize {
        estimate_memory_size::<(u64, u64)>(rows, 64)? + rows * 8
    } else {
        estimate_memory_size::<(u32, u64)>(rows, 64)? + rows * 4
    };
    let array = match spec.on.as_slice() {
        [(key, _)] if key.data_type(&spec.left_schema)?.is_integer() => {
            let options = &ctx.session_config().options().execution;
            let density = options.perfect_hash_join_min_key_density;
            let range = if density > 0.0 { (rows as f64 / density) as usize } else { usize::MAX };
            let range = range.max(options.perfect_hash_join_small_build_threshold);
            range.saturating_add(1).saturating_add(rows).saturating_mul(4)
        }
        _ => 0,
    };
    Ok(hash.max(array))
}

/// `ctx` with its memory from `held` (see HeldPool).
fn held_context(ctx: &TaskContext, held: MemoryReservation) -> Arc<TaskContext> {
    let mut env = (*ctx.runtime_env()).clone();
    env.memory_pool = Arc::new(HeldPool::new(ctx.memory_pool().clone(), held));
    Arc::new(TaskContext::new(
        ctx.task_id(),
        ctx.session_id(),
        ctx.session_config().clone(),
        ctx.scalar_functions().clone(),
        ctx.higher_order_functions().clone(),
        ctx.aggregate_functions().clone(),
        ctx.window_functions().clone(),
        Arc::new(env),
    ))
}

/// What a join is, to build it again over other inputs.
#[derive(Debug)]
struct JoinSpec {
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    projection: Option<Vec<usize>>,
    null_equality: NullEquality,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    /// the join's output
    schema: SchemaRef,
    /// AJ1: the sides may be swapped once the probe side is known small
    adaptive: bool,
}

impl JoinSpec {
    /// The join over a collected build side and a probe stream, run in
    /// memory by DataFusion.  The join takes its memory from `held` (see
    /// HeldPool).
    fn in_memory(
        &self,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
        ctx: &TaskContext,
        held: MemoryReservation,
    ) -> Result<SendableRecordBatchStream> {
        let ctx = held_context(ctx, held);
        let join = HashJoinExec::try_new(
            Arc::new(OneShotExec::new(build)),
            Arc::new(OneShotExec::new(probe)),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            self.projection.clone(),
            PartitionMode::CollectLeft,
            self.null_equality,
            false,
        )?;
        join.execute(0, ctx)
    }

    /// AJ1: the join over a small probe side, collected, and the build side
    /// streamed past it: DataFusion's own swap of the join's inputs, which
    /// swaps the join type and filter and restores the column order.
    fn swapped(
        &self,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
        ctx: &TaskContext,
        held: MemoryReservation,
    ) -> Result<SendableRecordBatchStream> {
        let ctx = held_context(ctx, held);
        let join = HashJoinExec::try_new(
            Arc::new(OneShotExec::new(build)),
            Arc::new(OneShotExec::new(probe)),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            self.projection.clone(),
            PartitionMode::CollectLeft,
            self.null_equality,
            false,
        )?;
        join.swap_inputs(PartitionMode::CollectLeft)?.execute(0, ctx)
    }
}

/// The memory pool of a join run in memory: it hands out the bytes a
/// reservation of the query's pool already holds, and grows that
/// reservation past them.  A build side reserved its join's memory while it
/// was collected; were the reservation freed for the join to reserve again,
/// another consumer could take the bytes in between, and the join, which
/// cannot spill, finds no room in a pool that consumers which can spill
/// have filled (DataFusion's fair pool limits each of those to its share,
/// not their sum).  What the join takes past them, once its share of the
/// pool has shrunk below, is memory that cannot spill (`extra`): the build
/// side is in (TPC-DS Q80 failed for 68 bytes).  The reservations are freed
/// when the join is.
#[derive(Debug)]
struct HeldPool {
    parent: Arc<dyn MemoryPool>,
    state: Mutex<Held>,
}

#[derive(Debug)]
struct Held {
    /// the build side's reservation, which can spill
    held: MemoryReservation,
    /// grown where `held` may not grow any more, as memory that cannot spill
    extra: MemoryReservation,
    /// the bytes handed out of the two
    used: usize,
}

impl HeldPool {
    fn new(parent: Arc<dyn MemoryPool>, held: MemoryReservation) -> Self {
        let extra = MemoryConsumer::new(format!("{} in memory", held.consumer().name()))
            .register(&parent);
        HeldPool {
            parent,
            state: Mutex::new(Held { held, extra, used: 0 }),
        }
    }
}

impl fmt::Display for HeldPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "held from {}", self.parent)
    }
}

impl MemoryPool for HeldPool {
    fn name(&self) -> &str {
        "held"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock().unwrap();
        state.used += additional;
        let short = state.used.saturating_sub(state.held.size() + state.extra.size());
        state.extra.grow(short);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        // the bytes stay held until the join ends
        self.state.lock().unwrap().used -= shrink;
    }

    fn try_grow(&self, _reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let short = (state.used + additional).saturating_sub(state.held.size() + state.extra.size());
        // Bytes already held are the join's even when the pool's share of
        // a consumer has shrunk below them since.
        if short > 0 && state.held.try_grow(short).is_err() {
            state.extra.try_grow(short)?;
        }
        state.used += additional;
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state.lock().unwrap().used
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.parent.memory_limit()
    }
}

#[derive(Debug)]
pub struct SpillingHashJoinExec {
    join: Arc<dyn ExecutionPlan>,
    spec: Arc<JoinSpec>,
    metrics: ExecutionPlanMetricsSet,
    memory_leaves: bool,
    /// RC1: rows the build side may have to be collected for all
    /// partitions (0: it is not), and what that needs
    collect_rows: usize,
    collect: Option<Arc<Collect>>,
    /// RC1: the join's properties but its partitioning, which the
    /// collected build side does not give
    properties: Arc<PlanProperties>,
}

impl SpillingHashJoinExec {
    fn new(join: Arc<dyn ExecutionPlan>, memory_leaves: bool, collect_rows: usize) -> Result<Self> {
        let Some(hj) = join.downcast_ref::<HashJoinExec>() else {
            return internal_err!("SpillingHashJoinExec wraps a HashJoinExec");
        };
        let spec = JoinSpec {
            on: hj.on.clone(),
            filter: hj.filter.clone(),
            join_type: hj.join_type,
            projection: hj.projection.as_ref().map(|p| p.to_vec()),
            null_equality: hj.null_equality,
            left_schema: hj.left.schema(),
            right_schema: hj.right.schema(),
            schema: join.schema(),
            adaptive: hj.join_type.supports_swap()
                && (memory_leaves || derived(&hj.right))
                && reads_itself(&hj.right, memory_leaves),
        };
        let collect = Collect::of(hj, collect_rows).map(Arc::new);
        let properties = match &collect {
            Some(c) => {
                let p = join.properties();
                Arc::new(PlanProperties::new(
                    p.equivalence_properties().clone(),
                    Partitioning::UnknownPartitioning(c.partitions),
                    p.emission_type,
                    p.boundedness,
                ))
            }
            None => join.properties().clone(),
        };
        Ok(SpillingHashJoinExec {
            join,
            spec: Arc::new(spec),
            metrics: ExecutionPlanMetricsSet::new(),
            memory_leaves,
            collect_rows,
            collect,
            properties,
        })
    }
}

impl DisplayAs for SpillingHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpillingHashJoinExec: ")?;
        if self.collect.is_some() {
            write!(f, "collect_rows={}, ", self.collect_rows)?;
        }
        self.join.fmt_as(t, f)
    }
}

impl ExecutionPlan for SpillingHashJoinExec {
    fn name(&self) -> &str {
        "SpillingHashJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.join.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.join.apply_expressions(f)
    }

    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let join = self.join.clone().with_new_children(children)?;
        Ok(Arc::new(SpillingHashJoinExec::new(join, self.memory_leaves, self.collect_rows)?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let Some(collect) = self.collect.clone() else {
            let join = self.join.downcast_ref::<HashJoinExec>().unwrap();
            let build = join.left.execute(partition, ctx.clone())?;
            let probe = join.right.execute(partition, ctx.clone())?;
            return Ok(self.run(partition, ctx, build, probe));
        };
        let this = SpillingHashJoinExec {
            join: self.join.clone(),
            spec: self.spec.clone(),
            metrics: self.metrics.clone(),
            memory_leaves: self.memory_leaves,
            collect_rows: self.collect_rows,
            collect: None,
            properties: self.properties.clone(),
        };
        let collected = MetricBuilder::new(&self.metrics).counter("collected", partition);
        let stream = futures::stream::once(async move {
            let join = this.join.downcast_ref::<HashJoinExec>().unwrap();
            let mode = collect
                .mode
                .get_or_init(|| async { collect.start(join, &this.spec, &ctx).await.map_err(Arc::new) })
                .await;
            match mode {
                Err(e) => Err(DataFusionError::Shared(e.clone())),
                Ok(Mode::Shared { join, probes, ctx: held }) => {
                    probes.put(partition, collect.probe.execute(partition, ctx.clone())?);
                    collected.add(1);
                    join.execute(partition, held.clone())
                }
                Ok(Mode::Partitioned(left)) => {
                    let build = left.execute(partition, ctx.clone())?;
                    let probe = join.right.execute(partition, ctx.clone())?;
                    Ok(this.run(partition, ctx, build, probe))
                }
            }
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl SpillingHashJoinExec {
    /// Partition `partition` of the join of `build` and `probe`.
    fn run(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let run = Run {
            spec: self.spec.clone(),
            ctx: ctx.clone(),
            build_spill: SpillManager::new(
                ctx.runtime_env(),
                SpillMetrics::new(&self.metrics, partition),
                self.spec.left_schema.clone(),
            ),
            probe_spill: SpillManager::new(
                ctx.runtime_env(),
                SpillMetrics::new(&self.metrics, partition),
                self.spec.right_schema.clone(),
            ),
            partition,
            swapped: MetricBuilder::new(&self.metrics).counter("swapped", partition),
        };
        let stream = futures::stream::once(Arc::new(run).start(build, probe)).try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(self.schema(), stream))
    }
}

/// One partition of a SpillingHashJoinExec.
struct Run {
    spec: Arc<JoinSpec>,
    ctx: Arc<TaskContext>,
    build_spill: SpillManager,
    probe_spill: SpillManager,
    partition: usize,
    /// AJ1: 1 once the sides are swapped
    swapped: Count,
}

/// A build side read so far: the batches held, and the rest of it if it did
/// not fit.
enum Collected {
    All(Vec<RecordBatch>, MemoryReservation),
    Overflow(Vec<RecordBatch>, SendableRecordBatchStream),
}

impl Run {
    fn reservation(&self, depth: u32) -> MemoryReservation {
        MemoryConsumer::new(format!(
            "SpillingHashJoin[{}] depth {depth}",
            self.partition
        ))
        .with_can_spill(true)
        .register(self.ctx.memory_pool())
    }

    /// Read a build side while it fits.  Each batch reserves its size and
    /// HASH_ROW_BYTES per row, room for the hash table DataFusion builds over
    /// it (a slot of the map, up to twice the rows at its load factor, and a
    /// link per row), so that the in-memory join finds its memory in this
    /// reservation, which it takes over.
    async fn collect(&self, mut build: SendableRecordBatchStream, depth: u32) -> Result<Collected> {
        let reservation = self.reservation(depth);
        let mut batches = Vec::new();
        let mut bytes = 0;
        while let Some(batch) = build.next().await {
            let batch = batch?;
            let size = get_record_batch_memory_size(&batch);
            bytes += size;
            if reservation
                .try_grow(size + batch.num_rows() * HASH_ROW_BYTES)
                .is_err()
            {
                drop(reservation);
                batches.push(batch);
                return Ok(Collected::Overflow(batches, build));
            }
            batches.push(batch);
        }
        // The join reserves its table, and the copy it concatenates the
        // batches into while it holds them (apache/datafusion#25496): a build
        // side fits only with room for those too, which HASH_ROW_BYTES a row
        // may not have left.
        let rows = batches.iter().map(|b| b.num_rows()).sum();
        let need = bytes + self.join_table_bytes(rows)? + concat_bytes(&batches)?;
        let short = need.saturating_sub(reservation.size());
        if short > 0 && reservation.try_grow(short).is_err() {
            drop(reservation);
            let rest = self.empty(&self.spec.left_schema);
            return Ok(Collected::Overflow(batches, rest));
        }
        Ok(Collected::All(batches, reservation))
    }

    async fn start(
        self: Arc<Self>,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        if !self.spec.adaptive {
            return self.join_parts(build, Some(probe), 0, 0).await;
        }
        // AJ1: the probe side first, while it is small
        let (small, rows, rest, held) = self.read_up_to(probe, PEEK_ROWS).await?;
        let probe = self.stream_of(&self.spec.right_schema, small);
        let Some(held) = held else {
            let probe = Box::pin(RecordBatchStreamAdapter::new(
                self.spec.right_schema.clone(),
                probe.chain(rest),
            ));
            return self.join_parts(build, Some(probe), 0, 0).await;
        };
        // a build side no larger keeps its role
        let (first, _, rest, build_held) = self.read_up_to(build, rows + 1).await?;
        let build = self.stream_of(&self.spec.left_schema, first);
        if build_held.is_some() {
            return self.join_parts(build, Some(probe), 0, 0).await;
        }
        let build = Box::pin(RecordBatchStreamAdapter::new(
            self.spec.left_schema.clone(),
            build.chain(rest),
        ));
        self.swapped.add(1);
        self.spec.swapped(build, probe, &self.ctx, held)
    }

    /// AJ1: a side's first batches, until they pass `limit` rows or
    /// PEEK_BYTES or the pool's room: their rows, the rest of the side, and
    /// the reservation they are held under when the side ended within them
    /// (None when it did not).
    async fn read_up_to(
        &self,
        mut input: SendableRecordBatchStream,
        limit: usize,
    ) -> Result<(Vec<RecordBatch>, usize, SendableRecordBatchStream, Option<MemoryReservation>)> {
        let reservation = MemoryConsumer::new(format!(
            "SpillingHashJoin[{}] small side",
            self.partition
        ))
        .register(self.ctx.memory_pool());
        let mut batches = Vec::new();
        let mut rows = 0;
        while let Some(batch) = input.next().await {
            let batch = batch?;
            rows += batch.num_rows();
            let size = get_record_batch_memory_size(&batch) + batch.num_rows() * HASH_ROW_BYTES;
            let fits = rows <= limit
                && reservation.size() + size <= PEEK_BYTES
                && reservation.try_grow(size).is_ok();
            batches.push(batch);
            if !fits {
                return Ok((batches, rows, input, None));
            }
        }
        Ok((batches, rows, input, Some(reservation)))
    }

    fn join_table_bytes(&self, rows: usize) -> Result<usize> {
        join_table_bytes(&self.spec, &self.ctx, rows)
    }

    /// Join a build side with a probe side (None: empty), splitting both by
    /// hash seeded with `seed` when the build side does not fit.
    fn join_parts(
        self: Arc<Self>,
        build: SendableRecordBatchStream,
        probe: Option<SendableRecordBatchStream>,
        depth: u32,
        seed: u64,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        async move {
            let probe = probe.unwrap_or_else(|| self.empty(&self.spec.right_schema));
            match self.collect(build, depth).await? {
                Collected::All(batches, reservation) => {
                    let build = self.stream_of(&self.spec.left_schema, batches);
                    self.spec.in_memory(build, probe, &self.ctx, reservation)
                }
                Collected::Overflow(held, rest) if depth >= MAX_DEPTH => {
                    // Rows of one key that do not fit cannot be split: the
                    // in-memory join takes them if the pool has the room,
                    // or fails as DataFusion's would, reserving as a
                    // consumer that cannot spill, past a spiller's share.
                    let held = self.stream_of(&self.spec.left_schema, held);
                    let build = Box::pin(RecordBatchStreamAdapter::new(
                        self.spec.left_schema.clone(),
                        held.chain(rest),
                    ));
                    let reservation = MemoryConsumer::new(format!(
                        "SpillingHashJoin[{}] in memory",
                        self.partition
                    ))
                    .register(self.ctx.memory_pool());
                    self.spec.in_memory(build, probe, &self.ctx, reservation)
                }
                Collected::Overflow(held, rest) => {
                    let seed = seed + 1;
                    let held = self.stream_of(&self.spec.left_schema, held);
                    let build = held.chain(rest);
                    let builds = self
                        .split(
                            Box::pin(RecordBatchStreamAdapter::new(
                                self.spec.left_schema.clone(),
                                build,
                            )),
                            true,
                            seed,
                        )
                        .await?;
                    let probes = self.split(probe, false, seed).await?;
                    let this = self.clone();
                    let buckets = builds.into_iter().zip(probes).filter_map(|(b, p)| {
                        // a bucket of neither side gives no rows
                        (b.is_some() || p.is_some()).then_some((b, p))
                    });
                    let stream = futures::stream::iter(buckets.collect::<Vec<_>>())
                        .then(move |(b, p)| {
                            let this = this.clone();
                            async move {
                                let build = match b {
                                    Some(f) => this.read(&this.build_spill, f)?,
                                    None => this.empty(&this.spec.left_schema),
                                };
                                let probe = match p {
                                    Some(f) => Some(this.read(&this.probe_spill, f)?),
                                    None => None,
                                };
                                this.join_parts(build, probe, depth + 1, seed).await
                            }
                        })
                        .try_flatten();
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        self.spec.schema.clone(),
                        stream,
                    )) as SendableRecordBatchStream)
                }
            }
        }
        .boxed()
    }

    /// Write a side's rows into FANOUT spill files by their keys' hash.
    async fn split(
        &self,
        mut input: SendableRecordBatchStream,
        build: bool,
        seed: u64,
    ) -> Result<Vec<Option<Arc<dyn SpillFile>>>> {
        let manager = if build {
            &self.build_spill
        } else {
            &self.probe_spill
        };
        let state = RandomState::with_seed(seed);
        let schema = input.schema();
        let batch_rows = self.ctx.session_config().batch_size();
        let mut files: Vec<_> = (0..FANOUT).map(|_| None).collect();
        // A bucket gets a sixteenth of each batch: its rows are gathered and
        // written as batches of a useful size.
        let mut parts: Vec<(Vec<RecordBatch>, usize, usize)> =
            (0..FANOUT).map(|_| (Vec::new(), 0, 0)).collect();
        let reservation = MemoryConsumer::new(format!(
            "SpillingHashJoin[{}] split",
            self.partition
        ))
        .register(self.ctx.memory_pool());
        let flush = |files: &mut Vec<Option<_>>,
                     part: &mut (Vec<RecordBatch>, usize, usize),
                     b: usize|
         -> Result<()> {
            if part.0.is_empty() {
                return Ok(());
            }
            let batch = concat_batches(&schema, &part.0)?;
            if files[b].is_none() {
                files[b] = Some(manager.create_in_progress_file("SpillingHashJoin")?);
            }
            files[b].as_mut().unwrap().append_batch(&batch)?;
            reservation.shrink(part.2);
            *part = (Vec::new(), 0, 0);
            Ok(())
        };
        let mut hashes = Vec::new();
        while let Some(batch) = input.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let keys = self
                .spec
                .on
                .iter()
                .map(|(l, r)| {
                    (if build { l } else { r })
                        .evaluate(&batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .collect::<Result<Vec<_>>>()?;
            hashes.clear();
            hashes.resize(batch.num_rows(), 0);
            create_hashes(&keys, &state, &mut hashes)?;
            let mut rows: Vec<Vec<u32>> = vec![Vec::new(); FANOUT];
            for (i, h) in hashes.iter().enumerate() {
                rows[(*h as usize) % FANOUT].push(i as u32);
            }
            for (b, idx) in rows.into_iter().enumerate() {
                if idx.is_empty() {
                    continue;
                }
                let part = take_record_batch(&batch, &UInt32Array::from(idx))?;
                let size = get_record_batch_memory_size(&part);
                // a few batches' worth at most, which the pool cannot refuse
                reservation.grow(size);
                let bucket = &mut parts[b];
                bucket.1 += part.num_rows();
                bucket.2 += size;
                bucket.0.push(part);
                if bucket.1 >= batch_rows || bucket.2 >= BUCKET_BATCH_BYTES {
                    flush(&mut files, bucket, b)?;
                }
            }
        }
        for (b, part) in parts.iter_mut().enumerate() {
            flush(&mut files, part, b)?;
        }
        files
            .into_iter()
            .map(|f| match f {
                Some(mut f) => f.finish(),
                None => Ok(None),
            })
            .collect()
    }

    /// A bucket's rows.  The batches read from a spill file share the
    /// chunks the file is read in, and each would count a whole chunk as its
    /// own memory (a batch of twenty rows, 128 KB): copied, a batch counts
    /// its rows, and the chunk is freed.
    fn read(
        &self,
        manager: &SpillManager,
        file: Arc<dyn SpillFile>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = manager.read_spill_as_stream(file, None)?;
        let schema = stream.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream.and_then(|batch| async move {
                let all = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
                Ok(take_record_batch(&batch, &all)?)
            }),
        )))
    }

    fn stream_of(
        &self,
        schema: &SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(batches.into_iter().map(Ok)),
        ))
    }

    fn empty(&self, schema: &SchemaRef) -> SendableRecordBatchStream {
        self.stream_of(schema, Vec::new())
    }
}

/// A stream as a plan of one partition, executed once.
struct OneShotExec {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for OneShotExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OneShotExec")
    }
}

impl OneShotExec {
    fn new(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();
        OneShotExec {
            stream: Mutex::new(Some(stream)),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }
}

impl DisplayAs for OneShotExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OneShotExec")
    }
}

impl ExecutionPlan for OneShotExec {
    fn name(&self) -> &str {
        "OneShotExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        match self.stream.lock().unwrap().take() {
            Some(stream) => Ok(stream),
            None => internal_err!("OneShotExec executed twice"),
        }
    }
}

/// Streams as a plan of a partition each, each executed once: put a
/// partition's stream before it is executed.
struct StreamsExec {
    streams: Mutex<Vec<Option<SendableRecordBatchStream>>>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for StreamsExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StreamsExec")
    }
}

impl StreamsExec {
    fn new(schema: SchemaRef, partitions: usize) -> Self {
        StreamsExec {
            streams: Mutex::new((0..partitions).map(|_| None).collect()),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }

    fn put(&self, partition: usize, stream: SendableRecordBatchStream) {
        self.streams.lock().unwrap()[partition] = Some(stream);
    }
}

impl DisplayAs for StreamsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamsExec")
    }
}

impl ExecutionPlan for StreamsExec {
    fn name(&self) -> &str {
        "StreamsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        match self.streams.lock().unwrap().get_mut(partition).and_then(Option::take) {
            Some(stream) => Ok(stream),
            None => internal_err!("StreamsExec partition {partition} not put or executed twice"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::datasource::MemTable;
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::memory_pool::FairSpillPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};

    /// Two tables of int keys with duplicates, NULLs and one hot key, in
    /// several batches.
    fn table(n: i32, step: i32, nulls: i32, hot: i32) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("k2", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batches = (0..4)
            .map(|b| {
                let ids = (b * n / 4)..((b + 1) * n / 4);
                let k = Int32Array::from_iter(ids.clone().map(|i| {
                    if i % nulls == 0 {
                        None
                    } else if i % hot == 0 {
                        Some(7)
                    } else {
                        Some((i * step) % (n / 3))
                    }
                }));
                let k2 = Int32Array::from_iter_values(ids.clone().map(|i| i % 5));
                let v = Int32Array::from_iter_values(ids.map(|i| i % 1000));
                RecordBatch::try_new(schema.clone(), vec![Arc::new(k), Arc::new(k2), Arc::new(v)])
                    .unwrap()
            })
            .collect::<Vec<_>>();
        Arc::new(MemTable::try_new(schema, vec![batches]).unwrap())
    }

    /// A context of 4 partitions; with `limit`, a pool of that many bytes and
    /// the spilling joins.
    fn context(limit: Option<usize>) -> SessionContext {
        context_with(limit, false, 12000)
    }

    /// `context`, the spilling joins swapping sides over in-memory tables
    /// too (AJ1) when `memory_leaves`, and `r` of `r_rows` rows.
    fn context_with(limit: Option<usize>, memory_leaves: bool, r_rows: i32) -> SessionContext {
        // AJ1's tests swap sides that RC1 would collect first
        let collect_rows = if memory_leaves { 0 } else { COLLECT_ROWS };
        context_rc(limit, memory_leaves, r_rows, collect_rows)
    }

    /// `context_with`, the build sides collected for all partitions up to
    /// `collect_rows` rows (RC1).
    fn context_rc(
        limit: Option<usize>,
        memory_leaves: bool,
        r_rows: i32,
        collect_rows: usize,
    ) -> SessionContext {
        let mut config = SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(1024);
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        // the sides as written, as the extension's sessions keep them
        config.options_mut().optimizer.join_reordering = false;
        let mut runtime = RuntimeEnvBuilder::new().with_disk_manager_builder(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(vec![std::env::temp_dir()])),
        );
        if let Some(limit) = limit {
            runtime = runtime.with_memory_pool(Arc::new(FairSpillPool::new(limit)));
        }
        let mut state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime.build_arc().unwrap())
            .with_default_features();
        if limit.is_some() {
            state = state.with_physical_optimizer_rule(Arc::new(SpillHashJoins {
                memory_leaves,
                collect_rows,
            }));
        }
        let ctx = SessionContext::new_with_state(state.build());
        ctx.register_table("l", table(20000, 7, 97, 10)).unwrap();
        ctx.register_table("r", table(r_rows, 11, 89, 23)).unwrap();
        ctx
    }

    /// Join `l` and `r` (columns renamed lk, lk2, lv and rk, rk2, rv) and
    /// count the rows: query `i` of the test's.
    async fn query(ctx: &SessionContext, i: usize) -> DataFrame {
        let (join, aggs) = joined(ctx, i).await;
        join.aggregate(vec![], aggs).unwrap()
    }

    /// The join of query `i`, and the aggregates that count its rows.
    async fn joined(ctx: &SessionContext, i: usize) -> (DataFrame, Vec<datafusion::prelude::Expr>) {
        use datafusion::functions_aggregate::expr_fn::{count, sum};
        use datafusion::prelude::{col, lit};
        let l = ctx
            .table("l")
            .await
            .unwrap()
            .select(vec![
                col("k").alias("lk"),
                col("k2").alias("lk2"),
                col("v").alias("lv"),
            ])
            .unwrap();
        let r = ctx
            .table("r")
            .await
            .unwrap()
            // in another order than l's, so that each side's keys are its own
            .select(vec![
                col("v").alias("rv"),
                col("k2").alias("rk2"),
                col("k").alias("rk"),
            ])
            .unwrap();
        // a build side of a few keys, most buckets of which get no build row
        let l = if i >= 12 {
            l.filter(col("lk").lt(lit(3)).or(col("lk").eq(lit(7))))
                .unwrap()
        } else {
            l
        };
        let (jt, keys, filter, aggs) = match i {
            0 => (
                JoinType::Inner,
                1,
                None,
                vec![count(lit(1)), sum(col("lv")), sum(col("rv"))],
            ),
            1 => (
                JoinType::Left,
                1,
                None,
                vec![count(lit(1)), count(col("rk")), sum(col("lv"))],
            ),
            2 => (
                JoinType::Right,
                1,
                None,
                vec![count(lit(1)), count(col("lk")), sum(col("rv"))],
            ),
            3 => (
                JoinType::Full,
                1,
                None,
                vec![count(lit(1)), count(col("lk")), count(col("rk"))],
            ),
            4 => (
                JoinType::LeftSemi,
                1,
                None,
                vec![count(lit(1)), sum(col("lv"))],
            ),
            5 => (
                JoinType::LeftAnti,
                1,
                None,
                vec![count(lit(1)), sum(col("lv"))],
            ),
            6 => (
                JoinType::RightSemi,
                1,
                None,
                vec![count(lit(1)), sum(col("rv"))],
            ),
            7 => (
                JoinType::RightAnti,
                1,
                None,
                vec![count(lit(1)), sum(col("rv"))],
            ),
            8 => (
                JoinType::Inner,
                1,
                Some(col("lv").gt(col("rv"))),
                vec![count(lit(1)), sum(col("lv"))],
            ),
            9 => (
                JoinType::Left,
                1,
                Some(col("lv").lt(col("rv"))),
                vec![count(lit(1)), count(col("rk"))],
            ),
            10 => (
                JoinType::LeftAnti,
                1,
                Some(col("lv").lt(col("rv"))),
                vec![count(lit(1))],
            ),
            11 => (
                JoinType::Inner,
                2,
                None,
                vec![count(lit(1)), sum(col("rv"))],
            ),
            12 => (
                JoinType::Right,
                1,
                None,
                vec![count(lit(1)), count(col("lk")), sum(col("rv"))],
            ),
            13 => (
                JoinType::Full,
                1,
                None,
                vec![count(lit(1)), count(col("lk")), count(col("rk"))],
            ),
            _ => (
                JoinType::RightAnti,
                1,
                None,
                vec![count(lit(1)), sum(col("rv"))],
            ),
        };
        let (lk, rk): (&[&str], &[&str]) = if keys == 1 {
            (&["lk"], &["rk"])
        } else {
            (&["lk", "lk2"], &["rk", "rk2"])
        };
        (l.join(r, jt, lk, rk, filter).unwrap(), aggs)
    }

    /// The query's rows as text, sorted, and how many spills its joins made.
    async fn run(ctx: &SessionContext, i: usize) -> (Vec<String>, usize) {
        let plan = query(ctx, i).await.create_physical_plan().await.unwrap();
        let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
            .await
            .unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            for r in 0..b.num_rows() {
                let row = (0..b.num_columns())
                    .map(|c| format!("{:?}", ScalarValue::try_from_array(b.column(c), r).unwrap()))
                    .collect::<Vec<_>>();
                rows.push(row.join("|"));
            }
        }
        rows.sort();
        fn spills(plan: &Arc<dyn ExecutionPlan>) -> usize {
            let own = if plan.name() == "SpillingHashJoinExec" {
                plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0)
            } else {
                0
            };
            own + plan.children().into_iter().map(spills).sum::<usize>()
        }
        (rows, spills(&plan))
    }

    /// Every join type, with and without a filter, on one key and on two,
    /// gives the same rows spilling under a 256 kB pool as in memory.
    #[test]
    fn spilling_joins_give_the_rows_of_joins_in_memory() {
        crate::runtime::init(2).unwrap();
        let handle = crate::runtime::handle().unwrap();
        handle.block_on(async {
            let plain = context(None);
            // the build sides partitioned as planned, of a few keys too
            let small = context_rc(Some(256 * 1024), false, 12000, 0);
            for i in 0..15 {
                let (want, _) = run(&plain, i).await;
                let (got, spills) = run(&small, i).await;
                assert_eq!(got, want, "query {i}");
                // the small pool made each join spill
                assert!(spills > 0, "query {i} did not spill");
            }
        });
    }

    /// RC1: the partitions that probed a build side collected for all.
    fn collected(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let own = if plan.name() == "SpillingHashJoinExec" {
            plan.metrics()
                .and_then(|m| m.sum_by_name("collected"))
                .map_or(0, |v| v.as_usize())
        } else {
            0
        };
        own + plan.children().into_iter().map(collected).sum::<usize>()
    }

    /// The rows of `plan`, as text, sorted.
    async fn rows_of(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Vec<String> {
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await.unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            for r in 0..b.num_rows() {
                let row = (0..b.num_columns())
                    .map(|c| format!("{:?}", ScalarValue::try_from_array(b.column(c), r).unwrap()))
                    .collect::<Vec<_>>();
                rows.push(row.join("|"));
            }
        }
        rows.sort();
        rows
    }

    /// RC1: every join type, with and without a filter, gives the same rows
    /// with its build side collected once for all probe partitions, and
    /// with one too large for that read ahead and partitioned after all.
    #[test]
    fn collected_build_sides_give_the_rows_of_joins_in_memory() {
        crate::runtime::init(2).unwrap();
        let handle = crate::runtime::handle().unwrap();
        handle.block_on(async {
            let plain = context(None);
            let collect = context_rc(Some(1 << 30), false, 12000, COLLECT_ROWS);
            let over = context_rc(Some(1 << 30), false, 12000, 3000);
            for i in 0..15 {
                let (want, _) = run(&plain, i).await;
                let plan = query(&collect, i).await.create_physical_plan().await.unwrap();
                assert_eq!(rows_of(plan.clone(), &collect).await, want, "query {i} collected");
                assert_eq!(collected(&plan), 4, "query {i} was not collected:\n{}",
                    datafusion::physical_plan::displayable(plan.as_ref()).indent(true));
                let plan = query(&over, i).await.create_physical_plan().await.unwrap();
                assert_eq!(rows_of(plan.clone(), &over).await, want, "query {i} past the rows");
                // queries 12-14 have a build side of a few keys
                if i < 12 {
                    assert_eq!(collected(&plan), 0, "query {i} collected past its rows");
                }
            }
        });
    }

    /// RC1: a join whose rows a node above takes as partitioned by its keys
    /// (an aggregate grouped by them) keeps hash-partitioning its sides.
    #[test]
    fn joins_partitioned_for_the_node_above_stay_partitioned() {
        use datafusion::functions_aggregate::expr_fn::count;
        use datafusion::prelude::{col, lit};
        crate::runtime::init(2).unwrap();
        let handle = crate::runtime::handle().unwrap();
        handle.block_on(async {
            let plain = context(None);
            let collect = context_rc(Some(1 << 30), false, 12000, COLLECT_ROWS);
            for ctx in [&plain, &collect] {
                let (join, _) = joined(ctx, 0).await;
                let df = join.aggregate(vec![col("lk")], vec![count(lit(1))]).unwrap();
                let plan = df.create_physical_plan().await.unwrap();
                let text = datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
                let rows = rows_of(plan.clone(), ctx).await;
                assert_eq!(rows.len(), 4000, "{text}");
                if std::ptr::eq(ctx, &collect) {
                    assert!(!text.contains("collect_rows"), "{text}");
                    assert_eq!(collected(&plan), 0);
                }
            }
        });
    }

    /// AJ1: the times the plan's spilling joins swapped sides.
    fn swaps(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let own = if plan.name() == "SpillingHashJoinExec" {
            plan.metrics()
                .and_then(|m| m.sum_by_name("swapped"))
                .map_or(0, |v| v.as_usize())
        } else {
            0
        };
        own + plan.children().into_iter().map(swaps).sum::<usize>()
    }

    /// AJ1: every join type, with and without a filter, gives the same rows
    /// with its sides swapped, a small probe side (300 rows of r) collected
    /// and the build side streamed past it, as in memory (queries 12-14,
    /// of a few build keys, swap or not by partition).
    #[test]
    fn swapped_joins_give_the_rows_of_joins_in_memory() {
        crate::runtime::init(2).unwrap();
        let handle = crate::runtime::handle().unwrap();
        handle.block_on(async {
            let plain = context_with(None, false, 300);
            let adaptive = context_with(Some(1 << 30), true, 300);
            for i in 0..15 {
                let (want, _) = run(&plain, i).await;
                let plan = query(&adaptive, i).await.create_physical_plan().await.unwrap();
                let batches =
                    datafusion::physical_plan::collect(plan.clone(), adaptive.task_ctx())
                        .await
                        .unwrap();
                let mut got = Vec::new();
                for b in &batches {
                    for r in 0..b.num_rows() {
                        let row = (0..b.num_columns())
                            .map(|c| {
                                format!("{:?}", ScalarValue::try_from_array(b.column(c), r).unwrap())
                            })
                            .collect::<Vec<_>>();
                        got.push(row.join("|"));
                    }
                }
                got.sort();
                assert_eq!(got, want, "query {i}");
                if i < 12 {
                    assert!(swaps(&plan) > 0, "query {i} kept its sides");
                }
            }
        });
    }

    /// AJ1: a probe side past PEEK_ROWS in a partition keeps its role, the
    /// rows read ahead joined first.
    #[test]
    fn large_probe_sides_keep_their_role() {
        crate::runtime::init(2).unwrap();
        let handle = crate::runtime::handle().unwrap();
        handle.block_on(async {
            let plain = context_with(None, false, 600_000);
            let adaptive = context_with(Some(1 << 30), true, 600_000);
            for i in 4..8 {
                let (want, _) = run(&plain, i).await;
                let (got, _) = run(&adaptive, i).await;
                assert_eq!(got, want, "query {i}");
                let plan = query(&adaptive, i).await.create_physical_plan().await.unwrap();
                let _ = datafusion::physical_plan::collect(plan.clone(), adaptive.task_ctx())
                    .await
                    .unwrap();
                assert_eq!(swaps(&plan), 0, "query {i}");
            }
        });
    }

    /// A build side split once fits in its buckets, and each row is
    /// spilled once: read back from the spill files, the rows of a bucket
    /// count the memory they take, not the chunks of the file they share.
    #[test]
    fn rows_are_spilled_once_when_their_buckets_fit() {
        use datafusion::prelude::col;
        crate::runtime::init(2).unwrap();
        let handle = crate::runtime::handle().unwrap();
        handle.block_on(async {
            let distinct = |n: i32| {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int32, false),
                    Field::new("v", DataType::Int32, false),
                ]));
                let batches = (0..n / 1024)
                    .map(|b| {
                        let ids = (b * 1024)..((b + 1) * 1024);
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![
                                Arc::new(Int32Array::from_iter_values(ids.clone())),
                                Arc::new(Int32Array::from_iter_values(ids.map(|i| i % 7))),
                            ],
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>();
                Arc::new(MemTable::try_new(schema, vec![batches]).unwrap())
            };
            // Build rows take 48 bytes reserved: 1.2 MB a partition, 77 kB
            // a bucket, under a share of the 1 MB pool of some 200 kB.  A
            // bucket's dozen batches would count 45 kB each, its whole file.
            let ctx = context(Some(1 << 20));
            ctx.register_table("b", distinct(102400)).unwrap();
            ctx.register_table("p", distinct(409600)).unwrap();
            let b = ctx.table("b").await.unwrap()
                .select(vec![col("k").alias("bk"), col("v").alias("bv")]).unwrap();
            let p = ctx.table("p").await.unwrap();
            let plan = b
                .join(p, JoinType::Inner, &["bk"], &["k"], None)
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
                .await
                .unwrap();
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 102400);
            fn spilled(plan: &Arc<dyn ExecutionPlan>) -> usize {
                let own = if plan.name() == "SpillingHashJoinExec" {
                    plan.metrics().and_then(|m| m.spilled_rows()).unwrap_or(0)
                } else {
                    0
                };
                own + plan.children().into_iter().map(spilled).sum::<usize>()
            }
            assert_eq!(spilled(&plan), 102400 + 409600);
        });
    }

    /// Join a build side of `rows` rows, keys `key(i)` and `cols` int
    /// columns in all, in batches of 1024 rows, with itself, under a pool
    /// with room for what the build side reserves a batch at a time and
    /// little more; return how many times the join spilled.
    fn join_under_a_tight_pool(rows: i32, cols: usize, key: fn(i32) -> i32) -> usize {
        use datafusion::prelude::col;
        crate::runtime::init(2).unwrap();
        let handle = crate::runtime::handle().unwrap();
        handle.block_on(async {
            let schema = Arc::new(Schema::new(
                (0..cols)
                    .map(|c| Field::new(format!("c{c}"), DataType::Int32, false))
                    .collect::<Vec<_>>(),
            ));
            let batches = (0..(rows + 1023) / 1024)
                .map(|b| {
                    let ids = (b * 1024)..((b + 1) * 1024).min(rows);
                    let columns = (0..cols)
                        .map(|c| {
                            Arc::new(Int32Array::from_iter_values(
                                ids.clone().map(|i| if c == 0 { key(i) } else { i % 7 }),
                            )) as _
                        })
                        .collect();
                    RecordBatch::try_new(schema.clone(), columns).unwrap()
                })
                .collect::<Vec<_>>();
            let collected: usize = batches
                .iter()
                .map(|b| get_record_batch_memory_size(b) + b.num_rows() * HASH_ROW_BYTES)
                .sum();
            let limit = collected + (rows as usize);
            let table = || Arc::new(MemTable::try_new(schema.clone(), vec![batches.clone()]).unwrap());
            let config = SessionConfig::new().with_target_partitions(1).with_batch_size(1024);
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(FairSpillPool::new(limit)))
                .with_disk_manager_builder(
                    DiskManagerBuilder::default()
                        .with_mode(DiskManagerMode::Directories(vec![std::env::temp_dir()])),
                )
                .build_arc()
                .unwrap();
            let state = SessionStateBuilder::new()
                .with_config(config)
                .with_runtime_env(runtime)
                .with_default_features()
                .with_physical_optimizer_rule(Arc::new(SpillHashJoins::default()))
                .build();
            let ctx = SessionContext::new_with_state(state);
            ctx.register_table("b", table()).unwrap();
            ctx.register_table("p", table()).unwrap();
            let b = ctx.table("b").await.unwrap()
                .select((0..cols).map(|c| col(format!("c{c}")).alias(format!("b{c}"))).collect::<Vec<_>>())
                .unwrap();
            let p = ctx.table("p").await.unwrap();
            let plan = b
                .join(p, JoinType::Inner, &["b0"], &["c0"], None)
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
                .await
                .unwrap();
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), rows as usize);
            fn spills(plan: &Arc<dyn ExecutionPlan>) -> usize {
                let own = if plan.name() == "SpillingHashJoinExec" {
                    plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0)
                } else {
                    0
                };
                own + plan.children().into_iter().map(spills).sum::<usize>()
            }
            spills(&plan)
        })
    }

    /// A build side of several batches fits only with room for the copy the
    /// join concatenates them into: one that fits without it spills, rather
    /// than leave the copy to a pool that has no room for it.
    #[test]
    fn a_build_side_fits_with_its_concatenated_copy() {
        // 8 bytes a row and 40 for the table, which takes 38 (twice as many
        // slots as rows, at its load factor), and the copy 8 more
        assert!(join_under_a_tight_pool(8192, 2, |i| i) > 0, "the build side did not spill");
    }

    /// Keys dense enough in their range make the join build an array over
    /// the range, which may take more than a hash table: a build side fits
    /// only with room for that.
    #[test]
    fn a_build_side_fits_with_an_array_over_its_keys() {
        // a hash table takes 24 bytes a row (its slots barely more than the
        // rows), an array over keys 6.5 apart 30 and its copy 12 more: past
        // the 40 a row reserved
        assert!(join_under_a_tight_pool(14000, 3, |i| i * 13 / 2) > 0, "the build side did not spill");
    }

    /// The join's memory comes out of the reservation first, although the
    /// pool would not grant the reservation as much any more; past it, out
    /// of the pool as memory that cannot spill.
    #[test]
    fn held_bytes_stay_the_joins() {
        let parent: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(1000));
        let spiller = || MemoryConsumer::new("s").with_can_spill(true);
        let held = spiller().register(&parent);
        held.try_grow(600).unwrap();
        // two more spillers: a share is a third of the pool
        let _others = (spiller().register(&parent), spiller().register(&parent));
        let pool: Arc<dyn MemoryPool> = Arc::new(HeldPool::new(parent.clone(), held));
        let join = MemoryConsumer::new("join").register(&pool);
        join.try_grow(500).unwrap();
        join.try_grow(100).unwrap();
        assert_eq!(parent.reserved(), 600);
        join.try_grow(300).unwrap();
        assert_eq!(parent.reserved(), 900);
        assert!(join.try_grow(101).is_err(), "the pool is full");
        drop(join);
        drop(pool);
        assert_eq!(parent.reserved(), 0);
    }

    /// A join that fits runs in memory although consumers that can spill
    /// have taken the whole pool: it joins in the memory its build side
    /// reserved while collected.
    #[test]
    fn a_join_that_fits_keeps_its_memory() {
        crate::runtime::init(2).unwrap();
        let handle = crate::runtime::handle().unwrap();
        handle.block_on(async {
            let limit = 64 << 20;
            let plain = context(None);
            let full = context(Some(limit));
            // the one consumer when it grew: its share was the whole pool
            let hog = MemoryConsumer::new("hog")
                .with_can_spill(true)
                .register(full.task_ctx().memory_pool());
            hog.try_grow(limit).unwrap();
            // The joins' rows, counted and summed here: an aggregate above
            // them reserves for its accumulators when it starts
            // (apache/datafusion#24692), which a full pool does not grant.
            async fn rows(ctx: &SessionContext, i: usize) -> (Vec<(usize, i64)>, usize) {
                let plan = joined(ctx, i).await.0.create_physical_plan().await.unwrap();
                let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
                    .await
                    .unwrap();
                let schema = plan.schema();
                let sums = (0..schema.fields().len())
                    .map(|c| {
                        batches.iter().fold((0, 0), |(n, sum), b| {
                            let a = b.column(c).as_any().downcast_ref::<Int32Array>().unwrap();
                            (n + a.len() - a.null_count(), sum + a.iter().flatten().map(i64::from).sum::<i64>())
                        })
                    })
                    .collect();
                fn spills(plan: &Arc<dyn ExecutionPlan>) -> usize {
                    let own = if plan.name() == "SpillingHashJoinExec" {
                        plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0)
                    } else {
                        0
                    };
                    own + plan.children().into_iter().map(spills).sum::<usize>()
                }
                (sums, spills(&plan))
            }
            for i in [0, 3, 5] {
                let (want, _) = rows(&plain, i).await;
                let (got, spills) = rows(&full, i).await;
                assert_eq!(got, want, "query {i}");
                assert_eq!(spills, 0, "query {i} spilled");
            }
        });
    }
}
