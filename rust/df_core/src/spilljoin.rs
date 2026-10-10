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

use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::compute::{concat_batches, take_record_batch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::hash_utils::{create_hashes, RandomState};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_err, NullEquality, Result};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::{SpillFile, TaskContext};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, PhysicalExprRef,
};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::{get_record_batch_memory_size, SpillManager};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
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

/// Replaces each partitioned HashJoinExec (and one reading single
/// partitions) by a SpillingHashJoinExec.
#[derive(Debug, Default)]
pub struct SpillHashJoins;

impl PhysicalOptimizerRule for SpillHashJoins {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
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
                return Ok(Transformed::no(plan));
            }
            Ok(Transformed::yes(
                Arc::new(SpillingHashJoinExec::new(plan.clone())?) as Arc<dyn ExecutionPlan>,
            ))
        })
        .data()
    }

    fn name(&self) -> &str {
        "spill_hash_joins"
    }

    fn schema_check(&self) -> bool {
        true
    }
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
        let mut env = (*ctx.runtime_env()).clone();
        env.memory_pool = Arc::new(HeldPool {
            parent: ctx.memory_pool().clone(),
            state: Mutex::new((held, 0)),
        });
        let ctx = Arc::new(TaskContext::new(
            ctx.task_id(),
            ctx.session_id(),
            ctx.session_config().clone(),
            ctx.scalar_functions().clone(),
            ctx.higher_order_functions().clone(),
            ctx.aggregate_functions().clone(),
            ctx.window_functions().clone(),
            Arc::new(env),
        ));
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
}

/// The memory pool of a join run in memory: it hands out the bytes a
/// reservation of the query's pool already holds, and grows that
/// reservation past them.  A build side reserved its join's memory while it
/// was collected; were the reservation freed for the join to reserve again,
/// another consumer could take the bytes in between, and the join, which
/// cannot spill, finds no room in a pool that consumers which can spill
/// have filled (DataFusion's fair pool limits each of those to its share,
/// not their sum).  The reservation is freed when the join is.
#[derive(Debug)]
struct HeldPool {
    parent: Arc<dyn MemoryPool>,
    /// the reservation, and the bytes handed out of it
    state: Mutex<(MemoryReservation, usize)>,
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
        state.1 += additional;
        let short = state.1.saturating_sub(state.0.size());
        state.0.grow(short);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        // the bytes stay held until the join ends
        self.state.lock().unwrap().1 -= shrink;
    }

    fn try_grow(&self, _reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let short = (state.1 + additional).saturating_sub(state.0.size());
        // Bytes already held are the join's even when the pool's share of
        // a consumer has shrunk below them since.
        if short > 0 {
            state.0.try_grow(short)?;
        }
        state.1 += additional;
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state.lock().unwrap().1
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
}

impl SpillingHashJoinExec {
    fn new(join: Arc<dyn ExecutionPlan>) -> Result<Self> {
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
        };
        Ok(SpillingHashJoinExec {
            join,
            spec: Arc::new(spec),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for SpillingHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpillingHashJoinExec: ")?;
        self.join.fmt_as(t, f)
    }
}

impl ExecutionPlan for SpillingHashJoinExec {
    fn name(&self) -> &str {
        "SpillingHashJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.join.properties()
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
        Ok(Arc::new(SpillingHashJoinExec::new(join)?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let join = self.join.downcast_ref::<HashJoinExec>().unwrap();
        let build = join.left.execute(partition, ctx.clone())?;
        let probe = join.right.execute(partition, ctx.clone())?;
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
        };
        let schema = self.schema();
        let stream = futures::stream::once(Arc::new(run).start(build, probe)).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// One partition of a SpillingHashJoinExec.
struct Run {
    spec: Arc<JoinSpec>,
    ctx: Arc<TaskContext>,
    build_spill: SpillManager,
    probe_spill: SpillManager,
    partition: usize,
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
        while let Some(batch) = build.next().await {
            let batch = batch?;
            let size = get_record_batch_memory_size(&batch);
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
        Ok(Collected::All(batches, reservation))
    }

    async fn start(
        self: Arc<Self>,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        self.join_parts(build, Some(probe), 0, 0).await
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
            state = state.with_physical_optimizer_rule(Arc::new(SpillHashJoins));
        }
        let ctx = SessionContext::new_with_state(state.build());
        ctx.register_table("l", table(20000, 7, 97, 10)).unwrap();
        ctx.register_table("r", table(12000, 11, 89, 23)).unwrap();
        ctx
    }

    /// Join `l` and `r` (columns renamed lk, lk2, lv and rk, rk2, rv) and
    /// count the rows: query `i` of the test's.
    async fn query(ctx: &SessionContext, i: usize) -> DataFrame {
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
        l.join(r, jt, lk, rk, filter)
            .unwrap()
            .aggregate(vec![], aggs)
            .unwrap()
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
            let small = context(Some(256 * 1024));
            for i in 0..15 {
                let (want, _) = run(&plain, i).await;
                let (got, spills) = run(&small, i).await;
                assert_eq!(got, want, "query {i}");
                // the small pool made each join spill
                assert!(spills > 0, "query {i} did not spill");
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

    /// The join's memory comes out of the reservation first, although the
    /// pool would not grant the reservation as much any more.
    #[test]
    fn held_bytes_stay_the_joins() {
        let parent: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(1000));
        let spiller = || MemoryConsumer::new("s").with_can_spill(true);
        let held = spiller().register(&parent);
        held.try_grow(600).unwrap();
        // two more spillers: a share is a third of the pool
        let _others = (spiller().register(&parent), spiller().register(&parent));
        let pool: Arc<dyn MemoryPool> = Arc::new(HeldPool {
            parent: parent.clone(),
            state: Mutex::new((held, 0)),
        });
        let join = MemoryConsumer::new("join").register(&pool);
        join.try_grow(500).unwrap();
        join.try_grow(100).unwrap();
        assert!(join.try_grow(1).is_err());
        assert_eq!(parent.reserved(), 600);
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
            for i in [0, 3, 5] {
                let (want, _) = run(&plain, i).await;
                let (got, spills) = run(&full, i).await;
                assert_eq!(got, want, "query {i}");
                assert_eq!(spills, 0, "query {i} spilled");
            }
        });
    }
}
