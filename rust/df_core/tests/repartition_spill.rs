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

//! A RepartitionExec that spills must not stop.
//!
//! The input tasks of a RepartitionExec that cannot reserve memory write
//! their batches to one spill pool per output partition, and the output
//! reads them back.  DataFusion 55.2.0's reader drains only the oldest file
//! of its pool: when two input tasks spill at once the pool holds two, the
//! reader waits on a drained one while its next batch is in the other, the
//! distributor gate closes, and every worker parks for good
//! (apache/datafusion#24883).  A TPC-H Q8 at scale factor 10 stopped so on
//! three segments.  rust/vendor/datafusion-physical-plan carries the fix
//! (apache/datafusion#24891); this is its test of the pool, through the
//! public interface.

use std::io::Write;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::execution::disk_manager::{DiskManager, DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SpillFile, SpillWriter, TempFileFactory};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::spill_pool::mpsc_channel;
use datafusion::physical_plan::spill::SpillManager;
use futures::{Stream, StreamExt};

type WriteHook = Arc<dyn Fn() + Send + Sync>;

/// Spill files on real temporary files that run `hook` before every write
/// and flush.  A writer writes holding its file's lock, so a hook that
/// blocks holds a push_batch with a file checked out.
struct HookFactory {
    inner: Arc<DiskManager>,
    hook: WriteHook,
}

impl TempFileFactory for HookFactory {
    fn create_temp_file(&self, description: &str) -> Result<Arc<dyn SpillFile>> {
        Ok(Arc::new(HookFile {
            inner: self.inner.create_tmp_file(description)?,
            hook: self.hook.clone(),
        }))
    }
}

struct HookFile {
    inner: Arc<dyn SpillFile>,
    hook: WriteHook,
}

impl SpillFile for HookFile {
    fn path(&self) -> Option<&std::path::Path> {
        self.inner.path()
    }

    fn size(&self) -> Option<u64> {
        self.inner.size()
    }

    fn read_stream(&self) -> Result<Pin<Box<dyn Stream<Item = Result<bytes::Bytes>> + Send>>> {
        self.inner.read_stream()
    }

    fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
        Ok(Box::new(HookWriter {
            inner: self.inner.open_writer()?,
            hook: self.hook.clone(),
        }))
    }
}

struct HookWriter {
    inner: Box<dyn SpillWriter>,
    hook: WriteHook,
}

impl Write for HookWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        (self.hook)();
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        (self.hook)();
        self.inner.flush()
    }
}

impl SpillWriter for HookWriter {
    fn finish(&mut self) -> Result<()> {
        self.inner.finish()
    }
}

/// Holds the first write until the test releases it, and reports every
/// other one, so that the test waits for a second writer to reach a file of
/// its own instead of sleeping.
struct WriteGate {
    taken: AtomicBool,
    entered: Barrier,
    release: Barrier,
    other_writes: Sender<()>,
}

impl WriteGate {
    fn hold_if_first(&self) {
        if self.taken.swap(true, Ordering::SeqCst) {
            // only a writer with a file of its own gets to write: one waiting
            // for the held writer's file would not
            let _ = self.other_writes.send(());
        } else {
            self.entered.wait();
            self.release.wait();
        }
    }
}

fn batch(start: i32, n: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(start..start + n))])
        .unwrap()
}

/// Two writers push at once, the first held inside its first write while
/// the second pushes: the pool then holds two files of one batch each.  The
/// reader must give both batches while both writers are still alive.
#[test]
fn a_spill_pool_reader_reads_every_open_file() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap();
    let (other_writes_tx, other_writes) = mpsc::channel();
    let gate = Arc::new(WriteGate {
        taken: AtomicBool::new(false),
        entered: Barrier::new(2),
        release: Barrier::new(2),
        other_writes: other_writes_tx,
    });
    let hook: WriteHook = {
        let gate = gate.clone();
        Arc::new(move || gate.hold_if_first())
    };
    let disk = Arc::new(DiskManagerBuilder::default().build().unwrap());
    let env = RuntimeEnvBuilder::new()
        .with_disk_manager_builder(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Custom(Arc::new(HookFactory { inner: disk, hook }))),
        )
        .build_arc()
        .unwrap();
    let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let files = metrics.spill_file_count.clone();
    let manager = Arc::new(SpillManager::new(env, metrics, batch(0, 1).schema()));

    let (writer1, mut reader) = mpsc_channel(1 << 20, manager);
    let writer2 = writer1.clone();
    // push_batch blocks: plain threads
    let writer1 = std::thread::spawn(move || {
        writer1.push_batch(&batch(0, 10)).unwrap();
        writer1
    });
    gate.entered.wait();
    let writer2 = std::thread::spawn(move || {
        writer2.push_batch(&batch(10, 10)).unwrap();
        writer2
    });
    other_writes
        .recv_timeout(Duration::from_secs(30))
        .expect("the second writer wrote to a file of its own while the first was held");
    gate.release.wait();
    let writer1 = writer1.join().unwrap();
    let writer2 = writer2.join().unwrap();
    assert_eq!(files.value(), 2, "the writers wrote to two files");

    let mut firsts = Vec::new();
    for _ in 0..2 {
        let b = rt
            .block_on(async { tokio::time::timeout(Duration::from_secs(5), reader.next()).await })
            .expect("the reader waited on a drained file while another had a batch")
            .expect("the reader ended while the writers were alive")
            .unwrap();
        assert_eq!(b.num_rows(), 10);
        let a = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        firsts.push(a.value(0));
    }
    firsts.sort_unstable();
    assert_eq!(firsts, vec![0, 10]);

    // the reader ends once every writer is gone
    drop(writer1);
    drop(writer2);
    assert!(rt.block_on(reader.next()).is_none());
}
