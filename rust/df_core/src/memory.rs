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

//! Memory accounting that Cloudberry can see.
//!
//! Two numbers matter:
//!
//! - **Heap bytes**: every allocation Rust code makes in this backend, on any
//!   thread, counted by a global allocator wrapper.  It covers what the
//!   DataFusion memory pool does not track (batches in flight, channel
//!   buffers, plan structures).  The backend's main thread leases at least
//!   this much from Cloudberry's vmem tracker, so the segment's memory
//!   protection sees DataFusion's memory like any other.
//! - **Pool bytes**: what DataFusion operators reserved from the query's
//!   memory pool.  The pool's limit is the slice's operator memory budget;
//!   operators that can spill (hash aggregation) spill instead of growing
//!   past it.  The limit is soft for memory that cannot spill, such as a
//!   sort merging its runs or a join whose build side is in: it may take up
//!   to [`UNSPILLABLE_FACTOR`] times the limit, as a PostgreSQL operator
//!   passes work_mem where it cannot spill; Cloudberry's vmem tracker, which
//!   sees the heap, stays the hard limit.  [`TrackingPool`] records the peak
//!   for EXPLAIN ANALYZE.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use std::sync::Mutex;

use datafusion::common::{human_readable_size, Result};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};

static HEAP_BYTES: AtomicI64 = AtomicI64::new(0);
/// Bytes held by C/C++ code on DataFusion's threads (PAX's reader), as it
/// reports them; leased from the vmem tracker together with the Rust heap.
static EXTERNAL_BYTES: AtomicI64 = AtomicI64::new(0);

/// System allocator that keeps a running total of live bytes.
pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            HEAP_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            HEAP_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        HEAP_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            HEAP_BYTES.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

/// Live bytes allocated by Rust code in this process, plus those reported
/// through `external_add`.
pub fn heap_bytes() -> i64 {
    HEAP_BYTES.load(Ordering::Relaxed) + EXTERNAL_BYTES.load(Ordering::Relaxed)
}

/// Record memory allocated (positive) or freed (negative) outside the Rust
/// allocator.
pub fn external_add(delta: i64) {
    EXTERNAL_BYTES.fetch_add(delta, Ordering::Relaxed);
}

/// How far past the pool's limit memory that cannot spill may go.
pub const UNSPILLABLE_FACTOR: usize = 2;

/// What a consumer that can spill may hold although its share is less:
/// enough for a few batches, so that it can go on, or spill.  A share
/// shrinks as consumers come, and those that came first keep what they
/// hold: the partitions of a sort that ended their input early merge it in
/// memory until every partition has ended, and one that ends late finds a
/// share too small for a single batch, which it can neither hold nor spill
/// (TPC-DS Q64).
pub const SPILL_FLOOR: usize = 2 << 20;

/// DataFusion's FairSpillPool, with a recorded peak, and room past the limit
/// for memory that cannot spill.
///
/// Each consumer that can spill may hold its share of what memory that
/// cannot spill leaves of the limit, as in FairSpillPool, or SPILL_FLOOR
/// where that is less (within UNSPILLABLE_FACTOR times the limit).  Memory that
/// cannot spill is granted while all reservations together stay within
/// UNSPILLABLE_FACTOR times the limit.  FairSpillPool grants it only within
/// the limit, which consumers that can spill may have filled: a sort whose
/// runs are merging in memory holds what it merges as memory that cannot
/// spill, and a partition that ends its input after others found no room
/// left for even its merge's first reservation (TPC-DS Q64).
#[derive(Debug)]
pub struct TrackingPool {
    limit: usize,
    state: Mutex<PoolState>,
    peak: AtomicUsize,
}

#[derive(Debug, Default)]
struct PoolState {
    /// consumers that can spill
    num_spill: usize,
    spillable: usize,
    unspillable: usize,
}

impl TrackingPool {
    pub fn new(limit: usize) -> Self {
        TrackingPool {
            limit,
            state: Mutex::new(PoolState::default()),
            peak: AtomicUsize::new(0),
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    fn note(&self, state: &PoolState) {
        self.peak
            .fetch_max(state.spillable + state.unspillable, Ordering::Relaxed);
    }

    fn refuse(&self, reservation: &MemoryReservation, additional: usize, available: usize) -> DataFusionError {
        DataFusionError::ResourcesExhausted(format!(
            "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {}",
            human_readable_size(additional),
            reservation.consumer().name(),
            human_readable_size(reservation.size()),
            human_readable_size(available),
            self
        ))
    }
}

impl fmt::Display for TrackingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cloudberry pool (limit {} bytes, peak {} bytes)",
            self.limit,
            self.peak()
        )
    }
}

impl MemoryPool for TrackingPool {
    fn name(&self) -> &str {
        "cloudberry"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill() {
            self.state.lock().unwrap().num_spill += 1;
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill() {
            self.state.lock().unwrap().num_spill -= 1;
        }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock().unwrap();
        if reservation.consumer().can_spill() {
            state.spillable += additional;
        } else {
            state.unspillable += additional;
        }
        self.note(&state);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock().unwrap();
        if reservation.consumer().can_spill() {
            state.spillable -= shrink;
        } else {
            state.unspillable -= shrink;
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if reservation.consumer().can_spill() {
            // a share of what memory that cannot spill leaves of the limit
            let spill_available = self.limit.saturating_sub(state.unspillable);
            let share = spill_available
                .checked_div(state.num_spill)
                .unwrap_or(spill_available);
            // up to SPILL_FLOOR past its share, within twice the limit
            let available = share.max(SPILL_FLOOR.min(
                (self.limit * UNSPILLABLE_FACTOR)
                    .saturating_sub(state.spillable + state.unspillable)
                    + reservation.size(),
            ));
            if reservation.size() + additional > available {
                return Err(self.refuse(reservation, additional, available));
            }
            state.spillable += additional;
        } else {
            let available = (self.limit * UNSPILLABLE_FACTOR)
                .saturating_sub(state.spillable + state.unspillable);
            if available < additional {
                return Err(self.refuse(reservation, additional, available));
            }
            state.unspillable += additional;
        }
        self.note(&state);
        Ok(())
    }

    fn reserved(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.spillable + state.unspillable
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.limit)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn heap_counter_follows_allocations() {
        let before = heap_bytes();
        let v: Vec<u8> = Vec::with_capacity(1 << 20);
        assert!(heap_bytes() - before >= 1 << 20);
        drop(v);
        assert!(heap_bytes() - before < 1 << 20);
    }

    fn consumer(pool: &Arc<dyn MemoryPool>, name: &str, spill: bool) -> MemoryReservation {
        MemoryConsumer::new(name).with_can_spill(spill).register(pool)
    }

    /// Consumers that can spill share the limit; memory that cannot spill
    /// gets room up to twice the limit, past what they hold.
    #[test]
    fn memory_that_cannot_spill_passes_the_limit() {
        let u = SPILL_FLOOR;
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackingPool::new(4 * u));
        let (a, b) = (consumer(&pool, "a", true), consumer(&pool, "b", true));
        a.try_grow(2 * u).unwrap();
        assert!(a.try_grow(1).is_err(), "past a spiller's share");
        b.try_grow(2 * u).unwrap();
        // the limit is full of memory that can spill
        let merge = consumer(&pool, "merge", false);
        merge.try_grow(3 * u).unwrap();
        assert!(merge.try_grow(u + 1).is_err(), "past twice the limit");
        merge.try_grow(u).unwrap();
        // memory that cannot spill leaves the spillers no share: their floor
        a.shrink(2 * u);
        a.try_grow(u).unwrap();
        b.shrink(2 * u);
        assert!(a.try_grow(u + 1).is_err(), "past its floor");
        drop(merge);
        a.try_grow(u).unwrap();
        assert_eq!(pool.reserved(), 2 * u);
    }

    /// A consumer that can spill may hold SPILL_FLOOR past a share that has
    /// shrunk below it, within twice the limit.
    #[test]
    fn a_spiller_gets_its_floor() {
        let limit = 4 * SPILL_FLOOR;
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackingPool::new(limit));
        let early = consumer(&pool, "early", true);
        early.try_grow(limit).unwrap();
        let merge = consumer(&pool, "merge", false);
        merge.try_grow(3 * SPILL_FLOOR).unwrap();
        // the share of a late one is nothing
        let late = consumer(&pool, "late", true);
        late.try_grow(SPILL_FLOOR).unwrap();
        assert!(late.try_grow(1).is_err(), "past its floor");
        // and at twice the limit, less than its floor
        let later = consumer(&pool, "later", true);
        later.try_grow(SPILL_FLOOR / 2).unwrap_err();
        assert_eq!(pool.reserved(), 2 * limit);
        drop(late);
        later.try_grow(SPILL_FLOOR).unwrap();
    }
}
