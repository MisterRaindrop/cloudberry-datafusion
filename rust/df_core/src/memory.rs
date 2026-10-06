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
//!   past it.  The limit is soft: reservations DataFusion cannot refuse,
//!   such as merging spilled runs, may exceed it.  [`TrackingPool`] records
//!   the peak for EXPLAIN ANALYZE.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use datafusion::common::Result;
use datafusion::execution::memory_pool::{
    FairSpillPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
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

/// FairSpillPool with a recorded peak.
#[derive(Debug)]
pub struct TrackingPool {
    inner: FairSpillPool,
    limit: usize,
    peak: AtomicUsize,
}

impl TrackingPool {
    pub fn new(limit: usize) -> Self {
        TrackingPool {
            inner: FairSpillPool::new(limit),
            limit,
            peak: AtomicUsize::new(0),
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    fn note(&self) {
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
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
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.note();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink)
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.note();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_counter_follows_allocations() {
        let before = heap_bytes();
        let v: Vec<u8> = Vec::with_capacity(1 << 20);
        assert!(heap_bytes() - before >= 1 << 20);
        drop(v);
        assert!(heap_bytes() - before < 1 << 20);
    }
}
