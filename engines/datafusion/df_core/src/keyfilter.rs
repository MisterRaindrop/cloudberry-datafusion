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

//! KF1: a hash join's build side, once read, as a filter on the PAX scans
//! of its probe side.
//!
//! The planner's estimate of a join's side can be far off, and the join
//! then probes with every row of a large table for the few keys a small
//! build side has: TPC-H Q18's
//! `o_orderkey IN (... HAVING sum(l_quantity) > 300)` keeps a few thousand
//! orders, and its plans probe them with all of lineitem and all of orders
//! joined with customer.  Once a build side is read, its keys are known: a
//! probe row whose key is not among them joins with nothing, and the PAX
//! scan the key column comes from drops it before its other columns are
//! built, before it is repartitioned, joined or aggregated (the
//! prefilter's stage after LM1's, see query.rs).
//!
//! A filter takes the keys of every partition of the join's build side,
//! and filters nothing until it has them all: a probe row of one
//! partition never meets another partition's build rows, but the scan
//! below the repartition feeds every partition.  A build side that spills,
//! or whose distinct keys pass `MAX_KEYS`, gives no filter.  So does a side that is
//! not joined as the build side after all: where AJ1 swaps a join's sides,
//! the probe side's keys filter the build side's scans instead (the
//! join's second filter).  Rows read before a filter is complete pass:
//! the join still checks every row, the filter only drops rows the join
//! would drop.
//!
//! The filter is planned where dropping a probe row cannot change the
//! join's rows (`probe_filtered`) and where the key column reaches a PAX
//! scan through nodes that pass a row's column on unchanged and keep or
//! drop the row by values the filter does not touch (`trace`): a
//! projection of the column, a filter, a repartition, an inner or semi
//! join's side that keeps only rows with a match, an aggregation's group
//! key.  A node or scan read by more than one parent is left alone.
//!
//! Keys are integers and dates compared as i64; a join of more keys gets a
//! filter for each such key (KF2), as a matching row has each among the
//! other side's.  A set of
//! many keys is a Bloom filter, small enough to stay in cache, which lets
//! a few rows the join drops through.  A filter that keeps more than 90%
//! of the first `DECIDE_ROWS` rows it sees switches itself off: the join
//! checks them anyway.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Date32Type, Int16Type, Int32Type, Int64Type};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{JoinType, NullEquality, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, PhysicalExpr};

use crate::query::PaxScan;
use crate::spilljoin::{Filters, SpillingHashJoinExec};

/// Keys of a build side past which it gives no filter (32 MB of keys).
pub const MAX_KEYS: usize = 1 << 22;
/// Rows a filter sees before it decides whether it is worth checking.
const DECIDE_ROWS: u64 = 100_000;

const EMPTY: i64 = i64::MIN;
/// Keys past which a set is a Bloom filter: an exact table of more keys
/// outgrows a core's L2 cache, and every check of a key would miss it.
const EXACT_KEYS: usize = 1 << 14;
/// Bits a Bloom filter gives each key: about 3% of other keys pass, and
/// the filter of 4M keys takes 4 MB.  Twice the bits passed fewer keys but
/// checked slower on TPC-H, the filter taking more of the cache.
const BLOOM_BITS: usize = 8;

/// A set of i64 keys, after a check of the keys' range: up to `EXACT_KEYS`
/// keys exactly, more as a split block Bloom filter (Parquet's), which
/// lets a few other keys through; the join checks every row it is given.
pub struct KeySet {
    min: i64,
    max: i64,
    /// keys given, a key given twice counted twice past `EXACT_KEYS`
    len: usize,
    probe: Probe,
}

enum Probe {
    Exact(Exact),
    Bloom(Vec<[u32; 8]>),
}

/// Open addressing with linear probing, over a table of at least twice
/// the keys.
struct Exact {
    /// whether i64::MIN, the empty slot's value, is a key
    has_empty: bool,
    shift: u32,
    mask: usize,
    slots: Vec<i64>,
}

const SALT: [u32; 8] = [
    0x47b6_137b, 0x4497_4d91, 0x8824_ad5b, 0xa2b7_289d, 0x7054_95c7, 0x2df1_424b, 0x9efc_4947, 0x5c6b_fb31,
];

impl KeySet {
    pub fn new(keys: &[i64]) -> Self {
        let min = keys.iter().copied().min().unwrap_or(i64::MAX);
        let max = keys.iter().copied().max().unwrap_or(i64::MIN);
        if keys.len() <= EXACT_KEYS {
            let mut set = Exact::new(keys.len());
            let len = keys.iter().filter(|&&k| set.insert(k)).count();
            return KeySet { min, max, len, probe: Probe::Exact(set) };
        }
        let mut blocks = vec![[0u32; 8]; (keys.len() * BLOOM_BITS).div_ceil(256)];
        for &k in keys {
            let (b, masks) = bloom_bits(k, blocks.len());
            for (w, m) in blocks[b].iter_mut().zip(masks) {
                *w |= m;
            }
        }
        KeySet { min, max, len: keys.len(), probe: Probe::Bloom(blocks) }
    }

    #[inline]
    pub fn contains(&self, k: i64) -> bool {
        if k < self.min || k > self.max {
            return false;
        }
        match &self.probe {
            Probe::Exact(e) => e.contains(k),
            Probe::Bloom(blocks) => {
                let (b, masks) = bloom_bits(k, blocks.len());
                let mut all = true;
                for (w, m) in blocks[b].iter().zip(masks) {
                    all &= w & m != 0;
                }
                all
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// A key's block of `n` and the bit it sets in each of the block's words.
#[inline]
fn bloom_bits(k: i64, n: usize) -> (usize, [u32; 8]) {
    // murmur3's finalizer: every bit of the key reaches every bit
    let mut h = k as u64;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    let block = (((h >> 32) * n as u64) >> 32) as usize;
    let x = h as u32;
    (block, SALT.map(|s| 1u32 << (x.wrapping_mul(s) >> 27)))
}

impl Exact {
    fn new(keys: usize) -> Self {
        let bits = (keys.max(1) * 2).next_power_of_two().trailing_zeros().max(4);
        let size = 1usize << bits;
        Exact {
            has_empty: false,
            shift: 64 - bits,
            mask: size - 1,
            slots: vec![EMPTY; size],
        }
    }

    #[inline]
    fn slot(&self, k: i64) -> usize {
        ((k as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> self.shift) as usize
    }

    /// Whether `k` is new.
    fn insert(&mut self, k: i64) -> bool {
        if k == EMPTY {
            return !std::mem::replace(&mut self.has_empty, true);
        }
        let mut i = self.slot(k);
        loop {
            let s = self.slots[i];
            if s == k {
                return false;
            }
            if s == EMPTY {
                self.slots[i] = k;
                return true;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline]
    fn contains(&self, k: i64) -> bool {
        if k == EMPTY {
            return self.has_empty;
        }
        let mut i = self.slot(k);
        loop {
            let s = self.slots[i];
            if s == k {
                return true;
            }
            if s == EMPTY {
                return false;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Whether a key column of this type can be filtered.
pub fn key_type(t: &DataType) -> bool {
    matches!(t, DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Date32)
}

/// The non-null values of a key column, as i64; None for another type.
fn values(a: &ArrayRef, out: &mut Vec<i64>) -> bool {
    macro_rules! push {
        ($t:ty) => {{
            let a = a.as_primitive::<$t>();
            if a.null_count() == 0 {
                out.extend(a.values().iter().map(|&v| v as i64));
            } else {
                out.extend(a.iter().flatten().map(|v| v as i64));
            }
        }};
    }
    match a.data_type() {
        DataType::Int16 => push!(Int16Type),
        DataType::Int32 => push!(Int32Type),
        DataType::Int64 => push!(Int64Type),
        DataType::Date32 => push!(Date32Type),
        _ => return false,
    }
    true
}

/// The keys of a join's side, gathered from its partitions, then the set
/// the scans check.
pub struct KeyFilter {
    state: Mutex<Gather>,
    /// Some once every partition gave its keys; None once one could not
    set: OnceLock<Option<Arc<KeySet>>>,
    seen: AtomicU64,
    kept: AtomicU64,
    off: AtomicBool,
}

struct Gather {
    /// partitions whose keys are in
    got: Vec<bool>,
    keys: Vec<i64>,
}

impl fmt::Debug for KeyFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.set.get() {
            None => write!(f, "KeyFilter(gathering)"),
            Some(None) => write!(f, "KeyFilter(none)"),
            Some(Some(s)) => write!(f, "KeyFilter({} keys)", s.len()),
        }
    }
}

impl KeyFilter {
    /// A filter of the keys of `partitions` partitions.
    pub fn new(partitions: usize) -> Self {
        KeyFilter {
            state: Mutex::new(Gather {
                got: vec![false; partitions.max(1)],
                keys: Vec::new(),
            }),
            set: OnceLock::new(),
            seen: AtomicU64::new(0),
            kept: AtomicU64::new(0),
            off: AtomicBool::new(false),
        }
    }

    /// The whole side of partition `partition`'s keys, or of all
    /// partitions' at once (None).
    pub fn add(&self, partition: Option<usize>, keys: &[ArrayRef]) {
        if self.set.get().is_some() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        match partition {
            Some(p) if p < state.got.len() && !state.got[p] => state.got[p] = true,
            Some(_) => return self.fail(),
            None => state.got.iter_mut().for_each(|g| *g = true),
        }
        for a in keys {
            if !values(a, &mut state.keys) {
                drop(state);
                return self.fail();
            }
        }
        if state.keys.len() > MAX_KEYS {
            // a side of more rows than keys (TPC-H Q9's 11M rows a segment
            // hold 1.1M part keys): drop the copies before giving up, and
            // give up while half full, not to sort again and again
            state.keys.sort_unstable();
            state.keys.dedup();
            if state.keys.len() > MAX_KEYS / 2 {
                state.keys = Vec::new();
                drop(state);
                return self.fail();
            }
        }
        if state.got.iter().all(|&g| g) {
            let set = KeySet::new(&state.keys);
            state.keys = Vec::new();
            let _ = self.set.set(Some(Arc::new(set)));
        }
    }

    /// A partition whose keys cannot be had: no filter.
    pub fn fail(&self) {
        let _ = self.set.set(None);
    }

    /// The set, once complete and while worth checking.
    pub fn get(&self) -> Option<&KeySet> {
        if self.off.load(Ordering::Relaxed) {
            return None;
        }
        self.set.get().and_then(|s| s.as_deref())
    }

    /// The positions of `col`'s rows whose key is in the set (a NULL is
    /// not); None when the filter is not ready or switched off.
    pub fn keep(&self, col: &ArrayRef) -> Option<Vec<u32>> {
        let set = self.get()?;
        let mut keep = Vec::new();
        macro_rules! scan {
            ($t:ty) => {{
                let a = col.as_primitive::<$t>();
                let v = a.values();
                match a.nulls() {
                    // every row written, the count advanced by the ones kept
                    None => {
                        keep.resize(v.len(), 0);
                        let mut m = 0;
                        for (r, &k) in v.iter().enumerate() {
                            keep[m] = r as u32;
                            m += set.contains(k as i64) as usize;
                        }
                        keep.truncate(m);
                    }
                    Some(n) => {
                        keep.resize(v.len(), 0);
                        let mut m = 0;
                        for (r, &k) in v.iter().enumerate() {
                            keep[m] = r as u32;
                            m += (n.is_valid(r) && set.contains(k as i64)) as usize;
                        }
                        keep.truncate(m);
                    }
                }
            }};
        }
        match col.data_type() {
            DataType::Int16 => scan!(Int16Type),
            DataType::Int32 => scan!(Int32Type),
            DataType::Int64 => scan!(Int64Type),
            DataType::Date32 => scan!(Date32Type),
            _ => return None,
        }
        let seen = self.seen.fetch_add(col.len() as u64, Ordering::Relaxed) + col.len() as u64;
        let kept = self.kept.fetch_add(keep.len() as u64, Ordering::Relaxed) + keep.len() as u64;
        if seen >= DECIDE_ROWS && seen < DECIDE_ROWS + col.len() as u64 && kept * 10 > seen * 9 {
            self.off.store(true, Ordering::Relaxed);
        }
        Some(keep)
    }

    /// Rows checked and kept so far.
    pub fn counts(&self) -> (u64, u64) {
        (self.seen.load(Ordering::Relaxed), self.kept.load(Ordering::Relaxed))
    }
}

/// Whether a join of this type gives the same rows when its probe (right)
/// side loses the rows without a match: the probe side's unmatched rows
/// give no row of their own.
pub fn probe_filtered(t: JoinType) -> bool {
    matches!(
        t,
        JoinType::Inner
            | JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark
            | JoinType::RightSemi
    )
}

/// The same of its build (left) side, filtered by the probe side's keys
/// where AJ1 swaps the join's sides.
pub fn build_filtered(t: JoinType) -> bool {
    matches!(
        t,
        JoinType::Inner
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark
            | JoinType::LeftSemi
    )
}

/// A scan's filters: its columns' and theirs.
type ScanFilters = Vec<(usize, Arc<KeyFilter>)>;

/// The PAX scans of a query, by the partitions their tables read.
pub type PaxTables = HashMap<usize, Arc<PaxScan>>;

/// The address an Arc's value is at, to find it again.
pub fn address<T: ?Sized>(a: &Arc<T>) -> usize {
    Arc::as_ptr(a) as *const () as usize
}

/// How many parents each node of `plan` has.
fn parents(plan: &Arc<dyn ExecutionPlan>) -> HashMap<usize, usize> {
    let mut counts = HashMap::new();
    let _ = plan.apply(|node| {
        for child in node.children() {
            *counts.entry(address(child)).or_insert(0) += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    });
    counts
}

/// Where column `col` of `plan`'s rows comes from a PAX scan's column
/// unchanged, through nodes that drop a row only by its own values: the
/// scan and the column.
fn trace(
    plan: &Arc<dyn ExecutionPlan>,
    col: usize,
    tables: &PaxTables,
    parents: &HashMap<usize, usize>,
) -> Option<(Arc<PaxScan>, usize)> {
    if parents.get(&address(plan)).copied().unwrap_or(0) > 1 {
        return None;
    }
    if let Some(s) = plan.downcast_ref::<StreamingTableExec>() {
        let mut scan: Option<&Arc<PaxScan>> = None;
        for p in s.partitions() {
            let t = tables.get(&address(p))?;
            if scan.is_some_and(|s| address(s) != address(t)) {
                return None;
            }
            scan = Some(t);
        }
        let scan = scan?;
        if !scan.late.is_empty() {
            return None;
        }
        let col = match s.projection() {
            Some(p) => *p.get(col)?,
            None => col,
        };
        return Some((scan.clone(), col));
    }
    if let Some(p) = plan.downcast_ref::<ProjectionExec>() {
        let c = p.expr().get(col)?.expr.downcast_ref::<Column>()?;
        return trace(p.input(), c.index(), tables, parents);
    }
    if let Some(f) = plan.downcast_ref::<FilterExec>() {
        let col = match f.projection() {
            Some(p) => *p.get(col)?,
            None => col,
        };
        return trace(f.input(), col, tables, parents);
    }
    if let Some(s) = plan.downcast_ref::<SortExec>() {
        return s.fetch().is_none().then(|| trace(s.input(), col, tables, parents))?;
    }
    if let Some(a) = plan.downcast_ref::<AggregateExec>() {
        let g = a.group_expr();
        if !g.is_single() || col >= g.expr().len() {
            return None;
        }
        let c = g.expr()[col].0.downcast_ref::<Column>()?;
        return trace(a.input(), c.index(), tables, parents);
    }
    let join = match plan.downcast_ref::<SpillingHashJoinExec>() {
        Some(s) => s.hash_join(),
        None => plan.downcast_ref::<HashJoinExec>(),
    };
    if let Some(j) = join {
        let col = match &j.projection {
            Some(p) => *p.get(col)?,
            None => col,
        };
        let left = j.left.schema().fields().len();
        return match j.join_type {
            JoinType::Inner if col < left => trace(&j.left, col, tables, parents),
            JoinType::Inner => trace(&j.right, col - left, tables, parents),
            JoinType::LeftSemi if col < left => trace(&j.left, col, tables, parents),
            JoinType::RightSemi => trace(&j.right, col, tables, parents),
            _ => None,
        };
    }
    if matches!(
        plan.name(),
        "RepartitionExec" | "CoalesceBatchesExec" | "CoalescePartitionsExec"
    ) {
        return trace(plan.children()[0], col, tables, parents);
    }
    None
}

/// A join's key on one side, as a column of an integer or date type.
fn key_column(e: &Arc<dyn PhysicalExpr>, plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let c = e.downcast_ref::<Column>()?;
    key_type(plan.schema().field(c.index()).data_type()).then(|| c.index())
}

/// Plan the filters of `plan`'s joins: each integer key of a
/// SpillingHashJoinExec gets a filter of its build side's keys over the scan
/// its probe side's key comes from, and where AJ1 may swap its sides one of
/// its probe side's keys over its build side's.  The scans get theirs at once.
/// Before the plan runs.
pub fn plan_key_filters(plan: &Arc<dyn ExecutionPlan>, tables: &PaxTables) -> Result<usize> {
    if tables.is_empty() {
        return Ok(0);
    }
    let parents = parents(plan);
    let mut scans: HashMap<usize, (Arc<PaxScan>, ScanFilters)> = HashMap::new();
    let mut planned = 0;
    let _ = plan.apply(|node| {
        let Some(s) = node.downcast_ref::<SpillingHashJoinExec>() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let j = s.hash_join().unwrap();
        if j.null_equality != NullEquality::NullEqualsNothing {
            return Ok(TreeNodeRecursion::Continue);
        }
        let partitions = node.output_partitioning().partition_count();
        let mut add = |side: &Arc<dyn ExecutionPlan>, c: usize| -> Option<Arc<KeyFilter>> {
            let (scan, col) = trace(side, c, tables, &parents)?;
            let f = Arc::new(KeyFilter::new(partitions));
            scans
                .entry(address(&scan))
                .or_insert_with(|| (scan.clone(), Vec::new()))
                .1
                .push((col, f.clone()));
            planned += 1;
            Some(f)
        };
        // KF2: a join of more keys, one filter per integer key: a row
        // matching has each of its keys among the other side's.  One that
        // keeps nearly every row switches itself off (TPC-H Q9's suppkey
        // beside its partkey).
        let mut filters = Filters::default();
        for (k, (l, r)) in j.on.iter().enumerate() {
            let (Some(lc), Some(rc)) = (key_column(l, &j.left), key_column(r, &j.right)) else {
                continue;
            };
            if probe_filtered(j.join_type) {
                filters.probe.extend(add(&j.right, rc).map(|f| (k, f)));
            }
            if s.adaptive() && build_filtered(j.join_type) {
                filters.build.extend(add(&j.left, lc).map(|f| (k, f)));
            }
        }
        s.set_key_filters(filters);
        Ok(TreeNodeRecursion::Continue)
    });
    for (_, (scan, filters)) in scans {
        scan.set_key_filters(filters);
    }
    Ok(planned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;

    #[test]
    fn key_sets_hold_their_keys_only() {
        let keys: Vec<i64> = (0..10_000).map(|i| i * 7919 - 3).chain([EMPTY, i64::MAX]).collect();
        let set = KeySet::new(&keys);
        assert_eq!(set.len(), keys.len());
        for &k in &keys {
            assert!(set.contains(k), "{k}");
        }
        for k in (0..10_000).map(|i| i * 7919 - 2) {
            assert!(!set.contains(k), "{k}");
        }
        assert!(!KeySet::new(&[1, 2]).contains(EMPTY));
        assert!(KeySet::new(&[]).is_empty());
    }

    #[test]
    fn large_key_sets_hold_their_keys_and_few_others() {
        let n = EXACT_KEYS as i64 * 40;
        let keys: Vec<i64> = (0..n).map(|i| i * 4 + 1).collect();
        let set = KeySet::new(&keys);
        assert!(matches!(set.probe, Probe::Bloom(_)));
        for &k in &keys {
            assert!(set.contains(k), "{k}");
        }
        // the other keys within the range: about 3% pass
        let passed = (0..n).filter(|i| set.contains(i * 4 + 2)).count();
        assert!(passed * 20 < n as usize, "{passed} of {n}");
        assert!(!set.contains(0) && !set.contains(n * 4 + 1));
    }

    fn ints(v: &[Option<i32>]) -> ArrayRef {
        Arc::new(Int32Array::from(v.to_vec()))
    }

    #[test]
    fn filters_wait_for_every_partition() {
        let f = KeyFilter::new(2);
        f.add(Some(0), &[ints(&[Some(1), None, Some(3)])]);
        assert!(f.keep(&ints(&[Some(1)])).is_none());
        f.add(Some(1), &[ints(&[Some(5)])]);
        let keep = f.keep(&ints(&[Some(1), Some(2), None, Some(3), Some(5)])).unwrap();
        assert_eq!(keep, vec![0, 3, 4]);
    }

    #[test]
    fn a_partition_without_keys_leaves_no_filter() {
        let f = KeyFilter::new(2);
        f.add(Some(0), &[ints(&[Some(1)])]);
        f.fail();
        f.add(Some(1), &[ints(&[Some(2)])]);
        assert!(f.keep(&ints(&[Some(1)])).is_none());
        // a partition giving its keys twice is a mistake, not a filter
        let g = KeyFilter::new(2);
        g.add(Some(0), &[ints(&[Some(1)])]);
        g.add(Some(0), &[ints(&[Some(1)])]);
        assert!(g.get().is_none());
    }

    #[test]
    fn many_rows_of_few_keys_still_filter() {
        // MAX_KEYS + 1000 rows of 3000 keys: a filter
        let f = KeyFilter::new(1);
        let rows = (MAX_KEYS + 1000) as i32;
        f.add(None, &[ints(&(0..rows).map(|i| Some(i % 3000)).collect::<Vec<_>>())]);
        let keep = f.keep(&ints(&[Some(5), Some(2999), Some(3000)])).unwrap();
        assert_eq!(keep, vec![0, 1]);
        // as many keys: none
        let g = KeyFilter::new(1);
        g.add(None, &[ints(&(0..rows).map(Some).collect::<Vec<_>>())]);
        assert!(g.get().is_none());
    }

    #[test]
    fn filters_that_keep_nearly_all_rows_switch_off() {
        let f = KeyFilter::new(1);
        f.add(None, &[ints(&(0..100).map(Some).collect::<Vec<_>>())]);
        let rows = ints(&(0..1000).map(|i| Some(i % 100)).collect::<Vec<_>>());
        for _ in 0..(DECIDE_ROWS / 1000) {
            assert!(f.keep(&rows).is_some());
        }
        assert!(f.keep(&rows).is_none());
        let g = KeyFilter::new(1);
        g.add(None, &[ints(&[Some(1)])]);
        for _ in 0..(DECIDE_ROWS / 1000 + 5) {
            assert_eq!(g.keep(&rows).unwrap().len(), 10);
        }
    }
}
