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

//! The DataFusion engine behind the ABI of `src/vec_engine.h`.
//!
//! The extension gets one table of functions from [`vec_engine_entry`].
//! Every function in it never calls into PostgreSQL, never lets a panic
//! cross the FFI boundary, and reports failure as a status code plus a
//! SQLSTATE and message written into the caller's `VecError`.  The C side
//! raises the ereport after the call returns, so PostgreSQL's longjmp never
//! unwinds through Rust frames.

// The entry points take raw pointers from C and dereference them after the
// checks each SAFETY comment names.  Marking them `unsafe fn` would change
// nothing for the C callers, the only callers there are.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::any::Any;
use std::ffi::{c_char, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::OnceLock;

// catch_unwind only works when panics unwind.
#[cfg(panic = "abort")]
compile_error!("df_ffi must be built with panic = \"unwind\"");

/// Version of the ABI; keep in sync with `src/vec_engine.h`.
pub const VEC_ENGINE_ABI: u32 = 1;

/// Status codes; keep in sync with `src/vec_engine.h`.
pub const VEC_OK: i32 = 0;
pub const VEC_ERROR: i32 = 1;
pub const VEC_PANIC: i32 = 2;
/// Still running, or a queue is full.
pub const VEC_PENDING: i32 = 3;
/// `task_wait` only: the task stopped because it was cancelled.
pub const VEC_CANCELLED: i32 = 4;
/// `query_poll` only: the query has produced all its rows.
pub const VEC_DONE: i32 = 5;
/// `query_poll`: Arrow IPC bytes are available (`query_bytes`).
pub const VEC_BYTES: i32 = 6;

/// Query flag: results leave as Arrow IPC streams (VEC_BYTES).
pub const VEC_QUERY_IPC_OUTPUT: u32 = 2;

const VEC_MSG_LEN: usize = 1024;

/// A failure's SQLSTATE and message, which may be followed by a newline
/// and a detail (the C side splits at the first newline).
#[repr(C)]
pub struct VecError {
    pub sqlstate: [c_char; 6],
    pub message: [c_char; VEC_MSG_LEN],
}

/// Copy `msg` into a C buffer as a NUL-terminated string, truncating on a
/// UTF-8 character boundary.
fn write_message(buf: &mut [c_char], msg: &str) {
    if buf.is_empty() {
        return;
    }
    let mut n = msg.len().min(buf.len() - 1);
    while n > 0 && !msg.is_char_boundary(n) {
        n -= 1;
    }
    for (d, s) in buf.iter_mut().zip(&msg.as_bytes()[..n]) {
        *d = *s as c_char;
    }
    buf[n] = 0;
}

/// Fill `err` (if not null) with `sqlstate` and `msg`; returns `status`.
fn fail(err: *mut VecError, status: i32, sqlstate: &str, msg: &str) -> i32 {
    // SAFETY: the caller passes a valid VecError or null.
    if let Some(e) = unsafe { err.as_mut() } {
        write_message(&mut e.sqlstate, sqlstate);
        write_message(&mut e.message, msg);
    }
    status
}

/// Fill `err` with the error's SQLSTATE, and its message followed, if it
/// has one, by a newline and the detail.
fn report(e: &df_core::pgfunc::PgError, err: *mut VecError) -> i32 {
    match &e.detail {
        Some(d) => fail(err, VEC_ERROR, e.sqlstate, &format!("{}\n{}", e.message, d)),
        None => fail(err, VEC_ERROR, e.sqlstate, &e.message),
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        "panic with a non-string payload"
    }
}

fn report_panic(payload: Box<dyn Any + Send>, err: *mut VecError) -> i32 {
    fail(err, VEC_PANIC, "XX000", panic_message(payload.as_ref()))
}

/// Run `body` with panics caught; an Err is an internal error (XX000).
fn guard<F>(err: *mut VecError, body: F) -> i32
where
    F: FnOnce() -> Result<(), String>,
{
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(())) => VEC_OK,
        Ok(Err(e)) => fail(err, VEC_ERROR, "XX000", &e),
        Err(payload) => report_panic(payload, err),
    }
}

/// Panics on purpose, so tests can check that a panic surfaces as an ERROR.
extern "C" fn debug_panic(err: *mut VecError) -> i32 {
    guard(err, || panic!("datafusion_debug_panic requested"))
}

/// CM1: set the case maps of the backend's LC_CTYPE: `nupper` and `nlower`
/// pairs (code point, mapped code point), flattened.  `upper` and `lower`
/// point to 2 * `nupper` and 2 * `nlower` values.
extern "C" fn set_case_maps(upper: *const u32, nupper: usize, lower: *const u32, nlower: usize) {
    let pairs = |p: *const u32, n: usize| -> Vec<(u32, u32)> {
        if p.is_null() || n == 0 {
            return Vec::new();
        }
        // SAFETY: the caller passes 2 * n values.
        let v = unsafe { std::slice::from_raw_parts(p, 2 * n) };
        v.chunks_exact(2).map(|c| (c[0], c[1])).collect()
    };
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let (u, l) = (pairs(upper, nupper), pairs(lower, nlower));
        df_core::pgstr::set_case_maps(&u, &l)
    }));
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

/// Start the per-backend runtime with `workers` threads (0 = one per CPU) if
/// it is not running yet, and store its actual worker count in `*out_workers`.
/// The caller must block all signals around this call; see df_core::runtime.
extern "C" fn runtime_init(workers: u32, out_workers: *mut u32, err: *mut VecError) -> i32 {
    guard(err, || {
        let n = df_core::runtime::init(workers as usize)?;
        if !out_workers.is_null() {
            // SAFETY: the caller passes a valid pointer or NULL.
            unsafe { *out_workers = n as u32 };
        }
        Ok(())
    })
}

/// Stop the runtime, waiting at most `timeout_ms` for running tasks.
/// Never fails; a panic during shutdown is swallowed.
extern "C" fn runtime_shutdown(timeout_ms: u32) {
    let _ = catch_unwind(|| {
        df_core::runtime::shutdown(std::time::Duration::from_millis(timeout_ms as u64))
    });
}

/// Live bytes allocated by Rust code in this process, on any thread.
extern "C" fn heap_bytes() -> i64 {
    df_core::memory::heap_bytes()
}

// ---------------------------------------------------------------------------
// Debug tasks (M1): spawn, wait, cancel, free
// ---------------------------------------------------------------------------

/// Opaque handle owned by the C caller; release it with `task_free`.
pub struct VecTask(df_core::debug::Task);

/// Start `ntasks` CPU-bound tasks that spin for `seconds` (`panic_in_worker`:
/// the first one panics instead).  On success stores a handle in `*out_task`.
extern "C" fn debug_spin_start(
    seconds: f64,
    ntasks: u32,
    panic_in_worker: bool,
    out_task: *mut *mut VecTask,
    err: *mut VecError,
) -> i32 {
    guard(err, || {
        if out_task.is_null() {
            return Err("out_task is NULL".into());
        }
        let task = df_core::debug::spin(seconds, ntasks as usize, panic_in_worker)?;
        // SAFETY: checked non-NULL above.
        unsafe { *out_task = Box::into_raw(Box::new(VecTask(task))) };
        Ok(())
    })
}

/// Wait up to `timeout_ms` for `task`.  Returns VEC_PENDING if it is still
/// running; otherwise VEC_OK (result text in the message), VEC_CANCELLED,
/// VEC_ERROR or VEC_PANIC.  After a non-pending return, do not wait on the
/// same task again.
extern "C" fn debug_task_wait(task: *mut VecTask, timeout_ms: u32, err: *mut VecError) -> i32 {
    use df_core::debug::{Outcome, Poll};
    if task.is_null() {
        return fail(err, VEC_ERROR, "XX000", "task is NULL");
    }
    // SAFETY: `task` came from debug_spin_start and is not yet freed.
    let task = unsafe { &*task };
    let polled = catch_unwind(AssertUnwindSafe(|| {
        task.0
            .wait(std::time::Duration::from_millis(timeout_ms as u64))
    }));
    match polled {
        Ok(Poll::Pending) => VEC_PENDING,
        Ok(Poll::Ready(Outcome::Done(text))) => fail(err, VEC_OK, "00000", &text),
        Ok(Poll::Ready(Outcome::Cancelled)) => fail(err, VEC_CANCELLED, "57014", "cancelled"),
        Ok(Poll::Ready(Outcome::Failed(msg))) => fail(err, VEC_ERROR, "XX000", &msg),
        Ok(Poll::Ready(Outcome::Panicked(msg))) => fail(err, VEC_PANIC, "XX000", &msg),
        Err(payload) => report_panic(payload, err),
    }
}

/// Ask `task` to stop.  Safe to call more than once.
extern "C" fn debug_task_cancel(task: *mut VecTask) {
    if !task.is_null() {
        // SAFETY: as in debug_task_wait.
        let task = unsafe { &*task };
        let _ = catch_unwind(AssertUnwindSafe(|| task.0.cancel()));
    }
}

/// Release `task`, cancelling it if it is still running.  The running work
/// owns what it uses, so it may finish after this returns.
extern "C" fn debug_task_free(task: *mut VecTask) {
    if !task.is_null() {
        // SAFETY: `task` came from Box::into_raw and is freed exactly once.
        let task = unsafe { Box::from_raw(task) };
        let _ = catch_unwind(AssertUnwindSafe(move || drop(task)));
    }
}

/// Number of debug worker tasks still running on the runtime.
extern "C" fn debug_active_tasks() -> u64 {
    df_core::debug::active_tasks() as u64
}

// ---------------------------------------------------------------------------
// Queries (M3): one slice executed by DataFusion
// ---------------------------------------------------------------------------

/// Opaque handle owned by the C caller; release it with `query_free`.
pub struct VecQuery(df_core::query::Query);

/// Column buffers as exchanged with C: native values and one byte per row
/// that is 1 for NULL.
#[repr(C)]
pub struct VecColumn {
    pub values: *const u8,
    pub nulls: *const u8,
    pub offsets: *const i32,
}

/// An input DataFusion reads from PAX itself (C3): its scan and blocks, or
/// a null scan.
#[repr(C)]
pub struct VecPaxInput {
    pub scan: *mut std::ffi::c_void,
    pub nblocks: u32,
}

/// What `query_start` needs; see `src/vec_engine.h`.
#[repr(C)]
pub struct VecQueryArgs {
    pub ir: *const c_char,
    pub partitions: u32,
    pub memory_limit: u64,
    pub spill_dir: *const c_char,
    pub flags: u32,
    pub ninputs: u32,
    pub ipc_inputs: u64,
    pub pax_inputs: *const VecPaxInput,
    pub pax_read: Option<df_core::query::PaxReadFn>,
    pub pax_end: Option<df_core::query::PaxEndFn>,
    pub pax_groups: Option<df_core::query::PaxGroupsFn>,
    pub pax_max_readers: u32,
}

/// Build the plan `args.ir` describes and start it with `partitions`
/// parallel partitions, an operator memory budget of `memory_limit` bytes
/// and spill files under `spill_dir`.  Bit j of `ipc_inputs` says input j
/// arrives as Arrow IPC streams from a Motion (`query_push_ipc`), otherwise
/// as pushed batches, unless `pax_inputs` (null, or one entry per input)
/// gives it a PAX scan, which the workers read with `pax_read`, `pax_end`
/// and `pax_groups`, at most `pax_max_readers` groups at once (0: no
/// limit), and which the query owns from the call on, even if it fails.
/// The runtime must be running.
extern "C" fn query_start(
    args: *const VecQueryArgs,
    out_query: *mut *mut VecQuery,
    err: *mut VecError,
) -> i32 {
    // SAFETY: the caller passes valid arguments.
    let a = unsafe { &*args };
    // The PAX scans are the query's from here on: wrapped first, so that
    // they are ended whatever fails below.
    let sources: Vec<df_core::query::Source> = (0..a.ninputs as usize)
        .map(|j| {
            // SAFETY: `pax_inputs` is null or holds `ninputs` entries.
            let p = if a.pax_inputs.is_null() {
                None
            } else {
                Some(unsafe { &*a.pax_inputs.add(j) })
            };
            match (p, a.pax_read, a.pax_end, a.pax_groups) {
                (Some(p), Some(read), Some(end), Some(groups)) if !p.scan.is_null() => {
                    df_core::query::Source::Pax(
                        df_core::query::PaxScan::new(p.scan, p.nblocks as usize, read, end, groups)
                            .with_max_readers(a.pax_max_readers as usize),
                    )
                }
                _ if j < 64 && a.ipc_inputs & (1u64 << j) != 0 => df_core::query::Source::Ipc,
                _ => df_core::query::Source::Pushed,
            }
        })
        .collect();
    let r = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller passes NUL-terminated strings.
        let ir = unsafe { CStr::from_ptr(a.ir) }.to_string_lossy();
        let dir = unsafe { CStr::from_ptr(a.spill_dir) }.to_string_lossy();
        df_core::query::Query::start_multi(
            &ir,
            a.partitions as usize,
            a.memory_limit as usize,
            &dir,
            sources,
            a.flags & VEC_QUERY_IPC_OUTPUT != 0,
        )
    }));
    match r {
        Ok(Ok(q)) => {
            // SAFETY: the caller passes a valid out pointer.
            unsafe { *out_query = Box::into_raw(Box::new(VecQuery(q))) };
            VEC_OK
        }
        Ok(Err(e)) => report(&e, err),
        Err(p) => report_panic(p, err),
    }
}

/// Offer `nrows` rows in `ncols` columns.  VEC_OK: taken.  VEC_PENDING: the
/// input queue is full and nothing was taken; poll, then offer again.
extern "C" fn query_push(
    query: *mut VecQuery,
    input: u32,
    cols: *const VecColumn,
    ncols: u32,
    nrows: u32,
    err: *mut VecError,
) -> i32 {
    // SAFETY: `query` is live; `cols` points to `ncols` columns of `nrows` rows.
    let q = unsafe { &mut (*query).0 };
    let raw: Vec<df_core::query::RawColumn> = (0..ncols as usize)
        .map(|i| unsafe {
            let c = &*cols.add(i);
            df_core::query::RawColumn {
                values: c.values,
                nulls: c.nulls,
                offsets: c.offsets,
            }
        })
        .collect();
    match catch_unwind(AssertUnwindSafe(|| unsafe {
        q.push_input(input as usize, &raw, nrows as usize)
    })) {
        Ok(Ok(true)) => VEC_OK,
        Ok(Ok(false)) => VEC_PENDING,
        Ok(Err(e)) => report(&e, err),
        Err(p) => report_panic(p, err),
    }
}

/// A piece of a byte stream.
#[repr(C)]
pub struct VecSlice {
    pub data: *const u8,
    pub len: usize,
}

/// Offer Arrow IPC stream bytes received from Motion route `route`, as the
/// `nparts` consecutive pieces `parts`.  The bytes are copied.  VEC_OK:
/// taken.  VEC_PENDING: the queue is full and nothing was taken; poll, then
/// offer again.
extern "C" fn query_push_ipc(
    query: *mut VecQuery,
    input: u32,
    route: i32,
    parts: *const VecSlice,
    nparts: u32,
    err: *mut VecError,
) -> i32 {
    // SAFETY: `query` is live; `parts` points to `nparts` readable pieces.
    let q = unsafe { &mut (*query).0 };
    let pieces: Vec<&[u8]> = (0..nparts as usize)
        .map(|i| unsafe {
            let p = &*parts.add(i);
            if p.len == 0 {
                &[][..]
            } else {
                std::slice::from_raw_parts(p.data, p.len)
            }
        })
        .collect();
    match catch_unwind(AssertUnwindSafe(|| {
        q.push_ipc_input(input as usize, route, &pieces)
    })) {
        Ok(Ok(true)) => VEC_OK,
        Ok(Ok(false)) => VEC_PENDING,
        Ok(Err(e)) => report(&e, err),
        Err(p) => report_panic(p, err),
    }
}

/// The Arrow IPC bytes returned by the last poll (VEC_BYTES), valid until
/// the next poll, and their route: the Redistribute Motion's receiver, or
/// -1 for a Motion's only stream.
extern "C" fn query_bytes(
    query: *mut VecQuery,
    route: *mut i32,
    data: *mut *const u8,
    len: *mut usize,
) {
    // SAFETY: `query` is live; the out pointers are valid.
    let q = unsafe { &(*query).0 };
    let b = q.output_bytes();
    unsafe {
        *route = q.output_route();
        *data = b.as_ptr();
        *len = b.len();
    }
}

/// Routes of `nrows` rows by Cloudberry's distribution hash, for checking
/// the Rust transcription against cdbhash(): `kinds[i]` names the hash of
/// key column `cols[i]` (0 bool, 1 int2, 2 int4, 3 int8, 4 float4,
/// 5 float8, 6 text, 7 character, 8 + s numeric of scale s, 85 tid).
/// Writes `nrows` routes to `out`.  VEC_OK, or VEC_ERROR for an unknown
/// kind.
extern "C" fn debug_cdbhash_routes(
    kinds: *const i32,
    cols: *const VecColumn,
    nkeys: u32,
    nrows: u32,
    segments: i32,
    workers: i32,
    out: *mut u32,
) -> i32 {
    use df_core::query::PgType;
    let r = catch_unwind(AssertUnwindSafe(|| {
        let mut keys = Vec::with_capacity(nkeys as usize);
        for i in 0..nkeys as usize {
            // SAFETY: the caller passes `nkeys` kinds and columns of `nrows` rows.
            let (kind, c) = unsafe { (*kinds.add(i), &*cols.add(i)) };
            let ty = match kind {
                0 => PgType::Bool,
                1 => PgType::Int2,
                2 => PgType::Int4,
                3 => PgType::Int8,
                4 => PgType::Float4,
                5 => PgType::Float8,
                6 => PgType::Text,
                7 => PgType::Bpchar,
                8..=84 => PgType::Numeric((kind - 8) as i8),
                85 => PgType::Tid,
                _ => return None,
            };
            let raw = df_core::query::RawColumn {
                values: c.values,
                nulls: c.nulls,
                offsets: c.offsets,
            };
            keys.push((
                ty.key_hash(),
                unsafe { df_core::query::build_array(ty, raw, nrows as usize) }.ok()?,
            ));
        }
        Some(df_core::cdbhash::routes(
            &keys,
            nrows as usize,
            segments,
            workers,
        ))
    }));
    match r {
        Ok(Some(routes)) => {
            // SAFETY: `out` has room for `nrows` routes.
            unsafe { std::ptr::copy_nonoverlapping(routes.as_ptr(), out, routes.len()) };
            VEC_OK
        }
        _ => VEC_ERROR,
    }
}

/// Tell the query input `input` has no more rows.
extern "C" fn query_finish_input(query: *mut VecQuery, input: u32) {
    // SAFETY: `query` is live.
    let q = unsafe { &mut (*query).0 };
    let _ = catch_unwind(AssertUnwindSafe(|| q.finish_input_at(input as usize)));
}

/// Wait up to `timeout_ms` for the next result batch.  VEC_OK: `*nrows`
/// rows are available through `query_column` until the next poll.
/// VEC_BYTES: `*nrows` bytes of the result stream are available through
/// `query_bytes` (VEC_QUERY_IPC_OUTPUT).  VEC_PENDING, VEC_DONE, or
/// VEC_ERROR / VEC_PANIC with SQLSTATE and message.
extern "C" fn query_poll(
    query: *mut VecQuery,
    timeout_ms: u32,
    nrows: *mut u32,
    err: *mut VecError,
) -> i32 {
    use df_core::query::Poll;
    // SAFETY: `query` is live.
    let q = unsafe { &mut (*query).0 };
    let r = catch_unwind(AssertUnwindSafe(|| {
        q.poll(std::time::Duration::from_millis(timeout_ms as u64))
    }));
    match r {
        Ok(Poll::Batch(n)) => {
            // SAFETY: valid out pointer.
            unsafe { *nrows = n as u32 };
            VEC_OK
        }
        Ok(Poll::Bytes(n)) => {
            // SAFETY: valid out pointer.
            unsafe { *nrows = n as u32 };
            VEC_BYTES
        }
        Ok(Poll::Pending) => VEC_PENDING,
        Ok(Poll::Done) => VEC_DONE,
        Ok(Poll::Failed(e)) => report(&e, err),
        Ok(Poll::Panicked(msg)) => fail(err, VEC_PANIC, "XX000", &msg),
        Err(p) => report_panic(p, err),
    }
}

/// Buffers of output column `col` of the batch returned by the last poll.
extern "C" fn query_column(query: *mut VecQuery, col: u32, out: *mut VecColumn) -> i32 {
    // SAFETY: `query` is live; `out` is valid.
    let q = unsafe { &(*query).0 };
    match q.output_column(col as usize) {
        Some(c) => {
            unsafe {
                (*out).values = c.values;
                (*out).nulls = c.nulls;
                (*out).offsets = c.offsets;
            }
            VEC_OK
        }
        None => VEC_ERROR,
    }
}

/// Memory and spill figures of a query, for EXPLAIN ANALYZE.
#[repr(C)]
pub struct VecQueryStats {
    pub partitions: u64,
    pub memory_limit: u64,
    pub memory_peak: u64,
    pub spilled_bytes: u64,
    pub spill_count: u64,
    pub pax_decode_peak: u64,
}

extern "C" fn query_stats(query: *mut VecQuery, out: *mut VecQueryStats) {
    // SAFETY: `query` is live; `out` is valid.
    let q = unsafe { &(*query).0 };
    let s = catch_unwind(AssertUnwindSafe(|| q.stats())).unwrap_or_default();
    unsafe {
        (*out).partitions = s.partitions;
        (*out).memory_limit = s.memory_limit;
        (*out).memory_peak = s.memory_peak;
        (*out).spilled_bytes = s.spilled_bytes;
        (*out).spill_count = s.spill_count;
        (*out).pax_decode_peak = s.pax_decode_peak;
    }
}

/// Stop the query and release it.  Never blocks; running work is aborted.
extern "C" fn query_free(query: *mut VecQuery) {
    if !query.is_null() {
        // SAFETY: `query` came from Box::into_raw and is freed exactly once.
        let q = unsafe { Box::from_raw(query) };
        let _ = catch_unwind(AssertUnwindSafe(move || drop(q)));
    }
}

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

/// What the host gives the engine.
#[repr(C)]
pub struct VecHost {
    pub abi: u32,
}

/// The test functions of `src/vec_engine.h`.
#[repr(C)]
pub struct VecEngineDebug {
    pub panic: extern "C" fn(*mut VecError) -> i32,
    pub spin_start: extern "C" fn(f64, u32, bool, *mut *mut VecTask, *mut VecError) -> i32,
    pub task_wait: extern "C" fn(*mut VecTask, u32, *mut VecError) -> i32,
    pub task_cancel: extern "C" fn(*mut VecTask),
    pub task_free: extern "C" fn(*mut VecTask),
    pub active_tasks: extern "C" fn() -> u64,
    pub cdbhash_routes:
        extern "C" fn(*const i32, *const VecColumn, u32, u32, i32, i32, *mut u32) -> i32,
}

/// The engine's table of `src/vec_engine.h`.
#[repr(C)]
pub struct VecEngine {
    pub abi: u32,
    pub name: *const c_char,
    pub version: *const c_char,
    pub runtime_init: extern "C" fn(u32, *mut u32, *mut VecError) -> i32,
    pub runtime_shutdown: extern "C" fn(u32),
    pub heap_bytes: extern "C" fn() -> i64,
    pub set_case_maps: extern "C" fn(*const u32, usize, *const u32, usize),
    pub query_start: extern "C" fn(*const VecQueryArgs, *mut *mut VecQuery, *mut VecError) -> i32,
    pub query_push:
        extern "C" fn(*mut VecQuery, u32, *const VecColumn, u32, u32, *mut VecError) -> i32,
    pub query_push_ipc:
        extern "C" fn(*mut VecQuery, u32, i32, *const VecSlice, u32, *mut VecError) -> i32,
    pub query_finish_input: extern "C" fn(*mut VecQuery, u32),
    pub query_poll: extern "C" fn(*mut VecQuery, u32, *mut u32, *mut VecError) -> i32,
    pub query_column: extern "C" fn(*mut VecQuery, u32, *mut VecColumn) -> i32,
    pub query_bytes: extern "C" fn(*mut VecQuery, *mut i32, *mut *const u8, *mut usize),
    pub query_stats: extern "C" fn(*mut VecQuery, *mut VecQueryStats),
    pub query_free: extern "C" fn(*mut VecQuery),
    pub debug: *const VecEngineDebug,
}

// SAFETY: the table only holds function pointers and pointers to strings
// and a table that live as long as the process and are never written.
unsafe impl Sync for VecEngine {}
unsafe impl Send for VecEngine {}

static NAME: &CStr = c"datafusion";

static DEBUG: VecEngineDebug = VecEngineDebug {
    panic: debug_panic,
    spin_start: debug_spin_start,
    task_wait: debug_task_wait,
    task_cancel: debug_task_cancel,
    task_free: debug_task_free,
    active_tasks: debug_active_tasks,
    cdbhash_routes: debug_cdbhash_routes,
};

/// "datafusion <version>", made once.
fn version() -> &'static CStr {
    static VERSION: OnceLock<std::ffi::CString> = OnceLock::new();
    VERSION.get_or_init(|| {
        std::ffi::CString::new(format!("datafusion {}", datafusion::DATAFUSION_VERSION))
            .expect("version without NUL")
    })
}

/// The table, or null if the host speaks another ABI.  Never panics.
#[no_mangle]
pub extern "C" fn vec_engine_entry(host_abi: u32, _host: *const VecHost) -> *const VecEngine {
    static ENGINE: OnceLock<VecEngine> = OnceLock::new();
    if host_abi != VEC_ENGINE_ABI {
        return std::ptr::null();
    }
    catch_unwind(|| {
        ENGINE.get_or_init(|| VecEngine {
            abi: VEC_ENGINE_ABI,
            name: NAME.as_ptr(),
            version: version().as_ptr(),
            runtime_init,
            runtime_shutdown,
            heap_bytes,
            set_case_maps,
            query_start,
            query_push,
            query_push_ipc,
            query_finish_input,
            query_poll,
            query_column,
            query_bytes,
            query_stats,
            query_free,
            debug: &DEBUG,
        }) as *const VecEngine
    })
    .unwrap_or(std::ptr::null())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> &'static VecEngine {
        let e = vec_engine_entry(VEC_ENGINE_ABI, std::ptr::null());
        assert!(!e.is_null());
        unsafe { &*e }
    }

    fn new_error() -> VecError {
        VecError {
            sqlstate: [0; 6],
            message: [0; VEC_MSG_LEN],
        }
    }

    fn text(buf: &[c_char]) -> String {
        unsafe { CStr::from_ptr(buf.as_ptr()) }
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn version_reports_datafusion() {
        let e = engine();
        assert_eq!(e.abi, VEC_ENGINE_ABI);
        assert_eq!(
            unsafe { CStr::from_ptr(e.name) }.to_str().unwrap(),
            "datafusion"
        );
        let v = unsafe { CStr::from_ptr(e.version) }.to_str().unwrap();
        assert!(v.starts_with("datafusion "), "{v}");
    }

    #[test]
    fn other_abi_gets_no_table() {
        assert!(vec_engine_entry(VEC_ENGINE_ABI + 1, std::ptr::null()).is_null());
        assert!(vec_engine_entry(0, std::ptr::null()).is_null());
    }

    #[test]
    fn panic_is_caught() {
        let mut err = new_error();
        let status = (unsafe { &*engine().debug }.panic)(&mut err);
        assert_eq!(status, VEC_PANIC);
        assert_eq!(text(&err.sqlstate), "XX000");
        assert_eq!(text(&err.message), "datafusion_debug_panic requested");
    }

    #[test]
    fn message_is_truncated_on_char_boundary() {
        let mut buf = [0 as c_char; 4];
        write_message(&mut buf, "a数据");
        assert_eq!(text(&buf), "a");
    }
}
