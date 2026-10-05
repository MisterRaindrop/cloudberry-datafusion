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

//! C ABI entry points of the datafusion_executor extension.
//!
//! Every function exported here follows the contract in `src/df_ffi.h`:
//! it never calls into PostgreSQL, it never lets a panic cross the FFI
//! boundary, and it reports failure as a status code plus a message written
//! into the caller's buffer.  The C side raises the ereport after the call
//! returns, so PostgreSQL's longjmp never unwinds through Rust frames.

use std::any::Any;
use std::ffi::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};

// catch_unwind only works when panics unwind.
#[cfg(panic = "abort")]
compile_error!("df_ffi must be built with panic = \"unwind\"");

/// Status codes; keep in sync with `src/df_ffi.h`.
pub const DF_OK: i32 = 0;
pub const DF_ERROR: i32 = 1;
pub const DF_PANIC: i32 = 2;
/// `df_ffi_task_wait` only: the task is still running.
pub const DF_PENDING: i32 = 3;
/// `df_ffi_task_wait` only: the task stopped because it was cancelled.
pub const DF_CANCELLED: i32 = 4;
/// `df_ffi_query_poll` only: the query has produced all its rows.
pub const DF_DONE: i32 = 5;
/// df_ffi_query_poll: Arrow IPC bytes are available (df_ffi_query_bytes).
pub const DF_BYTES: i32 = 6;

/// Query flags: input arrives as Arrow IPC streams from a Motion
/// (df_ffi_query_push_ipc); results leave as one Arrow IPC stream.
pub const DF_QUERY_IPC_INPUT: u32 = 1;
pub const DF_QUERY_IPC_OUTPUT: u32 = 2;

/// Copy `msg` into a C buffer of `buflen` bytes as a NUL-terminated string,
/// truncating on a UTF-8 character boundary.
fn write_message(buf: *mut c_char, buflen: usize, msg: &str) {
    if buf.is_null() || buflen == 0 {
        return;
    }
    let mut n = msg.len().min(buflen - 1);
    while n > 0 && !msg.is_char_boundary(n) {
        n -= 1;
    }
    // SAFETY: the caller guarantees `buf` points to `buflen` writable bytes,
    // and n + 1 <= buflen.
    unsafe {
        std::ptr::copy_nonoverlapping(msg.as_ptr(), buf.cast::<u8>(), n);
        *buf.add(n) = 0;
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

/// Run `body` with panics caught, and report its outcome through `buf`.
fn guard<F>(buf: *mut c_char, buflen: usize, body: F) -> i32
where
    F: FnOnce() -> Result<String, String>,
{
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(out)) => {
            write_message(buf, buflen, &out);
            DF_OK
        }
        Ok(Err(err)) => {
            write_message(buf, buflen, &err);
            DF_ERROR
        }
        Err(payload) => {
            write_message(buf, buflen, panic_message(payload.as_ref()));
            DF_PANIC
        }
    }
}

/// Writes "datafusion <version>" into `buf`.
#[no_mangle]
pub extern "C" fn df_ffi_version(buf: *mut c_char, buflen: usize) -> i32 {
    guard(buf, buflen, || {
        Ok(format!("datafusion {}", datafusion::DATAFUSION_VERSION))
    })
}

/// Panics on purpose, so tests can check that a panic surfaces as an ERROR.
#[no_mangle]
pub extern "C" fn df_ffi_debug_panic(buf: *mut c_char, buflen: usize) -> i32 {
    guard(buf, buflen, || panic!("datafusion_debug_panic requested"))
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

/// Start the per-backend runtime with `workers` threads (0 = one per CPU) if
/// it is not running yet, and store its actual worker count in `*out_workers`.
/// The caller must block all signals around this call; see df_core::runtime.
#[no_mangle]
pub extern "C" fn df_ffi_runtime_init(
    workers: u32,
    out_workers: *mut u32,
    buf: *mut c_char,
    buflen: usize,
) -> i32 {
    guard(buf, buflen, || {
        let n = df_core::runtime::init(workers as usize)?;
        if !out_workers.is_null() {
            // SAFETY: the caller passes a valid pointer or NULL.
            unsafe { *out_workers = n as u32 };
        }
        Ok(String::new())
    })
}

/// Stop the runtime, waiting at most `timeout_ms` for running tasks.
/// Never fails; a panic during shutdown is swallowed.
#[no_mangle]
pub extern "C" fn df_ffi_runtime_shutdown(timeout_ms: u32) {
    let _ = catch_unwind(|| {
        df_core::runtime::shutdown(std::time::Duration::from_millis(timeout_ms as u64))
    });
}

// ---------------------------------------------------------------------------
// Debug tasks (M1): spawn, wait, cancel, free
// ---------------------------------------------------------------------------

/// Opaque handle owned by the C caller; release it with `df_ffi_task_free`.
pub struct DfTask(df_core::debug::Task);

/// Start `ntasks` CPU-bound tasks that spin for `seconds` (`panic_in_worker`:
/// the first one panics instead).  On success stores a handle in `*out_task`.
#[no_mangle]
pub extern "C" fn df_ffi_debug_spin_start(
    seconds: f64,
    ntasks: u32,
    panic_in_worker: bool,
    out_task: *mut *mut DfTask,
    buf: *mut c_char,
    buflen: usize,
) -> i32 {
    guard(buf, buflen, || {
        if out_task.is_null() {
            return Err("out_task is NULL".into());
        }
        let task = df_core::debug::spin(seconds, ntasks as usize, panic_in_worker)?;
        // SAFETY: checked non-NULL above.
        unsafe { *out_task = Box::into_raw(Box::new(DfTask(task))) };
        Ok(String::new())
    })
}

/// Wait up to `timeout_ms` for `task`.  Returns DF_PENDING if it is still
/// running; otherwise DF_OK (result text in `buf`), DF_CANCELLED, DF_ERROR
/// or DF_PANIC (message in `buf`).  After a non-pending return, do not wait
/// on the same task again.
#[no_mangle]
pub extern "C" fn df_ffi_task_wait(
    task: *mut DfTask,
    timeout_ms: u32,
    buf: *mut c_char,
    buflen: usize,
) -> i32 {
    use df_core::debug::{Outcome, Poll};
    if task.is_null() {
        write_message(buf, buflen, "task is NULL");
        return DF_ERROR;
    }
    // SAFETY: `task` came from df_ffi_debug_spin_start and is not yet freed.
    let task = unsafe { &*task };
    let polled = catch_unwind(AssertUnwindSafe(|| {
        task.0.wait(std::time::Duration::from_millis(timeout_ms as u64))
    }));
    match polled {
        Ok(Poll::Pending) => DF_PENDING,
        Ok(Poll::Ready(Outcome::Done(text))) => {
            write_message(buf, buflen, &text);
            DF_OK
        }
        Ok(Poll::Ready(Outcome::Cancelled)) => {
            write_message(buf, buflen, "cancelled");
            DF_CANCELLED
        }
        Ok(Poll::Ready(Outcome::Failed(msg))) => {
            write_message(buf, buflen, &msg);
            DF_ERROR
        }
        Ok(Poll::Ready(Outcome::Panicked(msg))) => {
            write_message(buf, buflen, &msg);
            DF_PANIC
        }
        Err(payload) => {
            write_message(buf, buflen, panic_message(payload.as_ref()));
            DF_PANIC
        }
    }
}

/// Ask `task` to stop.  Safe to call more than once.
#[no_mangle]
pub extern "C" fn df_ffi_task_cancel(task: *mut DfTask) {
    if !task.is_null() {
        // SAFETY: as in df_ffi_task_wait.
        let task = unsafe { &*task };
        let _ = catch_unwind(AssertUnwindSafe(|| task.0.cancel()));
    }
}

/// Release `task`, cancelling it if it is still running.  The running work
/// owns what it uses, so it may finish after this returns.
#[no_mangle]
pub extern "C" fn df_ffi_task_free(task: *mut DfTask) {
    if !task.is_null() {
        // SAFETY: `task` came from Box::into_raw and is freed exactly once.
        let task = unsafe { Box::from_raw(task) };
        let _ = catch_unwind(AssertUnwindSafe(move || drop(task)));
    }
}

/// Number of debug worker tasks still running on the runtime.
#[no_mangle]
pub extern "C" fn df_ffi_debug_active_tasks() -> u64 {
    df_core::debug::active_tasks() as u64
}

// ---------------------------------------------------------------------------
// Queries (M3): one slice executed by DataFusion
// ---------------------------------------------------------------------------

/// Opaque handle owned by the C caller; release it with `df_ffi_query_free`.
pub struct DfQuery(df_core::query::Query);

/// Column buffers as exchanged with C: native values and one byte per row
/// that is 1 for NULL.
#[repr(C)]
pub struct DfColumn {
    pub values: *const u8,
    pub nulls: *const u8,
}

/// Write a five-character SQLSTATE plus NUL into `sqlstate` (6 bytes).
fn write_sqlstate(sqlstate: *mut c_char, code: &str) {
    write_message(sqlstate, 6, code);
}

/// Write the error's SQLSTATE, and its message followed, if it has one, by a
/// newline and the detail (the C side splits at the first newline).
fn report(e: &df_core::pgfunc::PgError, sqlstate: *mut c_char, buf: *mut c_char, buflen: usize) -> i32 {
    write_sqlstate(sqlstate, e.sqlstate);
    match &e.detail {
        Some(d) => write_message(buf, buflen, &format!("{}\n{}", e.message, d)),
        None => write_message(buf, buflen, &e.message),
    }
    DF_ERROR
}

fn report_panic(payload: Box<dyn Any + Send>, sqlstate: *mut c_char, buf: *mut c_char, buflen: usize) -> i32 {
    write_sqlstate(sqlstate, "XX000");
    write_message(buf, buflen, panic_message(payload.as_ref()));
    DF_PANIC
}

/// Build the plan described by the JSON `spec` and start it with
/// `partitions` parallel partitions, an operator memory budget of
/// `memory_limit` bytes and spill files under `spill_dir`; `flags` are
/// DF_QUERY_* bits.  The runtime must be running.
#[no_mangle]
pub extern "C" fn df_ffi_query_start(
    spec: *const c_char,
    partitions: u32,
    memory_limit: u64,
    spill_dir: *const c_char,
    flags: u32,
    out_query: *mut *mut DfQuery,
    sqlstate: *mut c_char,
    buf: *mut c_char,
    buflen: usize,
) -> i32 {
    let r = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller passes a NUL-terminated string.
        let spec = unsafe { std::ffi::CStr::from_ptr(spec) }.to_string_lossy();
        let dir = unsafe { std::ffi::CStr::from_ptr(spill_dir) }.to_string_lossy();
        let source = if flags & DF_QUERY_IPC_INPUT != 0 {
            df_core::query::Source::Ipc
        } else {
            df_core::query::Source::Pushed
        };
        df_core::query::Query::start_with(
            &spec,
            partitions as usize,
            memory_limit as usize,
            &dir,
            source,
            flags & DF_QUERY_IPC_OUTPUT != 0,
        )
    }));
    match r {
        Ok(Ok(q)) => {
            // SAFETY: the caller passes a valid out pointer.
            unsafe { *out_query = Box::into_raw(Box::new(DfQuery(q))) };
            DF_OK
        }
        Ok(Err(e)) => report(&e, sqlstate, buf, buflen),
        Err(p) => report_panic(p, sqlstate, buf, buflen),
    }
}

/// Like df_ffi_query_start, reading PAX micro-partitions on the workers:
/// `scan` (with `nblocks` blocks) is read with `read` and released with
/// `end`, which the query owns from now on, even if this call fails.
#[no_mangle]
pub extern "C" fn df_ffi_query_start_pax(
    spec: *const c_char,
    partitions: u32,
    memory_limit: u64,
    spill_dir: *const c_char,
    scan: *mut std::ffi::c_void,
    nblocks: u32,
    read: df_core::query::PaxReadFn,
    end: df_core::query::PaxEndFn,
    flags: u32,
    out_query: *mut *mut DfQuery,
    sqlstate: *mut c_char,
    buf: *mut c_char,
    buflen: usize,
) -> i32 {
    let source = df_core::query::Source::Pax(df_core::query::PaxScan::new(scan, nblocks as usize, read, end));
    let r = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller passes NUL-terminated strings.
        let spec = unsafe { std::ffi::CStr::from_ptr(spec) }.to_string_lossy();
        let dir = unsafe { std::ffi::CStr::from_ptr(spill_dir) }.to_string_lossy();
        df_core::query::Query::start_with(
            &spec,
            partitions as usize,
            memory_limit as usize,
            &dir,
            source,
            flags & DF_QUERY_IPC_OUTPUT != 0,
        )
    }));
    match r {
        Ok(Ok(q)) => {
            // SAFETY: the caller passes a valid out pointer.
            unsafe { *out_query = Box::into_raw(Box::new(DfQuery(q))) };
            DF_OK
        }
        Ok(Err(e)) => report(&e, sqlstate, buf, buflen),
        Err(p) => report_panic(p, sqlstate, buf, buflen),
    }
}

/// Offer `nrows` rows in `ncols` columns.  DF_OK: taken.  DF_PENDING: the
/// input queue is full and nothing was taken; poll, then offer again.
#[no_mangle]
pub extern "C" fn df_ffi_query_push(
    query: *mut DfQuery,
    cols: *const DfColumn,
    ncols: u32,
    nrows: u32,
    sqlstate: *mut c_char,
    buf: *mut c_char,
    buflen: usize,
) -> i32 {
    // SAFETY: `query` is live; `cols` points to `ncols` columns of `nrows` rows.
    let q = unsafe { &mut (*query).0 };
    let raw: Vec<df_core::query::RawColumn> = (0..ncols as usize)
        .map(|i| unsafe {
            let c = &*cols.add(i);
            df_core::query::RawColumn { values: c.values, nulls: c.nulls }
        })
        .collect();
    match catch_unwind(AssertUnwindSafe(|| unsafe { q.push(&raw, nrows as usize) })) {
        Ok(Ok(true)) => DF_OK,
        Ok(Ok(false)) => DF_PENDING,
        Ok(Err(e)) => report(&e, sqlstate, buf, buflen),
        Err(p) => report_panic(p, sqlstate, buf, buflen),
    }
}

/// A piece of a byte stream.
#[repr(C)]
pub struct DfSlice {
    pub data: *const u8,
    pub len: usize,
}

/// Offer Arrow IPC stream bytes received from Motion route `route`, as the
/// `nparts` consecutive pieces `parts` (DF_QUERY_IPC_INPUT).  The bytes are
/// copied.  DF_OK: taken.  DF_PENDING: the queue is full and nothing was
/// taken; poll, then offer again.
#[no_mangle]
pub extern "C" fn df_ffi_query_push_ipc(
    query: *mut DfQuery,
    route: i32,
    parts: *const DfSlice,
    nparts: u32,
    sqlstate: *mut c_char,
    buf: *mut c_char,
    buflen: usize,
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
    match catch_unwind(AssertUnwindSafe(|| q.push_ipc(route, &pieces))) {
        Ok(Ok(true)) => DF_OK,
        Ok(Ok(false)) => DF_PENDING,
        Ok(Err(e)) => report(&e, sqlstate, buf, buflen),
        Err(p) => report_panic(p, sqlstate, buf, buflen),
    }
}

/// The Arrow IPC bytes returned by the last poll (DF_BYTES), valid until
/// the next poll.
#[no_mangle]
pub extern "C" fn df_ffi_query_bytes(query: *mut DfQuery, data: *mut *const u8, len: *mut usize) {
    // SAFETY: `query` is live; the out pointers are valid.
    let q = unsafe { &(*query).0 };
    let b = q.output_bytes();
    unsafe {
        *data = b.as_ptr();
        *len = b.len();
    }
}

/// Tell the query there is no more input.
#[no_mangle]
pub extern "C" fn df_ffi_query_finish_input(query: *mut DfQuery) {
    // SAFETY: `query` is live.
    let q = unsafe { &mut (*query).0 };
    let _ = catch_unwind(AssertUnwindSafe(|| q.finish_input()));
}

/// Wait up to `timeout_ms` for the next result batch.  DF_OK: `*nrows` rows
/// are available through df_ffi_query_column until the next poll.
/// DF_BYTES: `*nrows` bytes of the result stream are available through
/// df_ffi_query_bytes (DF_QUERY_IPC_OUTPUT).
/// DF_PENDING, DF_DONE, or DF_ERROR / DF_PANIC with SQLSTATE and message.
#[no_mangle]
pub extern "C" fn df_ffi_query_poll(
    query: *mut DfQuery,
    timeout_ms: u32,
    nrows: *mut u32,
    sqlstate: *mut c_char,
    buf: *mut c_char,
    buflen: usize,
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
            DF_OK
        }
        Ok(Poll::Bytes(n)) => {
            // SAFETY: valid out pointer.
            unsafe { *nrows = n as u32 };
            DF_BYTES
        }
        Ok(Poll::Pending) => DF_PENDING,
        Ok(Poll::Done) => DF_DONE,
        Ok(Poll::Failed(e)) => report(&e, sqlstate, buf, buflen),
        Ok(Poll::Panicked(msg)) => {
            write_sqlstate(sqlstate, "XX000");
            write_message(buf, buflen, &msg);
            DF_PANIC
        }
        Err(p) => report_panic(p, sqlstate, buf, buflen),
    }
}

/// Buffers of output column `col` of the batch returned by the last poll.
#[no_mangle]
pub extern "C" fn df_ffi_query_column(query: *mut DfQuery, col: u32, out: *mut DfColumn) -> i32 {
    // SAFETY: `query` is live; `out` is valid.
    let q = unsafe { &(*query).0 };
    match q.output_column(col as usize) {
        Some(c) => {
            unsafe {
                (*out).values = c.values;
                (*out).nulls = c.nulls;
            }
            DF_OK
        }
        None => DF_ERROR,
    }
}

/// Memory and spill figures of a query, for EXPLAIN ANALYZE.
#[repr(C)]
pub struct DfQueryStats {
    pub partitions: u64,
    pub memory_limit: u64,
    pub memory_peak: u64,
    pub spilled_bytes: u64,
    pub spill_count: u64,
    pub pax_decode_peak: u64,
}

#[no_mangle]
pub extern "C" fn df_ffi_query_stats(query: *mut DfQuery, out: *mut DfQueryStats) {
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

/// Live bytes allocated by Rust code in this process, on any thread.
#[no_mangle]
pub extern "C" fn df_ffi_heap_bytes() -> i64 {
    df_core::memory::heap_bytes()
}

/// Stop the query and release it.  Never blocks; running work is aborted.
#[no_mangle]
pub extern "C" fn df_ffi_query_free(query: *mut DfQuery) {
    if !query.is_null() {
        // SAFETY: `query` came from Box::into_raw and is freed exactly once.
        let q = unsafe { Box::from_raw(query) };
        let _ = catch_unwind(AssertUnwindSafe(move || drop(q)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(f: extern "C" fn(*mut c_char, usize) -> i32) -> (i32, String) {
        let mut buf = vec![0 as c_char; 64];
        let status = f(buf.as_mut_ptr(), buf.len());
        let msg = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        (status, msg)
    }

    #[test]
    fn version_reports_datafusion() {
        let (status, msg) = call(df_ffi_version);
        assert_eq!(status, DF_OK);
        assert!(msg.starts_with("datafusion "), "{msg}");
    }

    #[test]
    fn panic_is_caught() {
        let (status, msg) = call(df_ffi_debug_panic);
        assert_eq!(status, DF_PANIC);
        assert_eq!(msg, "datafusion_debug_panic requested");
    }

    #[test]
    fn message_is_truncated_on_char_boundary() {
        let mut buf = [0 as c_char; 4];
        write_message(buf.as_mut_ptr(), buf.len(), "a数据");
        let msg = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) };
        assert_eq!(msg.to_str().unwrap(), "a");
    }
}
