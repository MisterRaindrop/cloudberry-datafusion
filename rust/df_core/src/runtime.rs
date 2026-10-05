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

//! The per-backend Tokio runtime.
//!
//! A PostgreSQL backend is a single-threaded process that owns all of its
//! signal handling.  The runtime's threads must therefore never receive a
//! signal: SIGINT (cancel), SIGTERM (terminate), SIGUSR1 (latch / procsignal)
//! and SIGALRM (timeouts) all have to reach the backend's main thread, whose
//! handlers set the flags that CHECK_FOR_INTERRUPTS acts on.
//!
//! Two layers make sure of that:
//! - the C caller blocks every signal around `init`, so the worker threads
//!   are created with a full mask (threads inherit the creator's mask);
//! - every runtime thread blocks every signal again in `on_thread_start`,
//!   which also covers threads Tokio starts later from its own threads.
//!
//! The runtime is created lazily, on first use inside a backend, never in
//! the postmaster.  It lives until the backend exits.

use std::sync::Mutex;
use std::time::Duration;

use tokio::runtime::{Builder, Handle, Runtime};

/// Name given to every runtime thread; visible in /proc/<pid>/task/*/comm.
pub const THREAD_NAME: &str = "df-worker";

static RUNTIME: Mutex<Option<Runtime>> = Mutex::new(None);

fn block_all_signals() {
    // SAFETY: plain libc calls on a local, fully initialised sigset_t.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut set);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}

/// Number of worker threads used when the caller passes 0.
pub fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Create the runtime if it does not exist yet, and return its worker count.
/// `workers == 0` means `default_workers()`.  A second call returns the
/// existing runtime's size and ignores `workers`.
pub fn init(workers: usize) -> Result<usize, String> {
    let mut slot = RUNTIME.lock().map_err(|_| "runtime lock poisoned".to_string())?;
    if let Some(rt) = slot.as_ref() {
        return Ok(rt.metrics().num_workers());
    }
    let workers = if workers == 0 { default_workers() } else { workers };
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .max_blocking_threads(workers)
        .thread_name(THREAD_NAME)
        .on_thread_start(block_all_signals)
        .enable_time()
        .build()
        .map_err(|e| format!("cannot start the DataFusion runtime: {e}"))?;
    let n = rt.metrics().num_workers();
    *slot = Some(rt);
    Ok(n)
}

/// Handle for spawning work, if the runtime exists.
pub fn handle() -> Option<Handle> {
    RUNTIME.lock().ok()?.as_ref().map(|rt| rt.handle().clone())
}

/// Stop the runtime, waiting at most `timeout` for running tasks to reach a
/// yield point.  Called when the backend exits.
pub fn shutdown(timeout: Duration) {
    let rt = match RUNTIME.lock() {
        Ok(mut slot) => slot.take(),
        Err(_) => None,
    };
    if let Some(rt) = rt {
        rt.shutdown_timeout(timeout);
    }
}
