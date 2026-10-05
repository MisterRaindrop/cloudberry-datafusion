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

//! Test workloads for the runtime milestones.  They exercise the paths real
//! query execution will use later: spawning CPU-bound tasks on the runtime,
//! waiting for them from the backend's main thread with a timeout so it can
//! keep servicing interrupts, cancelling them, and surviving a panic inside
//! a worker.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::runtime;

/// How a task ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Done(String),
    Cancelled,
    Failed(String),
    Panicked(String),
}

/// Result of one bounded wait.
#[derive(Debug, PartialEq, Eq)]
pub enum Poll {
    Pending,
    Ready(Outcome),
}

static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// Number of debug worker tasks currently running on the runtime.  Used by
/// tests to check that cancellation really stopped the work.
pub fn active_tasks() -> usize {
    ACTIVE.load(Ordering::SeqCst)
}

struct ActiveGuard;

impl ActiveGuard {
    fn new() -> Self {
        ACTIVE.fetch_add(1, Ordering::SeqCst);
        ActiveGuard
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A group of running tasks, owned by the backend's main thread.
pub struct Task {
    cancel: Arc<AtomicBool>,
    rx: Receiver<Outcome>,
}

impl Task {
    /// Ask the tasks to stop.  They notice within about a millisecond.
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
    }

    /// Wait up to `timeout` for the outcome.
    pub fn wait(&self, timeout: Duration) -> Poll {
        match self.rx.recv_timeout(timeout) {
            Ok(outcome) => Poll::Ready(outcome),
            Err(RecvTimeoutError::Timeout) => Poll::Pending,
            Err(RecvTimeoutError::Disconnected) => {
                Poll::Ready(Outcome::Failed("task ended without reporting".into()))
            }
        }
    }
}

impl Drop for Task {
    // Dropping the handle stops the work; the tasks own everything they use,
    // so they may finish after the handle is gone.
    fn drop(&mut self) {
        self.cancel();
    }
}

/// The message a panicking task was started with, without Tokio's task id.
fn panic_text(e: tokio::task::JoinError) -> String {
    let payload = e.into_panic();
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic with a non-string payload".to_string()
    }
}

/// Busy-loop on `ntasks` runtime tasks for `seconds`, in 1 ms slices with a
/// yield between slices, checking for cancellation each slice.  With
/// `panic_in_worker`, the first task panics instead.
pub fn spin(seconds: f64, ntasks: usize, panic_in_worker: bool) -> Result<Task, String> {
    let handle = runtime::handle().ok_or("the DataFusion runtime is not running")?;
    if !(seconds >= 0.0 && seconds.is_finite()) {
        return Err(format!("invalid duration: {seconds}"));
    }
    let ntasks = ntasks.max(1);
    let cancel = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();
    let deadline = Instant::now() + Duration::from_secs_f64(seconds);

    let flag = cancel.clone();
    handle.spawn(async move {
        let mut workers = Vec::with_capacity(ntasks);
        for i in 0..ntasks {
            let flag = flag.clone();
            workers.push(tokio::spawn(async move {
                let _active = ActiveGuard::new();
                if panic_in_worker && i == 0 {
                    panic!("datafusion_debug_worker_panic requested");
                }
                loop {
                    if flag.load(Ordering::SeqCst) {
                        return false;
                    }
                    if Instant::now() >= deadline {
                        return true;
                    }
                    let slice = Instant::now();
                    while slice.elapsed() < Duration::from_millis(1) {
                        std::hint::spin_loop();
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }
        let mut outcome = None;
        let mut all_done = true;
        for w in workers {
            match w.await {
                Ok(done) => all_done &= done,
                Err(e) if e.is_panic() => {
                    flag.store(true, Ordering::SeqCst);
                    outcome.get_or_insert(Outcome::Panicked(panic_text(e)));
                }
                Err(e) => {
                    outcome.get_or_insert(Outcome::Failed(e.to_string()));
                }
            }
        }
        let outcome = outcome.unwrap_or(if all_done {
            Outcome::Done(format!("{ntasks} tasks finished"))
        } else {
            Outcome::Cancelled
        });
        let _ = tx.send(outcome);
    });

    Ok(Task { cancel, rx })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait_ready(t: &Task) -> Outcome {
        loop {
            if let Poll::Ready(o) = t.wait(Duration::from_millis(10)) {
                return o;
            }
        }
    }

    #[test]
    fn spin_finishes() {
        runtime::init(2).unwrap();
        let t = spin(0.02, 4, false).unwrap();
        assert_eq!(wait_ready(&t), Outcome::Done("4 tasks finished".into()));
    }

    #[test]
    fn cancel_stops_quickly() {
        runtime::init(2).unwrap();
        let t = spin(30.0, 4, false).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let start = Instant::now();
        t.cancel();
        assert_eq!(wait_ready(&t), Outcome::Cancelled);
        assert!(start.elapsed() < Duration::from_millis(500), "{:?}", start.elapsed());
    }

    #[test]
    fn worker_panic_is_reported() {
        runtime::init(2).unwrap();
        let t = spin(30.0, 3, true).unwrap();
        match wait_ready(&t) {
            Outcome::Panicked(msg) => assert_eq!(msg, "datafusion_debug_worker_panic requested"),
            other => panic!("unexpected {other:?}"),
        }
        // The runtime keeps working after a worker panicked.
        let t = spin(0.01, 2, false).unwrap();
        assert_eq!(wait_ready(&t), Outcome::Done("2 tasks finished".into()));
    }
}
