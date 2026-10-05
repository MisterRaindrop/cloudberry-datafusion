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
