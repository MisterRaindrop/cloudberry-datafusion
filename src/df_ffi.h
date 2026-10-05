/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * df_ffi.h
 *	  C declarations for the Rust static library (rust/df_ffi).
 *
 * Every function here follows one contract:
 *   - it never calls into PostgreSQL;
 *   - it never lets a Rust panic escape;
 *   - it returns DF_OK, DF_ERROR or DF_PANIC, and writes either its result
 *     or the error text into the caller's buffer as a NUL-terminated string.
 * The caller raises any ereport only after the call has returned, so a
 * longjmp never crosses a Rust frame.
 *
 * Keep in sync with rust/df_ffi/src/lib.rs.
 *
 * src/df_ffi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DF_FFI_H
#define DF_FFI_H

#include <stddef.h>
#include <stdint.h>

#define DF_OK		0
#define DF_ERROR	1
#define DF_PANIC	2

#define DF_MSG_BUFLEN 1024

extern int32_t df_ffi_version(char *buf, size_t buflen);
extern int32_t df_ffi_debug_panic(char *buf, size_t buflen);

#endif							/* DF_FFI_H */
