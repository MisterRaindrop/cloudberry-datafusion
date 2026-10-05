-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

/* datafusion_executor--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION datafusion_executor" to load this file. \quit

-- Version of the DataFusion library linked into this extension.
CREATE FUNCTION datafusion_version()
RETURNS text
AS 'MODULE_PATHNAME', 'datafusion_version'
LANGUAGE C STRICT;

-- Test hook: panics inside Rust.  The panic must come back as an ordinary
-- ERROR and leave the backend usable.
CREATE FUNCTION datafusion_debug_panic()
RETURNS void
AS 'MODULE_PATHNAME', 'datafusion_debug_panic'
LANGUAGE C VOLATILE;

REVOKE ALL ON FUNCTION datafusion_debug_panic() FROM PUBLIC;

-- Runtime test functions (M1).  They run CPU-bound work on the backend's
-- DataFusion runtime so tests can check cancellation, panics inside worker
-- threads, and the workers' signal masks.

-- Spin on ntasks runtime tasks for the given number of seconds.
CREATE FUNCTION datafusion_debug_spin(seconds float8, ntasks int DEFAULT 4)
RETURNS text
AS 'MODULE_PATHNAME', 'datafusion_debug_spin'
LANGUAGE C STRICT VOLATILE;

-- Panic inside a runtime worker thread.
CREATE FUNCTION datafusion_debug_worker_panic()
RETURNS text
AS 'MODULE_PATHNAME', 'datafusion_debug_worker_panic'
LANGUAGE C VOLATILE;

-- Number of debug tasks still running on this backend's runtime.
CREATE FUNCTION datafusion_debug_active_tasks()
RETURNS bigint
AS 'MODULE_PATHNAME', 'datafusion_debug_active_tasks'
LANGUAGE C VOLATILE;

-- This backend's runtime threads, and whether all of them block the signals
-- PostgreSQL handles on the main thread.
CREATE FUNCTION datafusion_debug_runtime_threads(OUT threads int, OUT signals_blocked bool)
RETURNS record
AS 'MODULE_PATHNAME', 'datafusion_debug_runtime_threads'
LANGUAGE C VOLATILE;

REVOKE ALL ON FUNCTION datafusion_debug_spin(float8, int) FROM PUBLIC;
REVOKE ALL ON FUNCTION datafusion_debug_worker_panic() FROM PUBLIC;
REVOKE ALL ON FUNCTION datafusion_debug_active_tasks() FROM PUBLIC;
REVOKE ALL ON FUNCTION datafusion_debug_runtime_threads() FROM PUBLIC;

-- Hook test function (M2): number of times this backend routed a slice to
-- DataFusion.
CREATE FUNCTION datafusion_debug_takeovers()
RETURNS bigint
AS 'MODULE_PATHNAME', 'datafusion_debug_takeovers'
LANGUAGE C VOLATILE;

REVOKE ALL ON FUNCTION datafusion_debug_takeovers() FROM PUBLIC;

-- Memory test functions (M4): figures of the latest slice DataFusion ran to
-- the end in this backend, and the backend's vmem lease for Rust's heap.
CREATE FUNCTION datafusion_debug_last_run(OUT partitions int8, OUT memory_limit_kb int8,
                                          OUT memory_peak_kb int8, OUT spilled_kb int8,
                                          OUT spills int8)
RETURNS record
AS 'MODULE_PATHNAME', 'datafusion_debug_last_run'
LANGUAGE C VOLATILE;

CREATE FUNCTION datafusion_debug_vmem(OUT heap_bytes int8, OUT leased_bytes int8)
RETURNS record
AS 'MODULE_PATHNAME', 'datafusion_debug_vmem'
LANGUAGE C VOLATILE;

-- Run one vmem lease step with the given headroom; returns the bytes leased.
CREATE FUNCTION datafusion_debug_vmem_lease(headroom int8)
RETURNS int8
AS 'MODULE_PATHNAME', 'datafusion_debug_vmem_lease'
LANGUAGE C STRICT VOLATILE;

REVOKE ALL ON FUNCTION datafusion_debug_last_run() FROM PUBLIC;
REVOKE ALL ON FUNCTION datafusion_debug_vmem() FROM PUBLIC;
REVOKE ALL ON FUNCTION datafusion_debug_vmem_lease(int8) FROM PUBLIC;

-- Number of scans in this backend that read PAX micro-partitions directly
-- (datafusion.pax_direct_read).
CREATE FUNCTION datafusion_debug_pax_direct_scans()
RETURNS bigint
AS 'MODULE_PATHNAME', 'datafusion_debug_pax_direct_scans'
LANGUAGE C VOLATILE;

REVOKE ALL ON FUNCTION datafusion_debug_pax_direct_scans() FROM PUBLIC;

-- The latest completed direct PAX scan in this backend: micro-partitions
-- and groups skipped by their min/max statistics, and the peak memory PAX's
-- reader held while decoding.  NULLs if the latest run was not one.
CREATE FUNCTION datafusion_debug_last_pax(OUT files int8, OUT files_skipped int8,
                                          OUT groups int8, OUT groups_skipped int8,
                                          OUT decode_peak_kb int8)
RETURNS record
AS 'MODULE_PATHNAME', 'datafusion_debug_last_pax'
LANGUAGE C VOLATILE;

REVOKE ALL ON FUNCTION datafusion_debug_last_pax() FROM PUBLIC;

-- Rows (of nrows random keys per key-type combination) that the Rust
-- transcription of Cloudberry's distribution hash routes differently from
-- cdbhash(); must be 0 (M7c).
CREATE FUNCTION datafusion_debug_cdbhash_check(nrows int4, segments int4, workers int4)
RETURNS bigint
AS 'MODULE_PATHNAME', 'datafusion_debug_cdbhash_check'
LANGUAGE C VOLATILE STRICT;

REVOKE ALL ON FUNCTION datafusion_debug_cdbhash_check(int4, int4, int4) FROM PUBLIC;
