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
