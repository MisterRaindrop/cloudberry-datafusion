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
