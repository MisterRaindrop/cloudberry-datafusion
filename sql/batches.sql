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
--
-- Gather Motions between two DataFusion slices carrying Arrow IPC batches
-- (M7b, datafusion.motion_batches).  The senders encode their results and
-- send the bytes as tuple chunks of their own type; the receiver decodes
-- them.  Each query runs with datafusion.mode off, then on; the two results
-- must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;

CREATE TABLE df_bat (a int4, b int8, c float8, d bool, e int2) DISTRIBUTED BY (a);
INSERT INTO df_bat SELECT i, i * 10, i / 4.0, i % 2 = 0, (i % 100)::int2
FROM generate_series(1, 200000) i;
INSERT INTO df_bat VALUES (NULL, NULL, NULL, NULL, NULL);
CREATE TABLE df_bat_empty (a int4, b int8, c float8, e int2) DISTRIBUTED BY (a);
ANALYZE df_bat;

-- Which Motions carry batches: only a Gather whose receiving slice runs in
-- DataFusion too.
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(a), max(c) FROM df_bat WHERE e < 50;
EXPLAIN (COSTS OFF) SELECT e, count(*), sum(a) FROM df_bat GROUP BY e;
SET gp_enable_multiphase_agg = off;
EXPLAIN (COSTS OFF) SELECT count(*), sum(a), max(c) FROM df_bat;
SET datafusion.motion_batches = off;
EXPLAIN (COSTS OFF) SELECT count(*), sum(a), max(c) FROM df_bat;
SET datafusion.motion_batches = on;

-- Single-stage aggregates on the coordinator over every row, gathered as
-- batches.
SET datafusion.mode = off;
SELECT count(*), count(a), sum(a), sum(e), min(c), max(c), max(b) FROM df_bat;
SELECT count(*), sum(a), max(c), min(e) FROM df_bat WHERE a IS NULL OR a < 1000;
SELECT e, count(*), sum(a), min(c) FROM df_bat WHERE e >= 95 OR e IS NULL GROUP BY e;
SELECT count(*), sum(a), max(c) FROM df_bat WHERE a = 5;
SELECT count(*), count(a), sum(a), min(c), max(b) FROM df_bat_empty;
SET datafusion.mode = on;
SELECT count(*), count(a), sum(a), sum(e), min(c), max(c), max(b) FROM df_bat;
SELECT count(*), sum(a), max(c), min(e) FROM df_bat WHERE a IS NULL OR a < 1000;
SELECT e, count(*), sum(a), min(c) FROM df_bat WHERE e >= 95 OR e IS NULL GROUP BY e;
-- One sender (direct dispatch), and senders without rows.
SELECT count(*), sum(a), max(c) FROM df_bat WHERE a = 5;
SELECT count(*), count(a), sum(a), min(c), max(b) FROM df_bat_empty;
RESET gp_enable_multiphase_agg;

-- Partial states gathered as batches and combined on the coordinator.
SET datafusion.mode = off;
SELECT count(*), count(a), sum(a), min(c), max(b) FROM df_bat WHERE e < 50;
SET datafusion.mode = on;
SELECT count(*), count(a), sum(a), min(c), max(b) FROM df_bat WHERE e < 50;

-- Several senders per segment in Cloudberry's parallel mode.
SET gp_enable_multiphase_agg = off;
SET enable_parallel = on;
SET max_parallel_workers_per_gather = 2;
ALTER TABLE df_bat SET (parallel_workers = 2);
EXPLAIN (COSTS OFF) SELECT count(*), sum(a), max(c) FROM df_bat;
SET datafusion.mode = off;
SELECT count(*), sum(a), max(c) FROM df_bat;
SET datafusion.mode = on;
SELECT count(*), sum(a), max(c) FROM df_bat;
RESET enable_parallel;
RESET max_parallel_workers_per_gather;

-- Errors on a sender and on the receiver carry PostgreSQL's message.
SELECT count(*), sum(a / (a - a)) FROM df_bat WHERE a = 5;
SELECT count(*) FROM df_bat HAVING 1 / (count(*) - 200001) > 0;

-- A cursor, and a query after the errors.
BEGIN;
DECLARE df_cur CURSOR FOR SELECT count(*), sum(a) FROM df_bat;
FETCH 1 FROM df_cur;
CLOSE df_cur;
COMMIT;
SELECT count(*), sum(a) FROM df_bat WHERE e < 10;
RESET gp_enable_multiphase_agg;

DROP TABLE df_bat, df_bat_empty;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
