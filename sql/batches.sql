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
-- (M7b, datafusion.motion_batches), and Redistribute Motions routing them
-- by Cloudberry's distribution hash (M7c), and split avg passing
-- DataFusion's state through them (M7d).  The senders encode their
-- results and send the bytes as tuple chunks of their own type; the
-- receivers decode them.  Each query runs with datafusion.mode off, then on;
-- the two results must match.
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

-- Redistribute Motions (M7c).  The Rust transcription of cdbhash routes
-- every row as cdbhash() does, for every key type, alone and combined, in
-- and out of parallel mode.
SELECT datafusion_debug_cdbhash_check(100000, 3, 1) AS mismatches,
       datafusion_debug_cdbhash_check(100000, 7, 2) AS mismatches_parallel;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT e, count(*), sum(a) FROM df_bat GROUP BY e;
-- A key the planner has no statistics for: every row is redistributed.
EXPLAIN (COSTS OFF) SELECT a % 1000, count(*), sum(a) FROM df_bat GROUP BY a % 1000;
SET datafusion.mode = off;
SELECT e, count(*), count(a), sum(a), min(c), max(b) FROM df_bat
WHERE e >= 95 OR e IS NULL GROUP BY e;
SELECT d, e, count(*), sum(a) FROM df_bat WHERE e > 90 OR e IS NULL GROUP BY d, e;
SELECT c, count(*) FROM df_bat WHERE a < 20 OR a IS NULL GROUP BY c;
SELECT b, count(*), sum(a) FROM df_bat GROUP BY b HAVING count(*) > 1;
SELECT a % 1000 AS k, count(*), sum(a) FROM df_bat GROUP BY a % 1000 HAVING min(a) < 5;
SELECT e, count(*) FROM df_bat_empty GROUP BY e;
SET datafusion.mode = on;
SELECT e, count(*), count(a), sum(a), min(c), max(b) FROM df_bat
WHERE e >= 95 OR e IS NULL GROUP BY e;
SELECT d, e, count(*), sum(a) FROM df_bat WHERE e > 90 OR e IS NULL GROUP BY d, e;
SELECT c, count(*) FROM df_bat WHERE a < 20 OR a IS NULL GROUP BY c;
SELECT b, count(*), sum(a) FROM df_bat GROUP BY b HAVING count(*) > 1;
SELECT a % 1000 AS k, count(*), sum(a) FROM df_bat GROUP BY a % 1000 HAVING min(a) < 5;
SELECT e, count(*) FROM df_bat_empty GROUP BY e;
SELECT a % 1000, count(*), sum(a / (a - a)) FROM df_bat GROUP BY a % 1000;

-- Parallel mode: each segment's receiving worker comes from the hash too.
SET enable_parallel = on;
SET max_parallel_workers_per_gather = 2;
SET enable_groupagg = off;
EXPLAIN (COSTS OFF) SELECT e, count(*), sum(a) FROM df_bat GROUP BY e;
SET datafusion.mode = off;
SELECT e, count(*), sum(a), max(c) FROM df_bat WHERE e < 5 GROUP BY e;
SET datafusion.mode = on;
SELECT e, count(*), sum(a), max(c) FROM df_bat WHERE e < 5 GROUP BY e;
RESET enable_parallel;
RESET max_parallel_workers_per_gather;
RESET enable_groupagg;

-- Split avg (M7d): through batch Motions the partial stage passes sum and
-- count, and the combining stage divides their sums; without batches avg
-- stays on PostgreSQL.  The values are exact in binary, so the order of the
-- additions does not show.
CREATE TABLE df_bat_avg (a int4, f real, c float8, e int2) DISTRIBUTED BY (a);
INSERT INTO df_bat_avg SELECT i, (i % 977) / 8.0, i / 4.0, (i % 100)::int2
FROM generate_series(1, 200000) i;
INSERT INTO df_bat_avg VALUES (NULL, NULL, NULL, NULL), (200001, NULL, NULL, 7);
ANALYZE df_bat_avg;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT avg(c), avg(f) FROM df_bat_avg;
EXPLAIN (COSTS OFF) SELECT e, avg(c) FROM df_bat_avg GROUP BY e;
SET datafusion.motion_batches = off;
EXPLAIN (COSTS OFF) SELECT avg(c), avg(f) FROM df_bat_avg;
SET datafusion.motion_batches = on;
EXPLAIN (COSTS OFF) SELECT avg(a) FROM df_bat_avg;
SET datafusion.mode = off;
SELECT avg(c), avg(f), count(*), sum(a), min(c) FROM df_bat_avg;
SELECT avg(c), avg(f) FROM df_bat_avg WHERE a IS NULL;
SELECT avg(c), avg(f) FROM df_bat_avg WHERE a < 0;
SELECT e, avg(c), avg(f), count(*) FROM df_bat_avg WHERE e > 95 OR e IS NULL OR e = 7 GROUP BY e;
SELECT e, avg(f) FROM df_bat_avg WHERE e < 50 GROUP BY e HAVING avg(c) > 25000;
SET datafusion.mode = on;
SELECT avg(c), avg(f), count(*), sum(a), min(c) FROM df_bat_avg;
SELECT avg(c), avg(f) FROM df_bat_avg WHERE a IS NULL;
SELECT avg(c), avg(f) FROM df_bat_avg WHERE a < 0;
SELECT e, avg(c), avg(f), count(*) FROM df_bat_avg WHERE e > 95 OR e IS NULL OR e = 7 GROUP BY e;
SELECT e, avg(f) FROM df_bat_avg WHERE e < 50 GROUP BY e HAVING avg(c) > 25000;

DROP TABLE df_bat, df_bat_empty, df_bat_avg;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
