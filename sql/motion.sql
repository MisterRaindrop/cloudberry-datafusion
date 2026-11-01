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
-- Slices on the segments (M5).  A slice that sends through a Motion runs in
-- DataFusion below that Motion, which keeps running on PostgreSQL and sends
-- the rows.  Partial aggregates produce PostgreSQL transition states that a
-- Finalize Aggregate combines (since M7a in DataFusion as well, see
-- receive.sql).  Each query runs with
-- datafusion.mode off, then on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
-- QEs load the library when they connect.
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;

CREATE TABLE df_m5 (a int4, b int8, c float8, d bool, e int2) DISTRIBUTED BY (a);
INSERT INTO df_m5 SELECT i, i * 10, i / 4.0, i % 2 = 0, (i % 100)::int2
FROM generate_series(1, 200000) i;
INSERT INTO df_m5 VALUES (NULL, NULL, NULL, NULL, NULL);
ANALYZE df_m5;

-- Which slices qualify.
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(a), max(c) FROM df_m5 WHERE e < 50;
EXPLAIN (COSTS OFF) SELECT e, count(*), sum(a) FROM df_m5 GROUP BY e;
EXPLAIN (COSTS OFF) SELECT a, b FROM df_m5 WHERE a % 50000 = 0;
EXPLAIN (COSTS OFF) SELECT avg(c) FROM df_m5;

-- Partial plain aggregate on the segments, finalized on the coordinator.
SET datafusion.mode = off;
SELECT count(*), count(a), sum(a), sum(e), min(c), max(c), max(b) FROM df_m5 WHERE e < 50;
SET datafusion.mode = on;
SELECT count(*), count(a), sum(a), sum(e), min(c), max(c), max(b) FROM df_m5 WHERE e < 50;

-- Partial grouped aggregate, redistributed, finalized on the segments.
SET datafusion.mode = off;
SELECT e, count(*), sum(a), min(c) FROM df_m5 WHERE e >= 95 OR e IS NULL GROUP BY e;
SET datafusion.mode = on;
SELECT e, count(*), sum(a), min(c) FROM df_m5 WHERE e >= 95 OR e IS NULL GROUP BY e;

-- Grouping by the distribution key: one stage on the segments.
SET datafusion.mode = off;
SELECT a, count(*), sum(b) FROM df_m5 WHERE a % 40000 = 0 OR a IS NULL GROUP BY a;
SET datafusion.mode = on;
SELECT a, count(*), sum(b) FROM df_m5 WHERE a % 40000 = 0 OR a IS NULL GROUP BY a;

-- A filter and projection, gathered.
SET datafusion.mode = off;
SELECT a, b, c * 2, e + 1 FROM df_m5 WHERE a % 50000 = 0;
SET datafusion.mode = on;
SELECT a, b, c * 2, e + 1 FROM df_m5 WHERE a % 50000 = 0;

-- The scan below the partial aggregate never runs on PostgreSQL.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT count(*), sum(a) FROM df_m5 WHERE e < 50;

-- Each query runs in 8 * worker threads / (slices running together *
-- primaries on the host) partitions, at least 1: on the coordinator, a
-- primary of its host with the 3 segments, of the 2 slices of a plain
-- aggregate's plan, 2 of the 2 threads; of the 3 of a join's, 1.
-- datafusion.slice_partitions fixes them, up to the threads.
SELECT count(*), sum(a) FROM df_m5 WHERE e < 50;
SELECT partitions FROM datafusion_debug_last_run();
SELECT count(*) FROM df_m5 x JOIN df_m5 y ON x.a = y.e;
SELECT partitions FROM datafusion_debug_last_run();
SET datafusion.slice_partitions = 8;
SELECT count(*) FROM df_m5 x JOIN df_m5 y ON x.a = y.e;
SELECT partitions FROM datafusion_debug_last_run();
SET datafusion.slice_partitions = 1;
SELECT count(*), sum(a) FROM df_m5 WHERE e < 50;
SELECT partitions FROM datafusion_debug_last_run();
RESET datafusion.slice_partitions;

-- AP2: a partial aggregate may produce a group more than once (the
-- aggregate above combines them, as after a Streaming Partial
-- HashAggregate whose memory is full), so DataFusion runs it as a partial
-- aggregation alone, which emits its groups when its budget (here 1 MB)
-- is full instead of spilling them.  60,000 groups on each segment overflow
-- it; the sums must match PostgreSQL's.
CREATE TABLE df_ap2 (a int4, k int4, v int8, f float8) DISTRIBUTED BY (a);
INSERT INTO df_ap2 SELECT i, i % 60000, i, i / 8.0 FROM generate_series(1, 600000) i;
ANALYZE df_ap2;
SET optimizer = on;
SET work_mem = '1MB';
EXPLAIN (COSTS OFF) SELECT count(*), sum(n), sum(s), sum(lo), sum(hi)
FROM (SELECT k, count(*) AS n, sum(v) AS s, min(f) AS lo, max(a) AS hi FROM df_ap2 GROUP BY k) g;
SET datafusion.mode = off;
SELECT count(*), sum(n), sum(s), sum(lo), sum(hi)
FROM (SELECT k, count(*) AS n, sum(v) AS s, min(f) AS lo, max(a) AS hi FROM df_ap2 GROUP BY k) g;
SET datafusion.mode = on;
SELECT count(*), sum(n), sum(s), sum(lo), sum(hi)
FROM (SELECT k, count(*) AS n, sum(v) AS s, min(f) AS lo, max(a) AS hi FROM df_ap2 GROUP BY k) g;
-- Not when the aggregate above counts the partial aggregate's groups: a
-- multi-stage DISTINCT groups by its argument below and counts it above
-- (count(a % 200000) without DISTINCT), so a group produced twice would
-- count twice.
EXPLAIN (COSTS OFF) SELECT count(*), sum(s), sum(n), sum(d)
FROM (SELECT k % 100 AS g, sum(v) AS s, count(*) AS n, count(DISTINCT a % 200000) AS d FROM df_ap2 GROUP BY 1) t;
SET datafusion.mode = off;
SELECT count(*), sum(s), sum(n), sum(d)
FROM (SELECT k % 100 AS g, sum(v) AS s, count(*) AS n, count(DISTINCT a % 200000) AS d FROM df_ap2 GROUP BY 1) t;
SET datafusion.mode = on;
SELECT count(*), sum(s), sum(n), sum(d)
FROM (SELECT k % 100 AS g, sum(v) AS s, count(*) AS n, count(DISTINCT a % 200000) AS d FROM df_ap2 GROUP BY 1) t;
RESET work_mem;
SET optimizer = off;
DROP TABLE df_ap2;

-- An error on a segment carries PostgreSQL's SQLSTATE and message.
SELECT a / (a - a) FROM df_m5 WHERE a = 5;
\set VERBOSITY sqlstate
SELECT a / (a - a) FROM df_m5 WHERE a = 5;
\set VERBOSITY default

-- A cursor closed after one row stops the segments' work.
BEGIN;
DECLARE df_cur CURSOR FOR SELECT a FROM df_m5 WHERE a % 7 = 0;
FETCH 1 FROM df_cur \gset
CLOSE df_cur;
COMMIT;
SELECT count(*) FROM df_m5 WHERE a % 7 = 0;

DROP TABLE df_m5;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
