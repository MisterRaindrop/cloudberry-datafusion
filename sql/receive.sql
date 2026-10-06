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
-- Slices that receive rows through a Motion (M7a).  The Motion keeps
-- running on PostgreSQL; DataFusion aggregates the rows it receives, which
-- lets it run the combining stage (Finalize Aggregate) of count, sum, min
-- and max on the coordinator and on the segments.  Each query runs with
-- datafusion.mode off, then on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;

CREATE TABLE df_rcv (a int4, b int8, c float8, d bool, e int2) DISTRIBUTED BY (a);
INSERT INTO df_rcv SELECT i, i * 10, i / 4.0, i % 2 = 0, (i % 100)::int2
FROM generate_series(1, 200000) i;
INSERT INTO df_rcv VALUES (NULL, NULL, NULL, NULL, NULL);
CREATE TABLE df_rcv_empty (a int4, b int8, c float8, e int2) DISTRIBUTED BY (a);
ANALYZE df_rcv;

-- Which slices qualify.
SET datafusion.mode = explain;
-- Both stages: partial on the segments, combining on the coordinator.
EXPLAIN (COSTS OFF) SELECT count(*), sum(a), max(c) FROM df_rcv WHERE e < 50;
-- The combining stage on the segments, between two Motions.
EXPLAIN (COSTS OFF) SELECT e, count(*), sum(a) FROM df_rcv GROUP BY e;
-- avg's transition state is an array; DISTINCT aggregates nest two
-- aggregates; a slice that only receives has nothing to compute.
EXPLAIN (COSTS OFF) SELECT avg(c) FROM df_rcv;
EXPLAIN (COSTS OFF) SELECT count(DISTINCT e) FROM df_rcv;
EXPLAIN (COSTS OFF) SELECT a, b FROM df_rcv WHERE a % 50000 = 0;

-- Combining on the coordinator.
SET datafusion.mode = off;
SELECT count(*), count(a), sum(a), sum(e), min(c), max(c), max(b) FROM df_rcv WHERE e < 50;
SET datafusion.mode = on;
SELECT count(*), count(a), sum(a), sum(e), min(c), max(c), max(b) FROM df_rcv WHERE e < 50;
SELECT datafusion_debug_takeovers() AS coordinator_takeovers;

-- Combining on the segments, with HAVING.
SET datafusion.mode = off;
SELECT e, count(*), count(a), sum(a), min(c), max(b) FROM df_rcv
WHERE e >= 95 OR e IS NULL GROUP BY e;
SELECT e, count(*) FROM df_rcv GROUP BY e HAVING count(*) >= 2000 AND sum(a) > 200090000;
SET datafusion.mode = on;
SELECT e, count(*), count(a), sum(a), min(c), max(b) FROM df_rcv
WHERE e >= 95 OR e IS NULL GROUP BY e;
SELECT e, count(*) FROM df_rcv GROUP BY e HAVING count(*) >= 2000 AND sum(a) > 200090000;

-- No rows: count is 0, the other aggregates NULL.
SET datafusion.mode = off;
SELECT count(*), count(a), sum(a), min(c), max(b) FROM df_rcv_empty;
SELECT count(*), sum(e), max(c) FROM df_rcv WHERE a IS NULL;
SELECT count(*) FROM df_rcv HAVING count(*) > 1000000;
SET datafusion.mode = on;
SELECT count(*), count(a), sum(a), min(c), max(b) FROM df_rcv_empty;
SELECT count(*), sum(e), max(c) FROM df_rcv WHERE a IS NULL;
SELECT count(*) FROM df_rcv HAVING count(*) > 1000000;
SELECT e, count(*), sum(a) FROM df_rcv_empty GROUP BY e;

-- GPORCA's plans.
SET optimizer = on;
SET datafusion.mode = off;
SELECT count(*), count(a), sum(a), min(c), max(b) FROM df_rcv WHERE e < 50;
SELECT e, count(*), sum(a), min(c) FROM df_rcv WHERE e >= 95 OR e IS NULL GROUP BY e;
SET datafusion.mode = on;
SELECT count(*), count(a), sum(a), min(c), max(b) FROM df_rcv WHERE e < 50;
SELECT e, count(*), sum(a), min(c) FROM df_rcv WHERE e >= 95 OR e IS NULL GROUP BY e;
SET optimizer = off;

-- A cursor over the coordinator's slice.
BEGIN;
DECLARE df_cur CURSOR FOR SELECT count(*), sum(a) FROM df_rcv WHERE e < 50;
FETCH 1 FROM df_cur;
FETCH 1 FROM df_cur;
CLOSE df_cur;
COMMIT;

-- A plain aggregate gets work_mem, not the resource queue's 100 kB for a
-- light operator, and does not spill its batches.
SELECT count(*), sum(a) FROM df_rcv WHERE e < 50;
SELECT memory_limit_kb = pg_size_bytes(current_setting('work_mem')) / 1024 AS work_mem_budget,
       spills
FROM datafusion_debug_last_run();

-- A partial aggregate whose output no one reads (count(*) of a subquery
-- without columns) still makes its one row per segment.
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*) FROM (SELECT count(*) FROM df_rcv x JOIN df_rcv y ON x.a = y.e) s;
SET datafusion.mode = off;
SELECT count(*) FROM (SELECT count(*) FROM df_rcv x JOIN df_rcv y ON x.a = y.e) s;
SET datafusion.mode = on;
SELECT count(*) FROM (SELECT count(*) FROM df_rcv x JOIN df_rcv y ON x.a = y.e) s;
RESET datafusion.motion_batches;

DROP TABLE df_rcv, df_rcv_empty;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
