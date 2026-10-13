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
-- Window functions (WIN1): row_number, rank, dense_rank, and count, sum,
-- min and max over the whole partition, up to the current row's peers, or
-- up to the current row.  DataFusion sorts the rows by the partition and
-- order keys itself, in the order PostgreSQL sorts them by.  Each query runs
-- with datafusion.mode off, then on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;

-- g and a with NULLs, a with ties; f with -0, 0 and NaN; m with NaN; c of
-- one value padded two ways, as an expression without a length makes it.
CREATE TABLE df_w (k int, g int, a int, b int8, n numeric(10,2), m numeric(10,2),
  f float8, t text) DISTRIBUTED BY (k);
INSERT INTO df_w SELECT i, CASE WHEN i % 13 = 0 THEN NULL ELSE i % 7 END,
  CASE WHEN i % 17 = 0 THEN NULL ELSE i % 3 END, 4000000000000000000 + i,
  (i % 211) * 0.75, CASE WHEN i % 97 = 0 THEN 'NaN' ELSE (i % 89) * 0.5 END,
  CASE i % 5 WHEN 0 THEN '-0'::float8 WHEN 1 THEN 0 WHEN 2 THEN 'NaN' ELSE i % 3 END,
  'v' || (i % 4)
  FROM generate_series(1, 3000) i;
ANALYZE df_w;

-- Supported: a Sort below the WindowAgg is left out, and where its rows
-- leave in their order (to a merging Motion, or the result), DataFusion
-- sorts them by its keys; a window below a join is a node of the plan; the
-- top WindowAgg leaves out the rows that fail its run condition.
SET optimizer = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, a, k, rank() OVER (PARTITION BY g ORDER BY a) FROM df_w ORDER BY 1, 2, 3;
EXPLAIN (COSTS OFF) SELECT g, a, sum(n) * 100 / sum(sum(n)) OVER (PARTITION BY g) FROM df_w WHERE a < 3 GROUP BY g, a ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT x.k, x.r, y.r FROM (SELECT k, rank() OVER (ORDER BY a, k) r FROM df_w WHERE k < 40) x JOIN (SELECT k, rank() OVER (ORDER BY b DESC) r FROM df_w WHERE k < 40) y ON x.r = y.r ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY a DESC, k) r FROM df_w) s WHERE r <= 3 ORDER BY 1, 3, 2;
SET datafusion.mode = off;
SELECT g, a, k, rank() OVER w, dense_rank() OVER w, row_number() OVER (PARTITION BY g ORDER BY a, k) FROM df_w WHERE k <= 30 WINDOW w AS (PARTITION BY g ORDER BY a) ORDER BY 1, 2, 3;
SELECT g, a, k, rank() OVER (PARTITION BY g ORDER BY a DESC NULLS LAST), rank() OVER (PARTITION BY g ORDER BY a NULLS FIRST) FROM df_w WHERE k <= 30 ORDER BY 1, 2, 3;
SELECT k, f, rank() OVER (ORDER BY f), dense_rank() OVER (ORDER BY f DESC), count(*) OVER (PARTITION BY f) FROM df_w WHERE k <= 30 ORDER BY 3, 1;
SELECT g, a, k, rank() OVER (PARTITION BY g ORDER BY a) FROM df_w WHERE k <= 30 ORDER BY g, a, k DESC;
SELECT t, k, rank() OVER (PARTITION BY t ORDER BY k % 5 DESC) FROM df_w WHERE k <= 30 ORDER BY 1, 2;
SELECT g, a, k, sum(a) OVER (PARTITION BY g ORDER BY a), count(*) OVER (PARTITION BY g), count(a) OVER (PARTITION BY g ORDER BY a), sum(a) OVER (PARTITION BY g ORDER BY a, k ROWS UNBOUNDED PRECEDING) FROM df_w WHERE k <= 30 ORDER BY 1, 2, 3;
SELECT g, k, sum(b) OVER (PARTITION BY g), sum(b) OVER (PARTITION BY g ORDER BY k), sum(n) OVER (PARTITION BY g ORDER BY k), sum(m) OVER (PARTITION BY g ORDER BY k) FROM df_w WHERE k <= 100 AND g IN (1, 2) ORDER BY 1, 2;
SELECT g, k, f, min(t) OVER (PARTITION BY g), max(n) OVER (PARTITION BY g ORDER BY k ROWS UNBOUNDED PRECEDING), min(k) OVER (PARTITION BY g ORDER BY f) FROM df_w WHERE k <= 24 ORDER BY 1, 2;
SELECT k, count(*) OVER (), sum(a) OVER (), sum(f) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM df_w WHERE k <= 10 AND f = f ORDER BY 1;
SELECT c, k, count(*) OVER (PARTITION BY c), rank() OVER (ORDER BY c) FROM (SELECT k, CASE WHEN k % 2 = 0 THEN 'ab'::bpchar ELSE 'ab  '::bpchar END c FROM df_w WHERE k <= 6) s ORDER BY 2;
SELECT g, a, sum(n) * 100 / sum(sum(n)) OVER (PARTITION BY g), sum(sum(n)) OVER (PARTITION BY g ORDER BY a ROWS UNBOUNDED PRECEDING) FROM df_w WHERE a < 3 GROUP BY g, a ORDER BY 1, 2;
SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY a DESC, k) r FROM df_w) s WHERE r <= 3 ORDER BY 1, 3, 2;
SELECT * FROM (SELECT k, a, rank() OVER (ORDER BY a DESC NULLS LAST, k) r FROM df_w) s WHERE r < 5 ORDER BY 3;
SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY k) r, sum(a) OVER (PARTITION BY g ORDER BY k, a) s FROM df_w) x WHERE r < 3 ORDER BY 1, 2;
SELECT x.k, x.r, y.k, y.r FROM (SELECT k, rank() OVER (ORDER BY a, k) r FROM df_w WHERE k < 40) x JOIN (SELECT k, rank() OVER (ORDER BY b DESC) r FROM df_w WHERE k < 40) y ON x.r = y.r ORDER BY 1;
SET datafusion.mode = on;
SELECT g, a, k, rank() OVER w, dense_rank() OVER w, row_number() OVER (PARTITION BY g ORDER BY a, k) FROM df_w WHERE k <= 30 WINDOW w AS (PARTITION BY g ORDER BY a) ORDER BY 1, 2, 3;
SELECT g, a, k, rank() OVER (PARTITION BY g ORDER BY a DESC NULLS LAST), rank() OVER (PARTITION BY g ORDER BY a NULLS FIRST) FROM df_w WHERE k <= 30 ORDER BY 1, 2, 3;
SELECT k, f, rank() OVER (ORDER BY f), dense_rank() OVER (ORDER BY f DESC), count(*) OVER (PARTITION BY f) FROM df_w WHERE k <= 30 ORDER BY 3, 1;
SELECT g, a, k, rank() OVER (PARTITION BY g ORDER BY a) FROM df_w WHERE k <= 30 ORDER BY g, a, k DESC;
SELECT t, k, rank() OVER (PARTITION BY t ORDER BY k % 5 DESC) FROM df_w WHERE k <= 30 ORDER BY 1, 2;
SELECT g, a, k, sum(a) OVER (PARTITION BY g ORDER BY a), count(*) OVER (PARTITION BY g), count(a) OVER (PARTITION BY g ORDER BY a), sum(a) OVER (PARTITION BY g ORDER BY a, k ROWS UNBOUNDED PRECEDING) FROM df_w WHERE k <= 30 ORDER BY 1, 2, 3;
SELECT g, k, sum(b) OVER (PARTITION BY g), sum(b) OVER (PARTITION BY g ORDER BY k), sum(n) OVER (PARTITION BY g ORDER BY k), sum(m) OVER (PARTITION BY g ORDER BY k) FROM df_w WHERE k <= 100 AND g IN (1, 2) ORDER BY 1, 2;
SELECT g, k, f, min(t) OVER (PARTITION BY g), max(n) OVER (PARTITION BY g ORDER BY k ROWS UNBOUNDED PRECEDING), min(k) OVER (PARTITION BY g ORDER BY f) FROM df_w WHERE k <= 24 ORDER BY 1, 2;
SELECT k, count(*) OVER (), sum(a) OVER (), sum(f) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM df_w WHERE k <= 10 AND f = f ORDER BY 1;
SELECT c, k, count(*) OVER (PARTITION BY c), rank() OVER (ORDER BY c) FROM (SELECT k, CASE WHEN k % 2 = 0 THEN 'ab'::bpchar ELSE 'ab  '::bpchar END c FROM df_w WHERE k <= 6) s ORDER BY 2;
SELECT g, a, sum(n) * 100 / sum(sum(n)) OVER (PARTITION BY g), sum(sum(n)) OVER (PARTITION BY g ORDER BY a ROWS UNBOUNDED PRECEDING) FROM df_w WHERE a < 3 GROUP BY g, a ORDER BY 1, 2;
SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY a DESC, k) r FROM df_w) s WHERE r <= 3 ORDER BY 1, 3, 2;
SELECT * FROM (SELECT k, a, rank() OVER (ORDER BY a DESC NULLS LAST, k) r FROM df_w) s WHERE r < 5 ORDER BY 3;
SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY k) r, sum(a) OVER (PARTITION BY g ORDER BY k, a) s FROM df_w) x WHERE r < 3 ORDER BY 1, 2;
SELECT x.k, x.r, y.k, y.r FROM (SELECT k, rank() OVER (ORDER BY a, k) r FROM df_w WHERE k < 40) x JOIN (SELECT k, rank() OVER (ORDER BY b DESC) r FROM df_w WHERE k < 40) y ON x.r = y.r ORDER BY 1;

-- GPORCA's plans
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, a, k, rank() OVER (PARTITION BY g ORDER BY a) FROM df_w ORDER BY 1, 2, 3;
EXPLAIN (COSTS OFF) SELECT g, a, sum(n) * 100 / sum(sum(n)) OVER (PARTITION BY g) FROM df_w WHERE a < 3 GROUP BY g, a ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT x.k, x.r, y.r FROM (SELECT k, rank() OVER (ORDER BY a, k) r FROM df_w WHERE k < 40) x JOIN (SELECT k, rank() OVER (ORDER BY b DESC) r FROM df_w WHERE k < 40) y ON x.r = y.r ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY a DESC, k) r FROM df_w) s WHERE r <= 3 ORDER BY 1, 3, 2;
SET datafusion.mode = off;
SELECT g, a, k, rank() OVER w, dense_rank() OVER w, row_number() OVER (PARTITION BY g ORDER BY a, k) FROM df_w WHERE k <= 30 WINDOW w AS (PARTITION BY g ORDER BY a) ORDER BY 1, 2, 3;
SELECT g, a, k, rank() OVER (PARTITION BY g ORDER BY a DESC NULLS LAST), rank() OVER (PARTITION BY g ORDER BY a NULLS FIRST) FROM df_w WHERE k <= 30 ORDER BY 1, 2, 3;
SELECT k, f, rank() OVER (ORDER BY f), dense_rank() OVER (ORDER BY f DESC), count(*) OVER (PARTITION BY f) FROM df_w WHERE k <= 30 ORDER BY 3, 1;
SELECT g, a, k, rank() OVER (PARTITION BY g ORDER BY a) FROM df_w WHERE k <= 30 ORDER BY g, a, k DESC;
SELECT t, k, rank() OVER (PARTITION BY t ORDER BY k % 5 DESC) FROM df_w WHERE k <= 30 ORDER BY 1, 2;
SELECT g, a, k, sum(a) OVER (PARTITION BY g ORDER BY a), count(*) OVER (PARTITION BY g), count(a) OVER (PARTITION BY g ORDER BY a), sum(a) OVER (PARTITION BY g ORDER BY a, k ROWS UNBOUNDED PRECEDING) FROM df_w WHERE k <= 30 ORDER BY 1, 2, 3;
SELECT g, k, sum(b) OVER (PARTITION BY g), sum(b) OVER (PARTITION BY g ORDER BY k), sum(n) OVER (PARTITION BY g ORDER BY k), sum(m) OVER (PARTITION BY g ORDER BY k) FROM df_w WHERE k <= 100 AND g IN (1, 2) ORDER BY 1, 2;
SELECT g, k, f, min(t) OVER (PARTITION BY g), max(n) OVER (PARTITION BY g ORDER BY k ROWS UNBOUNDED PRECEDING), min(k) OVER (PARTITION BY g ORDER BY f) FROM df_w WHERE k <= 24 ORDER BY 1, 2;
SELECT k, count(*) OVER (), sum(a) OVER (), sum(f) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM df_w WHERE k <= 10 AND f = f ORDER BY 1;
SELECT c, k, count(*) OVER (PARTITION BY c), rank() OVER (ORDER BY c) FROM (SELECT k, CASE WHEN k % 2 = 0 THEN 'ab'::bpchar ELSE 'ab  '::bpchar END c FROM df_w WHERE k <= 6) s ORDER BY 2;
SELECT g, a, sum(n) * 100 / sum(sum(n)) OVER (PARTITION BY g), sum(sum(n)) OVER (PARTITION BY g ORDER BY a ROWS UNBOUNDED PRECEDING) FROM df_w WHERE a < 3 GROUP BY g, a ORDER BY 1, 2;
SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY a DESC, k) r FROM df_w) s WHERE r <= 3 ORDER BY 1, 3, 2;
SELECT * FROM (SELECT k, a, rank() OVER (ORDER BY a DESC NULLS LAST, k) r FROM df_w) s WHERE r < 5 ORDER BY 3;
SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY k) r, sum(a) OVER (PARTITION BY g ORDER BY k, a) s FROM df_w) x WHERE r < 3 ORDER BY 1, 2;
SELECT x.k, x.r, y.k, y.r FROM (SELECT k, rank() OVER (ORDER BY a, k) r FROM df_w WHERE k < 40) x JOIN (SELECT k, rank() OVER (ORDER BY b DESC) r FROM df_w WHERE k < 40) y ON x.r = y.r ORDER BY 1;
SET datafusion.mode = on;
SELECT g, a, k, rank() OVER w, dense_rank() OVER w, row_number() OVER (PARTITION BY g ORDER BY a, k) FROM df_w WHERE k <= 30 WINDOW w AS (PARTITION BY g ORDER BY a) ORDER BY 1, 2, 3;
SELECT g, a, k, rank() OVER (PARTITION BY g ORDER BY a DESC NULLS LAST), rank() OVER (PARTITION BY g ORDER BY a NULLS FIRST) FROM df_w WHERE k <= 30 ORDER BY 1, 2, 3;
SELECT k, f, rank() OVER (ORDER BY f), dense_rank() OVER (ORDER BY f DESC), count(*) OVER (PARTITION BY f) FROM df_w WHERE k <= 30 ORDER BY 3, 1;
SELECT g, a, k, rank() OVER (PARTITION BY g ORDER BY a) FROM df_w WHERE k <= 30 ORDER BY g, a, k DESC;
SELECT t, k, rank() OVER (PARTITION BY t ORDER BY k % 5 DESC) FROM df_w WHERE k <= 30 ORDER BY 1, 2;
SELECT g, a, k, sum(a) OVER (PARTITION BY g ORDER BY a), count(*) OVER (PARTITION BY g), count(a) OVER (PARTITION BY g ORDER BY a), sum(a) OVER (PARTITION BY g ORDER BY a, k ROWS UNBOUNDED PRECEDING) FROM df_w WHERE k <= 30 ORDER BY 1, 2, 3;
SELECT g, k, sum(b) OVER (PARTITION BY g), sum(b) OVER (PARTITION BY g ORDER BY k), sum(n) OVER (PARTITION BY g ORDER BY k), sum(m) OVER (PARTITION BY g ORDER BY k) FROM df_w WHERE k <= 100 AND g IN (1, 2) ORDER BY 1, 2;
SELECT g, k, f, min(t) OVER (PARTITION BY g), max(n) OVER (PARTITION BY g ORDER BY k ROWS UNBOUNDED PRECEDING), min(k) OVER (PARTITION BY g ORDER BY f) FROM df_w WHERE k <= 24 ORDER BY 1, 2;
SELECT k, count(*) OVER (), sum(a) OVER (), sum(f) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM df_w WHERE k <= 10 AND f = f ORDER BY 1;
SELECT c, k, count(*) OVER (PARTITION BY c), rank() OVER (ORDER BY c) FROM (SELECT k, CASE WHEN k % 2 = 0 THEN 'ab'::bpchar ELSE 'ab  '::bpchar END c FROM df_w WHERE k <= 6) s ORDER BY 2;
SELECT g, a, sum(n) * 100 / sum(sum(n)) OVER (PARTITION BY g), sum(sum(n)) OVER (PARTITION BY g ORDER BY a ROWS UNBOUNDED PRECEDING) FROM df_w WHERE a < 3 GROUP BY g, a ORDER BY 1, 2;
SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY a DESC, k) r FROM df_w) s WHERE r <= 3 ORDER BY 1, 3, 2;
SELECT * FROM (SELECT k, a, rank() OVER (ORDER BY a DESC NULLS LAST, k) r FROM df_w) s WHERE r < 5 ORDER BY 3;
SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY k) r, sum(a) OVER (PARTITION BY g ORDER BY k, a) s FROM df_w) x WHERE r < 3 ORDER BY 1, 2;
SELECT x.k, x.r, y.k, y.r FROM (SELECT k, rank() OVER (ORDER BY a, k) r FROM df_w WHERE k < 40) x JOIN (SELECT k, rank() OVER (ORDER BY b DESC) r FROM df_w WHERE k < 40) y ON x.r = y.r ORDER BY 1;

-- Not supported: avg, other window functions, min and max of floats (of
-- -0 and 0, PostgreSQL's keep the later), other frames, FILTER; a run
-- condition below the top WindowAgg.
SET optimizer = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, avg(n) OVER (PARTITION BY g) FROM df_w ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT g, k, ntile(3) OVER (PARTITION BY g ORDER BY k) FROM df_w ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT g, k, lag(k) OVER (PARTITION BY g ORDER BY k) FROM df_w ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT g, k, max(f) OVER (PARTITION BY g ORDER BY k) FROM df_w ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT g, k, sum(a) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM df_w ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT g, k, count(*) FILTER (WHERE a > 3) OVER (PARTITION BY g) FROM df_w ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT g, k, rank() OVER (PARTITION BY g ORDER BY k) r, count(*) OVER (PARTITION BY g ORDER BY k, a) c FROM df_w) x WHERE c < 3 ORDER BY 1, 2;

-- A window over whole partitions holds all its rows, within work_mem by
-- the planner's estimate; one up to the current row does not.
SET work_mem = '64kB';
EXPLAIN (COSTS OFF) SELECT g, k, t, n, m, f, b, sum(a) OVER (PARTITION BY g) FROM df_w ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT g, k, t, n, m, f, b, sum(a) OVER (PARTITION BY g ORDER BY k) FROM df_w ORDER BY 1, 2;
RESET work_mem;
RESET datafusion.mode;

DROP TABLE df_w;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
