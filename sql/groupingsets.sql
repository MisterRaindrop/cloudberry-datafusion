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
-- Grouping sets (GS1): ROLLUP, CUBE and GROUPING SETS.  DataFusion groups
-- by all of an Agg's sets, its own and its chain's, hashed, and numbers
-- each row's set as Cloudberry does: GROUPING(), GROUP_ID() and the
-- GROUPINGSET_ID() a two-stage plan groups its partial rows by.  Each
-- query runs with datafusion.mode off, then on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;

CREATE TABLE df_gs (k int, a int, b int, t text, n numeric(10,2), f float8, c char(3))
  DISTRIBUTED BY (k);
INSERT INTO df_gs SELECT i, CASE WHEN i % 11 = 0 THEN NULL ELSE i % 7 END,
  CASE WHEN i % 13 = 0 THEN NULL ELSE i % 5 END, 'v' || (i % 3), (i % 211) * 0.75,
  CASE WHEN i % 4 = 0 THEN '-0'::float8 ELSE (i % 4)::float8 END, (i % 2)::text
  FROM generate_series(1, 5000) i;
CREATE TABLE df_gse (LIKE df_gs) DISTRIBUTED BY (k);
ANALYZE df_gs;
ANALYZE df_gse;

-- Two stages: the first hashed or mixed, the second grouping by the sets'
-- columns and GROUPINGSET_ID().  An empty set gives its row on no rows too.
-- DISTINCT aggregates stay on PostgreSQL: DataFusion would count them over
-- a grouping by all columns first.
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT a, b, sum(n), count(*), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY 1, 2, 5;
EXPLAIN (COSTS OFF) SELECT a, t, sum(n), grouping(t, a) FROM df_gs GROUP BY GROUPING SETS ((a), (t)) ORDER BY 1, 2, 4;
EXPLAIN (COSTS OFF) SELECT a, b, count(DISTINCT t) FROM df_gs GROUP BY ROLLUP (a, b);
SET datafusion.mode = off;
SELECT a, b, sum(n), count(*), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY 1, 2, 5;
SELECT a, t, sum(n), grouping(t, a) FROM df_gs GROUP BY GROUPING SETS ((a), (t)) ORDER BY 1, 2, 4;
SELECT a, b, t, count(*), avg(n), min(t), max(b) FROM df_gs WHERE k < 2000 GROUP BY CUBE (a, b, t) ORDER BY 1, 2, 3, 4;
SELECT a, count(*), sum(n), group_id() FROM df_gs GROUP BY GROUPING SETS ((a), (a), ()) ORDER BY 1, 4, 2;
SELECT count(*), sum(n), group_id() FROM df_gs GROUP BY GROUPING SETS ((), ()) ORDER BY 3;
SELECT a, b, count(*), sum(n) FROM df_gse GROUP BY ROLLUP (a, b);
SELECT f, c, count(*) FROM df_gs GROUP BY ROLLUP (f, c) ORDER BY 1, 2, 3;
SELECT a, b, sum(n) FROM df_gs GROUP BY ROLLUP (a, b) HAVING grouping(a, b) > 0 OR sum(n) > 30000 ORDER BY 1, 2, 3;
SELECT x.a, x.s, y.c FROM (SELECT a, sum(n) s FROM df_gs GROUP BY ROLLUP (a)) x JOIN (SELECT a, count(*) c FROM df_gs GROUP BY a) y ON y.a = x.a ORDER BY 1, 2;
SET datafusion.mode = on;
SELECT a, b, sum(n), count(*), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY 1, 2, 5;
SELECT a, t, sum(n), grouping(t, a) FROM df_gs GROUP BY GROUPING SETS ((a), (t)) ORDER BY 1, 2, 4;
SELECT a, b, t, count(*), avg(n), min(t), max(b) FROM df_gs WHERE k < 2000 GROUP BY CUBE (a, b, t) ORDER BY 1, 2, 3, 4;
SELECT a, count(*), sum(n), group_id() FROM df_gs GROUP BY GROUPING SETS ((a), (a), ()) ORDER BY 1, 4, 2;
SELECT count(*), sum(n), group_id() FROM df_gs GROUP BY GROUPING SETS ((), ()) ORDER BY 3;
SELECT a, b, count(*), sum(n) FROM df_gse GROUP BY ROLLUP (a, b);
SELECT f, c, count(*) FROM df_gs GROUP BY ROLLUP (f, c) ORDER BY 1, 2, 3;
SELECT a, b, sum(n) FROM df_gs GROUP BY ROLLUP (a, b) HAVING grouping(a, b) > 0 OR sum(n) > 30000 ORDER BY 1, 2, 3;
SELECT x.a, x.s, y.c FROM (SELECT a, sum(n) s FROM df_gs GROUP BY ROLLUP (a)) x JOIN (SELECT a, count(*) c FROM df_gs GROUP BY a) y ON y.a = x.a ORDER BY 1, 2;

-- One stage, over a Gather.  A HAVING on grouping columns filters the
-- groups of every set, not the rows below.
SET gp_enable_multiphase_agg = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT a, b, sum(n), count(*), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY 1, 2, 5;
SET datafusion.mode = off;
SELECT a, b, sum(n), count(*), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY 1, 2, 5;
SELECT a, count(*), sum(n), group_id() FROM df_gs GROUP BY GROUPING SETS ((a), (a), ()) ORDER BY 1, 4, 2;
SELECT count(*), sum(n), group_id() FROM df_gs GROUP BY GROUPING SETS ((), ()) ORDER BY 3;
SELECT a, b, count(*), sum(n) FROM df_gse GROUP BY ROLLUP (a, b);
SELECT f, c, count(*) FROM df_gs GROUP BY ROLLUP (f, c) ORDER BY 1, 2, 3;
SELECT a, b, count(*) FROM df_gs GROUP BY ROLLUP (a, b) HAVING a = 3 OR b IS NULL ORDER BY 1, 2, 3;
SET datafusion.mode = on;
SELECT a, b, sum(n), count(*), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY 1, 2, 5;
SELECT a, count(*), sum(n), group_id() FROM df_gs GROUP BY GROUPING SETS ((a), (a), ()) ORDER BY 1, 4, 2;
SELECT count(*), sum(n), group_id() FROM df_gs GROUP BY GROUPING SETS ((), ()) ORDER BY 3;
SELECT a, b, count(*), sum(n) FROM df_gse GROUP BY ROLLUP (a, b);
SELECT f, c, count(*) FROM df_gs GROUP BY ROLLUP (f, c) ORDER BY 1, 2, 3;
SELECT a, b, count(*) FROM df_gs GROUP BY ROLLUP (a, b) HAVING a = 3 OR b IS NULL ORDER BY 1, 2, 3;

-- One sorted rollup at the top, below a Limit (D7): DataFusion sorts by
-- the keys of its input, a set's row after the longer sets', where their
-- NULLs sort last; a tie with NULLs of the data goes to the longer set
-- first, as the rollup gives them.
SET enable_hashagg = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT a, b, sum(n), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a, b LIMIT 1 OFFSET 54;
EXPLAIN (COSTS OFF) SELECT a, b, sum(n), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a DESC NULLS LAST, b DESC NULLS LAST LIMIT 9;
SET datafusion.mode = off;
SELECT a, b, sum(n), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a, b LIMIT 1 OFFSET 54;
SELECT a, b, sum(n), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a, b LIMIT 1 OFFSET 55;
SELECT a, b, sum(n), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a, b LIMIT 3 OFFSET 4;
SELECT a, b, sum(n), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a DESC NULLS LAST, b DESC NULLS LAST LIMIT 9;
SELECT t COLLATE "C", a, count(*) FROM df_gs GROUP BY ROLLUP (t COLLATE "C", a) ORDER BY 1, 2 LIMIT 4 OFFSET 5;
SET datafusion.mode = on;
SELECT a, b, sum(n), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a, b LIMIT 1 OFFSET 54;
SELECT a, b, sum(n), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a, b LIMIT 1 OFFSET 55;
SELECT a, b, sum(n), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a, b LIMIT 3 OFFSET 4;
SELECT a, b, sum(n), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a DESC NULLS LAST, b DESC NULLS LAST LIMIT 9;
SELECT t COLLATE "C", a, count(*) FROM df_gs GROUP BY ROLLUP (t COLLATE "C", a) ORDER BY 1, 2 LIMIT 4 OFFSET 5;

-- A sorted rollup whose order no one reads, as the first of two stages,
-- runs hashed without its Sort (D2).
RESET gp_enable_multiphase_agg;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT a, b, sum(n), count(*), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY 1, 2, 5;
EXPLAIN (COSTS OFF) SELECT a, b, count(*) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a NULLS FIRST, b, 3 LIMIT 8;
SET datafusion.mode = off;
SELECT a, b, sum(n), count(*), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY 1, 2, 5;
SELECT a, b, t, count(*), avg(n), min(t), max(b) FROM df_gs WHERE k < 2000 GROUP BY CUBE (a, b, t) ORDER BY 1, 2, 3, 4;
SELECT a, b, count(*) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a NULLS FIRST, b, 3 LIMIT 8;
SET datafusion.mode = on;
SELECT a, b, sum(n), count(*), grouping(a, b) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY 1, 2, 5;
SELECT a, b, t, count(*), avg(n), min(t), max(b) FROM df_gs WHERE k < 2000 GROUP BY CUBE (a, b, t) ORDER BY 1, 2, 3, 4;
SELECT a, b, count(*) FROM df_gs GROUP BY ROLLUP (a, b) ORDER BY a NULLS FIRST, b, 3 LIMIT 8;
RESET enable_hashagg;

DROP TABLE df_gs, df_gse;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
