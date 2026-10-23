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
-- Subtrees of a slice below PostgreSQL's nodes (T2).  When a slice as a
-- whole cannot run, DataFusion runs its highest subtrees that can, in
-- children their parents run once, and PostgreSQL's nodes above pull their
-- rows.  Each query runs with datafusion.mode off, then on; the two
-- results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;

CREATE TABLE df_pt (id int, k int, v int8, t text) DISTRIBUTED BY (id);
INSERT INTO df_pt SELECT i, i % 13, i * 7, 'k' || (i % 31) FROM generate_series(1, 30000) i;
CREATE TABLE df_pt2 (id int, w int) DISTRIBUTED BY (id);
INSERT INTO df_pt2 SELECT i, i % 5 FROM generate_series(1, 40) i;
ANALYZE df_pt;
ANALYZE df_pt2;

SET datafusion.mode = explain;
-- a Limit WITH TIES stays, the Sort and aggregate below run
EXPLAIN (COSTS OFF) SELECT k, count(*) FROM df_pt GROUP BY k ORDER BY 2 DESC FETCH FIRST 2 ROWS WITH TIES;
-- each child of an Append
EXPLAIN (COSTS OFF) SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k
UNION ALL SELECT w, count(*) FROM df_pt2 GROUP BY w;
-- Below a Nested Loop only its outer side may run, never the inner side,
-- which it rescans per row.  With sorts the GroupAggregates read their
-- input's order, which DataFusion running the outer aggregate hashed would
-- not keep; without, the Nested Loop runs in DataFusion (NL1).
SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS OFF) SELECT a.k, count(*) FROM (SELECT k, count(*) AS c FROM df_pt GROUP BY k) a
JOIN df_pt2 b ON a.c > b.w * 500 GROUP BY a.k;
SET enable_sort = off;
EXPLAIN (COSTS OFF) SELECT a.k, count(*) FROM (SELECT k, count(*) AS c FROM df_pt GROUP BY k) a
JOIN df_pt2 b ON a.c > b.w * 500 GROUP BY a.k;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_sort;
-- ORCA's Result on top, which runs in DataFusion since R1
SET optimizer = on;
EXPLAIN (COSTS OFF) SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k HAVING count(*) > 10 ORDER BY k LIMIT 5;
SET optimizer = off;

SET datafusion.mode = off;
SELECT k, count(*) FROM df_pt GROUP BY k ORDER BY 2 DESC, 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k UNION ALL SELECT w, count(*) FROM df_pt2 GROUP BY w ORDER BY 1, 2;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_sort = off;
SELECT a.k, count(*) FROM (SELECT k, count(*) AS c FROM df_pt GROUP BY k) a
JOIN df_pt2 b ON a.c > b.w * 500 GROUP BY a.k ORDER BY 1;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_sort;
SET optimizer = on;
SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k HAVING count(*) > 10 ORDER BY k LIMIT 5;
SET optimizer = off;
SET datafusion.mode = on;
SELECT k, count(*) FROM df_pt GROUP BY k ORDER BY 2 DESC, 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k UNION ALL SELECT w, count(*) FROM df_pt2 GROUP BY w ORDER BY 1, 2;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_sort = off;
SELECT a.k, count(*) FROM (SELECT k, count(*) AS c FROM df_pt GROUP BY k) a
JOIN df_pt2 b ON a.c > b.w * 500 GROUP BY a.k ORDER BY 1;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_sort;
SET optimizer = on;
SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k HAVING count(*) > 10 ORDER BY k LIMIT 5;
SET optimizer = off;

-- GPORCA's Shared Scan (a CTE read twice, a ROLLUP): the slice of its
-- Sequence runs in DataFusion, which reads the Sequence's last input;
-- PostgreSQL first runs the producer, as the Sequence would, and the
-- producer reads its input, run in DataFusion below it, once into the
-- store its readers share, in other slices (the first) or its own (the
-- second).  A reader stays on PostgreSQL, and DataFusion pulls its rows
-- for what computes on them (the join in the second), or for a Motion
-- carrying batches (the first's and the third's readers, right below
-- their Motions in the third).  The ROLLUP's grand total comes from a Motion with nothing
-- computed on it, read as an input of the union of the other levels.
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) WITH c AS (SELECT a.k, b.w, a.v FROM df_pt a JOIN df_pt2 b ON a.id = b.id * 700)
SELECT x.k, count(*), sum(x.v), sum(y.w) FROM c x JOIN c y ON x.k = y.w GROUP BY x.k;
EXPLAIN (COSTS OFF) WITH c AS (SELECT a.t, count(*) AS n, sum(a.v) AS s FROM df_pt a JOIN df_pt2 b ON a.id = b.id * 700 GROUP BY a.t)
SELECT x.t, x.n, y.s FROM c x JOIN c y ON x.t = y.t;
EXPLAIN (COSTS OFF) WITH c AS (SELECT a.k, b.w, a.v FROM df_pt a JOIN df_pt2 b ON a.id = b.id * 700)
SELECT x.k, x.w, x.v, y.k, y.w, y.v FROM c x JOIN c y ON x.k = y.w;
EXPLAIN (COSTS OFF) SELECT k, t, count(*), sum(v) FROM df_pt GROUP BY ROLLUP (k, t);
-- the Sort over the Sequence only sorts the rows of the coordinator's
-- Limit, sent back to the segments: nothing to compute on them (TPC-DS Q1)
EXPLAIN (COSTS OFF) WITH c AS (SELECT id % 500 AS g, k, sum(v) AS s FROM df_pt GROUP BY id % 500, k)
SELECT x.g, x.s FROM c x, (SELECT k, avg(s) AS a FROM c GROUP BY k) y
WHERE x.k = y.k AND x.s > y.a ORDER BY x.g, x.s LIMIT 4;
-- a UNION ALL as the producer's input runs in DataFusion as a whole: the
-- readers do not read the order its inputs come in (TPC-DS Q11)
EXPLAIN (COSTS OFF) WITH c AS (SELECT 's' AS src, k, sum(v) AS s FROM df_pt GROUP BY k
  UNION ALL SELECT 'w', w, count(*) FROM df_pt2 GROUP BY w)
SELECT x.k, x.s, y.s FROM c x JOIN c y ON x.k = y.k AND x.src = 's' AND y.src = 'w' ORDER BY x.k LIMIT 3;
SET datafusion.mode = off;
WITH c AS (SELECT a.k, b.w, a.v FROM df_pt a JOIN df_pt2 b ON a.id = b.id * 700)
SELECT x.k, count(*), sum(x.v), sum(y.w) FROM c x JOIN c y ON x.k = y.w GROUP BY x.k ORDER BY 1;
WITH c AS (SELECT a.t, count(*) AS n, sum(a.v) AS s FROM df_pt a JOIN df_pt2 b ON a.id = b.id * 700 GROUP BY a.t)
SELECT x.t, x.n, y.s FROM c x JOIN c y ON x.t = y.t ORDER BY 1;
WITH c AS (SELECT a.k, b.w, a.v FROM df_pt a JOIN df_pt2 b ON a.id = b.id * 700)
SELECT x.k, x.w, x.v, y.k, y.w, y.v FROM c x JOIN c y ON x.k = y.w ORDER BY 1, 2, 3, 4, 5, 6;
SELECT k, t, count(*), sum(v) FROM df_pt GROUP BY ROLLUP (k, t) ORDER BY 1, 2 LIMIT 5;
WITH c AS (SELECT id % 500 AS g, k, sum(v) AS s FROM df_pt GROUP BY id % 500, k)
SELECT x.g, x.s FROM c x, (SELECT k, avg(s) AS a FROM c GROUP BY k) y
WHERE x.k = y.k AND x.s > y.a ORDER BY x.g, x.s LIMIT 4;
WITH c AS (SELECT 's' AS src, k, sum(v) AS s FROM df_pt GROUP BY k
  UNION ALL SELECT 'w', w, count(*) FROM df_pt2 GROUP BY w)
SELECT x.k, x.s, y.s FROM c x JOIN c y ON x.k = y.k AND x.src = 's' AND y.src = 'w' ORDER BY x.k LIMIT 3;
SET datafusion.mode = on;
WITH c AS (SELECT a.k, b.w, a.v FROM df_pt a JOIN df_pt2 b ON a.id = b.id * 700)
SELECT x.k, count(*), sum(x.v), sum(y.w) FROM c x JOIN c y ON x.k = y.w GROUP BY x.k ORDER BY 1;
WITH c AS (SELECT a.t, count(*) AS n, sum(a.v) AS s FROM df_pt a JOIN df_pt2 b ON a.id = b.id * 700 GROUP BY a.t)
SELECT x.t, x.n, y.s FROM c x JOIN c y ON x.t = y.t ORDER BY 1;
WITH c AS (SELECT a.k, b.w, a.v FROM df_pt a JOIN df_pt2 b ON a.id = b.id * 700)
SELECT x.k, x.w, x.v, y.k, y.w, y.v FROM c x JOIN c y ON x.k = y.w ORDER BY 1, 2, 3, 4, 5, 6;
SELECT k, t, count(*), sum(v) FROM df_pt GROUP BY ROLLUP (k, t) ORDER BY 1, 2 LIMIT 5;
WITH c AS (SELECT id % 500 AS g, k, sum(v) AS s FROM df_pt GROUP BY id % 500, k)
SELECT x.g, x.s FROM c x, (SELECT k, avg(s) AS a FROM c GROUP BY k) y
WHERE x.k = y.k AND x.s > y.a ORDER BY x.g, x.s LIMIT 4;
WITH c AS (SELECT 's' AS src, k, sum(v) AS s FROM df_pt GROUP BY k
  UNION ALL SELECT 'w', w, count(*) FROM df_pt2 GROUP BY w)
SELECT x.k, x.s, y.s FROM c x JOIN c y ON x.k = y.k AND x.src = 's' AND y.src = 'w' ORDER BY x.k LIMIT 3;
SET optimizer = off;

-- Numeric values of display scales of their own (an avg, a sum of
-- COALESCE branches of different scales) keep them through a Shared Scan:
-- read from a reader's tuples, as from a Motion's; and a Motion of a
-- reader's rows (redistributed by n) carries tuples, as its producer's
-- input decides for both of its ends.
CREATE TABLE df_ps (id int, k int, a numeric(9,2), r numeric(9,2)) DISTRIBUTED BY (id);
INSERT INTO df_ps SELECT i, i % 11, round((i % 1000) / 7.0, 2),
  CASE WHEN i % 3 = 0 THEN NULL ELSE (i % 50) / 10.0 END
FROM generate_series(1, 30000) i;
ANALYZE df_ps;
SET optimizer = on;
SET datafusion.mode = off;
WITH c AS (SELECT k, avg(a) AS m FROM df_ps GROUP BY k) SELECT x.k, x.m, y.m FROM c x JOIN c y ON x.k = y.k ORDER BY 1;
WITH c AS (SELECT k, sum(a - COALESCE(r, 0.0)) AS s, count(*) % 11 AS n FROM df_ps GROUP BY k)
SELECT x.k, x.s, y.s - x.s, x.n FROM c x JOIN c y ON x.k = y.n ORDER BY 1, 2, 3;
SET datafusion.mode = on;
WITH c AS (SELECT k, avg(a) AS m FROM df_ps GROUP BY k) SELECT x.k, x.m, y.m FROM c x JOIN c y ON x.k = y.k ORDER BY 1;
WITH c AS (SELECT k, sum(a - COALESCE(r, 0.0)) AS s, count(*) % 11 AS n FROM df_ps GROUP BY k)
SELECT x.k, x.s, y.s - x.s, x.n FROM c x JOIN c y ON x.k = y.n ORDER BY 1, 2, 3;
SET optimizer = off;
DROP TABLE df_ps;

-- GPORCA combines partial aggregates after a join (CS1, TPC-DS Q95): the
-- partial stage groups one side by the join key, the join may repeat its
-- rows, and the combining stage adds up their counts.  Where every state
-- is a plain value (count, min, max, a sum of integers), DataFusion reads
-- it through the join as any column.  CS2: other states (an avg's, a sum
-- of numeric or int8, TPC-DS Q23) are read from the Motion below the join
-- as DataFusion's columns, from batches or from PostgreSQL's states in
-- tuples (PS1).
CREATE TABLE df_cs_s (o int, w int, a numeric(7,2), b int8) DISTRIBUTED BY (w);
CREATE TABLE df_cs_r (o int, x int) DISTRIBUTED BY (x);
INSERT INTO df_cs_s SELECT i % 3000, CASE WHEN i % 23 = 0 THEN NULL ELSE i % 7 END,
  CASE WHEN i % 29 = 0 THEN NULL ELSE (i % 1000) * 0.37 END, i::int8 * 100000000 FROM generate_series(1, 60000) i;
INSERT INTO df_cs_r SELECT i * 3, i FROM generate_series(1, 400) i;
INSERT INTO df_cs_r VALUES (3, 401), (6, 402), (6, 403);
ANALYZE df_cs_s;
ANALYZE df_cs_r;
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT r.o, count(*), count(s.w), sum(s.w), min(s.w), max(s.w) FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o;
EXPLAIN (COSTS OFF) SELECT r.o, count(*), avg(s.w) FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o;
SET datafusion.mode = off;
SELECT r.o, count(*), count(s.w), sum(s.w), min(s.w), max(s.w) FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o ORDER BY r.o LIMIT 5;
SELECT count(*), sum(c), sum(cw), sum(sw), min(mn), max(mx) FROM (SELECT r.o, count(*) c, count(s.w) cw, sum(s.w) sw, min(s.w) mn, max(s.w) mx FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o) t;
SET datafusion.mode = on;
SELECT r.o, count(*), count(s.w), sum(s.w), min(s.w), max(s.w) FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o ORDER BY r.o LIMIT 5;
SELECT count(*), sum(c), sum(cw), sum(sw), min(mn), max(mx) FROM (SELECT r.o, count(*) c, count(s.w) cw, sum(s.w) sw, min(s.w) mn, max(s.w) mx FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o) t;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT max(t), min(t), sum(t) FROM (SELECT r.o, sum(s.a) t FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o) q;
SET datafusion.motion_batches = off;
EXPLAIN (COSTS OFF) SELECT max(t), min(t), sum(t) FROM (SELECT r.o, sum(s.a) t FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o) q;
SET datafusion.mode = off;
SELECT r.o, sum(s.a), avg(s.a), sum(s.b), avg(s.w), avg(s.b) FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o ORDER BY r.o LIMIT 5;
SELECT max(t), min(t), sum(t), sum(m), sum(n) FROM (SELECT r.o, sum(s.a) t, avg(s.a) m, sum(s.b) n FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o) q;
SET datafusion.mode = on;
SELECT r.o, sum(s.a), avg(s.a), sum(s.b), avg(s.w), avg(s.b) FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o ORDER BY r.o LIMIT 5;
SELECT max(t), min(t), sum(t), sum(m), sum(n) FROM (SELECT r.o, sum(s.a) t, avg(s.a) m, sum(s.b) n FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o) q;
SET datafusion.motion_batches = on;
SELECT r.o, sum(s.a), avg(s.a), sum(s.b), avg(s.w), avg(s.b) FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o ORDER BY r.o LIMIT 5;
SELECT max(t), min(t), sum(t), sum(m), sum(n) FROM (SELECT r.o, sum(s.a) t, avg(s.a) m, sum(s.b) n FROM df_cs_r r JOIN df_cs_s s ON r.o = s.o GROUP BY r.o) q;
SET optimizer = off;
DROP TABLE df_cs_s, df_cs_r;

-- RS1: a slice that only passes on rows a Shared Scan reader reads,
-- filtered, to a Motion of tuples (an avg's columns go as tuples) is
-- reported as passing them on with nothing else to compute (GPORCA's
-- TPC-DS Q4, Q74, Q80).
CREATE TABLE df_rs (id int, c int, y int, a numeric(7,2)) DISTRIBUTED BY (id);
INSERT INTO df_rs SELECT i, i % 500, 2000 + i % 3, (i % 977) * 0.13 FROM generate_series(1, 30000) i;
ANALYZE df_rs;
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) WITH t AS (SELECT c, y, sum(a) s, avg(a) m FROM df_rs GROUP BY c, y)
SELECT t1.c, t1.s, t2.s, t2.m / t1.m FROM t t1 JOIN t t2 ON t1.c = t2.c JOIN t t3 ON t1.c = t3.c
WHERE t1.y = 2000 AND t2.y = 2001 AND t3.y = 2002 AND t2.m > t1.m ORDER BY 1 LIMIT 5;
SET datafusion.mode = off;
WITH t AS (SELECT c, y, sum(a) s, avg(a) m FROM df_rs GROUP BY c, y)
SELECT t1.c, t1.s, t2.s, t2.m / t1.m FROM t t1 JOIN t t2 ON t1.c = t2.c JOIN t t3 ON t1.c = t3.c
WHERE t1.y = 2000 AND t2.y = 2001 AND t3.y = 2002 AND t2.m > t1.m ORDER BY 1 LIMIT 5;
SET datafusion.mode = on;
WITH t AS (SELECT c, y, sum(a) s, avg(a) m FROM df_rs GROUP BY c, y)
SELECT t1.c, t1.s, t2.s, t2.m / t1.m FROM t t1 JOIN t t2 ON t1.c = t2.c JOIN t t3 ON t1.c = t3.c
WHERE t1.y = 2000 AND t2.y = 2001 AND t3.y = 2002 AND t2.m > t1.m ORDER BY 1 LIMIT 5;
SET optimizer = off;
DROP TABLE df_rs;

DROP TABLE df_pt, df_pt2;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
