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
-- Hash joins (J2-J4): inner, outer, semi and anti, both sides scanned
-- locally (tables colocated on the join key) or received through a
-- Redistribute or Broadcast Motion, under an optional aggregate.  DataFusion builds its hash table on
-- PostgreSQL's Hash side, which is also fed first.
-- Each query runs with datafusion.mode off, then on; the results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
SET enable_nestloop = off;
SET enable_mergejoin = off;

CREATE TABLE df_ja (k int4, a int4, c float8) DISTRIBUTED BY (k);
CREATE TABLE df_jb (k int4, b int8, e int2) DISTRIBUTED BY (k);
CREATE TABLE df_jc (k int4, d int8) DISTRIBUTED BY (k);
CREATE TABLE df_jempty (k int4, d int8) DISTRIBUTED BY (k);
INSERT INTO df_ja SELECT i, i % 100, i / 4.0 FROM generate_series(1, 20000) i;
INSERT INTO df_ja VALUES (NULL, 1, 1.0);
INSERT INTO df_jb SELECT i * 2, i * 10, (i % 50)::int2 FROM generate_series(1, 10000) i;
INSERT INTO df_jb VALUES (NULL, 1, 1);
INSERT INTO df_jc SELECT i * 3, i FROM generate_series(1, 7000) i;
-- duplicate keys on both sides
CREATE TABLE df_jdup (k int4, v int4) DISTRIBUTED BY (k);
INSERT INTO df_jdup SELECT i % 10, i FROM generate_series(1, 100) i;
ANALYZE df_ja; ANALYZE df_jb; ANALYZE df_jc; ANALYZE df_jempty; ANALYZE df_jdup;

-- Which slices qualify.
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(ja.a) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k WHERE jb.e < 10;
EXPLAIN (COSTS OFF) SELECT ja.k, jb.b + jc.d FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k JOIN df_jc jc ON ja.k = jc.k;
-- This stays on PostgreSQL, with the reason.
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ja ja WHERE ja.k NOT IN (SELECT k FROM df_jb);
-- Keys cast to floats run since E2 (int4 and int8 to float8 and real).
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k::real;
EXPLAIN (COSTS OFF) SELECT count(*), count(s.isn) FROM df_ja ja
LEFT JOIN (SELECT k, b IS NULL AS isn FROM df_jb) s ON ja.k = s.k;
SET work_mem = '64kB';
-- A build side past the Hash node's budget stays on PostgreSQL unless hash
-- joins spill (datafusion.join_estimates = spill, the default).
SET datafusion.join_estimates = trusted;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k;
RESET datafusion.join_estimates;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k;
RESET work_mem;

SET datafusion.mode = off;
SELECT count(*), sum(ja.a), max(jb.b) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k WHERE jb.e < 10;
SELECT ja.k, ja.c, jb.b FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k AND ja.a > jb.e WHERE ja.c < 10;
SELECT count(*), sum(jb.b + jc.d) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k JOIN df_jc jc ON ja.k = jc.k;
SELECT jb.e, count(*), sum(ja.c) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k WHERE jb.e < 5 GROUP BY jb.e;
SELECT count(*) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k WHERE ja.a + jb.e > 100;
SELECT count(*) FROM df_ja ja JOIN df_jb jb ON ja.k + 1 = jb.k + 1;
SELECT count(*), sum(x.v), sum(y.v) FROM df_jdup x JOIN df_jdup y ON x.k = y.k;
SELECT count(*) FROM df_ja ja JOIN df_jempty je ON ja.k = je.k;
SELECT count(*) FROM df_jempty je JOIN df_ja ja ON ja.k = je.k;
SET datafusion.mode = on;
SELECT count(*), sum(ja.a), max(jb.b) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k WHERE jb.e < 10;
SELECT ja.k, ja.c, jb.b FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k AND ja.a > jb.e WHERE ja.c < 10;
SELECT count(*), sum(jb.b + jc.d) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k JOIN df_jc jc ON ja.k = jc.k;
SELECT jb.e, count(*), sum(ja.c) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k WHERE jb.e < 5 GROUP BY jb.e;
SELECT count(*) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k WHERE ja.a + jb.e > 100;
SELECT count(*) FROM df_ja ja JOIN df_jb jb ON ja.k + 1 = jb.k + 1;
SELECT count(*), sum(x.v), sum(y.v) FROM df_jdup x JOIN df_jdup y ON x.k = y.k;
SELECT count(*) FROM df_ja ja JOIN df_jempty je ON ja.k = je.k;
SELECT count(*) FROM df_jempty je JOIN df_ja ja ON ja.k = je.k;
-- An error in the join filter carries PostgreSQL's message.
SELECT count(*) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k AND ja.a / (jb.e - jb.e) > 0;

-- With batch Motions, the joined rows' partial aggregates go out as batches.
SET datafusion.motion_batches = on;
SELECT jb.e, count(*), sum(ja.c) FROM df_ja ja JOIN df_jb jb ON ja.k = jb.k WHERE jb.e < 5 GROUP BY jb.e;
RESET datafusion.motion_batches;

-- Inputs received through Motions (J3): a table distributed otherwise is
-- redistributed on the join key, or broadcast.
CREATE TABLE df_jr (r int4, k int4, w int8) DISTRIBUTED BY (r);
INSERT INTO df_jr SELECT i, i * 2, i FROM generate_series(1, 5000) i;
INSERT INTO df_jr VALUES (0, NULL, 0);
ANALYZE df_jr;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(ja.a) FROM df_ja ja JOIN df_jr jr ON ja.k = jr.k;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ja ja JOIN df_jr jr ON ja.a = jr.r;
SET datafusion.motion_batches = on;
EXPLAIN (COSTS OFF) SELECT count(*), sum(ja.a) FROM df_ja ja JOIN df_jr jr ON ja.k = jr.k;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ja ja JOIN df_jr jr ON ja.a = jr.r;
RESET datafusion.motion_batches;
SET datafusion.mode = off;
SELECT count(*), sum(ja.a), max(jr.w) FROM df_ja ja JOIN df_jr jr ON ja.k = jr.k;
SELECT jr.r % 5, count(*), max(ja.c) FROM df_ja ja JOIN df_jr jr ON ja.k = jr.k GROUP BY jr.r % 5;
SELECT count(*), max(jr.w) FROM df_ja ja JOIN df_jr jr ON ja.a = jr.r;
SET datafusion.mode = on;
SELECT count(*), sum(ja.a), max(jr.w) FROM df_ja ja JOIN df_jr jr ON ja.k = jr.k;
SELECT jr.r % 5, count(*), max(ja.c) FROM df_ja ja JOIN df_jr jr ON ja.k = jr.k GROUP BY jr.r % 5;
SELECT count(*), max(jr.w) FROM df_ja ja JOIN df_jr jr ON ja.a = jr.r;
SET datafusion.motion_batches = on;
SELECT count(*), sum(ja.a), max(jr.w) FROM df_ja ja JOIN df_jr jr ON ja.k = jr.k;
SELECT jr.r % 5, count(*), max(ja.c) FROM df_ja ja JOIN df_jr jr ON ja.k = jr.k GROUP BY jr.r % 5;
SELECT count(*), max(jr.w) FROM df_ja ja JOIN df_jr jr ON ja.a = jr.r;
SELECT count(*) FROM df_ja ja JOIN df_jr jr ON ja.k = jr.k AND ja.a / (jr.w - jr.w) > 0;
RESET datafusion.motion_batches;

-- Outer, semi and anti joins, and keys of two types (J4).
CREATE TABLE df_jf (k8 int8, f4 real, k int4) DISTRIBUTED BY (k);
INSERT INTO df_jf SELECT i * 4, (i * 4)::real, i * 4 FROM generate_series(1, 5000) i;
INSERT INTO df_jf VALUES (NULL, NULL, NULL);
ANALYZE df_jf;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), count(jb.b) FROM df_ja ja LEFT JOIN df_jb jb ON ja.k = jb.k;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ja ja WHERE NOT EXISTS (SELECT 1 FROM df_jb jb WHERE jb.k = ja.k);
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ja ja JOIN df_jf f ON ja.k = f.k8;
SET datafusion.mode = off;
SELECT count(*), count(jb.b), sum(ja.a) FROM df_ja ja LEFT JOIN df_jb jb ON ja.k = jb.k;
SELECT ja.k, jb.b, jb.b IS NULL AS missing FROM df_ja ja LEFT JOIN df_jb jb ON ja.k = jb.k
WHERE ja.k < 8 OR ja.k IS NULL;
SELECT count(*), count(ja.k), count(jb.k) FROM df_jb jb RIGHT JOIN df_ja ja ON ja.k = jb.k;
SELECT count(*), count(ja.k), count(jb.k), max(jb.b) FROM df_ja ja FULL JOIN df_jb jb ON ja.k = jb.k;
SELECT count(*), count(jb.b) FROM df_ja ja LEFT JOIN df_jb jb ON ja.k = jb.k AND jb.e > 25;
SELECT count(*), sum(ja.a) FROM df_ja ja WHERE EXISTS (SELECT 1 FROM df_jb jb WHERE jb.k = ja.k);
SELECT count(*), sum(ja.a) FROM df_ja ja WHERE ja.k IN (SELECT k FROM df_jb);
SELECT count(*), sum(ja.a) FROM df_ja ja WHERE NOT EXISTS (SELECT 1 FROM df_jb jb WHERE jb.k = ja.k);
SELECT count(*), sum(ja.a) FROM df_ja ja JOIN df_jf f ON ja.k = f.k8;
SELECT count(*) FROM df_ja ja JOIN df_jf f ON ja.c = f.f4;
SET datafusion.mode = on;
SELECT count(*), count(jb.b), sum(ja.a) FROM df_ja ja LEFT JOIN df_jb jb ON ja.k = jb.k;
SELECT ja.k, jb.b, jb.b IS NULL AS missing FROM df_ja ja LEFT JOIN df_jb jb ON ja.k = jb.k
WHERE ja.k < 8 OR ja.k IS NULL;
SELECT count(*), count(ja.k), count(jb.k) FROM df_jb jb RIGHT JOIN df_ja ja ON ja.k = jb.k;
SELECT count(*), count(ja.k), count(jb.k), max(jb.b) FROM df_ja ja FULL JOIN df_jb jb ON ja.k = jb.k;
SELECT count(*), count(jb.b) FROM df_ja ja LEFT JOIN df_jb jb ON ja.k = jb.k AND jb.e > 25;
SELECT count(*), sum(ja.a) FROM df_ja ja WHERE EXISTS (SELECT 1 FROM df_jb jb WHERE jb.k = ja.k);
SELECT count(*), sum(ja.a) FROM df_ja ja WHERE ja.k IN (SELECT k FROM df_jb);
SELECT count(*), sum(ja.a) FROM df_ja ja WHERE NOT EXISTS (SELECT 1 FROM df_jb jb WHERE jb.k = ja.k);
SELECT count(*), sum(ja.a) FROM df_ja ja JOIN df_jf f ON ja.k = f.k8;
SELECT count(*) FROM df_ja ja JOIN df_jf f ON ja.c = f.f4;

-- A semi join may output a column of its inner side, which DataFusion's
-- semi join drops: one equal to an outer hash key reads that key (TPC-H's
-- Q20 shape, where p_partkey stands for ps_partkey above the join).
CREATE TABLE df_sj_ps (ps_partkey int, ps_suppkey int, ps_availqty int) DISTRIBUTED BY (ps_availqty);
CREATE TABLE df_sj_p (p_partkey int, p_name text) DISTRIBUTED BY (p_partkey);
CREATE TABLE df_sj_l (l_partkey int, l_suppkey int, l_quantity numeric(15,2)) DISTRIBUTED BY (l_quantity);
INSERT INTO df_sj_ps SELECT i % 2000, i % 100, i % 900 FROM generate_series(1, 8000) i;
INSERT INTO df_sj_p SELECT i, CASE WHEN i % 3 = 0 THEN 'medium ' ELSE 'small ' END || i
FROM generate_series(1, 2000) i;
INSERT INTO df_sj_l SELECT i % 2000, i % 100, i % 50 FROM generate_series(1, 60000) i;
ANALYZE df_sj_ps;
ANALYZE df_sj_p;
ANALYZE df_sj_l;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF)
SELECT count(*), sum(ps_partkey) FROM df_sj_ps
WHERE ps_partkey IN (SELECT p_partkey FROM df_sj_p WHERE p_name LIKE 'medium%')
  AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM df_sj_l WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey);
SET datafusion.mode = off;
SELECT count(*), sum(ps_partkey) FROM df_sj_ps
WHERE ps_partkey IN (SELECT p_partkey FROM df_sj_p WHERE p_name LIKE 'medium%')
  AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM df_sj_l WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey);
SET datafusion.mode = on;
SELECT count(*), sum(ps_partkey) FROM df_sj_ps
WHERE ps_partkey IN (SELECT p_partkey FROM df_sj_p WHERE p_name LIKE 'medium%')
  AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM df_sj_l WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey);
RESET datafusion.motion_batches;
DROP TABLE df_sj_ps, df_sj_p, df_sj_l;

-- Aggregates below a join or another aggregate (A1, TPC-H Q2, Q13, Q15
-- and Q20): a node of the plan whose groups and calls the nodes above read
-- by name, also on the side an outer join fills with NULLs.  An avg
-- returning numeric goes up with its display scale beside it (AV2).
CREATE TABLE df_ag1 (k int, g int, f float8, c char(5), n numeric(12,2)) DISTRIBUTED BY (k);
INSERT INTO df_ag1 SELECT i, i % 37,
  CASE WHEN i % 101 = 0 THEN 'NaN' WHEN i % 103 = 0 THEN '-0' ELSE (i % 17) * 0.5 END,
  CASE WHEN i % 2 = 0 THEN 'ab' ELSE 'ab  ' END,
  CASE WHEN i % 97 = 0 THEN NULL ELSE (i % 1000) * 1.25 END
FROM generate_series(1, 20000) i;
CREATE TABLE df_ag2 (k int, g int, n numeric(12,2)) DISTRIBUTED BY (k);
INSERT INTO df_ag2 SELECT i, i % 41, (i % 300) * 2.5 FROM generate_series(1, 3000) i;
ANALYZE df_ag1;
ANALYZE df_ag2;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(b.k) FROM df_ag2 b
  JOIN (SELECT g, 0.0001 * sum(n) h FROM df_ag1 GROUP BY g) a ON a.g = b.g AND b.n > a.h;
EXPLAIN (COSTS OFF) SELECT b.k, x.a FROM df_ag2 b JOIN (SELECT g, avg(k) a FROM df_ag1 GROUP BY g) x ON x.g = b.g;
SET optimizer = on;
EXPLAIN (COSTS OFF) SELECT c, count(*) FROM (SELECT b.k, count(a.k) c FROM df_ag2 b
  LEFT JOIN df_ag1 a ON a.g = b.g AND a.k < 400 GROUP BY b.k) s GROUP BY c ORDER BY 1;
-- computed below the join, NULL above it where nothing matched
EXPLAIN (COSTS OFF) SELECT b.k, x.m FROM df_ag2 b
  LEFT JOIN (SELECT g, coalesce(max(k), -1) m FROM df_ag1 WHERE g < 20 GROUP BY g) x ON x.g = b.g
WHERE b.k BETWEEN 15 AND 25 ORDER BY 1;
SET optimizer = off;
SET datafusion.mode = off;
SELECT x.g, x.c, y.c FROM (SELECT g, count(*) c FROM df_ag1 GROUP BY g) x
  JOIN (SELECT g, count(*) c FROM df_ag2 GROUP BY g) y ON x.g = y.g WHERE x.g < 4 ORDER BY 1;
SELECT count(*), sum(b.k) FROM df_ag2 b
  JOIN (SELECT g, 0.0001 * sum(n) h FROM df_ag1 GROUP BY g) a ON a.g = b.g AND b.n > a.h;
SELECT sum(s), max(c), count(*) FROM (SELECT g, sum(n) s, count(*) c FROM df_ag1 GROUP BY g) x;
SELECT x.f, x.c FROM (SELECT f, count(*) c FROM df_ag1 GROUP BY f) x JOIN df_ag2 b ON b.k = x.c ORDER BY 1;
SELECT x.ch, x.cnt FROM (SELECT c ch, count(*) cnt FROM df_ag1 GROUP BY c) x JOIN df_ag2 b ON b.k = x.cnt / 10 ORDER BY 2;
SELECT b.k, x.c, x.s FROM df_ag2 b JOIN (SELECT count(*) c, sum(n) s FROM df_ag1 WHERE k < 0) x ON x.c + 3 = b.k;
SELECT b.k, x.a, x.m FROM df_ag2 b JOIN (SELECT g, avg(k) a, avg(n) m FROM df_ag1 GROUP BY g) x ON x.g = b.g
WHERE b.k % 100 = 7 ORDER BY 1;
SET optimizer = on;
SELECT c, count(*) FROM (SELECT b.k, count(a.k) c FROM df_ag2 b
  LEFT JOIN df_ag1 a ON a.g = b.g AND a.k < 400 GROUP BY b.k) s GROUP BY c ORDER BY 1;
SELECT b.k, x.c, x.s FROM df_ag2 b
  LEFT JOIN (SELECT g, count(*) c, sum(n) s FROM df_ag1 WHERE g < 20 GROUP BY g) x ON x.g = b.g
WHERE b.k BETWEEN 15 AND 25 ORDER BY 1;
SELECT b.k, x.m FROM df_ag2 b
  LEFT JOIN (SELECT g, coalesce(max(k), -1) m FROM df_ag1 WHERE g < 20 GROUP BY g) x ON x.g = b.g
WHERE b.k BETWEEN 15 AND 25 ORDER BY 1;
SET optimizer = off;
SET datafusion.mode = on;
SELECT x.g, x.c, y.c FROM (SELECT g, count(*) c FROM df_ag1 GROUP BY g) x
  JOIN (SELECT g, count(*) c FROM df_ag2 GROUP BY g) y ON x.g = y.g WHERE x.g < 4 ORDER BY 1;
SELECT count(*), sum(b.k) FROM df_ag2 b
  JOIN (SELECT g, 0.0001 * sum(n) h FROM df_ag1 GROUP BY g) a ON a.g = b.g AND b.n > a.h;
SELECT sum(s), max(c), count(*) FROM (SELECT g, sum(n) s, count(*) c FROM df_ag1 GROUP BY g) x;
SELECT x.f, x.c FROM (SELECT f, count(*) c FROM df_ag1 GROUP BY f) x JOIN df_ag2 b ON b.k = x.c ORDER BY 1;
SELECT x.ch, x.cnt FROM (SELECT c ch, count(*) cnt FROM df_ag1 GROUP BY c) x JOIN df_ag2 b ON b.k = x.cnt / 10 ORDER BY 2;
SELECT b.k, x.c, x.s FROM df_ag2 b JOIN (SELECT count(*) c, sum(n) s FROM df_ag1 WHERE k < 0) x ON x.c + 3 = b.k;
SELECT b.k, x.a, x.m FROM df_ag2 b JOIN (SELECT g, avg(k) a, avg(n) m FROM df_ag1 GROUP BY g) x ON x.g = b.g
WHERE b.k % 100 = 7 ORDER BY 1;
SET optimizer = on;
SELECT c, count(*) FROM (SELECT b.k, count(a.k) c FROM df_ag2 b
  LEFT JOIN df_ag1 a ON a.g = b.g AND a.k < 400 GROUP BY b.k) s GROUP BY c ORDER BY 1;
SELECT b.k, x.c, x.s FROM df_ag2 b
  LEFT JOIN (SELECT g, count(*) c, sum(n) s FROM df_ag1 WHERE g < 20 GROUP BY g) x ON x.g = b.g
WHERE b.k BETWEEN 15 AND 25 ORDER BY 1;
SELECT b.k, x.m FROM df_ag2 b
  LEFT JOIN (SELECT g, coalesce(max(k), -1) m FROM df_ag1 WHERE g < 20 GROUP BY g) x ON x.g = b.g
WHERE b.k BETWEEN 15 AND 25 ORDER BY 1;
SET optimizer = off;
RESET datafusion.motion_batches;
DROP TABLE df_ag1, df_ag2;

-- A Subquery Scan (SQ1) passes its plan's rows on, filtered by its own
-- quals; the Motions below it carry batches too.
CREATE TABLE df_sq (k int, g int, n numeric(12,2)) DISTRIBUTED BY (k);
INSERT INTO df_sq SELECT i, i % 37, (i % 1000) * 1.25 FROM generate_series(1, 10000) i;
ANALYZE df_sq;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(k) FROM (SELECT k, g FROM df_sq WHERE k % 3 = 0 OFFSET 0) s WHERE g < 10;
EXPLAIN (COSTS OFF) SELECT b.k, x.s FROM df_sq b
  LEFT JOIN (SELECT g, sum(n) s FROM df_sq WHERE g < 5 GROUP BY g) x ON x.g = b.g
WHERE b.k < 12 ORDER BY 1;
SET datafusion.mode = off;
SELECT count(*), sum(k) FROM (SELECT k, g FROM df_sq WHERE k % 3 = 0 OFFSET 0) s WHERE g < 10;
SELECT b.k, x.s2 FROM df_sq b
  JOIN (SELECT g, sum(n) * 2 s2, count(*) + 1 c FROM df_sq GROUP BY g) x ON x.g = b.g
WHERE b.k < 8 AND x.c > 270 ORDER BY 1;
SELECT b.k, x.s FROM df_sq b
  LEFT JOIN (SELECT g, sum(n) s FROM df_sq WHERE g < 5 GROUP BY g) x ON x.g = b.g
WHERE b.k < 12 ORDER BY 1;
SET datafusion.mode = on;
SELECT count(*), sum(k) FROM (SELECT k, g FROM df_sq WHERE k % 3 = 0 OFFSET 0) s WHERE g < 10;
SELECT b.k, x.s2 FROM df_sq b
  JOIN (SELECT g, sum(n) * 2 s2, count(*) + 1 c FROM df_sq GROUP BY g) x ON x.g = b.g
WHERE b.k < 8 AND x.c > 270 ORDER BY 1;
SELECT b.k, x.s FROM df_sq b
  LEFT JOIN (SELECT g, sum(n) s FROM df_sq WHERE g < 5 GROUP BY g) x ON x.g = b.g
WHERE b.k < 12 ORDER BY 1;
RESET datafusion.motion_batches;
DROP TABLE df_sq;

-- A semi join deduplicated by RowIdExpr (RI1, TPC-H Q4 and Q21): the
-- planner numbers the rows of one side, joins, and groups by the numbers
-- to drop the copies the join made, keeping the other columns as they are.
-- DataFusion numbers them as a column of the node whose output has the
-- RowIdExpr.  df_ri_o has rows twice over, which only their numbers tell
-- apart.
CREATE TABLE df_ri_o (id int, k int, c char(4), n numeric(10,2)) DISTRIBUTED BY (id);
INSERT INTO df_ri_o SELECT i, i % 300, 'c' || (i % 7), (i % 300) * 0.25 FROM generate_series(1, 1000) i;
INSERT INTO df_ri_o SELECT id, k, c, n FROM df_ri_o WHERE id % 5 = 0;
CREATE TABLE df_ri_i (k int, s int, v int) DISTRIBUTED BY (v);
INSERT INTO df_ri_i SELECT i % 500, i % 37, i FROM generate_series(1, 60000) i;
ANALYZE df_ri_o;
ANALYZE df_ri_i;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT c, count(*), sum(n) FROM df_ri_o o WHERE EXISTS (SELECT 1 FROM df_ri_i i WHERE i.k = o.k) GROUP BY c ORDER BY 1;
SET datafusion.mode = off;
SELECT c, count(*), sum(n) FROM df_ri_o o WHERE EXISTS (SELECT 1 FROM df_ri_i i WHERE i.k = o.k) GROUP BY c ORDER BY 1;
SELECT count(*), sum(id) FROM df_ri_o o WHERE EXISTS (SELECT 1 FROM df_ri_i i WHERE i.k = o.k AND i.s <> o.k % 37);
SELECT id, k, c, n FROM df_ri_o o WHERE o.k IN (SELECT k FROM df_ri_i WHERE v < 3000) AND id < 30 ORDER BY 1, 2;
SET datafusion.mode = on;
SELECT c, count(*), sum(n) FROM df_ri_o o WHERE EXISTS (SELECT 1 FROM df_ri_i i WHERE i.k = o.k) GROUP BY c ORDER BY 1;
SELECT count(*), sum(id) FROM df_ri_o o WHERE EXISTS (SELECT 1 FROM df_ri_i i WHERE i.k = o.k AND i.s <> o.k % 37);
SELECT id, k, c, n FROM df_ri_o o WHERE o.k IN (SELECT k FROM df_ri_i WHERE v < 3000) AND id < 30 ORDER BY 1, 2;
RESET datafusion.motion_batches;
DROP TABLE df_ri_o, df_ri_i;

-- GPORCA's Result (R1): a projection and filter over its child's rows,
-- here a HAVING and a test of an outer join's NULLs.
CREATE TABLE df_rs (k int, g int) DISTRIBUTED BY (k);
INSERT INTO df_rs SELECT i, i % 37 FROM generate_series(1, 10000) i;
ANALYZE df_rs;
SET optimizer = on;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, count(*) FROM df_rs GROUP BY g HAVING count(*) > 270 ORDER BY 1;
SET datafusion.mode = off;
SELECT g, count(*) FROM df_rs GROUP BY g HAVING count(*) > 270 ORDER BY 1;
SELECT count(*), sum(a.k) FROM df_rs a
  LEFT JOIN (SELECT g, count(*) c FROM df_rs WHERE k % 2 = 0 AND g < 20 GROUP BY g) x ON x.g = a.g
WHERE coalesce(x.c, 0) = 0;
SET datafusion.mode = on;
SELECT g, count(*) FROM df_rs GROUP BY g HAVING count(*) > 270 ORDER BY 1;
SELECT count(*), sum(a.k) FROM df_rs a
  LEFT JOIN (SELECT g, count(*) c FROM df_rs WHERE k % 2 = 0 AND g < 20 GROUP BY g) x ON x.g = a.g
WHERE coalesce(x.c, 0) = 0;
RESET datafusion.motion_batches;
SET optimizer = off;
DROP TABLE df_rs;

-- NOT IN (NJ1, TPC-H Q16): DataFusion's null-aware anti join, which keeps
-- its left side, the outer one.  A NULL among the inner keys leaves no row;
-- an outer row with a NULL key goes unless the inner side is empty.
CREATE TABLE df_ni_o (id int, k int, t text) DISTRIBUTED BY (id);
INSERT INTO df_ni_o SELECT i, CASE WHEN i % 41 = 0 THEN NULL ELSE i % 500 END,
  CASE WHEN i % 43 = 0 THEN NULL ELSE 't' || (i % 300) END FROM generate_series(1, 5000) i;
CREATE TABLE df_ni_i (v int, k int, t text) DISTRIBUTED BY (v);
INSERT INTO df_ni_i SELECT i, i * 3 % 450, 't' || (i * 7 % 250) FROM generate_series(1, 1000) i;
INSERT INTO df_ni_i VALUES (7, NULL, NULL);
ANALYZE df_ni_o;
ANALYZE df_ni_i;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM df_ni_o WHERE k NOT IN (SELECT k FROM df_ni_i);
SET datafusion.mode = off;
SELECT count(*), sum(id) FROM df_ni_o WHERE k NOT IN (SELECT k FROM df_ni_i);
SELECT count(*), sum(id) FROM df_ni_o WHERE k NOT IN (SELECT k FROM df_ni_i WHERE v > 1000000);
SELECT count(*), sum(id) FROM df_ni_o WHERE k NOT IN (SELECT k FROM df_ni_i WHERE v <> 7);
SELECT count(*), sum(id) FROM df_ni_o WHERE t NOT IN (SELECT t FROM df_ni_i WHERE v <> 7);
SET datafusion.mode = on;
SELECT count(*), sum(id) FROM df_ni_o WHERE k NOT IN (SELECT k FROM df_ni_i);
SELECT count(*), sum(id) FROM df_ni_o WHERE k NOT IN (SELECT k FROM df_ni_i WHERE v > 1000000);
SELECT count(*), sum(id) FROM df_ni_o WHERE k NOT IN (SELECT k FROM df_ni_i WHERE v <> 7);
SELECT count(*), sum(id) FROM df_ni_o WHERE t NOT IN (SELECT t FROM df_ni_i WHERE v <> 7);
RESET datafusion.motion_batches;
DROP TABLE df_ni_o, df_ni_i;

-- A Nested Loop without parameters (NL1, GPORCA's TPC-H Q11): DataFusion's
-- nested loop join, which collects PostgreSQL's inner side and streams the
-- outer one; the join filter decides.
CREATE TABLE df_nl_o (id int, k int) DISTRIBUTED BY (id);
INSERT INTO df_nl_o SELECT i, CASE WHEN i % 41 = 0 THEN NULL ELSE i % 500 END
FROM generate_series(1, 3000) i;
CREATE TABLE df_nl_i (v int, k int) DISTRIBUTED BY (v);
INSERT INTO df_nl_i SELECT i, i * 3 % 450 FROM generate_series(1, 300) i;
ANALYZE df_nl_o;
ANALYZE df_nl_i;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(a.id) FROM df_nl_o a LEFT JOIN df_nl_i b ON a.k < b.k - 440;
SET datafusion.mode = off;
SELECT count(*), sum(a.id), sum(b.k) FROM df_nl_o a JOIN df_nl_i b ON a.k > b.k + 440;
SELECT count(*), sum(a.id) FROM df_nl_o a LEFT JOIN df_nl_i b ON a.k < b.k - 440;
SELECT count(*), sum(id) FROM df_nl_o a WHERE EXISTS (SELECT 1 FROM df_nl_i b WHERE b.k > a.k + 440);
SELECT count(*), sum(id) FROM df_nl_o a WHERE NOT EXISTS (SELECT 1 FROM df_nl_i b WHERE b.k > a.k + 440);
SET datafusion.mode = on;
SELECT count(*), sum(a.id), sum(b.k) FROM df_nl_o a JOIN df_nl_i b ON a.k > b.k + 440;
SELECT count(*), sum(a.id) FROM df_nl_o a LEFT JOIN df_nl_i b ON a.k < b.k - 440;
SELECT count(*), sum(id) FROM df_nl_o a WHERE EXISTS (SELECT 1 FROM df_nl_i b WHERE b.k > a.k + 440);
SELECT count(*), sum(id) FROM df_nl_o a WHERE NOT EXISTS (SELECT 1 FROM df_nl_i b WHERE b.k > a.k + 440);
RESET datafusion.motion_batches;
RESET enable_hashjoin;
RESET enable_mergejoin;
DROP TABLE df_nl_o, df_nl_i;

-- NL2: an inner Nested Loop whose inner side is estimated beyond the join's
-- memory and its outer side within it (GPORCA's TPC-DS Q54: rows between
-- two scalar subqueries, each one row below its Assert) collects the outer
-- side and streams the inner one, where no one reads the order of its rows.
-- A left join so stays on PostgreSQL.
CREATE TABLE df_nl2 (id int, m int, v int) DISTRIBUTED BY (id);
CREATE TABLE df_nl2s (id int, y int, m int) DISTRIBUTED BY (id);
INSERT INTO df_nl2 SELECT i, i % 120, i % 1000 FROM generate_series(1, 20000) i;
INSERT INTO df_nl2s SELECT i, 2000 + i % 10, i % 12 FROM generate_series(1, 200) i;
ANALYZE df_nl2;
ANALYZE df_nl2s;
SET work_mem = 64;
SET datafusion.motion_batches = on;
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(v) FROM df_nl2
WHERE m >= (SELECT DISTINCT m + 1 FROM df_nl2s WHERE y = 2003 AND m = 3)
  AND m <= (SELECT DISTINCT m + 3 FROM df_nl2s WHERE y = 2003 AND m = 3);
SET optimizer = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS OFF) SELECT count(*), sum(d.v) FROM df_nl2s s LEFT JOIN df_nl2 d ON d.m > s.m + 110;
SET datafusion.mode = off;
SELECT count(*), sum(d.v) FROM df_nl2s s LEFT JOIN df_nl2 d ON d.m > s.m + 110;
SET optimizer = on;
SELECT count(*), sum(v) FROM df_nl2
WHERE m >= (SELECT DISTINCT m + 1 FROM df_nl2s WHERE y = 2003 AND m = 3)
  AND m <= (SELECT DISTINCT m + 3 FROM df_nl2s WHERE y = 2003 AND m = 3);
SET datafusion.mode = on;
SELECT count(*), sum(v) FROM df_nl2
WHERE m >= (SELECT DISTINCT m + 1 FROM df_nl2s WHERE y = 2003 AND m = 3)
  AND m <= (SELECT DISTINCT m + 3 FROM df_nl2s WHERE y = 2003 AND m = 3);
SET optimizer = off;
SELECT count(*), sum(d.v) FROM df_nl2s s LEFT JOIN df_nl2 d ON d.m > s.m + 110;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET datafusion.motion_batches;
RESET work_mem;
SET datafusion.mode = off;
DROP TABLE df_nl2, df_nl2s;

-- x IN (subquery) as a hashed SubPlan (SP1, TPC-DS Q10, Q45: under an OR,
-- or as a column): the subquery's rows come from a Motion, which the
-- slice reads whole first into a set; each x is looked up in it.  Not
-- found, x gives NULL where the set holds a NULL; a NULL x gives NULL,
-- or FALSE against no rows.  Floats compare with -0 as 0 and NaN as NaN,
-- character(n) without trailing blanks, varchar as text.  The slices
-- sending the subquery's rows are reported too.
CREATE TABLE df_sp_o (id int, k int, t char(5), v varchar, c float8) DISTRIBUTED BY (id);
CREATE TABLE df_sp_i (w int, k int, t char(3), v varchar, c float8) DISTRIBUTED BY (w);
CREATE TABLE df_sp_n (w int, k int) DISTRIBUTED BY (w);
CREATE TABLE df_sp_e (w int, k int) DISTRIBUTED BY (w);
INSERT INTO df_sp_o SELECT i, i % 40, (i % 13)::text, (i % 17)::text, (i % 9) - 4.0 FROM generate_series(1, 3000) i;
INSERT INTO df_sp_o VALUES (3001, NULL, NULL, NULL, NULL), (3002, 3, '3  ', '3 ', '-0'), (3003, 4, 'x', 'x', 'NaN');
INSERT INTO df_sp_i SELECT i, i % 7 * 3, (i % 5)::text, (i % 4)::text, (i % 3)::float8 FROM generate_series(1, 500) i;
INSERT INTO df_sp_i VALUES (501, 1000, 'x  ', 'x  ', 'NaN'), (502, 1001, 'zz', 'zz', '0');
INSERT INTO df_sp_n SELECT i, i % 7 FROM generate_series(1, 300) i;
INSERT INTO df_sp_n VALUES (301, NULL);
ANALYZE df_sp_o; ANALYZE df_sp_i; ANALYZE df_sp_n; ANALYZE df_sp_e;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM df_sp_o WHERE k IN (SELECT k FROM df_sp_i) OR id < 100;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_sp_o WHERE t IN (SELECT t FROM df_sp_i) OR v IN (SELECT v FROM df_sp_i) OR c IN (SELECT c FROM df_sp_i);
-- a correlated subquery stays on PostgreSQL
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_sp_o o WHERE k IN (SELECT k FROM df_sp_i i WHERE i.w = o.id) OR id < 100;
-- an init plan whose top slice only gathers its rows is reported as a
-- query's top slice that does (TPC-DS Q58's)
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_sp_o WHERE k = (SELECT k FROM df_sp_i WHERE w = 7);
SET datafusion.mode = off;
SELECT count(*), sum(id) FROM df_sp_o WHERE k IN (SELECT k FROM df_sp_i) OR id < 100;
SELECT count(*) FROM df_sp_o WHERE t IN (SELECT t FROM df_sp_i) OR v IN (SELECT v FROM df_sp_i) OR c IN (SELECT c FROM df_sp_i);
SELECT id, t IN (SELECT t FROM df_sp_i), v IN (SELECT v FROM df_sp_i), c IN (SELECT c FROM df_sp_i) FROM df_sp_o WHERE id % 500 = 0 OR id > 2999 ORDER BY id;
SELECT id, k IN (SELECT k FROM df_sp_n), k IN (SELECT k FROM df_sp_i), k IN (SELECT k FROM df_sp_e) FROM df_sp_o WHERE id % 500 = 1 OR id > 2999 ORDER BY id;
SET datafusion.mode = on;
SELECT count(*), sum(id) FROM df_sp_o WHERE k IN (SELECT k FROM df_sp_i) OR id < 100;
SELECT count(*) FROM df_sp_o WHERE t IN (SELECT t FROM df_sp_i) OR v IN (SELECT v FROM df_sp_i) OR c IN (SELECT c FROM df_sp_i);
SELECT id, t IN (SELECT t FROM df_sp_i), v IN (SELECT v FROM df_sp_i), c IN (SELECT c FROM df_sp_i) FROM df_sp_o WHERE id % 500 = 0 OR id > 2999 ORDER BY id;
SELECT id, k IN (SELECT k FROM df_sp_n), k IN (SELECT k FROM df_sp_i), k IN (SELECT k FROM df_sp_e) FROM df_sp_o WHERE id % 500 = 1 OR id > 2999 ORDER BY id;
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM df_sp_o WHERE k IN (SELECT k FROM df_sp_i) OR id < 100;
SET datafusion.mode = off;
SELECT count(*), sum(id) FROM df_sp_o WHERE k IN (SELECT k FROM df_sp_i) OR id < 100;
SELECT id, k IN (SELECT k FROM df_sp_n), k IN (SELECT k FROM df_sp_i) FROM df_sp_o WHERE id % 500 = 1 OR id > 2999 ORDER BY id;
SET datafusion.mode = on;
SELECT count(*), sum(id) FROM df_sp_o WHERE k IN (SELECT k FROM df_sp_i) OR id < 100;
SELECT id, k IN (SELECT k FROM df_sp_n), k IN (SELECT k FROM df_sp_i) FROM df_sp_o WHERE id % 500 = 1 OR id > 2999 ORDER BY id;
SET optimizer = off;
SET datafusion.mode = off;
RESET datafusion.motion_batches;
DROP TABLE df_sp_o, df_sp_i, df_sp_n, df_sp_e;

-- The value of an uncorrelated scalar subquery (SP2, TPC-DS Q44 under
-- GPORCA), as GPORCA plans it when the subquery may return several rows:
-- its rows come from a Motion, read whole first as SP1's set; the value
-- is its one row's column, NULL without a row.  More than one row is
-- PostgreSQL's error where the value is computed, not where no row needs
-- it.  An avg compares exactly, its display scale coming with its value.
CREATE TABLE df_sc (id int, k int, n numeric(7,2)) DISTRIBUTED BY (id);
INSERT INTO df_sc SELECT i, i % 50, ((i * 7919) % 20001 - 10000) / 100.0 FROM generate_series(1, 20000) i;
ANALYZE df_sc;
SET optimizer = on;
SET optimizer_enforce_subplans = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT k, avg(n) FROM df_sc GROUP BY k HAVING avg(n) > 0.9 * (SELECT avg(n) FROM df_sc WHERE k = 3 GROUP BY k) ORDER BY k;
EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM df_sc WHERE id < (SELECT max(id) FROM df_sc WHERE k = 3 GROUP BY k) - 9000;
SET datafusion.mode = off;
SELECT k, avg(n) FROM df_sc GROUP BY k HAVING avg(n) > 0.9 * (SELECT avg(n) FROM df_sc WHERE k = 3 GROUP BY k) ORDER BY k;
SELECT k, avg(n), (SELECT avg(n) FROM df_sc WHERE k = 3 GROUP BY k) AS a3 FROM df_sc GROUP BY k ORDER BY k LIMIT 3;
SELECT count(*), sum(id) FROM df_sc WHERE id < (SELECT max(id) FROM df_sc WHERE k = 3 GROUP BY k) - 9000;
SELECT count(*) FROM df_sc WHERE id < (SELECT max(id) FROM df_sc WHERE k = 99 GROUP BY k);
SELECT count(*) FROM df_sc WHERE id < 0 AND id < (SELECT max(id) FROM df_sc WHERE k < 3 GROUP BY k);
SELECT count(*) FROM df_sc WHERE id < (SELECT max(id) FROM df_sc WHERE k < 3 GROUP BY k);
SET datafusion.mode = on;
SELECT k, avg(n) FROM df_sc GROUP BY k HAVING avg(n) > 0.9 * (SELECT avg(n) FROM df_sc WHERE k = 3 GROUP BY k) ORDER BY k;
SELECT k, avg(n), (SELECT avg(n) FROM df_sc WHERE k = 3 GROUP BY k) AS a3 FROM df_sc GROUP BY k ORDER BY k LIMIT 3;
SELECT count(*), sum(id) FROM df_sc WHERE id < (SELECT max(id) FROM df_sc WHERE k = 3 GROUP BY k) - 9000;
SELECT count(*) FROM df_sc WHERE id < (SELECT max(id) FROM df_sc WHERE k = 99 GROUP BY k);
SELECT count(*) FROM df_sc WHERE id < 0 AND id < (SELECT max(id) FROM df_sc WHERE k < 3 GROUP BY k);
SELECT count(*) FROM df_sc WHERE id < (SELECT max(id) FROM df_sc WHERE k < 3 GROUP BY k);
RESET optimizer_enforce_subplans;
SET optimizer = off;
SET datafusion.mode = off;
DROP TABLE df_sc;

-- A correlated scalar subquery that is one aggregate of rows equal to the
-- outer row on some keys (SC1, TPC-DS Q6 and Q41 under the Postgres
-- planner): its rows come from a Motion; the scan's rows are joined to
-- that aggregate grouped by the keys.  A row of no group, or of a NULL
-- key, gets the aggregate of no rows: NULL, or 0 for a count.  Keys of
-- integers of two widths, character(n) of two lengths, varchar.  One
-- correlated by other than =, on numeric, or outside a scan's filter
-- stays on PostgreSQL.
CREATE TABLE df_co (id int, k int, t char(5), v varchar, n numeric(7,2)) DISTRIBUTED BY (id);
CREATE TABLE df_ci (w int, k int8, t char(3), v varchar, n numeric(7,2), f int) DISTRIBUTED BY (w);
INSERT INTO df_co SELECT i, CASE WHEN i % 101 = 0 THEN NULL ELSE i % 40 END, (i % 13)::text,
  (i % 7)::text, ((i * 7919) % 20001 - 10000) / 100.0 FROM generate_series(1, 3000) i;
INSERT INTO df_co VALUES (3001, 3, '3  ', '3', 1.00), (3002, NULL, NULL, NULL, NULL);
INSERT INTO df_ci SELECT i, CASE WHEN i % 53 = 0 THEN NULL ELSE i % 30 END, (i % 11)::text,
  (i % 5)::text, CASE WHEN i % 17 = 0 THEN NULL ELSE ((i * 104729) % 20001 - 10000) / 100.0 END,
  i % 6 FROM generate_series(1, 600) i;
ANALYZE df_co; ANALYZE df_ci;
SET optimizer = off;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM df_co o WHERE n > 1.2 * (SELECT avg(i.n) FROM df_ci i WHERE i.k = o.k);
EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM df_co o WHERE (SELECT count(*) FROM df_ci i WHERE i.k = o.k AND i.f > 3) < 15;
EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM df_co o WHERE id > 2 * (SELECT max(w) FROM df_ci i WHERE i.t = o.t AND i.v = o.v);
-- stay on PostgreSQL
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_co o WHERE id > (SELECT max(w) FROM df_ci i WHERE i.k < o.k);
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_co o WHERE id > 2 * (SELECT max(w) FROM df_ci i WHERE i.n = o.n);
EXPLAIN (COSTS OFF) SELECT id, (SELECT max(w) FROM df_ci i WHERE i.k = o.k) FROM df_co o WHERE id < 5;
SET datafusion.mode = off;
SELECT count(*), sum(id) FROM df_co o WHERE n > 1.2 * (SELECT avg(i.n) FROM df_ci i WHERE i.k = o.k);
SELECT count(*), sum(id) FROM df_co o WHERE (SELECT count(*) FROM df_ci i WHERE i.k = o.k AND i.f > 3) < 15;
SELECT count(*), sum(id) FROM df_co o WHERE coalesce((SELECT sum(f) FROM df_ci i WHERE i.k = o.k), -1) = -1;
SELECT count(*), sum(id) FROM df_co o WHERE id > 2 * (SELECT max(w) FROM df_ci i WHERE i.t = o.t AND i.v = o.v);
SELECT count(*), sum(id) FROM df_co o WHERE (SELECT count(*) FROM df_ci i WHERE (i.k = o.k AND i.f = 1) OR (i.k = o.k AND i.f = 4)) > 3
  AND n < (SELECT avg(n) FROM df_ci i WHERE i.t = o.t);
SET datafusion.mode = on;
SELECT count(*), sum(id) FROM df_co o WHERE n > 1.2 * (SELECT avg(i.n) FROM df_ci i WHERE i.k = o.k);
SELECT count(*), sum(id) FROM df_co o WHERE (SELECT count(*) FROM df_ci i WHERE i.k = o.k AND i.f > 3) < 15;
SELECT count(*), sum(id) FROM df_co o WHERE coalesce((SELECT sum(f) FROM df_ci i WHERE i.k = o.k), -1) = -1;
SELECT count(*), sum(id) FROM df_co o WHERE id > 2 * (SELECT max(w) FROM df_ci i WHERE i.t = o.t AND i.v = o.v);
SELECT count(*), sum(id) FROM df_co o WHERE (SELECT count(*) FROM df_ci i WHERE (i.k = o.k AND i.f = 1) OR (i.k = o.k AND i.f = 4)) > 3
  AND n < (SELECT avg(n) FROM df_ci i WHERE i.t = o.t);
SET datafusion.mode = off;
RESET datafusion.motion_batches;
DROP TABLE df_co, df_ci;

-- A Merge Join whose order of rows no one reads runs as DataFusion's hash
-- join on its merge clauses (MJ1, TPC-DS Q51 under GPORCA), the Sorts
-- below it, there for the merge, left out: inner on two keys (one a date),
-- left with a join filter, full with NULL keys on both sides.  One whose
-- order a sorted Gather reads stays on PostgreSQL.
CREATE TABLE df_mj_a (id int, k int, d date, t text, v int) DISTRIBUTED BY (id);
CREATE TABLE df_mj_b (id int, k int, d date, t text, w int) DISTRIBUTED BY (id);
INSERT INTO df_mj_a SELECT i, CASE WHEN i % 97 = 0 THEN NULL ELSE i % 300 END,
  date '2001-01-01' + i % 20, 'k' || (i % 7), i FROM generate_series(1, 6000) i;
INSERT INTO df_mj_b SELECT i, CASE WHEN i % 89 = 0 THEN NULL ELSE i % 400 END,
  date '2001-01-01' + i % 25, 'k' || (i % 9), i FROM generate_series(1, 5000) i;
ANALYZE df_mj_a;
ANALYZE df_mj_b;
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(a.v), sum(b.w) FROM df_mj_a a JOIN df_mj_b b ON a.k = b.k AND a.d = b.d;
EXPLAIN (COSTS OFF) SELECT count(*), count(b.w), sum(a.v) FROM df_mj_a a LEFT JOIN df_mj_b b ON a.k = b.k AND a.v < b.w;
EXPLAIN (COSTS OFF) SELECT a.k, b.w FROM df_mj_a a JOIN df_mj_b b ON a.k = b.k WHERE a.id < 50 ORDER BY a.k LIMIT 5;
SET datafusion.mode = off;
SELECT count(*), sum(a.v), sum(b.w) FROM df_mj_a a JOIN df_mj_b b ON a.k = b.k AND a.d = b.d;
SELECT count(*), count(b.w), sum(a.v) FROM df_mj_a a LEFT JOIN df_mj_b b ON a.k = b.k AND a.v < b.w;
SET datafusion.mode = on;
SELECT count(*), sum(a.v), sum(b.w) FROM df_mj_a a JOIN df_mj_b b ON a.k = b.k AND a.d = b.d;
SELECT count(*), count(b.w), sum(a.v) FROM df_mj_a a LEFT JOIN df_mj_b b ON a.k = b.k AND a.v < b.w;
-- semi and anti, with a join filter or not
SET enable_hashagg = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(v) FROM df_mj_a a WHERE EXISTS (SELECT 1 FROM df_mj_b b WHERE b.k = a.k AND b.w > a.v);
EXPLAIN (COSTS OFF) SELECT count(*), sum(v) FROM df_mj_a a WHERE NOT EXISTS (SELECT 1 FROM df_mj_b b WHERE b.k = a.k AND b.w > a.v);
SET datafusion.mode = off;
SELECT count(*), sum(v) FROM df_mj_a a WHERE EXISTS (SELECT 1 FROM df_mj_b b WHERE b.k = a.k AND b.w > a.v);
SELECT count(*), sum(w) FROM df_mj_b b WHERE EXISTS (SELECT 1 FROM df_mj_a a WHERE a.k = b.k AND a.t = b.t);
SELECT count(*), sum(v) FROM df_mj_a a WHERE NOT EXISTS (SELECT 1 FROM df_mj_b b WHERE b.k = a.k AND b.w > a.v);
SELECT count(*), sum(w) FROM df_mj_b b WHERE NOT EXISTS (SELECT 1 FROM df_mj_a a WHERE a.k = b.k);
SET datafusion.mode = on;
SELECT count(*), sum(v) FROM df_mj_a a WHERE EXISTS (SELECT 1 FROM df_mj_b b WHERE b.k = a.k AND b.w > a.v);
SELECT count(*), sum(w) FROM df_mj_b b WHERE EXISTS (SELECT 1 FROM df_mj_a a WHERE a.k = b.k AND a.t = b.t);
SELECT count(*), sum(v) FROM df_mj_a a WHERE NOT EXISTS (SELECT 1 FROM df_mj_b b WHERE b.k = a.k AND b.w > a.v);
SELECT count(*), sum(w) FROM df_mj_b b WHERE NOT EXISTS (SELECT 1 FROM df_mj_a a WHERE a.k = b.k);
RESET enable_hashagg;
RESET enable_hashjoin;
RESET enable_nestloop;
SET optimizer = on;
SET optimizer_enable_hashjoin = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), count(b.w), count(a.v), sum(a.v), sum(b.w) FROM df_mj_a a FULL JOIN df_mj_b b ON a.k = b.k AND a.t = b.t;
SET datafusion.mode = off;
SELECT count(*), count(b.w), count(a.v), sum(a.v), sum(b.w) FROM df_mj_a a FULL JOIN df_mj_b b ON a.k = b.k AND a.t = b.t;
SELECT a.k IS NULL AS an, b.k IS NULL AS bn, count(*) FROM df_mj_a a FULL JOIN df_mj_b b ON a.k = b.k AND a.t = b.t GROUP BY 1, 2 ORDER BY 1, 2;
SET datafusion.mode = on;
SELECT count(*), count(b.w), count(a.v), sum(a.v), sum(b.w) FROM df_mj_a a FULL JOIN df_mj_b b ON a.k = b.k AND a.t = b.t;
SELECT a.k IS NULL AS an, b.k IS NULL AS bn, count(*) FROM df_mj_a a FULL JOIN df_mj_b b ON a.k = b.k AND a.t = b.t GROUP BY 1, 2 ORDER BY 1, 2;
RESET optimizer_enable_hashjoin;
SET optimizer = off;
SET datafusion.mode = off;
DROP TABLE df_mj_a, df_mj_b;

-- A hash join's build side coming from another join (JE1,
-- datafusion.join_estimates = bounded): DataFusion's own hash join cannot
-- spill, and the planner's estimate of a join can be far off (TPC-H Q9 at
-- scale factor 10: 40 rows estimated, a million per segment built).  Such
-- a build side is taken to hold up to as many rows as its largest input;
-- past the budget the join stays on PostgreSQL, the join below it in
-- DataFusion.  By default (spill, SHJ) hash joins spill what does not fit
-- and run in DataFusion whatever their size.
CREATE TABLE df_je_big (k int, v int) DISTRIBUTED BY (k);
CREATE TABLE df_je_a (k int, x int) DISTRIBUTED BY (k);
CREATE TABLE df_je_b (k int, y int) DISTRIBUTED BY (k);
INSERT INTO df_je_big SELECT i % 5000, i FROM generate_series(1, 200000) i;
INSERT INTO df_je_a SELECT i, i FROM generate_series(1, 20000) i;
INSERT INTO df_je_b SELECT i, i FROM generate_series(1, 20000) i;
ANALYZE df_je_big;
ANALYZE df_je_a;
ANALYZE df_je_b;
SET join_collapse_limit = 1;
SET work_mem = 64;
SET datafusion.mode = explain;
SET datafusion.join_estimates = bounded;
EXPLAIN (COSTS OFF) SELECT count(*), sum(g.v) FROM df_je_big g JOIN (df_je_a a JOIN df_je_b b ON b.k = a.k AND b.y < 3000) ON a.k = g.k;
SET datafusion.join_estimates = trusted;
EXPLAIN (COSTS OFF) SELECT count(*), sum(g.v) FROM df_je_big g JOIN (df_je_a a JOIN df_je_b b ON b.k = a.k AND b.y < 3000) ON a.k = g.k;
RESET datafusion.join_estimates;
-- spill: the whole slice in DataFusion, its joins spilling within 64 kB
EXPLAIN (COSTS OFF) SELECT count(*), sum(g.v) FROM df_je_big g JOIN (df_je_a a JOIN df_je_b b ON b.k = a.k AND b.y < 3000) ON a.k = g.k;
SET datafusion.mode = off;
SELECT count(*), sum(g.v) FROM df_je_big g JOIN (df_je_a a JOIN df_je_b b ON b.k = a.k AND b.y < 3000) ON a.k = g.k;
SELECT count(*), sum(a.x) FROM df_je_big g RIGHT JOIN df_je_a a ON a.k = g.k WHERE g.k IS NULL;
SET datafusion.mode = on;
SELECT count(*), sum(g.v) FROM df_je_big g JOIN (df_je_a a JOIN df_je_b b ON b.k = a.k AND b.y < 3000) ON a.k = g.k;
SELECT count(*), sum(a.x) FROM df_je_big g RIGHT JOIN df_je_a a ON a.k = g.k WHERE g.k IS NULL;
SET datafusion.join_estimates = bounded;
SELECT count(*), sum(g.v) FROM df_je_big g JOIN (df_je_a a JOIN df_je_b b ON b.k = a.k AND b.y < 3000) ON a.k = g.k;
RESET datafusion.join_estimates;
RESET work_mem;
RESET join_collapse_limit;
DROP TABLE df_je_big, df_je_a, df_je_b;

DROP TABLE df_ja, df_jb, df_jc, df_jempty, df_jdup, df_jr, df_jf;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
