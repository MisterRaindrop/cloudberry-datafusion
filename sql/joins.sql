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
-- by name, also on the side an outer join fills with NULLs.  One whose
-- value is finished where tuples are made (avg returning numeric) stays on
-- PostgreSQL.
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

-- A hash join's build side coming from another join (JE1,
-- datafusion.join_estimates, bounded by default): DataFusion's hash join
-- cannot spill, and the planner's estimate of a join can be far off
-- (TPC-H Q9 at scale factor 10: 40 rows estimated, a million per segment
-- built).  Such a build side is taken to hold up to as many rows as its
-- largest input; past the budget the join stays on PostgreSQL, the join
-- below it in DataFusion.
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
EXPLAIN (COSTS OFF) SELECT count(*), sum(g.v) FROM df_je_big g JOIN (df_je_a a JOIN df_je_b b ON b.k = a.k AND b.y < 3000) ON a.k = g.k;
SET datafusion.join_estimates = trusted;
EXPLAIN (COSTS OFF) SELECT count(*), sum(g.v) FROM df_je_big g JOIN (df_je_a a JOIN df_je_b b ON b.k = a.k AND b.y < 3000) ON a.k = g.k;
RESET datafusion.join_estimates;
SET datafusion.mode = off;
SELECT count(*), sum(g.v) FROM df_je_big g JOIN (df_je_a a JOIN df_je_b b ON b.k = a.k AND b.y < 3000) ON a.k = g.k;
SET datafusion.mode = on;
SELECT count(*), sum(g.v) FROM df_je_big g JOIN (df_je_a a JOIN df_je_b b ON b.k = a.k AND b.y < 3000) ON a.k = g.k;
RESET work_mem;
RESET join_collapse_limit;
DROP TABLE df_je_big, df_je_a, df_je_b;

DROP TABLE df_ja, df_jb, df_jc, df_jempty, df_jdup, df_jr, df_jf;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
