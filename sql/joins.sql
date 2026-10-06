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

DROP TABLE df_ja, df_jb, df_jc, df_jempty, df_jdup, df_jr, df_jf;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
