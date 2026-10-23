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
--
-- Set operations.  SO1: INTERSECT and EXCEPT (SetOp), as a grouping by
-- their columns counting each side's rows; not the ALL forms, nor columns
-- whose equal values may look different (numeric, floats, character
-- without a length).
-- UA1: UNION ALL (Append), as DataFusion's union of its
-- inputs, which keeps no order between them: an Append whose order a node
-- above reads stays on PostgreSQL; a Merge Append whose order nothing
-- reads runs as one, and a GroupAggregate whose order nothing reads runs
-- hashed whatever its input (TPC-DS Q33).  Numeric of different scales, or of
-- display scales of their own, carries each value's display scale beside
-- it.  Inputs that are Motions (GPORCA redistributes each branch below
-- the Append) are read from their streams; an input the planner proves
-- empty (a Result with a one-time filter, OF1) gives no rows, or an Agg's
-- one row on the segment its filter picks.  Each query runs with
-- datafusion.mode off, then on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;

CREATE TABLE df_u1 (k int, g int, a numeric(7,2), b int8, t text, c char(4)) DISTRIBUTED BY (k);
CREATE TABLE df_u2 (k int, g int, a numeric(12,4), b int4, t varchar(10), c char(4)) DISTRIBUTED BY (k);
INSERT INTO df_u1 SELECT i, i % 7, CASE WHEN i % 97 = 0 THEN 'NaN' ELSE (i % 300) * 1.25 END, i::int8 * 1000000000, CASE WHEN i % 11 = 0 THEN NULL ELSE 't' || (i % 5) END, CASE WHEN i % 17 = 0 THEN NULL ELSE 'c' || (i % 6) END FROM generate_series(1, 3000) i;
INSERT INTO df_u2 SELECT i, i % 5, (i % 400) / 7.0, i, CASE WHEN i % 13 = 0 THEN NULL ELSE 'v' || (i % 3) END, CASE WHEN i % 19 = 0 THEN NULL ELSE 'c' || (i % 4) END FROM generate_series(1, 2000) i;
CREATE TABLE df_up (k int, g int, a numeric(7,2)) DISTRIBUTED BY (k) PARTITION BY RANGE (g) (START (0) END (8) EVERY (4));
INSERT INTO df_up SELECT i, i % 8, i * 0.5 FROM generate_series(1, 1000) i;
ANALYZE df_u1;
ANALYZE df_u2;
ANALYZE df_up;

SET optimizer = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT u.g, count(*), sum(u.a) FROM (SELECT g, a FROM df_u1 UNION ALL SELECT g, a FROM df_u2) u GROUP BY u.g ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT u.k, u.t, x.g FROM (SELECT k, t FROM df_u1 WHERE k < 50 UNION ALL SELECT k, t FROM df_u2 WHERE k < 50) u JOIN df_u1 x ON x.k = u.k + 1 ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT g, sum(q), count(q) FROM (SELECT g, a / 3 q FROM df_u1 WHERE a <> 'NaN' UNION ALL SELECT g, a FROM df_u2 UNION ALL SELECT g, k * 1.5 FROM df_u1) u GROUP BY g ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT k, a FROM df_u1 UNION ALL SELECT k, a FROM df_u2) u LIMIT 5;
EXPLAIN (COSTS OFF) SELECT u.g, u.s FROM (SELECT g, sum(a) s FROM df_u1 GROUP BY g UNION ALL SELECT g, sum(a) FROM df_u2 WHERE 1 = 0 GROUP BY g) u JOIN df_u1 x ON x.k = u.g + 1 ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT u.g, u.v, u.v / x.k FROM (SELECT g, avg(a) v FROM df_u1 WHERE a <> 'NaN' GROUP BY g UNION ALL SELECT g, avg(a) FROM df_u2 GROUP BY g) u JOIN df_u1 x ON x.k = u.g + 1 ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT count(*) FROM (SELECT g, b FROM df_u1 INTERSECT SELECT g, b FROM df_u2) s;
EXPLAIN (COSTS OFF) SELECT count(*) FROM (SELECT g, a FROM df_u1 INTERSECT SELECT g, a FROM df_u2) s;
EXPLAIN (COSTS OFF) SELECT count(*) FROM (SELECT g FROM df_u1 INTERSECT ALL SELECT g FROM df_u2) s;
EXPLAIN (COSTS OFF) SELECT count(*), count(c) FROM (SELECT g, c FROM df_u1 INTERSECT SELECT g + 1, c FROM df_u2) s;
EXPLAIN (COSTS OFF) SELECT count(*) FROM (SELECT g, c::bpchar FROM df_u1 INTERSECT SELECT g, (c || 'x')::bpchar FROM df_u2) s;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT k % 50 x, g y FROM df_u1 EXCEPT SELECT k % 40, g FROM df_u2) s ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT t FROM df_u1 EXCEPT SELECT t FROM df_u2) s ORDER BY 1;
-- an input grouped before a Motion: the Motion is still read below the SetOp (TPC-DS Q38)
EXPLAIN (COSTS OFF) SELECT count(*), sum(x), sum(y) FROM (SELECT g x, k % 4 y FROM df_u1 INTERSECT SELECT g, k % 4 FROM df_u2 INTERSECT SELECT g % 3, k % 4 FROM df_u1 WHERE k < 300 GROUP BY 1, 2) s;
SET enable_hashagg = off;
EXPLAIN (COSTS OFF) SELECT g, sum(s) FROM (SELECT g, sum(a) s FROM df_u1 GROUP BY g UNION ALL SELECT g, sum(a) FROM df_u2 GROUP BY g) u GROUP BY g ORDER BY 2, 1;
RESET enable_hashagg;
SET datafusion.mode = off;
SELECT u.g, count(*), sum(u.a), sum(u.b), count(u.t) FROM (SELECT g, a, b, t FROM df_u1 UNION ALL SELECT g, a, b, t FROM df_u2) u GROUP BY u.g ORDER BY 1;
SELECT u.k, u.t, u.a, x.g FROM (SELECT k, t, a FROM df_u1 WHERE k < 50 UNION ALL SELECT k, t, a FROM df_u2 WHERE k < 50) u JOIN df_u1 x ON x.k = u.k + 1 ORDER BY 1, 2, 3;
SELECT g, sum(q), count(q) FROM (SELECT g, a / 3 q FROM df_u1 WHERE a <> 'NaN' UNION ALL SELECT g, a FROM df_u2 UNION ALL SELECT g, k * 1.5 FROM df_u1) u GROUP BY g ORDER BY 1;
SELECT k, a FROM (SELECT k, a FROM df_u1 WHERE k % 400 = 1 UNION ALL SELECT k, a FROM df_u2 WHERE k % 300 = 2) u ORDER BY a, k;
SELECT g, count(*), sum(a) FROM df_up WHERE k > 10 GROUP BY g ORDER BY 1;
SELECT count(*) FROM (SELECT g, b FROM df_u1 INTERSECT SELECT g, b FROM df_u2) s;
SELECT * FROM (SELECT k % 50 x, t FROM df_u1 EXCEPT SELECT k % 40, t FROM df_u2) s ORDER BY 1, 2;
SELECT count(*) FROM (SELECT g, t FROM df_u1 INTERSECT SELECT g, t FROM df_u2 INTERSECT SELECT g % 3, t FROM df_u1) s;
SELECT count(*), sum(x), sum(y) FROM (SELECT g x, k % 4 y FROM df_u1 INTERSECT SELECT g, k % 4 FROM df_u2 INTERSECT SELECT g % 3, k % 4 FROM df_u1 WHERE k < 300 GROUP BY 1, 2) s;
SELECT * FROM (SELECT t FROM df_u1 EXCEPT SELECT t FROM df_u2) s ORDER BY 1;
SELECT u.g, u.s FROM (SELECT g, sum(a) s FROM df_u1 GROUP BY g UNION ALL SELECT g, sum(a) FROM df_u2 WHERE 1 = 0 GROUP BY g) u JOIN df_u1 x ON x.k = u.g + 1 ORDER BY 1, 2;
SELECT count(*), count(u.s), sum(u.s) FROM (SELECT sum(a) s FROM df_u1 WHERE a <> 'NaN' UNION ALL SELECT sum(a) FROM df_u2 WHERE 1 = 0) u;
SELECT u.g, u.v, u.v / x.k FROM (SELECT g, avg(a) v FROM df_u1 WHERE a <> 'NaN' GROUP BY g UNION ALL SELECT g, avg(a) FROM df_u2 GROUP BY g) u JOIN df_u1 x ON x.k = u.g + 1 ORDER BY 1, 2;
SELECT * FROM (SELECT k % 50 x, g y FROM df_u1 EXCEPT SELECT k % 40, g FROM df_u2) s ORDER BY 1, 2;
SELECT count(*) FROM (SELECT k % 30 x, g FROM df_u1 WHERE k % 11 <> 0 INTERSECT SELECT k % 45, g FROM df_u2) s;
SELECT count(*), count(c) FROM (SELECT g, c FROM df_u1 INTERSECT SELECT g + 1, c FROM df_u2) s;
SET enable_hashagg = off;
SELECT g, sum(s) FROM (SELECT g, sum(a) s FROM df_u1 GROUP BY g UNION ALL SELECT g, sum(a) FROM df_u2 GROUP BY g) u GROUP BY g ORDER BY 2, 1;
RESET enable_hashagg;
SELECT u.g, count(*), sum(u.a) FROM (SELECT x.g, x.a FROM df_u1 x JOIN df_u2 y ON x.g = y.k UNION ALL SELECT y.g, y.a FROM df_u2 y JOIN df_u1 x ON y.g = x.k) u GROUP BY u.g ORDER BY 1;
SELECT u.t, count(*) FROM (SELECT t FROM df_u1 UNION ALL SELECT t FROM df_u2) u GROUP BY u.t ORDER BY 1;
SET datafusion.mode = on;
SELECT u.g, count(*), sum(u.a), sum(u.b), count(u.t) FROM (SELECT g, a, b, t FROM df_u1 UNION ALL SELECT g, a, b, t FROM df_u2) u GROUP BY u.g ORDER BY 1;
SELECT u.k, u.t, u.a, x.g FROM (SELECT k, t, a FROM df_u1 WHERE k < 50 UNION ALL SELECT k, t, a FROM df_u2 WHERE k < 50) u JOIN df_u1 x ON x.k = u.k + 1 ORDER BY 1, 2, 3;
SELECT g, sum(q), count(q) FROM (SELECT g, a / 3 q FROM df_u1 WHERE a <> 'NaN' UNION ALL SELECT g, a FROM df_u2 UNION ALL SELECT g, k * 1.5 FROM df_u1) u GROUP BY g ORDER BY 1;
SELECT k, a FROM (SELECT k, a FROM df_u1 WHERE k % 400 = 1 UNION ALL SELECT k, a FROM df_u2 WHERE k % 300 = 2) u ORDER BY a, k;
SELECT g, count(*), sum(a) FROM df_up WHERE k > 10 GROUP BY g ORDER BY 1;
SELECT count(*) FROM (SELECT g, b FROM df_u1 INTERSECT SELECT g, b FROM df_u2) s;
SELECT * FROM (SELECT k % 50 x, t FROM df_u1 EXCEPT SELECT k % 40, t FROM df_u2) s ORDER BY 1, 2;
SELECT count(*) FROM (SELECT g, t FROM df_u1 INTERSECT SELECT g, t FROM df_u2 INTERSECT SELECT g % 3, t FROM df_u1) s;
SELECT count(*), sum(x), sum(y) FROM (SELECT g x, k % 4 y FROM df_u1 INTERSECT SELECT g, k % 4 FROM df_u2 INTERSECT SELECT g % 3, k % 4 FROM df_u1 WHERE k < 300 GROUP BY 1, 2) s;
SELECT * FROM (SELECT t FROM df_u1 EXCEPT SELECT t FROM df_u2) s ORDER BY 1;
SELECT u.g, u.s FROM (SELECT g, sum(a) s FROM df_u1 GROUP BY g UNION ALL SELECT g, sum(a) FROM df_u2 WHERE 1 = 0 GROUP BY g) u JOIN df_u1 x ON x.k = u.g + 1 ORDER BY 1, 2;
SELECT count(*), count(u.s), sum(u.s) FROM (SELECT sum(a) s FROM df_u1 WHERE a <> 'NaN' UNION ALL SELECT sum(a) FROM df_u2 WHERE 1 = 0) u;
SELECT u.g, u.v, u.v / x.k FROM (SELECT g, avg(a) v FROM df_u1 WHERE a <> 'NaN' GROUP BY g UNION ALL SELECT g, avg(a) FROM df_u2 GROUP BY g) u JOIN df_u1 x ON x.k = u.g + 1 ORDER BY 1, 2;
SELECT * FROM (SELECT k % 50 x, g y FROM df_u1 EXCEPT SELECT k % 40, g FROM df_u2) s ORDER BY 1, 2;
SELECT count(*) FROM (SELECT k % 30 x, g FROM df_u1 WHERE k % 11 <> 0 INTERSECT SELECT k % 45, g FROM df_u2) s;
SELECT count(*), count(c) FROM (SELECT g, c FROM df_u1 INTERSECT SELECT g + 1, c FROM df_u2) s;
SET enable_hashagg = off;
SELECT g, sum(s) FROM (SELECT g, sum(a) s FROM df_u1 GROUP BY g UNION ALL SELECT g, sum(a) FROM df_u2 GROUP BY g) u GROUP BY g ORDER BY 2, 1;
RESET enable_hashagg;
SELECT u.g, count(*), sum(u.a) FROM (SELECT x.g, x.a FROM df_u1 x JOIN df_u2 y ON x.g = y.k UNION ALL SELECT y.g, y.a FROM df_u2 y JOIN df_u1 x ON y.g = x.k) u GROUP BY u.g ORDER BY 1;
SELECT u.t, count(*) FROM (SELECT t FROM df_u1 UNION ALL SELECT t FROM df_u2) u GROUP BY u.t ORDER BY 1;

-- GPORCA's plans
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT u.g, count(*), sum(u.a) FROM (SELECT x.g, x.a FROM df_u1 x JOIN df_u2 y ON x.g = y.k UNION ALL SELECT y.g, y.a FROM df_u2 y JOIN df_u1 x ON y.g = x.k) u GROUP BY u.g ORDER BY 1;
SET datafusion.mode = off;
SELECT u.g, count(*), sum(u.a), sum(u.b), count(u.t) FROM (SELECT g, a, b, t FROM df_u1 UNION ALL SELECT g, a, b, t FROM df_u2) u GROUP BY u.g ORDER BY 1;
SELECT u.k, u.t, u.a, x.g FROM (SELECT k, t, a FROM df_u1 WHERE k < 50 UNION ALL SELECT k, t, a FROM df_u2 WHERE k < 50) u JOIN df_u1 x ON x.k = u.k + 1 ORDER BY 1, 2, 3;
SELECT k, a FROM (SELECT k, a FROM df_u1 WHERE k % 400 = 1 UNION ALL SELECT k, a FROM df_u2 WHERE k % 300 = 2) u ORDER BY a, k;
SELECT g, count(*), sum(a) FROM df_up WHERE k > 10 GROUP BY g ORDER BY 1;
SELECT count(*) FROM (SELECT g, b FROM df_u1 INTERSECT SELECT g, b FROM df_u2) s;
SELECT * FROM (SELECT k % 50 x, t FROM df_u1 EXCEPT SELECT k % 40, t FROM df_u2) s ORDER BY 1, 2;
SELECT count(*) FROM (SELECT g, t FROM df_u1 INTERSECT SELECT g, t FROM df_u2 INTERSECT SELECT g % 3, t FROM df_u1) s;
SELECT * FROM (SELECT t FROM df_u1 EXCEPT SELECT t FROM df_u2) s ORDER BY 1;
SELECT * FROM (SELECT k % 50 x, g y FROM df_u1 EXCEPT SELECT k % 40, g FROM df_u2) s ORDER BY 1, 2;
SELECT count(*) FROM (SELECT k % 30 x, g FROM df_u1 WHERE k % 11 <> 0 INTERSECT SELECT k % 45, g FROM df_u2) s;
SELECT count(*), count(c) FROM (SELECT g, c FROM df_u1 INTERSECT SELECT g + 1, c FROM df_u2) s;
SELECT u.g, count(*), sum(u.a) FROM (SELECT x.g, x.a FROM df_u1 x JOIN df_u2 y ON x.g = y.k UNION ALL SELECT y.g, y.a FROM df_u2 y JOIN df_u1 x ON y.g = x.k) u GROUP BY u.g ORDER BY 1;
SELECT u.t, count(*) FROM (SELECT t FROM df_u1 UNION ALL SELECT t FROM df_u2) u GROUP BY u.t ORDER BY 1;
SET datafusion.mode = on;
SELECT u.g, count(*), sum(u.a), sum(u.b), count(u.t) FROM (SELECT g, a, b, t FROM df_u1 UNION ALL SELECT g, a, b, t FROM df_u2) u GROUP BY u.g ORDER BY 1;
SELECT u.k, u.t, u.a, x.g FROM (SELECT k, t, a FROM df_u1 WHERE k < 50 UNION ALL SELECT k, t, a FROM df_u2 WHERE k < 50) u JOIN df_u1 x ON x.k = u.k + 1 ORDER BY 1, 2, 3;
SELECT k, a FROM (SELECT k, a FROM df_u1 WHERE k % 400 = 1 UNION ALL SELECT k, a FROM df_u2 WHERE k % 300 = 2) u ORDER BY a, k;
SELECT g, count(*), sum(a) FROM df_up WHERE k > 10 GROUP BY g ORDER BY 1;
SELECT count(*) FROM (SELECT g, b FROM df_u1 INTERSECT SELECT g, b FROM df_u2) s;
SELECT * FROM (SELECT k % 50 x, t FROM df_u1 EXCEPT SELECT k % 40, t FROM df_u2) s ORDER BY 1, 2;
SELECT count(*) FROM (SELECT g, t FROM df_u1 INTERSECT SELECT g, t FROM df_u2 INTERSECT SELECT g % 3, t FROM df_u1) s;
SELECT * FROM (SELECT t FROM df_u1 EXCEPT SELECT t FROM df_u2) s ORDER BY 1;
SELECT * FROM (SELECT k % 50 x, g y FROM df_u1 EXCEPT SELECT k % 40, g FROM df_u2) s ORDER BY 1, 2;
SELECT count(*) FROM (SELECT k % 30 x, g FROM df_u1 WHERE k % 11 <> 0 INTERSECT SELECT k % 45, g FROM df_u2) s;
SELECT count(*), count(c) FROM (SELECT g, c FROM df_u1 INTERSECT SELECT g + 1, c FROM df_u2) s;
SELECT u.g, count(*), sum(u.a) FROM (SELECT x.g, x.a FROM df_u1 x JOIN df_u2 y ON x.g = y.k UNION ALL SELECT y.g, y.a FROM df_u2 y JOIN df_u1 x ON y.g = x.k) u GROUP BY u.g ORDER BY 1;
SELECT u.t, count(*) FROM (SELECT t FROM df_u1 UNION ALL SELECT t FROM df_u2) u GROUP BY u.t ORDER BY 1;

-- UQ1: a Unique (a DISTINCT, grouped sorted) whose order of rows no one
-- reads, as a grouping by its columns without the Sort below it: below a
-- join (TPC-DS Q54's, below a Redistribute Motion), an Agg, an unsorted
-- Gather in an init plan (TPC-DS Q6); NULLs make a group.  One whose order
-- the Gather above merges, and one on numeric, stay on PostgreSQL.
SET optimizer = off;
SET enable_hashagg = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT x.k, x.g, d.t, d.c FROM df_u1 x JOIN (SELECT DISTINCT g, k % 50 m, t, c FROM df_u1 WHERE k < 300) d ON x.k = d.m + d.g WHERE x.k < 60;
EXPLAIN (COSTS OFF) SELECT count(*), count(t), count(c) FROM (SELECT DISTINCT g + 1, t, c FROM df_u1) s;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_u1 WHERE g = (SELECT DISTINCT k % 5 FROM df_u1 WHERE g = 3 AND k < 10);
EXPLAIN (COSTS OFF) SELECT DISTINCT g, k % 50 FROM df_u1 ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT x.k FROM df_u1 x JOIN (SELECT DISTINCT g, a FROM df_u1) d ON x.k = d.g;
SET datafusion.mode = off;
SELECT x.k, x.g, d.t, d.c FROM df_u1 x JOIN (SELECT DISTINCT g, k % 50 m, t, c FROM df_u1 WHERE k < 300) d ON x.k = d.m + d.g WHERE x.k < 12 ORDER BY 1, 3, 4;
SELECT count(*), count(t), count(c) FROM (SELECT DISTINCT g + 1, t, c FROM df_u1) s;
SELECT count(*) FROM df_u1 WHERE g = (SELECT DISTINCT k % 5 FROM df_u1 WHERE g = 3 AND k < 10);
SELECT count(*) FROM df_u1 WHERE g = (SELECT DISTINCT k % 5 FROM df_u1 WHERE g = 3 AND k < 20);
SET datafusion.mode = on;
SELECT x.k, x.g, d.t, d.c FROM df_u1 x JOIN (SELECT DISTINCT g, k % 50 m, t, c FROM df_u1 WHERE k < 300) d ON x.k = d.m + d.g WHERE x.k < 12 ORDER BY 1, 3, 4;
SELECT count(*), count(t), count(c) FROM (SELECT DISTINCT g + 1, t, c FROM df_u1) s;
SELECT count(*) FROM df_u1 WHERE g = (SELECT DISTINCT k % 5 FROM df_u1 WHERE g = 3 AND k < 10);
SELECT count(*) FROM df_u1 WHERE g = (SELECT DISTINCT k % 5 FROM df_u1 WHERE g = 3 AND k < 20);
RESET enable_hashagg;
SET optimizer = on;

-- AS1: GPORCA's Assert over a scalar subquery's rows, at most one: every
-- row passes, or the slice raises PostgreSQL's error (P0003).
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_u1 WHERE g = (SELECT g FROM df_u2 WHERE k = 3);
SET datafusion.mode = off;
SELECT count(*) FROM df_u1 WHERE g = (SELECT g FROM df_u2 WHERE k = 3);
SELECT count(*) FROM df_u1 WHERE g = (SELECT g FROM df_u2 WHERE k < 5);
SET datafusion.mode = on;
SELECT count(*) FROM df_u1 WHERE g = (SELECT g FROM df_u2 WHERE k = 3);
SELECT count(*) FROM df_u1 WHERE g = (SELECT g FROM df_u2 WHERE k < 5);
RESET datafusion.mode;

DROP TABLE df_u1, df_u2, df_up;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
