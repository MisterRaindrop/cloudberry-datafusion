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
-- numeric results of aggregates over integers (N1): sum(int8) and avg of
-- int2, int4 and int8.  DataFusion adds the values exactly as
-- Decimal128(38, 0); the C side builds the numeric values with
-- numeric_in, and avg as numeric_div(sum, count), the way int8_avg and
-- numeric_poly_avg do, so display scales are PostgreSQL's.  Split through
-- a batch Motion, the partial stage sends that sum (and count) instead of
-- PostgreSQL's serialized state.  Each query runs with datafusion.mode
-- off, then on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;

CREATE TABLE df_nm (g int4, a int4, b int8, e int2) DISTRIBUTED BY (g);
INSERT INTO df_nm SELECT i % 20, i, i::int8 * 1000003, (i % 300)::int2 FROM generate_series(1, 100000) i;
-- sums beyond int8, and the extremes of each type
INSERT INTO df_nm SELECT 77, 2147483647, 9223372036854775807, 32767 FROM generate_series(1, 5);
INSERT INTO df_nm SELECT 78, -2147483648, -9223372036854775808, -32768 FROM generate_series(1, 3);
INSERT INTO df_nm VALUES (79, NULL, NULL, NULL), (80, 1, 1, 1), (80, 2, 2, 2), (81, 1, -1, 1), (81, 0, 0, 0);
CREATE TABLE df_nm_empty (g int4, a int4, b int8, e int2) DISTRIBUTED BY (g);
ANALYZE df_nm;

SET datafusion.mode = explain;
-- grouped by the distribution key: one stage
EXPLAIN (COSTS OFF) SELECT g, sum(b), avg(a) FROM df_nm GROUP BY g;
-- two stages: PostgreSQL's serialized states, unless batch Motions carry ours
EXPLAIN (COSTS OFF) SELECT sum(b), avg(b) FROM df_nm;
SET datafusion.motion_batches = on;
EXPLAIN (COSTS OFF) SELECT sum(b), avg(b) FROM df_nm;
-- an avg's numeric result has no fixed scale: compared at the largest it may
-- have (AVG1)
EXPLAIN (COSTS OFF) SELECT g FROM df_nm GROUP BY g HAVING avg(a) + 1 > 2;
RESET datafusion.motion_batches;

SET datafusion.mode = off;
SELECT g, sum(b), avg(a), avg(b), avg(e), count(*) FROM df_nm WHERE g >= 15 GROUP BY g ORDER BY g;
SELECT sum(b), avg(a), avg(b), avg(e) FROM df_nm;
SELECT sum(b), avg(a), avg(b), avg(e) FROM df_nm_empty;
SET datafusion.mode = on;
SELECT g, sum(b), avg(a), avg(b), avg(e), count(*) FROM df_nm WHERE g >= 15 GROUP BY g ORDER BY g;
SET datafusion.motion_batches = on;
SELECT sum(b), avg(a), avg(b), avg(e) FROM df_nm;
SELECT sum(b), avg(a), avg(b), avg(e) FROM df_nm_empty;
SELECT a % 7 AS k, sum(b), avg(e) FROM df_nm GROUP BY a % 7 ORDER BY k;
SET datafusion.mode = off;
SELECT a % 7 AS k, sum(b), avg(e) FROM df_nm GROUP BY a % 7 ORDER BY k;

-- numeric(p, s) columns with p <= 38 (N2): Decimal256(76, s), NaN above
-- every value; sum and avg need p <= 66 to stay within 76 digits.
CREATE TABLE df_nc (id int, g int, a numeric(10,2), b numeric(15,4), c numeric(38,0),
  d numeric(28,10), u numeric) DISTRIBUTED BY (id);
INSERT INTO df_nc SELECT i, i % 7, ((i::bigint * 7919) % 2000000 - 1000000) / 100.0,
  ((i::bigint * 104729) % 20000000 - 10000000) / 10000.0,
  i::numeric * 99999999999999999999999, ((i * 31) % 1000) / 1234567.0, i / 3.0
FROM generate_series(1, 50000) i;
INSERT INTO df_nc VALUES (-1, 90, 'NaN', 'NaN', 'NaN', 'NaN', 'NaN'),
  (-2, 90, 1.5, 2.25, 3, 0.0000000001, 1), (-3, 91, NULL, NULL, NULL, NULL, NULL),
  (-4, 92, 99999999.99, 99999999999.9999, 99999999999999999999999999999999999999,
   999999999999999999.9999999999, 0),
  (-5, 92, -99999999.99, -99999999999.9999, -99999999999999999999999999999999999999,
   -999999999999999999.9999999999, 0),
  (-6, 93, 0, -0.0001, 0, 0, 0), (-7, 93, 1.50, 1.5, 1, 1.5, 1.5);
CREATE TABLE df_nc2 AS SELECT id, b AS a2, a AS b2 FROM df_nc WHERE id % 3 = 0 OR id < 0
DISTRIBUTED BY (id);
ANALYZE df_nc;
ANALYZE df_nc2;
SET enable_nestloop = off;
SET enable_mergejoin = off;

SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, sum(a), avg(b), min(c), max(d) FROM df_nc WHERE b < a AND a > 100.5 GROUP BY g;
EXPLAIN (COSTS OFF) SELECT sum(c * c) FROM df_nc;
EXPLAIN (COSTS OFF) SELECT min(u) FROM df_nc;
-- + - * (N3) have a fixed scale, / and % one that depends on the values
EXPLAIN (COSTS OFF) SELECT a + 1, a * b, a - id FROM df_nc;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_nc WHERE a / b > 1;

SET datafusion.mode = off;
SELECT min(a), max(a), min(b), max(b), min(c), max(c), min(d), max(d), count(a) FROM df_nc;
SELECT g, sum(a), avg(a), sum(b), avg(b), sum(d), avg(d), min(c), count(*) FROM df_nc GROUP BY g ORDER BY g;
SELECT count(*) FROM df_nc WHERE a > 100.5 AND b <= 2.25 AND c > 1e30;
SELECT id, a, b FROM df_nc WHERE a = 1.5 OR b = 1.5000 OR a = 'NaN' OR a > 99999999 ORDER BY id;
SELECT count(*) FROM df_nc WHERE b < a;
SELECT count(*), sum(x.a) FROM df_nc x JOIN df_nc2 y ON x.a = y.a2;
SELECT id, a, b, c, d FROM df_nc WHERE id < 0 ORDER BY id;
SET datafusion.mode = on;
SELECT min(a), max(a), min(b), max(b), min(c), max(c), min(d), max(d), count(a) FROM df_nc;
SELECT g, sum(a), avg(a), sum(b), avg(b), sum(d), avg(d), min(c), count(*) FROM df_nc GROUP BY g ORDER BY g;
SELECT count(*) FROM df_nc WHERE a > 100.5 AND b <= 2.25 AND c > 1e30;
SELECT id, a, b FROM df_nc WHERE a = 1.5 OR b = 1.5000 OR a = 'NaN' OR a > 99999999 ORDER BY id;
SELECT count(*) FROM df_nc WHERE b < a;
SELECT count(*), sum(x.a) FROM df_nc x JOIN df_nc2 y ON x.a = y.a2;
SELECT id, a, b, c, d FROM df_nc WHERE id < 0 ORDER BY id;
-- split through batch Motions: DataFusion's sums, NaN included
SET datafusion.motion_batches = on;
SELECT sum(a), avg(a), sum(b), avg(b), sum(d), avg(d), sum(c) FROM df_nc;
SELECT sum(a), avg(b), max(a), min(b) FROM df_nc WHERE id > 0;
SET datafusion.mode = off;
SELECT sum(a), avg(a), sum(b), avg(b), sum(d), avg(d), sum(c) FROM df_nc;
SELECT sum(a), avg(b), max(a), min(b) FROM df_nc WHERE id > 0;

-- Redistributed by numeric keys as batches: hash_numeric of the NBASE
-- digits, placed by the column's scale (df_core::cdbhash).  df_nc3 is
-- distributed by PostgreSQL's hash, which the rows redistributed to join
-- it must meet.
CREATE TABLE df_nc3 AS SELECT b AS k, id FROM df_nc, generate_series(1, 3)
DISTRIBUTED BY (k);
ANALYZE df_nc3;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT b, count(*) FROM df_nc GROUP BY b;
EXPLAIN (COSTS OFF) SELECT count(*), sum(x.id) FROM df_nc x JOIN df_nc3 y ON x.b = y.k;
SET datafusion.mode = off;
SELECT count(*), sum(x.id) FROM df_nc x JOIN df_nc3 y ON x.b = y.k;
SET datafusion.mode = on;
SELECT count(*), sum(x.id) FROM df_nc x JOIN df_nc3 y ON x.b = y.k;
SET datafusion.mode = off;
SELECT count(*), count(DISTINCT n), sum(n) FROM (SELECT a, count(*) AS n FROM df_nc GROUP BY a) s;
SELECT count(*), count(DISTINCT n), sum(n) FROM (SELECT b, d, count(*) AS n FROM df_nc GROUP BY b, d) s;
SELECT c, count(*) FROM df_nc GROUP BY c HAVING c < 0 OR c > 1e38 OR c = 'NaN' ORDER BY c;
SET datafusion.mode = on;
SELECT count(*), count(DISTINCT n), sum(n) FROM (SELECT a, count(*) AS n FROM df_nc GROUP BY a) s;
SELECT count(*), count(DISTINCT n), sum(n) FROM (SELECT b, d, count(*) AS n FROM df_nc GROUP BY b, d) s;
SELECT c, count(*) FROM df_nc GROUP BY c HAVING c < 0 OR c > 1e38 OR c = 'NaN' ORDER BY c;
SET datafusion.mode = off;
-- A key of 76 digits: the digits are placed without overflowing.
CREATE TABLE df_nc4 (id int, x numeric(38, 1)) DISTRIBUTED BY (id);
INSERT INTO df_nc4 SELECT i, 9999999999999999999999999999999999999.9 - i % 50
FROM generate_series(1, 200) i;
ANALYZE df_nc4;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT x * x, count(*) FROM df_nc4 GROUP BY x * x;
SET datafusion.mode = on;
SELECT count(*), min(n), max(n) FROM (SELECT x * x, count(*) AS n FROM df_nc4 GROUP BY x * x) s;
SET datafusion.mode = off;

-- sum of a CASE or COALESCE whose branches differ in scale (MS1, TPC-H Q8
-- and Q14): PostgreSQL's sum shows the largest display scale of the values
-- it adds up, so DataFusion keeps that beside the sum, and a group whose
-- rows all took the ELSE 0 shows 0, not 0.0000.
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT 100.00 * sum(CASE WHEN g = 1 THEN a * (1 - b) ELSE 0 END) / sum(a * (1 - b)) FROM df_nc;
EXPLAIN (COSTS OFF) SELECT avg(CASE WHEN g = 1 THEN a ELSE 0 END) FROM df_nc;
SET datafusion.mode = off;
SELECT 100.00 * sum(CASE WHEN g = 1 THEN a * (1 - b) ELSE 0 END) / sum(a * (1 - b)) FROM df_nc WHERE id > 0;
SELECT g, sum(CASE WHEN id % 2 = 0 OR g = 93 THEN a * b ELSE 0 END), sum(COALESCE(b, a, 0))
FROM df_nc GROUP BY g ORDER BY g;
SELECT g, sum(CASE WHEN id < 0 AND g <> 93 THEN b END), sum(CASE g WHEN 90 THEN d WHEN 92 THEN c ELSE 1 END)
FROM df_nc WHERE id < 0 OR id % 1000 = 0 GROUP BY g ORDER BY g;
SET datafusion.mode = on;
SELECT 100.00 * sum(CASE WHEN g = 1 THEN a * (1 - b) ELSE 0 END) / sum(a * (1 - b)) FROM df_nc WHERE id > 0;
SELECT g, sum(CASE WHEN id % 2 = 0 OR g = 93 THEN a * b ELSE 0 END), sum(COALESCE(b, a, 0))
FROM df_nc GROUP BY g ORDER BY g;
SELECT g, sum(CASE WHEN id < 0 AND g <> 93 THEN b END), sum(CASE g WHEN 90 THEN d WHEN 92 THEN c ELSE 1 END)
FROM df_nc WHERE id < 0 OR id % 1000 = 0 GROUP BY g ORDER BY g;
SET datafusion.mode = off;

-- An init plan's value (IP1, TPC-H Q11, Q15 and Q22), computed before the
-- slices start, is a constant; numeric ones only compared with a numeric
-- of known scale, rounded to it: up for < and >=, down for <= and >, and
-- a value beyond every row's becomes the infinity on its side.
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_nc WHERE a > (SELECT avg(a) FROM df_nc WHERE id > 0);
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_nc WHERE a + (SELECT avg(a) FROM df_nc) > 0;
SET datafusion.mode = off;
SELECT count(*), sum(id) FROM df_nc WHERE a > (SELECT avg(a) FROM df_nc WHERE id > 0);
SELECT count(*), sum(id) FROM df_nc WHERE (SELECT avg(b) FROM df_nc WHERE id > 0) >= b;
SELECT count(*), sum(id) FROM df_nc WHERE a = (SELECT max(a) FROM df_nc WHERE id > 0);
SELECT count(*), sum(id) FROM df_nc WHERE a <> (SELECT 1.505::numeric);
SELECT count(*), sum(id) FROM df_nc WHERE a < (SELECT 'NaN'::numeric);
SELECT count(*), sum(id) FROM df_nc WHERE a <= (SELECT -1e40::numeric);
SELECT count(*), sum(id) FROM df_nc WHERE c > (SELECT 'Infinity'::numeric);
SELECT count(*) FROM df_nc WHERE a > (SELECT NULL::numeric);
SELECT g, sum(a) FROM df_nc GROUP BY g HAVING sum(a) > (SELECT sum(a) / 8 FROM df_nc WHERE id > 0) ORDER BY g;
SELECT count(*) FROM df_nc WHERE id > (SELECT max(id) / 2 FROM df_nc);
SET datafusion.mode = on;
SELECT count(*), sum(id) FROM df_nc WHERE a > (SELECT avg(a) FROM df_nc WHERE id > 0);
SELECT count(*), sum(id) FROM df_nc WHERE (SELECT avg(b) FROM df_nc WHERE id > 0) >= b;
SELECT count(*), sum(id) FROM df_nc WHERE a = (SELECT max(a) FROM df_nc WHERE id > 0);
SELECT count(*), sum(id) FROM df_nc WHERE a <> (SELECT 1.505::numeric);
SELECT count(*), sum(id) FROM df_nc WHERE a < (SELECT 'NaN'::numeric);
SELECT count(*), sum(id) FROM df_nc WHERE a <= (SELECT -1e40::numeric);
SELECT count(*), sum(id) FROM df_nc WHERE c > (SELECT 'Infinity'::numeric);
SELECT count(*) FROM df_nc WHERE a > (SELECT NULL::numeric);
SELECT g, sum(a) FROM df_nc GROUP BY g HAVING sum(a) > (SELECT sum(a) / 8 FROM df_nc WHERE id > 0) ORDER BY g;
SELECT count(*) FROM df_nc WHERE id > (SELECT max(id) / 2 FROM df_nc);
SET datafusion.mode = off;

-- The slices of an init plan (IP2, TPC-H Q15): the coordinator runs its top
-- slice before the query, on PostgreSQL's executor; those below it run in
-- DataFusion, a split sum's state passing between them as batches.
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, s FROM (SELECT g, sum(a) AS s FROM df_nc GROUP BY g) q
WHERE s = (SELECT max(s2) FROM (SELECT g, sum(a) AS s2 FROM df_nc WHERE id > 0 GROUP BY g) r);
SET datafusion.mode = off;
SELECT g, s FROM (SELECT g, sum(a) AS s FROM df_nc WHERE id > 0 GROUP BY g) q
WHERE s = (SELECT max(s2) FROM (SELECT g, sum(a) AS s2 FROM df_nc WHERE id > 0 GROUP BY g) r) ORDER BY g;
SELECT count(*) FROM df_nc WHERE b < (SELECT min(t) FROM (SELECT g, sum(b) AS t FROM df_nc GROUP BY g) r);
SET datafusion.mode = on;
SELECT g, s FROM (SELECT g, sum(a) AS s FROM df_nc WHERE id > 0 GROUP BY g) q
WHERE s = (SELECT max(s2) FROM (SELECT g, sum(a) AS s2 FROM df_nc WHERE id > 0 GROUP BY g) r) ORDER BY g;
SELECT count(*) FROM df_nc WHERE b < (SELECT min(t) FROM (SELECT g, sum(b) AS t FROM df_nc GROUP BY g) r);
SET datafusion.mode = off;

-- + - * (N3), with the scales of numeric.c, up to 76 digits (c * c)
SELECT id, a + b, a - b, a * b, a * 2, 1 - a, a * 0.5, c * c, d * d, a + id
FROM df_nc WHERE id < 0 OR id % 9973 = 0 ORDER BY id;
SELECT g, sum(a * (1 - b)), sum(a * (1 - b) * (1 + a)), avg(a - 1) FROM df_nc GROUP BY g ORDER BY g;
SELECT count(*) FROM df_nc WHERE a * 2 > b + 1.5 AND a - b < 100;
SELECT count(*) FROM df_nc WHERE a = 'NaN';
SELECT count(*) FROM df_nc WHERE b < 'NaN';
SET datafusion.mode = on;
SELECT id, a + b, a - b, a * b, a * 2, 1 - a, a * 0.5, c * c, d * d, a + id
FROM df_nc WHERE id < 0 OR id % 9973 = 0 ORDER BY id;
SELECT g, sum(a * (1 - b)), sum(a * (1 - b) * (1 + a)), avg(a - 1) FROM df_nc GROUP BY g ORDER BY g;
SELECT count(*) FROM df_nc WHERE a * 2 > b + 1.5 AND a - b < 100;
-- a NaN constant is Decimal256's NaN (it was int128's for a while)
SELECT count(*) FROM df_nc WHERE a = 'NaN';
SELECT count(*) FROM df_nc WHERE b < 'NaN';

-- Output columns PostgreSQL finishes (P1): DataFusion computes the largest
-- parts it can (columns, + - *, aggregates) and PostgreSQL evaluates the
-- rest over them, row by row: numeric / with its scale chosen per value,
-- functions DataFusion lacks.  Filters and sort keys stay DataFusion's.
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, sum(a) / sum(b), 100.00 * sum(a) / count(*) FROM df_nc GROUP BY g;
EXPLAIN (COSTS OFF) SELECT id, a / b, round(a * 2, 1), abs(c) FROM df_nc WHERE a > 0;
-- not a sort key: DataFusion sorts by what it computes
EXPLAIN (COSTS OFF) SELECT id, a / b FROM df_nc ORDER BY a / b LIMIT 3;
SET datafusion.mode = off;
SELECT g, sum(a) / nullif(sum(b), 0), 100.00 * sum(a) / count(*), round(avg(a), 3) FROM df_nc GROUP BY g ORDER BY g;
SELECT sum(b) / sum(a), sum(c) / 7, avg(id) / 3 FROM df_nc WHERE id > 0;
SELECT id, a / b, a / 3, d / 7.0, c / (id + 1), round(a * 2, 1), abs(b), (a + 1) / nullif(b, 0)
FROM df_nc WHERE id < 0 OR id % 9973 = 0 ORDER BY id;
SELECT id, CASE WHEN a = 0 THEN 0 ELSE b / a END FROM df_nc WHERE id IN (-6, -2, 9973) ORDER BY id;
SET datafusion.mode = on;
SELECT g, sum(a) / nullif(sum(b), 0), 100.00 * sum(a) / count(*), round(avg(a), 3) FROM df_nc GROUP BY g ORDER BY g;
SELECT sum(b) / sum(a), sum(c) / 7, avg(id) / 3 FROM df_nc WHERE id > 0;
SELECT id, a / b, a / 3, d / 7.0, c / (id + 1), round(a * 2, 1), abs(b), (a + 1) / nullif(b, 0)
FROM df_nc WHERE id < 0 OR id % 9973 = 0 ORDER BY id;
-- PostgreSQL only divides where a <> 0, here too
SELECT id, CASE WHEN a = 0 THEN 0 ELSE b / a END FROM df_nc WHERE id IN (-6, -2, 9973) ORDER BY id;
-- and raises its own error
\set VERBOSITY sqlstate
SELECT id, b / a FROM df_nc WHERE id = -6;
\set VERBOSITY default
RESET datafusion.motion_batches;

-- An avg returning numeric inside an expression or below the top (AVG1,
-- TPC-H Q17): numeric_avg's value, rounded as PostgreSQL rounds it, at the
-- largest scale it may have; compared, never shown at that scale.
CREATE TABLE df_av (id int, g int, x numeric(12,2), i int, y numeric(10,2)) DISTRIBUTED BY (id);
INSERT INTO df_av SELECT k, k % 4,
  CASE k % 4 WHEN 0 THEN CASE WHEN k % 3 = 0 THEN 0 ELSE 1 END
             WHEN 1 THEN CASE WHEN k % 3 = 0 THEN 0 ELSE -1 END
             ELSE k % 7 END,
  CASE WHEN k % 3 = 0 THEN 0 ELSE 1 END, (k % 50) * 0.5
FROM generate_series(1, 3000) k;
ANALYZE df_av;
SET datafusion.motion_batches = on;
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g FROM df_av GROUP BY g HAVING avg(x) = 0.66666666666666666667 OR avg(x) = -0.66666666666666666667 ORDER BY g;
SET datafusion.mode = off;
SELECT g FROM df_av GROUP BY g HAVING avg(x) = 0.66666666666666666667 OR avg(x) = -0.66666666666666666667 ORDER BY g;
SELECT g FROM df_av GROUP BY g HAVING avg(i) > 0.66666666666666666666 ORDER BY g;
SELECT count(*), sum(a.id) FROM df_av a WHERE a.y < (SELECT 0.2 * avg(y) * 4 FROM df_av b WHERE b.g = a.g);
SELECT g, avg(x), avg(x) * 2, avg(i) FROM df_av GROUP BY g ORDER BY g;
SET datafusion.mode = on;
SELECT g FROM df_av GROUP BY g HAVING avg(x) = 0.66666666666666666667 OR avg(x) = -0.66666666666666666667 ORDER BY g;
SELECT g FROM df_av GROUP BY g HAVING avg(i) > 0.66666666666666666666 ORDER BY g;
SELECT count(*), sum(a.id) FROM df_av a WHERE a.y < (SELECT 0.2 * avg(y) * 4 FROM df_av b WHERE b.g = a.g);
SELECT g, avg(x), avg(x) * 2, avg(i) FROM df_av GROUP BY g ORDER BY g;
SET optimizer = off;
SET datafusion.mode = off;
RESET datafusion.motion_batches;
DROP TABLE df_av;

-- Numeric values received as tuples (WT1, TPC-H Q22 under ORCA, whose avg
-- is broadcast from the coordinator) are read into all 76 digits of
-- Decimal256: an avg at scale 40, a product of 40 digits at scale 3,
-- whose last NBASE digit reaches past the scale.
CREATE TABLE df_wv (id int, g int, x numeric(36,2)) DISTRIBUTED BY (id);
INSERT INTO df_wv SELECT k, k % 7,
  (CASE WHEN k % 5 = 0 THEN -1 ELSE 1 END) *
  (k::numeric * 1000000000000000000000000000000 + k * 0.37 + (k % 3) * 0.01)
FROM generate_series(1, 300) k;
INSERT INTO df_wv VALUES (-1, 10, 1.00), (-2, 10, 3.00), (-3, 10, 2.00), (-4, 10, 2.01),
  (-7, 12, 999999999999999999999999999999999.99),
  (-8, 12, 999999999999999999999999999999999.98),
  (-9, 12, 999999999999999999999999999999999.97);
ANALYZE df_wv;
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM df_wv WHERE x > (SELECT avg(x) FROM df_wv);
SET datafusion.mode = off;
SELECT count(*), sum(id) FROM df_wv WHERE x > (SELECT avg(x) FROM df_wv);
SELECT count(*), sum(id) FROM df_wv WHERE x < (SELECT avg(x) FROM df_wv WHERE g = 3);
SELECT count(*), sum(id) FROM df_wv WHERE x >= (SELECT avg(x) FROM df_wv WHERE g = 10);
SELECT count(*), sum(id) FROM df_wv WHERE x <= (SELECT avg(x) FROM df_wv WHERE g = 12);
SELECT g, max(y), min(y), count(*) FROM (SELECT g, x * 1000.5 AS y FROM df_wv) s GROUP BY g ORDER BY g;
SET datafusion.mode = on;
SELECT count(*), sum(id) FROM df_wv WHERE x > (SELECT avg(x) FROM df_wv);
SELECT count(*), sum(id) FROM df_wv WHERE x < (SELECT avg(x) FROM df_wv WHERE g = 3);
SELECT count(*), sum(id) FROM df_wv WHERE x >= (SELECT avg(x) FROM df_wv WHERE g = 10);
SELECT count(*), sum(id) FROM df_wv WHERE x <= (SELECT avg(x) FROM df_wv WHERE g = 12);
SELECT g, max(y), min(y), count(*) FROM (SELECT g, x * 1000.5 AS y FROM df_wv) s GROUP BY g ORDER BY g;
SET optimizer = off;
SET datafusion.mode = off;
DROP TABLE df_wv;

-- NR1: a numeric's precision and scale found through the nodes that pass
-- columns on without a child to follow: an Append's inputs, GPORCA's
-- Sequence and Shared Scan (its producer's input; GPORCA's references drop
-- the type modifier).  A sum has 19 digits more than its argument, not 38:
-- avg(sum) * 1.2, compared with the sum (TPC-DS Q1), fits 76 digits.
CREATE TABLE df_nr (k int, cust int, store int, v numeric(7,2)) DISTRIBUTED BY (k);
CREATE TABLE df_nr2 (k int, cust int, store int, v numeric(7,2)) DISTRIBUTED BY (k);
INSERT INTO df_nr SELECT i, i % 997, i % 7, (i % 1000) * 0.37 FROM generate_series(1, 30000) i;
INSERT INTO df_nr2 SELECT i, i % 89, i % 5, (i % 300) * 1.25 FROM generate_series(1, 9000) i;
ANALYZE df_nr;
ANALYZE df_nr2;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) WITH ctr AS (SELECT cust, store, sum(v) AS total FROM df_nr GROUP BY cust, store)
SELECT count(*), sum(c1.total) FROM ctr c1
WHERE c1.total > (SELECT avg(total) * 1.2 FROM ctr c2 WHERE c2.store = c1.store);
-- an Append and GPORCA's CTE: their slices stop at what PostgreSQL still
-- runs (partial states, the Sequence), no more at the numeric columns
EXPLAIN (COSTS OFF) SELECT store, sum(v), max(v) FROM
  (SELECT store, v FROM df_nr UNION ALL SELECT store, v FROM df_nr2) u GROUP BY store;
SET optimizer = on;
EXPLAIN (COSTS OFF) WITH ctr AS (SELECT cust, store, sum(v) AS total FROM df_nr GROUP BY cust, store)
SELECT count(*), sum(c1.total) FROM ctr c1
WHERE c1.total > (SELECT avg(total) * 1.2 FROM ctr c2 WHERE c2.store = c1.store);
SET optimizer = off;
SET datafusion.mode = off;
WITH ctr AS (SELECT cust, store, sum(v) AS total FROM df_nr GROUP BY cust, store)
SELECT count(*), sum(c1.total) FROM ctr c1
WHERE c1.total > (SELECT avg(total) * 1.2 FROM ctr c2 WHERE c2.store = c1.store);
SELECT store, sum(v), max(v) FROM
  (SELECT store, v FROM df_nr UNION ALL SELECT store, v FROM df_nr2) u GROUP BY store ORDER BY store;
SET datafusion.mode = on;
WITH ctr AS (SELECT cust, store, sum(v) AS total FROM df_nr GROUP BY cust, store)
SELECT count(*), sum(c1.total) FROM ctr c1
WHERE c1.total > (SELECT avg(total) * 1.2 FROM ctr c2 WHERE c2.store = c1.store);
SELECT store, sum(v), max(v) FROM
  (SELECT store, v FROM df_nr UNION ALL SELECT store, v FROM df_nr2) u GROUP BY store ORDER BY store;
SET optimizer = on;
WITH ctr AS (SELECT cust, store, sum(v) AS total FROM df_nr GROUP BY cust, store)
SELECT count(*), sum(c1.total) FROM ctr c1
WHERE c1.total > (SELECT avg(total) * 1.2 FROM ctr c2 WHERE c2.store = c1.store);
SET optimizer = off;
SET datafusion.mode = off;
RESET datafusion.motion_batches;
DROP TABLE df_nr, df_nr2;

-- PS1: a partial sum or avg in DataFusion whose combining aggregate stays
-- on PostgreSQL (a HAVING it cannot run): the C side writes PostgreSQL's
-- states from DataFusion's, numeric_avg_serialize's and int8_avg_serialize's
-- bytea and avg(int)'s int8[].  sum and avg of one column share PostgreSQL's
-- state, combined from either column: the two agree, counts and all.  A
-- group of NaN, one of NULLs, sums past int8.
CREATE TABLE df_ps (k int, g int, v numeric(9,2), i int, s smallint, b bigint) DISTRIBUTED BY (k);
INSERT INTO df_ps SELECT n, n % 40,
  CASE WHEN n % 13 = 0 THEN NULL WHEN n % 7 = 0 THEN -n * 0.11 ELSE n * 0.37 END,
  n % 1000 - 300, (n % 77)::smallint,
  n::bigint * 1000003 * (CASE WHEN n % 5 = 0 THEN -1 ELSE 1 END)
FROM generate_series(1, 50000) n;
INSERT INTO df_ps SELECT n, 99, NULL, NULL, NULL, NULL FROM generate_series(50001, 50010) n;
INSERT INTO df_ps VALUES (60001, 98, 1.50, 1, 1, 1), (60002, 98, 'NaN', 2, 2, 2),
  (60003, 98, 2.25, 3, 3, 9223372036854775807), (60004, 98, NULL, NULL, NULL, 9223372036854775807);
ANALYZE df_ps;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, sum(v), avg(v), avg(i), avg(s), sum(b), avg(b), count(*), count(v)
FROM df_ps GROUP BY g HAVING count(*)::text ~ '^[0-9]' ORDER BY g;
SET datafusion.mode = off;
SELECT g, sum(v), avg(v), avg(i), avg(s), sum(b), avg(b), count(*), count(v)
FROM df_ps GROUP BY g HAVING count(*)::text ~ '^[0-9]' ORDER BY g;
SELECT sum(v), avg(v), avg(i), sum(b), avg(b) FROM df_ps WHERE g > 1000 HAVING count(*)::text ~ '^[0-9]';
SET datafusion.mode = on;
SELECT g, sum(v), avg(v), avg(i), avg(s), sum(b), avg(b), count(*), count(v)
FROM df_ps GROUP BY g HAVING count(*)::text ~ '^[0-9]' ORDER BY g;
SELECT sum(v), avg(v), avg(i), sum(b), avg(b) FROM df_ps WHERE g > 1000 HAVING count(*)::text ~ '^[0-9]';
-- and the other way: the partial aggregate stays on PostgreSQL (a filter
-- DataFusion cannot run), its combining aggregate reads PostgreSQL's states
-- from tuples as DataFusion's columns
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, sum(v), avg(v), avg(i), avg(s), sum(b), avg(b), count(*), count(v)
FROM df_ps WHERE k::text ~ '^[0-9]' GROUP BY g ORDER BY g;
SET datafusion.mode = off;
SELECT g, sum(v), avg(v), avg(i), avg(s), sum(b), avg(b), count(*), count(v)
FROM df_ps WHERE k::text ~ '^[0-9]' GROUP BY g ORDER BY g;
SELECT sum(v), avg(v), avg(i), sum(b), avg(b) FROM df_ps WHERE k::text ~ '^[0-9]' AND g > 1000;
SET datafusion.mode = on;
SELECT g, sum(v), avg(v), avg(i), avg(s), sum(b), avg(b), count(*), count(v)
FROM df_ps WHERE k::text ~ '^[0-9]' GROUP BY g ORDER BY g;
SELECT sum(v), avg(v), avg(i), sum(b), avg(b) FROM df_ps WHERE k::text ~ '^[0-9]' AND g > 1000;
-- without groups over no rows, PostgreSQL's partial state is NULL; a sum of
-- quotients passes the largest display scale only through batch Motions
-- (DN1), so with its partial stage on PostgreSQL it stays there too
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT sum(v), avg(v), avg(i), sum(b), avg(b), count(*) FROM df_ps WHERE k::text ~ '^x';
EXPLAIN (COSTS OFF) SELECT g, sum(v / 3), avg(v / 3) FROM df_ps WHERE k::text ~ '^[0-9]' GROUP BY g;
SET datafusion.mode = off;
SELECT sum(v), avg(v), avg(i), sum(b), avg(b), count(*) FROM df_ps WHERE k::text ~ '^x';
SELECT g, sum(v / 3), avg(v / 3) FROM df_ps WHERE k::text ~ '^[0-9]' AND g < 3 GROUP BY g ORDER BY g;
SET datafusion.mode = on;
SELECT sum(v), avg(v), avg(i), sum(b), avg(b), count(*) FROM df_ps WHERE k::text ~ '^x';
SELECT g, sum(v / 3), avg(v / 3) FROM df_ps WHERE k::text ~ '^[0-9]' AND g < 3 GROUP BY g ORDER BY g;
SET optimizer = on;
SELECT sum(v), avg(v), avg(i), sum(b), avg(b), count(*) FROM df_ps WHERE k::text ~ '^x';
SET optimizer = off;
SET datafusion.mode = off;
RESET datafusion.motion_batches;
DROP TABLE df_ps;

-- DN1: values with display scales of their own, row by row: a quotient,
-- whose scale select_div_scale picks by the values, a CASE or COALESCE of
-- branches of different scales, and sums or arithmetic of them.  Each is
-- carried at the largest scale it may have, bounded by its operands'
-- precision and scale, and compared, sorted and joined as a value; where
-- it leaves DataFusion, or divides or is added up, its display scale goes
-- beside it, as numeric.c gives it; abs keeps it.  A comparison of
-- values beyond 76 digits at a common scale (a quotient of an avg, of the
-- largest scale numeric_avg may pick) is exact: each side as PostgreSQL
-- computes it, in arbitrary precision.  An Append of inputs of different
-- scales gives such values too.  A Motion of such a column carries
-- tuples, which hold it; min, max, avg and GROUP BY of one stay on
-- PostgreSQL.
CREATE TABLE df_dn (k int, i int, a numeric(7,2), b numeric(7,2), c numeric(12,4)) DISTRIBUTED BY (k);
INSERT INTO df_dn SELECT g, g % 37,
  CASE WHEN g % 53 = 0 THEN NULL ELSE ((g * 7919) % 100000 - 30000) / 100.0 END,
  CASE WHEN g % 41 = 0 THEN 0 WHEN g % 43 = 0 THEN NULL ELSE ((g * 104729) % 9000 + 1) / 100.0 END,
  ((g * 31) % 1000000) / 10000.0 FROM generate_series(1, 20000) g;
INSERT INTO df_dn VALUES (20001, 1, 'NaN', 2.00, 1), (20002, 2, 3.00, 'NaN', 1);
ANALYZE df_dn;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT k, a / b, c / 3, (a / b) / 3, a / (b / 3), a / b + c, (a / b) * 2 FROM df_dn WHERE b <> 0 AND k % 797 = 1 ORDER BY k;
EXPLAIN (COSTS OFF) SELECT i, sum(a / 2), sum(a / 7), count(*) FROM df_dn GROUP BY i ORDER BY i;
EXPLAIN (COSTS OFF) SELECT g, r FROM (SELECT i g, sum(a) / sum(b) r FROM df_dn GROUP BY i) x WHERE r > 1 ORDER BY r, g;
EXPLAIN (COSTS OFF) SELECT x.g, x.s, x.r, y.n FROM (SELECT i g, sum(a / 3) s, sum(a) / sum(b) r FROM df_dn GROUP BY i) x JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = x.g ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT y.k, x.r, x.r + y.c FROM df_dn y JOIN (SELECT i, a / b r FROM df_dn WHERE b <> 0 AND k < 300 OFFSET 0) x ON x.i = y.k ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT i, max(a / b) FROM df_dn WHERE b <> 0 GROUP BY i;
EXPLAIN (COSTS OFF) SELECT a / b AS r, count(*) FROM df_dn WHERE b <> 0 GROUP BY 1;
EXPLAIN (COSTS OFF) SELECT k, abs(a / b) FROM df_dn WHERE b <> 0 AND abs(a - b) > 100 ORDER BY abs(a - c), k;
EXPLAIN (COSTS OFF) SELECT i, s, av FROM (SELECT i, sum(a) s, avg(a) av, count(a) n FROM df_dn GROUP BY i) x WHERE CASE WHEN av <> 0 THEN abs(s - av) / av ELSE NULL END > (n - 1)::numeric ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT t.i, t.v, t.v / y.n FROM (SELECT i, sum(a) v FROM df_dn GROUP BY i UNION ALL SELECT i, sum(c) FROM df_dn GROUP BY i) t JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = t.i + 1 WHERE t.v > 100 ORDER BY 1, 2;
SET datafusion.mode = off;
SELECT k, a / b, c / 3, (a / b) / 3, a / (b / 3), a / b + c, (a / b) * 2 FROM df_dn WHERE b <> 0 AND k % 797 = 1 ORDER BY k;
SELECT k, a / b FROM df_dn WHERE b <> 0 ORDER BY a / b DESC, k LIMIT 7;
SELECT count(*), sum(a) FROM df_dn WHERE b <> 0 AND a / b > 1.5;
SELECT count(*) FROM df_dn WHERE CASE WHEN b = 0 THEN 0 ELSE a / b END > 2;
SELECT i, sum(a / 2), sum(a / 7), count(*) FROM df_dn GROUP BY i ORDER BY i;
SELECT i, sum(a) / sum(b), sum(c) / count(*) FROM df_dn GROUP BY i ORDER BY i;
SELECT k, coalesce(a * i, 0), coalesce(a, 0.0), CASE WHEN b > 50 THEN a / b ELSE c END FROM df_dn WHERE k % 530 = 0 OR k % 1010 = 0 ORDER BY k;
SELECT g, r FROM (SELECT i g, sum(a) / sum(b) r FROM df_dn GROUP BY i) x WHERE r > 1 ORDER BY r, g;
SELECT x.g, x.s, x.r, y.n FROM (SELECT i g, sum(a / 3) s, sum(a) / sum(b) r FROM df_dn GROUP BY i) x JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = x.g ORDER BY 1;
SELECT k, a / b FROM df_dn WHERE k IN (20001, 20002) ORDER BY k;
SELECT k, abs(a), abs(a - c), abs(a / b), abs(b - a) / b FROM df_dn WHERE b <> 0 AND (k % 1597 = 0 OR k IN (20001, 20002)) ORDER BY abs(a - c), k;
SELECT count(*) FROM df_dn WHERE abs(a - b) > 100;
SELECT i, s, av FROM (SELECT i, sum(a) s, avg(a) av, count(a) n FROM df_dn GROUP BY i) x WHERE CASE WHEN av <> 0 THEN abs(s - av) / av ELSE NULL END > (n - 1)::numeric ORDER BY 1;
SELECT i FROM (SELECT i, sum(a) s, avg(a) av, count(a) n FROM df_dn GROUP BY i) x WHERE (s - av) / av = (n - 1)::numeric ORDER BY 1;
SELECT t.i, t.v, t.v / y.n FROM (SELECT i, sum(a) v FROM df_dn GROUP BY i UNION ALL SELECT i, sum(c) FROM df_dn GROUP BY i) t JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = t.i + 1 WHERE t.v > 100 ORDER BY 1, 2;
SELECT t.i, t.v FROM (SELECT i, sum(a / 3) v FROM df_dn GROUP BY i UNION ALL SELECT i, sum(c) FROM df_dn GROUP BY i) t JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = t.i + 1 ORDER BY 2 DESC, 1 LIMIT 10;
SELECT i FROM (SELECT i, avg(a) x1, avg(c) x2, sum(a) s1, sum(c) s2 FROM df_dn GROUP BY i) x WHERE CASE WHEN x1 > 0 THEN s1 / x1 END >= CASE WHEN x2 > 0 THEN s2 / x2 END ORDER BY 1;
SELECT y.k, x.r, x.r + y.c FROM df_dn y JOIN (SELECT i, a / b r FROM df_dn WHERE b <> 0 AND k < 300 OFFSET 0) x ON x.i = y.k ORDER BY 1, 2;
SET datafusion.mode = on;
SELECT k, a / b, c / 3, (a / b) / 3, a / (b / 3), a / b + c, (a / b) * 2 FROM df_dn WHERE b <> 0 AND k % 797 = 1 ORDER BY k;
SELECT k, a / b FROM df_dn WHERE b <> 0 ORDER BY a / b DESC, k LIMIT 7;
SELECT count(*), sum(a) FROM df_dn WHERE b <> 0 AND a / b > 1.5;
SELECT count(*) FROM df_dn WHERE CASE WHEN b = 0 THEN 0 ELSE a / b END > 2;
SELECT i, sum(a / 2), sum(a / 7), count(*) FROM df_dn GROUP BY i ORDER BY i;
SELECT i, sum(a) / sum(b), sum(c) / count(*) FROM df_dn GROUP BY i ORDER BY i;
SELECT k, coalesce(a * i, 0), coalesce(a, 0.0), CASE WHEN b > 50 THEN a / b ELSE c END FROM df_dn WHERE k % 530 = 0 OR k % 1010 = 0 ORDER BY k;
SELECT g, r FROM (SELECT i g, sum(a) / sum(b) r FROM df_dn GROUP BY i) x WHERE r > 1 ORDER BY r, g;
SELECT x.g, x.s, x.r, y.n FROM (SELECT i g, sum(a / 3) s, sum(a) / sum(b) r FROM df_dn GROUP BY i) x JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = x.g ORDER BY 1;
SELECT k, a / b FROM df_dn WHERE k IN (20001, 20002) ORDER BY k;
SELECT k, abs(a), abs(a - c), abs(a / b), abs(b - a) / b FROM df_dn WHERE b <> 0 AND (k % 1597 = 0 OR k IN (20001, 20002)) ORDER BY abs(a - c), k;
SELECT count(*) FROM df_dn WHERE abs(a - b) > 100;
SELECT i, s, av FROM (SELECT i, sum(a) s, avg(a) av, count(a) n FROM df_dn GROUP BY i) x WHERE CASE WHEN av <> 0 THEN abs(s - av) / av ELSE NULL END > (n - 1)::numeric ORDER BY 1;
SELECT i FROM (SELECT i, sum(a) s, avg(a) av, count(a) n FROM df_dn GROUP BY i) x WHERE (s - av) / av = (n - 1)::numeric ORDER BY 1;
SELECT t.i, t.v, t.v / y.n FROM (SELECT i, sum(a) v FROM df_dn GROUP BY i UNION ALL SELECT i, sum(c) FROM df_dn GROUP BY i) t JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = t.i + 1 WHERE t.v > 100 ORDER BY 1, 2;
SELECT t.i, t.v FROM (SELECT i, sum(a / 3) v FROM df_dn GROUP BY i UNION ALL SELECT i, sum(c) FROM df_dn GROUP BY i) t JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = t.i + 1 ORDER BY 2 DESC, 1 LIMIT 10;
SELECT i FROM (SELECT i, avg(a) x1, avg(c) x2, sum(a) s1, sum(c) s2 FROM df_dn GROUP BY i) x WHERE CASE WHEN x1 > 0 THEN s1 / x1 END >= CASE WHEN x2 > 0 THEN s2 / x2 END ORDER BY 1;
SELECT y.k, x.r, x.r + y.c FROM df_dn y JOIN (SELECT i, a / b r FROM df_dn WHERE b <> 0 AND k < 300 OFFSET 0) x ON x.i = y.k ORDER BY 1, 2;
SET optimizer = on;
SET datafusion.mode = off;
SELECT i, sum(a / 2), sum(a / 7), count(*) FROM df_dn GROUP BY i ORDER BY i;
SELECT i, sum(a) / sum(b), sum(c) / count(*) FROM df_dn GROUP BY i ORDER BY i;
SELECT k, coalesce(a * i, 0), coalesce(a, 0.0), CASE WHEN b > 50 THEN a / b ELSE c END FROM df_dn WHERE k % 530 = 0 OR k % 1010 = 0 ORDER BY k;
SELECT g, r FROM (SELECT i g, sum(a) / sum(b) r FROM df_dn GROUP BY i) x WHERE r > 1 ORDER BY r, g;
SELECT x.g, x.s, x.r, y.n FROM (SELECT i g, sum(a / 3) s, sum(a) / sum(b) r FROM df_dn GROUP BY i) x JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = x.g ORDER BY 1;
SET datafusion.mode = on;
SELECT i, sum(a / 2), sum(a / 7), count(*) FROM df_dn GROUP BY i ORDER BY i;
SELECT i, sum(a) / sum(b), sum(c) / count(*) FROM df_dn GROUP BY i ORDER BY i;
SELECT k, coalesce(a * i, 0), coalesce(a, 0.0), CASE WHEN b > 50 THEN a / b ELSE c END FROM df_dn WHERE k % 530 = 0 OR k % 1010 = 0 ORDER BY k;
SELECT g, r FROM (SELECT i g, sum(a) / sum(b) r FROM df_dn GROUP BY i) x WHERE r > 1 ORDER BY r, g;
SELECT x.g, x.s, x.r, y.n FROM (SELECT i g, sum(a / 3) s, sum(a) / sum(b) r FROM df_dn GROUP BY i) x JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = x.g ORDER BY 1;
SET optimizer = off;
-- division by zero, as PostgreSQL raises it; a guard before it in an AND
-- keeps the rows it fails out
SELECT k, a / 0 FROM df_dn WHERE k = 5;
SELECT sum(a / b) FROM df_dn;
-- AV2: an avg returning numeric that DataFusion divides (pg_avg) holds
-- PostgreSQL's quotient at a fixed scale; numeric_avg's display scale,
-- picked from the sum and the count, goes beside it, so it sorts in
-- DataFusion and leaves it above its Agg, as DN1's values do.  Through
-- batches it passes as a value only.
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT i, avg(a) FROM df_dn GROUP BY i ORDER BY avg(a) DESC NULLS LAST, i LIMIT 7;
EXPLAIN (COSTS OFF) SELECT x.g, x.m, y.n FROM (SELECT i g, avg(a) m FROM df_dn GROUP BY i) x JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = x.g ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT g, m, m * 3 FROM (SELECT i g, avg(a) m FROM df_dn GROUP BY i OFFSET 0) x ORDER BY m DESC, g;
SET optimizer = off;
SET datafusion.mode = off;
SELECT i, avg(a), avg(c) FROM df_dn GROUP BY i ORDER BY 2, 1;
SELECT i, avg(a) FROM df_dn GROUP BY i ORDER BY avg(a) DESC NULLS LAST, i LIMIT 7;
SELECT i % 5, avg(k), count(*) FROM df_dn GROUP BY 1 ORDER BY 2;
SELECT i, avg(a) * 2, avg(a) + avg(c), coalesce(avg(b), 0) FROM df_dn GROUP BY i ORDER BY 1;
SELECT x.g, x.m, y.n FROM (SELECT i g, avg(a) m FROM df_dn GROUP BY i) x JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = x.g ORDER BY 1;
SELECT * FROM (SELECT avg(a) FROM df_dn WHERE i < 5) a, (SELECT avg(c), count(*) FROM df_dn WHERE i > 30) b, (SELECT avg(k) FROM df_dn) c;
SELECT g, m, m * 3 FROM (SELECT i g, avg(a) m FROM df_dn GROUP BY i OFFSET 0) x ORDER BY m DESC, g;
SELECT k % 7, avg(k::numeric(9,0) * 1000) FROM df_dn GROUP BY 1 ORDER BY 2;
SET datafusion.mode = on;
SELECT i, avg(a), avg(c) FROM df_dn GROUP BY i ORDER BY 2, 1;
SELECT i, avg(a) FROM df_dn GROUP BY i ORDER BY avg(a) DESC NULLS LAST, i LIMIT 7;
SELECT i % 5, avg(k), count(*) FROM df_dn GROUP BY 1 ORDER BY 2;
SELECT i, avg(a) * 2, avg(a) + avg(c), coalesce(avg(b), 0) FROM df_dn GROUP BY i ORDER BY 1;
SELECT x.g, x.m, y.n FROM (SELECT i g, avg(a) m FROM df_dn GROUP BY i) x JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = x.g ORDER BY 1;
SELECT * FROM (SELECT avg(a) FROM df_dn WHERE i < 5) a, (SELECT avg(c), count(*) FROM df_dn WHERE i > 30) b, (SELECT avg(k) FROM df_dn) c;
SELECT g, m, m * 3 FROM (SELECT i g, avg(a) m FROM df_dn GROUP BY i OFFSET 0) x ORDER BY m DESC, g;
SELECT k % 7, avg(k::numeric(9,0) * 1000) FROM df_dn GROUP BY 1 ORDER BY 2;
SET optimizer = on;
SET datafusion.mode = off;
SELECT i, avg(a), avg(c) FROM df_dn GROUP BY i ORDER BY 2, 1;
SELECT i, avg(a) FROM df_dn GROUP BY i ORDER BY avg(a) DESC NULLS LAST, i LIMIT 7;
SELECT i % 5, avg(k), count(*) FROM df_dn GROUP BY 1 ORDER BY 2;
SELECT i, avg(a) * 2, avg(a) + avg(c), coalesce(avg(b), 0) FROM df_dn GROUP BY i ORDER BY 1;
SELECT x.g, x.m, y.n FROM (SELECT i g, avg(a) m FROM df_dn GROUP BY i) x JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = x.g ORDER BY 1;
SELECT * FROM (SELECT avg(a) FROM df_dn WHERE i < 5) a, (SELECT avg(c), count(*) FROM df_dn WHERE i > 30) b, (SELECT avg(k) FROM df_dn) c;
SELECT g, m, m * 3 FROM (SELECT i g, avg(a) m FROM df_dn GROUP BY i OFFSET 0) x ORDER BY m DESC, g;
SELECT k % 7, avg(k::numeric(9,0) * 1000) FROM df_dn GROUP BY 1 ORDER BY 2;
SET datafusion.mode = on;
SELECT i, avg(a), avg(c) FROM df_dn GROUP BY i ORDER BY 2, 1;
SELECT i, avg(a) FROM df_dn GROUP BY i ORDER BY avg(a) DESC NULLS LAST, i LIMIT 7;
SELECT i % 5, avg(k), count(*) FROM df_dn GROUP BY 1 ORDER BY 2;
SELECT i, avg(a) * 2, avg(a) + avg(c), coalesce(avg(b), 0) FROM df_dn GROUP BY i ORDER BY 1;
SELECT x.g, x.m, y.n FROM (SELECT i g, avg(a) m FROM df_dn GROUP BY i) x JOIN (SELECT i, count(*) n FROM df_dn GROUP BY i) y ON y.i = x.g ORDER BY 1;
SELECT * FROM (SELECT avg(a) FROM df_dn WHERE i < 5) a, (SELECT avg(c), count(*) FROM df_dn WHERE i > 30) b, (SELECT avg(k) FROM df_dn) c;
SELECT g, m, m * 3 FROM (SELECT i g, avg(a) m FROM df_dn GROUP BY i OFFSET 0) x ORDER BY m DESC, g;
SELECT k % 7, avg(k::numeric(9,0) * 1000) FROM df_dn GROUP BY 1 ORDER BY 2;
SET optimizer = off;
SET datafusion.mode = off;
RESET datafusion.motion_batches;
DROP TABLE df_dn;

DROP TABLE df_nm, df_nm_empty, df_nc, df_nc2, df_nc3, df_nc4;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
