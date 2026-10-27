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

-- A numeric init plan value anywhere in an expression (IP3, TPC-DS Q9,
-- Q23 and Q44 under the Postgres planner): a constant of the digits and
-- scale of its init plan's value, its display scale its own; NULL and NaN
-- as they are.  One of unknown digits (an unconstrained numeric's max)
-- stays on PostgreSQL.  Without sorted grouping the HAVING stays below the
-- Gather in a slice of its own, whichever plan ANALYZE's sample favours.
SET enable_groupagg = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, avg(a) FROM df_nc GROUP BY g HAVING avg(a) > 0.9 * (SELECT avg(a) FROM df_nc WHERE id > 0) ORDER BY g;
EXPLAIN (COSTS OFF) SELECT id, CASE WHEN (SELECT count(*) FROM df_nc WHERE id > 0) > 100 THEN (SELECT avg(a) FROM df_nc WHERE id > 0)
  ELSE (SELECT avg(b) FROM df_nc) END, (SELECT sum(b) FROM df_nc WHERE id > 0) * 2,
  a + (SELECT avg(d) FROM df_nc WHERE id > 0) FROM df_nc WHERE id IN (7, -2, -7) ORDER BY id;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_nc WHERE b * 2 > (SELECT max(u) FROM df_nc) + 1;
SET datafusion.mode = off;
SELECT g, avg(a) FROM df_nc GROUP BY g HAVING avg(a) > 0.9 * (SELECT avg(a) FROM df_nc WHERE id > 0) ORDER BY g;
SELECT count(*), sum(id) FROM df_nc WHERE a + (SELECT avg(a) FROM df_nc WHERE id > 0) > 0;
SELECT id, CASE WHEN (SELECT count(*) FROM df_nc WHERE id > 0) > 100 THEN (SELECT avg(a) FROM df_nc WHERE id > 0)
  ELSE (SELECT avg(b) FROM df_nc) END, (SELECT sum(b) FROM df_nc WHERE id > 0) * 2,
  a + (SELECT avg(d) FROM df_nc WHERE id > 0) FROM df_nc WHERE id IN (7, -2, -7) ORDER BY id;
SELECT count(*) FROM df_nc WHERE a < 2 * (SELECT avg(a) FROM df_nc WHERE id < -100);
SELECT count(*) FROM df_nc WHERE b < 0.5 * (SELECT avg(a) FROM df_nc);
SET datafusion.mode = on;
SELECT g, avg(a) FROM df_nc GROUP BY g HAVING avg(a) > 0.9 * (SELECT avg(a) FROM df_nc WHERE id > 0) ORDER BY g;
SELECT count(*), sum(id) FROM df_nc WHERE a + (SELECT avg(a) FROM df_nc WHERE id > 0) > 0;
SELECT id, CASE WHEN (SELECT count(*) FROM df_nc WHERE id > 0) > 100 THEN (SELECT avg(a) FROM df_nc WHERE id > 0)
  ELSE (SELECT avg(b) FROM df_nc) END, (SELECT sum(b) FROM df_nc WHERE id > 0) * 2,
  a + (SELECT avg(d) FROM df_nc WHERE id > 0) FROM df_nc WHERE id IN (7, -2, -7) ORDER BY id;
SELECT count(*) FROM df_nc WHERE a < 2 * (SELECT avg(a) FROM df_nc WHERE id < -100);
SELECT count(*) FROM df_nc WHERE b < 0.5 * (SELECT avg(a) FROM df_nc);
SET datafusion.mode = off;
RESET enable_groupagg;

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
-- beside it, as numeric.c gives it; abs keeps it, round gives its own
-- scale (RD1).  A comparison of
-- values beyond 76 digits at a common scale (a quotient of an avg, of the
-- largest scale numeric_avg may pick) is exact: each side as PostgreSQL
-- computes it, in arbitrary precision.  An Append of inputs of different
-- scales gives such values too.  A Motion of such a column carries
-- tuples, which hold it; min, max and avg of one stay on PostgreSQL.  A
-- GROUP BY of one groups by value (below).
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
EXPLAIN (COSTS OFF) SELECT count(*), sum(round(a / 7, 2)) FROM df_dn WHERE round(a / 7, 2) > 10;
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
SELECT k, round(a, 1), round(a / b, 3), round(c), round(a * 1.005, 2), round(a) FROM df_dn WHERE b <> 0 AND (k % 1597 = 0 OR k IN (20001, 20002)) ORDER BY round(a / b, 3), k;
SELECT count(*), sum(round(a / 7, 2)) FROM df_dn WHERE round(a / 7, 2) > 10;
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
SELECT k, round(a, 1), round(a / b, 3), round(c), round(a * 1.005, 2), round(a) FROM df_dn WHERE b <> 0 AND (k % 1597 = 0 OR k IN (20001, 20002)) ORDER BY round(a / b, 3), k;
SELECT count(*), sum(round(a / 7, 2)) FROM df_dn WHERE round(a / 7, 2) > 10;
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
-- DN1: a GROUP BY key of display scales of its own groups by value; the
-- group shows the largest display scale of its rows (PostgreSQL keeps the
-- first row's, in an order the plan does not fix; here they are alike
-- within a group); redistributed between the stages, it is hashed as a
-- value.  x - COALESCE(y, 0.0) of numeric(7, 2) x and y has x's display
-- scale throughout, as any sum or difference whose one side's display
-- scale is never below the other's, and goes through batch Motions.
CREATE TABLE df_dk (k int, x numeric(7,2), y numeric(7,2)) DISTRIBUTED BY (k);
INSERT INTO df_dk SELECT g, g % 50, CASE WHEN g % 11 = 0 THEN NULL ELSE g % 7 + 1 END FROM generate_series(1, 20000) g;
ANALYZE df_dk;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT x / y r, count(*), sum(x) FROM df_dk GROUP BY 1;
EXPLAIN (COSTS OFF) SELECT x - coalesce(y, 0.0) d, count(*) FROM df_dk GROUP BY 1;
SET datafusion.mode = off;
SELECT x / y r, count(*), sum(x) FROM df_dk GROUP BY 1 ORDER BY 1 LIMIT 12;
SELECT count(*), sum(r), max(n) FROM (SELECT x / y r, count(*) n FROM df_dk GROUP BY 1) s;
SELECT r, n FROM (SELECT x / y r, count(*) n FROM df_dk GROUP BY 1) s WHERE r > 20 ORDER BY 1;
SELECT x - coalesce(y, 0.0) d, count(*) FROM df_dk GROUP BY 1 ORDER BY 1 LIMIT 8;
SET datafusion.mode = on;
SELECT x / y r, count(*), sum(x) FROM df_dk GROUP BY 1 ORDER BY 1 LIMIT 12;
SELECT count(*), sum(r), max(n) FROM (SELECT x / y r, count(*) n FROM df_dk GROUP BY 1) s;
SELECT r, n FROM (SELECT x / y r, count(*) n FROM df_dk GROUP BY 1) s WHERE r > 20 ORDER BY 1;
SELECT x - coalesce(y, 0.0) d, count(*) FROM df_dk GROUP BY 1 ORDER BY 1 LIMIT 8;
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT x / y r, count(*), sum(x) FROM df_dk GROUP BY 1;
SET datafusion.mode = off;
SELECT x / y r, count(*), sum(x) FROM df_dk GROUP BY 1 ORDER BY 1 LIMIT 12;
SELECT count(*), sum(r), max(n) FROM (SELECT x / y r, count(*) n FROM df_dk GROUP BY 1) s;
SELECT x - coalesce(y, 0.0) d, count(*) FROM df_dk GROUP BY 1 ORDER BY 1 LIMIT 8;
SET datafusion.mode = on;
SELECT x / y r, count(*), sum(x) FROM df_dk GROUP BY 1 ORDER BY 1 LIMIT 12;
SELECT count(*), sum(r), max(n) FROM (SELECT x / y r, count(*) n FROM df_dk GROUP BY 1) s;
SELECT x - coalesce(y, 0.0) d, count(*) FROM df_dk GROUP BY 1 ORDER BY 1 LIMIT 8;
SET optimizer = off;
SET datafusion.mode = off;
DROP TABLE df_dk;
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

-- stddev and variance of int2 and int4 (STD1): DataFusion adds up their
-- count, sum and sum of squares exactly and finishes as
-- numeric_stddev_internal does, the variance rounded at select_div_scale's
-- scale and the root sqrt_var's, each value with that display scale: NULL
-- for no value (or one, for a sample), 0 where all are alike, int4's
-- extremes.  Over an output column, in an expression (TPC-DS Q17's
-- stddev / avg, a comparison), below a join.  Of int8 they stay on
-- PostgreSQL.  STD2: in two stages (TPC-DS Q35), the partial sum, count
-- and sum of squares pass through a batch Motion; without batch Motions
-- (PostgreSQL's serialized state) they stay on PostgreSQL.
CREATE TABLE df_sd (id int, g int, a int4, b int2, c int8) DISTRIBUTED BY (id);
INSERT INTO df_sd SELECT i, i % 37, CASE WHEN i % 11 = 0 THEN NULL ELSE (i * 7919) % 100003 - 50000 END,
  (i % 9)::int2, i FROM generate_series(1, 20000) i;
INSERT INTO df_sd VALUES (20001, 100, 5, 5, 5), (20002, 101, 2147483647, 1, 1), (20003, 101, -2147483648, 2, 2),
  (20004, 101, 0, 3, 3), (20005, 102, 7, 7, 7), (20006, 102, 7, 7, 7), (20007, 103, NULL, NULL, NULL);
ANALYZE df_sd;
SET datafusion.motion_batches = on;
SET gp_enable_multiphase_agg = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, stddev_samp(a), stddev_pop(a), var_samp(b), var_pop(b), stddev(b), variance(a) FROM df_sd GROUP BY g ORDER BY g;
EXPLAIN (COSTS OFF) SELECT g, stddev_samp(a) / avg(a) AS cov, stddev_samp(b) > 2.58 AS wide FROM df_sd GROUP BY g HAVING stddev_pop(a) > 28800 OR g > 99 ORDER BY g;
EXPLAIN (COSTS OFF) SELECT g, stddev_samp(c) FROM df_sd GROUP BY g;
RESET gp_enable_multiphase_agg;
EXPLAIN (COSTS OFF) SELECT g, stddev_samp(a) FROM df_sd GROUP BY g;
SET datafusion.motion_batches = off;
EXPLAIN (COSTS OFF) SELECT g, stddev_samp(a) FROM df_sd GROUP BY g;
SET datafusion.motion_batches = on;
SET gp_enable_multiphase_agg = off;
SET datafusion.mode = off;
SELECT g, stddev_samp(a), stddev_pop(a), var_samp(b), var_pop(b), stddev(b), variance(a) FROM df_sd GROUP BY g ORDER BY g;
SELECT g, stddev_samp(a) / avg(a) AS cov, stddev_samp(b) > 2.58 AS wide FROM df_sd GROUP BY g HAVING stddev_pop(a) > 28800 OR g > 99 ORDER BY g;
SELECT x.g, x.s, y.n FROM (SELECT g, stddev_samp(a) s FROM df_sd GROUP BY g) x JOIN (SELECT g, count(*) n FROM df_sd GROUP BY g) y ON x.g = y.g WHERE x.g % 9 = 1 OR x.g > 99 ORDER BY 1;
SET datafusion.mode = on;
SELECT g, stddev_samp(a), stddev_pop(a), var_samp(b), var_pop(b), stddev(b), variance(a) FROM df_sd GROUP BY g ORDER BY g;
SELECT g, stddev_samp(a) / avg(a) AS cov, stddev_samp(b) > 2.58 AS wide FROM df_sd GROUP BY g HAVING stddev_pop(a) > 28800 OR g > 99 ORDER BY g;
SELECT x.g, x.s, y.n FROM (SELECT g, stddev_samp(a) s FROM df_sd GROUP BY g) x JOIN (SELECT g, count(*) n FROM df_sd GROUP BY g) y ON x.g = y.g WHERE x.g % 9 = 1 OR x.g > 99 ORDER BY 1;
RESET gp_enable_multiphase_agg;
SET datafusion.mode = off;
SELECT g, stddev_samp(a), stddev_pop(a), var_samp(b), var_pop(b), stddev(b), variance(a) FROM df_sd GROUP BY g ORDER BY g;
SELECT stddev_samp(a), var_pop(b), stddev_samp(a) / avg(a) FROM df_sd;
SET datafusion.mode = on;
SELECT g, stddev_samp(a), stddev_pop(a), var_samp(b), var_pop(b), stddev(b), variance(a) FROM df_sd GROUP BY g ORDER BY g;
SELECT stddev_samp(a), var_pop(b), stddev_samp(a) / avg(a) FROM df_sd;
RESET datafusion.motion_batches;
SET datafusion.mode = off;
DROP TABLE df_sd;

-- QM1: a quotient's scale and digits follow from its operands' least
-- nonzero magnitude: a standard deviation of int4 values is above 10^-10
-- (a variance at least 1 / n) and below 10^10, an avg of them above
-- 10^-19, so stddev / avg is within 76 digits at scale 44, computed in an
-- output column, a filter, in one stage and two; and TPC-DS Q39's cov,
-- computed by PostgreSQL in a Shared Scan producer, is a column DataFusion
-- can receive above its readers (GPORCA).  Groups at the bounds: one
-- value 1 among zeros (the least variance and avg, 1 / n), one -1 among
-- zeros, int4's extremes (the largest standard deviation), extremes adding
-- up to 1 among zeros, all alike (0, a zero avg left to the guard).
CREATE TABLE df_qm (id int, k int, mo int, a int4, b int2) DISTRIBUTED BY (id);
CREATE TEMP TABLE df_qm0 (id int, k int, a int4, b int2);
INSERT INTO df_qm0 SELECT i, 1, CASE WHEN i = 7 THEN 1 ELSE 0 END, 0 FROM generate_series(1, 5000) i;
INSERT INTO df_qm0 SELECT 5000 + i, 2, CASE WHEN i = 3 THEN -1 ELSE 0 END, (i % 2)::int2 FROM generate_series(1, 3000) i;
INSERT INTO df_qm0 VALUES (8001, 3, 2147483647, 32767), (8002, 3, -2147483648, -32768);
INSERT INTO df_qm0 SELECT 8100 + i, 4, CASE WHEN i = 1 THEN 2147483647 WHEN i = 2 THEN -2147483646 ELSE 0 END, 1
  FROM generate_series(1, 10000) i;
INSERT INTO df_qm0 SELECT 18200 + i, 5, 0, 3 FROM generate_series(1, 50) i;
INSERT INTO df_qm0 SELECT 18300 + i, 6, i % 7 - 3, (i % 5)::int2 FROM generate_series(1, 700) i;
INSERT INTO df_qm SELECT id, k, 4, a, b FROM df_qm0 UNION ALL SELECT 100000 + id, k % 6 + 1, 5, a, b FROM df_qm0;
DROP TABLE df_qm0;
ANALYZE df_qm;
SET datafusion.motion_batches = on;
SET optimizer = off;
SET gp_enable_multiphase_agg = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT k, stddev_samp(a) / NULLIF(avg(a), 0) FROM df_qm WHERE mo = 4 GROUP BY k ORDER BY k;
EXPLAIN (COSTS OFF) SELECT k, mo, count(*) FROM df_qm GROUP BY k, mo HAVING CASE WHEN avg(a) = 0 THEN 0 ELSE stddev_samp(a) / avg(a) END > 1;
EXPLAIN (COSTS OFF) SELECT k, var_pop(b) / NULLIF(avg(b), 0), stddev_pop(a) / NULLIF(avg(a), 0) FROM df_qm WHERE mo = 4 GROUP BY k;
RESET gp_enable_multiphase_agg;
EXPLAIN (COSTS OFF) SELECT k, stddev_samp(a) / NULLIF(avg(a), 0) FROM df_qm WHERE mo = 4 GROUP BY k ORDER BY k;
SET optimizer = on;
EXPLAIN (COSTS OFF) WITH v AS (SELECT k, mo, stdev, mean, CASE mean WHEN 0 THEN NULL ELSE stdev / mean END cov
  FROM (SELECT k, mo, stddev_samp(a) stdev, avg(a) mean FROM df_qm GROUP BY k, mo) x
  WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1 OR k > 2)
  SELECT v1.k, v1.mo, v1.mean, v1.cov, v2.mo, v2.mean, v2.cov FROM v v1, v v2
  WHERE v1.k = v2.k AND v1.mo = 4 AND v2.mo = 5 ORDER BY v1.k, v1.mean, v1.cov, v2.cov;
SET optimizer = off;
SET gp_enable_multiphase_agg = off;
SET datafusion.mode = off;
SELECT k, stddev_samp(a) / NULLIF(avg(a), 0) FROM df_qm WHERE mo = 4 GROUP BY k ORDER BY k;
SELECT k, mo, count(*) FROM df_qm GROUP BY k, mo HAVING CASE WHEN avg(a) = 0 THEN 0 ELSE stddev_samp(a) / avg(a) END > 1 ORDER BY k, mo;
SELECT k, var_pop(b) / NULLIF(avg(b), 0), stddev_pop(a) / NULLIF(avg(a), 0) FROM df_qm WHERE mo = 4 GROUP BY k ORDER BY k;
SET datafusion.mode = on;
SELECT k, stddev_samp(a) / NULLIF(avg(a), 0) FROM df_qm WHERE mo = 4 GROUP BY k ORDER BY k;
SELECT k, mo, count(*) FROM df_qm GROUP BY k, mo HAVING CASE WHEN avg(a) = 0 THEN 0 ELSE stddev_samp(a) / avg(a) END > 1 ORDER BY k, mo;
SELECT k, var_pop(b) / NULLIF(avg(b), 0), stddev_pop(a) / NULLIF(avg(a), 0) FROM df_qm WHERE mo = 4 GROUP BY k ORDER BY k;
RESET gp_enable_multiphase_agg;
SELECT k, stddev_samp(a) / NULLIF(avg(a), 0) FROM df_qm WHERE mo = 4 GROUP BY k ORDER BY k;
SELECT k, mo, count(*) FROM df_qm GROUP BY k, mo HAVING CASE WHEN avg(a) = 0 THEN 0 ELSE stddev_samp(a) / avg(a) END > 1 ORDER BY k, mo;
SET optimizer = on;
SET datafusion.mode = off;
WITH v AS (SELECT k, mo, stdev, mean, CASE mean WHEN 0 THEN NULL ELSE stdev / mean END cov
  FROM (SELECT k, mo, stddev_samp(a) stdev, avg(a) mean FROM df_qm GROUP BY k, mo) x
  WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1 OR k > 2)
  SELECT v1.k, v1.mo, v1.mean, v1.cov, v2.mo, v2.mean, v2.cov FROM v v1, v v2
  WHERE v1.k = v2.k AND v1.mo = 4 AND v2.mo = 5 ORDER BY v1.k, v1.mean, v1.cov, v2.cov;
SET datafusion.mode = on;
WITH v AS (SELECT k, mo, stdev, mean, CASE mean WHEN 0 THEN NULL ELSE stdev / mean END cov
  FROM (SELECT k, mo, stddev_samp(a) stdev, avg(a) mean FROM df_qm GROUP BY k, mo) x
  WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1 OR k > 2)
  SELECT v1.k, v1.mo, v1.mean, v1.cov, v2.mo, v2.mean, v2.cov FROM v v1, v v2
  WHERE v1.k = v2.k AND v1.mo = 4 AND v2.mo = 5 ORDER BY v1.k, v1.mean, v1.cov, v2.cov;
-- QM1: a quotient's digits are those of the largest dividend over the
-- smallest nonzero divisor, none more for rounding: TPC-DS Q66's sum of
-- monthly sums of price * quantity (MS1, 34 digits) over an int4 square
-- footage is 66 digits at scale 32, a sum PostgreSQL can add up within 76.
CREATE TABLE df_qs (id int, w int, sq int, m int, price numeric(7,2), qty int) DISTRIBUTED BY (id);
INSERT INTO df_qs SELECT i, i % 4, CASE i % 4 WHEN 0 THEN 1 WHEN 1 THEN 2147483647 WHEN 2 THEN -3 ELSE 977 END,
  i % 3, CASE WHEN i % 17 = 0 THEN 99999.99 ELSE ((i * 7919) % 2000001 - 1000000) / 100.0 END,
  CASE WHEN i % 23 = 0 THEN 2147483647 ELSE i % 100 END FROM generate_series(1, 3000) i;
ANALYZE df_qs;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT w, sum(js / sq), sum(fs / sq) FROM (SELECT w, sq, sum(CASE WHEN m = 1 THEN price * qty ELSE 0 END) js,
  sum(CASE WHEN m = 2 THEN price * qty ELSE 0 END) fs FROM df_qs GROUP BY w, sq) x GROUP BY w ORDER BY w;
SET optimizer = off;
EXPLAIN (COSTS OFF) SELECT w, sum(js / sq), sum(fs / sq) FROM (SELECT w, sq, sum(CASE WHEN m = 1 THEN price * qty ELSE 0 END) js,
  sum(CASE WHEN m = 2 THEN price * qty ELSE 0 END) fs FROM df_qs GROUP BY w, sq) x GROUP BY w ORDER BY w;
SET datafusion.mode = off;
SELECT w, sum(js / sq), sum(fs / sq) FROM (SELECT w, sq, sum(CASE WHEN m = 1 THEN price * qty ELSE 0 END) js,
  sum(CASE WHEN m = 2 THEN price * qty ELSE 0 END) fs FROM df_qs GROUP BY w, sq) x GROUP BY w ORDER BY w;
SET datafusion.mode = on;
SELECT w, sum(js / sq), sum(fs / sq) FROM (SELECT w, sq, sum(CASE WHEN m = 1 THEN price * qty ELSE 0 END) js,
  sum(CASE WHEN m = 2 THEN price * qty ELSE 0 END) fs FROM df_qs GROUP BY w, sq) x GROUP BY w ORDER BY w;
SET optimizer = on;
SELECT w, sum(js / sq), sum(fs / sq) FROM (SELECT w, sq, sum(CASE WHEN m = 1 THEN price * qty ELSE 0 END) js,
  sum(CASE WHEN m = 2 THEN price * qty ELSE 0 END) fs FROM df_qs GROUP BY w, sq) x GROUP BY w ORDER BY w;
DROP TABLE df_qs;
SET optimizer = off;
RESET datafusion.motion_batches;
SET datafusion.mode = off;
DROP TABLE df_qm;

-- CN1: a CASE on a numeric value, CASE x WHEN v THEN ..., runs as the
-- searched CASE it stands for, x = v in each WHEN: on a column (NULL
-- matches no WHEN), an avg, a quotient, nested, in an output column and
-- a filter.  A standard deviation and an expression on one have their
-- display scale beside them, so TPC-DS Q39's cov, CASE mean WHEN 0 THEN
-- NULL ELSE stdev / mean END, is an output column under the Postgres
-- planner too, sent through a Motion and sorted by, and GPORCA's Shared
-- Scan producer computes its filter.  A value PostgreSQL computes once (a
-- volatile function) stays on PostgreSQL.
CREATE TABLE df_cn (id int, k int, mo int, a int4, x numeric(7,2), y numeric(5,1)) DISTRIBUTED BY (id);
INSERT INTO df_cn SELECT i, i % 6, 4 + (i / 6) % 2, CASE WHEN i % 6 = 5 THEN 0 ELSE (i * 37) % 11 - 3 END,
  CASE WHEN i % 13 = 0 THEN NULL ELSE ((i * 7) % 5) / 2.0 END, (i % 4) / 2.0 + 0.5 FROM generate_series(1, 3000) i;
ANALYZE df_cn;
SET datafusion.motion_batches = on;
SET optimizer = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT id, CASE x WHEN 1.5 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END, CASE x / y WHEN 1 THEN x END
  FROM df_cn WHERE CASE x WHEN 0 THEN false ELSE true END;
EXPLAIN (COSTS OFF) SELECT k, CASE avg(a) WHEN 0 THEN NULL ELSE stddev_samp(a) / avg(a) END,
  coalesce(stddev_pop(a) / NULLIF(avg(a), 0), 0) FROM df_cn GROUP BY k;
EXPLAIN (COSTS OFF) SELECT CASE (random() * 0)::numeric WHEN 0 THEN 1 END FROM df_cn;
EXPLAIN (COSTS OFF) WITH v AS (SELECT k, mo, stdev, mean, CASE mean WHEN 0 THEN NULL ELSE stdev / mean END cov
  FROM (SELECT k, mo, stddev_samp(a) stdev, avg(a) mean FROM df_cn GROUP BY k, mo) x
  WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1 OR k = 5)
  SELECT v1.k, v1.mean, v1.cov, v2.mean, v2.cov FROM v v1, v v2
  WHERE v1.k = v2.k AND v1.mo = 4 AND v2.mo = 5 ORDER BY v1.k, v1.mean, v1.cov, v2.mean, v2.cov;
SET datafusion.mode = off;
SELECT x, CASE x WHEN 1.5 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END, CASE x / y WHEN 1 THEN x END, count(*)
  FROM df_cn WHERE CASE x WHEN 0 THEN false ELSE true END GROUP BY 1, 2, 3 ORDER BY 1, 3;
SELECT k, CASE avg(a) WHEN 0 THEN NULL ELSE stddev_samp(a) / avg(a) END,
  coalesce(stddev_pop(a) / NULLIF(avg(a), 0), 0) FROM df_cn GROUP BY k ORDER BY k;
SELECT k, CASE avg(a) WHEN 0 THEN -1 ELSE CASE stddev_pop(a) WHEN 0 THEN 0 ELSE 1 END END FROM df_cn GROUP BY k ORDER BY k;
WITH v AS (SELECT k, mo, stdev, mean, CASE mean WHEN 0 THEN NULL ELSE stdev / mean END cov
  FROM (SELECT k, mo, stddev_samp(a) stdev, avg(a) mean FROM df_cn GROUP BY k, mo) x
  WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1 OR k = 5)
  SELECT v1.k, v1.mean, v1.cov, v2.mean, v2.cov FROM v v1, v v2
  WHERE v1.k = v2.k AND v1.mo = 4 AND v2.mo = 5 ORDER BY v1.k, v1.mean, v1.cov, v2.mean, v2.cov;
SET datafusion.mode = on;
SELECT x, CASE x WHEN 1.5 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END, CASE x / y WHEN 1 THEN x END, count(*)
  FROM df_cn WHERE CASE x WHEN 0 THEN false ELSE true END GROUP BY 1, 2, 3 ORDER BY 1, 3;
SELECT k, CASE avg(a) WHEN 0 THEN NULL ELSE stddev_samp(a) / avg(a) END,
  coalesce(stddev_pop(a) / NULLIF(avg(a), 0), 0) FROM df_cn GROUP BY k ORDER BY k;
SELECT k, CASE avg(a) WHEN 0 THEN -1 ELSE CASE stddev_pop(a) WHEN 0 THEN 0 ELSE 1 END END FROM df_cn GROUP BY k ORDER BY k;
WITH v AS (SELECT k, mo, stdev, mean, CASE mean WHEN 0 THEN NULL ELSE stdev / mean END cov
  FROM (SELECT k, mo, stddev_samp(a) stdev, avg(a) mean FROM df_cn GROUP BY k, mo) x
  WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1 OR k = 5)
  SELECT v1.k, v1.mean, v1.cov, v2.mean, v2.cov FROM v v1, v v2
  WHERE v1.k = v2.k AND v1.mo = 4 AND v2.mo = 5 ORDER BY v1.k, v1.mean, v1.cov, v2.mean, v2.cov;
DROP TABLE df_cn;

-- RL1: a coercion to numeric(p, s) of a value of scale s and at most p
-- digits is a relabeling of it (numeric_support), which keeps the value's
-- range: an avg of one with ROLLUP, in two stages (TPC-DS Q18), a sum, an
-- output column, a comparison, under both planners.  A coercion to another
-- scale rounds (E2).
CREATE TABLE df_rl (k int, g int, a numeric(7,2), b numeric(5,0)) DISTRIBUTED BY (k);
INSERT INTO df_rl SELECT i, i % 7, CASE WHEN i % 13 = 0 THEN NULL ELSE ((i * 7919) % 199999 - 99999) / 100.0 END,
  (i * 31) % 99999 - 49999 FROM generate_series(1, 5000) i;
ANALYZE df_rl;
SET datafusion.motion_batches = on;
SET optimizer = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, avg(a::numeric(12,2)), avg(b::decimal(9,0)) FROM df_rl GROUP BY ROLLUP (g) ORDER BY g;
EXPLAIN (COSTS OFF) SELECT k, a::numeric(12,2), b::numeric(10,0) FROM df_rl WHERE a::numeric(9,2) > 900 ORDER BY k;
SET datafusion.mode = off;
SELECT g, avg(a::numeric(12,2)), avg(b::decimal(9,0)) FROM df_rl GROUP BY ROLLUP (g) ORDER BY g;
SELECT g, sum(a::numeric(12,2)), sum(b::numeric(10,0)) FROM df_rl GROUP BY g ORDER BY g;
SELECT k, a::numeric(12,2), b::numeric(10,0), a::numeric(12,3) FROM df_rl WHERE a::numeric(9,2) > 990 ORDER BY k;
SET datafusion.mode = on;
SELECT g, avg(a::numeric(12,2)), avg(b::decimal(9,0)) FROM df_rl GROUP BY ROLLUP (g) ORDER BY g;
SELECT g, sum(a::numeric(12,2)), sum(b::numeric(10,0)) FROM df_rl GROUP BY g ORDER BY g;
SELECT k, a::numeric(12,2), b::numeric(10,0), a::numeric(12,3) FROM df_rl WHERE a::numeric(9,2) > 990 ORDER BY k;
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, avg(a::numeric(12,2)), avg(b::decimal(9,0)) FROM df_rl GROUP BY ROLLUP (g) ORDER BY g;
SET datafusion.mode = on;
SELECT g, avg(a::numeric(12,2)), avg(b::decimal(9,0)) FROM df_rl GROUP BY ROLLUP (g) ORDER BY g;
SELECT k, a::numeric(12,2), b::numeric(10,0), a::numeric(12,3) FROM df_rl WHERE a::numeric(9,2) > 990 ORDER BY k;
RESET datafusion.motion_batches;
SET datafusion.mode = off;
DROP TABLE df_rl;

DROP TABLE df_nm, df_nm_empty, df_nc, df_nc2, df_nc3, df_nc4;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
