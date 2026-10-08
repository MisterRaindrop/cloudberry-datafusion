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

DROP TABLE df_nm, df_nm_empty, df_nc, df_nc2, df_nc3, df_nc4;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
