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
-- an avg's numeric result has no fixed scale: not computed on further
EXPLAIN (COSTS OFF) SELECT g, avg(a) + 1 FROM df_nm GROUP BY g;
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
EXPLAIN (COSTS OFF) SELECT a / b FROM df_nc;

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

-- + - * (N3), with the scales of numeric.c, up to 76 digits (c * c)
SELECT id, a + b, a - b, a * b, a * 2, 1 - a, a * 0.5, c * c, d * d, a + id
FROM df_nc WHERE id < 0 OR id % 9973 = 0 ORDER BY id;
SELECT g, sum(a * (1 - b)), sum(a * (1 - b) * (1 + a)), avg(a - 1) FROM df_nc GROUP BY g ORDER BY g;
SELECT count(*) FROM df_nc WHERE a * 2 > b + 1.5 AND a - b < 100;
SET datafusion.mode = on;
SELECT id, a + b, a - b, a * b, a * 2, 1 - a, a * 0.5, c * c, d * d, a + id
FROM df_nc WHERE id < 0 OR id % 9973 = 0 ORDER BY id;
SELECT g, sum(a * (1 - b)), sum(a * (1 - b) * (1 + a)), avg(a - 1) FROM df_nc GROUP BY g ORDER BY g;
SELECT count(*) FROM df_nc WHERE a * 2 > b + 1.5 AND a - b < 100;
RESET datafusion.motion_batches;

DROP TABLE df_nm, df_nm_empty, df_nc, df_nc2;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
