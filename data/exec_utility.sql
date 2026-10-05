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
-- Body of the exec test, run in utility mode on the coordinator by
-- sql/exec.sql.  A table created in utility mode lives on the coordinator
-- only, so its plans have one slice and no Motion, which is what DataFusion
-- runs today.  The same file runs with datafusion.mode off and on, and the
-- outputs must match apart from the mode and the final takeover count.
--
LOAD 'datafusion_executor';
SET datafusion.mode = :dfmode;

CREATE TABLE df_m3 (a int4, b int8, c float8, d bool, e int2, f float4);
INSERT INTO df_m3
SELECT i, i * 10, i / 4.0, i % 2 = 0, (i % 100)::int2, (i / 8.0)::float4
FROM generate_series(1, 100000) i;
INSERT INTO df_m3 VALUES (NULL, NULL, NULL, NULL, NULL, NULL),
                         (100001, NULL, 1.5, NULL, 7, NULL);

-- Aggregates over every type, across many input batches.
SELECT count(*), count(a), count(b), count(c), count(d), count(e), count(f) FROM df_m3;
SELECT sum(a), sum(e), sum(c), min(a), max(a), min(b), max(b),
       min(c), max(c), min(e), max(e), min(f), max(f), avg(c), avg(f)
FROM df_m3;

-- Grouping, including the NULL group, and HAVING.
SELECT d, count(*), sum(a), max(c) FROM df_m3 GROUP BY d;
SELECT e, count(*), sum(a) FROM df_m3 WHERE a <= 1000 GROUP BY e HAVING sum(a) > 5000;
SELECT e, d, count(*) FROM df_m3 WHERE e < 3 GROUP BY e, d;

-- Filters: AND, OR, NOT, NULL tests and cross-type comparisons.
SELECT count(*) FROM df_m3 WHERE a > 50000 AND (d OR e = 7) AND NOT (c < 20000);
SELECT count(*) FROM df_m3 WHERE b > 999990;
SELECT count(*) FROM df_m3 WHERE e > 98::int8;
SELECT count(*) FROM df_m3 WHERE f > 12499.5::float8;
SELECT count(*), count(a) FROM df_m3 WHERE a IS NULL OR d IS NULL;
SELECT count(*) FROM df_m3 WHERE d IS NOT NULL AND a IS NOT NULL;

-- Arithmetic in the output.
SELECT a, a + 1, a - 1, a * 2, a / 7, a % 7, e + e, e * 300, b / 3, b % 7,
       c * 2, c / 4, f + f
FROM df_m3 WHERE a % 20000 = 0;

-- Integer division truncates toward zero; INT_MIN % -1 is 0.
CREATE TABLE df_m3_neg (x int4, y int4);
INSERT INTO df_m3_neg VALUES (-7, 2), (7, -2), (-8, 3), (-2147483648, -1);
SELECT x, y, x % y FROM df_m3_neg;
SELECT x, y, x / y FROM df_m3_neg WHERE y <> -1;

-- Errors carry PostgreSQL's SQLSTATE and message.
SELECT a + 2147483647 FROM df_m3 WHERE a = 1;
SELECT e * 1000::int2 FROM df_m3 WHERE a = 99;
SELECT b * 922337203685477580 FROM df_m3 WHERE a = 2;
SELECT a / (a - a) FROM df_m3 WHERE a = 5;
SELECT e % (e - e) FROM df_m3 WHERE a = 5;
SELECT c / 0 FROM df_m3 WHERE a = 5;
SELECT c * 1e308 FROM df_m3 WHERE a = 40;
SELECT c / 1e308 / 1e308 FROM df_m3 WHERE a = 1;
SELECT x / y FROM df_m3_neg WHERE y = -1;

-- The same failures, showing the SQLSTATE instead of the message.
\set VERBOSITY sqlstate
SELECT a + 2147483647 FROM df_m3 WHERE a = 1;
SELECT a / (a - a) FROM df_m3 WHERE a = 5;
SELECT c * 1e308 FROM df_m3 WHERE a = 40;
\set VERBOSITY default

-- A guard keeps the division from running where it would fail.
SELECT count(*) FROM df_m3 WHERE a <> 5 AND 10 / (a - 5) >= 0;
SELECT count(*) FROM df_m3 WHERE a = 5 OR 10 / (a - 5) >= 0;

-- No input rows.
SELECT count(*), sum(a), max(c) FROM df_m3 WHERE a < 0;
SELECT d, count(*) FROM df_m3 WHERE a < 0 GROUP BY d;

-- Special floating-point values.
CREATE TABLE df_m3_float (k int4, c float8, f float4);
INSERT INTO df_m3_float VALUES (1, 'NaN', 'NaN'), (2, 'Infinity', 'Infinity'),
  (3, '-Infinity', '-Infinity'), (4, '-0', '-0'), (5, '0', '0'), (6, 1.5, 1.5),
  (7, NULL, NULL);
SELECT count(*) FROM df_m3_float WHERE c = 'NaN';
SELECT count(*) FROM df_m3_float WHERE c > 1;
SELECT count(*) FROM df_m3_float WHERE c = 0;
SELECT count(*) FROM df_m3_float WHERE f = 0 OR f = 'NaN';
SELECT max(c), min(c), max(f), min(f) FROM df_m3_float;
SELECT sum(c) FROM df_m3_float WHERE k > 1;
SELECT k, c, c * 2, f, f * 2 FROM df_m3_float;

-- A cursor fetches the result in parts.
BEGIN;
DECLARE df_cur CURSOR FOR SELECT count(*), sum(a) FROM df_m3;
FETCH 1 FROM df_cur;
FETCH 1 FROM df_cur;
CLOSE df_cur;
COMMIT;

DROP TABLE df_m3;
DROP TABLE df_m3_neg;
DROP TABLE df_m3_float;

SELECT datafusion_debug_takeovers() AS takeovers;
