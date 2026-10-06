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
-- Expressions (E1): IN and NOT IN lists of constants, CASE (searched and
-- simple), COALESCE and NULLIF (as CASE, so that later arguments only run
-- where PostgreSQL runs them), IS [NOT] DISTINCT FROM; casts (E2).  NULLs follow SQL's
-- three-valued logic as in PostgreSQL.  Each query runs with
-- datafusion.mode off, then on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;

CREATE TABLE df_ex (id int, a int, b int8, f float8, t text, d date, n numeric(10,2), m numeric(10,2))
DISTRIBUTED BY (id);
INSERT INTO df_ex SELECT i, CASE WHEN i % 13 = 0 THEN NULL ELSE i % 10 END, i % 7, (i % 5) / 2.0,
  CASE WHEN i % 11 = 0 THEN NULL ELSE 'k' || (i % 6) END, date '2020-01-01' + i % 9,
  CASE WHEN i % 17 = 0 THEN NULL WHEN i % 19 = 0 THEN 'NaN' ELSE (i % 8) / 4.0 END, (i % 3) * 1.25
FROM generate_series(1, 50000) i;
ANALYZE df_ex;

SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ex WHERE a IN (1, 3) AND t NOT IN ('k1') AND coalesce(b, 0) > 1;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ex WHERE a > ALL ('{0}');
EXPLAIN (COSTS OFF) SELECT CASE WHEN n > 1 THEN n ELSE 0 END FROM df_ex;

SET datafusion.mode = off;
SELECT count(*), count(a) FROM df_ex WHERE a IN (1, 3, 5);
SELECT (a IN (1, NULL)) IS NULL AS isn, a IN (1, NULL) AS r, count(*) FROM df_ex GROUP BY 1, 2 ORDER BY 1, 2;
SELECT (a NOT IN (1, NULL)) IS NULL AS isn, a NOT IN (1, NULL) AS r, count(*) FROM df_ex GROUP BY 1, 2 ORDER BY 1, 2;
SELECT count(*) FROM df_ex WHERE t IN ('k1', 'k3', '中') OR d IN ('2020-01-02', '2020-01-05');
SELECT count(*) FROM df_ex WHERE n IN (0.5, 1.25, 'NaN') AND b NOT IN (0, 6);
SELECT CASE WHEN a < 3 THEN 'low' WHEN a < 7 THEN 'mid' ELSE 'high' END AS k, count(*)
FROM df_ex GROUP BY 1 ORDER BY 1;
SELECT CASE a WHEN 1 THEN 10 WHEN 2 THEN 20 END AS k, count(*) FROM df_ex GROUP BY 1 ORDER BY 1;
-- the division only runs where b <> 0, as in PostgreSQL
SELECT sum(CASE WHEN b = 0 THEN 0 ELSE 100 / b END), count(*) FROM df_ex;
SELECT count(*), sum(coalesce(a, -1)), sum(coalesce(a, b, 0)) FROM df_ex;
-- coalesce's second argument only runs where a is NULL
SELECT sum(coalesce(a, 100 / (b - b))) FROM df_ex WHERE a IS NOT NULL;
SELECT count(*), sum(nullif(a, 3)), count(nullif(t, 'k1')) FROM df_ex;
SELECT count(*) FROM df_ex WHERE a IS NOT DISTINCT FROM NULL OR t IS NOT DISTINCT FROM 'k2';
SELECT count(*) FROM df_ex WHERE n IS DISTINCT FROM m;
SELECT coalesce(n, m) AS v, count(*) FROM df_ex GROUP BY 1 ORDER BY 1;
SET datafusion.mode = on;
SELECT count(*), count(a) FROM df_ex WHERE a IN (1, 3, 5);
SELECT (a IN (1, NULL)) IS NULL AS isn, a IN (1, NULL) AS r, count(*) FROM df_ex GROUP BY 1, 2 ORDER BY 1, 2;
SELECT (a NOT IN (1, NULL)) IS NULL AS isn, a NOT IN (1, NULL) AS r, count(*) FROM df_ex GROUP BY 1, 2 ORDER BY 1, 2;
SELECT count(*) FROM df_ex WHERE t IN ('k1', 'k3', '中') OR d IN ('2020-01-02', '2020-01-05');
SELECT count(*) FROM df_ex WHERE n IN (0.5, 1.25, 'NaN') AND b NOT IN (0, 6);
SELECT CASE WHEN a < 3 THEN 'low' WHEN a < 7 THEN 'mid' ELSE 'high' END AS k, count(*)
FROM df_ex GROUP BY 1 ORDER BY 1;
SELECT CASE a WHEN 1 THEN 10 WHEN 2 THEN 20 END AS k, count(*) FROM df_ex GROUP BY 1 ORDER BY 1;
SELECT sum(CASE WHEN b = 0 THEN 0 ELSE 100 / b END), count(*) FROM df_ex;
SELECT count(*), sum(coalesce(a, -1)), sum(coalesce(a, b, 0)) FROM df_ex;
SELECT sum(coalesce(a, 100 / (b - b))) FROM df_ex WHERE a IS NOT NULL;
SELECT count(*), sum(nullif(a, 3)), count(nullif(t, 'k1')) FROM df_ex;
SELECT count(*) FROM df_ex WHERE a IS NOT DISTINCT FROM NULL OR t IS NOT DISTINCT FROM 'k2';
SELECT count(*) FROM df_ex WHERE n IS DISTINCT FROM m;
SELECT coalesce(n, m) AS v, count(*) FROM df_ex GROUP BY 1 ORDER BY 1;

-- Casts (E2): widening as DataFusion casts (exact, or rounding as C);
-- narrowing, float to integer (rint), float8 to float4, date/timestamp,
-- numeric to integer (half away from zero), to float8 and to numeric(p, s)
-- with PostgreSQL's rounding and errors.
CREATE TABLE df_cs (id int, i2 int2, i4 int4, i8 int8, f4 float4, f8 float8, d date, ts timestamp,
  n numeric(12,3)) DISTRIBUTED BY (id);
INSERT INTO df_cs SELECT i, (i % 30000)::int2, i * 7, i::int8 * 1000003, i / 4.0 - 1000.5, i / 8.0 - 5000.5,
  date '1999-12-25' + (i % 20), timestamp '1999-12-31 23:00' + i * interval '17 minutes', (i - 50000) / 8.0
FROM generate_series(1, 50000) i;
INSERT INTO df_cs VALUES
  (-1, 32767, 2147483647, 9223372036854775807, 'NaN', 'NaN', 'infinity', 'infinity', 'NaN'),
  (-2, -32768, -2147483648, -9223372036854775808, '-Infinity', 'Infinity', '-infinity', '-infinity',
   -999999999.999),
  (-3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (-4, 0, 40000, 3000000000, 2.5, -2.5, '4713-01-01 BC', '4713-01-01 00:00:01 BC', 2.5),
  (-5, 1, -40000, -3000000000, -0.5, 1e300, '294276-12-31', '294276-12-31 23:59:59', -2.5),
  (-6, 2, 3, 4, 3.5, 1e-300, '2000-01-01', '1999-12-31 23:59:59.999999', 0.0005);
ANALYZE df_cs;
SET datafusion.mode = off;
SELECT id, i2::int8, i4::float8, i8::float4, f4::float8, i4::int2, f8::int4, f4::int2, d::timestamp,
  ts::date, n::int4, n::float8, n::numeric(10,1)
FROM df_cs WHERE id IN (-6, -4, -3) OR id % 9973 = 0 ORDER BY id;
SELECT sum(i4::int8 * 1000), sum(n::float8), sum((i4 + 0.5)::int4), count(*) FROM df_cs WHERE id > 0;
SET datafusion.mode = on;
SELECT id, i2::int8, i4::float8, i8::float4, f4::float8, i4::int2, f8::int4, f4::int2, d::timestamp,
  ts::date, n::int4, n::float8, n::numeric(10,1)
FROM df_cs WHERE id IN (-6, -4, -3) OR id % 9973 = 0 ORDER BY id;
SELECT sum(i4::int8 * 1000), sum(n::float8), sum((i4 + 0.5)::int4), count(*) FROM df_cs WHERE id > 0;
-- PostgreSQL's errors, with their SQLSTATEs
\set VERBOSITY sqlstate
SELECT count(i4::int2) FROM df_cs;
SELECT count(f8::int4) FROM df_cs WHERE id = -1;
SELECT count(f8::float4) FROM df_cs WHERE id = -6;
SELECT count(d::timestamp) FROM df_cs WHERE id = -5;
SELECT count(n::int4) FROM df_cs WHERE id = -1;
SELECT count(n::numeric(5,2)) FROM df_cs;
\set VERBOSITY default

DROP TABLE df_ex, df_cs;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
