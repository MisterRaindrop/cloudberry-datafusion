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
-- where PostgreSQL runs them), IS [NOT] DISTINCT FROM.  NULLs follow SQL's
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

DROP TABLE df_ex;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
