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
-- Engines (include/vec_engine.h): the null engine, which runs nothing; one
-- that claims another ABI version; DataFusion with features turned off
-- (datafusion.debug_features_off), whose slices that need them stay on
-- PostgreSQL; and DataFusion's result batches given at an offset
-- (datafusion.debug_result_offset), which must read the same.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
CREATE TABLE df_en (a int, b text, c numeric(10,2), d bool, e date, f float8, g int8)
  DISTRIBUTED BY (a);
INSERT INTO df_en SELECT i, 'v' || (i % 7) || CASE WHEN i % 5 = 0 THEN 'é' ELSE '' END,
  i * 1.25, i % 3 = 0, date '2024-01-01' + i, i / 4.0, i * 1000
FROM generate_series(1, 3000) i;
INSERT INTO df_en VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL);
CREATE TABLE df_en2 (a int, w text) DISTRIBUTED BY (a);
INSERT INTO df_en2 SELECT i, 'w' || (i % 3) FROM generate_series(1, 3000, 7) i;
ANALYZE df_en;
ANALYZE df_en2;

-- The null engine: the slices stay on PostgreSQL, saying why.
SET datafusion.engine = 'null';
SET datafusion.mode = on;
SELECT datafusion_version();
EXPLAIN (COSTS OFF) SELECT d, count(*), sum(c) FROM df_en GROUP BY d;
SELECT d, count(*), sum(c), min(b), max(e) FROM df_en GROUP BY d ORDER BY d;
SELECT datafusion_debug_panic();

-- An engine of another ABI version is refused.
\c
SET datafusion.engine = 'null_abi1';
SELECT datafusion_version();

-- DataFusion without one feature: what needs it stays on PostgreSQL, the
-- rest still runs in DataFusion.
\c
SET optimizer = off;
SET datafusion.mode = on;
SET datafusion.debug_features_off = 2;	-- joins
EXPLAIN (COSTS OFF) SELECT w, count(*) FROM df_en JOIN df_en2 USING (a) GROUP BY w;
SELECT w, count(*) FROM df_en JOIN df_en2 USING (a) GROUP BY w ORDER BY w;
EXPLAIN (COSTS OFF) SELECT d, count(*) FROM df_en GROUP BY d;
SET datafusion.debug_features_off = 4;	-- sorts
EXPLAIN (COSTS OFF) SELECT a, b FROM df_en ORDER BY g DESC LIMIT 3;
SELECT a, b FROM df_en ORDER BY g DESC NULLS LAST LIMIT 3;
SET datafusion.debug_features_off = 8;	-- window functions
EXPLAIN (COSTS OFF) SELECT a, sum(g) OVER (PARTITION BY d) FROM df_en WHERE a < 5;
SELECT a, sum(g) OVER (PARTITION BY d) FROM df_en WHERE a < 5 ORDER BY a;
SET datafusion.debug_features_off = 16;	-- grouping sets
EXPLAIN (COSTS OFF) SELECT d, b, count(*) FROM df_en WHERE a < 30 GROUP BY ROLLUP (d, b);
SELECT d, b, count(*) FROM df_en WHERE a < 30 GROUP BY ROLLUP (d, b) ORDER BY 1, 2;
SET datafusion.debug_features_off = 32;	-- set operations
EXPLAIN (COSTS OFF) SELECT a FROM df_en WHERE a < 3 UNION ALL SELECT a FROM df_en2 WHERE a < 9;
SELECT a FROM df_en WHERE a < 3 UNION ALL SELECT a FROM df_en2 WHERE a < 9 ORDER BY a;
SET datafusion.motion_batches = on;
SET datafusion.debug_features_off = 0;
EXPLAIN (COSTS OFF) SELECT count(*), sum(g), max(f) FROM df_en WHERE a < 50;
SET datafusion.debug_features_off = 128;	-- batch Motions
EXPLAIN (COSTS OFF) SELECT count(*), sum(g), max(f) FROM df_en WHERE a < 50;
SELECT count(*), sum(g), max(f) FROM df_en WHERE a < 50;
RESET datafusion.motion_batches;
SET datafusion.debug_features_off = 1;	-- any slice
EXPLAIN (COSTS OFF) SELECT d, count(*) FROM df_en GROUP BY d;
RESET datafusion.debug_features_off;

-- Result batches at an offset: bitmaps, strings and values read from
-- there.  Each query runs with datafusion.mode off, then on.
SET datafusion.debug_result_offset = 3;
EXPLAIN (COSTS OFF) SELECT * FROM df_en WHERE a % 397 = 1 OR a IS NULL;
SET datafusion.mode = off;
SELECT * FROM df_en WHERE a % 397 = 1 OR a IS NULL ORDER BY a;
SELECT b, d, count(*), sum(c), avg(f), min(e), max(g) FROM df_en GROUP BY b, d ORDER BY b, d;
SET datafusion.mode = on;
SELECT * FROM df_en WHERE a % 397 = 1 OR a IS NULL ORDER BY a;
SELECT b, d, count(*), sum(c), avg(f), min(e), max(g) FROM df_en GROUP BY b, d ORDER BY b, d;
RESET datafusion.debug_result_offset;

DROP TABLE df_en;
DROP TABLE df_en2;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
