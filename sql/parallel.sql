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
-- Cloudberry's parallel mode (M6): several QEs per segment share a parallel
-- scan through the table AM, and each runs DataFusion on its part.  The
-- costs below force parallel plans on small tables.  Each query runs with
-- datafusion.mode off, then on; the results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 1;
SET enable_parallel = on;
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;

CREATE TABLE df_par_heap (a int4, b int8, c float8, e int2) USING heap
  WITH (parallel_workers = 2) DISTRIBUTED BY (a);
CREATE TABLE df_par_pax (a int4, b int8, c float8, e int2) USING pax
  WITH (parallel_workers = 2) DISTRIBUTED BY (a);
INSERT INTO df_par_heap SELECT i, i * 10, i / 4.0, (i % 100)::int2 FROM generate_series(1, 200000) i;
INSERT INTO df_par_heap VALUES (NULL, NULL, NULL, NULL);
INSERT INTO df_par_pax SELECT * FROM df_par_heap;
DELETE FROM df_par_heap WHERE a % 1000 = 7;
DELETE FROM df_par_pax WHERE a % 1000 = 7;
ANALYZE df_par_heap;
ANALYZE df_par_pax;

SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*), sum(a), max(c) FROM df_par_heap WHERE e < 50;
EXPLAIN (COSTS OFF) SELECT e, count(*), sum(a) FROM df_par_pax GROUP BY e;

SET datafusion.mode = off;
SELECT count(*), count(a), sum(a), max(c), sum(e) FROM df_par_heap WHERE e < 50;
SELECT count(*), count(a), sum(a), max(c), sum(e) FROM df_par_pax WHERE e < 50;
SELECT e, count(*), sum(a), max(c) FROM df_par_pax WHERE e >= 97 OR e IS NULL GROUP BY e;
SELECT a, b, c FROM df_par_heap WHERE a % 50000 = 0 OR a % 1000 = 7;
SET datafusion.mode = on;
SELECT count(*), count(a), sum(a), max(c), sum(e) FROM df_par_heap WHERE e < 50;
SELECT count(*), count(a), sum(a), max(c), sum(e) FROM df_par_pax WHERE e < 50;
SELECT e, count(*), sum(a), max(c) FROM df_par_pax WHERE e >= 97 OR e IS NULL GROUP BY e;
SELECT a, b, c FROM df_par_heap WHERE a % 50000 = 0 OR a % 1000 = 7;

-- In every parallel QE the scan runs in DataFusion's slice.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT count(*), sum(a) FROM df_par_pax WHERE e < 50;

DROP TABLE df_par_heap, df_par_pax;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
