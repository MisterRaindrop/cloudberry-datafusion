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
-- Aggregates over DISTINCT arguments (D1, D2).  DataFusion runs the DISTINCT
-- aggregates of an Agg over a grouping by their argument, which spills as
-- any grouping does, then over its distinct values: count, sum and avg
-- DISTINCT and min and max of one argument.  Floats are told apart as
-- PostgreSQL does (-0 = 0, one NaN), numeric NaN is one value, strings need
-- a deterministic collation.  The planner's own form, an aggregate over a
-- grouping by the argument, runs as two aggregates (D2), and a
-- GroupAggregate whose order no one reads runs hashed, without its Sort.
-- Each query runs with datafusion.mode off, then on; the two results must
-- match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;

CREATE TABLE df_di (id int, a int, b int8, f float8, n numeric(10,2), t text) DISTRIBUTED BY (id);
INSERT INTO df_di SELECT i,
  CASE WHEN i % 13 = 0 THEN NULL ELSE i % 97 END,
  (i * 7919) % 1009,
  CASE i % 7 WHEN 0 THEN '-0'::float8 WHEN 1 THEN 0 WHEN 2 THEN 'NaN' WHEN 3 THEN '-NaN' WHEN 4 THEN NULL
    ELSE i % 50 END,
  CASE WHEN i % 11 = 0 THEN 'NaN' WHEN i % 17 = 0 THEN NULL ELSE (i % 300) / 4.0 END,
  CASE WHEN i % 19 = 0 THEN NULL ELSE 'k' || (i % 211) END
FROM generate_series(1, 30000) i;
ANALYZE df_di;

SET datafusion.mode = explain;
-- one argument, split at its distribution (batches carry the partial counts)
EXPLAIN (COSTS OFF) SELECT count(DISTINCT a), sum(DISTINCT a), avg(DISTINCT a), min(a), max(a) FROM df_di;
-- distinct by id, the distribution key: each segment counts its own
EXPLAIN (COSTS OFF) SELECT count(DISTINCT id), sum(DISTINCT id) FROM df_di;
-- an aggregate over a grouping (D2)
EXPLAIN (COSTS OFF) SELECT count(DISTINCT b), sum(DISTINCT b) FROM df_di;
EXPLAIN (COSTS OFF) SELECT a % 5 AS k, count(DISTINCT b) FROM df_di GROUP BY 1;
-- a GroupAggregate below an unsorted Gather (D2)
EXPLAIN (COSTS OFF) SELECT a, count(DISTINCT b), sum(DISTINCT b) FROM df_di GROUP BY a;
-- these stay on PostgreSQL, with the reason
EXPLAIN (COSTS OFF) SELECT count(DISTINCT a), count(*) FROM df_di;
EXPLAIN (COSTS OFF) SELECT count(DISTINCT a), count(DISTINCT b) FROM df_di WHERE id < 0;
EXPLAIN (COSTS OFF) SELECT sum(DISTINCT f) FROM df_di WHERE id < 0;
EXPLAIN (COSTS OFF) SELECT count(DISTINCT f), max(f) FROM df_di WHERE id < 0;
EXPLAIN (COSTS OFF) SELECT count(a ORDER BY a) FROM df_di;
-- ORDER BY: a Sort over the two aggregates (S1, D2)
EXPLAIN (COSTS OFF) SELECT a, count(DISTINCT b) FROM df_di GROUP BY a ORDER BY a;

SET datafusion.mode = off;
SELECT count(DISTINCT a), sum(DISTINCT a), avg(DISTINCT a), min(a), max(a) FROM df_di;
SELECT count(DISTINCT id), sum(DISTINCT id), avg(DISTINCT id) FROM df_di;
SELECT count(DISTINCT b), sum(DISTINCT b) FROM df_di WHERE id % 4 = 1;
SELECT count(DISTINCT n), sum(DISTINCT n), avg(DISTINCT n), min(n), max(DISTINCT n) FROM df_di;
SELECT count(DISTINCT n), sum(DISTINCT n), avg(DISTINCT n) FROM df_di WHERE n <> 'NaN';
SELECT count(DISTINCT f) FROM df_di;
SELECT count(DISTINCT t COLLATE "C"), min(t COLLATE "C"), max(t COLLATE "C") FROM df_di;
SELECT count(DISTINCT a), sum(DISTINCT a) FROM df_di WHERE id < 0;
SELECT count(DISTINCT b), sum(DISTINCT b) FROM df_di;
SELECT a % 5 AS k, count(DISTINCT b), avg(DISTINCT b) FROM df_di GROUP BY 1 ORDER BY 1;
SELECT a, count(DISTINCT b), sum(DISTINCT b) FROM df_di WHERE a < 6 GROUP BY a ORDER BY a;
SELECT count(DISTINCT a % 7) FROM df_di WHERE b > 500;
SELECT count(*) FROM (SELECT a, count(DISTINCT n) AS c FROM df_di GROUP BY a) s WHERE c > 70;
SET datafusion.mode = on;
SELECT count(DISTINCT a), sum(DISTINCT a), avg(DISTINCT a), min(a), max(a) FROM df_di;
SELECT count(DISTINCT id), sum(DISTINCT id), avg(DISTINCT id) FROM df_di;
SELECT count(DISTINCT b), sum(DISTINCT b) FROM df_di WHERE id % 4 = 1;
SELECT count(DISTINCT n), sum(DISTINCT n), avg(DISTINCT n), min(n), max(DISTINCT n) FROM df_di;
SELECT count(DISTINCT n), sum(DISTINCT n), avg(DISTINCT n) FROM df_di WHERE n <> 'NaN';
SELECT count(DISTINCT f) FROM df_di;
SELECT count(DISTINCT t COLLATE "C"), min(t COLLATE "C"), max(t COLLATE "C") FROM df_di;
SELECT count(DISTINCT a), sum(DISTINCT a) FROM df_di WHERE id < 0;
SELECT count(DISTINCT b), sum(DISTINCT b) FROM df_di;
SELECT a % 5 AS k, count(DISTINCT b), avg(DISTINCT b) FROM df_di GROUP BY 1 ORDER BY 1;
SELECT a, count(DISTINCT b), sum(DISTINCT b) FROM df_di WHERE a < 6 GROUP BY a ORDER BY a;
SELECT count(DISTINCT a % 7) FROM df_di WHERE b > 500;
SELECT count(*) FROM (SELECT a, count(DISTINCT n) AS c FROM df_di GROUP BY a) s WHERE c > 70;

DROP TABLE df_di;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
