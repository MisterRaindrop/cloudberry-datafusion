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
-- Subtrees of a slice below PostgreSQL's nodes (T2).  When a slice as a
-- whole cannot run, DataFusion runs its highest subtrees that can, in
-- children their parents run once, and PostgreSQL's nodes above pull their
-- rows.  Each query runs with datafusion.mode off, then on; the two
-- results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;

CREATE TABLE df_pt (id int, k int, v int8, t text) DISTRIBUTED BY (id);
INSERT INTO df_pt SELECT i, i % 13, i * 7, 'k' || (i % 31) FROM generate_series(1, 30000) i;
CREATE TABLE df_pt2 (id int, w int) DISTRIBUTED BY (id);
INSERT INTO df_pt2 SELECT i, i % 5 FROM generate_series(1, 40) i;
ANALYZE df_pt;
ANALYZE df_pt2;

SET datafusion.mode = explain;
-- a Limit WITH TIES stays, the Sort and aggregate below run
EXPLAIN (COSTS OFF) SELECT k, count(*) FROM df_pt GROUP BY k ORDER BY 2 DESC FETCH FIRST 2 ROWS WITH TIES;
-- each child of an Append
EXPLAIN (COSTS OFF) SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k
UNION ALL SELECT w, count(*) FROM df_pt2 GROUP BY w;
-- Below a Nested Loop only its outer side may run, never the inner side,
-- which it rescans per row.  With sorts the GroupAggregates read their
-- input's order, which DataFusion running the outer aggregate hashed would
-- not keep; without, the outer side only receives rows: nothing runs there.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS OFF) SELECT a.k, count(*) FROM (SELECT k, count(*) AS c FROM df_pt GROUP BY k) a
JOIN df_pt2 b ON a.c > b.w * 500 GROUP BY a.k;
SET enable_sort = off;
EXPLAIN (COSTS OFF) SELECT a.k, count(*) FROM (SELECT k, count(*) AS c FROM df_pt GROUP BY k) a
JOIN df_pt2 b ON a.c > b.w * 500 GROUP BY a.k;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_sort;
-- ORCA's Result on top
SET optimizer = on;
EXPLAIN (COSTS OFF) SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k HAVING count(*) > 10 ORDER BY k LIMIT 5;
SET optimizer = off;

SET datafusion.mode = off;
SELECT k, count(*) FROM df_pt GROUP BY k ORDER BY 2 DESC, 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k UNION ALL SELECT w, count(*) FROM df_pt2 GROUP BY w ORDER BY 1, 2;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_sort = off;
SELECT a.k, count(*) FROM (SELECT k, count(*) AS c FROM df_pt GROUP BY k) a
JOIN df_pt2 b ON a.c > b.w * 500 GROUP BY a.k ORDER BY 1;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_sort;
SET optimizer = on;
SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k HAVING count(*) > 10 ORDER BY k LIMIT 5;
SET optimizer = off;
SET datafusion.mode = on;
SELECT k, count(*) FROM df_pt GROUP BY k ORDER BY 2 DESC, 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k UNION ALL SELECT w, count(*) FROM df_pt2 GROUP BY w ORDER BY 1, 2;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_sort = off;
SELECT a.k, count(*) FROM (SELECT k, count(*) AS c FROM df_pt GROUP BY k) a
JOIN df_pt2 b ON a.c > b.w * 500 GROUP BY a.k ORDER BY 1;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_sort;
SET optimizer = on;
SELECT k, count(*) FROM df_pt WHERE v > 100 GROUP BY k HAVING count(*) > 10 ORDER BY k LIMIT 5;
SET optimizer = off;

DROP TABLE df_pt, df_pt2;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
