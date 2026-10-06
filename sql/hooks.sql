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
-- Executor and EXPLAIN hooks (M2): datafusion.mode, slice eligibility and
-- routing.  Coordinator-only catalog tables give single-slice plans with
-- no Motion, the only shape that qualifies before Motion support.
--
CREATE EXTENSION datafusion_executor;
SET optimizer = off;

-- off (the default): EXPLAIN is unchanged.
SHOW datafusion.mode;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pg_class WHERE relpages > 0;

-- explain: one line per slice.
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pg_class WHERE relpages > 0;
EXPLAIN (COSTS OFF) SELECT relnatts, sum(relpages), max(reltuples)
FROM pg_class WHERE relhasindex AND relpages >= 0 GROUP BY relnatts;

-- Each of these stays on the PostgreSQL executor, with the reason.
EXPLAIN (COSTS OFF) SELECT relname FROM pg_class;
EXPLAIN (COSTS OFF) SELECT avg(relpages) FROM pg_class;
EXPLAIN (COSTS OFF) SELECT count(DISTINCT relnatts) FROM pg_class;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pg_class WHERE relpages > 0 ORDER BY 1 FETCH FIRST 1 ROW WITH TIES;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pg_class WHERE abs(relpages) > 0;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pg_class WHERE relnatts IN (1, 2);
EXPLAIN (COSTS OFF) SELECT count(*) FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid;

-- Distributed tables: the coordinator's slice receives from a Motion and
-- stays on PostgreSQL; a segment slice below its sending Motion can qualify.
CREATE TABLE df_hooks_t (a int, b int8, c float8) DISTRIBUTED BY (a);
EXPLAIN (COSTS OFF) SELECT a, b FROM df_hooks_t WHERE c > 0;
EXPLAIN (COSTS OFF) SELECT sum(b) FROM df_hooks_t;

-- Structured formats are left untouched.
EXPLAIN (COSTS OFF, FORMAT JSON) SELECT count(*) FROM pg_class WHERE relpages > 0;

-- explain mode never routes anything.
SELECT count(*) > 0 AS ok FROM pg_class WHERE relpages >= 0;
SELECT datafusion_debug_takeovers();

-- on: eligible slices are routed (still executed by PostgreSQL until M3),
-- and give the same results.
SET datafusion.mode = on;
SELECT count(*) > 0 AS ok FROM pg_class WHERE relpages >= 0;
SELECT datafusion_debug_takeovers();
SELECT count(*) > 0 AS ok FROM pg_class WHERE relpages >= 0 ORDER BY 1 FETCH FIRST 1 ROW WITH TIES;
SELECT datafusion_debug_takeovers();
INSERT INTO df_hooks_t SELECT i, i, i FROM generate_series(1, 100) i;
SELECT sum(b), count(*) FROM df_hooks_t WHERE c > 50;
SELECT datafusion_debug_takeovers();
EXPLAIN (COSTS OFF) SELECT count(*) FROM pg_class WHERE relpages > 0;
SELECT datafusion_debug_takeovers();

-- An error while a routed slice runs drops its record with the query; the
-- next query is routed normally.
SELECT count(*) FROM pg_class WHERE relpages / 0 > 0;
SELECT count(*) > 0 AS ok FROM pg_class WHERE relpages >= 0;
SELECT datafusion_debug_takeovers();

-- A cursor runs its slice once per FETCH.
BEGIN;
DECLARE df_cur CURSOR FOR
  SELECT relnatts, count(*) FROM pg_class WHERE relpages >= 0 GROUP BY relnatts;
FETCH 1 FROM df_cur \gset
FETCH 1 FROM df_cur \gset
CLOSE df_cur;
COMMIT;
SELECT datafusion_debug_takeovers();

-- Not tested here: an updatable cursor (e.g. a plain SELECT over pg_class)
-- carries the system columns ctid and gp_segment_id for WHERE CURRENT OF and
-- so stays on PostgreSQL.  Whether Cloudberry treats the cursor as updatable
-- currently varies from run to run for the same statement, which would make
-- the takeover count flaky.

DROP TABLE df_hooks_t;
RESET datafusion.mode;
DROP EXTENSION datafusion_executor;
