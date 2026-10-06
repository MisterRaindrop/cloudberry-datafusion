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
-- numeric results are not computed on further yet
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

DROP TABLE df_nm, df_nm_empty;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
