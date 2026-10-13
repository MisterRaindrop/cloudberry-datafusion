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
--
-- Set operations.  UA1: UNION ALL (Append), as DataFusion's union of its
-- inputs, which keeps no order between them: an Append whose order a node
-- above reads stays on PostgreSQL.  Numeric of different scales, or of
-- display scales of their own, carries each value's display scale beside
-- it.  Inputs that are Motions (GPORCA redistributes each branch below
-- the Append) are read from their streams.  Each query runs with
-- datafusion.mode off, then on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;

CREATE TABLE df_u1 (k int, g int, a numeric(7,2), b int8, t text) DISTRIBUTED BY (k);
CREATE TABLE df_u2 (k int, g int, a numeric(12,4), b int4, t varchar(10)) DISTRIBUTED BY (k);
INSERT INTO df_u1 SELECT i, i % 7, CASE WHEN i % 97 = 0 THEN 'NaN' ELSE (i % 300) * 1.25 END, i::int8 * 1000000000, CASE WHEN i % 11 = 0 THEN NULL ELSE 't' || (i % 5) END FROM generate_series(1, 3000) i;
INSERT INTO df_u2 SELECT i, i % 5, (i % 400) / 7.0, i, CASE WHEN i % 13 = 0 THEN NULL ELSE 'v' || (i % 3) END FROM generate_series(1, 2000) i;
CREATE TABLE df_up (k int, g int, a numeric(7,2)) DISTRIBUTED BY (k) PARTITION BY RANGE (g) (START (0) END (8) EVERY (4));
INSERT INTO df_up SELECT i, i % 8, i * 0.5 FROM generate_series(1, 1000) i;
ANALYZE df_u1;
ANALYZE df_u2;
ANALYZE df_up;

SET optimizer = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT u.g, count(*), sum(u.a) FROM (SELECT g, a FROM df_u1 UNION ALL SELECT g, a FROM df_u2) u GROUP BY u.g ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT u.k, u.t, x.g FROM (SELECT k, t FROM df_u1 WHERE k < 50 UNION ALL SELECT k, t FROM df_u2 WHERE k < 50) u JOIN df_u1 x ON x.k = u.k + 1 ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT g, sum(q), count(q) FROM (SELECT g, a / 3 q FROM df_u1 WHERE a <> 'NaN' UNION ALL SELECT g, a FROM df_u2 UNION ALL SELECT g, k * 1.5 FROM df_u1) u GROUP BY g ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT k, a FROM df_u1 UNION ALL SELECT k, a FROM df_u2) u LIMIT 5;
SET datafusion.mode = off;
SELECT u.g, count(*), sum(u.a), sum(u.b), count(u.t) FROM (SELECT g, a, b, t FROM df_u1 UNION ALL SELECT g, a, b, t FROM df_u2) u GROUP BY u.g ORDER BY 1;
SELECT u.k, u.t, u.a, x.g FROM (SELECT k, t, a FROM df_u1 WHERE k < 50 UNION ALL SELECT k, t, a FROM df_u2 WHERE k < 50) u JOIN df_u1 x ON x.k = u.k + 1 ORDER BY 1, 2, 3;
SELECT g, sum(q), count(q) FROM (SELECT g, a / 3 q FROM df_u1 WHERE a <> 'NaN' UNION ALL SELECT g, a FROM df_u2 UNION ALL SELECT g, k * 1.5 FROM df_u1) u GROUP BY g ORDER BY 1;
SELECT k, a FROM (SELECT k, a FROM df_u1 WHERE k % 400 = 1 UNION ALL SELECT k, a FROM df_u2 WHERE k % 300 = 2) u ORDER BY a, k;
SELECT g, count(*), sum(a) FROM df_up WHERE k > 10 GROUP BY g ORDER BY 1;
SELECT u.g, count(*), sum(u.a) FROM (SELECT x.g, x.a FROM df_u1 x JOIN df_u2 y ON x.g = y.k UNION ALL SELECT y.g, y.a FROM df_u2 y JOIN df_u1 x ON y.g = x.k) u GROUP BY u.g ORDER BY 1;
SELECT u.t, count(*) FROM (SELECT t FROM df_u1 UNION ALL SELECT t FROM df_u2) u GROUP BY u.t ORDER BY 1;
SET datafusion.mode = on;
SELECT u.g, count(*), sum(u.a), sum(u.b), count(u.t) FROM (SELECT g, a, b, t FROM df_u1 UNION ALL SELECT g, a, b, t FROM df_u2) u GROUP BY u.g ORDER BY 1;
SELECT u.k, u.t, u.a, x.g FROM (SELECT k, t, a FROM df_u1 WHERE k < 50 UNION ALL SELECT k, t, a FROM df_u2 WHERE k < 50) u JOIN df_u1 x ON x.k = u.k + 1 ORDER BY 1, 2, 3;
SELECT g, sum(q), count(q) FROM (SELECT g, a / 3 q FROM df_u1 WHERE a <> 'NaN' UNION ALL SELECT g, a FROM df_u2 UNION ALL SELECT g, k * 1.5 FROM df_u1) u GROUP BY g ORDER BY 1;
SELECT k, a FROM (SELECT k, a FROM df_u1 WHERE k % 400 = 1 UNION ALL SELECT k, a FROM df_u2 WHERE k % 300 = 2) u ORDER BY a, k;
SELECT g, count(*), sum(a) FROM df_up WHERE k > 10 GROUP BY g ORDER BY 1;
SELECT u.g, count(*), sum(u.a) FROM (SELECT x.g, x.a FROM df_u1 x JOIN df_u2 y ON x.g = y.k UNION ALL SELECT y.g, y.a FROM df_u2 y JOIN df_u1 x ON y.g = x.k) u GROUP BY u.g ORDER BY 1;
SELECT u.t, count(*) FROM (SELECT t FROM df_u1 UNION ALL SELECT t FROM df_u2) u GROUP BY u.t ORDER BY 1;

-- GPORCA's plans
SET optimizer = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT u.g, count(*), sum(u.a) FROM (SELECT x.g, x.a FROM df_u1 x JOIN df_u2 y ON x.g = y.k UNION ALL SELECT y.g, y.a FROM df_u2 y JOIN df_u1 x ON y.g = x.k) u GROUP BY u.g ORDER BY 1;
SET datafusion.mode = off;
SELECT u.g, count(*), sum(u.a), sum(u.b), count(u.t) FROM (SELECT g, a, b, t FROM df_u1 UNION ALL SELECT g, a, b, t FROM df_u2) u GROUP BY u.g ORDER BY 1;
SELECT u.k, u.t, u.a, x.g FROM (SELECT k, t, a FROM df_u1 WHERE k < 50 UNION ALL SELECT k, t, a FROM df_u2 WHERE k < 50) u JOIN df_u1 x ON x.k = u.k + 1 ORDER BY 1, 2, 3;
SELECT k, a FROM (SELECT k, a FROM df_u1 WHERE k % 400 = 1 UNION ALL SELECT k, a FROM df_u2 WHERE k % 300 = 2) u ORDER BY a, k;
SELECT g, count(*), sum(a) FROM df_up WHERE k > 10 GROUP BY g ORDER BY 1;
SELECT u.g, count(*), sum(u.a) FROM (SELECT x.g, x.a FROM df_u1 x JOIN df_u2 y ON x.g = y.k UNION ALL SELECT y.g, y.a FROM df_u2 y JOIN df_u1 x ON y.g = x.k) u GROUP BY u.g ORDER BY 1;
SELECT u.t, count(*) FROM (SELECT t FROM df_u1 UNION ALL SELECT t FROM df_u2) u GROUP BY u.t ORDER BY 1;
SET datafusion.mode = on;
SELECT u.g, count(*), sum(u.a), sum(u.b), count(u.t) FROM (SELECT g, a, b, t FROM df_u1 UNION ALL SELECT g, a, b, t FROM df_u2) u GROUP BY u.g ORDER BY 1;
SELECT u.k, u.t, u.a, x.g FROM (SELECT k, t, a FROM df_u1 WHERE k < 50 UNION ALL SELECT k, t, a FROM df_u2 WHERE k < 50) u JOIN df_u1 x ON x.k = u.k + 1 ORDER BY 1, 2, 3;
SELECT k, a FROM (SELECT k, a FROM df_u1 WHERE k % 400 = 1 UNION ALL SELECT k, a FROM df_u2 WHERE k % 300 = 2) u ORDER BY a, k;
SELECT g, count(*), sum(a) FROM df_up WHERE k > 10 GROUP BY g ORDER BY 1;
SELECT u.g, count(*), sum(u.a) FROM (SELECT x.g, x.a FROM df_u1 x JOIN df_u2 y ON x.g = y.k UNION ALL SELECT y.g, y.a FROM df_u2 y JOIN df_u1 x ON y.g = x.k) u GROUP BY u.g ORDER BY 1;
SELECT u.t, count(*) FROM (SELECT t FROM df_u1 UNION ALL SELECT t FROM df_u2) u GROUP BY u.t ORDER BY 1;
RESET datafusion.mode;

DROP TABLE df_u1, df_u2, df_up;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
