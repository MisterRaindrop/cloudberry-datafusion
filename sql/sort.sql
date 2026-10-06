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
-- Sort and Limit at the top of a slice (S1): ORDER BY, LIMIT and OFFSET
-- with constants.  A slice sending through a sorted Gather Motion sorts in
-- DataFusion (with a Limit: the top rows only) and PostgreSQL's receiver
-- merges the streams.  Keys sort as PostgreSQL's default btree order:
-- NULLS FIRST/LAST, floats with -0 = 0 and NaN above all, numeric NaN
-- above all, dates and timestamps with infinities, strings under C.  Each
-- query runs with datafusion.mode off, then on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;

CREATE TABLE df_so (id int, a int, b int8, f float8, g float4, t text, n numeric(12,3),
  d date, ts timestamp, z bool) DISTRIBUTED BY (id);
INSERT INTO df_so SELECT i,
  CASE WHEN i % 17 = 0 THEN NULL ELSE (i * 7919) % 1000 END,
  (i * 104729::int8) % 100003 - 50000,
  CASE i % 11 WHEN 0 THEN '-0'::float8 WHEN 1 THEN 0 WHEN 2 THEN 'NaN' WHEN 3 THEN '-NaN'
    WHEN 4 THEN 'Infinity' WHEN 5 THEN '-Infinity' WHEN 6 THEN NULL ELSE (i % 997) / 7.0 END,
  CASE WHEN i % 13 = 0 THEN NULL ELSE ((i * 31) % 501) / 3.0 END,
  CASE WHEN i % 19 = 0 THEN NULL ELSE 'k' || ((i * 37) % 3001) END,
  CASE WHEN i % 29 = 0 THEN NULL WHEN i % 31 = 0 THEN 'NaN' ELSE ((i * 13) % 20011) / 7.0 END,
  CASE WHEN i % 37 = 0 THEN NULL WHEN i % 41 = 0 THEN 'infinity' WHEN i % 43 = 0 THEN '-infinity'
    ELSE date '2000-01-01' + (i * 3) % 9000 END,
  timestamp '2020-01-01' + ((i * 7) % 100000) * interval '1 minute',
  CASE WHEN i % 7 = 0 THEN NULL ELSE i % 3 = 0 END
FROM generate_series(1, 20000) i;
ANALYZE df_so;

SET datafusion.mode = explain;
-- top rows on each segment, merged by the coordinator
EXPLAIN (COSTS OFF) SELECT id, a FROM df_so ORDER BY a, id LIMIT 5;
-- every row sorted on each segment
EXPLAIN (COSTS OFF) SELECT id, b FROM df_so WHERE id % 500 = 0 ORDER BY b, id;
-- the top groups of a two-stage aggregation
EXPLAIN (COSTS OFF) SELECT a, count(*), sum(b) FROM df_so GROUP BY a ORDER BY 3 DESC, 1 LIMIT 5;
-- a single slice
EXPLAIN (COSTS OFF) SELECT relnatts, count(*) FROM pg_class GROUP BY relnatts ORDER BY 2 DESC, 1 LIMIT 3;
-- these stay on PostgreSQL, with the reason
EXPLAIN (COSTS OFF) SELECT id FROM df_so ORDER BY a FETCH FIRST 3 ROWS WITH TIES;
EXPLAIN (COSTS OFF) SELECT id FROM df_so ORDER BY t USING ~<~ LIMIT 3;
EXPLAIN (COSTS OFF) SELECT a, avg(b) FROM df_so GROUP BY a ORDER BY 2 LIMIT 3;
EXPLAIN (COSTS OFF) SELECT count(*) FROM (SELECT a FROM df_so ORDER BY a LIMIT 5) s;

-- a LIMIT from a parameter (a generic plan) stays on PostgreSQL
PREPARE df_top(int8) AS SELECT id FROM df_so ORDER BY id DESC LIMIT $1;
SET plan_cache_mode = force_generic_plan;

SET datafusion.mode = off;
EXECUTE df_top(3);
SELECT id, a FROM df_so ORDER BY a, id LIMIT 5;
SELECT id, a FROM df_so ORDER BY a DESC NULLS LAST, id DESC LIMIT 5 OFFSET 3;
SELECT id, a FROM df_so ORDER BY a NULLS FIRST, id LIMIT 3;
SELECT id, b FROM df_so WHERE id % 500 = 0 ORDER BY b, id;
SELECT id, f FROM df_so WHERE id < 40 ORDER BY f, id;
SELECT id, f FROM df_so ORDER BY f DESC NULLS LAST, id LIMIT 6;
SELECT id, g FROM df_so ORDER BY g DESC, id LIMIT 4;
SELECT id, t FROM df_so ORDER BY t COLLATE "C" DESC, id LIMIT 4;
SELECT id, n FROM df_so ORDER BY n DESC NULLS LAST, id LIMIT 4;
SELECT id, n FROM df_so ORDER BY n NULLS FIRST, id LIMIT 4;
SELECT id, d FROM df_so ORDER BY d, id LIMIT 4;
SELECT id, d FROM df_so ORDER BY d DESC, id LIMIT 4;
SELECT id, ts FROM df_so ORDER BY ts DESC, id LIMIT 3 OFFSET 100;
SELECT id, z FROM df_so ORDER BY z DESC NULLS LAST, id LIMIT 3;
SELECT id, a + b AS s FROM df_so ORDER BY a + b, id LIMIT 3;
SELECT id FROM df_so ORDER BY a * 2 DESC, b, id LIMIT 3;
SELECT a, count(*), sum(b) FROM df_so GROUP BY a ORDER BY 3 DESC, 1 LIMIT 5;
SELECT f, count(*) FROM df_so WHERE f IS NULL OR f <> 0 GROUP BY f ORDER BY f NULLS FIRST LIMIT 4;
SELECT relnatts, count(*) > 0 AS any FROM pg_class GROUP BY relnatts ORDER BY 1 LIMIT 3;
SELECT x.id, y.id FROM df_so x JOIN df_so y ON x.id = y.a ORDER BY x.id, y.id LIMIT 5;
SELECT count(*) FROM (SELECT id FROM df_so LIMIT 10) s;
SELECT id FROM df_so ORDER BY id LIMIT 0;
SELECT id FROM df_so ORDER BY b, id LIMIT ALL OFFSET 19998;
SELECT id FROM df_so ORDER BY b, id LIMIT NULL OFFSET 19998;
SELECT id FROM df_so ORDER BY b, id OFFSET 30000;
SET datafusion.mode = on;
EXECUTE df_top(3);
SELECT id, a FROM df_so ORDER BY a, id LIMIT 5;
SELECT id, a FROM df_so ORDER BY a DESC NULLS LAST, id DESC LIMIT 5 OFFSET 3;
SELECT id, a FROM df_so ORDER BY a NULLS FIRST, id LIMIT 3;
SELECT id, b FROM df_so WHERE id % 500 = 0 ORDER BY b, id;
SELECT id, f FROM df_so WHERE id < 40 ORDER BY f, id;
SELECT id, f FROM df_so ORDER BY f DESC NULLS LAST, id LIMIT 6;
SELECT id, g FROM df_so ORDER BY g DESC, id LIMIT 4;
SELECT id, t FROM df_so ORDER BY t COLLATE "C" DESC, id LIMIT 4;
SELECT id, n FROM df_so ORDER BY n DESC NULLS LAST, id LIMIT 4;
SELECT id, n FROM df_so ORDER BY n NULLS FIRST, id LIMIT 4;
SELECT id, d FROM df_so ORDER BY d, id LIMIT 4;
SELECT id, d FROM df_so ORDER BY d DESC, id LIMIT 4;
SELECT id, ts FROM df_so ORDER BY ts DESC, id LIMIT 3 OFFSET 100;
SELECT id, z FROM df_so ORDER BY z DESC NULLS LAST, id LIMIT 3;
SELECT id, a + b AS s FROM df_so ORDER BY a + b, id LIMIT 3;
SELECT id FROM df_so ORDER BY a * 2 DESC, b, id LIMIT 3;
SELECT a, count(*), sum(b) FROM df_so GROUP BY a ORDER BY 3 DESC, 1 LIMIT 5;
SELECT f, count(*) FROM df_so WHERE f IS NULL OR f <> 0 GROUP BY f ORDER BY f NULLS FIRST LIMIT 4;
SELECT relnatts, count(*) > 0 AS any FROM pg_class GROUP BY relnatts ORDER BY 1 LIMIT 3;
SELECT x.id, y.id FROM df_so x JOIN df_so y ON x.id = y.a ORDER BY x.id, y.id LIMIT 5;
SELECT count(*) FROM (SELECT id FROM df_so LIMIT 10) s;
SELECT id FROM df_so ORDER BY id LIMIT 0;
SELECT id FROM df_so ORDER BY b, id LIMIT ALL OFFSET 19998;
SELECT id FROM df_so ORDER BY b, id LIMIT NULL OFFSET 19998;
SELECT id FROM df_so ORDER BY b, id OFFSET 30000;
RESET plan_cache_mode;
DEALLOCATE df_top;

DROP TABLE df_so;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
