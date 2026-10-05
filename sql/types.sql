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
-- Date and time types: date, time, timestamp and timestamptz travel as
-- PostgreSQL stores them (days or microseconds from 2000-01-01, time from
-- midnight), infinities included.  Comparing two values of one type and
-- min/max are integer comparisons; arithmetic, which checks for overflow
-- and infinities, and comparisons across types stay on PostgreSQL.  Each
-- query runs with datafusion.mode off, then on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
SET enable_nestloop = off;
SET enable_mergejoin = off;
SET TimeZone = 'UTC';

CREATE TABLE df_ty (id int4, d date, t time, ts timestamp, tz timestamptz) DISTRIBUTED BY (id);
INSERT INTO df_ty SELECT i, date '2000-01-01' + (i % 4000) - 2000,
  time '00:00' + (i % 86400) * interval '1 second',
  timestamp '1999-12-31 12:00' + i * interval '37 minutes',
  timestamptz '2024-03-10 01:00 America/New_York' + i * interval '13 minutes'
FROM generate_series(1, 100000) i;
INSERT INTO df_ty VALUES
  (-1, 'infinity', '24:00', 'infinity', 'infinity'),
  (-2, '-infinity', '00:00', '-infinity', '-infinity'),
  (-3, NULL, NULL, NULL, NULL),
  (-4, '4713-01-01 BC', '23:59:59.999999', '294276-12-31 23:59:59.999999', '4714-11-24 00:00:00+00 BC');
CREATE TABLE df_ty2 AS SELECT id * 2 AS id, d, t, ts, tz FROM df_ty
DISTRIBUTED BY (id);
ANALYZE df_ty;
ANALYZE df_ty2;

SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT min(d), max(ts) FROM df_ty WHERE tz > '2024-05-01 00:00+08';
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ty WHERE d - 1 > '2001-01-01';
EXPLAIN (COSTS OFF) SELECT max(d - date '2000-01-01') FROM df_ty;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ty WHERE ts > '2010-01-01'::date;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ty WHERE tz > ts;

SET datafusion.mode = off;
SELECT min(d), max(d), min(t), max(t), min(ts), max(ts), min(tz), max(tz), count(d) FROM df_ty;
SELECT count(*) FROM df_ty WHERE d >= '2001-01-01' AND d < '2002-01-01';
SELECT count(*) FROM df_ty WHERE ts > '2005-06-01 10:00' AND tz <= '2024-05-01 00:00+08' AND t <> '12:00';
SELECT id, d, t, ts, tz FROM df_ty
WHERE d = 'infinity' OR ts = '-infinity' OR d IS NULL OR d < '0001-01-01' ORDER BY id;
SELECT d, count(*), min(ts), max(tz) FROM df_ty WHERE id % 1000 = 0 GROUP BY d ORDER BY d;
SET datafusion.mode = on;
SELECT min(d), max(d), min(t), max(t), min(ts), max(ts), min(tz), max(tz), count(d) FROM df_ty;
SELECT count(*) FROM df_ty WHERE d >= '2001-01-01' AND d < '2002-01-01';
SELECT count(*) FROM df_ty WHERE ts > '2005-06-01 10:00' AND tz <= '2024-05-01 00:00+08' AND t <> '12:00';
SELECT id, d, t, ts, tz FROM df_ty
WHERE d = 'infinity' OR ts = '-infinity' OR d IS NULL OR d < '0001-01-01' ORDER BY id;
SELECT d, count(*), min(ts), max(tz) FROM df_ty WHERE id % 1000 = 0 GROUP BY d ORDER BY d;
-- Arithmetic stays on PostgreSQL, and fails there as it should.
SELECT max(d - date '2000-01-01') FROM df_ty WHERE id >= -1;

-- Redistributed by date and time keys (cdbhash: hashint4 for date,
-- time_hash and timestamp_hash for the others), as batches.
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ty a JOIN df_ty2 b ON a.ts = b.ts;
EXPLAIN (COSTS OFF) SELECT d, count(*) FROM df_ty GROUP BY d;
SET datafusion.mode = off;
SELECT count(*), count(DISTINCT n) FROM (SELECT d, count(*) AS n FROM df_ty GROUP BY d) s;
SELECT count(*), count(DISTINCT n) FROM (SELECT tz, count(*) AS n FROM df_ty GROUP BY tz) s;
SELECT count(*), sum(n) FROM (SELECT t, count(*) AS n FROM df_ty GROUP BY t HAVING count(*) > 1) s;
SELECT count(*), min(a.ts) FROM df_ty a JOIN df_ty2 b ON a.ts = b.ts;
SELECT count(*), count(b.id) FROM df_ty a LEFT JOIN df_ty2 b ON a.d = b.d AND b.id % 30 = 0;
SELECT count(*) FROM df_ty a WHERE EXISTS (SELECT 1 FROM df_ty2 b WHERE b.t = a.t AND b.id < 2000);
SET datafusion.mode = on;
SELECT count(*), count(DISTINCT n) FROM (SELECT d, count(*) AS n FROM df_ty GROUP BY d) s;
SELECT count(*), count(DISTINCT n) FROM (SELECT tz, count(*) AS n FROM df_ty GROUP BY tz) s;
SELECT count(*), sum(n) FROM (SELECT t, count(*) AS n FROM df_ty GROUP BY t HAVING count(*) > 1) s;
SELECT count(*), min(a.ts) FROM df_ty a JOIN df_ty2 b ON a.ts = b.ts;
SELECT count(*), count(b.id) FROM df_ty a LEFT JOIN df_ty2 b ON a.d = b.d AND b.id % 30 = 0;
SELECT count(*) FROM df_ty a WHERE EXISTS (SELECT 1 FROM df_ty2 b WHERE b.t = a.t AND b.id < 2000);

DROP TABLE df_ty, df_ty2;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
