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
-- and infinities, and comparisons across types stay on PostgreSQL, but for
-- a date against a timestamp constant (DT1), such as the folded
-- "d < date '1994-01-01' + interval '1' year", which compares dates.  Each
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

-- A date against a timestamp constant compares dates (DT1): the constant
-- rounded up for < and >=, down for <= and >; = and <> on a constant
-- within a day are constant.  Dates past the timestamp range sort after
-- every finite timestamp, before infinity.
CREATE TABLE df_ty3 (id int4, d date) DISTRIBUTED BY (id);
INSERT INTO df_ty3 VALUES (1, '-infinity'), (2, 'infinity'), (3, '4713-01-01 BC'),
  (4, '0044-03-15 BC'), (5, '1993-12-31'), (6, '1994-01-01'), (7, '1994-01-02'),
  (8, '294276-12-31'), (9, '294277-01-01'), (10, '5874897-12-31'), (11, NULL);
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ty WHERE d < date '1994-01-01' + interval '1' year;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ty WHERE d < ts;
EXPLAIN (COSTS OFF) SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d <> timestamp '1994-01-02 01:00';
SET datafusion.mode = off;
SELECT count(*) FROM df_ty WHERE d >= date '2001-01-01' AND d < date '2001-01-01' + interval '3' month;
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d < timestamp '1994-01-01 00:00:00.000001';
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d <= timestamp '1993-12-31 23:59:59';
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE timestamp '1994-01-01 12:00' < d;
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE timestamp '0044-03-15 12:00 BC' >= d;
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d = timestamp '1994-01-01' OR d = timestamp '1994-01-02 01:00';
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d <> timestamp '1994-01-02 01:00';
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d > timestamp '294276-12-31 23:59:59.999999';
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d < timestamp 'infinity' AND d >= timestamp '-infinity';
SET datafusion.mode = on;
SELECT count(*) FROM df_ty WHERE d >= date '2001-01-01' AND d < date '2001-01-01' + interval '3' month;
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d < timestamp '1994-01-01 00:00:00.000001';
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d <= timestamp '1993-12-31 23:59:59';
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE timestamp '1994-01-01 12:00' < d;
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE timestamp '0044-03-15 12:00 BC' >= d;
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d = timestamp '1994-01-01' OR d = timestamp '1994-01-02 01:00';
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d <> timestamp '1994-01-02 01:00';
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d > timestamp '294276-12-31 23:59:59.999999';
SELECT count(*), sum(id), sum(id * id) FROM df_ty3 WHERE d < timestamp 'infinity' AND d >= timestamp '-infinity';
DROP TABLE df_ty3;

-- extract(year | quarter | month | day from date) as numeric of scale 0
-- (DT2).  The year of an infinite date is ±Infinity, which DataFusion
-- groups, sorts, compares at scale 0 and returns, but arithmetic, sums,
-- casts and rescaling comparisons on it stay on PostgreSQL.
CREATE TABLE df_ty4 (id int4, d date) DISTRIBUTED BY (id);
INSERT INTO df_ty4 SELECT i, date '1992-01-01' + (i * 7919) % 2600 FROM generate_series(1, 3000) i;
INSERT INTO df_ty4 VALUES (-1, '-infinity'), (-2, 'infinity'), (-3, '4714-11-24 BC'),
  (-4, '0001-12-31 BC'), (-5, '0001-01-01'), (-6, '5874897-12-31'), (-7, '2000-02-29'),
  (-8, NULL), (-9, 'infinity');
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT extract(year from d), count(*) FROM df_ty4 GROUP BY 1;
EXPLAIN (COSTS OFF) SELECT sum(extract(year from d)) FROM df_ty4;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_ty4 WHERE extract(year from d) > 1995.5;
SET datafusion.mode = off;
SELECT extract(year from d) AS y, count(*) FROM df_ty4 GROUP BY 1 ORDER BY 1;
SELECT extract(quarter from d) AS q, extract(month from d) AS m, count(*) FROM df_ty4 GROUP BY 1, 2 ORDER BY 1, 2;
SELECT id, extract(year from d), extract(month from d), extract(day from d) FROM df_ty4 WHERE id < 0 ORDER BY id;
SELECT min(extract(year from d)), max(extract(year from d)), count(*) FROM df_ty4;
SELECT count(*) FROM df_ty4 WHERE extract(year from d) BETWEEN 1995 AND 1997;
SET datafusion.mode = on;
SELECT extract(year from d) AS y, count(*) FROM df_ty4 GROUP BY 1 ORDER BY 1;
SELECT extract(quarter from d) AS q, extract(month from d) AS m, count(*) FROM df_ty4 GROUP BY 1, 2 ORDER BY 1, 2;
SELECT id, extract(year from d), extract(month from d), extract(day from d) FROM df_ty4 WHERE id < 0 ORDER BY id;
SELECT min(extract(year from d)), max(extract(year from d)), count(*) FROM df_ty4;
SELECT count(*) FROM df_ty4 WHERE extract(year from d) BETWEEN 1995 AND 1997;
DROP TABLE df_ty4;

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
