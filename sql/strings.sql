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
-- text and varchar: UTF-8 bytes in Arrow string columns, compared byte by
-- byte.  Equality (filters, GROUP BY, join keys) agrees with PostgreSQL
-- under any deterministic collation; ordering comparisons and min/max only
-- under the C collation.  Whether the database's default collation is C is
-- each node's own, so a slice that depends on it decides per node and its
-- Motions carry tuples.  Each query runs with datafusion.mode off, then
-- on; the two results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
SET datafusion.motion_batches = on;
SET enable_nestloop = off;
SET enable_mergejoin = off;

CREATE COLLATION df_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
CREATE TABLE df_st (id int4, t text, v varchar(20), u text COLLATE "unicode", ci text COLLATE df_ci)
DISTRIBUTED BY (id);
INSERT INTO df_st SELECT i, 'k' || (i % 5000), 'v' || (i % 300), chr(65 + i % 58) || i % 7,
  CASE WHEN i % 2 = 0 THEN 'ABC' ELSE 'abc' END
FROM generate_series(1, 100000) i;
INSERT INTO df_st VALUES (-1, '', '', '', ''), (-2, NULL, NULL, NULL, NULL),
  (-3, '中文字符', 'ünïcödé', '😀', 'Ä'), (-4, E'quo"te\\back\nline', E'tab\there', 'b', 'B'),
  (-5, 'k1 ', 'v1 ', 'a', 'A');
CREATE TABLE df_st2 AS SELECT id, t::varchar(30) AS vt, v::text AS tv FROM df_st
WHERE id % 3 = 0 OR id < 0 DISTRIBUTED BY (id);
-- Values stored out of line in the TOAST table.
CREATE TABLE df_st_big (id int4, t text) DISTRIBUTED BY (id);
ALTER TABLE df_st_big ALTER COLUMN t SET STORAGE EXTERNAL;
INSERT INTO df_st_big SELECT i, repeat(md5(i::text), 300 + i % 7) FROM generate_series(1, 2000) i;
ANALYZE df_st;
ANALYZE df_st2;
ANALYZE df_st_big;

SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st WHERE t = 'k42' AND v <> 'v1';
EXPLAIN (COSTS OFF) SELECT min(t COLLATE "C"), max(v COLLATE "C") FROM df_st WHERE t COLLATE "C" > 'k4';
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st WHERE u < 'b';
EXPLAIN (COSTS OFF) SELECT min(u) FROM df_st;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st WHERE ci = 'abc';
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st WHERE t LIKE 'k1%';
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st WHERE t || 'x' = 'k1x';
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st a JOIN df_st2 b ON a.t = b.vt;
EXPLAIN (COSTS OFF) SELECT t FROM df_st_big WHERE id = 1500;

SET datafusion.mode = off;
SELECT count(*), count(t), count(v) FROM df_st WHERE t = 'k42' OR v = 'v7';
SELECT id, t, v FROM df_st WHERE id < 0 ORDER BY id;
SELECT id FROM df_st WHERE t = E'quo"te\\back\nline' OR t = '中文字符' OR t = '' OR v = E'tab\there'
ORDER BY id;
SELECT min(t COLLATE "C"), max(t COLLATE "C"), min(v COLLATE "C"), max(v COLLATE "C") FROM df_st;
SELECT count(*) FROM df_st WHERE t COLLATE "C" > 'k4' AND t COLLATE "C" <= 'k5';
SELECT count(*), min(t), max(v) FROM df_st WHERE t >= 'k2' AND v < 'v5';
SELECT count(*), count(DISTINCT n) FROM (SELECT t, count(*) AS n FROM df_st GROUP BY t) s;
SELECT v, count(*), max(id) FROM df_st
WHERE v = 'v1' OR v = 'v1 ' OR v = 'ünïcödé' OR v = '' OR v IS NULL GROUP BY v ORDER BY v;
SELECT u, count(*) FROM df_st GROUP BY u ORDER BY u COLLATE "C" LIMIT 5;
SELECT count(*), max(a.id) FROM df_st a JOIN df_st2 b ON a.t = b.vt;
SELECT count(*), count(b.id) FROM df_st a LEFT JOIN df_st2 b ON a.v = b.tv AND b.id < 3000;
SELECT count(*) FROM df_st_big WHERE t = repeat(md5('7'), 300);
SELECT count(*) FROM df_st_big a JOIN df_st_big b ON a.t = b.t;
SELECT count(*), count(DISTINCT n) FROM (SELECT t, count(*) AS n FROM df_st_big GROUP BY t) s;
-- A value the segments' slice hands out, checked on the client.
SELECT t AS big FROM df_st_big WHERE id = 1500 \gset
SELECT length(:'big'), md5(:'big');
SET datafusion.mode = on;
SELECT count(*), count(t), count(v) FROM df_st WHERE t = 'k42' OR v = 'v7';
SELECT id, t, v FROM df_st WHERE id < 0 ORDER BY id;
SELECT id FROM df_st WHERE t = E'quo"te\\back\nline' OR t = '中文字符' OR t = '' OR v = E'tab\there'
ORDER BY id;
SELECT min(t COLLATE "C"), max(t COLLATE "C"), min(v COLLATE "C"), max(v COLLATE "C") FROM df_st;
SELECT count(*) FROM df_st WHERE t COLLATE "C" > 'k4' AND t COLLATE "C" <= 'k5';
SELECT count(*), min(t), max(v) FROM df_st WHERE t >= 'k2' AND v < 'v5';
SELECT count(*), count(DISTINCT n) FROM (SELECT t, count(*) AS n FROM df_st GROUP BY t) s;
SELECT v, count(*), max(id) FROM df_st
WHERE v = 'v1' OR v = 'v1 ' OR v = 'ünïcödé' OR v = '' OR v IS NULL GROUP BY v ORDER BY v;
SELECT u, count(*) FROM df_st GROUP BY u ORDER BY u COLLATE "C" LIMIT 5;
SELECT count(*), max(a.id) FROM df_st a JOIN df_st2 b ON a.t = b.vt;
SELECT count(*), count(b.id) FROM df_st a LEFT JOIN df_st2 b ON a.v = b.tv AND b.id < 3000;
SELECT count(*) FROM df_st_big WHERE t = repeat(md5('7'), 300);
SELECT count(*) FROM df_st_big a JOIN df_st_big b ON a.t = b.t;
SELECT count(*), count(DISTINCT n) FROM (SELECT t, count(*) AS n FROM df_st_big GROUP BY t) s;
-- A value the segments' slice hands out, checked on the client.
SELECT t AS big FROM df_st_big WHERE id = 1500 \gset
SELECT length(:'big'), md5(:'big');

-- Strings handed out row by row through a cursor.
BEGIN;
DECLARE df_st_cur CURSOR FOR SELECT t, v FROM df_st WHERE id BETWEEN 1 AND 3 OR id = -3;
FETCH 2 FROM df_st_cur;
FETCH 2 FROM df_st_cur;
CLOSE df_st_cur;
COMMIT;

DROP TABLE df_st, df_st2, df_st_big;
DROP COLLATION df_ci;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
