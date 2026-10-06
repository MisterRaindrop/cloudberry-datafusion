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
-- Motions carry tuples, but for those read by a part of it that does not
-- depend on it.  Each query runs with datafusion.mode off, then
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
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st WHERE t LIKE 'k1%' AND v NOT LIKE '%9';
-- PostgreSQL raises an error only on rows that reach the dangling escape.
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st WHERE t LIKE E'k1\\';
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st WHERE t || 'x' = 'k1x' AND char_length(v) > 2;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st a JOIN df_st2 b ON a.t = b.vt;
EXPLAIN (COSTS OFF) SELECT t FROM df_st_big WHERE id = 1500;
-- Redistributed by text and varchar keys as batches (hashtext through
-- cdbhash is hash_any of the bytes).
EXPLAIN (COSTS OFF) SELECT t, count(*) FROM df_st GROUP BY t;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_st a JOIN df_st b ON a.t = b.t;

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
SELECT count(*) FROM df_st WHERE t LIKE 'k1%' AND v NOT LIKE '%9';
SELECT count(*) FROM df_st WHERE t LIKE '%2_3' OR t LIKE '中_字%' OR v LIKE E'tab\t%';
SELECT id FROM df_st WHERE t LIKE E'%\\\\%' OR t LIKE E'%quo"te%line' OR t LIKE '' ORDER BY id;
-- a pattern from a column
SELECT count(*) FROM df_st WHERE (t LIKE v) IS NOT NULL AND t NOT LIKE u;
SELECT count(*), sum(a.id), sum(b.id) FROM df_st a JOIN df_st b ON a.t = b.t;
SELECT count(*), sum(n) FROM (SELECT v, count(*) AS n FROM df_st GROUP BY v) s;
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
SELECT count(*) FROM df_st WHERE t LIKE 'k1%' AND v NOT LIKE '%9';
SELECT count(*) FROM df_st WHERE t LIKE '%2_3' OR t LIKE '中_字%' OR v LIKE E'tab\t%';
SELECT id FROM df_st WHERE t LIKE E'%\\\\%' OR t LIKE E'%quo"te%line' OR t LIKE '' ORDER BY id;
-- a pattern from a column
SELECT count(*) FROM df_st WHERE (t LIKE v) IS NOT NULL AND t NOT LIKE u;
SELECT count(*), sum(a.id), sum(b.id) FROM df_st a JOIN df_st b ON a.t = b.t;
SELECT count(*), sum(n) FROM (SELECT v, count(*) AS n FROM df_st GROUP BY v) s;
SELECT count(*), count(b.id) FROM df_st a LEFT JOIN df_st2 b ON a.v = b.tv AND b.id < 3000;
SELECT count(*) FROM df_st_big WHERE t = repeat(md5('7'), 300);
SELECT count(*) FROM df_st_big a JOIN df_st_big b ON a.t = b.t;
SELECT count(*), count(DISTINCT n) FROM (SELECT t, count(*) AS n FROM df_st_big GROUP BY t) s;
-- A value the segments' slice hands out, checked on the client.
SELECT t AS big FROM df_st_big WHERE id = 1500 \gset
SELECT length(:'big'), md5(:'big');

-- String functions over edge values: empty, multibyte and 4-byte
-- characters, separators, and integers at both ends of their range.
CREATE TABLE df_sf (id serial, s text, p text, n int4) DISTRIBUTED BY (id);
INSERT INTO df_sf (s, p, n)
SELECT s, p, n FROM
  unnest(ARRAY['', 'abc', ' pad  ', '中文字符串', '😀a😀', 'a,b,,c', 'aaa', NULL]) s,
  unnest(ARRAY['', 'a', ',', 'aa', '😀', NULL]) p,
  unnest(ARRAY[-2147483648, -2, 0, 3, 2147483647, NULL]::int4[]) n;
ANALYZE df_sf;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT s || p, concat(s, p), substr(s, n), split_part(s, p, 2), lower(s COLLATE "C") FROM df_sf;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_sf WHERE concat(s, n) = s;
EXPLAIN (COSTS OFF) SELECT count(*) FROM df_sf WHERE md5(s) = s;
SET datafusion.mode = off;
SELECT s, p, n, char_length(s), octet_length(s), s || p, concat(s, NULL::text, p), strpos(s, p),
  replace(s, p, '#'), starts_with(s, p), split_part(s, p, 2), split_part(s, p, -1)
FROM df_sf WHERE n = 3 ORDER BY id;
SELECT s, n, substr(s, n), substr(s, n, 3), left(s, n), right(s, n), lpad(s, n, 'xy'), rpad(s, n, 'xy')
FROM df_sf WHERE p = 'a' AND n < 100 ORDER BY id;
SELECT s, btrim(s), btrim(s, 'a😀'), ltrim(s, ' a'), rtrim(s), reverse(s), repeat(s, 2),
  lower(s COLLATE "C"), upper(s COLLATE "C")
FROM df_sf WHERE p = '' AND n = 0 ORDER BY id;
SELECT count(*) FROM df_sf WHERE n <> 0 AND split_part(s, ',', n) = 'b';
SET datafusion.mode = on;
SELECT s, p, n, char_length(s), octet_length(s), s || p, concat(s, NULL::text, p), strpos(s, p),
  replace(s, p, '#'), starts_with(s, p), split_part(s, p, 2), split_part(s, p, -1)
FROM df_sf WHERE n = 3 ORDER BY id;
SELECT s, n, substr(s, n), substr(s, n, 3), left(s, n), right(s, n), lpad(s, n, 'xy'), rpad(s, n, 'xy')
FROM df_sf WHERE p = 'a' AND n < 100 ORDER BY id;
SELECT s, btrim(s), btrim(s, 'a😀'), ltrim(s, ' a'), rtrim(s), reverse(s), repeat(s, 2),
  lower(s COLLATE "C"), upper(s COLLATE "C")
FROM df_sf WHERE p = '' AND n = 0 ORDER BY id;
SELECT count(*) FROM df_sf WHERE n <> 0 AND split_part(s, ',', n) = 'b';
-- PostgreSQL's errors, with their SQLSTATEs.
\set VERBOSITY sqlstate
SELECT count(substr(s, 1, -1)) FROM df_sf;
SELECT count(split_part(s, ',', 0)) FROM df_sf;
SELECT count(repeat(s, 1000000000)) FROM df_sf;
SELECT count(lpad(s, 300000000, 'x')) FROM df_sf;
\set VERBOSITY default

-- Strings handed out row by row through a cursor (rows arrive from the
-- segments in any order, so they are all alike).
BEGIN;
DECLARE df_st_cur CURSOR FOR SELECT v, t FROM df_st WHERE v = 'v1' AND t = 'k301';
FETCH 2 FROM df_st_cur;
FETCH 2 FROM df_st_cur;
CLOSE df_st_cur;
COMMIT;

-- character (B1): values as stored, blank-padded, so they print and match
-- LIKE padded; compared, hashed, grouped and sorted without trailing blanks
-- (a tab sorts before the end of the value, not after the padding), as
-- bpcharcmp does.  The cast to text drops the blanks; length counts without
-- them, octet_length with.  min and max stay on PostgreSQL.
CREATE TABLE df_bp (id int, f char(1), c char(10), w char(25)) DISTRIBUTED BY (id);
INSERT INTO df_bp SELECT i,
  CASE i % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'N' ELSE 'R' END,
  CASE WHEN i % 17 = 0 THEN NULL WHEN i % 13 = 0 THEN E'ab\t' WHEN i % 11 = 0 THEN 'ab'
       WHEN i % 7 = 0 THEN ' x ' ELSE 'SEG' || (i % 5) END,
  'NATION_' || (i % 25)
FROM generate_series(1, 20000) i;
CREATE TABLE df_bp2 (k char(15), n int) DISTRIBUTED BY (n);
INSERT INTO df_bp2 SELECT 'SEG' || i, i FROM generate_series(0, 6) i;
INSERT INTO df_bp2 VALUES ('ab', 100), (E'ab\t', 101);
ANALYZE df_bp;
ANALYZE df_bp2;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT f, c, count(*) FROM df_bp WHERE c IN ('ab', 'SEG2') GROUP BY f, c;
EXPLAIN (COSTS OFF) SELECT b.n, count(*) FROM df_bp a JOIN df_bp2 b ON a.c = b.k GROUP BY b.n;
EXPLAIN (COSTS OFF) SELECT max(c) FROM df_bp;
SET datafusion.mode = off;
SELECT count(*) FROM df_bp WHERE c = 'SEG1' OR c = 'ab      ';
SELECT count(*) FROM df_bp WHERE c LIKE 'SEG%' OR c LIKE 'ab';
SELECT count(*) FROM df_bp WHERE c LIKE 'ab        ';
SELECT count(*) FROM df_bp WHERE c COLLATE "C" < 'ab' OR c COLLATE "C" > E'ab\t';
SELECT f, c, count(*) FROM df_bp WHERE c IN ('ab', 'SEG2', E'ab\t') GROUP BY f, c ORDER BY f, c COLLATE "C";
SELECT id, c, '|' || c || '|' AS t, length(c), octet_length(c), concat(c, '|') FROM df_bp
WHERE id % 1000 < 8 ORDER BY c COLLATE "C" DESC NULLS LAST, id LIMIT 12;
SELECT b.n, count(*) FROM df_bp a JOIN df_bp2 b ON a.c = b.k GROUP BY b.n ORDER BY b.n;
SELECT substring(w::text from 1 for 8) AS s, count(DISTINCT c), count(*) FROM df_bp GROUP BY 1 ORDER BY 1;
SET datafusion.mode = on;
SELECT count(*) FROM df_bp WHERE c = 'SEG1' OR c = 'ab      ';
SELECT count(*) FROM df_bp WHERE c LIKE 'SEG%' OR c LIKE 'ab';
SELECT count(*) FROM df_bp WHERE c LIKE 'ab        ';
SELECT count(*) FROM df_bp WHERE c COLLATE "C" < 'ab' OR c COLLATE "C" > E'ab\t';
SELECT f, c, count(*) FROM df_bp WHERE c IN ('ab', 'SEG2', E'ab\t') GROUP BY f, c ORDER BY f, c COLLATE "C";
SELECT id, c, '|' || c || '|' AS t, length(c), octet_length(c), concat(c, '|') FROM df_bp
WHERE id % 1000 < 8 ORDER BY c COLLATE "C" DESC NULLS LAST, id LIMIT 12;
SELECT b.n, count(*) FROM df_bp a JOIN df_bp2 b ON a.c = b.k GROUP BY b.n ORDER BY b.n;
SELECT substring(w::text from 1 for 8) AS s, count(DISTINCT c), count(*) FROM df_bp GROUP BY 1 ORDER BY 1;

-- A slice sorting by the default collation still reads the batches of a
-- split numeric sum (TPC-H Q1): every node runs the aggregate below the
-- Sort in DataFusion, and the Sort too where the collation is C.
CREATE TABLE df_lc (id int4, f char(1), s char(1), q numeric(15,2)) DISTRIBUTED BY (id);
INSERT INTO df_lc SELECT i, chr(65 + i % 3), chr(70 + i % 2), (i % 50) + 0.25
FROM generate_series(1, 30000) i;
ANALYZE df_lc;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT f, s, sum(q), avg(q), count(*) FROM df_lc GROUP BY f, s ORDER BY f, s;
SET datafusion.mode = off;
SELECT f, s, sum(q), avg(q), count(*) FROM df_lc GROUP BY f, s ORDER BY f, s;
SET datafusion.mode = on;
SELECT f, s, sum(q), avg(q), count(*) FROM df_lc GROUP BY f, s ORDER BY f, s;
DROP TABLE df_lc;

DROP TABLE df_st, df_st2, df_st_big, df_sf, df_bp, df_bp2;
DROP COLLATION df_ci;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
