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
-- Body of the paxdirect test, run in utility mode on the coordinator by
-- sql/paxdirect.sql, so that the scans and the counter of direct reads are
-- in the same backend.  Each query runs with vec.mode off, then on
-- with vec.pax_direct_read; the results must match, and the counter
-- shows that the PAX reader read the table directly.
--
LOAD 'vec_executor';
CREATE TABLE df_pd (a int4, b int8, c float8, d bool, e int2) USING pax;
CREATE TABLE df_pd_vec (a int4, b int8, c float8, d bool, e int2) USING pax
  WITH (storage_format = 'porc_vec');
INSERT INTO df_pd SELECT i, i * 10, i / 4.0, i % 2 = 0, (i % 100)::int2
FROM generate_series(1, 300000) i;
INSERT INTO df_pd VALUES (NULL, NULL, NULL, NULL, NULL), (300001, NULL, 1.5, NULL, 7);
INSERT INTO df_pd_vec SELECT * FROM df_pd;
DELETE FROM df_pd WHERE a % 1000 = 7;
DELETE FROM df_pd_vec WHERE a % 1000 = 7;

SET vec.mode = off;
SELECT count(*), count(a), count(b), sum(a), sum(e), min(c), max(b) FROM df_pd WHERE e < 50;
SELECT d, count(*), min(e), max(c) FROM df_pd GROUP BY d ORDER BY d;
SELECT count(*) FROM df_pd;
SELECT a, b, c, d, e FROM df_pd WHERE a % 100000 = 0 OR a % 1000 = 7 OR a IS NULL OR b IS NULL ORDER BY a, b;
SELECT count(*), count(a), count(b), sum(a), sum(e), min(c), max(b) FROM df_pd_vec WHERE e < 50;
SELECT a, b, c, d, e FROM df_pd_vec WHERE a % 100000 = 0 OR a IS NULL OR b IS NULL ORDER BY a, b;
SELECT d, count(*), sum(e) FROM df_pd_vec GROUP BY d ORDER BY d;

SET vec.mode = on;
SET vec.pax_direct_read = on;
SELECT count(*), count(a), count(b), sum(a), sum(e), min(c), max(b) FROM df_pd WHERE e < 50;
SELECT d, count(*), min(e), max(c) FROM df_pd GROUP BY d ORDER BY d;
SELECT count(*) FROM df_pd;
SELECT a, b, c, d, e FROM df_pd WHERE a % 100000 = 0 OR a % 1000 = 7 OR a IS NULL OR b IS NULL ORDER BY a, b;
SELECT count(*), count(a), count(b), sum(a), sum(e), min(c), max(b) FROM df_pd_vec WHERE e < 50;
SELECT a, b, c, d, e FROM df_pd_vec WHERE a % 100000 = 0 OR a IS NULL OR b IS NULL ORDER BY a, b;
SELECT d, count(*), sum(e) FROM df_pd_vec GROUP BY d ORDER BY d;
SELECT vec_debug_pax_direct_scans() AS direct_scans;

-- Rows inserted and deleted earlier in the same transaction are seen as
-- the snapshot sees them.
BEGIN;
INSERT INTO df_pd SELECT i, i, i, true, 1 FROM generate_series(400001, 400010) i;
DELETE FROM df_pd WHERE a BETWEEN 1 AND 100;
SELECT count(*), sum(a) FROM df_pd;
SET vec.mode = off;
SELECT count(*), sum(a) FROM df_pd;
ROLLBACK;
SET vec.mode = on;
SELECT count(*), sum(a) FROM df_pd;

-- Min/max skipping: micro-partitions and groups whose statistics rule out
-- the scan's qual are not read (4 micro-partitions of 8 groups or fewer).
-- Only column a has statistics.
SET pax.max_tuples_per_file = 131072;
SET pax.max_tuples_per_group = 16384;
CREATE TABLE df_pd_skip (a int4, b int8, e int2) USING pax WITH (minmax_columns = 'a');
INSERT INTO df_pd_skip SELECT i, i * 10, (i % 100)::int2 FROM generate_series(1, 500000) i;
RESET pax.max_tuples_per_file;
RESET pax.max_tuples_per_group;
SET vec.mode = off;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a BETWEEN 200000 AND 210000;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a < 5000 OR a > 490000;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a > 600000;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE e = 7;
SET vec.mode = on;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a BETWEEN 200000 AND 210000;
SELECT files, files_skipped, groups, groups_skipped, decode_peak_kb > 0 AS decoded
FROM vec_debug_last_pax();
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a < 5000 OR a > 490000;
SELECT files, files_skipped, groups, groups_skipped FROM vec_debug_last_pax();
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a > 600000;
SELECT files, files_skipped, groups, groups_skipped, decode_peak_kb FROM vec_debug_last_pax();
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE e = 7;
SELECT files, files_skipped, groups, groups_skipped FROM vec_debug_last_pax();
-- PAX's switch for its own skipping applies too.
SET pax.enable_sparse_filter = off;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a BETWEEN 200000 AND 210000;
SELECT files, files_skipped, groups, groups_skipped FROM vec_debug_last_pax();
RESET pax.enable_sparse_filter;
-- Statistics on a date column skip as well.
SET pax.max_tuples_per_group = 16384;
CREATE TABLE df_pd_date (d date, ts timestamp) USING pax WITH (minmax_columns = 'd');
INSERT INTO df_pd_date SELECT date '1990-01-01' + i / 100, timestamp '1990-01-01' + i * interval '1 minute'
FROM generate_series(1, 100000) i;
INSERT INTO df_pd_date VALUES ('infinity', '-infinity'), (NULL, NULL);
RESET pax.max_tuples_per_group;
SET vec.mode = off;
SELECT count(*), min(ts), max(ts) FROM df_pd_date WHERE d >= '1991-03-01' AND d < '1991-04-01';
SELECT d, ts FROM df_pd_date WHERE d > '2040-01-01' OR d IS NULL ORDER BY d, ts;
SET vec.mode = on;
SELECT count(*), min(ts), max(ts) FROM df_pd_date WHERE d >= '1991-03-01' AND d < '1991-04-01';
SELECT files, files_skipped, groups, groups_skipped FROM vec_debug_last_pax();
SELECT d, ts FROM df_pd_date WHERE d > '2040-01-01' OR d IS NULL ORDER BY d, ts;
-- The memory PAX's reader held is given back once the scan is over.
SELECT count(*) FROM df_pd_skip;
SELECT heap_bytes < 4 * 1024 * 1024 AS heap_returned FROM vec_debug_vmem();

DROP TABLE df_pd, df_pd_vec, df_pd_skip, df_pd_date;

-- DC1: the decoded-column cache (vec.pax_decode_cache), in a
-- directory of the coordinator's data directory.  The first scans fill it;
-- run again, they read all 4 groups from it each and write nothing.
-- Entries that do not fit what they should hold, one cut short and one
-- with offsets out of order, are removed and rebuilt.  A cache at its size
-- limit takes no more.  Every result matches the table AM's.
SET pax.max_tuples_per_group = 5000;
CREATE TABLE df_pd_cache (a int4, b int8, n numeric(15, 2), t text, v varchar(10)) USING pax;
INSERT INTO df_pd_cache SELECT i, i * 7, i / 100.0,
  CASE WHEN i % 9 = 0 THEN NULL ELSE 'v' || i END,
  CASE WHEN i % 5 = 0 THEN '' ELSE repeat('x', i % 10) END
FROM generate_series(1, 20000) i;
RESET pax.max_tuples_per_group;
DELETE FROM df_pd_cache WHERE a % 777 = 0;
SET vec.mode = off;
SELECT count(*), sum(a), sum(b), sum(n), count(t), sum(length(t)), sum(length(v)), max(t) FROM df_pd_cache;
SELECT a, n, t, v FROM df_pd_cache WHERE a % 4999 = 0 ORDER BY a;
SET vec.mode = on;
SELECT current_setting('data_directory') AS dd \gset
\setenv DF_PD_DD :dd
SET vec.pax_decode_cache = 'df_pd_cache_dir';
SELECT hits AS h0, fills AS f0, bad AS b0 FROM vec_debug_pax_cache() \gset
SELECT count(*), sum(a), sum(b), sum(n), count(t), sum(length(t)), sum(length(v)), max(t) FROM df_pd_cache;
SELECT a, n, t, v FROM df_pd_cache WHERE a % 4999 = 0 ORDER BY a;
SELECT fills - :f0 > 0 AS filled, bad - :b0 AS bad, unusable FROM vec_debug_pax_cache();
SELECT hits AS h0, fills AS f0 FROM vec_debug_pax_cache() \gset
SELECT count(*), sum(a), sum(b), sum(n), count(t), sum(length(t)), sum(length(v)), max(t) FROM df_pd_cache;
SELECT a, n, t, v FROM df_pd_cache WHERE a % 4999 = 0 ORDER BY a;
SELECT hits - :h0 AS hits, fills - :f0 AS fills FROM vec_debug_pax_cache();
-- one micro-partition: one directory of entries
SELECT count(*) FROM pg_ls_dir('df_pd_cache_dir') d WHERE d <> '.sweep';
SELECT :'dd' || '/df_pd_cache_dir/' || d AS mp FROM pg_ls_dir('df_pd_cache_dir') d WHERE d <> '.sweep' \gset
-- Column t of group 0 cut short.  Column v of group 1 with its second
-- offset past the third: v has no NULLs, so its offsets follow the header
-- (40 bytes) at once.
-- (a large object is seen from the statement after the one creating it;
-- Cloudberry refuses lo_unlink, the objects go with the database)
SELECT lo_from_bytea(0, substr(b, 1, length(b) - 3)) AS lo
FROM pg_read_binary_file(:'mp' || '/g0_c3_w-1_s-1') b \gset
SELECT lo_export(:lo, :'mp' || '/g0_c3_w-1_s-1');
SELECT lo_from_bytea(0, overlay(b PLACING '\xffffff7f'::bytea FROM 41 + 4 FOR 4)) AS lo
FROM pg_read_binary_file(:'mp' || '/g1_c4_w-1_s-1') b \gset
SELECT lo_export(:lo, :'mp' || '/g1_c4_w-1_s-1');
SELECT hits AS h0, fills AS f0, bad AS b0 FROM vec_debug_pax_cache() \gset
SELECT count(*), sum(a), sum(b), sum(n), count(t), sum(length(t)), sum(length(v)), max(t) FROM df_pd_cache;
SELECT hits - :h0 AS hits, fills - :f0 AS fills, bad - :b0 AS bad FROM vec_debug_pax_cache();
SELECT hits AS h0, fills AS f0, bad AS b0 FROM vec_debug_pax_cache() \gset
SELECT count(*), sum(a), sum(b), sum(n), count(t), sum(length(t)), sum(length(v)), max(t) FROM df_pd_cache;
SELECT hits - :h0 AS hits, fills - :f0 AS fills, bad - :b0 AS bad FROM vec_debug_pax_cache();
-- a cache of 1 MB, smaller than the table's columns (about 200 kB a group)
INSERT INTO df_pd_cache SELECT * FROM df_pd_cache;
SET vec.pax_decode_cache = 'df_pd_cache_small';
SET vec.pax_decode_cache_size = '1MB';
SELECT refused AS r0 FROM vec_debug_pax_cache() \gset
SELECT count(*), sum(a), sum(b), sum(n), count(t), sum(length(t)), sum(length(v)), max(t) FROM df_pd_cache;
SET vec.mode = off;
SELECT count(*), sum(a), sum(b), sum(n), count(t), sum(length(t)), sum(length(v)), max(t) FROM df_pd_cache;
SET vec.mode = on;
SELECT refused - :r0 > 0 AS refused FROM vec_debug_pax_cache();
SELECT sum((pg_stat_file('df_pd_cache_small/' || d || '/' || f)).size) <= 1024 * 1024 AS within_limit
FROM pg_ls_dir('df_pd_cache_small') d, pg_ls_dir('df_pd_cache_small/' || d) f WHERE d <> '.sweep';
-- a directory that cannot be used leaves scans without the cache
SET vec.pax_decode_cache = '/proc/df_pd_no_cache';
SELECT unusable AS u0 FROM vec_debug_pax_cache() \gset
SELECT count(*), sum(a) FROM df_pd_cache;
SELECT unusable - :u0 AS unusable FROM vec_debug_pax_cache();
RESET vec.pax_decode_cache;
RESET vec.pax_decode_cache_size;
\! rm -rf "$DF_PD_DD/df_pd_cache_dir" "$DF_PD_DD/df_pd_cache_small"
DROP TABLE df_pd_cache;
