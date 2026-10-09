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
-- The experimental direct PAX reader (datafusion.pax_direct_read): PAX
-- micro-partitions decoded column by column on DataFusion's threads,
-- through datafusion_pax.so.  The coordinator-local part runs in utility
-- mode (data/paxdirect_utility.sql); a distributed table exercises the
-- segments.  Each query runs with the reader off, then on; results match.
--
CREATE EXTENSION datafusion_executor;
\! PGOPTIONS='-c gp_role=utility' psql -X -a -q -d contrib_regression -f data/paxdirect_utility.sql 2>&1

ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
CREATE TABLE df_pd_dist (a int4, b int8, c float8, e int2) USING pax DISTRIBUTED BY (a);
INSERT INTO df_pd_dist SELECT i, i * 10, i / 4.0, (i % 100)::int2 FROM generate_series(1, 200000) i;
INSERT INTO df_pd_dist VALUES (NULL, NULL, NULL, NULL);
DELETE FROM df_pd_dist WHERE a % 1000 = 7;

SET datafusion.mode = off;
SELECT count(*), count(a), sum(a), max(c), sum(e) FROM df_pd_dist WHERE e < 50;
SELECT e, count(*), sum(a) FROM df_pd_dist WHERE e >= 97 OR e IS NULL GROUP BY e;
SET datafusion.mode = on;
SET datafusion.pax_direct_read = on;
SELECT count(*), count(a), sum(a), max(c), sum(e) FROM df_pd_dist WHERE e < 50;
SELECT e, count(*), sum(a) FROM df_pd_dist WHERE e >= 97 OR e IS NULL GROUP BY e;

-- Strings (C1): text, varchar and char(n) as offsets and bytes, with NULLs,
-- empty strings, multibyte characters and char(n)'s padding; values PAX
-- compressed (over pax.min_size_of_compress_toast) or put in its toast file
-- (over pax.min_size_of_external_toast); the vectorized storage format,
-- which keeps char(n) without padding and strings without headers.
CREATE TABLE df_pd_str (id int, t text, v varchar(12), b char(5)) USING pax DISTRIBUTED BY (id);
INSERT INTO df_pd_str SELECT k,
  CASE WHEN k % 11 = 0 THEN NULL WHEN k % 7 = 0 THEN '' ELSE 'é' || k END,
  CASE WHEN k % 13 = 0 THEN NULL ELSE substr('中文字符串abcdefgh', 1 + k % 5, k % 12) END,
  CASE WHEN k % 17 = 0 THEN NULL WHEN k % 5 = 0 THEN '' ELSE chr(97 + k % 26) || (k % 100) END
FROM generate_series(1, 20000) k;
INSERT INTO df_pd_str VALUES (-1, repeat('ab', 300000), 'big', 'z'),
  (-2, repeat('xyz', 3600000), 'bigger', 'z');
DELETE FROM df_pd_str WHERE id % 1000 = 3;
CREATE TABLE df_pd_vec (id int, t text, b char(4)) USING pax
  WITH (storage_format = porc_vec) DISTRIBUTED BY (id);
INSERT INTO df_pd_vec SELECT k, CASE WHEN k % 9 = 0 THEN NULL ELSE 'v' || k END,
  CASE WHEN k % 6 = 0 THEN NULL ELSE (k % 50)::text END FROM generate_series(1, 5000) k;

SET datafusion.mode = off;
SET datafusion.pax_direct_read = off;
SELECT count(*), count(t), count(v), count(b), sum(length(t)), sum(length(v)), sum(octet_length(b)) FROM df_pd_str;
SELECT id, t, v, b, octet_length(b) FROM df_pd_str WHERE id BETWEEN 1 AND 12 ORDER BY id;
SELECT id, length(t), substr(t, 1, 7) FROM df_pd_str WHERE id < 0 ORDER BY id;
SELECT b, count(*) FROM df_pd_str WHERE b < 'b' COLLATE "C" OR b IS NULL GROUP BY b ORDER BY b COLLATE "C";
SELECT count(*), count(t), sum(length(t)), sum(octet_length(b)) FROM df_pd_vec;
SELECT id, t, b, octet_length(b) FROM df_pd_vec WHERE id BETWEEN 5 AND 13 ORDER BY id;
SET datafusion.mode = on;
SET datafusion.pax_direct_read = on;
SELECT count(*), count(t), count(v), count(b), sum(length(t)), sum(length(v)), sum(octet_length(b)) FROM df_pd_str;
SELECT id, t, v, b, octet_length(b) FROM df_pd_str WHERE id BETWEEN 1 AND 12 ORDER BY id;
SELECT id, length(t), substr(t, 1, 7) FROM df_pd_str WHERE id < 0 ORDER BY id;
SELECT b, count(*) FROM df_pd_str WHERE b < 'b' COLLATE "C" OR b IS NULL GROUP BY b ORDER BY b COLLATE "C";
SELECT count(*), count(t), sum(length(t)), sum(octet_length(b)) FROM df_pd_vec;
SELECT id, t, b, octet_length(b) FROM df_pd_vec WHERE id BETWEEN 5 AND 13 ORDER BY id;

DROP TABLE df_pd_dist, df_pd_str, df_pd_vec;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
