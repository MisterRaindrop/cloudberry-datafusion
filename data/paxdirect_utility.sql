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
-- in the same backend.  Each query runs with datafusion.mode off, then on
-- with datafusion.pax_direct_read; the results must match, and the counter
-- shows that the PAX reader read the table directly.
--
LOAD 'datafusion_executor';
CREATE TABLE df_pd (a int4, b int8, c float8, d bool, e int2) USING pax;
CREATE TABLE df_pd_vec (a int4, b int8, c float8, d bool, e int2) USING pax
  WITH (storage_format = 'porc_vec');
INSERT INTO df_pd SELECT i, i * 10, i / 4.0, i % 2 = 0, (i % 100)::int2
FROM generate_series(1, 300000) i;
INSERT INTO df_pd VALUES (NULL, NULL, NULL, NULL, NULL), (300001, NULL, 1.5, NULL, 7);
INSERT INTO df_pd_vec SELECT * FROM df_pd;
DELETE FROM df_pd WHERE a % 1000 = 7;
DELETE FROM df_pd_vec WHERE a % 1000 = 7;

SET datafusion.mode = off;
SELECT count(*), count(a), count(b), sum(a), sum(e), min(c), max(b) FROM df_pd WHERE e < 50;
SELECT d, count(*), min(e), max(c) FROM df_pd GROUP BY d;
SELECT count(*) FROM df_pd;
SELECT a, b, c, d, e FROM df_pd WHERE a % 100000 = 0 OR a % 1000 = 7 OR a IS NULL OR b IS NULL;
SELECT count(*), count(a), count(b), sum(a), sum(e), min(c), max(b) FROM df_pd_vec WHERE e < 50;
SELECT a, b, c, d, e FROM df_pd_vec WHERE a % 100000 = 0 OR a IS NULL OR b IS NULL;
SELECT d, count(*), sum(e) FROM df_pd_vec GROUP BY d;

SET datafusion.mode = on;
SET datafusion.pax_direct_read = on;
SELECT count(*), count(a), count(b), sum(a), sum(e), min(c), max(b) FROM df_pd WHERE e < 50;
SELECT d, count(*), min(e), max(c) FROM df_pd GROUP BY d;
SELECT count(*) FROM df_pd;
SELECT a, b, c, d, e FROM df_pd WHERE a % 100000 = 0 OR a % 1000 = 7 OR a IS NULL OR b IS NULL;
SELECT count(*), count(a), count(b), sum(a), sum(e), min(c), max(b) FROM df_pd_vec WHERE e < 50;
SELECT a, b, c, d, e FROM df_pd_vec WHERE a % 100000 = 0 OR a IS NULL OR b IS NULL;
SELECT d, count(*), sum(e) FROM df_pd_vec GROUP BY d;
SELECT datafusion_debug_pax_direct_scans() AS direct_scans;

-- Rows inserted and deleted earlier in the same transaction are seen as
-- the snapshot sees them.
BEGIN;
INSERT INTO df_pd SELECT i, i, i, true, 1 FROM generate_series(400001, 400010) i;
DELETE FROM df_pd WHERE a BETWEEN 1 AND 100;
SELECT count(*), sum(a) FROM df_pd;
SET datafusion.mode = off;
SELECT count(*), sum(a) FROM df_pd;
ROLLBACK;
SET datafusion.mode = on;
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
SET datafusion.mode = off;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a BETWEEN 200000 AND 210000;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a < 5000 OR a > 490000;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a > 600000;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE e = 7;
SET datafusion.mode = on;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a BETWEEN 200000 AND 210000;
SELECT files, files_skipped, groups, groups_skipped, decode_peak_kb > 0 AS decoded
FROM datafusion_debug_last_pax();
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a < 5000 OR a > 490000;
SELECT files, files_skipped, groups, groups_skipped FROM datafusion_debug_last_pax();
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a > 600000;
SELECT files, files_skipped, groups, groups_skipped, decode_peak_kb FROM datafusion_debug_last_pax();
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE e = 7;
SELECT files, files_skipped, groups, groups_skipped FROM datafusion_debug_last_pax();
-- PAX's switch for its own skipping applies too.
SET pax.enable_sparse_filter = off;
SELECT count(*), sum(a), max(b) FROM df_pd_skip WHERE a BETWEEN 200000 AND 210000;
SELECT files, files_skipped, groups, groups_skipped FROM datafusion_debug_last_pax();
RESET pax.enable_sparse_filter;
-- The memory PAX's reader held is given back once the scan is over.
SELECT count(*) FROM df_pd_skip;
SELECT heap_bytes < 4 * 1024 * 1024 AS heap_returned FROM datafusion_debug_vmem();

DROP TABLE df_pd, df_pd_vec, df_pd_skip;
