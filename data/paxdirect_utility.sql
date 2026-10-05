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

DROP TABLE df_pd, df_pd_vec;
