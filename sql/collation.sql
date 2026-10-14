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
-- Ordering strings by the database's default collation runs in DataFusion
-- where that collation orders them by their bytes (C, POSIX, or C.UTF-8 by
-- code point), but whether it does is each node's own: the coordinator and
-- the segments can differ.  Which Motions carry batches
-- must come out alike on every node, so the coordinator asks every segment
-- and sets datafusion.cluster_collation_c, which it sends them.
--
-- A database of C on every node.  Its sessions start without segment
-- processes: the first query here creates them with datafusion.mode off,
-- so that the coordinator, asking at the next one, sends the setting to
-- running processes.
CREATE DATABASE df_coll_c TEMPLATE template0 LC_COLLATE 'C' LC_CTYPE 'C';
ALTER DATABASE df_coll_c SET session_preload_libraries = 'datafusion_executor';
\c df_coll_c
SET datafusion.mode = off;
CREATE TABLE df_coll (k int, name char(25), v numeric(12,2)) DISTRIBUTED BY (k);
INSERT INTO df_coll SELECT i, (ARRAY['FRANCE', 'GERMANY', 'CHINA', 'PERU'])[i % 4 + 1],
  (i % 1000) / 10.0 FROM generate_series(1, 20000) i;
ANALYZE df_coll;
SELECT DISTINCT current_setting('datafusion.cluster_collation_c') FROM gp_dist_random('gp_id');
SET optimizer = on;
SET optimizer_enable_hashagg = off;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
-- The Sort below the combining aggregate orders by the default collation:
-- the whole slice runs in DataFusion, reading the partial sums' state from
-- a batch Motion.
EXPLAIN (COSTS OFF) SELECT name, sum(v) FROM df_coll GROUP BY name ORDER BY name;
SHOW datafusion.cluster_collation_c;
SELECT DISTINCT current_setting('datafusion.cluster_collation_c') FROM gp_dist_random('gp_id');
SET datafusion.mode = off;
SELECT name, sum(v) FROM df_coll GROUP BY name ORDER BY name;
SET datafusion.mode = on;
SELECT name, sum(v) FROM df_coll GROUP BY name ORDER BY name;

-- A database of C.UTF-8 (CU1), which orders by code point: for UTF-8,
-- by the bytes, as C.  The coordinator checks that it does so on every
-- node, so the slice runs in DataFusion as under C.
CREATE DATABASE df_coll_utf8 TEMPLATE template0 LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8';
ALTER DATABASE df_coll_utf8 SET session_preload_libraries = 'datafusion_executor';
\c df_coll_utf8
SET datafusion.mode = off;
CREATE TABLE df_coll (k int, name char(25), v numeric(12,2)) DISTRIBUTED BY (k);
INSERT INTO df_coll SELECT i, (ARRAY['FRANCE', 'GERMANY', 'CHINA', 'PERU', 'chile', 'Éire', 'ÅLAND'])[i % 7 + 1],
  (i % 1000) / 10.0 FROM generate_series(1, 20000) i;
ANALYZE df_coll;
SET optimizer = on;
SET optimizer_enable_hashagg = off;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT name, sum(v) FROM df_coll GROUP BY name ORDER BY name;
SHOW datafusion.cluster_collation_c;
SELECT DISTINCT current_setting('datafusion.cluster_collation_c') FROM gp_dist_random('gp_id');
SET datafusion.mode = off;
SELECT name, sum(v) FROM df_coll GROUP BY name ORDER BY name;
SET datafusion.mode = on;
SELECT name, sum(v) FROM df_coll GROUP BY name ORDER BY name;

-- A database of a language's collation: no slice orders strings in
-- DataFusion, and the setting stays off even when set.
CREATE DATABASE df_coll_en TEMPLATE template0 LC_COLLATE 'en_US.utf8' LC_CTYPE 'en_US.utf8';
ALTER DATABASE df_coll_en SET session_preload_libraries = 'datafusion_executor';
\c df_coll_en
CREATE TABLE df_coll (k int, name char(25), v numeric(12,2)) DISTRIBUTED BY (k);
INSERT INTO df_coll SELECT i, (ARRAY['FRANCE', 'GERMANY', 'CHINA', 'PERU', 'chile', 'Éire', 'ÅLAND'])[i % 7 + 1],
  (i % 1000) / 10.0 FROM generate_series(1, 20000) i;
ANALYZE df_coll;
SET datafusion.cluster_collation_c = on;
SET optimizer = on;
SET optimizer_enable_hashagg = off;
SET datafusion.motion_batches = on;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT name, sum(v) FROM df_coll GROUP BY name ORDER BY name;
SHOW datafusion.cluster_collation_c;
SELECT DISTINCT current_setting('datafusion.cluster_collation_c') FROM gp_dist_random('gp_id');
SET datafusion.mode = off;
SELECT name, sum(v) FROM df_coll GROUP BY name ORDER BY name;
SET datafusion.mode = on;
SELECT name, sum(v) FROM df_coll GROUP BY name ORDER BY name;

-- Comparisons and min/max under it (CU2) run in DataFusion by strcoll_l
-- with the node's locale: case, punctuation and accents order as the
-- language has them, not as the bytes.
SET datafusion.mode = off;
CREATE TABLE df_cu (k int, g int, t text, u varchar(20)) DISTRIBUTED BY (k);
INSERT INTO df_cu SELECT i, i % 5,
  CASE WHEN i % 97 = 0 THEN NULL ELSE (ARRAY['apple', 'Apple', 'a-b', 'ab', 'a b', '0_4z', '04z',
  'Éire', 'eire', 'zebra', 'Zebra', 'co-op', 'coop', 'ä', 'b', '', ' x', 'Ω'])[1 + i % 18] || (i % 3)::text END,
  (ARRAY['m', 'M', 'Ä', 'n', '-n'])[1 + i % 5] || (i % 3)::text
FROM generate_series(1, 6000) i;
ANALYZE df_cu;
SET optimizer = off;
SET datafusion.mode = explain;
EXPLAIN (COSTS OFF) SELECT g, min(t), max(t) FROM df_cu WHERE t < 'b' GROUP BY g;
EXPLAIN (COSTS OFF) SELECT min(t), max(u) FROM df_cu;
EXPLAIN (COSTS OFF) SELECT sum(CASE WHEN t < 'B' THEN 1 ELSE 0 END), sum(CASE WHEN t > u THEN 1 ELSE 0 END) FROM df_cu;
SET datafusion.mode = off;
SELECT sum(CASE WHEN t < 'B' THEN 1 ELSE 0 END), sum(CASE WHEN t >= 'apple1' THEN 1 ELSE 0 END),
  sum(CASE WHEN t BETWEEN 'a' AND 'coop2' THEN 1 ELSE 0 END), sum(CASE WHEN t > u THEN 1 ELSE 0 END) FROM df_cu;
SELECT g, min(t), max(t), min(u), max(u) FROM df_cu WHERE t < 'b' GROUP BY g ORDER BY g;
SELECT min(t), max(t), min(u), max(u), min(t COLLATE "C") FROM df_cu;
SET datafusion.mode = on;
SELECT sum(CASE WHEN t < 'B' THEN 1 ELSE 0 END), sum(CASE WHEN t >= 'apple1' THEN 1 ELSE 0 END),
  sum(CASE WHEN t BETWEEN 'a' AND 'coop2' THEN 1 ELSE 0 END), sum(CASE WHEN t > u THEN 1 ELSE 0 END) FROM df_cu;
SELECT g, min(t), max(t), min(u), max(u) FROM df_cu WHERE t < 'b' GROUP BY g ORDER BY g;
SELECT min(t), max(t), min(u), max(u), min(t COLLATE "C") FROM df_cu;

\c contrib_regression
DROP DATABASE df_coll_c WITH (FORCE);
DROP DATABASE df_coll_utf8 WITH (FORCE);
DROP DATABASE df_coll_en WITH (FORCE);
