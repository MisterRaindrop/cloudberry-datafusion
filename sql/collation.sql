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
-- where that collation is C, but whether it is C is each node's own: the
-- coordinator and the segments can differ.  Which Motions carry batches
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

-- A database of another collation: no slice orders strings in DataFusion,
-- and the setting stays off even when set.
CREATE DATABASE df_coll_utf8 TEMPLATE template0 LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8';
ALTER DATABASE df_coll_utf8 SET session_preload_libraries = 'datafusion_executor';
\c df_coll_utf8
CREATE TABLE df_coll (k int, name char(25), v numeric(12,2)) DISTRIBUTED BY (k);
INSERT INTO df_coll SELECT i, (ARRAY['FRANCE', 'GERMANY', 'CHINA', 'PERU'])[i % 4 + 1],
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

\c contrib_regression
DROP DATABASE df_coll_c WITH (FORCE);
DROP DATABASE df_coll_utf8 WITH (FORCE);
