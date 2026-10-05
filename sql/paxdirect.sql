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

DROP TABLE df_pd_dist;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
