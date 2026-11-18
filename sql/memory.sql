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
-- Memory (M4): the slice's operator budget bounds DataFusion's pool, hash
-- aggregation spills instead of failing, and Rust's heap is leased from
-- Cloudberry's vmem tracker.
--
CREATE EXTENSION vec_executor;
\! PGOPTIONS='-c gp_role=utility' psql -X -a -q -d contrib_regression -f data/memory_utility.sql 2>&1

-- Cloudberry enforces vmem limits on QEs.  A 1 TB lease exceeds
-- gp_vmem_protect_limit and fails like an allocation would; afterwards the
-- lease bookkeeping is intact and ordinary leases succeed.
\set VERBOSITY sqlstate
SELECT vec_debug_vmem_lease(1::int8 << 40) FROM gp_dist_random('gp_id');
\set VERBOSITY default
SELECT gp_segment_id, vec_debug_vmem_lease(64::int8 << 20) >= 64::int8 << 20 AS leased
FROM gp_dist_random('gp_id');

-- WF1: the engine's spill files count toward Cloudberry's workfile limits,
-- with the other workfiles of the query, and fail it with the workfile
-- manager's own errors (53000), as PostgreSQL's hash aggregation over the
-- same limit does.  Nothing is left in the account after a failure, a
-- success, or a subtransaction rolled back.
-- Every segment loads the extension, so that their slices spill in the
-- engine.
ALTER DATABASE contrib_regression SET session_preload_libraries = 'vec_executor';
\c
-- enough groups per segment for the engine to spill at 2 MB on clusters of
-- more segments too (ci/multihost.sh: four)
CREATE TABLE df_wf AS SELECT i AS k, i % 100 AS g, repeat('x', 40) AS pad
FROM generate_series(1, 2000000) i DISTRIBUTED BY (g);
SET vec.mode = on;
SET statement_mem = '2MB';
\set VERBOSITY sqlstate
SET gp_workfile_limit_per_query = 1024;
SELECT count(*) FROM (SELECT k, count(*) FROM df_wf GROUP BY k) s;
RESET gp_workfile_limit_per_query;
-- a hash join, which spills to several files on PostgreSQL too (its hash
-- aggregation keeps them in one)
SET gp_workfile_limit_files_per_query = 1;
SELECT count(*) FROM df_wf a JOIN df_wf b USING (k);
RESET gp_workfile_limit_files_per_query;
SET vec.mode = off;
SET gp_workfile_limit_per_query = 1024;
SELECT count(*) FROM (SELECT k, count(*) FROM df_wf GROUP BY k) s;
RESET gp_workfile_limit_per_query;
SET vec.mode = on;
BEGIN;
SAVEPOINT a;
SET LOCAL gp_workfile_limit_per_query = 1024;
SELECT count(*) FROM (SELECT k, count(*) FROM df_wf GROUP BY k) s;
ROLLBACK TO a;
SELECT count(*) FROM (SELECT k, count(*) FROM df_wf GROUP BY k) s;
COMMIT;
\set VERBOSITY default
SELECT count(*) FROM (SELECT k, count(*) FROM df_wf GROUP BY k) s;
SELECT coalesce(sum(size), 0) AS size, coalesce(sum(numfiles), 0) AS files
FROM gp_toolkit.gp_workfile_usage_per_segment;
RESET statement_mem;
RESET vec.mode;
DROP TABLE df_wf;
ALTER DATABASE contrib_regression RESET session_preload_libraries;

DROP EXTENSION vec_executor;
