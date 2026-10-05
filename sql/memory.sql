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
CREATE EXTENSION datafusion_executor;
\! PGOPTIONS='-c gp_role=utility' psql -X -a -q -d contrib_regression -f data/memory_utility.sql 2>&1

-- Cloudberry enforces vmem limits on QEs.  A 1 TB lease exceeds
-- gp_vmem_protect_limit and fails like an allocation would; afterwards the
-- lease bookkeeping is intact and ordinary leases succeed.
\set VERBOSITY sqlstate
SELECT datafusion_debug_vmem_lease(1::int8 << 40) FROM gp_dist_random('gp_id');
\set VERBOSITY default
SELECT gp_segment_id, datafusion_debug_vmem_lease(64::int8 << 20) >= 64::int8 << 20 AS leased
FROM gp_dist_random('gp_id');

DROP EXTENSION datafusion_executor;
