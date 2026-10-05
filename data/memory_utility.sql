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
-- Body of the memory test, run in utility mode on the coordinator by
-- sql/memory.sql (a coordinator-local table gives a single-slice plan).
--
LOAD 'datafusion_executor';
CREATE TABLE df_m4 AS SELECT i AS a, i % 7 AS b FROM generate_series(1, 300000) i;
INSERT INTO df_m4 SELECT i, 0 FROM generate_series(1, 5) i;

-- 300,000 groups do not fit in a 2 MB budget (work_mem * hash_mem_multiplier),
-- so PostgreSQL and DataFusion both spill.  PostgreSQL first, then
-- DataFusion: the rows must match.  The budget runs on one partition.
SET work_mem = '1MB';
SET datafusion.mode = off;
SELECT a, count(*), sum(b) FROM df_m4 GROUP BY a HAVING count(*) > 1;
SET datafusion.mode = on;
SELECT a, count(*), sum(b) FROM df_m4 GROUP BY a HAVING count(*) > 1;
SELECT partitions, memory_limit_kb, spilled_kb > 0 AS spilled, spills > 0 AS spills
FROM datafusion_debug_last_run();

-- A budget the groups fit in: no spill.
SET work_mem = '64MB';
SELECT a, count(*), sum(b) FROM df_m4 GROUP BY a HAVING count(*) > 1;
SELECT memory_limit_kb, spilled_kb, spills FROM datafusion_debug_last_run();

-- The backend's vmem lease covers Rust's heap.
SELECT leased_bytes >= heap_bytes AS covered FROM datafusion_debug_vmem();

DROP TABLE df_m4;
