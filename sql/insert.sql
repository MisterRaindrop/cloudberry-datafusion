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
-- The queries of INSERT, CREATE TABLE AS and SELECT INTO (IN1): the slices
-- and subtrees that read run in DataFusion, the node that writes stays on
-- PostgreSQL.  Each statement runs with vec.mode off into one table, then
-- on into another; the two tables must hold the same rows.
--
CREATE EXTENSION vec_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'vec_executor';
\c
SET optimizer = off;
SET vec.worker_threads = 2;

CREATE TABLE df_in_src (a int4, b int8, c numeric(12, 2), e int2, t text) DISTRIBUTED BY (a);
INSERT INTO df_in_src SELECT i, i % 1000, i / 4.0, (i % 100)::int2, 'v' || (i % 37)
FROM generate_series(1, 100000) i;
INSERT INTO df_in_src VALUES (NULL, NULL, NULL, NULL, NULL);
CREATE TABLE df_in_dim (b int8, name text) DISTRIBUTED BY (b);
INSERT INTO df_in_dim SELECT i, 'name' || i FROM generate_series(0, 999, 3) i;
ANALYZE df_in_src;
ANALYZE df_in_dim;

-- Whether two tables hold the same rows (each row as often).
CREATE FUNCTION df_in_same(x regclass, y regclass) RETURNS text AS $$
DECLARE
  nx bigint; ny bigint; dxy bigint; dyx bigint;
BEGIN
  EXECUTE format('SELECT count(*) FROM %s', x) INTO nx;
  EXECUTE format('SELECT count(*) FROM %s', y) INTO ny;
  EXECUTE format('SELECT count(*) FROM (SELECT * FROM %s EXCEPT ALL SELECT * FROM %s) d', x, y) INTO dxy;
  EXECUTE format('SELECT count(*) FROM (SELECT * FROM %s EXCEPT ALL SELECT * FROM %s) d', y, x) INTO dyx;
  RETURN format('%s rows, %s rows, %s and %s differ', nx, ny, dxy, dyx);
END $$ LANGUAGE plpgsql;

SET vec.mode = explain;
-- The rows go to the segment that stores them: that slice holds ModifyTable
-- and stays on PostgreSQL; the one below it runs in DataFusion.
EXPLAIN (COSTS OFF)
INSERT INTO df_in_src SELECT b::int4, count(*), sum(c), max(e), min(t) FROM df_in_src GROUP BY b;
-- CREATE TABLE AS and SELECT INTO: every slice.
EXPLAIN (COSTS OFF)
CREATE TABLE df_in_x AS SELECT b, count(*) n, sum(c) s FROM df_in_src GROUP BY b DISTRIBUTED BY (b);
EXPLAIN (COSTS OFF)
SELECT b, count(*) n, sum(c) s INTO df_in_x FROM df_in_src GROUP BY b;
-- UPDATE and DELETE stay on PostgreSQL.
EXPLAIN (COSTS OFF) UPDATE df_in_src SET e = 0 WHERE a < 10;
EXPLAIN (COSTS OFF) DELETE FROM df_in_src WHERE a < 10;

-- INSERT ... SELECT into each table access method: an aggregate, a join
-- with a filter, and plain rows.
CREATE TABLE df_in_heap_off (k int8, n int8, s numeric, m int2, name text) USING heap DISTRIBUTED BY (k);
CREATE TABLE df_in_heap_on (LIKE df_in_heap_off) USING heap DISTRIBUTED BY (k);
CREATE TABLE df_in_ao_off (LIKE df_in_heap_off) USING ao_row DISTRIBUTED BY (k);
CREATE TABLE df_in_ao_on (LIKE df_in_heap_off) USING ao_row DISTRIBUTED BY (k);
CREATE TABLE df_in_aocs_off (LIKE df_in_heap_off) USING ao_column DISTRIBUTED BY (k);
CREATE TABLE df_in_aocs_on (LIKE df_in_heap_off) USING ao_column DISTRIBUTED BY (k);
CREATE TABLE df_in_pax_off (LIKE df_in_heap_off) USING pax DISTRIBUTED BY (k);
CREATE TABLE df_in_pax_on (LIKE df_in_heap_off) USING pax DISTRIBUTED BY (k);

-- Grouped by the target's distribution key, the final aggregate is in the
-- slice holding ModifyTable, and runs below it in DataFusion (T2).
SET vec.mode = explain;
EXPLAIN (COSTS OFF)
INSERT INTO df_in_heap_off SELECT b, count(*), sum(c), max(e), min(t) FROM df_in_src GROUP BY b;

SET vec.mode = off;
INSERT INTO df_in_heap_off SELECT b, count(*), sum(c), max(e), min(t) FROM df_in_src GROUP BY b;
INSERT INTO df_in_heap_off SELECT s.a, s.b, s.c, s.e, d.name FROM df_in_src s JOIN df_in_dim d USING (b) WHERE s.e < 20;
INSERT INTO df_in_heap_off SELECT a, b, c, e, t FROM df_in_src WHERE a % 7 = 0 OR a IS NULL;
INSERT INTO df_in_ao_off SELECT * FROM df_in_heap_off;
INSERT INTO df_in_aocs_off SELECT * FROM df_in_heap_off;
INSERT INTO df_in_pax_off SELECT * FROM df_in_heap_off;
SET vec.mode = on;
INSERT INTO df_in_heap_on SELECT b, count(*), sum(c), max(e), min(t) FROM df_in_src GROUP BY b;
INSERT INTO df_in_heap_on SELECT s.a, s.b, s.c, s.e, d.name FROM df_in_src s JOIN df_in_dim d USING (b) WHERE s.e < 20;
INSERT INTO df_in_heap_on SELECT a, b, c, e, t FROM df_in_src WHERE a % 7 = 0 OR a IS NULL;
INSERT INTO df_in_ao_on SELECT b, count(*), sum(c), max(e), min(t) FROM df_in_src GROUP BY b;
INSERT INTO df_in_ao_on SELECT s.a, s.b, s.c, s.e, d.name FROM df_in_src s JOIN df_in_dim d USING (b) WHERE s.e < 20;
INSERT INTO df_in_ao_on SELECT a, b, c, e, t FROM df_in_src WHERE a % 7 = 0 OR a IS NULL;
INSERT INTO df_in_aocs_on SELECT * FROM df_in_ao_on;
INSERT INTO df_in_pax_on SELECT * FROM df_in_aocs_on;
SET vec.mode = off;
SELECT df_in_same('df_in_heap_off', 'df_in_heap_on');
SELECT df_in_same('df_in_heap_off', 'df_in_ao_on');
SELECT df_in_same('df_in_heap_off', 'df_in_aocs_on');
SELECT df_in_same('df_in_heap_off', 'df_in_pax_on');

-- A table that the statement reads and inserts into: the scans see the
-- table as it was when the statement started, not the rows it adds.
SET vec.mode = off;
INSERT INTO df_in_heap_off SELECT k + 1000000, n, s, m, name FROM df_in_heap_off WHERE m < 50;
INSERT INTO df_in_ao_off SELECT k + 1000000, n, s, m, name FROM df_in_ao_off WHERE m < 50;
INSERT INTO df_in_aocs_off SELECT k + 1000000, n, s, m, name FROM df_in_aocs_off WHERE m < 50;
INSERT INTO df_in_pax_off SELECT k + 1000000, n, s, m, name FROM df_in_pax_off WHERE m < 50;
INSERT INTO df_in_heap_off SELECT max(k) + 1, count(*), sum(s), NULL, NULL FROM df_in_heap_off;
SET vec.mode = on;
INSERT INTO df_in_heap_on SELECT k + 1000000, n, s, m, name FROM df_in_heap_on WHERE m < 50;
INSERT INTO df_in_ao_on SELECT k + 1000000, n, s, m, name FROM df_in_ao_on WHERE m < 50;
INSERT INTO df_in_aocs_on SELECT k + 1000000, n, s, m, name FROM df_in_aocs_on WHERE m < 50;
INSERT INTO df_in_pax_on SELECT k + 1000000, n, s, m, name FROM df_in_pax_on WHERE m < 50;
INSERT INTO df_in_heap_on SELECT max(k) + 1, count(*), sum(s), NULL, NULL FROM df_in_heap_on;
SET vec.mode = off;
SELECT df_in_same('df_in_heap_off', 'df_in_heap_on');
SELECT df_in_same('df_in_ao_off', 'df_in_ao_on');
SELECT df_in_same('df_in_aocs_off', 'df_in_aocs_on');
SELECT df_in_same('df_in_pax_off', 'df_in_pax_on');

-- RETURNING: ModifyTable's own output.
CREATE TABLE df_in_ret (k int8, n int8) DISTRIBUTED BY (k);
SET vec.mode = on;
INSERT INTO df_in_ret SELECT e, count(*) FROM df_in_src WHERE e >= 97 GROUP BY e;
INSERT INTO df_in_ret SELECT e, count(*) FROM df_in_src WHERE e = 96 GROUP BY e RETURNING k, n;

-- A partitioned table: ModifyTable routes each row to its partition.
CREATE TABLE df_in_part_off (k int8, n int8, s numeric) DISTRIBUTED BY (k)
  PARTITION BY RANGE (k) (START (0) END (1000) EVERY (250), DEFAULT PARTITION other);
CREATE TABLE df_in_part_on (LIKE df_in_part_off) DISTRIBUTED BY (k)
  PARTITION BY RANGE (k) (START (0) END (1000) EVERY (250), DEFAULT PARTITION other);
SET vec.mode = off;
INSERT INTO df_in_part_off SELECT b, count(*), sum(c) FROM df_in_src GROUP BY b;
SET vec.mode = on;
INSERT INTO df_in_part_on SELECT b, count(*), sum(c) FROM df_in_src GROUP BY b;
SET vec.mode = off;
SELECT df_in_same('df_in_part_off', 'df_in_part_on');
SELECT count(*) FROM df_in_part_on_1_prt_other;

-- CREATE TABLE AS, SELECT INTO, materialized views.
SET vec.mode = off;
CREATE TABLE df_in_ctas_off AS
  SELECT b, count(*) n, sum(c) s, max(t) t FROM df_in_src WHERE e < 90 GROUP BY b DISTRIBUTED BY (b);
CREATE MATERIALIZED VIEW df_in_mv_off AS
  SELECT d.name, count(*) n, sum(s.c) s FROM df_in_src s JOIN df_in_dim d USING (b) GROUP BY d.name
  DISTRIBUTED BY (name);
SET vec.mode = on;
CREATE TABLE df_in_ctas_on AS
  SELECT b, count(*) n, sum(c) s, max(t) t FROM df_in_src WHERE e < 90 GROUP BY b DISTRIBUTED BY (b);
SELECT b, count(*) n, sum(c) s, max(t) t INTO df_in_into_on FROM df_in_src WHERE e < 90 GROUP BY b;
CREATE MATERIALIZED VIEW df_in_mv_on AS
  SELECT d.name, count(*) n, sum(s.c) s FROM df_in_src s JOIN df_in_dim d USING (b) GROUP BY d.name
  DISTRIBUTED BY (name);
CREATE TABLE df_in_nodata AS SELECT b, count(*) n FROM df_in_src GROUP BY b WITH NO DATA DISTRIBUTED BY (b);
SET vec.mode = off;
SELECT df_in_same('df_in_ctas_off', 'df_in_ctas_on');
SELECT df_in_same('df_in_ctas_off', 'df_in_into_on');
SELECT df_in_same('df_in_mv_off', 'df_in_mv_on');
SELECT count(*) FROM df_in_nodata;
INSERT INTO df_in_src SELECT a + 200000, b, c, e, t FROM df_in_src WHERE a <= 1000;
REFRESH MATERIALIZED VIEW df_in_mv_off;
SET vec.mode = on;
REFRESH MATERIALIZED VIEW df_in_mv_on;
SET vec.mode = off;
SELECT df_in_same('df_in_mv_off', 'df_in_mv_on');

-- An error in the engine's part of the statement inserts nothing; so does
-- one rolled back to a savepoint.
SET vec.mode = on;
SELECT count(*) FROM df_in_ret;
INSERT INTO df_in_ret SELECT a, 100 / (e - 50) FROM df_in_src;
SELECT count(*) FROM df_in_ret;
BEGIN;
INSERT INTO df_in_ret VALUES (-1, -1);
SAVEPOINT s;
INSERT INTO df_in_ret SELECT a, 100 / (e - 50) FROM df_in_src;
ROLLBACK TO SAVEPOINT s;
INSERT INTO df_in_ret SELECT e, count(*) FROM df_in_src WHERE e < 3 GROUP BY e;
COMMIT;
SELECT k, n FROM df_in_ret ORDER BY k;
CREATE TABLE df_in_bad AS SELECT a, 100 / (e - 50) q FROM df_in_src DISTRIBUTED BY (a);
SELECT count(*) FROM pg_class WHERE relname = 'df_in_bad';

-- A statement timeout while DataFusion runs the query (a join of rows
-- far too many to finish): nothing is inserted, and the next statement
-- runs.
SET statement_timeout = '1s';
INSERT INTO df_in_ret SELECT x.a, y.a FROM df_in_src x JOIN df_in_src y ON x.b + y.b > -1;
RESET statement_timeout;
SELECT count(*) FROM df_in_ret;
INSERT INTO df_in_ret SELECT e, count(*) FROM df_in_src WHERE e = 5 GROUP BY e;
SELECT count(*) FROM df_in_ret;

DROP MATERIALIZED VIEW df_in_mv_off, df_in_mv_on;
DROP TABLE df_in_src, df_in_dim, df_in_heap_off, df_in_heap_on, df_in_ao_off, df_in_ao_on,
  df_in_aocs_off, df_in_aocs_on, df_in_pax_off, df_in_pax_on, df_in_ret, df_in_part_off,
  df_in_part_on, df_in_ctas_off, df_in_ctas_on, df_in_into_on, df_in_nodata;
DROP FUNCTION df_in_same(regclass, regclass);
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION vec_executor;
