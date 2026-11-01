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
-- SQ1: a correlated aggregate subquery of the WHERE clause matched on a key
-- of an outer relation gets "key IN (SELECT key FROM relation WHERE <its
-- conditions in the outer query>)" before planning, with datafusion.mode
-- on (the planner hook), so that only the outer query's keys are
-- aggregated.  Each query runs with datafusion.mode off, then on; the
-- results must match.
--
CREATE EXTENSION datafusion_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'datafusion_executor';
\c
SET optimizer = off;
SET datafusion.worker_threads = 2;
CREATE TABLE df_sq_part (p_key int, p_brand text, p_size int) DISTRIBUTED BY (p_key);
CREATE TABLE df_sq_line (l_key int, l_qty numeric(12,2), l_price numeric(12,2)) DISTRIBUTED BY (l_key);
INSERT INTO df_sq_part SELECT k, 'Brand#' || (k % 25), k % 50 FROM generate_series(1, 2000) k;
INSERT INTO df_sq_line SELECT k % 2100, (k * 7919) % 97 + 1, (k * 13) % 1000 + 0.25 FROM generate_series(1, 60000) k;
INSERT INTO df_sq_line VALUES (NULL, 1, 1), (5, NULL, 2);
ANALYZE df_sq_part, df_sq_line;

-- Q17's shape: the subquery gets the IN, planned as a semi join below its
-- aggregate
SET datafusion.mode = on;
EXPLAIN (COSTS OFF)
SELECT sum(l_price) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND p_brand = 'Brand#7' AND p_size < 30
  AND l_qty < (SELECT 0.5 * avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
SET datafusion.mode = off;
SELECT sum(l_price) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND p_brand = 'Brand#7' AND p_size < 30
  AND l_qty < (SELECT 0.5 * avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
SET datafusion.mode = on;
SELECT sum(l_price) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND p_brand = 'Brand#7' AND p_size < 30
  AND l_qty < (SELECT 0.5 * avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
-- the same with ORCA
SET optimizer = on;
SET datafusion.mode = off;
SELECT sum(l_price) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND p_brand = 'Brand#7' AND p_size < 30
  AND l_qty < (SELECT 0.5 * avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
SET datafusion.mode = on;
SELECT sum(l_price) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND p_brand = 'Brand#7' AND p_size < 30
  AND l_qty < (SELECT 0.5 * avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
SET optimizer = off;

-- Q20's shape: the outer relation's condition is itself an uncorrelated
-- subquery, carried into the IN; the subquery has a condition of its own
-- and the correlation its outer side first; the outer query is a subquery
SET datafusion.mode = on;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM df_sq_part
WHERE p_key IN (SELECT l_key FROM df_sq_line o
                WHERE o.l_key IN (SELECT p_key FROM df_sq_part WHERE p_brand LIKE 'Brand#1%')
                  AND o.l_price > (SELECT 0.8 * avg(l2.l_price) FROM df_sq_line l2
                                   WHERE l2.l_price > 1 AND o.l_key = l2.l_key));
SET datafusion.mode = off;
SELECT count(*) FROM df_sq_part
WHERE p_key IN (SELECT l_key FROM df_sq_line o
                WHERE o.l_key IN (SELECT p_key FROM df_sq_part WHERE p_brand LIKE 'Brand#1%')
                  AND o.l_price > (SELECT 0.8 * avg(l2.l_price) FROM df_sq_line l2
                                   WHERE l2.l_price > 1 AND o.l_key = l2.l_key));
SET datafusion.mode = on;
SELECT count(*) FROM df_sq_part
WHERE p_key IN (SELECT l_key FROM df_sq_line o
                WHERE o.l_key IN (SELECT p_key FROM df_sq_part WHERE p_brand LIKE 'Brand#1%')
                  AND o.l_price > (SELECT 0.8 * avg(l2.l_price) FROM df_sq_line l2
                                   WHERE l2.l_price > 1 AND o.l_key = l2.l_key));

-- Left alone: the outer relation's conditions under OR with another
-- relation's, or volatile; the relation under an outer join; a subquery
-- with GROUP BY; datafusion.subquery_key_filter off.  None gets an IN.
SET datafusion.mode = on;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND (p_size < 3 OR l_price < 10)
  AND l_qty < (SELECT avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
EXPLAIN (COSTS OFF)
SELECT count(*) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND p_size < random() * 3
  AND l_qty < (SELECT avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
EXPLAIN (COSTS OFF)
SELECT count(*) FROM df_sq_line LEFT JOIN df_sq_part ON p_key = l_key
WHERE p_size < 3
  AND l_qty < (SELECT avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
EXPLAIN (COSTS OFF)
SELECT count(*) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND p_size < 3
  AND l_qty < ALL (SELECT avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key GROUP BY l2.l_price);
SET datafusion.subquery_key_filter = off;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND p_size < 3
  AND l_qty < (SELECT avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
RESET datafusion.subquery_key_filter;
-- and their results do not change
SET datafusion.mode = off;
SELECT count(*) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND (p_size < 3 OR l_price < 10)
  AND l_qty < (SELECT avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
SELECT count(*) FROM df_sq_line LEFT JOIN df_sq_part ON p_key = l_key
WHERE p_size < 3
  AND l_qty < (SELECT avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
SET datafusion.mode = on;
SELECT count(*) FROM df_sq_line, df_sq_part
WHERE p_key = l_key AND (p_size < 3 OR l_price < 10)
  AND l_qty < (SELECT avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);
SELECT count(*) FROM df_sq_line LEFT JOIN df_sq_part ON p_key = l_key
WHERE p_size < 3
  AND l_qty < (SELECT avg(l_qty) FROM df_sq_line l2 WHERE l2.l_key = p_key);

DROP TABLE df_sq_part, df_sq_line;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION datafusion_executor;
