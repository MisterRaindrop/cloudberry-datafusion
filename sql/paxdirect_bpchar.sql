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
-- PV1, the direct PAX reader and char(n) of PAX's vectorized format, apart
-- from paxdirect: writing these values fails an assertion of PAX's
-- (pax_vec_bpchar_column.cc, size == number_of_char_), so a Cloudberry
-- built with --enable-cassert does not run this test (Makefile).
--
CREATE EXTENSION vec_executor;
ALTER DATABASE contrib_regression SET session_preload_libraries = 'vec_executor';
\c
SET optimizer = off;
SET vec.worker_threads = 2;

-- PV1: char(n) of the vectorized format.  PAX stores each value without
-- its trailing blanks and records as n the bytes of the first value of a
-- group, so it pads multibyte values to the wrong length (equal values of
-- different groups even differ).  Both the direct reader and the table AM
-- path pad to n characters, as the heap table holds them.  PostgreSQL
-- reads PAX's padding, so the engine check leaves those results out.
SET pax.max_tuples_per_group = 7;
CREATE TABLE df_pd_vbh (id int, t text, b char(4)) DISTRIBUTED BY (id);
INSERT INTO df_pd_vbh VALUES (1, 'ab', 'ab'), (2, '中文', '中文'), (3, '', ''),
  (4, NULL, NULL), (5, '中文字符四', '中文字符'), (6, 'x', 'a'), (7, repeat('ab', 300000), 'abcd'),
  (8, 'é', 'é'), (9, 'ab ', 'ab  '), (10, repeat('中', 5000), '中');
INSERT INTO df_pd_vbh SELECT id + 100, t, b FROM df_pd_vbh;
CREATE TABLE df_pd_vb (LIKE df_pd_vbh) USING pax WITH (storage_format = porc_vec) DISTRIBUTED BY (id);
INSERT INTO df_pd_vb SELECT * FROM df_pd_vbh;
RESET pax.max_tuples_per_group;
SET vec.mode = off;
SET vec.pax_direct_read = off;
SELECT id, length(t), octet_length(t), b, octet_length(b), length(b) FROM df_pd_vbh ORDER BY id;
SELECT b, count(*) FROM df_pd_vbh GROUP BY b ORDER BY b COLLATE "C";
SET vec.mode = on;
SET vec.pax_direct_read = on;
SELECT id, length(t), octet_length(t), b, octet_length(b), length(b) FROM df_pd_vb ORDER BY id /* differs on PostgreSQL: PV1 */;
SELECT b, count(*) FROM df_pd_vb GROUP BY b ORDER BY b COLLATE "C" /* differs on PostgreSQL: PV1 */;
SET vec.pax_direct_read = off;
SELECT id, length(t), octet_length(t), b, octet_length(b), length(b) FROM df_pd_vb ORDER BY id /* differs on PostgreSQL: PV1 */;
SELECT b, count(*) FROM df_pd_vb GROUP BY b ORDER BY b COLLATE "C" /* differs on PostgreSQL: PV1 */;

DROP TABLE df_pd_vbh, df_pd_vb;
ALTER DATABASE contrib_regression RESET session_preload_libraries;
DROP EXTENSION vec_executor;
