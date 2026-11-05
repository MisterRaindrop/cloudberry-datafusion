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
-- The per-backend DataFusion runtime (M1): lazy start, fixed size, signal
-- masks, cancellation, and panics inside worker threads.
--
CREATE EXTENSION vec_executor;
SET vec.worker_threads = 2;

-- Nothing runs until the first use.
SELECT * FROM vec_debug_runtime_threads();
SELECT vec_debug_spin(0.05, 4);
SELECT * FROM vec_debug_runtime_threads();

-- The size is fixed once the runtime has started.
SET vec.worker_threads = 3;
SELECT vec_debug_spin(0.01, 1);
SELECT threads FROM vec_debug_runtime_threads();
SET vec.worker_threads = 2;

-- A statement timeout cancels the work, and the tasks have stopped by the
-- time the error is reported.
SET statement_timeout = '200ms';
SELECT vec_debug_spin(30, 4);
RESET statement_timeout;
SELECT vec_debug_active_tasks();

-- A panic in a worker thread is an ordinary ERROR; the runtime keeps working.
SELECT vec_debug_worker_panic();
SELECT vec_debug_active_tasks();
SELECT vec_debug_spin(0.01, 2);

-- The same on the segments' QE processes.
SELECT gp_segment_id, vec_debug_spin(0.02, 2)
FROM gp_dist_random('gp_id') ORDER BY 1;
SELECT gp_segment_id, (vec_debug_runtime_threads()).*
FROM gp_dist_random('gp_id') ORDER BY 1;
SET statement_timeout = '300ms';
SELECT vec_debug_spin(30, 2) FROM gp_dist_random('gp_id');
RESET statement_timeout;

DROP EXTENSION vec_executor;
