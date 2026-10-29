/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * vec_engine_null.c
 *	  An engine that runs nothing: every feature off.
 *
 * It shows that the ABI of vec_engine.h is not DataFusion's alone, and lets
 * the regression tests check that the host loads an engine of its own,
 * explains why each slice stays on PostgreSQL, and gives PostgreSQL's
 * results.  Built with VEC_NULL_ABI it claims another ABI version, for the
 * test of that refusal.
 *
 * engines/null/vec_engine_null.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <string.h>

#include "vec_engine.h"

#ifndef VEC_NULL_ABI
#define VEC_NULL_ABI VEC_ENGINE_ABI
#endif

#define NULL_EXPORT __attribute__((visibility("default")))

static void
null_fail(VecError *err)
{
	memcpy(err->sqlstate, "0A000", 6);
	snprintf(err->message, sizeof(err->message), "the null engine runs no queries");
}

static int32_t
null_runtime_init(uint32_t workers, uint32_t *out_workers, VecError *err)
{
	*out_workers = 1;
	return VEC_OK;
}

static void
null_runtime_shutdown(uint32_t timeout_ms)
{
}

static int64_t
null_heap_bytes(void)
{
	return 0;
}

static void
null_set_case_maps(const uint32_t *upper, size_t nupper,
				   const uint32_t *lower, size_t nlower)
{
}

static int32_t
null_query_start(const VecQueryArgs *args, struct ArrowSchema *out_schema,
				 VecQuery **out_query, VecError *err)
{
	null_fail(err);
	return VEC_ERROR;
}

static int32_t
null_query_push(VecQuery *query, uint32_t input, const struct ArrowArray *batch,
				VecError *err)
{
	null_fail(err);
	return VEC_ERROR;
}

static int32_t
null_query_push_ipc(VecQuery *query, uint32_t input, int32_t route,
					const VecSlice *parts, uint32_t nparts, VecError *err)
{
	null_fail(err);
	return VEC_ERROR;
}

static void
null_query_finish_input(VecQuery *query, uint32_t input)
{
}

static int32_t
null_query_poll(VecQuery *query, uint32_t timeout_ms, uint32_t *nrows, VecError *err)
{
	null_fail(err);
	return VEC_ERROR;
}

static int32_t
null_query_result(VecQuery *query, struct ArrowArray *out)
{
	return VEC_ERROR;
}

static void
null_query_bytes(VecQuery *query, int32_t *route, const uint8_t **data, size_t *len)
{
	*route = -1;
	*data = NULL;
	*len = 0;
}

static void
null_query_stats(VecQuery *query, VecQueryStats *out)
{
	memset(out, 0, sizeof(*out));
}

static void
null_query_free(VecQuery *query)
{
}

static const VecEngine null_engine = {
	.abi = VEC_NULL_ABI,
	.name = "null",
	.version = "null 1",
	.caps = {.ir_min = VEC_IR_VERSION, .ir_max = VEC_IR_VERSION},
	.runtime_init = null_runtime_init,
	.runtime_shutdown = null_runtime_shutdown,
	.heap_bytes = null_heap_bytes,
	.set_case_maps = null_set_case_maps,
	.query_start = null_query_start,
	.query_push = null_query_push,
	.query_push_ipc = null_query_push_ipc,
	.query_finish_input = null_query_finish_input,
	.query_poll = null_query_poll,
	.query_result = null_query_result,
	.query_bytes = null_query_bytes,
	.query_stats = null_query_stats,
	.query_free = null_query_free,
	.debug = NULL,
};

NULL_EXPORT const VecEngine *
vec_engine_entry(uint32_t host_abi, const VecHost *host)
{
	return &null_engine;
}
