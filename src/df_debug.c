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
 * df_debug.c
 *	  SQL-callable test functions for the runtime milestone (M1).
 *
 * They run CPU-bound work on the runtime, panic inside a worker, and
 * report on the runtime's threads, so the regression tests can check
 * cancellation, panic handling and signal masks from SQL.
 *
 * src/df_debug.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <signal.h>

#include <math.h>

#include "access/htup_details.h"
#include "cdb/cdbhash.h"
#include "common/pg_prng.h"
#include "fmgr.h"
#include "funcapi.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

#include "df_executor.h"

/* Thread name the runtime gives its threads (df_core::runtime::THREAD_NAME). */
#define DF_THREAD_NAME "df-worker"

PG_FUNCTION_INFO_V1(datafusion_debug_spin);
PG_FUNCTION_INFO_V1(datafusion_debug_worker_panic);
PG_FUNCTION_INFO_V1(datafusion_debug_active_tasks);
PG_FUNCTION_INFO_V1(datafusion_debug_runtime_threads);
PG_FUNCTION_INFO_V1(datafusion_debug_last_run);
PG_FUNCTION_INFO_V1(datafusion_debug_vmem);
PG_FUNCTION_INFO_V1(datafusion_debug_vmem_lease);
PG_FUNCTION_INFO_V1(datafusion_debug_pax_direct_scans);
PG_FUNCTION_INFO_V1(datafusion_debug_last_pax);
PG_FUNCTION_INFO_V1(datafusion_debug_cdbhash_check);

static Datum
df_int8_record(FunctionCallInfo fcinfo, int n, const int64 *v)
{
	TupleDesc	tupdesc;
	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};
	int			i;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	for (i = 0; i < n; i++)
		values[i] = Int64GetDatum(v[i]);
	tupdesc = BlessTupleDesc(tupdesc);
	return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls));
}

/*
 * datafusion_debug_last_run(OUT partitions int8, OUT memory_limit_kb int8,
 *                           OUT memory_peak_kb int8, OUT spilled_kb int8,
 *                           OUT spills int8)
 */
Datum
datafusion_debug_last_run(PG_FUNCTION_ARGS)
{
	int64		v[5];

	v[0] = (int64) df_last_run.partitions;
	v[1] = (int64) (df_last_run.memory_limit / 1024);
	v[2] = (int64) ((df_last_run.memory_peak + 1023) / 1024);
	v[3] = (int64) ((df_last_run.spilled_bytes + 1023) / 1024);
	v[4] = (int64) df_last_run.spill_count;
	PG_RETURN_DATUM(df_int8_record(fcinfo, 5, v));
}

/* datafusion_debug_vmem(OUT heap_bytes int8, OUT leased_bytes int8) */
Datum
datafusion_debug_vmem(PG_FUNCTION_ARGS)
{
	int64		v[2];

	v[0] = df_ffi_heap_bytes();
	v[1] = df_vmem_leased_bytes();
	PG_RETURN_DATUM(df_int8_record(fcinfo, 2, v));
}

static text *
df_run_spin(float8 seconds, int32 ntasks, bool panic_in_worker)
{
	char		buf[DF_MSG_BUFLEN];
	DfTask	   *task = NULL;
	int32		status;

	if (ntasks < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ntasks must be at least 1")));

	df_runtime_ensure();

	status = df_ffi_debug_spin_start(seconds, (uint32_t) ntasks,
									 panic_in_worker, &task, buf, sizeof(buf));
	if (status != DF_OK)
		df_raise(status, buf);

	status = df_task_wait_interruptible(task, buf, sizeof(buf));
	if (status == DF_OK)
		return cstring_to_text(buf);
	if (status == DF_CANCELLED)
		elog(ERROR, "DataFusion tasks were cancelled");
	df_raise(status, buf);
}

/* datafusion_debug_spin(seconds float8, ntasks int) returns text */
Datum
datafusion_debug_spin(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(df_run_spin(PG_GETARG_FLOAT8(0), PG_GETARG_INT32(1), false));
}

/* datafusion_debug_worker_panic() returns text */
Datum
datafusion_debug_worker_panic(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(df_run_spin(10.0, 2, true));
}

/* datafusion_debug_active_tasks() returns bigint */
Datum
datafusion_debug_active_tasks(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64((int64) df_ffi_debug_active_tasks());
}

#ifdef __linux__
/* Read the first line of a small /proc file into buf; false if unreadable. */
static bool
df_read_proc_line(const char *path, const char *prefix, char *buf, size_t buflen)
{
	FILE	   *f;
	bool		found = false;

	f = AllocateFile(path, "r");
	if (f == NULL)
		return false;			/* the thread exited meanwhile */
	while (fgets(buf, (int) buflen, f) != NULL)
	{
		if (prefix == NULL || strncmp(buf, prefix, strlen(prefix)) == 0)
		{
			buf[strcspn(buf, "\n")] = '\0';
			found = true;
			break;
		}
	}
	FreeFile(f);
	return found;
}
#endif

/*
 * datafusion_debug_runtime_threads(OUT threads int, OUT signals_blocked bool)
 *
 * Counts this backend's runtime threads and checks that each blocks the
 * signals PostgreSQL relies on.  Linux only (reads /proc/self/task).
 */
Datum
datafusion_debug_runtime_threads(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	int32		threads = 0;
	bool		all_blocked = true;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

#ifdef __linux__
	{
		const int	sigs[] = {SIGHUP, SIGINT, SIGQUIT, SIGTERM,
		SIGUSR1, SIGUSR2, SIGALRM};
		uint64		needed = 0;
		DIR		   *dir;
		struct dirent *de;
		int			i;

		for (i = 0; i < lengthof(sigs); i++)
			needed |= UINT64CONST(1) << (sigs[i] - 1);

		dir = AllocateDir("/proc/self/task");
		while ((de = ReadDir(dir, "/proc/self/task")) != NULL)
		{
			char		path[MAXPGPATH];
			char		line[256];
			uint64		blocked;

			if (de->d_name[0] == '.')
				continue;

			snprintf(path, sizeof(path), "/proc/self/task/%s/comm", de->d_name);
			if (!df_read_proc_line(path, NULL, line, sizeof(line)) ||
				strcmp(line, DF_THREAD_NAME) != 0)
				continue;

			snprintf(path, sizeof(path), "/proc/self/task/%s/status", de->d_name);
			if (!df_read_proc_line(path, "SigBlk:", line, sizeof(line)))
				continue;

			threads++;
			blocked = strtoull(line + strlen("SigBlk:"), NULL, 16);
			if ((blocked & needed) != needed)
				all_blocked = false;
		}
		FreeDir(dir);
	}
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("datafusion_debug_runtime_threads() needs /proc (Linux)")));
#endif

	values[0] = Int32GetDatum(threads);
	values[1] = BoolGetDatum(all_blocked);
	tupdesc = BlessTupleDesc(tupdesc);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * datafusion_debug_vmem_lease(headroom int8) returns int8
 *
 * Run the lease step a query runs between waits, with the given headroom,
 * and return the bytes leased.  Lets tests reach the vmem tracker's limits,
 * which Cloudberry enforces on QEs only.
 */
Datum
datafusion_debug_vmem_lease(PG_FUNCTION_ARGS)
{
	int64		headroom = PG_GETARG_INT64(0);
	int64		leased;

	if (headroom < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("headroom must not be negative")));
	df_vmem_sync(headroom);
	leased = df_vmem_leased_bytes();
	df_vmem_trim();
	PG_RETURN_INT64(leased);
}

/* datafusion_debug_pax_direct_scans() returns bigint */
Datum
datafusion_debug_pax_direct_scans(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64((int64) df_pax_direct_scans);
}

/*
 * datafusion_debug_last_pax(OUT files int8, OUT files_skipped int8,
 *                           OUT groups int8, OUT groups_skipped int8,
 *                           OUT decode_peak_kb int8)
 * The min/max skipping and the decoding memory of the latest completed run,
 * if it read PAX directly; NULLs otherwise.
 */
Datum
datafusion_debug_last_pax(PG_FUNCTION_ARGS)
{
	int64		v[5];

	if (!df_last_run_pax)
	{
		TupleDesc	tupdesc;
		Datum		values[5] = {0};
		bool		nulls[5] = {true, true, true, true, true};

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");
		tupdesc = BlessTupleDesc(tupdesc);
		PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
	}
	v[0] = df_last_pax_scan.files;
	v[1] = df_last_pax_scan.files_skipped;
	v[2] = df_last_pax_scan.groups;
	v[3] = df_last_pax_scan.groups_skipped;
	v[4] = (int64) ((df_last_run.pax_decode_peak + 1023) / 1024);
	PG_RETURN_DATUM(df_int8_record(fcinfo, 5, v));
}

/* ---------------------------------------------------------------------
 * datafusion_debug_cdbhash_check(nrows int4, segments int4, workers int4)
 * returns bigint
 *
 * Route 'nrows' rows of random keys, with the special values mixed in, by
 * cdbhash() and by the Rust transcription (df_core::cdbhash), for every
 * supported key type alone and for two multi-key combinations, and return
 * the number of rows the two route differently.  With workers >= 2 the
 * route includes the worker, as nodeMotion.c computes it in parallel mode.
 * ---------------------------------------------------------------------
 */
typedef struct DfKeyType
{
	int32		kind;			/* as df_ffi_cdbhash_routes numbers it */
	Oid			hashfunc;
	int			width;
} DfKeyType;

static const DfKeyType df_key_types[] = {
	{0, F_HASHCHAR, 1}, {1, F_HASHINT2, 2}, {2, F_HASHINT4, 4},
	{3, F_HASHINT8, 8}, {4, F_HASHFLOAT4, 4}, {5, F_HASHFLOAT8, 8},
};

/* A random value of key type 'kind', a special value one time in four. */
static void
df_random_key(pg_prng_state *rng, int32 kind, void *dst)
{
	static const double fspecial[] = {0.0, -0.0, NAN, -NAN, INFINITY, -INFINITY, 1.5, -1e300};
	static const int64 ispecial[] = {0, -1, 1, PG_INT64_MIN, PG_INT64_MAX,
	PG_INT32_MIN, PG_INT32_MAX, 4294967296LL};
	bool		special = pg_prng_uint64(rng) % 4 == 0;
	uint64		r = pg_prng_uint64(rng);
	int			s = r % 8;

	switch (kind)
	{
		case 0:
			*(uint8 *) dst = r & 1;
			break;
		case 1:
			*(int16 *) dst = special ? (int16) ispecial[s] : (int16) r;
			break;
		case 2:
			*(int32 *) dst = special ? (int32) ispecial[s] : (int32) r;
			break;
		case 3:
			*(int64 *) dst = special ? ispecial[s] : (int64) r;
			break;
		case 4:
			*(float4 *) dst = special ? (float4) fspecial[s] :
				(float4) ((pg_prng_double(rng) - 0.5) * 1e6);
			break;
		case 5:
			*(float8 *) dst = special ? fspecial[s] : (pg_prng_double(rng) - 0.5) * 1e12;
			break;
	}
}

static Datum
df_key_datum(int32 kind, const void *v)
{
	switch (kind)
	{
		case 0:
			return BoolGetDatum(*(const uint8 *) v != 0);
		case 1:
			return Int16GetDatum(*(const int16 *) v);
		case 2:
			return Int32GetDatum(*(const int32 *) v);
		case 3:
			return Int64GetDatum(*(const int64 *) v);
		case 4:
			return Float4GetDatum(*(const float4 *) v);
		default:
			return Float8GetDatum(*(const float8 *) v);
	}
}

/* Mismatching rows for one combination of key types. */
static int64
df_cdbhash_check_keys(pg_prng_state *rng, const int *types, int nkeys,
					  int nrows, int segments, int workers)
{
	char	   *values[3];
	uint8	   *nulls[3];
	DfColumn	cols[3];
	int32		kinds[3];
	Oid			funcs[3];
	uint32	   *routes = palloc(sizeof(uint32) * nrows);
	CdbHash    *h;
	CdbHash    *hw;
	int64		bad = 0;
	int			k,
				r;

	for (k = 0; k < nkeys; k++)
	{
		const DfKeyType *t = &df_key_types[types[k]];

		kinds[k] = t->kind;
		funcs[k] = t->hashfunc;
		values[k] = palloc(t->width * nrows);
		nulls[k] = palloc(nrows);
		for (r = 0; r < nrows; r++)
		{
			nulls[k][r] = pg_prng_uint64(rng) % 10 == 0;
			df_random_key(rng, t->kind, values[k] + r * t->width);
		}
		cols[k].values = (const void *) values[k];
		cols[k].nulls = nulls[k];
		cols[k].offsets = NULL;
	}
	h = makeCdbHash(segments, nkeys, funcs);
	hw = makeCdbHash(segments * Max(workers, 1), nkeys, funcs);

	if (df_ffi_cdbhash_routes(kinds, cols, nkeys, nrows, segments, workers, routes) != DF_OK)
		elog(ERROR, "datafusion: df_ffi_cdbhash_routes failed");

	for (r = 0; r < nrows; r++)
	{
		uint32		seg,
					route;

		cdbhashinit(h);
		cdbhashinit(hw);
		for (k = 0; k < nkeys; k++)
		{
			Datum		d = df_key_datum(kinds[k],
										 values[k] + r * df_key_types[types[k]].width);

			cdbhash(h, k + 1, d, nulls[k][r] != 0);
			cdbhash(hw, k + 1, d, nulls[k][r] != 0);
		}
		seg = cdbhashreduce(h);
		route = workers >= 2 ? seg * workers + cdbhashreduce(hw) / segments : seg;
		if (route != routes[r])
			bad++;
	}
	return bad;
}

Datum
datafusion_debug_cdbhash_check(PG_FUNCTION_ARGS)
{
	static const int combos[][4] = {
		/* nkeys, key types (indexes into df_key_types) */
		{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5},
		{2, 2, 5}, {3, 3, 1, 0},
	};
	int32		nrows = PG_GETARG_INT32(0);
	int32		segments = PG_GETARG_INT32(1);
	int32		workers = PG_GETARG_INT32(2);
	pg_prng_state rng;
	int64		bad = 0;
	int			i;

	if (nrows < 1 || segments < 1 || workers < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("nrows, segments and workers must be positive")));
	pg_prng_seed(&rng, 20261005);
	for (i = 0; i < lengthof(combos); i++)
		bad += df_cdbhash_check_keys(&rng, &combos[i][1], combos[i][0],
									 nrows, segments, workers);
	PG_RETURN_INT64(bad);
}
