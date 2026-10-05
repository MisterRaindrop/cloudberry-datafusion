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

#include "access/htup_details.h"
#include "fmgr.h"
#include "funcapi.h"
#include "storage/fd.h"
#include "utils/builtins.h"

#include "df_executor.h"

/* Thread name the runtime gives its threads (df_core::runtime::THREAD_NAME). */
#define DF_THREAD_NAME "df-worker"

PG_FUNCTION_INFO_V1(datafusion_debug_spin);
PG_FUNCTION_INFO_V1(datafusion_debug_worker_panic);
PG_FUNCTION_INFO_V1(datafusion_debug_active_tasks);
PG_FUNCTION_INFO_V1(datafusion_debug_runtime_threads);

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
