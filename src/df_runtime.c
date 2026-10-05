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
 * df_runtime.c
 *	  Lifecycle of the per-backend DataFusion runtime, and waiting on it.
 *
 * The runtime's threads never touch PostgreSQL and never receive signals.
 * The backend's main thread keeps every PostgreSQL duty: it waits for the
 * runtime in short slices and runs CHECK_FOR_INTERRUPTS between them, so a
 * cancel, a statement timeout or a terminate request is acted on within one
 * slice.
 *
 * src/df_runtime.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <pthread.h>
#include <signal.h>

#include "catalog/pg_tablespace_d.h"
#include "common/file_utils.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/vmem_tracker.h"

#include "df_executor.h"

/* How long one wait on the runtime lasts before interrupts are checked. */
#define DF_WAIT_SLICE_MS	10

/* After a cancel, how long to wait for the tasks to stop before giving up. */
#define DF_STOP_TIMEOUT_MS	10000

/* On backend exit, how long running tasks get to reach a yield point. */
#define DF_SHUTDOWN_TIMEOUT_MS	1000

int			df_worker_threads = 0;

/* The process that started the runtime, or 0 if none has. */
static pid_t df_runtime_pid = 0;
static int	df_runtime_workers = 0;

/* Bytes this backend has reserved from the vmem tracker for Rust's heap. */
static int64 df_vmem_leased = 0;

/* This backend's directory for DataFusion spill files, once created. */
static char df_spill_path[MAXPGPATH];

static void
df_runtime_atexit(int code, Datum arg)
{
	if (df_runtime_pid == MyProcPid)
	{
		df_ffi_runtime_shutdown(DF_SHUTDOWN_TIMEOUT_MS);
		df_runtime_pid = 0;
	}
	if (df_spill_path[0] != '\0')
	{
		(void) rmtree(df_spill_path, true);
		df_spill_path[0] = '\0';
	}
}

/*
 * Directory for this backend's DataFusion spill files, created on first use
 * in the default temporary-file directory.  Its name starts with the
 * temporary-file prefix, so the postmaster removes it with the other
 * temporary files after a crash; a normal backend exit removes it.
 */
const char *
df_spill_dir(void)
{
	char		parent[MAXPGPATH];

	if (df_spill_path[0] != '\0')
		return df_spill_path;

	TempTablespacePath(parent, DEFAULTTABLESPACE_OID);
	(void) MakePGDirectory(parent);	/* usually exists already */
	snprintf(df_spill_path, sizeof(df_spill_path), "%s/%s/%s_datafusion_%d",
			 DataDir, parent, PG_TEMP_FILE_PREFIX, (int) MyProcPid);
	if (MakePGDirectory(df_spill_path) < 0 && errno != EEXIST)
	{
		int			save_errno = errno;
		char		path[MAXPGPATH];

		strlcpy(path, df_spill_path, sizeof(path));
		df_spill_path[0] = '\0';
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", path)));
	}
	return df_spill_path;
}

/* Report a failed vmem reservation the way Cloudberry's allocator does. */
static void
df_vmem_failed(MemoryAllocationStatus status, int64 bytes)
{
	if (status == MemoryFailure_QueryMemoryExhausted)
		ereport(ERROR,
				(errcode(ERRCODE_GP_MEMPROT_KILL),
				 errmsg("Out of memory"),
				 errdetail("Per-query memory limit reached: current limit is %d kB, "
						   "requested " INT64_FORMAT " bytes, has %d MB available for this query",
						   gp_vmem_limit_per_query, bytes,
						   VmemTracker_GetAvailableQueryVmemMB())));
	if (status == MemoryFailure_VmemExhausted)
		ereport(ERROR,
				(errcode(ERRCODE_GP_MEMPROT_KILL),
				 errmsg("Out of memory"),
				 errdetail("Vmem limit reached, failed to allocate " INT64_FORMAT
						   " bytes from tracker, which has %d MB available",
						   bytes, VmemTracker_GetAvailableVmemMB())));
	ereport(ERROR,
			(errcode(ERRCODE_GP_MEMPROT_KILL),
			 errmsg("Out of memory"),
			 errdetail("Could not reserve " INT64_FORMAT " bytes of vmem for DataFusion.",
					   bytes)));
}

/*
 * Keep the vmem lease at least 'headroom' bytes above what Rust has
 * allocated in this process.  The runtime's threads allocate without asking
 * anyone, so the main thread leases ahead of them: it calls this between
 * waits, and the headroom covers what the threads allocate in between.
 * Fails the query, like an allocation would, when the tracker says no.
 */
void
df_vmem_sync(int64 headroom)
{
	int64		want = Max(df_ffi_heap_bytes(), 0) + headroom;

	if (want > df_vmem_leased)
	{
		int64		more = want - df_vmem_leased;
		MemoryAllocationStatus status = VmemTracker_ReserveVmem(more);

		if (status != MemoryAllocation_Success)
			df_vmem_failed(status, more);
		df_vmem_leased = want;
	}
}

/* Give back the part of the lease Rust no longer uses. */
void
df_vmem_trim(void)
{
	int64		want = Max(df_ffi_heap_bytes(), 0);

	if (want < df_vmem_leased)
	{
		VmemTracker_ReleaseVmem(df_vmem_leased - want);
		df_vmem_leased = want;
	}
}

int64
df_vmem_leased_bytes(void)
{
	return df_vmem_leased;
}

/*
 * Start the runtime in this backend if it is not running yet, and return its
 * number of worker threads.
 */
int
df_runtime_ensure(void)
{
	char		buf[DF_MSG_BUFLEN];
	sigset_t	all;
	sigset_t	saved;
	uint32_t	workers = 0;
	int32		status;
	int			rc;

	if (df_runtime_pid == MyProcPid)
		return df_runtime_workers;

	/*
	 * A runtime inherited across fork has no threads left.  This cannot
	 * happen as long as nothing starts the runtime in the postmaster, but
	 * using one would hang, so refuse loudly.
	 */
	if (df_runtime_pid != 0)
		elog(ERROR, "DataFusion runtime was started in process %d, not in this backend",
			 (int) df_runtime_pid);

	/*
	 * Threads inherit the signal mask of the thread that creates them.
	 * Block everything while the runtime starts its workers, so no signal
	 * meant for the backend can land on a worker.  Signals that arrive in
	 * the meantime stay pending and are delivered once the mask is restored.
	 */
	sigfillset(&all);
	rc = pthread_sigmask(SIG_BLOCK, &all, &saved);
	if (rc != 0)
		elog(ERROR, "pthread_sigmask failed: %s", strerror(rc));

	status = df_ffi_runtime_init((uint32_t) df_worker_threads, &workers,
								 buf, sizeof(buf));

	rc = pthread_sigmask(SIG_SETMASK, &saved, NULL);
	if (rc != 0)
		elog(FATAL, "pthread_sigmask failed to restore the signal mask: %s",
			 strerror(rc));

	if (status != DF_OK)
		df_raise(status, buf);

	df_runtime_pid = MyProcPid;
	df_runtime_workers = (int) workers;
	on_proc_exit(df_runtime_atexit, (Datum) 0);

	return df_runtime_workers;
}

/*
 * Cancel 'task', wait (without servicing interrupts) until it has stopped,
 * and free it.  Used while an error is propagating.
 */
static void
df_task_stop(DfTask *task)
{
	char		buf[DF_MSG_BUFLEN];
	int			waited;

	df_ffi_task_cancel(task);
	for (waited = 0; waited < DF_STOP_TIMEOUT_MS; waited += DF_WAIT_SLICE_MS)
	{
		if (df_ffi_task_wait(task, DF_WAIT_SLICE_MS, buf, sizeof(buf)) != DF_PENDING)
		{
			df_ffi_task_free(task);
			return;
		}
	}

	/*
	 * The tasks own everything they use, so freeing the handle is safe even
	 * though they are still running; they only keep burning CPU.
	 */
	elog(LOG, "DataFusion tasks did not stop within %d ms after cancel",
		 DF_STOP_TIMEOUT_MS);
	df_ffi_task_free(task);
}

/*
 * Wait for 'task' to finish while servicing interrupts, then free it.
 * Returns the final status (never DF_PENDING), with the result text or
 * error message in buf.
 *
 * If an interrupt raises an ERROR while waiting, the task is cancelled and
 * has stopped before the error propagates.  A FATAL (terminate) exits the
 * process directly; df_runtime_atexit then shuts the runtime down.
 */
int32
df_task_wait_interruptible(DfTask *task, char *buf, size_t buflen)
{
	volatile int32 status = DF_PENDING;

	PG_TRY();
	{
		for (;;)
		{
			status = df_ffi_task_wait(task, DF_WAIT_SLICE_MS, buf, buflen);
			if (status != DF_PENDING)
				break;
			CHECK_FOR_INTERRUPTS();
		}
	}
	PG_CATCH();
	{
		df_task_stop(task);
		PG_RE_THROW();
	}
	PG_END_TRY();

	df_ffi_task_free(task);
	return status;
}
