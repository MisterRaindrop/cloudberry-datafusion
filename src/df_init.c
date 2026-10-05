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
 * df_init.c
 *	  Module entry point and SQL-callable functions of datafusion_executor.
 *
 * src/df_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#include "df_executor.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

PG_FUNCTION_INFO_V1(datafusion_version);
PG_FUNCTION_INFO_V1(datafusion_debug_panic);

static const struct config_enum_entry df_mode_options[] = {
	{"off", DF_MODE_OFF, false},
	{"explain", DF_MODE_EXPLAIN, false},
	{"on", DF_MODE_ON, false},
	{NULL, 0, false}
};

void
_PG_init(void)
{
	DefineCustomEnumVariable("datafusion.mode",
							 "Whether DataFusion runs the slices it supports.",
							 "off: never.  explain: EXPLAIN reports which slices "
							 "qualify, but nothing runs in DataFusion.  on: "
							 "qualifying slices run in DataFusion.",
							 &df_mode,
							 DF_MODE_OFF,
							 df_mode_options,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("datafusion.worker_threads",
							"Number of DataFusion worker threads per backend.",
							"0 means one per CPU.  Takes effect when a backend "
							"first uses DataFusion; changing it later does not "
							"resize a running backend's runtime.",
							&df_worker_threads,
							0, 0, 1024,
							PGC_SUSET,
							GUC_GPDB_NEED_SYNC,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("datafusion.pax_direct_read",
							 "Experimental: read PAX tables column by column on DataFusion's threads.",
							 "Bypasses the table access method's row-at-a-time interface "
							 "by calling PAX's reader classes directly (datafusion_pax.so).  "
							 "Not used in Cloudberry's parallel mode.",
							 &df_pax_direct_read,
							 false,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("datafusion.motion_batches",
							 "Experimental: send Arrow batches through Gather Motions between DataFusion slices.",
							 "When both the sending and the receiving slice of a Gather Motion run "
							 "in DataFusion, the rows travel over the interconnect as Arrow IPC "
							 "batches instead of one tuple at a time.",
							 &df_motion_batches,
							 false,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("datafusion");

	df_install_hooks();

	/*
	 * Never start threads here.  When the library is listed in
	 * shared_preload_libraries this runs in the postmaster, and every
	 * backend would inherit the threads' memory without the threads.  The
	 * Tokio runtime is created lazily inside a backend instead.
	 */
}

/*
 * Turn a failed FFI call into a PostgreSQL error.  Called only after the
 * Rust frames have returned.
 */
void
df_raise(int32 status, const char *msg)
{
	if (status == DF_PANIC)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("datafusion panicked: %s", msg)));

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("datafusion error: %s", msg)));
}

Datum
datafusion_version(PG_FUNCTION_ARGS)
{
	char		buf[DF_MSG_BUFLEN];
	int32		status;

	status = df_ffi_version(buf, sizeof(buf));
	if (status != DF_OK)
		df_raise(status, buf);

	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

Datum
datafusion_debug_panic(PG_FUNCTION_ARGS)
{
	char		buf[DF_MSG_BUFLEN];
	int32		status;

	status = df_ffi_debug_panic(buf, sizeof(buf));
	if (status != DF_OK)
		df_raise(status, buf);

	PG_RETURN_VOID();
}
