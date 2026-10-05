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

#include "df_ffi.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

PG_FUNCTION_INFO_V1(datafusion_version);
PG_FUNCTION_INFO_V1(datafusion_debug_panic);

static void df_raise(int32 status, const char *msg) pg_attribute_noreturn();

void
_PG_init(void)
{
	/*
	 * Executor hooks and GUCs are installed here from milestone M2 on.
	 *
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
static void
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
