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

#include <dlfcn.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#include "df_executor.h"

PG_MODULE_MAGIC;

char	   *df_engine_name = NULL;

/* The engine slices run in, once loaded: the one datafusion.engine names. */
const VecEngine *df_engine = NULL;

/* What datafusion.engine said when df_engine was loaded. */
static char df_engine_loaded[NAMEDATALEN];

static const VecHost df_host = {VEC_ENGINE_ABI};

static bool df_check_engine(char **newval, void **extra, GucSource source);

void		_PG_init(void);

PG_FUNCTION_INFO_V1(datafusion_version);
PG_FUNCTION_INFO_V1(datafusion_debug_panic);

static const struct config_enum_entry df_mode_options[] = {
	{"off", DF_MODE_OFF, false},
	{"explain", DF_MODE_EXPLAIN, false},
	{"on", DF_MODE_ON, false},
	{NULL, 0, false}
};

static const struct config_enum_entry df_join_estimates_options[] = {
	{"spill", DF_JOIN_ESTIMATES_SPILL, false},
	{"trusted", DF_JOIN_ESTIMATES_TRUSTED, false},
	{"bounded", DF_JOIN_ESTIMATES_BOUNDED, false},
	{"strict", DF_JOIN_ESTIMATES_STRICT, false},
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

	DefineCustomEnumVariable("datafusion.join_estimates",
							 "How a hash join's build side size is checked.",
							 "spill: not at all, the join spills what does not fit "
							 "(except a NOT IN anti join, checked as with trusted).  With "
							 "DataFusion's own hash join, which cannot spill, a slice "
							 "whose build side may outgrow its budget stays on "
							 "PostgreSQL.  trusted: by the planner's estimate.  bounded: "
							 "a build side coming from a join may hold as many rows as "
							 "its largest input.  strict: a build side coming from a "
							 "join stays on PostgreSQL.",
							 &df_join_estimates,
							 DF_JOIN_ESTIMATES_SPILL,
							 df_join_estimates_options,
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
							 "Reads PAX tables column by column on DataFusion's threads.",
							 "Bypasses the table access method's row-at-a-time interface "
							 "by calling PAX's reader classes directly (datafusion_pax.so), "
							 "when installed and built for the running pax.so.  Not used in "
							 "Cloudberry's parallel mode.",
							 &df_pax_direct_read,
							 true,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("datafusion.pax_max_decoders",
							"Most PAX groups the direct reader decodes at once per query.",
							"Each holds a decoded group, about 30 MB of a QE's memory on "
							"TPC-H's lineitem; fewer save memory and cost time, since "
							"decoding is most of a scan's work.  0 decodes one group per "
							"partition.",
							&df_pax_max_decoders,
							0, 0, 1024,
							PGC_USERSET,
							GUC_GPDB_NEED_SYNC,
							NULL, NULL, NULL);

	DefineCustomIntVariable("datafusion.slice_partitions",
							"Partitions each DataFusion query of a slice runs in.",
							"0 picks them by the slices running together and the "
							"primaries on the host: eight times the worker "
							"threads over the product, at least 1.  At most the "
							"worker threads.",
							&df_slice_partitions,
							0, 0, 1024,
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

	DefineCustomBoolVariable("datafusion.cluster_collation_c",
							 "Whether the database's default collation is C on every node.",
							 "Set by the extension on the coordinator, which asks every "
							 "segment, and sent to them: ordering strings by the default "
							 "collation then runs in DataFusion where batch Motions are "
							 "chosen alike on every node.",
							 &df_cluster_collation_c,
							 false,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE |
							 GUC_DISALLOW_IN_FILE,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("datafusion.engine",
							   "The execution engine slices run in.",
							   "The library $libdir/vec_engine_<name>.so.  A backend "
							   "opens it when it first needs an engine and keeps it; "
							   "from then on the setting cannot change.",
							   &df_engine_name,
							   "datafusion",
							   PGC_SUSET,
							   GUC_GPDB_NEED_SYNC,
							   df_check_engine, NULL, NULL);

	MarkGUCPrefixReserved("datafusion");

	df_install_hooks();

	/*
	 * Never open the engine or start threads here.  When the library is
	 * listed in shared_preload_libraries this runs in the postmaster, and
	 * every backend would inherit the threads' memory without the threads.
	 * A backend opens the engine, and starts its runtime, on first use.
	 */
}

/*
 * datafusion.engine names a library in $libdir, and stays as it was once a
 * backend has loaded one.
 */
static bool
df_check_engine(char **newval, void **extra, GucSource source)
{
	const char *p;

	if (**newval == '\0' || strlen(*newval) >= NAMEDATALEN)
	{
		GUC_check_errdetail("An engine name has 1 to %d characters.", NAMEDATALEN - 1);
		return false;
	}
	for (p = *newval; *p != '\0'; p++)
	{
		if (!((*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') || *p == '_'))
		{
			GUC_check_errdetail("An engine name consists of lowercase letters, digits and underscores.");
			return false;
		}
	}
	if (df_engine != NULL && strcmp(*newval, df_engine_loaded) != 0)
	{
		GUC_check_errdetail("This session already runs engine \"%s\".", df_engine_loaded);
		return false;
	}
	return true;
}

/*
 * The engine, opened on first use: $libdir/vec_engine_<datafusion.engine>,
 * whose vec_engine_entry must give a table of this ABI.  The library stays
 * loaded, and the engine the same, for the rest of the backend's life.
 * RTLD_LOCAL keeps its symbols, and the libraries it brings, to itself.
 */
const VecEngine *
df_engine_load(void)
{
	char		path[MAXPGPATH];
	sigset_t	saved;
	void	   *handle;
	VecEngineEntry entry = NULL;
	const VecEngine *engine = NULL;
	const char *dlerr = NULL;

	if (df_engine != NULL)
		return df_engine;

	snprintf(path, sizeof(path), "%s/vec_engine_%s.so", pkglib_path, df_engine_name);

	/* Its constructors, or the entry, may start threads (vec_engine.h). */
	df_signals_block(&saved);
	handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
	if (handle == NULL)
		dlerr = dlerror();
	else
	{
		entry = (VecEngineEntry) dlsym(handle, "vec_engine_entry");
		if (entry != NULL)
			engine = entry(VEC_ENGINE_ABI, &df_host);
	}
	df_signals_restore(&saved);

	if (handle == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FILE),
				 errmsg("datafusion: could not load engine \"%s\"", df_engine_name),
				 errdetail("%s", dlerr)));
	if (entry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("datafusion: engine \"%s\" does not export vec_engine_entry",
						df_engine_name),
				 errdetail("Library: \"%s\".", path)));
	if (engine == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("datafusion: engine \"%s\" gave no table of functions",
						df_engine_name)));
	if (engine->abi != VEC_ENGINE_ABI)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("datafusion: engine \"%s\" speaks ABI version %u, this extension %d",
						df_engine_name, engine->abi, VEC_ENGINE_ABI),
				 errhint("Install the engine built with this extension.")));

	strlcpy(df_engine_loaded, df_engine_name, sizeof(df_engine_loaded));
	df_engine = engine;
	return df_engine;
}

/*
 * Turn a failed FFI call into a PostgreSQL error.  Called only after the
 * Rust frames have returned.
 */
void
df_raise(int32 status, const char *msg)
{
	if (status == VEC_PANIC)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("datafusion panicked: %s", msg)));

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("datafusion error: %s", msg)));
}

/* The engine's test functions, or an error if it has none. */
const VecEngineDebug *
df_engine_debug(void)
{
	const VecEngine *engine = df_engine_load();

	if (engine->debug == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("datafusion: engine %s has no test functions", engine->name)));
	return engine->debug;
}

Datum
datafusion_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(df_engine_load()->version));
}

Datum
datafusion_debug_panic(PG_FUNCTION_ARGS)
{
	VecError	err;
	int32		status;

	DF_ERROR_INIT(err);
	status = df_engine_debug()->panic(&err);
	if (status != VEC_OK)
		df_raise(status, err.message);

	PG_RETURN_VOID();
}
