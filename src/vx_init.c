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
 * vx_init.c
 *	  Module entry point and SQL-callable functions of vec_executor.
 *
 * src/vx_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dlfcn.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#include "vx_executor.h"

PG_MODULE_MAGIC;

char	   *vx_engine_name = NULL;
int			vx_debug_features_off = 0;
int			vx_debug_result_offset = 0;

/* The engine slices run in, once loaded: the one vec.engine names. */
const VecEngine *vx_engine = NULL;

/* What vec.engine said when vx_engine was loaded. */
static char vx_engine_loaded[NAMEDATALEN];

static const VecHost vx_host = {VEC_ENGINE_ABI};

static bool vx_check_engine(char **newval, void **extra, GucSource source);

void		_PG_init(void);

PG_FUNCTION_INFO_V1(vec_version);
PG_FUNCTION_INFO_V1(vec_debug_panic);

static const struct config_enum_entry vx_mode_options[] = {
	{"off", VX_MODE_OFF, false},
	{"explain", VX_MODE_EXPLAIN, false},
	{"on", VX_MODE_ON, false},
	{NULL, 0, false}
};

static const struct config_enum_entry vx_join_estimates_options[] = {
	{"spill", VX_JOIN_ESTIMATES_SPILL, false},
	{"trusted", VX_JOIN_ESTIMATES_TRUSTED, false},
	{"bounded", VX_JOIN_ESTIMATES_BOUNDED, false},
	{"strict", VX_JOIN_ESTIMATES_STRICT, false},
	{NULL, 0, false}
};

void
_PG_init(void)
{
	DefineCustomEnumVariable("vec.mode",
							 "Whether the engine runs the slices it supports.",
							 "off: never.  explain: EXPLAIN reports which slices "
							 "qualify, but nothing runs in the engine.  on: "
							 "qualifying slices run in the engine.",
							 &vx_mode,
							 VX_MODE_OFF,
							 vx_mode_options,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomEnumVariable("vec.join_estimates",
							 "How a hash join's build side size is checked.",
							 "spill: not at all, the join spills what does not fit "
							 "(except a NOT IN anti join, checked as with trusted).  The "
							 "others check as if the join could not spill: a slice "
							 "whose build side may outgrow its budget stays on "
							 "PostgreSQL.  trusted: by the planner's estimate.  bounded: "
							 "a build side coming from a join may hold as many rows as "
							 "its largest input.  strict: a build side coming from a "
							 "join stays on PostgreSQL.",
							 &vx_join_estimates,
							 VX_JOIN_ESTIMATES_SPILL,
							 vx_join_estimates_options,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("vec.worker_threads",
							"Number of engine worker threads per backend.",
							"0 means one per CPU.  Takes effect when a backend "
							"first uses the engine; changing it later does not "
							"resize a running backend's runtime.",
							&vx_worker_threads,
							0, 0, 1024,
							PGC_SUSET,
							GUC_GPDB_NEED_SYNC,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("vec.pax_direct_read",
							 "Reads PAX tables column by column on the engine's threads.",
							 "Bypasses the table access method's row-at-a-time interface "
							 "by calling PAX's reader classes directly (vec_pax.so), "
							 "when installed and built for the running pax.so.  Not used in "
							 "Cloudberry's parallel mode.",
							 &vx_pax_direct_read,
							 true,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("vec.pax_decode_cache",
							   "Directory where the direct PAX reader caches decoded columns.",
							   "Each group's columns are kept there as the reader hands them "
							   "out, in files read back through mmap by later scans, which "
							   "then skip PAX's decompression and decoding.  A relative path "
							   "is in each segment's data directory; segments on one host may "
							   "share an absolute one.  Entries of a rewritten micro-partition "
							   "are not used again and go once the size limit needs room.  "
							   "Empty (the default): no cache.",
							   &vx_pax_decode_cache,
							   "",
							   PGC_SUSET,
							   GUC_GPDB_NEED_SYNC,
							   NULL, NULL, NULL);

	DefineCustomIntVariable("vec.pax_decode_cache_size",
							"Most disk space the decoded-column cache takes.",
							"For all segments using the directory.  Once it is reached, "
							"micro-partitions unused for ten minutes are removed, least "
							"recently used first; while nothing can go, scans add nothing.",
							&vx_pax_decode_cache_mb,
							16384, 1, INT_MAX,
							PGC_SUSET,
							GUC_GPDB_NEED_SYNC | GUC_UNIT_MB,
							NULL, NULL, NULL);

	DefineCustomIntVariable("vec.pax_max_decoders",
							"Most PAX groups the direct reader decodes at once per query.",
							"Each holds a decoded group, about 30 MB of a QE's memory on "
							"TPC-H's lineitem; fewer save memory and cost time, since "
							"decoding is most of a scan's work.  0 decodes one group per "
							"partition.",
							&vx_pax_max_decoders,
							0, 0, 1024,
							PGC_USERSET,
							GUC_GPDB_NEED_SYNC,
							NULL, NULL, NULL);

	DefineCustomIntVariable("vec.slice_partitions",
							"Partitions each engine query of a slice runs in.",
							"0 picks them by the slices running together and the "
							"primaries on the host: eight times the worker "
							"threads over the product, at least 1.  At most the "
							"worker threads.",
							&vx_slice_partitions,
							0, 0, 1024,
							PGC_USERSET,
							GUC_GPDB_NEED_SYNC,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("vec.subquery_key_filter",
							 "Lets a correlated aggregate subquery read only the keys the outer query keeps.",
							 "With vec.mode = on, a scalar subquery with aggregates whose "
							 "WHERE clause matches a relation of the outer query on a key gets "
							 "\"key IN (SELECT key FROM relation WHERE <its conditions in the outer "
							 "query>)\" before planning, so that only those keys are aggregated.",
							 &vx_subquery_key_filter,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("vec.key_filters",
							 "Lets a hash join's build side filter the PAX scans of its probe side.",
							 "Once a hash join run in the engine has read its build side, the "
							 "PAX scans its probe side's key comes from drop the rows whose key "
							 "the build side does not have, before building their other columns. "
							 "Each integer or date key of the join gets such a filter.",
							 &vx_key_filters,
							 true,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomRealVariable("vec.partial_agg_skip_ratio",
							 "Groups per row above which a partial aggregation passes its rows on unaggregated.",
							 "A partial aggregation run in DataFusion whose states do not cross "
							 "a Motion checks its first rows: when they made more than this many "
							 "groups per row, it stops aggregating and gives the final aggregation "
							 "the rows as states.  DataFusion's own default is 0.8.",
							 &vx_partial_agg_skip_ratio,
							 0.2, 0.0, 1.0,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("vec.motion_batches",
							 "Experimental: send Arrow batches through Gather Motions between engine slices.",
							 "When both the sending and the receiving slice of a Gather Motion run "
							 "in the engine, the rows travel over the interconnect as Arrow IPC "
							 "batches instead of one tuple at a time.",
							 &vx_motion_batches,
							 false,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("vec.cluster_collation_c",
							 "Whether the database's default collation is C on every node.",
							 "Set by the extension on the coordinator, which asks every "
							 "segment, and sent to them: ordering strings by the default "
							 "collation then runs in DataFusion where batch Motions are "
							 "chosen alike on every node.",
							 &vx_cluster_collation_c,
							 false,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE |
							 GUC_DISALLOW_IN_FILE,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("vec.engine",
							   "The execution engine slices run in.",
							   "The library $libdir/vec_engine_<name>.so.  A backend "
							   "opens it when it first needs an engine and keeps it; "
							   "from then on the setting cannot change.",
							   &vx_engine_name,
							   "datafusion",
							   PGC_SUSET,
							   GUC_GPDB_NEED_SYNC,
							   vx_check_engine, NULL, NULL);

	DefineCustomIntVariable("vec.debug_features_off",
							"Test setting: engine features (VEC_F_* bits) to treat as missing.",
							NULL,
							&vx_debug_features_off,
							0, 0, INT_MAX,
							PGC_SUSET,
							GUC_GPDB_NEED_SYNC | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	DefineCustomIntVariable("vec.debug_result_offset",
							"Test setting: rows the engine offsets its result batches by.",
							"Only for an engine with test functions.",
							&vx_debug_result_offset,
							0, 0, 1024,
							PGC_SUSET,
							GUC_GPDB_NEED_SYNC | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("vec");

	vx_install_hooks();
	vx_install_planner_hook();

	/*
	 * Never open the engine or start threads here.  When the library is
	 * listed in shared_preload_libraries this runs in the postmaster, and
	 * every backend would inherit the threads' memory without the threads.
	 * A backend opens the engine, and starts its runtime, on first use.
	 */
}

/*
 * vec.engine names a library in $libdir, and stays as it was once a
 * backend has loaded one.
 */
static bool
vx_check_engine(char **newval, void **extra, GucSource source)
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
	if (vx_engine != NULL && strcmp(*newval, vx_engine_loaded) != 0)
	{
		GUC_check_errdetail("This session already runs engine \"%s\".", vx_engine_loaded);
		return false;
	}
	return true;
}

/*
 * The engine, opened on first use: $libdir/vec_engine_<vec.engine>,
 * whose vec_engine_entry must give a table of this ABI.  The library stays
 * loaded, and the engine the same, for the rest of the backend's life.
 * RTLD_LOCAL keeps its symbols, and the libraries it brings, to itself.
 */
const VecEngine *
vx_engine_load(void)
{
	char		path[MAXPGPATH];
	sigset_t	saved;
	void	   *handle;
	VecEngineEntry entry = NULL;
	const VecEngine *engine = NULL;
	const char *dlerr = NULL;

	if (vx_engine != NULL)
		return vx_engine;

	snprintf(path, sizeof(path), "%s/vec_engine_%s.so", pkglib_path, vx_engine_name);

	/* Its constructors, or the entry, may start threads (vec_engine.h). */
	vx_signals_block(&saved);
	handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
	if (handle == NULL)
		dlerr = dlerror();
	else
	{
		entry = (VecEngineEntry) dlsym(handle, "vec_engine_entry");
		if (entry != NULL)
			engine = entry(VEC_ENGINE_ABI, &vx_host);
	}
	vx_signals_restore(&saved);

	if (handle == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FILE),
				 errmsg("vec_executor: could not load engine \"%s\"", vx_engine_name),
				 errdetail("%s", dlerr)));
	if (entry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("vec_executor: engine \"%s\" does not export vec_engine_entry",
						vx_engine_name),
				 errdetail("Library: \"%s\".", path)));
	if (engine == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("vec_executor: engine \"%s\" gave no table of functions",
						vx_engine_name)));
	if (engine->abi != VEC_ENGINE_ABI)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("vec_executor: engine \"%s\" speaks ABI version %u, this extension %d",
						vx_engine_name, engine->abi, VEC_ENGINE_ABI),
				 errhint("Install the engine built with this extension.")));

	strlcpy(vx_engine_loaded, vx_engine_name, sizeof(vx_engine_loaded));
	vx_engine = engine;
	return vx_engine;
}

/* The engine's features, but those vec.debug_features_off turns off. */
uint64
vx_engine_features(void)
{
	const VecEngine *engine = vx_engine_load();

	if (VEC_IR_VERSION < engine->caps.ir_min || VEC_IR_VERSION > engine->caps.ir_max)
		return 0;
	return engine->caps.features & ~(uint64) vx_debug_features_off;
}

/* Does the engine take numeric as the IR has it (vec_engine.h)? */
bool
vx_engine_numeric(void)
{
	const VecCaps *caps = &vx_engine_load()->caps;

	return caps->numeric_digits >= 76 && caps->numeric_width == 32 &&
		caps->numeric_special && caps->numeric_dynamic_scale;
}

/* What EXPLAIN calls a feature an engine lacks. */
const char *
vx_feature_name(uint64 feature)
{
	switch (feature)
	{
		case VEC_F_QUERIES:
			return "queries";
		case VEC_F_JOIN:
			return "joins";
		case VEC_F_SORT:
			return "sorts";
		case VEC_F_WINDOW:
			return "window functions";
		case VEC_F_GROUPING_SETS:
			return "grouping sets";
		case VEC_F_SETOPS:
			return "set operations";
		case VEC_F_COLLATE:
			return "locale collations";
		case VEC_F_IPC_MOTION:
			return "batch Motions";
		case VEC_F_PAX_DIRECT:
			return "direct PAX reads";
	}
	return "?";
}

/*
 * Turn a failed FFI call into a PostgreSQL error.  Called only after the
 * Rust frames have returned.
 */
void
vx_raise(int32 status, const char *msg)
{
	if (status == VEC_PANIC)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("engine panicked: %s", msg)));

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("engine error: %s", msg)));
}

/* The engine's test functions, or an error if it has none. */
const VecEngineDebug *
vx_engine_debug(void)
{
	const VecEngine *engine = vx_engine_load();

	if (engine->debug == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("vec_executor: engine %s has no test functions", engine->name)));
	return engine->debug;
}

Datum
vec_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(vx_engine_load()->version));
}

Datum
vec_debug_panic(PG_FUNCTION_ARGS)
{
	VecError	err;
	int32		status;

	VX_ERROR_INIT(err);
	status = vx_engine_debug()->panic(&err);
	if (status != VEC_OK)
		vx_raise(status, err.message);

	PG_RETURN_VOID();
}
