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
 * df_executor.h
 *	  Declarations shared by the C sources of datafusion_executor.
 *
 * src/df_executor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DF_EXECUTOR_H
#define DF_EXECUTOR_H

#include "df_ffi.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#include "nodes/plannodes.h"

/* GUC datafusion.mode */
typedef enum DfMode
{
	DF_MODE_OFF,				/* hooks pass everything through */
	DF_MODE_EXPLAIN,			/* EXPLAIN reports eligibility; nothing runs */
	DF_MODE_ON					/* eligible slices run in DataFusion */
} DfMode;

extern int	df_mode;

/* GUC datafusion.worker_threads */
extern int	df_worker_threads;

/* df_init.c */
extern void df_raise(int32 status, const char *msg) pg_attribute_noreturn();

/* What df_translate_slice produces for one slice. */
typedef struct DfSliceSpec
{
	char	   *json;			/* plan for the Rust side */
	Index		scanrelid;
	int			nscan;			/* columns read from the table */
	AttrNumber *scan_attnos;
	Oid		   *scan_types;
	int			nout;			/* output columns, in targetlist order */
	Oid		   *out_types;
} DfSliceSpec;

/* df_exec.c */
extern bool df_exec_attach(QueryDesc *queryDesc, PlanState *root,
						   char *reason, size_t reasonlen);

/* df_translate.c */
extern const char *df_type_tag(Oid type);
extern bool df_translate_slice(Plan *root, DfSliceSpec *spec,
							   char *reason, size_t reasonlen);

/* df_hooks.c */
extern void df_install_hooks(void);

/* df_plan_check.c */
extern bool df_check_slice(PlannedStmt *stmt, Plan *root, bool root_is_sender,
						   char *reason, size_t reasonlen);
extern Plan *df_local_slice_root(QueryDesc *queryDesc, int *slice_index,
								 bool *is_sender);
extern void df_explain_slices(PlannedStmt *stmt, StringInfo out);

/* df_runtime.c */
extern int	df_runtime_ensure(void);
extern int32 df_task_wait_interruptible(DfTask *task, char *buf, size_t buflen);

#endif							/* DF_EXECUTOR_H */
