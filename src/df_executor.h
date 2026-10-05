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

/* GUC datafusion.worker_threads */
extern int	df_worker_threads;

/* df_init.c */
extern void df_raise(int32 status, const char *msg) pg_attribute_noreturn();

/* df_runtime.c */
extern int	df_runtime_ensure(void);
extern int32 df_task_wait_interruptible(DfTask *task, char *buf, size_t buflen);

#endif							/* DF_EXECUTOR_H */
