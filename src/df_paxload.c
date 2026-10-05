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
 * df_paxload.c
 *	  Load datafusion_pax.so, the experimental PAX reader, built from the
 *	  PAX patch in patches/pax (access/datafusion_scan_api.cc).
 *
 * The library exports one symbol, datafusion_pax_scan_api(), a table of
 * functions.  They call PAX's internal C++ classes, so the table is only
 * used with the pax.so it was built against: both carry pax.so's ELF build
 * ID, the table as a string recorded at build time, the running pax.so in
 * its note segment.  Any problem (library missing, symbols that do not bind,
 * another build ID) is reported once per backend as a WARNING and the scan
 * goes through the table AM instead.
 *
 * src/df_paxload.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dlfcn.h>
#include <elf.h>
#include <link.h>

#include "miscadmin.h"

#include "df_executor.h"

typedef const DfPaxReader *(*ScanApiFn) (void);

static bool df_pax_tried = false;
static const DfPaxReader *df_pax_reader;
static bool df_pax_ok = false;

typedef struct BuildIdSearch
{
	char		hex[128];
	bool		found;
} BuildIdSearch;

/* dl_iterate_phdr callback: the GNU build ID of the loaded pax.so. */
static int
df_find_pax_build_id(struct dl_phdr_info *info, size_t size, void *data)
{
	BuildIdSearch *search = (BuildIdSearch *) data;
	const char *name = info->dlpi_name;
	size_t		len = name ? strlen(name) : 0;
	int			i;

	if (len < 7 || strcmp(name + len - 7, "/pax.so") != 0)
		return 0;

	for (i = 0; i < info->dlpi_phnum; i++)
	{
		const ElfW(Phdr) *ph = &info->dlpi_phdr[i];
		const char *p,
				   *end;

		if (ph->p_type != PT_NOTE)
			continue;
		p = (const char *) (info->dlpi_addr + ph->p_vaddr);
		end = p + ph->p_memsz;
		while (p + sizeof(ElfW(Nhdr)) <= end)
		{
			const ElfW(Nhdr) *nh = (const ElfW(Nhdr) *) p;
			const char *nname = p + sizeof(ElfW(Nhdr));
			const unsigned char *desc =
				(const unsigned char *) (nname + ((nh->n_namesz + 3) & ~3));

			if (nh->n_type == NT_GNU_BUILD_ID && nh->n_namesz == 4 &&
				memcmp(nname, "GNU", 4) == 0 && nh->n_descsz * 2 < sizeof(search->hex))
			{
				unsigned	j;

				for (j = 0; j < nh->n_descsz; j++)
					snprintf(search->hex + 2 * j, 3, "%02x", desc[j]);
				search->found = true;
				return 1;
			}
			p = (const char *) desc + ((nh->n_descsz + 3) & ~3);
		}
	}
	return 1;
}

static bool
df_pax_load(char *why, size_t whylen)
{
	char		path[MAXPGPATH];
	void	   *handle;
	ScanApiFn	scan_api;
	const DfPaxReader *api;
	BuildIdSearch running;

	memset(&running, 0, sizeof(running));
	dl_iterate_phdr(df_find_pax_build_id, &running);
	if (!running.found)
	{
		snprintf(why, whylen, "pax.so is not loaded or has no build ID");
		return false;
	}

	snprintf(path, sizeof(path), "%s/datafusion_pax.so", pkglib_path);
	handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
	if (handle == NULL)
	{
		snprintf(why, whylen, "%s", dlerror());
		return false;
	}

	scan_api = (ScanApiFn) dlsym(handle, "datafusion_pax_scan_api");
	if (scan_api == NULL)
	{
		snprintf(why, whylen, "%s lacks datafusion_pax_scan_api", path);
		return false;
	}
	api = scan_api();
	if (api == NULL || api->version != DF_PAX_SCAN_API_VERSION)
	{
		snprintf(why, whylen, "%s has scan interface version %u, expected %u",
				 path, api ? api->version : 0, DF_PAX_SCAN_API_VERSION);
		return false;
	}
	if (strcmp(api->pax_build_id, running.hex) != 0)
	{
		snprintf(why, whylen, "built against pax.so %s, running pax.so is %s",
				 api->pax_build_id, running.hex);
		return false;
	}
	df_pax_reader = api;
	return true;
}

/*
 * The experimental PAX reader, or NULL if it cannot be used in this backend.
 * Call only when a PAX relation is open, so that pax.so is loaded.
 */
const DfPaxReader *
df_pax_reader_get(void)
{
	if (!df_pax_tried)
	{
		char		why[512];

		df_pax_tried = true;
		df_pax_ok = df_pax_load(why, sizeof(why));
		if (!df_pax_ok)
			ereport(WARNING,
					(errmsg("datafusion: reading PAX tables through the table access method"),
					 errdetail("The direct PAX reader is unavailable: %s", why)));
	}
	return df_pax_ok ? df_pax_reader : NULL;
}
