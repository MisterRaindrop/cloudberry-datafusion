# Operating vec_executor

For whoever installs and runs the extension on a cluster: how to check
that it works, how to size it, what its errors mean, and how to upgrade or
remove it.  Installing and the settings are in the README (Build and
install, Usage, Configuration); how each part works is in
[`internals.md`](internals.md).

## Checking that it works

- `SELECT vec_version();` (after `CREATE EXTENSION vec_executor`) names
  the engine the backend loaded and its version ("datafusion 57.0.0-dev");
  `SELECT extversion FROM pg_extension WHERE extname = 'vec_executor'` the
  extension's.
- `SET vec.mode = explain;` then `EXPLAIN` a query: the last lines say,
  slice by slice, whether the engine runs it and if not why
  ("Vectorized: slice 1 eligible, sends Arrow batches", "Vectorized:
  slice 0 not eligible: ...", "eligible below the top: ..." for a slice of
  which the engine runs the subtrees under PostgreSQL's nodes).
- `EXPLAIN ANALYZE` with `vec.mode = on` adds the engine's rows at each
  node it ran, and a line for the coordinator's slice: partitions, memory
  limit, peak, spilled bytes and files.
- On a segment, `log_min_messages = debug1` logs per slice "vec_executor:
  running slice N" or "slice N stays on the PostgreSQL executor: REASON",
  and the plan the engine was given ("engine plan: {...}").

Every host must have the libraries, and every backend, on the segments
too, must load `vec_executor.so` (`shared_preload_libraries`, or a
database's `session_preload_libraries`).  A session that loaded it on the
coordinator only (`LOAD`, `CREATE EXTENSION`) fails on queries whose
Motions carry batches: "vec_executor: Motion N received tuples where
batches were expected".

## Sizing

**Memory.**  A slice in the engine gets the memory PostgreSQL would give
the node it replaces: `statement_mem` divided among the query's operators,
capped by `work_mem` times `hash_mem_multiplier` for hash tables, or
`work_mem` for a plain aggregate or a scan.  Beyond it the engine spills
hash aggregations, sorts and hash joins to disk, as PostgreSQL does.  Its
allocations are leased from Cloudberry's vmem tracker, so
`gp_vmem_protect_limit` and `gp_vmem_limit_per_query` apply to it too.

- Size `gp_vmem_protect_limit` so that all segments of a host together
  fit its memory, with room for the OS and the page cache.  Past the
  host's memory the kernel's OOM killer ends backends (and the segment
  restarts) before the vmem tracker refuses anything.
- What the engine frees after a query goes back to the system
  (`malloc_trim`).  Idle backends that have run engine queries still keep
  more than ones that have not: after the TPC-H queries at SF10, one
  session's 19 idle QEs held 1.2 GB, against 0.3 GB without the engine.
  `vec.malloc_arenas = 8` keeps a fifth less, for 2.5% of speed.
- A larger `statement_mem` lets more stay in memory: TPC-H at SF100 was
  measured with 4 GB (docs/benchmarks.md).

**Threads.**  Each backend that runs the engine starts
`vec.worker_threads` threads (0, the default: one per CPU) on first use,
and each slice runs in `vec.slice_partitions` partitions (0: from the
threads, the slices that run at once and the primaries on the host, so
that a host's segments do not each take every CPU).  The threads belong to
the backend, and so to its resource group: CPU limits apply.  Many
concurrent sessions oversubscribe the CPUs as PostgreSQL's processes
would; lower `vec.worker_threads` (superuser) to share them out.

**Disk.**  Spill files go to a directory per backend under the default
temporary directory (`base/pgsql_tmp/pgsql_tmp_vec_PID`) and count toward
`gp_workfile_limit_per_query`, `gp_workfile_limit_per_segment` and
`gp_workfile_limit_files_per_query`; `gp_toolkit.gp_workfile_entries`
shows them as the workfile set `vec_executor`.  The limits are checked as
often as the main thread polls the engine, so a query can write a little
past one before it fails.

**PAX tables** are read column by column on the engine's threads when the
direct reader (`vec_pax.so`) matches the installed `pax.so`; otherwise
through the table AM: "vec_executor: reading PAX tables through the table
access method" says why, as a warning when `vec_pax.so` is installed but
cannot be used (built for another `pax.so`), in the log otherwise.  `vec.pax_decode_cache`
(superuser) caches decoded columns on a local disk for repeated scans.

## Errors

| Error | What it means | What to do |
|---|---|---|
| `Out of memory` (53500), detail "Per-query memory limit reached" or "Vmem limit reached" | The vmem tracker refused the engine's lease, as it would refuse PostgreSQL's allocation | Raise `gp_vmem_limit_per_query` / `gp_vmem_protect_limit`, or run fewer queries at once |
| `out of memory` (53200), detail "DataFusion: ..." | An operator could not stay within the slice's budget and could not spill | Raise `statement_mem` (and `work_mem`); `vec.mode = off` for that query |
| `datafusion error: ... No space left on device` (53100) | The disk of the temporary directory is full | Free space, or move temporary files (`temp_tablespaces`) |
| 53000 (too many open files, or a workfile limit) | The process's open files limit, or `gp_workfile_limit_*`, was reached while spilling | Raise `ulimit -n` for the cluster, or the workfile limits |
| `Motion N received tuples where batches were expected` | A segment backend did not load `vec_executor.so` | Preload it on every host (Checking that it works) |
| `engine "datafusion" speaks ABI version X, this extension Y` | `vec_executor.so` and `vec_engine_datafusion.so` come from different builds | Install both from the same build on every host |
| `engine panicked: ...` (XX000) | A bug in the engine; the query fails, the backend goes on | Report it with the query; `vec.mode = off` meanwhile |
| A result that differs from `vec.mode = off` | One of the README's known differences, or a bug | Compare with `vec.mode = off`; report what is not in the list |

A cancel or a `statement_timeout` returns as soon as it does without the
extension (about half a second at SF10 in the measurements of
internals.md); the engine's work stops within a second after that.

Any query can run without the engine: `SET vec.mode = off` for the
session, or `ALTER ROLE` / `ALTER DATABASE ... SET vec.mode = off`.

## Upgrading

1. Install the new release's files on every host (`install.sh --hosts`,
   or `make install` everywhere).  The extension library and the engine
   must come from the same build.
2. With `shared_preload_libraries`, restart the cluster (`gpstop -ar`):
   the postmaster loaded the old library, and backends inherit it.  With
   `session_preload_libraries`, new sessions load the new files; end the
   old ones.
3. If the release changed the extension's version (CHANGELOG, README
   Versions), run `ALTER EXTENSION vec_executor UPDATE;` in each database
   that has it.

## Removing

1. Take `vec_executor` out of `shared_preload_libraries` (and restart) or
   `ALTER DATABASE ... RESET session_preload_libraries`.
2. `DROP EXTENSION vec_executor;` in each database that has it.
3. Delete `vec_executor.so`, `vec_engine_*.so`, `vec_pax.so` from
   `pg_config --pkglibdir` and `vec_executor*` from
   `pg_config --sharedir`/extension on every host.

Queries then run as they did before; the extension changes no data and no
catalog beyond its SQL functions.
