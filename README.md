# cloudberry-datafusion

`vec_executor` is an extension that adds a vectorized execution engine to
[Apache Cloudberry](https://github.com/apache/cloudberry), built on
[Apache DataFusion](https://datafusion.apache.org/).  It takes over the
slices of a query plan that it can run and leaves the rest to
Cloudberry's executor, so every query still runs, with the same results.

Design constraints:

- **Extension only.** No changes to the Cloudberry kernel; it plugs in through
  the executor and planner hooks.
- **Reuse the interconnect.** Motion data still travels over Cloudberry's
  interconnect; DataFusion parallelises the work before sending and after
  receiving.
- **Cloudberry memory accounting.** DataFusion's memory pool is bounded by the
  per-operator memory the coordinator assigns, and leased from the vmem
  tracker.

## Performance

TPC-H at scale factor 100 on PAX tables, one host with 3 primaries,
against the same cluster without the extension:

| Planner | Cloudberry | vec_executor | Speedup |
|---|---:|---:|---:|
| Postgres planner | 1089 s | 116 s | **9.4x** |
| GPORCA | 1101 s | 173 s | **6.4x** |

Every query is faster (3.3x to 67x) and returns the same rows.  Setup,
method and per-query times: [`docs/benchmarks.md`](docs/benchmarks.md).

## What runs in the engine

A slice runs in the engine when everything in it does; otherwise the
whole slice stays on PostgreSQL, and `EXPLAIN` says why.  Today that
covers:

- **Scans** of heap, AO row, AO column and PAX tables.  PAX tables are
  read column by column on the engine's threads (the direct PAX reader,
  optional at build time), others through the table AM with the query's
  snapshot.
- **Joins**: hash joins (inner, outer, semi, anti, `NOT IN`), which
  spill beyond their memory; merge joins, run as hash joins; nested loop
  joins.
- **Aggregates**: plain, hashed and split (partial / final) aggregates,
  `DISTINCT` aggregates, grouping sets, and the common aggregate functions
  (`count`, `sum`, `avg`, `min`, `max`, `stddev`, `variance`, ...).
- **Sort, LIMIT, window functions, `UNION ALL`, `INTERSECT`, `EXCEPT`**,
  subtrees below PostgreSQL nodes, and correlated aggregate subqueries.
- **Types**: integers, floats, `numeric`, `bool`, date and time types,
  `text`, `varchar`, `char(n)`, with PostgreSQL's semantics: the same
  errors (overflow, division by zero) with the same SQLSTATE, the same
  numeric scales and rounding, short-circuit `AND`/`OR`.
- **Motions**: a slice runs below the Motion it sends through and above
  the one it receives from; with `vec.motion_batches` both ends of a
  Motion between engine slices exchange Arrow batches instead of tuples.

The full list, with what stays on PostgreSQL and why, is in
[`docs/internals.md`](docs/internals.md).

Known differences and limits:

- Floating-point sums can differ in the last bits, because the engine adds
  partial sums in a different order.
- When values that compare equal fall into one group, such as `-0` and `0`
  in a `float8` column, the value shown for the group may differ.
- `EXPLAIN ANALYZE` counts rows for a slice's top node only.
- Sorts by text run in the engine only under a C collation (or C.UTF-8);
  under another collation they stay on PostgreSQL.

## Architecture

The extension (the host) decides what runs where and moves the rows; an
engine runs the plans it is given.  DataFusion is one engine; others can
take its place behind the same interface.

```text
 PostgreSQL backend (QD or QE), main thread         engine library, own threads
 ------------------------------------------         ---------------------------
 planner/executor hooks       src/vx_hooks.c
 which slices qualify         src/vx_plan_check.c  <- VecCaps
 slice -> plan IR (JSON)      src/vx_translate.c   -> query_start   plan, inputs
 scan: table AM or PAX        src/vx_exec.c        -> query_push    Arrow arrays
 receive Motion bytes                              -> query_push_ipc  IPC bytes
 hand out rows / send bytes                        <- query_poll    Arrow arrays,
                                                                    IPC bytes
 threads, vmem, spill dir     src/vx_runtime.c     <- heap_bytes
 open the engine              src/vx_init.c           vec_engine_<name>.so
```

- **The host** (`src/`, C) runs inside the backend and alone calls into
  PostgreSQL.  It checks each slice against what the engine declares,
  translates the slices that qualify, scans their tables with the query's
  snapshot, frames batch Motions on the interconnect, leases the engine's
  heap from the vmem tracker, and turns engine errors into `ereport`s.
- **An engine** (`engines/<name>/`) is a library of its own,
  `vec_engine_<name>.so`, chosen by `vec.engine` and opened on first
  use.  It runs a slice's plan on its own threads over the rows it is
  given, encodes and routes the batches of a sending Motion, and spills
  within the memory it is allotted.  `engines/datafusion` (Rust, on
  Apache DataFusion) runs everything; `engines/null` (C) runs nothing, for
  the tests.
- **Three contracts** sit between them, each written down so that an
  engine can be built from them alone:
  - `include/vec_engine.h`: the table of functions, the threads, signals,
    errors and memory every engine respects, and the capabilities
    (`VecCaps`) it declares; see `docs/engines.md`.
  - `docs/ir.md`: the plans, with their PostgreSQL semantics: nodes,
    expressions, types and the PostgreSQL functions an engine computes
    exactly as PostgreSQL does.
  - `docs/wire.md`: the Arrow IPC streams of a batch Motion between two
    engine slices, which run in different processes and must agree byte
    for byte, cdbhash routing included.


## Requirements

- Apache Cloudberry (main), installed, with `pg_config` and the server
  headers (which include OpenSSL's: install the OpenSSL development
  package).
- A Rust toolchain; the version is pinned in
  `engines/datafusion/rust-toolchain.toml` (rustup picks it up).
  DataFusion is a commit of a fork of apache/datafusion with
  apache/datafusion#26264 (`engines/datafusion/Cargo.toml`), which cargo
  fetches from GitHub.
- For the direct PAX reader (optional): the Cloudberry source tree the
  installed `pax.so` was built from, the protobuf headers of the version
  `pax.so` links, and GNU `patch`.

## Build and install

```sh
make PG_CONFIG=/usr/local/cloudberry-db/bin/pg_config
make PG_CONFIG=/usr/local/cloudberry-db/bin/pg_config install
```

This installs the extension (`vec_executor.so`) and the DataFusion engine
(`vec_engine_datafusion.so`) on the host it runs on; install them on
every host of the cluster.

To build the direct PAX reader (`vec_pax.so`) as well, name the PAX
sources of the Cloudberry tree:

```sh
make VX_PAX_SRC=<cloudberry>/contrib/pax_storage/src/cpp
make VX_PAX_SRC=<cloudberry>/contrib/pax_storage/src/cpp install
```

The build applies `patches/pax/*.patch` to a copy of those sources in
`pax_build/`; neither the tree nor the installed `pax.so` is changed.
The library records `pax.so`'s build ID and is used only with that
`pax.so`; without it, PAX tables are read through the table AM.

## Usage

Load the library in every backend, segments included.  For the whole
cluster, add it to `shared_preload_libraries`.  `gpconfig -c` replaces
the whole value, so keep the libraries already listed:

```sh
gpconfig -s shared_preload_libraries        # the current list, if any
gpconfig -c shared_preload_libraries -v '<current list>,vec_executor'
gpstop -ar
```

or for one database:

```sql
ALTER DATABASE mydb SET session_preload_libraries = 'vec_executor';
```

Then turn it on:

```sql
SET vec.mode = on;              -- off (default), explain, on
SET vec.motion_batches = on;    -- optional, experimental
```

With `vec.mode = explain` or `on`, `EXPLAIN` ends with one line per
slice saying whether the engine runs it, and if not, why:

```
 Vectorized: slice 0 not eligible: receives rows from a Gather Motion with nothing to compute on them
 Vectorized: slice 1 eligible, sends Arrow batches
 Vectorized: slice 2 eligible
```

`CREATE EXTENSION vec_executor` is needed only for the SQL functions
(`vec_version()` and the `vec_debug_*` functions the tests use).

## Configuration

| Setting | Default | |
|---|---|---|
| `vec.mode` | `off` | `off`, `explain` (only report in `EXPLAIN`), `on` (run eligible slices) |
| `vec.motion_batches` | `off` | Experimental: Motions between two engine slices carry Arrow batches |
| `vec.pax_direct_read` | `on` | Read PAX tables column by column on the engine's threads |
| `vec.pax_max_decoders` | `0` | PAX groups decoded at once per query; 0 = one per partition |
| `vec.pax_decode_cache` | empty | Superuser.  Directory for a cache of decoded PAX columns; empty = no cache |
| `vec.pax_decode_cache_size` | `16GB` | Superuser.  Disk space the decoded-column cache may take |
| `vec.subquery_key_filter` | `on` | Rewrite correlated aggregate subqueries to aggregate only the keys the outer query keeps |
| `vec.join_estimates` | `spill` | How a hash join's build side size is checked: `spill`, `trusted`, `bounded`, `strict` |
| `vec.worker_threads` | `0` | Superuser.  Engine threads per backend; 0 = one per CPU |
| `vec.slice_partitions` | `0` | Partitions per slice; 0 = from the threads, the slices running together and the primaries on the host |
| `vec.engine` | `datafusion` | Superuser.  Engine library, `$libdir/vec_engine_<name>.so` |
| `vec.engine_options` | empty | The engine's own settings, `name=value, ...`; DataFusion's: `key_filters` (on/off), `partial_agg_skip_ratio` (0 to 1) |

Memory: a slice's budget is what Cloudberry gives the node it replaces
(`statement_mem`, `work_mem`, `hash_mem_multiplier`); the engine spills to
the backend's temporary directory beyond it, and its allocations are
leased from the vmem tracker, so `gp_vmem_protect_limit` applies.

## Testing

```sh
make installcheck                          # regression tests, against a running cluster
make VX_PAX_SRC=... installcheck           # adds the direct PAX reader's tests
make installcheck-engine ENGINE=datafusion # an engine's results against PostgreSQL's
make df-rust-test                          # the engine's unit tests
```

## Documentation

| | |
|---|---|
| [`docs/internals.md`](docs/internals.md) | How each part works, what runs where, and what each change measured |
| [`docs/benchmarks.md`](docs/benchmarks.md) | Benchmark setup, method and results |
| [`docs/engines.md`](docs/engines.md) | The engine ABI, and adding another engine |
| [`docs/ir.md`](docs/ir.md) | The plan IR an engine receives |
| [`docs/wire.md`](docs/wire.md) | The Arrow stream format of batch Motions |

## License

Apache License 2.0. See `LICENSE`.
