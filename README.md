# cloudberry-datafusion

`datafusion_executor` is a PostgreSQL extension that adds a vectorized
execution backend to [Apache Cloudberry](https://github.com/apache/cloudberry),
built on [Apache DataFusion](https://datafusion.apache.org/).

Design constraints:

- **Extension only.** No changes to the Cloudberry kernel; it plugs in through
  the executor and planner hooks.
- **Reuse the interconnect.** Motion data still travels over Cloudberry's
  interconnect; DataFusion parallelises the work before sending and after
  receiving.
- **Cloudberry memory accounting.** DataFusion's memory pool is bounded by the
  per-operator memory the coordinator assigns, and leased from the vmem
  tracker.

## Status

Milestones M0 and M1: the Rust static library links into the extension, and
each backend starts a Tokio runtime on first use.  The runtime's threads block
every signal, so cancel, statement timeout and terminate requests still reach
the backend's main thread, which stops the runtime's work before reporting
the error.  A panic inside Rust, on the main thread or on a worker, surfaces
as an ordinary `ERROR` without taking the backend down.  Nothing executes
queries yet.

`datafusion.worker_threads` sets the runtime size (0 = one thread per CPU).
It takes effect when a backend first uses DataFusion.

Milestone M2 adds the executor and EXPLAIN hooks.  `datafusion.mode` is
`off` (default), `explain` or `on`.  With `explain` or `on`, text-format
EXPLAIN ends with one line per slice saying whether DataFusion can run it,
and if not, why:

```
 Aggregate
   ->  Seq Scan on pg_class
         Filter: (relpages > 0)
 Optimizer: Postgres query optimizer
 DataFusion: slice 0 eligible
```

Load the library in every backend, including the segments' QEs, by adding
`datafusion_executor` to `shared_preload_libraries`; otherwise the hooks only
exist in sessions that have called one of its functions.

Milestone M3 executes eligible slices in DataFusion.  The slice's top
PlanState keeps its place in the executor and only its `ExecProcNode` is
replaced, so `ExecutorRun`, cursors, `es_processed` and error cleanup work
as before.  The backend's main thread scans the heap table with the query's
snapshot and pushes column batches of 8192 rows through a bounded queue;
DataFusion filters and aggregates them on the runtime's threads; result
batches come back and are returned one row at a time.

What qualifies today: a heap Seq Scan with a filter, optionally under one
single-stage plain or hashed aggregate (`count`, `sum`, `min`, `max`, `avg`),
over `bool`, `int2`, `int4`, `int8`, `float4` and `float8`, with comparison
and arithmetic operators.  Any Motion keeps its slice on PostgreSQL, so in a
normal cluster only coordinator-only plans qualify until Motion support
lands; the regression test uses a coordinator-local table in utility mode.

PostgreSQL semantics are kept where DataFusion differs:

- Integer and floating-point arithmetic raises PostgreSQL's errors with the
  same SQLSTATE and message (overflow, underflow, division by zero), instead
  of wrapping around or returning infinity.
- `AND` and `OR` skip later arguments once the result is decided, so a guard
  such as `b <> 0 AND a / b > 1` never divides by zero.  DataFusion's
  expression simplifier is disabled for this, since it would undo the guard;
  PostgreSQL's planner has already simplified the expressions.

Known differences:

- When values that compare equal fall into one group, such as `-0` and `0`
  in a `float8` column, the value shown for the group may differ from the
  one PostgreSQL shows.
- Floating-point sums can differ in the last bits, because DataFusion adds
  partial sums in a different order.
- `EXPLAIN ANALYZE` counts rows for the slice's top node only; the nodes
  below it show as never executed.
- DataFusion's spill files live in the backend's temporary directory but
  are not yet counted by Cloudberry's workfile limits.

Milestone M4 puts DataFusion's memory under Cloudberry's control:

- **Operator budget.** A slice gets what the executor would give its Agg
  node, `min(operatorMemKB, work_mem) * hash_mem_multiplier`, or `work_mem`
  for a scan alone.  That is the limit of the DataFusion memory pool.  Hash
  aggregation spills to a directory in the backend's temporary-file area
  instead of growing past it, as PostgreSQL's hash aggregation does; the
  directory is removed when the backend exits, and after a crash with the
  other temporary files.  The limit is soft for reservations DataFusion
  cannot refuse, such as merging spilled runs.  A small budget runs on fewer
  partitions (at least 2 MB each) instead of starving all of them.
- **Vmem.** A counting allocator tracks every byte Rust allocates in the
  backend.  Between waits the main thread leases that amount plus
  budget/8 of headroom from Cloudberry's vmem tracker, and gives back the
  unused part when a query ends.  On QEs, where Cloudberry enforces vmem
  limits, a refused lease fails the query with the same error as a refused
  allocation (`Out of memory`, SQLSTATE 53500).
- **EXPLAIN ANALYZE.** The top node's Instrumentation carries the peak pool
  use and, after spilling, the memory that would have avoided it, so
  Cloudberry's slice statistics show `Work_mem: ... max, ... wanted`.  A
  DataFusion line adds partitions, limit, peak and spill volume:

```
 DataFusion: 1 partitions, memory limit 2048 kB, peak 2625 kB, spilled 146510 kB in 72 files
```

Measured in a 3-segment container on 10 ARM cores, a grouped aggregate over
a 20-million-row coordinator-local heap table took 0.47 s in DataFusion and
1.41 s on the PostgreSQL executor, with identical results (0.50 s once the
counting allocator was added in M4).  Grouping 2 million distinct keys with
`work_mem = 1MB` spills in both engines and took 0.34 s in DataFusion and
0.60 s in PostgreSQL.

| Milestone | Scope |
|---|---|
| M0 | Link Rust into the extension; panic safety across FFI |
| M1 | Tokio runtime inside a backend; signals and cancellation |
| M2 | Executor hooks, `datafusion.mode` GUC, eligibility check |
| M3 | Heap scan on the main thread, Filter and Aggregate in DataFusion, results back to PostgreSQL |
| M4 | Memory pool bound to `operatorMemKB`, vmem lease, SQLSTATE mapping |

## Build

Requires a Cloudberry installation (for `pg_config` and server headers; the
headers include OpenSSL's, so install the OpenSSL development package) and a
Rust toolchain; the version is pinned in `rust/rust-toolchain.toml`.

```sh
make PG_CONFIG=/usr/local/cloudberry-db/bin/pg_config
make PG_CONFIG=/usr/local/cloudberry-db/bin/pg_config install
make PG_CONFIG=/usr/local/cloudberry-db/bin/pg_config installcheck
make df-rust-test
```

## FFI contract

Every function the Rust library exports never calls into PostgreSQL, never
lets a panic escape, and reports failure as a status code plus a message in
the caller's buffer. The C side raises the `ereport` after the call returns,
so PostgreSQL's `longjmp` never unwinds through Rust frames. See
`src/df_ffi.h`.

## License

Apache License 2.0. See `LICENSE`.
