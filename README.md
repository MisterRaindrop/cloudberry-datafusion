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

What qualifies today: a Seq Scan with a filter over a heap, AO row, AO
column or PAX table, optionally under one
plain or hashed aggregate (`count`, `sum`, `min`, `max`, `avg`), over
`bool`, `int2`, `int4`, `int8`, `float4` and `float8`, with comparison and
arithmetic operators.  Since M5 this includes the segments' slices: a slice
that ends in the Motion it sends through runs in DataFusion below that
Motion, which keeps running on PostgreSQL and sends the rows one at a time,
and the aggregate may be the partial stage of a split aggregate (`count`,
`sum`, `min`, `max`, whose transition states are plain values) that a
PostgreSQL Finalize Aggregate combines.  Slices that receive from a Motion
stay on PostgreSQL.

PostgreSQL semantics are kept where DataFusion differs:

- Integer and floating-point arithmetic raises PostgreSQL's errors with the
  same SQLSTATE and message (overflow, underflow, division by zero), instead
  of wrapping around or returning infinity.
- `AND` and `OR` skip later arguments once the result is decided, so a guard
  such as `b <> 0 AND a / b > 1` never divides by zero.  DataFusion's
  expression simplifier is disabled for this, since it would undo the guard;
  PostgreSQL's planner has already simplified the expressions.

Known differences and limits:

- `datafusion.worker_threads` defaults to one thread per CPU in every
  backend, so several QEs on one host oversubscribe it; set it to cores
  divided by segments per host until the default does that.
- With GPORCA, `HAVING` lands in a separate Result node, which does not
  qualify yet; the PostgreSQL planner puts it in the aggregate.
- A sorted Gather Motion (with a Sort below it, chosen for example for a
  `GROUP BY` with very few groups) keeps its slice on PostgreSQL.
- Within one QE the main thread still reads the table, and that is where
  most of a query's CPU goes.  Sampled stacks of a QE's main thread put
  60% of it into PAX filling a tuple slot per row (its columns turned into
  rows that are then turned back into columns), 17% into reading files and
  20% into this extension's copying and batch building; on heap, 54% into
  reading buffers and scanning tuples, 27% into deforming them and 17% into
  this extension.  Cloudberry's parallel mode spreads that work over
  several QEs per segment (see below).

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

- **Operator budget.** A slice gets what the executor would give its hashed
  Agg node, `min(operatorMemKB, work_mem) * hash_mem_multiplier`, or
  `work_mem` for a plain aggregate or a scan alone, which hold only the
  batches in flight (the resource queue grants a plain Agg 100 kB, on which
  DataFusion's repartitioning spilled those batches).  That is the limit of
  the DataFusion memory pool.  Hash
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

Milestone M5 runs the segments' slices.  QEs need the library loaded when
they start: add it to `shared_preload_libraries`, or for one database
`ALTER DATABASE ... SET session_preload_libraries = 'datafusion_executor'`.
In a 3-segment container on 10 ARM cores with `datafusion.worker_threads =
2`, both planners return identical results; for example a grouped aggregate
over a 30-million-row distributed table (partial aggregate on the segments in
DataFusion, final aggregate on PostgreSQL) took 0.27 s instead of 1.31 s,
and grouping it by its distribution key with a HAVING filter took 0.61 s
instead of 5.8 s.  A statement timeout or cancel stops the segments' work
sooner than on PostgreSQL (about 0.25 s against 0.5 s for a 100 ms timeout).

Milestone M6 (first part) reads AO row, AO column and PAX tables as well
as heap tables, through the table AM interface on the main thread.  The scan
starts the way the Seq Scan node would, so column stores read only the
columns the scan uses and PAX skips micro-partitions its statistics rule
out.  Over 30 million distributed rows, a grouped aggregate took 0.19 s on
PAX and 0.26 s on heap in DataFusion, against 1.43 s and 1.27 s on
PostgreSQL.

Cloudberry's parallel mode works too (`enable_parallel`, a table's
`parallel_workers`): several QEs per segment share a parallel scan through
the table AM, and each runs DataFusion on its part, reusing the parallel
scan descriptor Cloudberry sets up.  Over 30 million rows with 3 QEs per
segment and one DataFusion thread each, a grouped aggregate on heap took
0.14 s instead of 0.27 s without parallel mode (PostgreSQL: 0.58 s with
parallel mode, 1.16 s without), and on PAX 0.14 s instead of 0.20 s.

### Experimental: direct PAX reader

`datafusion.pax_direct_read = on` reads PAX tables without the table AM's
row-at-a-time interface.  The main thread lists the micro-partitions
visible to the snapshot; DataFusion's partitions then take blocks one at a
time and decode the needed columns themselves, through PAX's own reader
classes, in a small separate library, `datafusion_pax.so`.

Its source is a patch to PAX, `patches/pax/0001-pax-columnar-scan-api.patch`,
which adds a columnar scan interface (`access/datafusion_scan_api.{h,cc}`):
one exported C function, `datafusion_pax_scan_api()`, returning a versioned
table of functions; every other symbol stays local, and the library links
with `-Bsymbolic`, so nothing clashes with PAX or PostgreSQL.  The build
copies the PAX sources to `pax_build/`, applies the patches there and
compiles what they add; neither the Cloudberry tree nor the installed
pax.so is modified.  The same patch could later go upstream into PAX.

That code calls PAX's internal C++ classes, not a published API:

- It is built only with `make DF_PAX_SRC=<cloudberry>/contrib/pax_storage/src/cpp`,
  against the installed PAX headers plus the sources of the same tree, and
  needs the protobuf headers of the version pax.so links and GNU `patch`.
- It records the installed pax.so's ELF build ID.  At run time the extension
  compares it with the running pax.so and, if they differ or the library
  does not load, warns once and reads through the table AM.
- Cloudberry's parallel mode keeps the table AM path.

Like PAX's own scan, it skips micro-partitions and then groups whose
min/max statistics (`minmax_columns`) rule out the scan's qual, honouring
`pax.enable_sparse_filter`.  That evaluates operators through fmgr, so it
happens on the main thread while the blocks are listed; the workers only
read the groups that remain, and DataFusion still filters every row.

PAX's decoding allocates with malloc, outside the counting allocator.  The
reader passes PAX a read buffer of its own (`pax.scan_reuse_buffer_size`)
and reports what it holds (that buffer, the decoded columns and null
bitmaps, the visibility map, the rows handed out); the figure is added to
the Rust heap and leased from the vmem tracker with it.  Measured with
`mallinfo2` it is within a few kB of what PAX actually allocates per group
(the file footer is not counted).  `datafusion_debug_last_pax()` and
EXPLAIN ANALYZE show what was skipped and the peak decoding memory.

Selective queries over 30 million rows with `minmax_columns = 'a'`
(3 segments, 3 DataFusion threads per QE):

| Query | PostgreSQL | table AM | direct, skipping off | direct |
|---|---|---|---|---|
| `a BETWEEN 1000000 AND 1100000` | 4.4 ms | 7.7 ms | 43.7 ms | 4.7 ms |
| `a < 3000000` | 145 ms | 20.5 ms | 44.0 ms | 13.6 ms |
| `e = 7` (no statistics) | 857 ms | 190 ms | 55.9 ms | 57.1 ms |

Over 30 million rows on 3 segments, 3 DataFusion threads per QE:

| Query | PostgreSQL | table AM | table AM, parallel 3 | direct PAX |
|---|---|---|---|---|
| grouped aggregate | 1.08 s | 0.18 s | 0.13 s | 0.07 s |
| filtered aggregate | 0.99 s | 0.19 s | 0.13 s | 0.06 s |
| `count(*)` | 0.75 s | 0.10 s | 0.08 s | 0.009 s |

### Receiving slices

Milestone M7a runs slices that receive rows through a Motion, below an
aggregate.  The receiving Motion stays on PostgreSQL; the main thread pulls
the rows it receives and hands them to DataFusion in batches.  That brings
the combining stage of a two-stage aggregate (Finalize Aggregate) into
DataFusion, on the coordinator and on the segments, for count, sum, min and
max, whose transition states are plain values (a combined count is 0 when
no partial count arrives).  A slice that only receives, with nothing to
compute, stays on PostgreSQL, as do avg (an array transition state),
DISTINCT aggregates (an aggregate over another) and sorted receives.

Over 30 million distributed rows, 3 DataFusion threads per QE:

| Query | PostgreSQL | DataFusion |
|---|---|---|
| `GROUP BY b HAVING count(*) > 1` (30 million groups) | 5.99 s | 1.93 s |
| `GROUP BY e HAVING sum(a) > 0` | 1.44 s | 0.19 s |
| `GROUP BY a % 1000` | 1.34 s | 1.34 s |
| `GROUP BY b % 1000`, three aggregates | 1.71 s | 1.94 s |

The last two are the planner's choice to skip the partial stage for an
expression key it has no statistics for: each segment then redistributes
10 million raw rows through a row-at-a-time Motion, and converting them
from and to DataFusion's batches on both sides costs more than DataFusion
saves.  Sending batches over the interconnect (M7b) is meant for that.

### Experimental: batches through Motions

With `datafusion.motion_batches = on` (milestones M7b and M7c), a Gather or
Redistribute Motion whose sending and receiving slices both run in
DataFusion carries Arrow IPC batches instead of tuples; EXPLAIN marks the
sending slice "sends Arrow batches".  Every process decides this from the plan and the synchronized
settings alone, so both ends agree.

- **Sending.**  The slice runs in place of the Motion node.  The workers
  encode the results; the main thread cuts the bytes into tuple chunks and
  sends them through the interconnect's own `SendTupleChunkToAMS`, then
  `SendEndOfStream`.  A stop request from the receiver ends the slice as
  it would end the Motion.
- **Receiving.**  The main thread takes the chunks with
  `RecvTupleChunkFromAny`, hands their bytes to the workers to decode and
  returns the receive buffer; it keeps the motion layer's end-of-stream
  count the way `cdbmotion.c` does for an unordered receiver.  A full
  decode queue is waited on until it has room, not by waiting for output.
- **Safety.**  The chunks carry a chunk type of their own, outside
  PostgreSQL's range, so a PostgreSQL receiver rejects them with an error,
  and our receiver rejects tuple chunks; each stream starts with a magic
  and the Motion's signature (its id and column types), which the receiver
  checks.  A mismatch can only fail the query, never be misread.

Over 30 million distributed rows with `gp_enable_multiphase_agg = off`, so
that every row is gathered to the coordinator's aggregate, 3 DataFusion
threads per QE:

| Query | PostgreSQL | DataFusion, tuple Motion | DataFusion, batch Motion |
|---|---|---|---|
| `count(*), sum(a), max(c)` | 1.45 s | 1.11 s | 0.53 s |
| five aggregates, `WHERE e < 50` | 1.00 s | 0.78 s | 0.58 s |

Then the coordinator waits for data and the senders wait in the UDP
interconnect's flow control: the transport is the limit (about 700 MB/s
here).

A Redistribute Motion (M7c) routes each row the way its PostgreSQL sender
would: `df_core::cdbhash` transcribes Cloudberry's distribution hash (key
hashes rotated and combined, Jump Consistent Hash over the segments, and in
parallel mode the receiving worker from a second jump) and the hash
functions of the supported types (`hashchar` for bool, `hashint2/4/8`,
`hashfloat4/8` with their handling of -0 and NaN).  Each receiver gets its
own stream.  Keys must be plain columns hashed by their type's own
non-legacy function; random distribution, legacy and cross-type hashing
stay on tuples.  `datafusion_debug_cdbhash_check(nrows, segments, workers)`
routes random keys, special values mixed in, through both `cdbhash()` and
the Rust code for every key type alone and combined, and counts the rows
they route differently: 0 over 120 million rows (1 to 128 segments, 1 to 3
workers), while a deliberately broken NaN case is caught.  An aggregate
would come out right with any consistent routing, so this check, not the
query results, is what shows the routes match.

| Query (30 million rows) | PostgreSQL | tuple Motion | batch Motion |
|---|---|---|---|
| `GROUP BY a % 1000` (every row redistributed) | 1.44 s | 1.24 s | 0.39 s |
| `GROUP BY b % 1000`, three aggregates | 1.54 s | 1.93 s | 0.80 s |
| `GROUP BY b` (30 million groups) | 5.98 s | 1.86 s | 1.07 s |

Limits: only Gather and Redistribute Motions between DataFusion slices (a
slice that only receives, such as a gather to the client, stays on tuples;
Broadcast Motions come with joins, which DataFusion does not run yet);
EXPLAIN ANALYZE shows the Motion as never executed and the interconnect's
per-Motion statistics are not updated; tested with the UDP interconnect.

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
| M5 | Segment slices below a sending Motion; partial aggregates |
| M6 | AO, AOCS and PAX tables; parallel mode; experimental direct PAX reader |
| M7a | Slices that receive through a Motion; combining aggregates |
| M7b | Arrow IPC batches through Gather Motions between DataFusion slices |
| M7c | Redistribute Motions with batches, routed by a checked transcription of cdbhash |

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
