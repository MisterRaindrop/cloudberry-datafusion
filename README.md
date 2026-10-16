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
arithmetic operators (and since T1 `date`, `time`, `timestamp` and
`timestamptz`, since X1 `text` and `varchar`, see below).  Since M5 this includes the segments' slices: a slice
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

### Direct PAX reader

`datafusion.pax_direct_read`, on by default (C4), reads PAX tables without the table AM's
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
  does not load, warns once per backend and reads through the table AM.  An
  extension built without it (no `DF_PAX_SRC`) reads PAX through the table
  AM and says so in the server log only.  The library is loaded only once a
  slice reads a PAX table.
- Cloudberry's parallel mode keeps the table AM path.

Every PAX scan of a slice is read this way (C3), beside inputs read
through the table AM or received from a Motion: the two sides of a join of
co-located PAX tables, a PAX table joined with a broadcast or
redistributed one.  The scans of one query share the account their
decoding memory is reported to.  Over TPC-H at scale factor 1 (both
planners, 44 runs) DataFusion took 11.6 s reading PAX directly against
15.1 s through the table AM, with no query slower.

It reads fixed-width columns, strings (text, varchar, char(n)) and
numeric(p, s) columns (C2); a scan of ctid goes through the table AM.
Numerics come out in PostgreSQL's representation and are read by
df_core::pgnum::from_pg_numeric, as df_numeric.c reads them on the main
thread (within 38 digits in i128, without allocating); the vectorized
format's 16-byte numerics are rebuilt in that representation by the
reader, since PAX's own conversion calls PostgreSQL's numeric functions.
On one segment of TPC-H at scale factor 1, summing four numeric(15,2)
columns of lineitem takes 43 ms against 154 ms through the table AM.  DataFusion's
partitions take micro-partitions one at a time; a partition taking one
counts its groups from the footer, reads the first and queues the others
(C1b), which partitions take before any new micro-partition, so that one
micro-partition is read by several partitions in parallel.  Every read is
one group (C4b), and a partition reads another only once the rows decoded
so far are taken: reading whole micro-partitions queued all their groups'
rows first, which at scale factor 10 took a QE to 600 MB on TPC-H's Q1.

Each group being decoded holds about 30 MB of a QE on TPC-H's lineitem
(the decoded columns, half of it, and the Arrow arrays built from them),
so a QE with 10 partitions peaks near 330 MB, against 90 MB through the
table AM; PAX's read buffer (pax.scan_reuse_buffer_size) makes no
difference.  `datafusion.pax_max_decoders` (C4c, 0 by default: one per
partition) bounds the groups decoded at once for a query, the other
partitions taking the rows decoded meanwhile.  Decoding being most of a
scan's work, it costs time about in proportion: on one segment at scale
factor 10, Q1's scan took 400 ms with no limit, 650 ms with 4 (270-300
MB) and 1 s with 2 (190-225 MB).  The reader hands out a group's
rows 8192 at a time, and each piece goes to a queue all partitions take
from (C2b): the work above the scan starts while the group is still being
converted, on every partition, not only the one reading.  Partitions
waiting for work sleep until a read queues some or ends; only partitions
holding work count as pending, since two that were only looking each
waited for the other, with nobody left to wake them.  Without that, a table of one
micro-partition per segment (TPC-H's orders at scale factor 1) was decoded
and filtered on one thread, slower than through the table AM; on one
segment it now takes 23 ms against 32 ms through the table AM.

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
  Chunks taken without bytes to decode (a stream's head, alone in the
  first packet of each sender, or its end) are taken all the same: the
  main thread comes back for more at once instead of waiting 10 ms for
  output, during which the senders, unacknowledged, stalled.  That cost
  about 12 ms a sender at every batch Motion: a join of seven TPC-DS
  tables (Q18's) grouped under the Postgres planner, six batch Motions,
  took 0.25 s and now 0.11 s, against 0.12 s through tuple Motions; the
  TPC-DS executions without a Shared Scan 70.7 s and now 58.6 s.
- **Safety.**  The chunks carry a chunk type of their own, outside
  PostgreSQL's range, so a PostgreSQL receiver rejects them with an error,
  and our receiver rejects tuple chunks; each stream starts with a magic
  and the Motion's signature (its id and column types), which the receiver
  checks.  A mismatch can only fail the query, never be misread.
- **Deciding.**  Which Motions carry batches is the largest set whose
  slices all run given the rest, found by checking and translating each
  slice until none drops out.  Each process now decides once per
  statement, at ExecutorStart or EXPLAIN, and keeps each slice's verdict
  while the set does not change; it used to decide again for every
  question about the plan.  TPC-DS Q14a under GPORCA, 32 slices, spent
  100 to 600 ms per decision and up to 13 decisions in each process as it
  started: its EXPLAIN took 0.95 s with the checks (0.5 s without) and
  takes 0.55 s, and the query 1.15 s and now 1.0 s; Q88 1.50 and 0.76 s,
  Q9 1.12 and 0.52 s.  All 206 TPC-DS executions, DataFusion alone, took
  72.0 s and now 69.3 s (medians of three runs).

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
`hashfloat4/8` with their handling of -0 and NaN, since X2 `hashtext`,
since B1 `hashbpchar`, and `hash_numeric`, whose NBASE digits a Decimal256
value at its column's scale gives).  Each receiver gets its
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

A split avg of float4 or float8 (M7d) passes DataFusion's state through
batch Motions instead of PostgreSQL's array: the partial stage sends
sum(x) and count(x), two columns of the stream, and the combining stage
divides their sums, NULL when the count is 0 (as `float8_avg`).  Since a
slice may then need its Motions to carry batches, the set of batch Motions
is the largest one for which every slice next to one of them runs in
DataFusion, computed from the plan alone.  Without batches, and for avg of
integers (numeric results), avg stays on PostgreSQL.  Over 30 million rows,
`GROUP BY e` with `avg(c)` took 0.18 s instead of 1.04 s on PostgreSQL,
`GROUP BY a % 1000` 0.56 s instead of 1.41 s.  As for float sums anywhere in
DataFusion, values that are not exact in binary may differ from
PostgreSQL's in the last digits, the rows being added in another order (as
they are between PostgreSQL plans with different parallelism).

A slice that only receives, such as the coordinator returning a Gather's
rows to the client, stays on tuples, and its Gather with it.  Running it in
DataFusion just to decode batches was tried and measured: results matched,
but over 30 million rows `COPY (SELECT ...) TO '/dev/null'` took 3.31 s
against 3.34 s with tuples and 3.38 s on PostgreSQL, and two filtered
queries were 5-10% slower.  Sampling the coordinator showed why: formatting
the rows for the client took 60-70% of its time either way, and while it
handed out a whole decoded batch it did not read the interconnect, so the
senders stalled in flow control (21% of its time waiting for chunks,
against 8% with tuples, which interleaves receiving and output row by row).
The coordinator is the bottleneck of such queries and the motion layer
offers no non-blocking receive to interleave with.

### Joins

Milestones J2 to J4 run hash joins, inner, outer (left, right, full),
semi and anti, whose inputs are scanned locally
(tables colocated on the join key) or received through a Redistribute or
Broadcast Motion, several levels deep, under an optional aggregate.  DataFusion builds its hash table on PostgreSQL's Hash
side; the main thread feeds the inputs one after the other, the Hash side
first, as PostgreSQL's Hash Join reads them; since the Hash side is
DataFusion's left, the join type turns around (a PostgreSQL left join is
DataFusion's right join, a semi join its right semi join).  Keys of two
integer or two float types are compared in the wider one; a join filter
and a filter above the join are evaluated as in PostgreSQL; NULL keys match
nothing.  Expressions that the side an outer join fills with NULLs computes
below the join (a subquery's `b IS NULL`, say) would be evaluated above it
by DataFusion and give another result for nulled rows, so such a join stays
on PostgreSQL.  Reading the Hash side to its
end before the other, also from the interconnect, keeps the deadlock
properties of PostgreSQL's plans.  With `datafusion.motion_batches` the
Motions feeding a join carry batches too, Broadcast included (its one
stream goes to every receiver).  A NOT IN anti join reads its outer side
first (NJ1, below): each Motion's sending slice sends to that join's slice
alone, so the inner side's senders only wait for room meanwhile.  IS NOT
DISTINCT FROM joins stay on PostgreSQL.

Hash joins spill what does not fit (SHJ, below), the default.  With
DataFusion's own hash join, which does not spill
(`datafusion.join_estimates` other than spill), a slice qualifies only if
the Hash node's estimated size (planner rows times width plus a per-row
allowance) fits its budget, `min(operatorMemKB, work_mem) *
hash_mem_multiplier`, as a PostgreSQL hash table's would; EXPLAIN gives the figures otherwise.  An
underestimate (stale statistics) can still end in an "out of memory" error
(53200) naming DataFusion's reservation, where PostgreSQL would have
spilled.

Over a 30-million-row table joined to a 2-million-row one, colocated, 3
DataFusion threads per QE (results identical):

| Query | PostgreSQL | DataFusion |
|---|---|---|
| `count(*), sum, max` over the join | 3.01 s | 0.26 s |
| the same with filters on both sides | 0.89 s | 0.23 s |
| `GROUP BY` a column of the build side | 4.17 s | 0.24 s |

With the 2-million-row table distributed on another column, so that it is
redistributed on the join key:

| Query | PostgreSQL | tuple Motions | batch Motions |
|---|---|---|---|
| `count(*), sum, max` over the join | 3.17 s | 0.36 s | 0.31 s |
| `GROUP BY` a column of the build side | 4.29 s | 0.39 s | 0.32 s |

Outer, semi and anti joins over the same tables (colocated unless noted):

| Query | PostgreSQL | DataFusion |
|---|---|---|
| `LEFT JOIN`, `count(*), count(b), sum(a)` | 3.68 s | 0.25 s |
| `NOT EXISTS` | 3.20 s | 0.25 s |
| `EXISTS`, redistributed, batch Motions | 2.99 s | 0.31 s |

Broadcasting it instead (joining on another column) would put about 100 MB
in each segment's hash table, beyond the 64 MB budget, so that slice stays
on PostgreSQL.

Limits: only Gather, Redistribute and (with joins, J3) Broadcast Motions
between DataFusion slices (a slice that only receives stays on tuples, see
above);
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
| M7d | Split avg through batch Motions with DataFusion's state |
| J1 | Plan tree over several inputs (groundwork for joins) |
| J2 | Inner hash joins over local scans, with a memory guard |
| J3 | Joins over Redistribute and Broadcast Motions; Broadcast batches |
| J4 | Outer, semi and anti joins; keys of two integer or float types |
| T1 | `date`, `time`, `timestamp` and `timestamptz` |
| X1 | `text` and `varchar`; batch size from the row width |
| X2 | Redistribute batches by `text` and `varchar` keys (`hashtext`) |
| L1 | `LIKE` and `NOT LIKE` |
| L2 | Common string functions and `\|\|` |
| N1 | numeric results of `sum(int8)` and `avg` of integers |
| N2 | `numeric(p, s)` columns with p <= 38 |
| N3 | numeric `+`, `-`, `*`; Decimal256 |
| E1 | `IN`/`NOT IN` lists, `CASE`, `COALESCE`, `NULLIF`, `IS [NOT] DISTINCT FROM` |
| E2 | Casts between integers, floats, date/timestamp and numeric |
| A1 | Aggregates below a join or another aggregate |
| SQ1 | Subquery Scan; batch Motions below Subquery Scan and Append |
| RI1 | RowIdExpr: semi joins deduplicated after the join |
| R1 | GPORCA's Result: a projection and filter over its child |
| D4 | A Limit over a GroupAggregate below a sorted Motion |
| D5 | Sorted Gathers carry batches where the order is not read |
| D6 | GroupAggregates below a Hash Join or Hash, whose order no one reads |
| S2 | GPORCA's Result over the slice's Sort |
| D7 | GroupAggregates whose order the slice's rows leave in, sorted after |
| GS1 | GROUPING SETS, ROLLUP and CUBE; `GROUPING()`, `GROUP_ID()` |
| DN1 | numeric of display scales of their own: `/`, CASE and COALESCE of mixed scales |
| AV2 | avg returning numeric as a sort key and above its Agg, with its display scale |
| WIN1 | Window functions: row_number, rank, dense_rank; count, sum, min, max over a frame |
| WIN2 | avg of numeric and of integers over a frame, with its display scale |
| UA1 | UNION ALL (Append) |
| SO1 | INTERSECT and EXCEPT (SetOp) |
| AS1 | GPORCA's Assert of a scalar subquery's rows |
| SR1 | D3 below Subquery Scans |
| CL2 | EXPLAIN checks a segment slice by the segments' collation |
| CU1 | The default collation C.UTF-8 orders strings in DataFusion, as C |
| CU2 | Comparisons and min/max of strings under a libc collation of a language, by strcoll_l |
| NJ1 | NOT IN anti joins, null-aware |
| NL1 | Nested Loops without parameters; Materialize |
| TID1 | ctid and gp_segment_id of scans; tid columns |
| AVG1 | avg returning numeric inside expressions and below the top |
| WT1 | Numeric values received as tuples with up to 76 digits |
| C1 | Direct PAX reader: strings (text, varchar, char(n)) |
| C1b | Direct PAX reader: a micro-partition's groups read in parallel |
| C2 | Direct PAX reader: numeric(p, s) columns |
| C2b | Direct PAX reader: decoded rows shared by all partitions as they come |
| C3 | Direct PAX reader: every PAX scan of a slice, beside other inputs |
| C4b | Direct PAX reader: one group per read, memory bounded by the partitions |
| C4c | datafusion.pax_max_decoders: fewer groups decoded at once, for memory |
| C4 | Direct PAX reader on by default |
| JE1 | Hash joins whose build side comes from a join: bounded by its largest input |
| SHJ | Hash joins that spill (datafusion.join_estimates = spill, the default) |
| CL1 | Strings ordered by the default collation where it is C on every node |
| IP2 | The slices below an init plan's top slice, with batch Motions between them |
| NR1 | Numeric columns through Append, GPORCA's Sequence and Shared Scan; sums 19 digits wider than their argument |
| PS1 | Partial sum and avg through tuples both ways: PostgreSQL's states, written and read by the C side |

JE1: DataFusion's hash join cannot spill: a build side larger than the
memory pool fails the query ("out of memory") where PostgreSQL's would
spill.  The planner hook keeps a Hash Join on PostgreSQL when its build
side's estimate exceeds the Hash node's budget, but a join's estimate can
be far off: TPC-H Q9 at scale factor 10 builds on partsupp joined with
lineitem, estimated at 40 rows and holding about a million per segment.
`datafusion.join_estimates = bounded` therefore takes a build
side coming from a join to hold up to as many rows as its largest input
(any node below it, across Motions); strict keeps every such join on
PostgreSQL, trusted believes the planner.  Over TPC-H, bounded made every
run complete and match PostgreSQL, at a cost: 12.9 s instead of 10.8 at
scale factor 1, 109 s instead of 77 at scale factor 10 (where Q9 failed),
against 207 s on PostgreSQL.  A hash join that spills would remove it.

SHJ: hash joins spill.  With `datafusion.join_estimates = spill`, the
default, a hash join runs in DataFusion whatever the size of its build
side, and a slice no longer stays on PostgreSQL for it; a NOT IN anti join
(null-aware) and a nested loop are checked as with trusted.  Each hash join
hash-partitions both sides (no table collected for all partitions on the
planner's estimate) and is wrapped in `SpillingHashJoinExec`
(`rust/df_core/src/spilljoin.rs`), a grace hash join around DataFusion's:

- a partition collects its build side under a reservation of the batches'
  size and 40 bytes a row for the hash table, and, read to its end, of what
  DataFusion's join reserves beyond: its hash table as it estimates it (on
  one integer key, the most an array over the keys' range may take, which
  it builds instead where the keys are dense), and the copy it concatenates
  the batches into while it still holds them (apache/datafusion#25496;
  TPC-DS Q4 failed for 1.2 MB without it); when it fits, DataFusion's
  in-memory hash join runs on it, taking that reservation over (a pool of
  its own drawn from it): freed and reserved again, the bytes could go to
  another consumer in between, and the join, which cannot spill, finds no
  room in a pool that consumers which can spill have filled (DataFusion's
  fair pool bounds each of those by its share, not their sum: TPC-H Q21 on
  GPORCA at scale factor 10 failed so, with repartition buffers holding
  165 of its 192 MB);
- past the reservation, the build rows and then the probe rows are written
  to 16 spill files by their keys' hash, a bucket's rows gathered into
  batches of up to 1 MB or the batch size, and the buckets are joined one
  after the other; one that still does not fit is split again with another
  hash, four times at most, then joined in memory if the pool has the room
  (rows of one key cannot be split).  A batch read back is copied: the
  batches of a spill file share the chunks it is read in, and each would
  count a whole chunk (twenty rows counted as 128 kB, which split every
  bucket down to the last depth);
- rows of equal keys share a bucket, so inner, outer, semi, anti and mark
  joins, with or without a join filter, give the rows they give in memory;
  the build side is read to its end before the probe side, the order in
  which the main thread feeds a slice's inputs.

Over TPC-H every query matched PostgreSQL, Q9 at scale factor 10
included: 11.5 s at scale factor 1 (bounded 12.9, trusted 10.8), 83.8 s at
scale factor 10 (bounded 109, trusted 77 with Q9 failing), against 223 s
on PostgreSQL in the same run.  Under `work_mem` of 512 kB to 2 MB, eleven
joins spilling on one segment matched PostgreSQL.

NJ1: `x NOT IN (SELECT k ...)` is Cloudberry's Hash Left Anti Semi
(Not-In) Join.  PostgreSQL's executor returns no row once the inner side
has a NULL key, and drops an outer row with a NULL key unless the inner
side is empty, each process over the inner rows it has.  DataFusion's
null-aware anti join does the same, but only as a left anti join, which
builds its table on the side it keeps: the outer one is the left side, fed
first, and must fit the Hash node's budget.  One key and no join filter;
NOT IN over several columns stays on PostgreSQL.

NL1: a Nested Loop without parameters (GPORCA's TPC-H Q11 compares each
group with a broadcast total in its join filter) runs as DataFusion's
nested loop join, inner, left, semi or anti.  It collects PostgreSQL's
inner side, often a Materialize, which passes its child's rows on, and
streams the outer one, so the inner side is fed first and must fit the
inner node's budget.  A parameterized one, which scans its inner side again
for each outer row, stays on PostgreSQL.

### Aggregates below a join or another aggregate

A1: an aggregate below a join, or below another aggregate, runs as a node
of the plan whose groups and calls the nodes above read by name
(`q<plan node id>_g<i>`, `_a<k>`), as TPC-H Q2, Q13, Q15 and Q20 have
them: a grouped sum compared with a column in a join filter, a count over
counts, a combining aggregate over a batch Motion below a join.  It
computes every call of its target list.  Its columns are made below the
join, so they may be on the side an outer join fills with NULLs; an
expression over them in its target list may not (`coalesce(max(k), -1)`
is NULL above an unmatched row, not -1).  An avg returning numeric and a
sum of branches of different scales go up with their display scale
beside them (DN1, AV2); a partial stage, whose state only a batch Motion
carries, stays on PostgreSQL there.

SQ1: a Subquery Scan passes its plan's rows on, as the Postgres planner
leaves one over a view or a subquery in FROM (TPC-H Q15): its columns
read its plan's, and its own filter runs over them.  An aggregate below it
is not the slice's top one, so it runs as A1 has it.  The Motions below a
Subquery Scan or an Append now count when choosing batch Motions; before,
they always carried tuples, so a partial sum below one stayed on
PostgreSQL.  Those of init plans count too since IP2.

RI1: for a semi join whose outer side is small, Cloudberry's planner
joins first and drops the copies afterwards (JOIN_DEDUP_SEMI, TPC-H Q4 and
Q21): RowIdExpr numbers the rows of one side, unique within the query, and
an aggregate grouped by those numbers keeps one row of each, its other
columns as they are.  DataFusion numbers the rows of the scan or join whose
output has the RowIdExpr as a column of that node (`df_core::rowid`: the
segment's dbid shifted left by 48 bits, as PostgreSQL's, plus a counter
the partitions share), so the copies the join makes share a number.  An
aggregate's column it does not group by, which the planner puts there
only where the groups determine it, is any value of its group
(`first_value`).

R1: GPORCA puts a Result over aggregates and joins to filter (a HAVING,
a test of an outer join's NULLs) and compute columns.  It runs as a filter
over its child's rows, its columns read the child's, and an aggregate
below it is not the slice's top one (A1).  One without a child or with a
one-time filter stays on PostgreSQL.

### Date and time types

`date`, `time`, `timestamp` and `timestamptz` travel as PostgreSQL stores
them: days (`date`) or microseconds from 2000-01-01 (`time`: from
midnight), in Int32 and Int64 Arrow columns, with no conversion when
scanning, sending or returning rows.  The infinities are the integers'
extremes, and `timestamptz` is UTC, so comparing two values of one type and
`min`/`max` are integer comparisons that agree with PostgreSQL for every
value, including the infinities and the ends of the range, in any
`TimeZone`.  Constants arrive folded by the planner (`'today'`, `'2024-05-01
00:00+08'`).  These columns can be filtered, grouped, joined on, counted
and distribution keys (cdbhash with `hashint4` for date, `time_hash` and
`timestamp_hash`, which are `hashint8`), and the direct PAX reader skips
blocks by their min/max.

Arithmetic (`d - 1`, `d1 - d2`, anything returning `interval`) checks for
overflow and infinities and stays on PostgreSQL, as do comparisons between
two of these types (`ts > date`, `tz > ts`), which convert one side first,
but for a `date` against a `timestamp` constant: the planner folds
`d < date '1994-01-01' + interval '1' year` into one, as in most TPC-H
queries.  PostgreSQL compares the date's midnight with the timestamp,
dates past the timestamp range after every finite one; DataFusion compares
the date with the constant rounded to a date (up for `<` and `>=`, down
for `<=` and `>`; `=` and `<>` on a constant within a day are constant),
which gives the same answer for every date, infinities and dates past the
timestamp range included, as each lies on the same side of the rounded
constant as of the timestamp.

`extract(year | quarter | month | day from date)` returns `numeric` of
scale 0, computed as PostgreSQL's `extract_date` (its `j2date`, no year 0).
The year of an infinite date is `±Infinity`, which `numeric(p, s)` columns
cannot hold: DataFusion holds them as two values beyond 76 digits that
sort below and above every number (and below NaN), so grouping, sorting,
`min`/`max` and comparisons at scale 0 on such a year agree with
PostgreSQL, and they come back as `±Infinity`.  Arithmetic, `sum`/`avg`,
casts and comparisons that rescale such a year (`> 1995.5`) would take an
infinity for a number and stay on PostgreSQL; the other fields of an
infinite date are NULL and have no such limits.

Over 20 million heap rows on 3 segments, counting one year by a `date`
range with `min`/`max` of a timestamp took 0.15 s in DataFusion and 1.00 s
in PostgreSQL; grouping by `date` with a `timestamp` filter took 0.17 s and
0.89 s (only the segments' slice in DataFusion; the coordinator sorts and
limits).

DT3: a date plus or minus a number of days (`date + integer`, `integer +
date`, `date - integer`) runs as date_pli and date_mii do
(df_core::pgdate::add_days): an infinite date stays as it is, a result
outside the dates PostgreSQL holds raises 22008 "date out of range".
TPC-DS Q72 (`d1.d_date + 5`) runs in DataFusion, 0.37 s against 0.82 s
under the Postgres planner.

### tid

TID1: GPORCA turns a semi join into a join and drops the copies by the
rows' ctid and gp_segment_id (TPC-H Q21), grouping by them and
redistributing by them.  A scan now reads those two system columns
(slot_getsysattr: tts_tid, and the segment's index), and a tid column is
an int8, the block number shifted left by 16 bits with the offset below,
ordered as tids are; the C side makes ItemPointerData of it again.  A
Redistribute by tid hashes as hashtid does: hash_any of the block number's
halves and the offset, in memory order, which datafusion_debug_cdbhash_check
checks.  A tid constant, other system columns, and a statement that carries
ctid to its result for WHERE CURRENT OF stay on PostgreSQL.

### Text and varchar

In a UTF-8 database, `text` and `varchar` columns travel as Arrow string
columns: the main thread detoasts each value and copies its bytes, and
results come back as new `text` values.  DataFusion compares the bytes,
which agrees with PostgreSQL as follows:

| Operation | Runs in DataFusion when the collation is |
|---|---|
| `=`, `<>`, GROUP BY keys, hash join keys | deterministic (any libc or ICU collation that is not `deterministic = false`) |
| `<`, `<=`, `>`, `>=`, `min`, `max` | C or POSIX; the default collation also where it is libc's C.UTF-8 (CU1); any deterministic libc collation by `strcoll_l` (CU2) |
| ORDER BY, merge keys, window order | C or POSIX, or C.UTF-8 (CU1) |

Anything else stays on PostgreSQL with the reason in EXPLAIN (`operator <
on text under a collation other than C`).  Whether the database's default
collation is C is decided by each node itself: the coordinator and the
segments can disagree (a cluster initialized with `C` on the coordinator
and `C.UTF-8` on the segments does), so EXPLAIN shows the coordinator's
verdict, a segment may run the same slice on PostgreSQL, and the Motions
of a slice whose verdict depends on the default collation carry tuples
rather than batches, so that every node agrees on which Motions carry
batches.  The exception is a Motion read by a part of the slice that does
not depend on it: in TPC-H Q1 the aggregate under a Sort by `char(1)`
columns reads the batches of a split numeric sum; every node runs that
aggregate in DataFusion, and the Sort above it too where the collation is
C, or on PostgreSQL where it is not.  An explicit `COLLATE "C"` is the
same everywhere and keeps the batches.

CU1: libc's C.UTF-8 orders by code point, which for UTF-8 is the order
of the bytes, so a default collation of C.UTF-8 counts as C where the
node checks that it does: strcoll must order a few pairs that any
language's collation orders otherwise (case, punctuation, accents, a code
point beyond U+FFFF against one below it) as their bytes.  The
coordinator asks each segment the same, comparing those pairs in a scan
of `pg_database` under its default collation.  glibc 2.34 in the
development container passes, and 118 thousand strings of every BMP code
point and some beyond sort alike under C.UTF-8 and C, and alike in
DataFusion and PostgreSQL.

CU2: under a deterministic libc collation of a language (en_US.utf8),
`<`, `<=`, `>`, `>=` (and BETWEEN) of strings and their min and max run in
DataFusion by `strcoll_l` with the locale the node orders by, then the
bytes where it finds two strings equal, as varstr_cmp
(`df_core::pgcoll`): the database's LC_COLLATE for the default
collation, a collation's collcollate otherwise.  glibc's `strxfrm` keys
would let DataFusion sort by them, but on glibc 2.34 they order 540 of
400 thousand adjacent pairs of ordinary strings otherwise than strcoll
(`'04z'` and `'0_4z'`), so sorts stay on PostgreSQL under such a
collation.  Over 3 million rows under en_US.utf8, a count with `t < '8'`
took 35-54 ms against 161-198 ms, a grouped min and max 122-133 ms
against 318-357 ms.

CL1: where the default collation is C (or POSIX, by libc, or C.UTF-8 by
CU1) on every node,
ordering by it no longer depends on the node.  The coordinator asks once
per session, before its first query with `datafusion.mode` on is planned
for EXPLAIN or dispatched, querying `pg_database` on every segment, and
sets the internal `datafusion.cluster_collation_c` accordingly (a value
set by hand is put back); the segments' running processes get the SET,
those started later every synchronized setting.  Each process's checker
then reads the same value, and the Motions of such slices carry batches:
in TPC-H Q7 on GPORCA the Sort below the combining aggregate kept its
slice on PostgreSQL, and the partial sums below it in tuples.  A database
created without `LC_COLLATE` takes each node's own template's, so a
cluster initialized with different locales needs `CREATE DATABASE ...
TEMPLATE template0 LC_COLLATE 'C'` for this.  Asking costs about 4 ms
once per session.  CL2: the same question records the segments'
`LC_COLLATE` and `LC_CTYPE`, and EXPLAIN reports a slice the segments
run as they check it: with the coordinator at C and the segments at
C.UTF-8 (as in the development container), a Sort by a text column under
the default collation used to read "eligible" while the segments kept it
on PostgreSQL.  `ILIKE`, regular expressions, `md5`, `initcap` and other
string functions not listed below stay on PostgreSQL for now, as do `char(n)`, `name` and databases in
other encodings.  The direct PAX reader hands out strings too (C1), as
offsets and bytes like the main thread's batches: char(n) keeps its
padding, PAX's toast is decoded on the worker with PAX's own pglz/lz4, and
files of the vectorized format (no headers, char(n) trimmed) go through
the column's GetDatum, which restores both.  A group whose strings take
more than 64 MB is handed out in several pieces, so that Arrow's int32
offsets cannot overflow.

`LIKE` and `NOT LIKE` (L1) run through `df_core::pgstr`, a transcription
of PostgreSQL's matcher for UTF-8 (`UTF8_MatchText`): `%`, `_` as one
character, backslash escapes, newlines matched by `%`.  In a UTF-8
database it does not depend on the locale, so any deterministic collation
qualifies (PostgreSQL rejects LIKE under a nondeterministic one).  A
constant pattern is prepared once: without `_` it becomes literal pieces
between `%`'s, checked with prefix, suffix and substring searches.  A
pattern ending in an unescaped backslash raises an error in PostgreSQL
only on rows whose matching reaches it, so such a constant pattern stays
on PostgreSQL; a pattern from a column goes through the same matcher and
raises the same error (22025) on the same rows, and runs behind the
guards of AND and OR like arithmetic does.  Over 20 million rows, `LIKE
'%00042%'` took 0.24 s against 1.04 s in PostgreSQL, `LIKE 'city-4%'`
0.23 s against 0.72 s.

String functions (L2), identified by their pg_proc OID so that overloads
on other types do not slip through, are transcribed in `df_core::pgstr`
from varlena.c and oracle_compat.c, counting characters, not bytes,
wherever PostgreSQL does:

| Functions | Collation |
|---|---|
| `length`, `char_length`, `octet_length`, `substr`/`substring(s, n [, len])`, `text \|\| text`, `concat` of text, `btrim`/`ltrim`/`rtrim`, `left`, `right`, `reverse`, `repeat`, `lpad`, `rpad` | any |
| `strpos`/`position`, `replace`, `split_part`, `starts_with` | deterministic (PostgreSQL rejects the others) |
| `lower`, `upper` | C or POSIX (ASCII letters only, as PostgreSQL maps them there) |

Their errors keep PostgreSQL's SQLSTATEs (negative substring length 22011,
field position zero 22023, results over 1 GB 54000), and calls that can
raise them run behind the AND/OR guards.  `text || integer`, `concat` with
other types and `VARIADIC` calls stay on PostgreSQL.  Whether the default
collation's LC_CTYPE is C is each node's own, so `lower` and `upper`
under it are decided per node like ordering comparisons (in this
container PostgreSQL itself takes the multibyte path on the coordinator,
and these calls stay there).  A test runs every function over 6,500
combinations of edge values (empty, multibyte and 4-byte strings,
separators that overlap, integers from INT_MIN to INT_MAX, NULLs) with
DataFusion off and on: identical.  Over 20 million rows,
`upper(city COLLATE "C") || '/' || btrim(cust, 'c') = ...` took 0.27 s
against 1.00 s in PostgreSQL, grouping by `split_part(city, '-', 2)` 0.25 s
against 1.02 s.

Redistribute Motions by a `text` or `varchar` key carry batches too (X2):
cdbhash calls `hashtext` with the default collation, which is always
deterministic, so it is `hash_any` of the value's bytes, which
`df_core::cdbhash::hash_bytes` transcribes (the little-endian byte path,
equal to the word path; big-endian builds keep such Motions on tuples).
`datafusion_debug_cdbhash_check` routes random strings of every tail
length, multibyte characters included, alone and with other keys.  Joining
two 2-million-row tables on a text key, both sides redistributed as
batches, took 0.32 s against 0.93 s in PostgreSQL (LEFT JOIN 0.25 s
against 1.06 s, NOT EXISTS 0.21 s against 0.95 s).

Batches hold about 256 kB of the widest row the planner expects in the
slice (at most 8192 rows), the same on the C side and in DataFusion,
since DataFusion's operators allocate in proportion to a batch's bytes and
a partition's share of the memory budget is sized for that; the C side
also ends a batch at 4 MB of one string column.  A table without
statistics on wide strings (the planner assumes 32 bytes) can therefore
fail with `out of memory` (53200) and DataFusion's reservation in the
detail, as stale statistics do for joins; ANALYZE fixes it.

Over 20 million heap rows on 3 segments: an equality filter on a `text`
and a `varchar` column took 0.24 s in DataFusion and 0.98 s in
PostgreSQL; grouping by a `varchar` of 500 values with `max(cust COLLATE
"C")` 0.26 s and 1.00 s; grouping by a `text` of 200,000 values 0.67 s
and 1.57 s.  Grouping 300,000 keys of 1 kB with `work_mem = 4MB` spills
in both and took 2.16 s in DataFusion against 1.42 s.

### character

B1 runs `char(n)` (bpchar) columns, the most frequent reason TPC-H slices
stayed on PostgreSQL.  Values travel as stored, blank-padded, so they
print as PostgreSQL prints them and match LIKE padded (`bpcharlike`:
`'ab'::char(5) LIKE 'ab'` is false).  Everything that compares them
does so without trailing blanks, as `bpcharcmp` and `hashbpchar` do:
comparison operands, IN lists and join keys go through
`pg_bpchar_key`, grouping keys and DISTINCT arguments group by it
(keeping one of the group's padded values), sort keys sort by it (a tab
then sorts before the end of the value, not after the padding), and a
Redistribute Motion hashes it (checked against cdbhash with random
padding).  The cast to text (and varchar) is that key; `length` counts
without the blanks, `octet_length` with them; `concat` prints them.
min and max of character stay on PostgreSQL (they compare without the
blanks but return the padded value).  Over the TPC-H and TPC-DS queries
of Cloudberry's regression tests, slices DataFusion can run went from
23% to 44% (Postgres planner) and from 21% to 39% (ORCA).  A Q1-like
grouping by two char(1) columns over 10 million rows took 0.23 s
against 1.03 s; with an ORDER BY under the database's default collation
the aggregate still reads batches (TPC-H Q1 at scale factor 1 on PAX:
0.27 s against 1.35 s).

A grouping key or DISTINCT argument that is a table's `char(n)` column,
followed through the nodes above the scan as numeric precision is (an
Append needs it of all its inputs, with one n), groups by the value as
stored: every value is padded to n, so equal values look alike, and
neither the key nor a group's padded value is computed.  The type
modifier of a reference does not tell: GPORCA gives an Append of
char(5) and char(10) the first.  `pg_bpchar_key` cuts the blanks by
bytes (a blank never ends a character of more than one byte) into a
builder sized for them.  Grouping 2.34 million rows by four char(50)
TPC-DS item columns (Q22's body, GPORCA's two-stage plan) took
0.16 s and 0.66 CPU-seconds, against 0.27 s and 1.49 before and
0.69 s and 1.07 on PostgreSQL; Q22's five-level union of such groupings
0.49 s and 2.52 CPU-seconds, against 0.70 s and 4.47 before and 0.74 s
and 3.47.

Such a column crosses a batch Motion without its trailing blanks (CP1),
most of the bytes of a wide `char(n)`: the sender cuts them as
`pg_bpchar_key` does (a Redistribute hashes the same value either way),
and the receiver pads each value back to n characters, not bytes, so
it arrives as stored.  Both ends find n from the Motion itself, and it
is part of the Motion's signature, so a disagreement fails instead of
misreading.  A `char(1)` is sent as it is.  Over TPC-DS at scale factor
1 (medians of three runs, both planners), DataFusion took 79.2 s
against 83.3 s before; Q67 under GPORCA 1.42 s against 1.97, Q22 0.60 s
against 0.82.

### numeric results of integer aggregates

`sum(int8)` and `avg` of `int2`, `int4` and `int8` return numeric (N1).
DataFusion adds the values exactly as numeric of scale 0 (Decimal256 since
N3); the C side
builds the numeric values with `numeric_in`, and avg as `numeric_div(sum,
count)`, which is what `int8_avg` and `numeric_poly_avg` compute, so
values and display scales are PostgreSQL's (`1.5000000000000000`, sums
beyond int8).  Grouped by the distribution key the aggregate runs in one
stage; split in two, its partial stage would have to send PostgreSQL's
serialized state, so it stays on PostgreSQL unless batch Motions carry
DataFusion's state instead (the exact sum, and the count for avg, as for
avg of floats in M7d).  An avg returning numeric is divided where tuples
are made, so it does not go into a batch Motion itself.  Computing on
an avg's result (`avg(a) + 1`), whose scale depends on the values, came
with AVG1 and AV2, below.  Over 30 million heap rows, `sum(b), avg(a),
avg(b)` took 0.26 s with batch Motions against 1.04 s in PostgreSQL, and
grouped by a column 0.31 s against 1.46 s.

### avg inside expressions

AVG1: numeric_avg divides the sum by the count at a scale it picks by the
values (select_div_scale: 16 significant digits, not below the sum's), so
avg of numeric or of integers had no fixed scale and stayed whole output
columns, which the C side divides.  DataFusion now computes it inside an
expression, a HAVING, a join filter or an aggregate below the top (TPC-H
Q17's `l_quantity < 0.2 * avg(l_quantity)`): df_core::pgnum::pg_avg picks
the same scale, rounds half away from zero as div_var does, and gives the
value at the largest scale numeric_avg may pick, 36 + 4 * ceil(s / 4) for
a sum of scale s (a dividend of at least 10^-s, a count below 10^20).
Comparing, grouping and hashing by it read the value only.  Its display
scale is not PostgreSQL's, so it left DataFusion only through a batch
Motion: as an output column PostgreSQL finished the expression over the
avg the C side divides, or the slice stayed on PostgreSQL, until AV2
gave it its display scale.  A numeric
constant compared with it may have up to 76 digits.

WT1: a numeric value received through a tuple Motion is read into all 76
digits of Decimal256 (df_numeric_value_wide), not only the 38 of int128:
TPC-H Q22 under GPORCA broadcasts an avg PostgreSQL computes on the
coordinator, held at scale 40 by the slices receiving it.  Values within
38 digits keep the int128 path.

### numeric(p, s) columns

A column declared `numeric(p, s)` with p <= 38 (N2) holds values whose
display scale is s, so each is an integer times 10^-s: it travels as
`Decimal256(76, s)` (Decimal128 in N2), read on the main thread from
PostgreSQL's representation (`df_numeric.c`, without a copy for short
headers) and written back through `numeric_in`.  NaN, which such a column
may hold, is `i256::MAX`, above every 76-digit value: PostgreSQL sorts NaN above all
numbers and NaN equals NaN, so comparisons, min/max, grouping and join keys
need nothing more, and `sum`/`avg` add the other values and return NaN if
any was NaN.

| Operation | Condition |
|---|---|
| comparisons with columns and constants, join keys | both sides within 76 digits at the larger of their scales (they are rescaled to it) |
| `+`, `-` (N3) | scale max(s1, s2), at most 76 digits: max(p1 - s1, p2 - s2) + scale + 1 |
| `*` (N3) | scale s1 + s2 (`mul_var`'s rscale), at most 76 digits: p1 + p2 |
| integers made numeric (`a + id`) | scale 0 |
| `min`, `max`, GROUP BY keys | a column of p <= 38 or such an expression |
| `sum`, `avg` | of at most 66 digits, so that sums stay within 76; avg is `numeric_div(sum, count)` on the C side, as `numeric_avg` |

The planner hook finds precision and scale from the column's declared type
through every reference above it (`df_numeric_ps`), including a partial
aggregate's result below a Motion, and bounds arithmetic by the digits its
operands may have, not by their values, so nothing can overflow at run
time.  That is why numeric travels as Decimal256: `price * (1 - disc) * (1
+ tax)` over `numeric(15,2)` may have 47 digits.  Columns of plain
`numeric`, `%` and unary minus stay on PostgreSQL (`/` since DN1, below;
`abs`, which keeps its argument's digits and display scale, since WIN2's
batch, and `round(x, s)` of a constant s, RD1: numeric_round, half away
from zero at scale s, its display scale); the direct PAX reader reads numeric
columns too (C2).  Over 20 million rows of `numeric(15,2)` columns,
a query shaped like TPC-H Q1 (`sum(price * (1 - disc) * (1 + disc))` and
six more aggregates grouped by a flag) took 0.55 s with batch Motions
against 2.31 s in PostgreSQL.

A CASE or COALESCE whose branches have different scales (`CASE WHEN
p_type LIKE 'PROMO%' THEN price * (1 - disc) ELSE 0 END` in TPC-H Q8 and
Q14) returns values of different display scales, row by row, so it has no
one Decimal256 scale; DataFusion ran it only as the argument of `sum`
(MS1), and since DN1 anywhere.  PostgreSQL's sum shows the largest display scale of the values it adds
up: DataFusion adds the values at the largest scale of the branches and
keeps, beside the sum, the largest scale of the branches the values came
from (NULL values left out, as `sum` leaves them), split through batch
Motions as a second state column; the C side rounds the sum to that scale,
which only drops zeros.  A group whose rows all took `ELSE 0` shows `0`,
as in PostgreSQL, not `0.0000`.  `avg` of such an argument stays on
PostgreSQL.

An uncorrelated scalar subquery becomes an init plan whose value
(`$0`) the rest of the plan reads: TPC-H Q11, Q15 and Q22 compare an
aggregate or a column with one.  Its value is known before a slice starts
(the coordinator computes it before dispatching, `preprocess_initplans`,
and the segments receive it), so DataFusion takes it as a constant.  A
numeric value has no declared scale, so it is only compared, with a
numeric of known scale and at most 38 digits: the value is rounded to that
scale (up for `<` and `>=`, down for `<=` and `>`; `=` and `<>` with a
finer value are constant), and a value beyond every value of the other
side becomes the infinity on its side, which keeps NaN above it as in
PostgreSQL.  Whether a slice runs in DataFusion depends on the plan only,
not on the value.  Other parameters (of a correlated subquery, a Nested
Loop or a prepared statement) stay on PostgreSQL.

IP2: an init plan has slices of its own.  Its top slice runs on the
coordinator, on PostgreSQL's executor, when the coordinator sets the init
plan's parameters before the query (`preprocess_initplans`): the executor
hook never sees it, so it counts as a slice that does not run in
DataFusion, and the Motion into it carries tuples.  The slices below it are
like any other, found by the Motion they send through (`subplan_sliceIds`
maps an init plan to its top slice, the one without a parent), and the
Motions between them count when choosing batch Motions: TPC-H Q15's
maximum revenue sums by supplier in DataFusion, the partial sums passing to
the final ones as batches.  EXPLAIN lists these slices too.

### numeric of display scales of their own

DN1: a numeric value carries its display scale, and some expressions
give each row its own: a quotient, whose scale `select_div_scale` picks
from the pair of values (16 significant digits, not below either
operand's display scale); a CASE or COALESCE whose branches have
different scales (TPC-DS Q67's `coalesce(price * quantity, 0)`); a sum of
such values, which shows the largest display scale added up; and
arithmetic on them.  DataFusion carries such a value at the largest scale
it may have and keeps its display scale apart, as numeric.c gives it.

The largest scale is bounded by the operands' precision and scale, not
by their values: a quotient's is that of the smallest dividend, 10^-s1,
over the largest divisor, below 10^(p2 - s2) (TPC-DS Q36's
`sum(net_profit) / sum(ext_sales_price)` over `numeric(7,2)`, 44 digits
after the point, 71 in all), and the planner hook keeps an expression
whose bound passes 76 digits on PostgreSQL, so nothing overflows at run
time.  df_core::pgnum::pg_div divides as div_var does, exactly and
rounding half away from zero at that row's scale; 0 as a divisor raises
PostgreSQL's 22012, and an AND or OR runs the division only on the rows
its earlier arguments leave undecided, as PostgreSQL does.  Comparisons,
sorting and join keys read the value only.  The display scale is computed
only where it matters: where the value leaves DataFusion (beside it in an
output column, from which the C side gives it the scale, as for MS1's
sums), as an operand of `/`, and added up.  It follows numeric.c: the
larger of the operands' for + and -, their sum for *, select_div_scale's
for /, the branch's for a CASE or COALESCE, the largest of the values for
a sum; a value received from a Motion brings its own, read from the
tuple.  A Motion of such a column carries tuples; a sum's partial state
passes the scale as MS1's does.  min, max, avg, GROUP BY and Redistribute
keys of such a value stay on PostgreSQL.

Over 20 million rows of `numeric(15,2)`, `sum(price) / sum(qty)` per
group took 0.42 s against 0.92 s, `sum(disc / 3)` 1.17 s against 1.38 s,
and a filter on `price / qty` 0.92 s against 1.07 s.  TPC-DS SF1:
whole-slice runs 1558 -> 1572, queries wholly in DataFusion 84 -> 89 of
206 executions, results unchanged apart from Q71's ties (205/206).

A comparison whose sides have no common scale within 76 digits is
exact instead (df_numcmp_needed): an avg is carried at the largest scale
numeric_avg may pick (40 for a sum of scale 2), and a quotient by one may
need 84 digits after the point, so `CASE WHEN avg_sales > 0 THEN
abs(sales - avg_sales) / avg_sales END > 0.1` (TPC-DS Q47, Q53, Q57,
Q63, Q89) and a quotient compared with another (Q4, Q74) stayed on
PostgreSQL.  df_core::pgnum::PgNumCmp computes each side as PostgreSQL
does, a quotient as pg_div at select_div_scale's scale, in arbitrary
precision (num-bigint), and compares them, NaN above every number; a
CASE's comparison is that of the branch taken, abs of a quotient that of
abs(x) / abs(y).  TPC-DS SF1 with WIN2 and abs: queries wholly in
DataFusion 105 -> 112 of 206 executions by EXPLAIN (Q47, Q57 and Q89
under the Postgres planner, Q53 and Q63 under both), their results
unchanged.

### avg as a sort key and above its Agg

AV2: an avg returning numeric that DataFusion divides (AVG1) holds
PostgreSQL's quotient at the largest scale numeric_avg may pick; its
display scale, which numeric_avg picks from the sum and the count
(df_core::pgnum::pg_avg_rscale), goes beside it as DN1's do, computed from
the same sum and count.  So a sort by an avg runs in DataFusion, which
divides it there instead of leaving it to the C side (TPC-DS Q22, Q85),
and an avg leaves DataFusion above its Agg, through joins, a Subquery Scan
or a Motion carrying tuples (TPC-DS Q9, Q28's avgs of six scalar
subqueries joined by Nested Loops), as do expressions on it.  A value
through a batch Motion has no display scale beside it: a slice that needs
one from there does not run, and the Motion then carries tuples.

Over 20 million rows, the ten groups of largest `avg(price)` took 1.03 s
against 1.34 s, `avg(price) * 2, avg(price) - avg(disc)` per group 0.37 s
against 1.18 s.  TPC-DS SF1: whole-slice runs 1572 -> 1578, queries wholly
in DataFusion 89 -> 95 of 206 executions, results unchanged apart from
Q71's ties (205/206).  A UNION's dedup over a quotient (Q49) still stays
on PostgreSQL: rows of equal values and different display scales are one
group, shown as whichever PostgreSQL meets first.

### Expressions

E1 adds the expressions found most in filters and select lists:

| Expression | How |
|---|---|
| `x IN (...)`, `x NOT IN (...)` of constants (`= ANY`, `<> ALL`) | DataFusion's InList, whose NULL rules are SQL's: `2 IN (1, NULL)` is NULL |
| `CASE WHEN ... THEN ... END`, `CASE x WHEN ...` | DataFusion's CASE, which evaluates a branch only on the rows reaching it, so `CASE WHEN b = 0 THEN 0 ELSE 100 / b END` never divides by zero |
| `COALESCE(a, b, ...)` | as `CASE WHEN a IS NOT NULL THEN a ...`, so later arguments only run where the earlier ones are NULL, as in PostgreSQL |
| `NULLIF(a, b)` | `CASE WHEN a = b THEN NULL ELSE a END` |
| `IS [NOT] DISTINCT FROM` | DataFusion's operator |

Comparisons follow the rules of `=`: integers of two widths or floats
compare in the wider type, numeric at the larger scale, strings under a
deterministic collation.  A numeric value carries its display scale, so a
CASE or COALESCE returning numeric runs only when every branch has the
same scale (`CASE WHEN n > 1 THEN n ELSE 0 END` returns `0` and `1.50`
in PostgreSQL, not one column of one scale).  Other ANY/ALL operators
(`> ALL`) stay on PostgreSQL.  Over 20 million rows, an IN list of five
strings with a NOT IN of integers took 0.21 s against 0.86 s, a CASE with
COALESCE and NULLIF grouped 0.26 s against 1.25 s.

Casts (E2), known by the cast function's pg_proc OID:

| Cast | How |
|---|---|
| int2 to int4/int8, int4 to int8, integers to float4/float8, float4 to float8 | DataFusion's cast: exact, or rounding to nearest as C converts |
| int8/int4 to a narrower integer | range errors (22003, "integer out of range") |
| float4/float8 to integers | `rint` (half to even), then the range of float.c and int8.c |
| float8 to float4 | overflow and underflow errors |
| date to timestamp | infinities kept, dates from 294277 on out of range (22008) |
| timestamp to date | days rounded down |
| numeric to integers | half away from zero (`round_var`); NaN is an error (0A000) |
| numeric to float8 | the decimal string, correctly rounded (as `float8in`) |
| numeric to numeric(p, s) | rounded to s, "numeric field overflow" with PostgreSQL's detail beyond p digits |

Casts that can fail run behind the AND/OR guards.  Casts involving
time zones (timestamptz), text and float to numeric stay on PostgreSQL.

Floats compare as in PostgreSQL (F0).  Arrow compares, hashes and sorts
float4 and float8 in IEEE total order, where -0 is below 0 and a NaN with
its sign bit set (`'-NaN'`, or what `inf - inf` gives on x86) is below
-Infinity; PostgreSQL has -0 equal to 0 and every NaN equal to every other
and above all numbers.  Comparison operands, IN lists, join and grouping
keys therefore go through `pg_float_key`, which makes -0 0 and every NaN
the positive one; on those values total order is PostgreSQL's.  Values
keep their bits: a float grouping key shows the smallest member of its
group, so a group of -0 alone shows `-0`.

min and max of floats run on integers (F1): DataFusion's grouped min and
max start from the largest and smallest finite float, so a group holding
only +Infinity came out as `1.7976931348623157e+308`, and they let a NaN
replace or be replaced depending on row order.  `pg_float_order_key` maps
a float, NaN made positive, to an integer of its width in total order,
and the result is mapped back.  Of -0 and 0, min returns -0 and max 0;
PostgreSQL returns either, depending on row order.

### Output columns PostgreSQL finishes

P1: an output column DataFusion cannot compute whole is split.  DataFusion
computes its largest parts it can (columns, + - *, aggregates, also an
avg returning numeric); PostgreSQL's expression machinery evaluates the
rest over those values, row by row, as its own projection would.  That
covers any function or operator DataFusion lacks (`round`, `abs`,
`to_char`, casts to text, ...), and covered numeric division until
DataFusion computed it (DN1).  Not finished this way: filters, HAVING, sort keys (DataFusion
sorts by what it computes), output sent as Arrow batches, and parts
needing more than the values (subplans, parameters, window or
set-returning functions, a simple `CASE x WHEN` around such a part).

The top slice is checked on the tree the coordinator executes: the
dispatcher replaces `plannedstmt->planTree` with a copy (parameters
folded, for the segments) after the executor was set up, and the parts
PostgreSQL finishes are found by node.

Over 30 million rows, `sum(b) / count(*)::numeric, 100.0 * sum(a) /
sum(b)` grouped took 0.30 s against 1.50 s, a filtered `sum(b)::numeric
/ nullif(sum(a), 0)` 0.29 s against 1.03 s.

### DISTINCT aggregates

D1 runs count, sum and avg over DISTINCT arguments.  DataFusion's own
distinct accumulators keep every distinct value in memory and fail beyond
the slice's budget (30 million distinct values under a 32 MB
statement_mem did), where PostgreSQL sorts and spills.  So the Agg runs
as a grouping by its groups and the argument, which spills like any
grouping, then as the aggregates over the distinct values.  With several
DISTINCT arguments, or other aggregates beside (D3), each input row is
repeated once per argument and once for the other aggregates (a cross
join with the numbers 0..k, the small side buffered), copy j keeping
argument j; the grouping then holds each argument's distinct values, the
other aggregates take partial results over copy 0 only (FILTER), and the
aggregate above merges them, as Cloudberry's TupleSplit does.  Floats are
told apart through `pg_float_key`, strings need a deterministic
collation.  Sum and avg of distinct floats (of -0 and 0 PostgreSQL keeps
one, and the sum shows its sign) and ORDER BY inside an aggregate stay
on PostgreSQL.  A combining stage keeps the DISTINCT of the call but adds
up partial results, which the planner splits only where each segment sees
all of a value.  `count(DISTINCT b)` over 30 million distinct values took
1.21 s against 3.68 s under that 32 MB; `count(DISTINCT b), count(DISTINCT
a % 1000), count(*), sum(c)` 2.8 s against 6.1 s.

A plan with several groupings and sorts holds that many shares of the
budget per partition, so a slice runs on budget / (2 MB x their number)
partitions at most, and a sort reserves at most a quarter of a share to
merge its spilled runs (10 MB by default, which several partitions of a
16 MB slice could not get).

D2 runs the planner's own form of DISTINCT aggregates: an aggregate over
a grouping by the argument (`HashAggregate` below `Partial Aggregate`,
or two `HashAggregate`s), within one slice.  The grouping, without
aggregates and with only grouping columns read above it, becomes a named
aggregate node of the spec.  And a GroupAggregate whose order no one
reads, below an unsorted Motion, a Sort or another aggregate, runs
hashed without the Sort below it.  Below a sorted Motion whose keys are
its groups (D3), it runs hashed and the slice sorts its output by those
keys: the groups are unique, so the order is the same.  Also below
Subquery Scans that pass its grouping columns on as the Motion's keys
(SR1, TPC-DS Q24), with the keys a prefix of the groups.  One below a
Limit or at the top of a slice is D7's.  `count(DISTINCT a % 100000)`
over 30 million rows took 0.65 s against 3.29 s.

D4: the planner also pushes a LIMIT below the sorted Motion, over the
GroupAggregate (TPC-H Q18), where it keeps the first groups in the order of
the Sort below.  The slice runs the GroupAggregate hashed, sorts its output
by that Sort's keys and then limits it.  The order is the GroupAggregate's
only when every grouping column is among those keys (one row per group, no
ties) and output; the Motion merges by a prefix of them.

D5: a GroupAggregate over a sorted Gather's merge, as the Postgres
planner combines grouped partial aggregates (TPC-H Q5), runs hashed when no
one reads its order (a Sort or another aggregate above it), and the Gather
then carries batches, received unmerged.  Its sender need not sort for
them, so a sort by a character key under a collation that differs between
the coordinator and the segments no longer keeps it out.  Where the
query's order rests on the merge (`GROUP BY a ORDER BY a` without a Sort
above), D7 sorts.

D6: no one reads the order of a Hash Join's sides, a Nested Loop's inner
side or a Hash's child: to both planners a Hash Join's rows come in no
order and a Nested Loop's in its outer side's.  A GroupAggregate there
(TPC-DS Q32, in a subquery joined) runs hashed, as D2.

D7: a GroupAggregate at the top of a slice, or below a Limit there, whose
order the rows leave in: on the coordinator over a sorted Gather's merge
(TPC-DS Q7, a Finalize GroupAggregate under the query's LIMIT), or the top
of a subtree a PostgreSQL node above reads in order (T2).  It runs hashed,
and the slice sorts its output by the keys of its input, the merge's or
the Sort's, then limits it; as D4, every grouping column must be among
them.

### Grouping sets

GS1: GROUPING SETS, ROLLUP and CUBE.  Cloudberry's Agg computes the sets
in phases: its own grouping and that of each Agg of its chain, a hashed
one a set, a sorted one a rollup (a set per prefix of its columns) over a
Sort of its own.  DataFusion groups by all the sets at once, hashed, so
those Sorts go unused.  Where the slice's rows leave in a sorted rollup's
order (D7, TPC-DS Q27 under its LIMIT), the slice sorts by the keys of the
rollup's input: a set's row follows those of the longer sets it sums up
where their NULLs sort last, and a tie with a NULL of the data goes to the
longer set first, as the rollup gives them.

Each row's set is DataFusion's `__grouping_id`, taken to its place among
the sets in the order nodeAgg.c numbers them.  GROUPING(), GROUP_ID() and
the GROUPINGSET_ID() that a two-stage plan groups its partial rows by are
a number per set, which the C side computes as nodeAgg.c does.  An empty
set gives its row with no input rows, as PostgreSQL's.  DISTINCT
aggregates with grouping sets stay on PostgreSQL: DataFusion counts them
over a grouping by all columns first, which the sets would count again.

Over 30 million rows, a ROLLUP of two keys took 0.36 s against 1.8 s, and
a CUBE of two 0.37 s against 2.0 s.  The grouping sets blocked eleven
TPC-DS executions under the Postgres planner; none now.

Before GS1 the plan check did not look at the sets.  Their Agg names the
grouping columns of one set only, and running it as that GROUP BY lost
the other sets' rows.

### Window functions

WIN1: `row_number`, `rank` and `dense_rank`, and `count`, `sum`, `min` and
`max` over a frame from the partition's first row to the current row
(ROWS), to its last peer (RANGE, the default with an ORDER BY) or to the
partition's last row (the default without).  DataFusion sorts the rows by
the partition and order keys itself, in the directions of the Sort or
merging Motion that ordered PostgreSQL's rows, found through the nodes
that keep their order (a GroupAggregate, Subquery Scan, Result, another
WindowAgg); the keys compare as PostgreSQL compares them (floats through
`pg_float_key`, character values without trailing blanks, strings under a
C collation).  The Sort below goes unused.  An aggregate over a frame is
checked and built as over a group: `sum(int8)` is numeric, `sum` of
numeric exact and NaN if any value is; ranks are int8.  The window
functions are columns of the WindowAgg's node, which the nodes above read
by name, so that a WindowAgg runs below a join as well (TPC-DS Q44).

Where the WindowAgg's rows leave the slice in the order they came in (to
a merging Gather when the ORDER BY is the window's, or to the client),
the slice sorts its output by the keys of the Sort below, the order
PostgreSQL's rows have.  The top WindowAgg's run condition (a subquery's
`WHERE r <= 3` over `rank()`): PostgreSQL returns a partition's rows up
to the first that fails it, the planner admits only a condition that
then fails for the rest, and DataFusion filters by it.  A run condition
of a WindowAgg below another, which passes the rows on with its
functions NULL, stays on PostgreSQL.

DataFusion's window operators reserve nothing from the memory pool and do
not spill.  One over whole partitions (WindowAggExec) holds all its input
before it computes: it runs only where the planner's estimate of that
input fits the WindowAgg's budget, min(operatorMemKB, work_mem), as a
Nested Loop's build side does.  One up to the current row
(BoundedWindowAggExec) holds a partition's rows up to the current row's
peers, and runs whatever the estimate.  Their sorts spill as any.

TPC-DS SF1: whole-slice runs 1578 -> 1615, queries wholly in DataFusion
95 -> 105 of 206 executions (Q12, Q20, Q98 under both planners; Q36, Q67,
Q70, Q86 under the Postgres planner), results unchanged apart from Q71's
ties, now under both planners (204/206).  Over 20 million rows, the three
largest per group by `rank()` and a running `sum` did not finish in
DataFusion: ten partitions sort under the slice's 65 MB, partitions that
end their input early merge their spilled runs with as many buffers as the
pool grants, and a partition still sorting then finds no memory left
(DataFusion 55.2.0; apache/datafusion#25804, finding 9).  A plain sort
does the same under 64 MB.  apache/datafusion#26264 proposes the fix; on
DataFusion's main with it (see Build) both finish, with the rows
PostgreSQL gives: 2.1 s against 9.0 s, and 3.0 s against 6.4 s.

A sum over a frame adds 19 digits to its argument's bound, as a sum over
a group does; a sum of the sums of an Agg's groups (TPC-DS Q12, Q20, Q98:
`sum(x) * 100 / sum(sum(x)) OVER (PARTITION BY ...)`) adds up each row
below at most once and keeps its argument's bound, which leaves the
division above it within 76 digits.

WIN2: `avg` of numeric and of integers over a frame is numeric_avg of the
frame's sum and count, which DataFusion computes as two window columns
(an exact sum, NaN if any value is, and a count) and divides as an Agg's
avg (AVG1, df_core::pgnum::pg_avg), with the display scale numeric_avg
picks beside it where the value leaves DataFusion or is divided (AV2).  So
an avg over partitions (TPC-DS Q47, Q53, Q57, Q63, Q89) runs in DataFusion,
as do expressions on it and a sort by it.  avg of floats stays on
PostgreSQL: float8_accum adds the frame's values one by one, DataFusion a
batch's in another order, which may give other last digits.

Not done: `avg` of floats; `min` and `max` of floats (of -0 and 0, PostgreSQL's keep the later
value, which the window's order determines); `ntile`, `lag`, `lead` and
the other window functions; frames with offsets, GROUPS or EXCLUDE;
FILTER and DISTINCT.

### UNION ALL

UA1: an Append (UNION ALL) runs as DataFusion's union of its inputs, each
projected to columns q<id>_c<k> of the Append's node, which the nodes above
read by name; an input that is a Motion (GPORCA redistributes each branch
below the Append) is read from its stream, as any node reads its child.
A numeric column is rescaled to the largest scale of its inputs; inputs of
different scales, or of display scales of their own, give each value its
own (DN1), carried beside it as q<id>_c<k>_s.  DataFusion keeps no order
between the inputs, so an Append whose order a node above reads (a Limit
without a sort) stays on PostgreSQL, as does one with run-time partition
pruning.  An input the planner proves empty is a Result with a
one-time filter (OF1): one without a child whose filter is false gives no
rows (its columns, of no value, read as NULLs), and gp_execution_segment()
= n over an Agg of no groups is known on each segment, which keeps that
Agg's one row on the one segment (TPC-DS Q4, Q11, Q74).  A Merge Append whose order nothing reads runs as an
Append, and a GroupAggregate whose order nothing reads runs hashed over
any input (D2), not only over a Sort: TPC-DS Q33, Q56 and Q60 group the
GroupAggregates of a Merge Append again.

TPC-DS SF1: queries wholly in DataFusion 112 -> 120 of 206 executions by
EXPLAIN (Q2, Q5, Q76, Q77, Q80 under the Postgres planner, Q71 and Q76
under GPORCA too); the 44 executions of the queries with a UNION ALL give
PostgreSQL's results, Q71's in another order of rows tied on its ORDER BY
keys, as before.

### INTERSECT and EXCEPT

SO1: a SetOp (INTERSECT, EXCEPT) reads its child's rows each flagged 0
(left) or 1 (right), and nodeSetOp.c keeps a group of equal rows by the
counts of each side's.  DataFusion runs it as a grouping by the columns
the SetOp tells duplicates by, named q<id>_g<k>, counting each side's rows,
with the same test on the counts (INTERSECT: both above 0; EXCEPT: the
left above 0 and the right 0); NULLs are equal, as in the SetOp.  A
group's row is any of its members', so only columns whose equal values
look alike: integers, bool, date, timestamp, text and varchar, and
character(n), which holds every value padded to n; not floats (-0 and 0),
numeric (1.0 and 1.00) or character without a length.  The ALL forms,
which repeat rows, stay on PostgreSQL.  GPORCA plans INTERSECT and
EXCEPT as joins on IS NOT DISTINCT FROM keys (a Hash Join's
hashqualclauses): DataFusion's hash join matches NULL keys there
(NullEqualsNull, NE1) when every key is such, as in those plans.  TPC-DS
SF1: Q8 and Q87 under the
Postgres planner wholly in DataFusion (124 -> 126 of 206 executions by
EXPLAIN), Q8, Q38 and Q87 giving PostgreSQL's results; Q38 and Q87 run
slower than on PostgreSQL there (0.45 s and 0.64 s against 0.38 s and
0.45 s).

### GPORCA's Assert

AS1: GPORCA checks that a scalar subquery gives at most one row with an
Assert over its rows (`row_number() OVER () = 1`), which raises "one or
more assertions failed" (P0003, the Assert's message as the detail) for a
row whose condition is false.  DataFusion runs it as a filter keeping
every row, whose predicate (df_core::pgfunc::PgAssert) raises the same
error where the condition is false; other Asserts stay on PostgreSQL.
TPC-DS Q6 under GPORCA runs wholly in DataFusion, 0.15 s against 0.38 s;
Q6, Q14b and Q58 give PostgreSQL's results.

### Subtrees below PostgreSQL's nodes

T2: when a slice as a whole cannot run in DataFusion, DataFusion runs the
highest subtrees of it that can, and PostgreSQL's nodes above them pull
their rows as from any child: ORCA's Result on top, a Sort under the
database's default collation, a Limit WITH TIES, a combining aggregate
over a Nested Loop.  Only children their parents run once qualify: below
Result, Sort, Limit, Unique, Material, ProjectSet, WindowAgg, Agg,
SubqueryScan, every child of an Append or a Sequence, a Shared Scan's
producer, both sides of a Hash Join, the
outer side of a Nested Loop or Merge Join (whose inner side is rescanned
or marked), never a Hash node itself (its join builds the table through
MultiExecProcNode; its child qualifies).  DataFusion's output is ordered
only below a Sort of its own, which is what a GroupAggregate or WindowAgg
above it relies on.  A subtree that only passes rows on (a Motion's, an
unfiltered scan's) is not worth starting.  Such a slice sends tuples;
its subtrees still read batch Motions, which the batch decision now
counts.  EXPLAIN shows `eligible below the top: <nodes> (above:
<reason>)`.

GPORCA's Shared Scan (a CTE read more than once, and the grouping sets
of a ROLLUP, which GPORCA turns into one) puts its producer under a
Sequence, in a slice with the producer's joins and aggregates.  The
Sequence runs each input once, in order, and the producer reads its input
once into the store its readers share, in its own slice or others
(`init_tuplestore_state`; a rescan reads the store again), so both
qualify: DataFusion runs the producer's input, and PostgreSQL keeps the
store and its synchronization.  The Motions below the
Sequence are now seen by the batch decision too.  Of TPC-DS's 206
executions, the 32 with a Shared Scan (all GPORCA) run this way, with
PostgreSQL's rows, together 18.99 s before and 14.86 s now (PostgreSQL
alone: Q23a 2.10 s, Q23b 2.24, Q95 0.78, Q22 1.48, Q36 0.61): Q23a 1.50 s
and now 0.66 s, Q23b 1.84 and 0.85, Q95 1.27 and 0.55, Q22 0.94 and 0.68,
Q36 0.64 and 0.23, Q67 1.67 and 1.48.  Where the producer's input is only
joins, little changes: TPC-DS Q18's chain of six Redistribute joins under
GPORCA, now through batch Motions, took 0.36 s and now 0.39 s.

A reader of a Shared Scan is an input of the slice: PostgreSQL's node
waits for the producer and reads the store, and the main thread pulls
its rows, as a receiving Motion's, for what computes on them or for a
Motion carrying batches.  Its rows are the producer's input's,
unprojected, so its columns must be those, at their positions.  Numeric
values of display scales of their own (an avg, a sum of COALESCE
branches of different scales) keep them, as from a Motion of tuples;
finding that they are such follows the reader to its producer, also
when the Motions that carry batches are chosen, so that a Motion of a
reader's rows carries tuples at both of its ends.  Since a reader ends
by telling its producer it is done whether it read every row or not
(`ExecEndShareInputScan`), DataFusion may stop early.  With both ends,
the 32 TPC-DS executions with a Shared Scan give PostgreSQL's rows;
Q22 took 0.68 s with the producer alone and now 0.47 s, Q67 1.48 and
1.29 to 1.34 (PostgreSQL 1.40 to 1.45 s and 1.96 to 2.36 s); the others
change within the noise, the 32 together 14.86 s and 15.33 s in single
runs.

Over the TPC-H queries of Cloudberry's regression tests, run with their
data under both planners, 144 slices run this way (50 under the Postgres
planner, 94 under ORCA) besides the 388 that run whole, and every query
returns PostgreSQL's rows.  Running them found three older bugs, fixed
before: an aggregate without outputs, a semi join reading its inner side,
and DataFusion's own join reordering (eliminate_cross_join), which hung
Q7.  A grouping below ORCA's Result over 30 million rows took 0.27 s
against 1.39 s.

### Sort and LIMIT

S1 runs a Sort and a Limit at the top of a slice: ORDER BY with LIMIT and
OFFSET of constants (or expressions folding to one).  Below a sorted
Gather Motion, each segment's slice sorts in DataFusion, with a Limit only
its top rows (DataFusion's TopK), and PostgreSQL's receiver on the
coordinator merges the sorted streams as it would PostgreSQL's.  The
coordinator's own Limit over that merge stays on PostgreSQL: it computes
nothing.  A slice without a Motion (catalog queries) runs its Sort and
Limit itself.

S2: GPORCA puts a Result over the Sort, projecting its columns (TPC-DS
Q58).  DataFusion projects, then sorts by the columns the Result passes
the Sort's keys on as, and a Limit above applies after; a key the Result
does not pass on keeps the slice on PostgreSQL.

| Sort key | Order |
|---|---|
| integers, bool, date, time, timestamp, timestamptz | as stored; date and timestamp infinities at the ends |
| float4, float8 | through `pg_float_key`: -0 equals 0, NaN above all numbers |
| numeric(p, s) | Decimal256; NaN, stored above every number, last |
| text, varchar | bytes, under a C collation only (or C.UTF-8, CU1) |

NULLS FIRST/LAST and DESC are DataFusion's.  An avg returning numeric
sorts as its value, divided in DataFusion (AV2).  A sort key ordered by
another operator (`USING ~<~`), LIMIT ... WITH TIES, a LIMIT
from a parameter and a Sort or Limit further down the slice stay on
PostgreSQL.  A Sort's memory budget is the node's operator memory, as for
PostgreSQL's Sort; DataFusion spills sorted runs to the backend's
temporary directory beyond it.  A slice that stops early (a LIMIT
reached) ends its scan there.

Over 30 million rows: the top 10 by one column took 0.39 s against
1.0-1.35 s, the top 100 of a filtered scan 0.27 s against 1.0-1.1 s, the
top 5 groups by sum 0.27 s against 1.2-1.4 s, and sorting 30,000
filtered rows 0.25 s against 0.84-1.09 s; a full sort of all 30 million
under a 64 MB statement_mem spilled and returned PostgreSQL's rows.

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

DataFusion is a commit of MisterRaindrop/datafusion's branch of
apache/datafusion#26264 (`rust/Cargo.toml`), DataFusion's main with that
fix, which cargo fetches from GitHub; back to a release from crates.io once
one has it.

## FFI contract

Every function the Rust library exports never calls into PostgreSQL, never
lets a panic escape, and reports failure as a status code plus a message in
the caller's buffer. The C side raises the `ereport` after the call returns,
so PostgreSQL's `longjmp` never unwinds through Rust frames. See
`src/df_ffi.h`.

## License

Apache License 2.0. See `LICENSE`.
