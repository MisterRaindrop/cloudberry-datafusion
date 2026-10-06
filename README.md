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
`hashfloat4/8` with their handling of -0 and NaN, and since X2
`hashtext`).  Each receiver gets its
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
stream goes to every receiver).  NOT IN anti joins (with their NULL rule)
and IS NOT DISTINCT FROM joins stay on PostgreSQL.

DataFusion's hash join does not spill.  A slice qualifies only if the Hash
node's estimated size (planner rows times width plus a per-row allowance)
fits its budget, `min(operatorMemKB, work_mem) * hash_mem_multiplier`, as a
PostgreSQL hash table's would; EXPLAIN gives the figures otherwise.  An
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
two of these types (`ts > date`, `tz > ts`), which convert one side first.

Over 20 million heap rows on 3 segments, counting one year by a `date`
range with `min`/`max` of a timestamp took 0.15 s in DataFusion and 1.00 s
in PostgreSQL; grouping by `date` with a `timestamp` filter took 0.17 s and
0.89 s (only the segments' slice in DataFusion; the coordinator sorts and
limits).

### Text and varchar

In a UTF-8 database, `text` and `varchar` columns travel as Arrow string
columns: the main thread detoasts each value and copies its bytes, and
results come back as new `text` values.  DataFusion compares the bytes,
which agrees with PostgreSQL as follows:

| Operation | Runs in DataFusion when the collation is |
|---|---|
| `=`, `<>`, GROUP BY keys, hash join keys | deterministic (any libc or ICU collation that is not `deterministic = false`) |
| `<`, `<=`, `>`, `>=`, `min`, `max` | C or POSIX |

Anything else stays on PostgreSQL with the reason in EXPLAIN (`operator <
on text under a collation other than C`).  Whether the database's default
collation is C is decided by each node itself: the coordinator and the
segments can disagree (a cluster initialized with `C` on the coordinator
and `C.UTF-8` on the segments does), so EXPLAIN shows the coordinator's
verdict, a segment may run the same slice on PostgreSQL, and the Motions
of a slice whose verdict depends on the default collation carry tuples
rather than batches, so that every node agrees on which Motions carry
batches.  An explicit `COLLATE "C"` is the same everywhere and keeps the
batches.  `ILIKE`, regular expressions, `md5`, `initcap` and other
string functions not listed below stay on PostgreSQL for now, as do `char(n)`, `name` and databases in
other encodings.  The direct PAX reader hands out fixed-width values only;
a PAX table with a string column is read through the table AM.

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
its slices decide per node and the batch Motions it needs are off.

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
an avg's result (`avg(a) + 1`), whose scale depends on the values, stays
on PostgreSQL.  Over 30 million heap rows, `sum(b), avg(a),
avg(b)` took 0.26 s with batch Motions against 1.04 s in PostgreSQL, and
grouped by a column 0.31 s against 1.46 s.

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
`numeric`, `/` and `%` (whose result scale depends on the values), unary
minus, and Redistribute Motions by a numeric key (whose `hash_numeric` is
not transcribed) stay on PostgreSQL; the direct PAX reader leaves numeric
columns to the table AM.  Over 20 million rows of `numeric(15,2)` columns,
a query shaped like TPC-H Q1 (`sum(price * (1 - disc) * (1 + disc))` and
six more aggregates grouped by a flag) took 0.55 s with batch Motions
against 2.31 s in PostgreSQL.

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
covers numeric division, whose result scale PostgreSQL picks from each
pair of values (`select_div_scale`), so a fixed-scale column cannot hold
it, and any function or operator DataFusion lacks (`round`, `abs`,
`to_char`, casts to text, ...).  `CASE WHEN b = 0 THEN 0 ELSE a / b END`
still divides only where b is not 0, and `a / 0` raises PostgreSQL's
error.  Not finished this way: filters, HAVING, sort keys (DataFusion
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
keys: the groups are unique, so the order is the same.  One below a Limit
or at the top of the coordinator's slice stays on PostgreSQL, since the
query's ORDER BY may rest on its order.  `count(DISTINCT a % 100000)`
over 30 million rows took 0.65 s against 3.29 s.

### Subtrees below PostgreSQL's nodes

T2: when a slice as a whole cannot run in DataFusion, DataFusion runs the
highest subtrees of it that can, and PostgreSQL's nodes above them pull
their rows as from any child: ORCA's Result on top, a Sort under the
database's default collation, a Limit WITH TIES, a combining aggregate
over a Nested Loop.  Only children their parents run once qualify: below
Result, Sort, Limit, Unique, Material, ProjectSet, WindowAgg, Agg,
SubqueryScan, every child of an Append, both sides of a Hash Join, the
outer side of a Nested Loop or Merge Join (whose inner side is rescanned
or marked), never a Hash node itself (its join builds the table through
MultiExecProcNode; its child qualifies).  DataFusion's output is ordered
only below a Sort of its own, which is what a GroupAggregate or WindowAgg
above it relies on.  A subtree that only passes rows on (a Motion's, an
unfiltered scan's) is not worth starting.  Such a slice sends tuples;
its subtrees still read batch Motions, which the batch decision now
counts.  EXPLAIN shows `eligible below the top: <nodes> (above:
<reason>)`.

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

| Sort key | Order |
|---|---|
| integers, bool, date, time, timestamp, timestamptz | as stored; date and timestamp infinities at the ends |
| float4, float8 | through `pg_float_key`: -0 equals 0, NaN above all numbers |
| numeric(p, s) | Decimal256; NaN, stored above every number, last |
| text, varchar | bytes, under a C collation only |

NULLS FIRST/LAST and DESC are DataFusion's.  A sort key ordered by
another operator (`USING ~<~`), a sort by an avg returning numeric
(divided on the C side, after DataFusion), LIMIT ... WITH TIES, a LIMIT
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

## FFI contract

Every function the Rust library exports never calls into PostgreSQL, never
lets a panic escape, and reports failure as a status code plus a message in
the caller's buffer. The C side raises the `ereport` after the call returns,
so PostgreSQL's `longjmp` never unwinds through Rust frames. See
`src/df_ffi.h`.

## License

Apache License 2.0. See `LICENSE`.
