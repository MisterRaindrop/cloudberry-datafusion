# Changelog

## Unreleased

- Releases come with a binary package (Rocky Linux 9, x86-64, for the
  Cloudberry commit CI builds against) and `install.sh`.
- `tools/tpch`: the TPC-H schema, queries, loader and benchmark behind
  `docs/benchmarks.md`, with a baseline to compare a change against.
- CI checks license headers, formatting, compiler warnings and clippy, and
  weekly runs the tests on Cloudberry built with assertions, the engine's
  unit tests under AddressSanitizer, and a stress and fuzz soak.
- The query of `INSERT ... SELECT`, `CREATE TABLE AS`, `SELECT INTO` and
  materialized views runs in the engine; the writing stays on PostgreSQL.
  At SF10, grouping lineitem into 7.9 million rows and inserting them
  took 5.6 s instead of 11.7 s.  `UPDATE` and `DELETE` still run on
  PostgreSQL.

## 0.1.0 (2026-11-17)

The first release: a pure extension that runs the slices of Apache
Cloudberry plans it can in a vectorized engine (Apache DataFusion), and
leaves the rest to Cloudberry's executor.  It builds against Cloudberry's
`main` (CI pins a commit, see `.github/workflows/ci.yml`) and needs a Rust
toolchain; the kernel is unchanged.

### What runs in the engine

- Scans of heap, AO row, AO column and PAX tables; the optional direct PAX
  reader (`vec_pax.so`) reads PAX files column by column, including the
  `porc_vec` format.
- Hash joins of every kind (spilling), merge joins as hash joins, nested
  loop joins; plain, hashed, two-stage and DISTINCT aggregates, grouping
  sets; sorts, LIMIT, window functions, UNION ALL / INTERSECT / EXCEPT,
  correlated aggregate subqueries, Shared Scans.
- Integers, floats, numeric, bool, date and time types, text / varchar /
  char(n), with PostgreSQL's semantics, errors and SQLSTATEs.
- Batch Motions: Motions between two engine slices carry Arrow IPC
  batches (`vec.motion_batches`, on by default).
- Coverage at SF1: all 44 TPC-H and all 206 TPC-DS runs (both planners)
  use the engine.

### Under Cloudberry's control

- Memory: the engine's pool is the operator memory Cloudberry assigns;
  every byte it allocates is leased from the vmem tracker, and what it
  frees goes back to the system after each query (`malloc_trim`), so idle
  sessions do not hold it out of the tracker's sight.
- Workfiles: the engine's spill files count toward
  `gp_workfile_limit_per_query`, `_per_segment` and `_files_per_query`
  and show in `gp_toolkit`'s workfile views.
- CPU: engine threads belong to the QE and its resource group.
- Cancel, `statement_timeout`, terminated backends and killed QEs stop the
  engine and release its threads and files; spill files that a task still
  running at exit leaves are removed by the next backend.
- Out of disk space or file descriptors while spilling, a query fails
  with PostgreSQL's SQLSTATE (53100, 53000) and leaves nothing behind.
- `EXPLAIN` says which slices run in the engine and why the others do not;
  `EXPLAIN ANALYZE` shows the engine's rows and times at every node.

### Testing

- 25 regression test files, each query run with the extension off and
  on; an engine check against the null engine; the engine's Rust tests.
- CI on GitHub's runners in Cloudberry's Rocky 9 build container: builds
  Cloudberry, starts a demo cluster, runs all of the above; a weekly run
  against Cloudberry's latest `main`.
- `tools/fuzz.py`: random queries, `vec.mode` off against on.
- `tools/stress.py`: concurrent sessions with cancels and terminated
  backends; results checked, leftovers looked for.  Eight sessions at
  SF10 and four at SF100, thirty minutes each, with every result right.

### Known differences and limits

See the README.  In short: floating-point sums may differ in the last
digits; with LIMIT and no ORDER BY, or a WHERE condition that can fail,
errors may differ; text sorts run in the engine only under a C collation;
tested on single hosts only (no multi-host cluster yet).
