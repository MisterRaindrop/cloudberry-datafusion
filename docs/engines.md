# Adding an engine

DataFusion is the only engine today.  This document is for whoever adds a
second one: what an engine implements, what the host still assumes about
the engine it talks to, and the designs agreed for the parts that must
change first.  None of it changes the DataFusion path; the changes it
describes are made when a second engine is actually being connected, so
that they are shaped by that engine rather than guessed.

The engine most likely to come next is Velox.  It is C++, and an engine
added to this extension must not bring a Rust toolchain into its build:
everything a non-Rust engine needs is a specification plus a test, never a
Rust library to link.

## What an engine implements

| Piece | Specification | Check |
|---|---|---|
| The function table | `include/vec_engine.h` | `engines/null` (an engine that runs nothing), `sql/engines.sql` |
| Plans | `docs/ir.md`, plan IR version 1 | the regression tests run with the engine (see Testing) |
| Batch Motions | `docs/wire.md` | `vec_debug_cdbhash_check()` against `cdbhash()`; `sql/motion.sql`, `sql/batches.sql` |
| Direct PAX reads (optional) | the callbacks in `include/vec_engine.h`, `patches/pax` | `sql/paxdirect.sql` |
| Options | `vec.engine_options`, passed through as text | the engine's own |

An engine is `$libdir/vec_engine_<name>.so`, chosen by `vec.engine`.  Its
name goes into every batch Motion's signature, so two processes running
different engines refuse each other's streams; mixing engines, or engine
versions, inside one query is not supported.

The wire format keeps two constants from the time it had only DataFusion:
the tuple chunk type `0x4446` ("DF") and the stream head `DFARROW1`.  They
are byte values, not names, and stay as they are.

## What the host assumes

`src/vx_plan_check.c` decides which slices an engine may run.  Its rules
carry tags (WIN1, D2, ...); 68 of them.  Most are about PostgreSQL's plans
and hold for any engine; the rest assume something of the engine, and were
written against DataFusion.  They fall into five classes, each with its
own treatment.  The tags are the ones in the source, so `grep` finds each.

### 1. Plan semantics: any engine, no capability needed

The rule recognizes a shape of PostgreSQL's plan and sends it as an IR
construct whose meaning `docs/ir.md` defines.  An engine that implements
the construct as documented is right; the regression and conformance tests
check it.

TID1, UQ1, UQ2, R1, SQ1, A1, SO1, NE1, OF1, AS1, SP1, SP2, SC1, IP1, IP2,
IP3, T2, FW1, RS1, D6, CN1, E1, E2, GS1 (the `gset` numbering is in the
IR), RX1 (once its subset is written as matching semantics, below), B1,
ET1, DT2, DT3, RI1.

### 2. Orderings and physical choices the engine makes

The rule leaves out a Sort, or keeps a slice, because DataFusion is known
to produce rows in some order or to build a join on some side:

| Tags | Assumption |
|---|---|
| D2, D3, D4, D5, D7, SR1 | a GroupAggregate runs hashed, and the engine sorts its output where an order is read |
| WIN1 (window parts), S1 | the engine sorts a window's input and a slice's top Sort itself |
| S2 | the types the engine sorts as PostgreSQL does |
| UA1 | UNION ALL and Append give each input's rows in turn, or may interleave them |
| NL1, NL2 | the engine's nested loop join collects its left side and streams the other |
| NJ1 | the engine's null-aware anti join takes one key and builds on the outer side |
| MJ1 | a Merge Join runs as a hash join on its merge clauses, without its Sorts |
| D1 | DISTINCT tells arguments apart by equality, strings by bytes or collation |

Treatment, in this order of preference:

1. Make the requirement part of the IR: a node says which order its output
   must have (the IR already has the top-level `sort`), and the engine
   decides whether it must sort or already does.  The engine then carries
   no description of how it works.
2. Where that is not practical, a small set of capability bits, four to
   six, each a guarantee ("sorts a window's input itself"), not a
   description of an operator.

Either way DataFusion's plans must not change: for each rule moved, the
EXPLAIN of every TPC-H and TPC-DS query, under both planners, and
DataFusion's physical plans stay as they were.  An explicit order in the
IR could make DataFusion add a Sort it does not need today; that is the
risk to check.

### 3. Numbers from DataFusion's implementation

| Tags | Assumption |
|---|---|
| AVG1, WIN2, AV2 | the scale of avg of numeric: `36 + 4 * ceil(s / 4)`, DataFusion's `pg_avg` |
| N1, STD2, PS1, CS1, CS2, AP2, MS1 | partial aggregate states: an avg's sum then count, stddev's (n, sum, sum of squares) |
| DN1, QM1, RL1, RD1, STD1 | precision reasoning within 76 digits (see Numeric) |

The partial states are already IR (`avg_merge`, `count_merge`, the
`motion_columns` of an input) and belong to class 1 once `docs/ir.md`
states their layout in full.  The avg scale is DataFusion's own and moves
to a capability: the engine reports the scale it gives, or the host
computes the avg itself above the engine.

### 4. Collation and case mapping

CL2, CU1, CU2, CM1.  `VEC_F_COLLATE` is defined by mechanism: the engine
orders strings by `strcoll_l` under a libc locale and maps case by the maps
`set_case_maps` passes.  An engine with no locale support then runs no
slice that orders, compares or takes min/max of text under a language
collation, which on a cluster whose default collation is not C is most of
them.  Equality, LIKE and GROUP BY compare bytes and are unaffected.

Design: the capability says what is guaranteed, not how: "orders text as
the libc locale named by `collate` does".  The IR already passes the
locale name.  Until an engine has it, the documentation says plainly that
such an engine is complete only on clusters with C collation, and the
effect on TPC-H under a language collation is measured before deciding.

### 5. Cost and memory estimates

The 48 bytes a row of a hash table, the window input estimate, and
`vec.join_estimates` are DataFusion's figures, used to keep a slice on
PostgreSQL when the engine might not cope.  They stay in the host for
DataFusion and become engine-reported figures (or engine options) for the
next engine.  The comment that said DataFusion's hash join cannot spill
was out of date and has been corrected: every hash join spills but a NOT IN
anti join.

## Numeric

The IR's numeric is Decimal256(76, scale) with NaN and the infinities as
sentinels and columns of no declared scale.  `vx_engine_numeric()`
requires all of it: an engine that lacks any part runs no slice with a
numeric column at all, which for TPC-H and TPC-DS is nearly every slice.
Velox's decimal holds at most 38 digits and has no NaN or infinity.

Design, a "38-digit mode" the engine declares in its caps:

- `numeric:S` columns of at most 38 declared digits travel as
  Decimal128(38, S), as N6 already reads table columns and NW1 already
  narrows Motion columns.
- An expression whose result may pass 38 digits (sums widened by 19
  digits, avg, `numdiv`) keeps its slice on PostgreSQL; the host's
  precision reasoning (`vx_numeric_ps`, `vx_numeric_range`) already knows
  the digits.
- NaN and the infinities: columns that may hold them stay on PostgreSQL,
  or use NW1's sentinels; decided with the engine.
- Columns of no declared scale stay on PostgreSQL.

The date epoch (PostgreSQL's 2000-01-01, Velox's 1970-01-01) is a constant
offset the engine applies on its side of the boundary; timestamps likewise
(microseconds from 2000-01-01 in an Int64).

These have to be settled before the ABI changes for the second engine, so
that it changes once.

## Contracts to write into `vec_engine.h`

DataFusion meets these today; they are not yet written down, and an engine
with its own memory pool or thread pool needs them.

- **Memory.**  `heap_bytes` is every byte the engine has allocated in the
  process, on any thread, within a few percent.  `memory_limit` is a soft
  budget the engine spills to stay within; the hard limit is Cloudberry's
  vmem tracker, which the host leases `heap_bytes` plus headroom from
  between polls.  Past what it can spill, the engine returns `VEC_ERROR`
  with SQLSTATE 53200.  After `query_free` the engine still counts what
  its stopped tasks hold until they end.
- **Errors.**  Besides the SQLSTATE, the message is PostgreSQL's own text
  ("division by zero", "smallint out of range", "date out of range", "more
  than one row returned by a subquery used as an expression", ...): the
  regression tests compare it.  A table of IR construct, SQLSTATE and text
  goes into `docs/ir.md`.
- **Threads and signals.**  Threads started in `runtime_init` and
  `query_start` inherit all signals blocked; any other thread (a pool's
  helper, a cache's, a memory arbitrator's) must block them itself.
- **Versions.**  The ABI version must match exactly.  An engine kept in
  this repository is released with the host and that is enough; an engine
  kept elsewhere needs the table to grow compatibly instead (a size or
  minor version in `VecEngine`, new members only at its end).

## Engine options

`vec.engine_options` is a list `name=value, ...` the host passes to
`query_start` as it is; the engine parses it and fails the query on a name
it does not know.  DataFusion's: `key_filters` (on, off) and
`partial_agg_skip_ratio` (0 to 1).

## Testing an engine

- **Level 0**: `make installcheck-engine ENGINE=<name>`
  (`tools/engine_check.py`) runs the regression tests with
  `vec.engine = <name>`, and again with the null engine, under which
  every query stays on PostgreSQL, and compares the two without EXPLAIN's
  plans and the `vec_debug_*` functions' results: the same rows (sorted
  where the query has no ORDER BY) and the same errors.  The expected
  files are not used, so it works for an engine whose plans differ from
  DataFusion's.  It needs nothing but a cluster with the extension
  installed.  DataFusion passes all 22 tests it covers.
- **Level 1** (when a second engine comes): offline cases of IR, input
  batches and expected output or SQLSTATE, the expected results taken from
  Cloudberry, run against any `vec_engine_*.so` without a cluster.
- **PostgreSQL semantics**: functions such as numeric division, string
  functions and casts are fuzzed against PostgreSQL itself (random inputs,
  `SELECT` on Cloudberry as the oracle), not against DataFusion's code.

## Notes for Velox

- **No Rust.**  Batch Motions are written from `docs/wire.md` with Arrow
  C++'s IPC writer; cdbhash is transcribed and checked with
  `vec_debug_cdbhash_check()`.  PostgreSQL's semantics are written as Velox
  functions in C++, so that they inline; `engines/datafusion/df_core/src/pg*.rs`
  is a readable reference, not a dependency.
- **The Arrow boundary costs.**  Velox's strings are 16-byte views, its
  timestamps (seconds, nanoseconds), its dates from 1970, its decimals at
  most int128.  Input batches, PAX columns, Motions and results all cross
  in Arrow layout, so each is converted.  This is where a Velox engine is
  most likely to be slower than DataFusion; measure it first.
- **Per-backend start.**  Each backend opens the engine; Velox's function
  registry and memory manager start once per connection.
- **Threads.**  folly's executors and Velox's background threads start
  inside `runtime_init` or block signals themselves (see Contracts).
- **Collation.**  See class 4: without libc locale ordering, text ordering
  stays on PostgreSQL unless the cluster's collation is C.
- **regexp_replace.**  The IR describes its subset by `src/vx_regex.c`;
  it is to be restated as matching semantics (leftmost, ending at the end
  of the value, each segment as late as the rest allows) so that RE2 or
  another library can be checked against it.

## Deferred until a second engine

| Item | Why it waits |
|---|---|
| Classes 2, 3 and 5 as IR members or capabilities | shaped by the engine that needs them; DataFusion's plans stay as they are |
| Per-function capability lists | today the host lists what DataFusion runs, by `pg_proc` OID; the engine reports the IR names it supports instead |
| The 38-digit numeric mode | needed by the first engine without Decimal256 |
| Collation as a guarantee | needed by the first engine without libc locales |
| ABI that grows compatibly | needed by the first engine kept outside this repository |
| Level 1 conformance cases | needed by the first engine developed outside a cluster |
