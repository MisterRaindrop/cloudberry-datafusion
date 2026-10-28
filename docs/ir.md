# Plan IR, version 1

The C side (`src/df_translate.c`, plus the Motion members `src/df_exec.c`
adds) describes each slice it hands to an engine as one JSON object, the
plan IR.  This document is the contract between the two: an engine that
reads every construct below, with the semantics given here, runs any slice
the extension sends it.  `rust/df_core/src/query.rs` is the reference
reader.

The IR describes a plan PostgreSQL has already planned and the extension
has already checked.  An engine executes it as given: it may choose how
(partitioning, operator algorithms), but not what (the join tree, which
side of a semi join is kept, the order AND and OR evaluate their
arguments in).

## Versioning

Every plan carries `"ir": 1`.  `VEC_IR_VERSION` (`src/vec_engine.h`) and
`query::IR_VERSION` must match; an engine refuses a plan of any other
version, or of none, before it builds anything.

The version changes when a construct changes its meaning or goes away.  A
new construct, or a new optional member that defaults to the old meaning,
is added to this document and its catalog without a new version.  An engine
fails a plan with a node, expression, aggregate or function name it does
not know; it must not skip it.

## Conventions

- An index (`input`, `col`, `group`, `agg`, positions in lists) is a
  0-based JSON integer.
- A type tag names a PostgreSQL type as the engine sees its values (see
  [Types](#types)).
- `null` for an optional member means the member is absent.
- Names (`"name": s` of aggregates, windows and unions) are chosen by the C
  side, unique within a plan; the columns they introduce are referred to by
  `{"name": "<s>_..."}`.

## Envelope

```text
{ "ir":     1,
  "inputs": [ <input>, ... ],
  "plan":   <node>,
  "output": [ {"expr": <expr>, "type": <type tag>}, ... ],
  "sort":   [ <sort key>, ... ],           optional
  "limit":  {"skip": n, "fetch": m},       optional
  "batch_rows": n,                         optional
  "trim":   [k, ...],                      optional, Motion senders only
  "narrow": [[k, digits], ...],            optional, Motion senders only
  "route":  <route> }                      optional, Motion senders only
```

- `output`: the slice's result columns, in order, each converted to its
  type tag.  An output of `bpchar` is padded as stored.
- `sort`: the order of the output rows.  A key is
  `{"col": c, "desc": b, "nulls_first": b}` over output column `c`, or
  `{"expr": <expr>, "desc": b, "nulls_first": b}` over a column of the plan
  the output leaves out (the top aggregate's `gset`).  `desc` and
  `nulls_first` default to false.  Floats sort as PostgreSQL orders them:
  NaN above every other value, -0 equal to 0.
- `limit`: after the sort, skip `skip` rows (default 0) and keep `fetch`
  (all without it).
- `batch_rows`: rows per result batch the host wants, at most 8192; a hint.
- `trim`, `narrow`, `route`: how a slice that sends through a Motion lays
  out its Arrow IPC streams; see [Motion output](#motion-output).

## Inputs

```text
<input> := { "columns":        [ {"type": <type tag>}, ... ],
             "motion_columns": [k, ...],      optional
             "motion_pad":     [n, ...],      optional
             "motion_narrow":  [digits, ...], optional
             "set":            true }         optional
```

Column `k` of input `j` is `{"col": k, "input": j}` in expressions
(`input` defaults to 0).

- Without `motion_columns` the host pushes the input as column batches.
- With `motion_columns` the input arrives as Arrow IPC streams from a
  Motion; entry `i` is the stream column behind input column `i`.
  `motion_pad[i] > 0` pads a `bpchar` stream column back to that many
  characters (the sender trimmed it); `motion_narrow[i] > 0` widens a
  numeric stream column sent narrowed to that many digits (`-1`: sent
  as is).  See [Motion output](#motion-output).
- `"set": true` marks a subquery's rows that `in_set` and `scalar_of`
  read: the engine reads the whole input before the plan starts.

## Nodes

```text
<node> := {"input": j}
        | {"filter":    {"input": <node>, "pred": <expr>}}
        | {"aggregate": {...}}
        | {"join":      {...}}
        | {"window":    {...}}
        | {"union":     {...}}
        | {"rowid":     {"input": <node>, "name": s, "base": n}}
        | {"empty":     {}}
```

### input

The rows of input `j`.  Each input is read by exactly one `input` node.

### filter

The rows of `input` for which `pred` is true (not false, not NULL).

### aggregate

```text
{"aggregate": { "input":  <node>,
                "group":  [<expr>, ...],
                "aggs":   [<agg>, ...],
                "having": <expr> | null,
                "name":   s,                optional
                "sets":   [[g, ...], ...] }} optional
```

One row per group of equal `group` values (one row in all without
`group`, even over no rows).  Its columns are `g<i>` for each group
expression and `a<i>` for each aggregate, read as `{"group": i}` and
`{"agg": i}`; `having` filters the groups.

- Grouping compares values as PostgreSQL does: a float key groups -0 with
  0 and every NaN together, and the group shows the smallest member by
  PostgreSQL's order; a key written `{"bpchar": <expr>}` groups character
  values without their trailing blanks and shows one member as stored.
- `name`: an aggregate below a join or another aggregate.  Its columns are
  then `<s>_g<i>`, `<s>_a<i>` (and `<s>_gset`), read by `{"name": ...}`;
  without groups or aggregates it has one column, `<s>_rows`.
- `sets`: grouping sets, each a list of indexes into `group`.  The rows of
  every set are produced, the keys outside a row's set NULL, and a column
  `gset` (`<s>_gset`) holds the row's set, its position in `sets`.

`<agg>`:

```text
{"fn": f, "arg": <expr> | null, "arg2": <expr>, "distinct": b, "collate": l}
```

| `fn` | Result |
|---|---|
| `count` | rows with a non-NULL `arg`; all rows without `arg` (`count(*)`) |
| `count_merge` | sum of partial counts, 0 over no rows (PostgreSQL's combining count) |
| `sum` | sum, NULL over no rows |
| `sum_numeric` | exact sum of integers as numeric of scale 0 (`sum(int8)`, an `avg(int)`'s sum) |
| `sum_decimal` | exact sum of numeric values at their scale; NaN if any value is NaN |
| `min`, `max` | smallest, largest; floats by PostgreSQL's order; strings by `collate` when given |
| `avg` | mean of float values |
| `avg_merge` | `sum(arg) / sum(arg2)`, NULL when the latter is 0: combines avg states sent as a sum and a count |
| `any` | a value every row of the group shares (a column the groups determine) |

- `distinct`: the aggregate over the distinct non-NULL values of `arg`.
- `collate`: libc locale name ordering `min` and `max` of strings.

### join

```text
{"join": { "type":  "inner" | "left" | "right" | "full" | "rightsemi"
                    | "leftanti" | "rightanti",
           "left":  <node>, "right": <node>,
           "on":    [[<left expr>, <right expr>], ...],
           "filter": <expr> | null,
           "null_equal": b,      optional
           "null_aware": b }}    optional
```

An equi-join on the `on` pairs, then `filter` over the joined row (for
outer, semi and anti joins it decides which rows match, as a join qual
does).  Semi and anti joins keep only the named side's columns: a
`rightsemi` keeps the right rows that have a match.

- Keys compare as PostgreSQL does (float keys: -0 equals 0, NaN equals
  NaN).  A NULL key matches nothing.
- `null_equal`: keys compared with IS NOT DISTINCT FROM; NULL matches NULL.
- `null_aware` (with `leftanti`, one key): NOT IN.  No row at all when the
  right side has a NULL key; a left row with a NULL key only when the
  right side is empty.
- The reference reader builds its hash table on the left side, and the
  C side arranges the sides for that.  For another engine this is a
  hint, not a requirement.

### window

```text
{"window": { "input": <node>, "name": s,
             "partition": [<expr>, ...],
             "order": [{"expr": <expr>, "desc": b, "nulls_first": b}, ...],
             "frame": "partition" | "range" | "rows",
             "funcs": [<window fn>, ...] }}
```

The rows of `input`, each with a column `<s>_w<i>` per function, computed
over the rows of its partition sorted by `order`, within the frame:

- `partition`: every row of the partition;
- `range`: from the first row to the current row's last peer;
- `rows`: from the first row to the current row.

`<window fn>` is `{"fn": "row_number" | "rank" | "dense_rank"}` (int8, as
PostgreSQL's), an `<agg>` computed over the frame, or
`{"fn": "avg_parts", "sum": <agg>, "count": <agg>}`: an average's frame
sum as `<s>_w<i>` and its count as `<s>_w<i>_n`, which an `avg_numeric`
above divides.

### union

```text
{"union": { "inputs":  [<node>, ...], "name": s,
            "columns": [[{"expr": <expr>, "dscale": <expr>}, ...], ...] }}
```

UNION ALL.  `columns` has one list per input; input `i`'s rows contribute
column `k` as `<s>_c<k>` and, where `dscale` is given, its display scale
as `<s>_c<k>_s`.

### rowid

The rows of `input`, each with a column `name` holding an int8 unique to
the row and greater than `base` (Cloudberry's RowIdExpr).

### empty

No rows.  The expressions above it refer to no columns.

## Expressions

### References and literals

```text
{"col": k, "input": j}       column k of input j (j defaults to 0)
{"group": i} | {"agg": i}    a column of the unnamed aggregate
{"name": s}                  a named column (aggregate, window, union, rowid)
{"lit": {"type": t, "value": v}} | {"lit": {"type": t, "null": true}}
{"per_set": [v, ...], "set": <expr>}   int4 v[set]: GROUPING() and the like
```

Literal values: JSON booleans and integers; floats as numbers or the
strings `"NaN"`, `"Infinity"`, `"-Infinity"`; text as strings; date and
time types as integers in PostgreSQL's epoch and units (days, or
microseconds); a numeric as a string of the integer `value * 10^scale`,
or `"NaN"`, `"Infinity"`, `"-Infinity"` (a parameter compared beyond every
value).

### Operators

```text
{"op": o, "type": t, "args": [<expr>, <expr>], "collate": l}
```

| `op` | Meaning |
|---|---|
| `=` `<>` `<` `<=` `>` `>=` | comparison (bool); strings byte by byte, or by libc locale `collate` |
| `+` `-` `*` `/` `%` | arithmetic in result type `t`, the operands converted to it first (PostgreSQL computes a cross-type operator in its result type); overflow raises 22003, division by zero 22012; for `numeric:S` only `+ - *`, the operands already at scale S for `+` and `-` |
| `~~` `!~~` | LIKE, NOT LIKE |
| `\|\|` | text concatenation |

### Logic

```text
{"and": [<expr>, ...]} | {"or": [<expr>, ...]} | {"not": <expr>}
{"isnull": <expr>} | {"isnotnull": <expr>}
{"distinct": [<a>, <b>]}                       a IS DISTINCT FROM b
{"inlist": <expr>, "values": [<expr>, ...], "negated": b}   IN / NOT IN, three-valued
{"case": [{"when": <expr>, "then": <expr>}, ...], "else": <expr>}
```

**Evaluation order is part of the semantics.**  AND and OR evaluate their
arguments left to right and stop once the result is decided; an argument
they skip must not raise an error.  A guard such as `b <> 0 AND a / b > 1`
never divides by zero.  CASE evaluates a branch only for the rows that
reach it.  An engine whose optimizer reorders conjuncts, or evaluates
every argument over a whole batch, must keep this (the reference reader
evaluates an argument that may fail only on the rows still undecided).

### Casts

```text
{"cast": <expr>, "type": t}       a widening one: int2 → int4 → int8, float4 → float8
{"pgcast": <expr>, "kind": k, "from": t0, "type": t, "precision": p}
```

`pgcast` kinds, each with PostgreSQL's rounding and errors:

| `kind` | Cast |
|---|---|
| `int` | between integer types; out of range raises 22003 |
| `float_int` | float to integer, rounded half to even |
| `float8_float4` | float8 to float4; overflow and underflow raise 22003 |
| `date_timestamp`, `timestamp_date` | between date and timestamp |
| `numeric_int` | numeric to integer, rounded half away from zero |
| `numeric_float8` | numeric to float8 |
| `numeric_typmod` | numeric to `numeric(precision, scale of t)`; too many digits raises 22003 |

### Keys

```text
{"floatkey": <expr>}    a float as PostgreSQL compares it: -0 as 0, every NaN as one
{"bpkey": <expr>}       a character value without its trailing blanks
{"bpchar": <expr>}      a character grouping key or DISTINCT argument (see aggregate)
```

### Functions

```text
{"call": f, "args": [<expr>, ...]}
```

String functions with PostgreSQL's results and errors: `char_length`,
`octet_length`, `substr`, `textcat`, `concat`, `btrim`, `ltrim`, `rtrim`,
`left`, `right`, `reverse`, `repeat`, `lpad`, `rpad`, `strpos`, `replace`,
`split_part`, `starts_with`, `lower`, `upper`, and `lower_locale`,
`upper_locale` (by the backend's LC_CTYPE case maps, which the host passes
to the engine).

### PostgreSQL numeric and date functions

| Expression | PostgreSQL | Notes |
|---|---|---|
| `{"extract": <date>, "field": "year" \| "quarter" \| "month" \| "day"}` | `extract(field from date)` | numeric of scale 0 |
| `{"dateadd": [<date>, <int4>], "sign": 1 \| -1}` | `date_pli`, `date_mii` | 22008 beyond the dates |
| `{"numabs": <x>}` | `numeric_abs` | NaN stays NaN |
| `{"numround": <x>, "from": s, "type": "numeric:S"}` | `numeric_round` | from scale s to S |
| `{"rescale": <x>, "by": n, "type": "numeric:S"}` | | the integer times 10^n, as scale S (the same value at a larger scale); NaN stays NaN |
| `{"numdiv": [x, dx, y, dy], "scales": [sx, sy], "type": "numeric:S"}` | `numeric_div` | dx, dy: the operands' display scales (int4); the result's scale follows them as PostgreSQL's select_div_scale; 22012 on zero |
| `{"numdiv_scale": [x, dx, y, dy], "scales": [sx, sy]}` | | that quotient's display scale (int4) |
| `{"greatest": [<a>, <b>]}` | | the larger of two int4 (display scales) |
| `{"numcmp": op, "left": <side>, "right": <side>}` | numeric comparison | exact, any digits; `<side>` is `{"value": <x>, "scale": s}` or `{"div": [x, dx, y, dy], "scales": [sx, sy]}` |
| `{"avg_numeric": [<sum>, <count>], "scale": s, "type": "numeric:S"}` | `numeric_avg` | rounded as PostgreSQL does |
| `{"avg_numeric_scale": [<sum>, <count>], "scale": s}` | | its display scale (int4) |
| `{"stddev_numeric": [<n>, <sum>, <sumsq>], "sample": b, "variance": b, "type": "numeric:S"}` | `numeric_stddev_internal` | stddev or variance of integers |
| `{"stddev_numeric_scale": [...], "sample": b, "variance": b}` | | its display scale (int4) |
| `{"assert": <bool>, "detail": s}` | GPORCA's Assert | true where true or NULL; else P0003 "one or more assertions failed" with `detail` |

### Subquery sets

```text
{"in_set": <expr>, "set": j, "key": "plain" | "float" | "bpchar"}
{"scalar_of": j, "col": k}
```

- `in_set`: `<expr> = ANY (rows of input j)`, three-valued.  `key` says how
  values compare: as they are, as floats (`floatkey`), or as characters
  without trailing blanks.
- `scalar_of`: column `k` of the one row of input `j`; NULL without a row,
  21000 with more than one.

## Types

| Tag | Values | Arrow layout (DataFusion) |
|---|---|---|
| `bool` | | Boolean |
| `int2`, `int4`, `int8` | | Int16, Int32, Int64 |
| `float4`, `float8` | IEEE; NaN and -0 kept | Float32, Float64 |
| `date` | days since 2000-01-01 | Int32 |
| `time`, `timestamp`, `timestamptz` | microseconds since midnight (time) or since 2000-01-01 00:00 (timestamps, UTC for timestamptz) | Int64 |
| `text` | UTF-8 (`text` and `varchar`), compared byte by byte unless `collate` | Utf8 |
| `bpchar` | blank-padded as stored; compared, grouped, hashed and sorted without trailing blanks | Utf8 |
| `numeric`, `numeric:S` | the integer `value * 10^S` (S 0 to 76); NaN and ±Infinity possible | Decimal256(76, S), NaN = i256::MAX, +Infinity = i256::MAX - 1, -Infinity = i256::MIN |
| `tid` | `block << 16 \| offset`, ordered as tids are | Int64 |

A numeric whose display scale varies by row (an unconstrained `numeric`)
travels as `numeric:S` at its largest scale plus an int4 column of each
value's display scale, which the C side adds and expressions such as
`numdiv` take as arguments.

## Motion output

A slice that sends through a Motion between two engine slices produces
Arrow IPC streams instead of rows (`DF_QUERY_IPC_OUTPUT`).

- `trim: [k, ...]`: output columns of type `bpchar` sent without their
  trailing blanks; the receiver pads them back (`motion_pad`).
- `narrow: [[k, digits], ...]`: numeric output columns of at most 38
  declared digits, sent as Int64 (up to 18 digits) or Decimal128(38, S)
  (up to 38), NaN, +Infinity and -Infinity as the type's largest, next
  largest and smallest values; the receiver widens them back
  (`motion_narrow`).  A value beyond its digits is an error.
- `route: {"kind": "hash", "keys": [{"col": k, "hash": h}, ...],
  "segments": n, "workers": w}`: a Redistribute Motion.  One stream per
  receiver (`n * w`), each row sent to the receiver Cloudberry's cdbhash
  of its key columns picks, computed on the values before narrowing.
  `hash` is the key's type tag (`date` hashes as int4; `time`, `timestamp`
  and `timestamptz` as int8).

Both ends check the Motion's signature (`df_motion_signature`), which
covers the stream's column types, pads and digits, so a mismatch fails
instead of misreading.

## Catalog

Every member name and every value of a member that selects a kind.
`tools/ir_keys.py` checks a corpus of plans against it.

```catalog
member ir inputs plan output sort limit batch_rows trim narrow route
member columns type motion_columns motion_pad motion_narrow set
member input filter pred aggregate group aggs having name sets
member fn arg arg2 distinct collate
member join left right on null_equal null_aware
member window partition order frame funcs sum count desc nulls_first
member union dscale rowid base empty
member skip fetch expr col kind keys hash segments workers
member agg lit value null per_set
member op args and or not isnull isnotnull inlist values negated
member case when then else
member cast pgcast from precision floatkey bpkey bpchar call
member extract field dateadd sign numabs numround rescale by
member numdiv numdiv_scale scales greatest numcmp div scale
member avg_numeric avg_numeric_scale stddev_numeric stddev_numeric_scale sample variance
member assert detail in_set key scalar_of
value type bool int2 int4 int8 float4 float8 date time timestamp timestamptz text bpchar tid numeric numeric:S
value type inner left right full rightsemi leftanti rightanti
value fn count count_merge sum sum_numeric sum_decimal min max avg avg_merge any
value fn row_number rank dense_rank avg_parts
value op = <> < <= > >= + - * / % ~~ !~~ ||
value numcmp = <> < <= > >=
value frame partition range rows
value kind hash int float_int float8_float4 date_timestamp timestamp_date numeric_int numeric_float8 numeric_typmod
value from bool int2 int4 int8 float4 float8 date time timestamp timestamptz text bpchar tid numeric numeric:S
value hash bool int2 int4 int8 float4 float8 date time timestamp timestamptz text bpchar tid numeric numeric:S
value key plain float bpchar
value field year quarter month day
value call char_length octet_length substr textcat concat btrim ltrim rtrim left right reverse repeat lpad rpad strpos replace split_part starts_with lower upper lower_locale upper_locale
```
