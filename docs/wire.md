# Batch Motion wire format

Between two slices that both run in an engine, a Motion can carry Arrow
IPC batches instead of PostgreSQL tuples (`datafusion.motion_batches`).
The two ends are different processes, often on different hosts, and each
runs its own copy of the engine.  This document is what both must agree
on, byte for byte: an engine that writes and reads streams as below can
take either end.  `docs/ir.md` (Motion output, and the `motion_*` members
of an input) gives the plan members that ask for it.

## Which Motions

`df_motion_batchable` and `df_batch_motions` (`src/df_plan_check.c`)
decide, once per statement and alike in every process:

- a Gather (sorted or not: a receiver that reads batches merges nothing),
  a Broadcast, or a Redistribute whose every hash key the engine hashes as
  Cloudberry's cdbhash does (see Routing);
- whose sending and receiving slices both run in the engine, given the
  other Motions chosen;
- with no numeric column of per-row display scales, which tuples carry
  beside the value (but a partial sum's state carries its own);
- and an engine that declares `VEC_F_IPC_MOTION`.

Every other Motion carries tuples as before.  A process that expected
batches and gets tuples fails: "received tuples where DataFusion batches
were expected".

## Streams

Each sender writes one stream per receiver it sends to:

- Gather: one stream, to the one receiver.
- Broadcast: one stream, which the interconnect gives every receiver.
- Redistribute: one stream per receiver (route) that gets rows, `segments
  * workers` receivers in all; a receiver that gets no rows gets
  end-of-stream only, without a stream.

A receiver keeps the streams of its senders apart by the route the
interconnect reports, and decodes each on its own.

## Framing

A stream is a sequence of bytes, cut into Cloudberry tuple chunks:

```text
chunk  := header (TUPLE_CHUNK_HEADER_SIZE bytes: payload size, type)
          payload (at most Gp_max_tuple_chunk_size - TUPLE_CHUNK_HEADER_SIZE)
type   := 0x4446 ("DF"), outside PostgreSQL's TupleChunkType range
stream := head (16 bytes) arrow-ipc-stream
head   := "DFARROW1" (8 bytes) signature (8 bytes)
```

- Chunk boundaries mean nothing: the receiver concatenates the payloads of
  one route and decodes the result.  The head may span chunks.
- `signature` is `df_motion_signature` (`src/df_plan_check.c`), a 64-bit
  FNV-1a hash in the sending host's byte order, of:
  the engine's name and `VEC_IR_VERSION`; the Motion's ID; the number of
  its columns and, for each, its PostgreSQL type, the length it pads a
  `character(n)` back to (or -1) and the digits it narrows a numeric to
  (or -1).  The receiver computes its own and compares it to every
  stream's head: a mismatch is "received batches of another plan".
- After its last batch a sender finishes each stream (the Arrow IPC
  end-of-stream marker), then sends Cloudberry's end-of-stream chunk
  (`TC_END_OF_STREAM`) to every receiver.  A receiver fails on data after
  end-of-stream, a second end-of-stream, or a stream left unfinished.
- When the receiver asks its senders to stop (a Limit above has its
  rows), a sender stops writing at once and sends no end-of-stream, as
  PostgreSQL's Motion does.

## Arrow IPC streams

`arrow-ipc-stream` is the Arrow IPC streaming format: a schema message,
then record batch messages, then the end-of-stream marker, without
compression or dictionaries.  Batches with no rows are not sent.

The stream's columns are the Motion's output columns in order, except that
a partial aggregate's state may take several (an `avg`: sum, then count);
the receiving input's `motion_columns` maps them.  Each column is laid out
as its IR type (`docs/ir.md`, Types), with two exceptions, which shrink the
bytes on the wire:

| Rule | Column | On the wire | Receiver |
|---|---|---|---|
| CP1 | `bpchar` of a table's `character(n)`, n > 1 (`trim`) | without trailing blanks | pads back to n characters (`motion_pad`) |
| NW1 | numeric of one scale and at most 18 digits (`narrow`) | Int64, the unscaled integer | widens back to Decimal256(76, S) (`motion_narrow`) |
| NW1 | numeric of one scale and 19 to 38 digits | Decimal128(38, S) | the same |

A narrowed column keeps the special values: NaN, +Infinity and -Infinity
become the type's largest, next largest and smallest values (`i64::MAX`,
`i64::MAX - 1`, `i64::MIN`, and the same of i128).  A value beyond its
digits is an error at the sender; the planner's digits rule it out.

## Routing

A Redistribute sends each row to the receiver Cloudberry's own sender
would (`cdbhash.c`, `nodeMotion.c`):

1. The row's hash starts at 0.  For each hash key in order, it is rotated
   left one bit and, unless the key is NULL, xor-ed with the key's hash
   from its operator class's hash function (`hashint4`, `hashtext`,
   `hash_numeric`, ...).
2. Jump Consistent Hash of the 32-bit result over `segments` picks the
   segment; in parallel mode a second jump over `segments * workers` picks
   the receiving worker.

The keys are hashed as PostgreSQL holds them, before CP1 or NW1: a
`character(n)` key by `hashbpchar` (the value without trailing blanks), a
numeric by `hash_numeric` of the value at its column's scale, `date` as
int4, `time`, `timestamp` and `timestamptz` as int8.  A row routed to the
wrong segment gives wrong results with no error, so every engine has to
pass the check that compares its routes with `cdbhash()` value by value
(`datafusion_debug_cdbhash_check`, through the engine's
`cdbhash_routes`): every key type with its special values, two
combinations of several keys, and parallel workers.
`engines/datafusion/df_core/src/cdbhash.rs` is DataFusion's
transcription.
