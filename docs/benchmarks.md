# Benchmarks

## TPC-H, scale factor 100

`vec_executor` against Cloudberry without the extension, on the same
cluster and the same tables.  Measured on 2026-11-17 with the code of
0.1.0 (`b64d007`) minus its last two commits, which change only spill
cleanup at exit and the error codes of failed spill I/O.

| Planner | Cloudberry | vec_executor | Speedup | Geometric mean | Smallest |
|---|---:|---:|---:|---:|---:|
| Postgres planner | 1103.5 s | 120.8 s | **9.1x** | 8.0x | 3.6x |
| GPORCA | 1090.1 s | 182.8 s | **6.0x** | 6.0x | 2.8x |

Times are the sums of the 22 queries' medians.  Every query is faster,
and every query returns the same rows with and without the extension.

### Setup

| | |
|---|---|
| Machine | one x86-64 host, 20 hardware threads, 80 GB of memory, NVMe SSD |
| Cluster | Apache Cloudberry 3.0.0-devel (`83022b58b2`), 3 primaries, no mirrors |
| Data | `dbgen` scale factor 100, loaded with `COPY` into PAX tables (lineitem 600,037,902 rows), analyzed |
| Cloudberry settings | `work_mem` and `statement_mem` 4 GB, `max_statement_mem` 8 GB, `gp_vmem_protect_limit` 20 GB, `shared_buffers` 256 MB |
| vec_executor | `vec.mode = on`, `vec.motion_batches = on`, everything else default (no decoded-column cache) |
| Baseline | the database's `session_preload_libraries` reset, so no backend loads the extension |

### Method

- Four configurations: with and without the extension, under each
  planner.
- One warm-up round, then three timed rounds; the order of the
  configurations rotates from round to round.  Cloudberry without the
  extension and vec_executor ran as two separate sets the same day.
- Each query runs in its own `psql` and is timed from the client, so
  connection setup and result output are included.
- The table reports the median of the three runs.  The data was in the
  page cache (warm runs).
- Each query's output under vec_executor was compared with its output
  without the extension: the same for every query under both planners
  (44 of 44).
- 264 timed runs: no errors, no timeouts.

### Per query (seconds, median of 3)

| Query | Cloudberry | vec_executor | Speedup | Cloudberry, GPORCA | vec_executor, GPORCA | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 94.95 | 10.87 | 8.7x | 94.56 | 10.20 | 9.3x |
| Q2 | 9.42 | 0.71 | 13.3x | 10.40 | 1.00 | 10.4x |
| Q3 | 26.43 | 7.34 | 3.6x | 25.80 | 9.24 | 2.8x |
| Q4 | 22.52 | 2.76 | 8.1x | 50.21 | 7.02 | 7.2x |
| Q5 | 28.40 | 4.34 | 6.5x | 58.53 | 7.64 | 7.7x |
| Q6 | 14.07 | 2.71 | 5.2x | 13.74 | 2.65 | 5.2x |
| Q7 | 31.27 | 3.56 | 8.8x | 32.81 | 6.29 | 5.2x |
| Q8 | 34.29 | 3.30 | 10.4x | 34.01 | 3.63 | 9.4x |
| Q9 | 122.04 | 21.52 | 5.7x | 87.11 | 17.52 | 5.0x |
| Q10 | 24.70 | 6.24 | 4.0x | 27.75 | 4.82 | 5.8x |
| Q11 | 9.31 | 0.61 | 15.2x | 5.30 | 0.69 | 7.7x |
| Q12 | 19.84 | 2.60 | 7.6x | 20.14 | 2.72 | 7.4x |
| Q13 | 40.75 | 7.78 | 5.2x | 40.63 | 7.97 | 5.1x |
| Q14 | 13.81 | 2.54 | 5.4x | 14.61 | 2.42 | 6.0x |
| Q15 | 31.12 | 7.25 | 4.3x | 17.71 | 6.21 | 2.9x |
| Q16 | 7.81 | 1.76 | 4.4x | 7.92 | 1.95 | 4.1x |
| Q17 | 237.32 | 3.12 | 76.2x | 250.91 | 29.52 | 8.5x |
| Q18 | 180.52 | 11.47 | 15.7x | 118.08 | 26.76 | 4.4x |
| Q19 | 28.05 | 6.13 | 4.6x | 25.39 | 6.38 | 4.0x |
| Q20 | 38.31 | 2.02 | 19.0x | 38.16 | 2.33 | 16.4x |
| Q21 | 75.98 | 9.95 | 7.6x | 92.39 | 21.84 | 4.2x |
| Q22 | 12.62 | 2.22 | 5.7x | 23.91 | 4.02 | 5.9x |
| **Total** | **1103.50** | **120.81** | **9.1x** | **1090.10** | **182.84** | **6.0x** |

### Notes

- Q17 under the Postgres planner gains most from the correlated
  subquery rewrite (`vec.subquery_key_filter`); GPORCA's plan does not
  take it.
- Under GPORCA, vec_executor's time is higher mainly in Q17, Q18 and
  Q21, whose plans differ from the Postgres planner's.
- Q11's fraction (0.0001) is the one for scale factor 1; it was not
  scaled to scale factor 100 (0.000001), so the query returns no rows
  with or without the extension.
- Under vec_executor, half the queries varied by 10% or less of their
  median across the three runs; Q8, Q15 and Q3 by a quarter to a third,
  and Q16 (under two seconds) by 70%.
