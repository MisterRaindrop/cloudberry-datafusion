# Benchmarks

## TPC-H, scale factor 100

`vec_executor` against Cloudberry without the extension, on the same
cluster and the same tables.  Measured on 2026-11-06 at commit `1a20a91`.

| Planner | Cloudberry | vec_executor | Speedup | Geometric mean | Smallest |
|---|---:|---:|---:|---:|---:|
| Postgres planner | 1089.1 s | 116.2 s | **9.4x** | 8.4x | 3.3x |
| GPORCA | 1100.7 s | 173.0 s | **6.4x** | 6.4x | 3.4x |

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
- One warm-up round, then five timed rounds; the order of the four
  configurations rotates from round to round.
- Each query runs in its own `psql` and is timed from the client, so
  connection setup and result output are included.
- The table reports the median of the five runs.  The data was in the
  page cache (warm runs).
- Each run's output was hashed.  All configurations returned the same
  output for every query (44 of 44).
- 440 timed runs: no errors, no timeouts.

### Per query (seconds, median of 5)

| Query | Cloudberry | vec_executor | Speedup | Cloudberry, GPORCA | vec_executor, GPORCA | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 93.75 | 10.14 | 9.2x | 95.40 | 12.29 | 7.8x |
| Q2 | 11.19 | 0.66 | 16.8x | 9.94 | 0.93 | 10.7x |
| Q3 | 27.84 | 8.41 | 3.3x | 25.60 | 7.49 | 3.4x |
| Q4 | 23.48 | 2.49 | 9.4x | 48.64 | 6.15 | 7.9x |
| Q5 | 29.26 | 4.29 | 6.8x | 60.35 | 6.78 | 8.9x |
| Q6 | 12.02 | 2.66 | 4.5x | 12.14 | 2.49 | 4.9x |
| Q7 | 32.37 | 3.35 | 9.7x | 34.02 | 5.94 | 5.7x |
| Q8 | 34.44 | 2.89 | 11.9x | 34.62 | 3.07 | 11.3x |
| Q9 | 123.85 | 22.30 | 5.6x | 88.20 | 15.23 | 5.8x |
| Q10 | 25.80 | 5.97 | 4.3x | 26.34 | 4.58 | 5.8x |
| Q11 | 8.77 | 0.58 | 15.2x | 5.74 | 0.62 | 9.3x |
| Q12 | 21.65 | 2.44 | 8.9x | 20.38 | 2.41 | 8.5x |
| Q13 | 40.44 | 7.21 | 5.6x | 40.59 | 7.55 | 5.4x |
| Q14 | 15.83 | 2.40 | 6.6x | 14.22 | 2.44 | 5.8x |
| Q15 | 31.54 | 6.31 | 5.0x | 19.86 | 5.92 | 3.4x |
| Q16 | 7.61 | 1.61 | 4.7x | 7.48 | 1.79 | 4.2x |
| Q17 | 241.59 | 3.61 | 66.9x | 255.19 | 29.03 | 8.8x |
| Q18 | 159.99 | 11.13 | 14.4x | 119.16 | 27.18 | 4.4x |
| Q19 | 25.78 | 5.65 | 4.6x | 26.48 | 6.03 | 4.4x |
| Q20 | 35.64 | 1.84 | 19.4x | 38.06 | 2.18 | 17.5x |
| Q21 | 75.03 | 8.21 | 9.1x | 93.96 | 18.67 | 5.0x |
| Q22 | 11.26 | 2.07 | 5.5x | 24.29 | 4.18 | 5.8x |
| **Total** | **1089.12** | **116.21** | **9.4x** | **1100.66** | **172.95** | **6.4x** |

### Notes

- Q17 under the Postgres planner gains most from the correlated
  subquery rewrite (`vec.subquery_key_filter`); GPORCA's plan does not
  take it.
- Under GPORCA, vec_executor's time is higher mainly in Q17, Q18 and
  Q21, whose plans differ from the Postgres planner's.
- Q11's fraction (0.0001) is the one for scale factor 1; it was not
  scaled to scale factor 100 (0.000001), so the query returns no rows
  with or without the extension.
- Most queries varied by less than 10% of their median across the five
  runs.
