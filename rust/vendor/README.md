# Vendored crates

## datafusion-physical-plan 55.2.0, with apache/datafusion#24891

`datafusion-physical-plan/` is the crate as published on crates.io
(`datafusion-physical-plan-55.2.0.crate`, sha256
`b1451d6817e5ac0dd21819b19949d1c420ab51d748f55dd9a96ecb5418a24c97`), with
`patches/datafusion-pr24891-spill-pool.diff` applied: the change of
apache/datafusion#24891 to `src/spill/spill_pool.rs`, as merged into main
on 2026-09-04 (09a2aff72d).  Its `Cargo.toml` also allows the `deprecated`
lint in `[lints.rust]`: cargo caps the lints of a registry crate but not of
a path dependency, and the build treats warnings as errors.  `../Cargo.toml`
puts it in place of the published crate with `[patch.crates-io]`.

Why: the spill pool reader of a RepartitionExec drains only the oldest of
its files.  When two input tasks spill at once the pool holds two, the
reader waits on a drained one while its next batch is in the other, the
distributor gate closes, and every worker parks for good
(apache/datafusion#24883).  A TPC-H Q8 at scale factor 10 stopped so on
three segments under a 32 MB budget.  The fix is not on branch-55: 55.2.0
ships the reader unchanged.  `../df_core/tests/repartition_spill.rs` fails
without it.

Remove this directory and the `[patch.crates-io]` entry when the
DataFusion in use contains #24891 (a 55.x release that backports it, or
56).

To rebuild it:

    tar -xzf datafusion-physical-plan-55.2.0.crate
    mv datafusion-physical-plan-55.2.0 datafusion-physical-plan
    (cd datafusion-physical-plan && patch -p3 < ../patches/datafusion-pr24891-spill-pool.diff)

and allow `deprecated` in its `[lints.rust]` again.
