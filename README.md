# Shopping Sessions — native Rust pipeline

A single static Rust binary (`FROM scratch` image, ~3 MB) that implements `rules.md`.

- **Stage 1 (parallel, 1 MiB chunks of mmapped CSV):** AVX2 bitmasks of separators and quotes per
  chunk; lines are walked with `tzcnt`, quoting is validated with one popcount identity per line.
  IDs pack exactly into order-preserving 63-bit keys (`bswap` + `pext`); timestamps parse with SWAR.
  Events are radix-scattered by `event_id`, clicks by person. The thread that finishes the last
  links chunk builds the union-find while the others keep parsing events.
- **Stage 2 (parallel `event_id` partitions):** dedup on min `ingest_ts`, validity, scatter by
  `user_id`.
- **Stage 2b (parallel `user_id` partitions):** the link map is split the same way, so each
  partition resolves IDs to persons with an L1-sized table; scatter by person.
- **Stage 3 (parallel person partitions):** group, sort, sessionize, sliding-window attribution and
  assists, per-day aggregation; each thread writes its own `user_daily` Parquet file.
- A never-freeing bump allocator on one huge-page mapping, and 8 KiB scatter blocks recycled
  from consumed partitions, keep page faults and kernel zeroing low.
- Parquet is written by hand (PLAIN, uncompressed, one row group).

`scripts/reference.py` is an independent slow implementation; `scripts/fuzz.py` mutates fixture
inputs and diffs the binary against it.

`cargo test --release` runs differential tests of the SWAR parsers. Fixtures:
`python3 scripts/run_fixtures.py fixtures -- target/release/sessions`.
