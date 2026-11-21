# Shopping Sessions: native C++ pipeline

One dependency-free C++17 file (`src/main.cpp`) that implements `rules.md` and writes both Parquet tables
with a small hand-written Parquet writer (PLAIN, uncompressed, one row group per file).

## Pipeline

1. **Setup**: manifest, `products.csv`, `campaigns.csv`. Every CSV is `mmap`ed; a background thread issues `MADV_WILLNEED`.
2. **Links**: parallel AVX2 scan, lock-free CAS hash table (slot index = ID), union-find toward the smaller slot,
   byte-wise smallest ID per component as the person name.
3. **Parse** events and clicks in parallel 8 MB chunks with an AVX2 structural scanner (commas/newlines iterated, quotes
   checked by popcount: a line is well-formed iff its quote count equals 2 x its quoted fields). Each event copy is
   validated up front and scattered by `event_id` hash; valid clicks are resolved to their person and scattered by person.
4. **Dedup** per `event_id` bucket: keep the smallest `ingest_ts`, then resolve each kept valid event to its person
   (batched, prefetched link lookups) and scatter by person.
5. **Sessions** per person partition: group, sort by `ts`, split on gaps over 30 min, credit each session with a
   two-pointer sliding 7-day click window (latest click, ties to the smallest `click_id`), assists from the window's
   live campaign set, aggregate `user_daily` rows and per-thread `campaign_daily` counters.
6. **Write** one `user_daily` file per thread plus one `campaign_daily` file.

## Test

```sh
make fixtures                                   # 36/36 edge cases (python3 + polars)
generate --seed 1 --events 1000000 --clicks 250000 --output dev
DATA_DIR=dev OUTPUT_DIR=out build/pipeline && make check-dev
```

Local timing (i7-8700K, 8 threads, warm cache): 20M events in ~2.0 s.
