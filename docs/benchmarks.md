# Benchmarks

```bash
make bench          # crystal run --release bench/main.cr
```

[`bench/main.cr`](../bench/main.cr) builds one 1e6-row frame, runs the verbs and
the Parquet paths on it, and prints a markdown table. Each scenario runs once to
warm up and then five times; the table reports the best and the median, with
`GC.collect` between runs. The fixtures are seeded, so the row counts are
reproducible.

The numbers below come from a single-threaded run on an Apple M4 (arm64) with
Crystal 1.22.2, on a frame of `region` (`Utf8`, 4 values), `user_id` (`Int64`,
1e5 values), `qty` (`Int64`, 0–999) and `price` (`Float64`), one chunk. The run
was made on a loaded machine (load average ~16 on 10 cores), so every absolute
is roughly 1.3–1.5× an idle run — read the table for comparisons *within* the
run, and the grouping A/B below for the Phase 15 change.

## Results

| scenario | best (ms) | median (ms) | out rows | note |
| --- | ---: | ---: | ---: | --- |
| filter col > lit | 50.4 | 51.4 | 499241 | ≈ half the rows |
| filter on two conjuncts | 65.2 | 67.4 | 125221 | ≈ a quarter |
| select 2 of 4 columns | 0.0 | 0.0 | 1000000 | projection, no copy |
| with_column (price * qty) | 4.8 | 5.0 | 1000000 | casts qty to Float64 |
| to_arrow (rechunk) | 0.0 | 0.0 | 1000000 | single chunk already |
| sort_by one key | 211.5 | 213.6 | 1000000 | lexsort_to_indices + take |
| sort_by two keys | 1134.4 | 1509.6 | 1000000 | second key is `Utf8` |
| sort_by + limit 10 (eager) | 317.8 | 326.2 | 10 | sorts every row, then trims |
| scan → sort_by → head(10) | 152.7 | 161.5 | 10 | gathers only the window |
| group_by 4 keys | 263.3 | 266.9 | 4 | sort-based grouping |
| group_by 1e5 keys | 292.5 | 303.2 | 99999 | one sort, three aggregates |
| distinct on 1e5 keys | 79.5 | 95.6 | 99999 | row-keyed seen-set |
| series n_unique | 69.1 | 76.4 | 99999 | row-keyed seen-set |
| join 1e6 → 1e5 on user_id | 308.9 | 324.8 | 1000000 | row-encoded keys |
| write_parquet zstd(3) | 684.8 | 886.3 | 0 | row groups of 100k, 15.0 MiB |
| read_parquet (all columns) | 138.5 | 146.0 | 1000000 | baseline |
| read_parquet projection (2 of 4) | 83.3 | 85.5 | 1000000 | projection pushdown |
| read_parquet predicate | 176.3 | 185.5 | 99557 | row filter, unclustered |
| read_parquet projection + predicate | 136.8 | 149.0 | 99557 | pushdown, unclustered |
| read_parquet limit 100 | 3.6 | 3.8 | 100 | slice pushdown |
| scan → filter → select → collect | 159.4 | 173.5 | 99557 | lazy, pushed down |
| eager read + filter + select | 187.2 | 263.2 | 99557 | no pushdown |
| read clustered (all rows) | 285.0 | 301.1 | 1000000 | 20.8 MiB, `seq` column |
| read clustered predicate (last 10%) | 224.1 | 226.9 | 99999 | row groups pruned |
| read clustered predicate + projection | 95.0 | 118.8 | 99999 | row groups pruned |
| lazy group_by → sort → head | 302.7 | 500.5 | 10 | slice pinned to the sort |

## What the numbers say

**Element-wise verbs are free.** `select` and `to_arrow` measure 0 ms because
they hand back the same buffers: projection reuses the arrays, and one chunk is
already the concatenated form. `with_column` costs one new buffer (5 ms), and a
half-frame `filter` costs 50 ms — about 20M rows/s including null-safe predicate
evaluation.

**Sorting is the expensive verb, and it is amortized.** One key is 212 ms, two
keys 1134 ms (the second key is `Utf8`, so it compares variable-length bytes).
`group_by` at 4 keys costs 263 ms and at 1e5 keys 293 ms — one sort either way,
and now the aggregates are nearly free in both cases (see the A/B below).

**A pinned slice gathers only its window.** `sort_by + head(10)` costs 153 ms
through the plan (`DataFrame#sort_by_slice`) against 318 ms for the eager
sort-then-trim, which still copies all 1e6 rows before discarding them. The sort
itself stays `O(n log n)`: a bounded-heap top-k would need an `arrow-cr` kernel
this port does not model (`lexsort_to_indices` accepts a `limit` but sorts
everything).

**Row keys made `distinct` and `n_unique` ~3x faster.** `Prism::RowKeys` encodes
the key columns with `Arrow::RowConverter` and walks a byte seen-set, so the sort
is gone. Measured on an idle machine before the switch: `distinct` 163 ms,
`n_unique` 144 ms, join 284 ms; now (this loaded run) 80 ms, 69 ms, 309 ms.

The encoding also fixed two semantics that no amount of tuning would have: NaN
keys now match (Crystal's `==` kept them apart, so a NaN could never join or
dedupe), and keys follow `==` rather than `totalOrder`, so `-0.0` and `0.0` are
one key. That needs one normalizing pass over float key columns, because the row
format stores `totalOrder` bits — the same `canonical_f64` step Polars takes
before hashing. Int and string keys pay nothing for it.

**Where a group-by spends its time: the aggregates used to dominate, and no
longer do.** `GroupedAgg` (Phase 15) reduces one operand across every group
instead of evaluating the expression per group. Same harness, same session, best
of five, on `group_by("user_id")` over the 1e6-row frame (1e5 groups):

| step | before (ms) | after (ms) |
| --- | ---: | ---: |
| `lexsort_to_indices` on the key alone | 118 | 151 |
| `group_by` + `count` | 419 | 235 |
| `group_by` + one `sum` | 326 | 235 |
| `group_by` + three aggregates | 544 | 237 |
| `group_by` + `n_unique` | 469 | 329 |
| `distinct` on the same keys (one hash pass) | 61 | 58 |

The sort row is unchanged work, measured in both runs only to size the floor:
the two runs five minutes apart differ by ~25% on it, so read the ratios.

Three things follow. The aggregate count barely matters any more: one aggregate
and three cost the same 235 ms, so a per-group cost of ~2–4 µs became ~0. The
floor is now the sort plus the row gather, not the aggregation. And `n_unique` is
the one aggregate still visibly above the floor, because distinctness per group
is a hash pass per group rather than a fold.

That also settles hash-based grouping for good, in a stronger form than the
earlier measurement did: with contiguous slice groups, `first`/`last` are one
`take` of the boundary rows, `count` is a validity-bitmap popcount, and the
numeric folds are one pass each. A hash table would remove ~150 ms of sort and
replace it with a gather per group, which is the per-group work this change just
deleted. Polars' own `GroupsType::Slice` path is the fast one for the same
reason; its `Idx` path gathers indices per group.

**Pushdown pays, and row-group pruning needs clustered data.** Reading 2 of 4
columns is 83 ms against 139 ms for all of them (40% saved). `limit 100` is 4 ms
because the reader stops after the first batch (97% saved). A predicate on an
*unclustered* column is the one case that loses: 176 ms to read and filter
against 139 ms to just read, because Parquet's min/max statistics cannot prune
row groups that each span the whole value range — every group must be decoded,
and the rows thrown away afterwards. The same predicate on a clustered column
(`seq` ascending, so a threshold excludes 9 of 10 row groups) is 224 ms against
285 ms, and 95 ms with projection too. The lesson is about the data, not the
optimizer: predicate pushdown is worth it when the predicate correlates with the
physical layout, which is what row-group statistics are for.

**Lazy beats eager by the projection it pushes.** `scan → filter → select` is
159 ms against 187 ms for the eager `read → filter → select` on the same file:
the scan reads 3 of 4 columns instead of 4 and filters while decoding. The gap is
small because the predicate's own column dominates the read.

**Eager `group_by` now gathers only the columns it reads (Phase 16).** The
aggregation output is keys plus aggregates, so payload columns are dead weight;
the frame is pruned to the pruning set (`GroupBy.required_columns`) before the key
sort and the `take`. Measured on a 52-column frame, 200k rows, 1e5 groups, two
aggregates, best and median of five in one session:

| scenario | before (ms) | after (ms) |
| --- | ---: | ---: |
| `group_by` on 52 columns, 2 aggregates (best) | 252.6 | 20.2 |
| same (median) | 256.4 | 20.9 |

Roughly 12.5×. What remains is the key sort plus gathering the three columns the
aggregation actually reads; the 48 payload columns were pure overhead. The
1e6-row four-column frame in the table above is unaffected, because it had
nothing to prune. `bench/main.cr` tracks the case as "group_by 1e5 keys, 48
unused columns".

## What is deliberately missing

- **Allocation/copy counts.** The plan's "never allocates" list is still checked
  by specs (a projected `select` is the same buffers), not by a counter; Crystal
  has no per-operation allocation hook, so a copy-count harness would need
  `GC.stats` deltas, which are too coarse to attribute.
- **A bounded-heap top-k.** `sort_by_slice` gathers only the window, but the sort
  still orders every row. That is an `arrow-cr` kernel change, not a Prism one.
- **Hash-based grouping.** Still deliberately sort → partition (contiguous group
  views, deterministic order, and — since Phase 15 — the contiguous ranges that
  make vectorized aggregation a single pass). Measured twice: before vectorizing,
  the sort was a few percent of a group-by, and afterwards it is the floor
  rather than a cost a hash table could remove, because each group's work became
  one pass over a slice.
- **Column pruning inside eager `group_by`.** The key sort and the
  `take_record_batch` that follows it still carry every column, so a 50-column
  frame pays to gather 50 columns to aggregate two. The planner prunes for lazy
  pipelines; the eager verb does not yet. This is the next lever on this path.
- **Multi-chunk reads.** Every scenario reads with the default batch size, so
  the frame is one chunk. Chunked behaviour is covered by specs, not measured.
- **Comparisons with other libraries.** The table compares Prism's own paths
  against each other; cross-library numbers would need a Polars/pandas fixture
  and are out of scope for v0.1.

## Machines and load

Absolute timings move by 15–80% with background load: two runs of this harness
on the same machine differed by ~15% on unchanged scenarios (`sort_by` one key:
202 ms vs 240 ms) and by ~1.8x once, with unrelated processes at load average 26.
Compare paths within one run, and re-run `make bench` on an idle machine before
believing a change of less than ~20%.

The table above was taken at load average ~16 on 10 cores, which is why its
unchanged rows sit 1.3–1.5x above the earlier idle snapshot (`filter` 50 vs 48 ms,
`sort_by` 212 vs 202 ms, `join` 309 vs 219 ms). The grouping A/B is the
load-independent reading of the Phase 15 change: both columns come from the same
harness in the same session, five minutes apart.
