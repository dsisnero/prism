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
1e5 values), `qty` (`Int64`, 0–999) and `price` (`Float64`), one chunk.

## Results

| scenario | best (ms) | median (ms) | out rows | note |
| --- | ---: | ---: | ---: | --- |
| filter col > lit | 48.2 | 49.6 | 499241 | ≈ half the rows |
| filter on two conjuncts | 59.3 | 61.2 | 125221 | ≈ a quarter |
| select 2 of 4 columns | 0.0 | 0.0 | 1000000 | projection, no copy |
| with_column (price * qty) | 4.6 | 5.0 | 1000000 | casts qty to Float64 |
| to_arrow (rechunk) | 0.0 | 0.0 | 1000000 | single chunk already |
| sort_by one key | 202.4 | 204.5 | 1000000 | lexsort_to_indices + take |
| sort_by two keys | 934.5 | 998.8 | 1000000 | second key is `Utf8` |
| sort_by + limit 10 (eager) | 211.2 | 212.7 | 10 | sorts every row, then trims |
| scan → sort_by → head(10) | 130.2 | 131.3 | 10 | gathers only the window |
| group_by 4 keys | 220.5 | 221.0 | 4 | sort-based grouping |
| group_by 1e5 keys | 381.1 | 383.0 | 99999 | one sort, three aggregates |
| distinct on 1e5 keys | 53.8 | 55.4 | 99999 | row-keyed seen-set |
| series n_unique | 47.7 | 49.5 | 99999 | row-keyed seen-set |
| join 1e6 → 1e5 on user_id | 219.4 | 227.6 | 1000000 | row-encoded keys |
| write_parquet zstd(3) | 532.4 | 542.8 | 0 | row groups of 100k, 15.0 MiB |
| read_parquet (all columns) | 116.6 | 119.6 | 1000000 | baseline |
| read_parquet projection (2 of 4) | 74.3 | 75.1 | 1000000 | projection pushdown |
| read_parquet predicate | 154.7 | 155.5 | 99557 | row filter, unclustered |
| read_parquet projection + predicate | 116.1 | 117.9 | 99557 | pushdown, unclustered |
| read_parquet limit 100 | 3.0 | 3.4 | 100 | slice pushdown |
| scan → filter → select → collect | 118.6 | 119.9 | 99557 | lazy, pushed down |
| eager read + filter + select | 142.4 | 144.1 | 99557 | no pushdown |
| read clustered (all rows) | 159.1 | 161.2 | 1000000 | 20.8 MiB, `seq` column |
| read clustered predicate (last 10%) | 110.6 | 115.4 | 99999 | row groups pruned |
| read clustered predicate + projection | 86.0 | 89.4 | 99999 | row groups pruned |
| lazy group_by → sort → head | 306.0 | 321.2 | 10 | slice pinned to the sort |

## What the numbers say

**Element-wise verbs are free.** `select` and `to_arrow` measure 0 ms because
they hand back the same buffers: projection reuses the arrays, and one chunk is
already the concatenated form. `with_column` costs one new buffer (5 ms), and a
half-frame `filter` costs 48 ms — about 21M rows/s including null-safe predicate
evaluation.

**Sorting is the expensive verb, and it is amortized.** One key is 202 ms, two
keys 935 ms (the second key is `Utf8`, so it compares variable-length bytes).
`group_by` at 4 keys costs 220 ms and at 1e5 keys 381 ms — one sort either way,
so extra aggregates in one `aggregate` call share it. They are not free, though:
each one still pays per group (see the decomposition below).

**A pinned slice gathers only its window.** `sort_by + head(10)` costs 130 ms
through the plan (`DataFrame#sort_by_slice`) against 211 ms for the eager
sort-then-trim, which still copies all 1e6 rows before discarding them. The sort
itself stays `O(n log n)`: a bounded-heap top-k would need an `arrow-cr` kernel
this port does not model (`lexsort_to_indices` accepts a `limit` but sorts
everything).

**Row keys made `distinct` and `n_unique` ~3x faster.** `Prism::RowKeys` encodes
the key columns with `Arrow::RowConverter` and walks a byte seen-set, so the sort
is gone. Before the switch, measured on the same frame with the same harness:
`distinct` 163 ms, `n_unique` 144 ms, join 284 ms. Now: 54 ms, 48 ms, 219 ms. The
earlier run's unchanged scenarios were ~15% slower (`sort_by` 240 ms vs 202 ms
here), so even discounted for that the first two are ~2.5x, and the join is the
smallest win — ~1.3x — because a 1e6-row encode, probe and 1e6-row gather is not
dominated by key hashing.

The encoding also fixed two semantics that no amount of tuning would have: NaN
keys now match (Crystal's `==` kept them apart, so a NaN could never join or
dedupe), and keys follow `==` rather than `totalOrder`, so `-0.0` and `0.0` are
one key. That needs one normalizing pass over float key columns, because the row
format stores `totalOrder` bits — the same `canonical_f64` step Polars takes
before hashing. Int and string keys pay nothing for it.

**Where a group-by actually spends its time.** The sort is *not* the cost, which
settles the hash-grouping question. Decomposing `group_by("user_id")` on the same
1e6-row frame (1e5 distinct keys; this probe ran under load, so read the ratios,
not the absolutes):

| step | best (ms) |
| --- | ---: |
| `lexsort_to_indices` on the key alone | 35 |
| `sort_by("user_id")` (lexsort + take 4 columns) | 183 |
| `group_by` + `count` only | 403 |
| `group_by` + one `sum` | 508 |
| `group_by` + three aggregates | 1005 |
| `distinct` on the same keys (row-keyed) | 91 |

So the sort that a hash table would remove is ~35 ms of a 1005 ms group-by —
under 4%. The rest is per group: ~4 µs each for `count`, and ~2.5 µs per extra
aggregate (a `Series`, an `EvalContext`, a length-1 array, and one `Arrow.concat`
piece per group). `distinct` is 11x cheaper on the identical keys because it does
one hash pass and no per-group slicing at all. A hash-based `group_by` would
therefore buy a few percent while giving up contiguous group views and
deterministic order, which is why it was measured and then declined rather than
built.

**Pushdown pays, and row-group pruning needs clustered data.** Reading 2 of 4
columns is 74 ms against 117 ms for all of them (37% saved). `limit 100` is 3 ms
because the reader stops after the first batch (97% saved). A predicate on an
*unclustered* column is the one case that loses: 155 ms to read and filter
against 117 ms to just read, because Parquet's min/max statistics cannot prune
row groups that each span the whole value range — every group must be decoded,
and the rows thrown away afterwards. The same predicate on a clustered column
(`seq` ascending, so a threshold excludes 9 of 10 row groups) is 111 ms against
159 ms, and 86 ms with projection too. The lesson is about the data, not the
optimizer: predicate pushdown is worth it when the predicate correlates with the
physical layout, which is what row-group statistics are for.

**Lazy beats eager by the projection it pushes.** `scan → filter → select` is
119 ms against 142 ms for the eager `read → filter → select` on the same file:
the scan reads 3 of 4 columns instead of 4 and filters while decoding. The gap is
small because the predicate's own column dominates the read.

## What is deliberately missing

- **Allocation/copy counts.** The plan's "never allocates" list is still checked
  by specs (a projected `select` is the same buffers), not by a counter; Crystal
  has no per-operation allocation hook, so a copy-count harness would need
  `GC.stats` deltas, which are too coarse to attribute.
- **A bounded-heap top-k.** `sort_by_slice` gathers only the window, but the sort
  still orders every row. That is an `arrow-cr` kernel change, not a Prism one.
- **Hash-based grouping.** Still deliberately sort → partition (contiguous group
  views, deterministic order), and the measurement below says to keep it that
  way: the sort is a small fraction of a group-by, so a hash table cannot pay for
  the views and the ordering it would give up. Measured, not assumed.
- **Per-group aggregate overhead.** A group-by at 1e5 keys spends ~95% of its
  time *per group*, not in the sort (see below). Vectorizing that — computing one
  kernel across all groups instead of one slice per group — is the real
  optimization, and it is a redesign of the aggregate layer, not a flag.
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
