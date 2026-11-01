# Benchmarks

```bash
make bench          # crystal run --release bench/main.cr
```

[`bench/main.cr`](../bench/main.cr) builds one 1e6-row frame, runs the verbs and
the Parquet paths on it, and prints a markdown table. Each scenario runs once to
warm up and then five times; the table reports the best and the median, with
`GC.collect` between runs. The fixtures are seeded, so the row counts are
reproducible.

The numbers below are from a single-threaded run on an Apple M4 (arm64) with
Crystal 1.22.2, on a frame of `region` (`Utf8`, 4 values), `user_id` (`Int64`,
1e5 values), `qty` (`Int64`, 0–999) and `price` (`Float64`), single chunk.

## Results

| scenario | best (ms) | median (ms) | out rows | note |
| --- | ---: | ---: | ---: | --- |
| filter col > lit | 53.1 | 54.0 | 499241 | ≈ half the rows |
| filter on two conjuncts | 70.8 | 71.9 | 125221 | ≈ a quarter |
| select 2 of 4 columns | 0.0 | 0.0 | 1000000 | projection, no copy |
| with_column (price * qty) | 5.2 | 5.3 | 1000000 | casts qty to Float64 |
| to_arrow (rechunk) | 0.0 | 0.0 | 1000000 | single chunk already |
| sort_by one key | 240.1 | 260.6 | 1000000 | lexsort_to_indices + take |
| sort_by two keys | 1018.9 | 1025.0 | 1000000 | second key is `Utf8` |
| sort_by + limit 10 | 228.6 | 230.8 | 10 | eager: sorts every row, then trims |
| group_by 4 keys | 249.4 | 253.1 | 4 | sort-based grouping |
| group_by 1e5 keys | 439.3 | 443.0 | 99999 | one sort, three aggregates |
| distinct on 1e5 keys | 162.8 | 163.1 | 99999 | sort + partition |
| series n_unique | 143.7 | 145.9 | 99999 | sort + partition |
| join 1e6 -> 1e5 on user_id | 284.4 | 289.4 | 1000000 | hash on boxed keys |
| write_parquet zstd(3) | 598.4 | 603.8 | 0 | row groups of 100k |
| read_parquet (all columns) | 148.9 | 159.5 | 1000000 | baseline, 15.0 MiB |
| read_parquet projection (2 of 4) | 90.2 | 90.9 | 1000000 | projection pushdown |
| read_parquet predicate | 174.1 | 182.2 | 99557 | row filter, unclustered |
| read_parquet projection + predicate | 133.0 | 134.9 | 99557 | pushdown, unclustered |
| read_parquet limit 100 | 4.0 | 4.0 | 100 | slice pushdown |
| scan -> filter -> select -> collect | 133.2 | 135.4 | 99557 | lazy, pushed down |
| eager read + filter + select | 153.1 | 154.0 | 99557 | no pushdown |
| read clustered (all rows) | 167.1 | 167.9 | 1000000 | 20.8 MiB, `seq` column |
| read clustered predicate (last 10%) | 117.7 | 118.3 | 99999 | row groups pruned |
| read clustered predicate + projection | 89.8 | 91.8 | 99999 | row groups pruned |
| lazy group_by -> sort -> head | 345.3 | 349.1 | 10 | slice pinned to the sort |

## What the numbers say

**Element-wise verbs are free.** `select` and `to_arrow` measure 0 ms because
they hand back the same buffers: projection reuses the arrays, and one chunk is
already the concatenated form. `with_column` costs one new buffer (5 ms), and a
half-frame `filter` costs 53 ms — about 19M rows/s including the null-safe
predicate evaluation.

**Sorting is the expensive verb, and it is amortized.** One key is 240 ms, two
keys 1.0 s (the second key is `Utf8`, so it compares variable-length bytes).
`group_by` at 4 keys costs 249 ms and at 1e5 keys 439 ms — one sort either way,
which is why adding more aggregates to a single `aggregate` call is nearly
free.

**A pinned slice avoids the gather, not the sort.** The table above measures the
pre-window code path: `sort + head(10)` cost 229 ms against 240 ms for the plain
sort, because every row was copied and then thrown away. Since then
`Plan::Sort` executes `sort_by_slice`, which sorts and then collects only the
window's indices. That run was taken while the machine carried an unrelated load
average of ~26, so only its ratios are meaningful:

| scenario (same run, machine loaded) | best (ms) | vs plain sort |
| --- | ---: | ---: |
| `sort_by` one key | 318.9 | 1.00 |
| `sort_by + head(10)`, eager (sort, then trim) | 312.9 | 0.98 |
| `scan → sort_by → head(10)`, window gathered | 216.3 | 0.68 |

The eager sort in that run is 1.33× its idle figure, so scaled back
`sort_by + head(10)` lands near 160 ms: roughly 30% saved against 229 ms, and now
*faster than a plain sort*, which is what dropping the 1e6-row gather should do.
The sort itself is still `O(n log n)`: `arrow-cr`'s `lexsort_to_indices` accepts
a `limit`, but this port sorts the whole frame and truncates, so a bounded-heap
top-k would need an upstream kernel.

**Row-keyed paths are viable but not dominant.** `distinct` (163 ms) and
`n_unique` (144 ms) on 1e5 distinct keys are sort-bound, not hash-bound, and the
1e6→1e5 join is 284 ms with boxed `Array(Value)` keys. An `arrow-row` encoding
would replace the sort with a hash set and the boxing with byte views, so these
are the three scenarios it should move — but each is already cheaper than a
single sort of the same frame, and the whole-pipeline costs (read + transform)
sit in the hundreds of milliseconds. That is the evidence behind "not urgent;
revisit when the `arrow-cr` port lands".

**Pushdown pays, and row-group pruning needs clustered data.** Reading 2 of 4
columns is 90 ms against 149 ms for all of them (40% saved). `limit 100` is 4 ms
because the reader stops after the first batch (97% saved). A predicate on an
*unclustered* column is the one case that loses: 174 ms to read and filter
against 149 ms to just read, because Parquet's min/max statistics cannot prune
row groups that each span the whole value range — every group must be decoded,
and the rows thrown away afterwards. The same predicate on a clustered column
(`seq` ascending, so a threshold excludes 9 of 10 row groups) is 118 ms against
167 ms, and 90 ms with projection too. The lesson is about the data, not the
optimizer: predicate pushdown is worth it when the predicate correlates with the
physical layout, which is what row-group statistics are for.

**Lazy beats eager by the projection it pushes.** `scan → filter → select` is
133 ms against 153 ms for the eager `read → filter → select` on the same file:
the scan reads 3 of 4 columns instead of 4 and filters while decoding. The gap is
small because the predicate's own column dominates the read.

## What is deliberately missing

- **Allocation/copy counts.** The plan's "never allocates" list is still checked
  by specs (a projected `select` is the same buffers), not by a counter; Crystal
  has no per-operation allocation hook, so a copy-count harness would need
  `GC.stats` deltas, which are too coarse to attribute.
- **A bounded-heap top-k.** `sort_by_slice` gathers only the window, but the sort
  still orders every row. `arrow-cr`'s `lexsort_to_indices` takes a `limit` and
  does not model upstream's bounded heap, so a true top-k is an upstream change,
  not a Prism one.
- **Multi-chunk reads.** Every scenario reads with the default batch size, so
  the frame is one chunk. Chunked behaviour is covered by specs, not measured.
- **Comparisons with other libraries.** The table compares Prism's own paths
  against each other; cross-library numbers would need a Polars/pandas fixture
  and are out of scope for v0.1.

## Machines and load

Absolute timings move by ~1.3x with background load: the idle run above and the
loaded run used for the top-k comparison differ by that much on identical
scenarios. Compare paths within one run, and re-run `make bench` on an idle
machine before drawing conclusions about a change of less than ~20%.
