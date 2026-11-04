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
1e5 values), `qty` (`Int64`, 0–999) and `price` (`Float64`), one chunk. Read the
table for comparisons *within* the run, and the A/B sections below for the
Phase 15 (grouping) and Phase 18 (bounded sort) changes, which are paired runs
that background load cannot fake.

## Results

| scenario | best (ms) | median (ms) | out rows | note |
| --- | ---: | ---: | ---: | --- |
| filter col > lit | 53.2 | 54.1 | 499241 | ≈ half the rows |
| filter on two conjuncts | 71.7 | 72.7 | 125221 | ≈ a quarter |
| select 2 of 4 columns | 0.0 | 0.0 | 1000000 | projection, no copy |
| with_column (price * qty) | 5.1 | 5.7 | 1000000 | casts qty to Float64 |
| to_arrow (rechunk) | 0.0 | 0.0 | 1000000 | single chunk already |
| sort_by one key | 229.1 | 230.2 | 1000000 | lexsort_to_indices + take |
| sort_by two keys | 402.4 | 406.0 | 1000000 | second key is `Utf8` |
| sort_by + limit 10 (eager) | 224.9 | 227.4 | 10 | sorts every row, then trims |
| top_n(10) | 8.8 | 9.5 | 10 | bounded sort + window, eager |
| scan → sort_by → head(10) | 8.8 | 9.4 | 10 | bounded selection + window |
| group_by region (4 groups) | 237.5 | 241.0 | 4 | sort-based grouping |
| group_by region + user_id (2 keys) | 557.8 | 563.6 | 367321 | two-key sort, 4 x user_id |
| group_by 1e5 keys | 205.2 | 210.3 | 99999 | one sort, three aggregates |
| group_by 1e5 keys, 48 unused columns | 49.8 | 50.6 | 86463 | Phase 16 pruning |
| distinct on 1e5 keys | 61.2 | 62.5 | 99999 | row-keyed seen-set |
| series n_unique | 51.7 | 52.2 | 99999 | row-keyed seen-set |
| join 1e6 → 1e5 on user_id | 223.6 | 226.7 | 1000000 | row-encoded keys |
| write_parquet zstd(3) | 615.1 | 618.8 | 0 | row groups of 100k, 15.0 MiB |
| read_parquet (all columns) | 133.9 | 134.5 | 1000000 | baseline |
| read_parquet projection (2 of 4) | 84.4 | 85.0 | 1000000 | projection pushdown |
| read_parquet predicate | 172.8 | 175.6 | 99557 | row filter, unclustered |
| read_parquet projection + predicate | 134.4 | 135.1 | 99557 | pushdown, unclustered |
| read_parquet limit 100 | 3.9 | 4.0 | 100 | slice pushdown |
| scan → filter → select → collect | 134.6 | 136.8 | 99557 | lazy, pushed down |
| eager read + filter + select | 158.6 | 159.8 | 99557 | no pushdown |
| read clustered (all rows) | 170.0 | 173.4 | 1000000 | 20.8 MiB, `seq` column |
| read clustered predicate (last 10%) | 120.3 | 121.3 | 99999 | row groups pruned |
| read clustered predicate + projection | 92.2 | 92.9 | 99999 | row groups pruned |
| lazy group_by → sort → head | 192.7 | 194.4 | 10 | slice pinned to the sort |

## What the numbers say

**Element-wise verbs are free.** `select` and `to_arrow` measure 0 ms because
they hand back the same buffers: projection reuses the arrays, and one chunk is
already the concatenated form. `with_column` costs one new buffer (5 ms), and a
half-frame `filter` costs 50 ms — about 20M rows/s including null-safe predicate
evaluation.

**Sorting is the expensive verb, and it is amortized.** One key is 229 ms and two
keys 402 ms (the second key is `Utf8`, so it compares variable-length bytes; the
multi-key path got 2.4x faster in Phase 21 and row encoding took the two-string
case another 1.4x in Phase 24 — see the A/Bs below). `group_by` at 4 groups costs
238 ms, at 1e5 keys 205 ms, and at two keys 558 ms — one sort either way, and the
aggregates are nearly free in all of them.

**A pinned window selects and gathers only its rows.** `sort_by + head(10)`
costs 8.7 ms through the plan and 8.8 ms through the eager `top_n(10)` verb,
against 218 ms for the two-call eager form, which copies all 1e6 rows before
discarding them. Eager code has to say so: `sort_by` cannot see the `head` that
follows it, which is why `top_n` exists (`DataFrame#top_n`, Phase 20).

Both halves were needed: the gather came first (`DataFrame#sort_by_slice`), and
since Phase 18 the sort is bounded too — `sort_by_slice` hands
`Arrow.lexsort_to_indices` a limit of `offset + count`, and the sort selects that
prefix with `partial_sort` instead of ordering every row (`arrow-cr` `5b4b958`,
filed as
[`shard_issues/arrow-cr.1.md`](../shard_issues/arrow-cr.1.md)).

Paired before/after on this machine, same 1e6-row frame, back to back
(`src/prism/verbs/sort.cr` stashed for the "before" run):

| scenario | before (ms) | after (ms) | speedup |
| --- | ---: | ---: | ---: |
| sort_by(qty) → head(10) | 81.9 | 10.1 | 8.1× |
| sort_by(qty desc, price) → head(10) | 80.4 | 3.1 | 26× |
| sort_by(qty) → slice(500_000, 10) | 80.0 | 49.7 | 1.6× |

The mid-frame window is the case the bound cannot help: `offset + count` is
500,010 of 1,000,000 rows, so most of the selection is still paid. A window near
either end of the frame is where the bound pays off.

**Multi-key sorts compose stable single-key passes when a comparison is
expensive (Phases 21–22).** The profile that started this: a two-key sort cost
900 ms while each key alone cost 100–200 ms and the row gather only 35 ms, so the
cost was the comparator chain — one proc call per key per comparison, ~37 ns
across ~20M comparisons.

Composing stable single-key sorts instead (least significant key first, then
refining with `Arrow.sort_to_indices` through `Arrow.take`) produces exactly the
same indices and runs the dtype-specialized sort per pass. Paired before/after,
same 1e6-row frame, minutes apart:

| scenario | before (ms) | after (ms) | speedup |
| --- | ---: | ---: | ---: |
| `sort_by(region, price)` (2 keys, full) | 900.4 | 360.0 | 2.5× |
| `sort_by(region, qty, price)` (3 keys, full) | 964.2 | 482.4 | 2.0× |
| `top_n(10, region, price)` (bounded) | 84.1 | 87.0 | unchanged |
| `sort_by(price)` (1 key) | 201.1 | 203.3 | unchanged |

Phase 22 turned that into a dtype rule, because the passes only pay when a
comparison itself is expensive (best of five, 1e6 rows):

| keys | chain (ms) | passes (ms) |
| --- | ---: | ---: |
| `Utf8` (4 values) + `Int64` | 877 | 293 |
| `Utf8` (1e3 values) + `Utf8` | 1146 | 367 |
| `Utf8` (4 values) + `Float64` | 911 | 329 |
| `Int64` (1e5 values) + `Float64` | 224 | 301 |
| `Int64` (1e3 values) + `Int64` | 277 | 248 |
| `Float64` (4 values) + `Float64` | 325 | 257 |

Fixed-width keys land within ~15% in either direction, so they keep the chain; a
variable-length (or nested) key makes the chain 2.5–3x slower than the passes.
Both numbers are this dependency's, not Arrow's: `arrow-cr` compares whole byte
slices and materializes a string per row, where upstream `arrow-rs` compares a
4-byte big-endian prefix first and specializes 2–5 columns. Polars sidesteps the
comparison entirely — its default multi-key sort row-encodes the keys once and
sorts the bytes. The threshold here is calibrated to `arrow-cr`; moving a dtype
across it needs a paired measurement.

`Prism::SortEngine` holds that dispatch, and `group_by`'s key sort now uses it
too — the case where the difference shows most, since the key sort is most of a
multi-key group-by:

| scenario | before (ms) | after (ms) | speedup |
| --- | ---: | ---: | ---: |
| `group_by(region, user_id)` | 1178.8 | 528.7 | 2.2× |
| `group_by(region, qty, user_id)` | 1493.3 | 843.8 | 1.8× |
| `group_by(user_id, price)` (fixed width) | 520.4 | 514.6 | unchanged |
| `group_by(user_id)` (one key) | 179.2 | 182.1 | unchanged |

Three shapes deliberately keep the comparator chain. A bounded window: the pass
sequence has to order every row to build its least-significant ordering first,
which costs a full pass (~110 ms) against ~80 ms for the whole bounded lexsort.
Keys the per-key sort cannot express — struct, map, decimal, interval, run-end,
union — because `make_comparator` supports more dtypes than `sort_to_indices`.
And all-fixed-width keys, per the table above.

**A sort with two string keys encodes rows instead of gathering them (Phase 24).**
The pass path re-sorts by each key and gathers that key through the current order,
and a gather of a string column copies the strings — so with two variable-length
keys the passes pay twice for what the output pays once. Encoding the keys into
`arrow-row` bytes and sorting those (`arrow-row` already backs `Prism::RowKeys`)
avoids the intermediate gathers, and it is what Polars does by default
(`arg_sort_multiple` -> `_get_rows_encoded` -> `argsort_multiple_row_fmt`), with
per-key direction and null placement baked into the bytes by `SortField` options.

Paired, two prebuilt binaries interleaved over three rounds, best of five each,
1e6 rows:

| scenario | Phase 22 paths | row encoded | speedup |
| --- | ---: | ---: | ---: |
| `sort_by(region, city)` (2 × `Utf8`) | 519.4 | 377.9 | 1.37× |
| `group_by(region, city)` (2 × `Utf8`) | 615.7 | 485.1 | 1.26× |
| `sort_by(region, user_id)` (1 × `Utf8`) | 451.4 | 441.0 | unchanged |
| `sort_by(user_id, qty)` (fixed width) | 416.8 | 419.8 | unchanged |
| `top_n(10, region, city)` (bounded) | 154.3 | 154.4 | unchanged |

The rule is two or more variable-length keys, and each side of it is in the table:
with one variable key the encoding buys nothing (the output gathers that column
anyway), and with none it is pure overhead. Bounded windows keep the bounded
lexsort, which selects a prefix that a full row sort cannot. `Prism::SortEngine`
holds the decision table and its spec pins it.

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
- **A bounded-heap top-k in Prism.** The sort now selects only the prefix a
  window needs (`Arrow.lexsort_to_indices` with a `limit`, `arrow-cr` `5b4b958`),
  so Prism has nothing left to add here: the remaining work is the selection
  itself inside `arrow-cr`.
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

The table above is a later, less loaded run: its unchanged rows sit ~0.9x the
earlier loaded snapshot (`filter` 46 vs 50 ms, `sort_by` 185 vs 212 ms, `join`
187 vs 309 ms). The two A/B sections are the load-independent readings — the
grouping change (Phase 15) and the bounded sort (Phase 18) — because each pair
comes from the same harness in the same session, minutes apart.
