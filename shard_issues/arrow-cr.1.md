# `arrow-cr`: sorting with `limit` sorts every row instead of selecting the top-k

## Status: open, reproduced 2026-11-02

`sort_to_indices` and `lexsort_to_indices` accept a `limit`, but only truncate the
result *after* a full sort. The port already contains `partial_sort`; nothing
calls it, so `limit: 10` on a million rows costs the same as no limit.

## Failure

`limit` is documented as a bound on the number of rows returned, and it does
bound the output. It does not bound the work: the sort still compares every row.

`src/arrow/sort.cr`:

- `sort_to_indices` (line 72) keeps the `limit` only to pass it to
  `build_index_output` (line 116), which appends at most `limit` indices from
  fully sorted `valid`/`nulls` lists.
- `sorted_valid_indices` (line 139) sorts all valid positions per dtype, before
  truncation ever happens.
- `lexsort_to_indices` (line 470) has a separate multi-column path: it allocates
  `Array(UInt32).new(row_count)`, sorts **every** index with a comparator chain,
  then returns `indices[0, length]`.
- `partial_sort` (line 15) is implemented (quickselect, then sort the selected
  prefix) and has no caller in `src/`. `grep -n "partial_sort" src/arrow/sort.cr`
  returns only its own definition and its partition helper.

## Reproduction

```crystal
require "arrow-cr"

ROWS   = 1_000_000
values = Arrow::Int64Array.from_values(
  Array(Int64).new(ROWS) { |i| (i.to_i64 * 7919_i64) % ROWS.to_i64 }
).as(Arrow::ArrayBase)

def timeit(label : String, &) : Nil
  yield
  times = [] of Float64
  3.times do
    started = Time.monotonic
    yield
    times << (Time.monotonic - started).total_milliseconds
  end
  puts "#{label.ljust(36)} best #{times.min.round(1)} ms"
end

timeit("sort_to_indices, no limit") { Arrow.sort_to_indices(values) }
timeit("sort_to_indices, limit 10") { Arrow.sort_to_indices(values, nil, 10) }

two = [
  Arrow::SortColumn.new(values),
  Arrow::SortColumn.new(values),
]
timeit("lexsort_to_indices 2 keys, no limit") do
  Arrow.lexsort_to_indices(two)
end
timeit("lexsort_to_indices 2 keys, limit 10") do
  Arrow.lexsort_to_indices(two, 10)
end
```

Output on this machine (Apple M4, Crystal 1.22.2, `--release`, best of three):

```text
sort_to_indices, no limit            best 94.7 ms
sort_to_indices, limit 10            best 87.1 ms
lexsort_to_indices 2 keys, no limit  best 261.2 ms
lexsort_to_indices 2 keys, limit 10  best 231.5 ms
```

## Expected vs actual

| Call | Expected work | Actual work |
| --- | --- | --- |
| `sort_to_indices(values, nil, 10)` | one pass + bounded selection, `O(n)` | full sort, `O(n log n)` |
| `lexsort_to_indices(two, 10)` | bounded selection over the comparator chain | full sort of `row_count` indices |
| result length | `min(limit, len)` | `min(limit, len)` (correct) |

For 1e6 rows and k=10 the gap is two orders of magnitude: the port spends ~90 ms
and ~230 ms to produce ten indices.

## Downstream impact (`prism`)

`prism` sorts through this API for `DataFrame#sort_by(...).head(n)` /
`LazyFrame#sort_by(...).head(n)`. The pinned-slice path already gathers only the
window (`DataFrame#sort_by_slice`), but it cannot avoid the sort:
`docs/benchmarks.md` measures `sort_by -> head(10)` at 153 ms against 318 ms for
eager sort-then-trim, and the 165 ms difference is the gather, not the sort. This
is the only item in `prism`'s "Next up" that is blocked on `arrow-cr`; the
consumer side is ready.

## Suggested fix direction (not a patch)

When `limit` is set and is small relative to the row count:

1. Build the candidate index array (`Array(UInt32)` of positions, as the
   multi-column path does today).
2. `partial_sort` it with the comparator (per-key comparator chain for
   `lexsort_to_indices`, the dtype-specific comparison for `sort_to_indices`),
   with the existing index tie-break so equal keys stay stable.
3. Keep `partition_validity` and the `nulls_first?` / `nulls_last?` handling in
   `build_index_output` so null placement does not change; only the prefix of
   `valid` needs to be selected.

Upstream `arrow-ord` implements exactly this: `partial_sort` exists for the
bounded case and `lexsort_to_indices` uses it when a limit is supplied. Falling
back to the current full sort for large limits (or `limit >= len`) is fine — the
win is at `k ≪ n`.

## Acceptance

- Every existing sort test still passes, and outputs are byte-identical to today
  for `limit: nil` and for `limit >= len`.
- Outputs match today for small limits too, including null placement and the
  equal-key index tie-break.
- A benchmark in the crate's style showing the bounded path at `k=10`,
  `n=1e6` well below the full sort (target: within a small multiple of one pass).
- `partial_sort` gains at least one caller, or is deleted if it stays unused.
