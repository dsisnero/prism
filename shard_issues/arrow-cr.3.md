# `arrow-cr`: `is_in` is a comparator binary search, not a hash probe

## Status: open enhancement, consumer-visible (2026-11-03)

Verified against `arrow-cr` `744f9ae`. Unlike most of this port, there is no
upstream `arrow-rs` function to be faithful to: `arrow-rs` 60.0.0 has no `is_in`
kernel (`arrow-ord/src/comparison.rs`, `arrow-array/src/array/mod.rs` and
`arrow-arith/src/numeric.rs` all lack it), so this is `arrow-cr`'s own addition
(its comment says the name follows Red Arrow's array API). The algorithm is
therefore a free choice, and the current one costs 23x what the obvious one does
for string keys.

## Failure

`ArrayBase#is_in` (`src/arrow/collection_sets.cr:31`) sorts the set's indices with
`make_comparator` and then binary-searches per row:

```crystal
compare = Arrow.make_comparator(self, values, SortOptions.default)
set_compare = Arrow.make_comparator(values, values, SortOptions.default)
indices = (0...values.len).to_a
  .sort! { |left, right| set_compare.call(left, right) }
matches = Array(Bool).new(len) do |index|
  ...
  collection_set_contains?(index, indices, compare)   # binary search
end
```

That is `O(n log m)` comparator calls, and each call for a string key walks the
full byte slice through a proc.

## Reproduction

```crystal
require "arrow-cr"

ROWS   = 1_000_000
RNG    = Random.new(42)
titles = Array.new(12_000) { |index| "title #{index}" }
column = Arrow::StringArray.from_values(
  Array.new(ROWS) { "title #{RNG.rand(40_000)}" },
)
set    = Arrow.array(titles)

column.is_in(set)   # ~673 ms (best of five, --release, Apple M4)
```

The same probe against a `Set(String)` in Crystal is **29.6 ms** for the same
1,000,000 rows and 12,000 members — 23x faster.

Measured through `Prism` (`filter(col("title").is_in(titles))`, which is this
kernel plus the mask): 682.0 ms for 1e6 rows.

## Expected vs actual

| Path | Work | Measured |
| --- | --- | --- |
| `ArrayBase#is_in`, 12k string set, 1e6 rows | `n log m` comparator calls | 673.2 ms |
| `Set(String)#includes?`, same input | `n` hashes | 29.6 ms |
| `Prism.filter(is_in)` end-to-end | kernel + mask | 682.0 ms |

For a 9.2 GB scan the difference is seconds against minutes, so a consumer that
already holds a `Set` cannot switch to this kernel without a regression, even
though the kernel is what makes the predicate expressible (and pushable) at all.

## Downstream impact (`prism`)

`Expr#is_in` (Phase 26) reaches this kernel for every dtype. It closes the
expressibility gap: `Prism.read_parquet(path, columns: [...], predicate:
col("title").is_in(titles))` now pushes a set-membership filter into the reader.
The per-row cost is still this kernel's, so the phase records the hash fast path
as the next measured item rather than duplicating a kernel inside Prism
([`docs/benchmarks.md`](../docs/benchmarks.md), plan §14).

## Suggested fix direction (not a patch)

1. Build a `Set` of the set's values for the dtypes where a `Value` probe is
   exact and cheap (strings, binary, primitives, booleans), and probe per row —
   `O(n)`, no comparator.
2. Keep the comparator path for everything else (nested, decimal, interval,
   dictionary-of-nested), and for `skip_nulls: true`'s null rule.
3. Nulls: the current kernel's rule is "null matches null unless `skip_nulls`".
   A hash path must preserve that (probe null against a set-null flag), and
   `Prism` relies on it for `nulls_equal: true`.
4. If a row-encoded (`arrow-row`) set is simpler to share across dtypes, that is
   also `O(n)` after one encoding pass, but it allocates per row; measure both.

## Acceptance

- `is_in` returns exactly what the comparator path returns, including nulls,
  `skip_nulls`, NaN payloads and signed zero (floats use total order today).
- The string-key benchmark above lands within ~2x of a Crystal `Set` probe rather
  than 23x.
- Every existing `is_in` test still passes unchanged.
