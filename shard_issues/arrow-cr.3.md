# `arrow-cr`: `is_in` is a comparator binary search, not a hash probe

## Status: fixed upstream (2026-11-03)

Fixed in `arrow-cr` commit `78b0cd4` ("perf: probe a hash set in `is_in`
instead of binary-searching the sorted set"), on `main`. `ArrayBase#is_in` now
builds a hash set of the member values and probes each row; the comparator sort
and binary search survive only as the fallback for the types whose equality is
not a hash-key equality (nested, dictionary, run-end, interval, null).

Re-measured with the reproducer below on this machine (Crystal 1.22.2,
`--release`, best of five, same 1e6 rows / 12k members):

```text
ArrayBase#is_in                best  1017.9 ms   ->  29.5 ms
Set(String)#includes?          best    45.6 ms   ->  25.8 ms
ratio (kernel / Set)                  21.95x    ->   1.14x
matched rows                  300044 in every run
```

The absolute numbers differ from the M4 in the original report; the ratio is the
thing that moved: 21.95x → 1.14x, which is inside the "~2x of a Crystal `Set`
probe" acceptance bar. The benchmark is now checked in, so the regression is
measured rather than remembered:

```bash
crystal run --release scripts/benchmarks/is_in.cr
```

Correctness is pinned by `spec/arrow/is_in_hash_spec.cr`, a differential oracle
that compares the kernel against a spec-local total-order `Set` probe for every
leaf dtype: nulls, `skip_nulls`, NaN payloads, signed zero, decimals, logical
temporal types, empty sets/sources, the comparator fallback types and a
randomized sample. `spec/arrow/crystal_sets_spec.cr` passes unchanged.

### What changed (so the consumer can reason about it)

- Primitives (including `Int128`/`I256` decimals and the logical
  temporal/duration types that share their storage) key on the value.
- `Float16`/`Float32`/`Float64` key on the **bit pattern**, because equality here
  is the Arrow total order: a numeric key would lose NaN payloads and signed
  zero (`Float64#==` is false for NaN; `Float16#==` says `-0.0 == 0.0`).
- `Utf8`/`LargeUtf8`/`Utf8View`/`Binary`/`LargeBinary`/`BinaryView`/
  `FixedSizeBinary` key on their zero-copy byte slice. `value_bytes` became
  public for this, so the probe no longer allocates a `String` per row — that
  copy was roughly half the remaining cost.
- A null in the member set is a flag, not a key: a null input row matches iff the
  set holds a null and `skip_nulls` is false. This is exactly the old comparator
  rule, so `nulls_equal: true` consumers keep the same results.
- `arrow-rs` 60.0.0 still has no `is_in`, so there is no upstream kernel to track:
  this is the port's own algorithm, shaped like Arrow C++
  (`SetLookupState`, `cpp/src/arrow/compute/kernels/scalar_set_lookup.cc`) and
  Polars (`PlHashSet` over `ToTotalOrd` keys,
  `polars-ops/src/series/ops/is_in.rs`).

### Downstream (`prism`)

The 23x caveat is gone, so the kernel is now the faster per-row path for a large
scan as well as the expressible one: a hand-written `Set(String)` `RowFilter`
(29.6 ms at the original report, 25.8 ms here) and this kernel (29.5 ms here) are
within a few percent. Switching `Expr#is_in` from the hand-rolled filter to the
kernel is now a code-simplicity call rather than a performance trade; either way
the plan item can close with the 1.14x figure instead of "kernel is 23x, wait".

---

## Original report (verified against `arrow-cr` `744f9ae`)

Unlike most of this port, there is no upstream `arrow-rs` function to be faithful
to: `arrow-rs` 60.0.0 has no `is_in` kernel (`arrow-ord/src/comparison.rs`,
`arrow-array/src/array/mod.rs` and `arrow-arith/src/numeric.rs` all lack it), so
this is `arrow-cr`'s own addition (its comment says the name follows Red Arrow's
array API). The algorithm was therefore a free choice, and the then-current one
cost 23x what the obvious one did for string keys.

## Failure (historical)

`ArrayBase#is_in` (`src/arrow/collection_sets.cr:31`) sorted the set's indices with
`make_comparator` and then binary-searched per row:

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

That is `O(n log m)` comparator calls, and each call for a string key walked the
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

The same probe against a `Set(String)` in Crystal was **29.6 ms** for the same
1,000,000 rows and 12,000 members — 23x faster.

Measured through `Prism` (`filter(col("title").is_in(titles))`, which is this
kernel plus the mask): 682.0 ms for 1e6 rows.

## Expected vs actual (before the fix)

| Path | Work | Measured (M4) |
| --- | --- | --- |
| `ArrayBase#is_in`, 12k string set, 1e6 rows | `n log m` comparator calls | 673.2 ms |
| `Set(String)#includes?`, same input | `n` hashes | 29.6 ms |
| `Prism.filter(is_in)` end-to-end | kernel + mask | 682.0 ms |

For a 9.2 GB scan the difference was seconds against minutes, so a consumer that
already held a `Set` could not switch to this kernel without a regression, even
though the kernel is what makes the predicate expressible (and pushable) at all.

## Suggested fix direction (implemented as described)

1. Build a `Set` of the set's values for the dtypes where a value probe is exact
   and cheap (strings, binary, primitives, booleans), and probe per row — `O(n)`,
   no comparator. Done, with float keys taken from the bit pattern.
2. Keep the comparator path for everything else (nested, decimal, interval,
   dictionary-of-nested), and for `skip_nulls: true`'s null rule. Done for
   nested/dictionary/run-end/interval/null; decimal turned out to be exactly a
   hash-key equality (`Int128`/`I256`), so it uses the fast path.
3. Nulls: the old rule was "null matches null unless `skip_nulls`". A hash path
   preserves that (null probed against a set-null flag), and `Prism` relies on it
   for `nulls_equal: true`. Done.
4. A row-encoded (`arrow-row`) set would also be `O(n)` but allocates per row;
   not needed — the byte-slice key avoided the per-row `String` copy that a
   row-encoding would have paid for anyway.

## Acceptance

- `is_in` returns exactly what the comparator path returns, including nulls,
  `skip_nulls`, NaN payloads and signed zero (floats use total order today).
  Met: `spec/arrow/is_in_hash_spec.cr` is the differential oracle.
- The string-key benchmark lands within ~2x of a Crystal `Set` probe rather than
  23x. Met: 1.14x.
- Every existing `is_in` test still passes unchanged. Met:
  `spec/arrow/crystal_sets_spec.cr` untouched and green.
