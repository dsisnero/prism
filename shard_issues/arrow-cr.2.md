# `arrow-cr`: a single-column `lexsort_to_indices` raises for struct/map keys

## Status: open, worked around in Prism (2026-11-03)

Verified against `arrow-cr` `728ce51` (what `shard.lock` resolves) and against
upstream `arrow-rs` `60.0.0`, which the port pins.

Upstream `arrow-ord/src/sort.rs` guards the single-column fast path:

```rust
if columns.len() == 1 && can_sort_to_indices(columns[0].values.data_type()) {
    // fallback to non-lexical sort
    let column = &columns[0];
    return sort_to_indices(&column.values, column.options, limit);
}
```

so a single column whose dtype `sort_to_indices` cannot express falls through to
the lexicographic comparator path below it (`LexicographicalComparator` over
`make_comparator`), and sorts fine. The port calls `sort_to_indices` for every
single-column input, so the same call raises.

## Failure

```text
Arrow.lexsort_to_indices([Struct("x": Int64)])  -> ArrowError: Compute error:
                                                   Sort not supported for data type
                                                   Struct("x": Int64)
Arrow.lexsort_to_indices([Struct, Int64])       -> ok, order=[1, 0]
Arrow.sort_to_indices(Struct)                   -> ArrowError (same message)
```

`lib/arrow-cr/src/arrow/sort.cr` (`lexsort_to_indices`):

```crystal
# line 470 — no `can_sort_to_indices` guard around this
return sort_to_indices(
  columns[0].values, columns[0].options, limit,
) if columns.size == 1
```

The guard is unconditional, and `sorted_valid_indices` (line 166) dispatches on
the array class with no struct/map/decimal/interval/union arm, so the `else`
raises. The comparator chain in the multi-column branch of the same function
handles those dtypes, which is why two columns work and one does not.

## Reproduction

```crystal
require "arrow-cr"

structs = Arrow::StructArray.from_pairs([
  {Arrow::Field.new("x", Arrow::DataType.int64, true),
   Arrow::Int64Array.from_values([2_i64, 1_i64]).as(Arrow::ArrayBase)},
]).as(Arrow::ArrayBase)

Arrow.lexsort_to_indices([Arrow::SortColumn.new(structs)])
```

Observed: `Compute error: Sort not supported for data type Struct("x": Int64)`.
Expected (upstream): indices `[1, 0]`.

## Expected vs actual

| Call | Upstream `arrow-rs` 60.0.0 | `arrow-cr` 728ce51 |
| --- | --- | --- |
| `lexsort_to_indices([struct])` | comparator path, sorts | raises |
| `lexsort_to_indices([struct, int])` | chain, sorts | chain, sorts |
| `sort_to_indices(struct)` | raises | raises |
| `lexsort_to_indices([decimal])` | `sort_to_indices` (primitive) | raises |
| `lexsort_to_indices([interval])` | `sort_to_indices` (primitive) | raises |
| `lexsort_to_indices([run_end])` | `sort_to_indices` (`RunArray` arm) | sorts (`RunArray` arm) |

Two further gaps in the same dispatch, both in upstream's `can_sort_to_indices`
but not in the port's `sorted_valid_indices`:

- decimals and intervals are primitives upstream (`DataType::is_primitive()`), so
  upstream sorts them with `sort_primitive`; the port's `when PrimitiveArray(T)`
  arms enumerate `Int8..Float64` only, so those dtypes fall to the `else`;
- `can_sort_to_indices` recurses into list children through `can_rank`, so a list
  of an unsortable child is correctly refused upstream and simply raises in the
  port.

The port's own header documents the deliberate divergences it knows about (the
heap top-k path and the `nulls_last` bound), not this one.

## Downstream impact (`prism`)

`Prism::GroupBy#execute` sorts its key columns through this API, so
`group_by` on a single struct key raised `Sort not supported`, while
`distinct`/`n_unique` handled the same key through the `arrow-row` encoding, and
a two-key `group_by` with a struct key worked. `DataFrame#sort_by` and `top_n` hit
the same wall for a single struct/map/decimal/interval key.

Workaround (Prism Phase 23): `Prism::SortEngine.comparator_sort` sorts the index
array with `Arrow.make_comparator` and tie-breaks by position whenever
`per_key_sortable?` says the per-key sort cannot express the dtype — the same
fallback upstream reaches through its guard, with the tie-break made explicit
rather than inherited. No `lib/` changes; `Prism::SortEngine.per_key_sortable?`
stays conservative (it mirrors the port's dispatch, not upstream's wider
`can_sort_to_indices`).

## Suggested fix direction (not a patch)

1. Port `can_sort_to_indices` (primitive-or-listed-or-dictionary-or-run-end, with
   the `can_rank` recursion for list children).
2. Gate the single-column fast path in `lexsort_to_indices` on it, so the
   comparator path below is reachable for one column as well.
3. Extend `sorted_valid_indices` (or the `sort_to_indices` dispatch above it) with
   the decimal and interval primitives and the run-end arm, matching upstream;
   then `can_sort_to_indices` and the dispatch agree.

## Acceptance

- `lexsort_to_indices([struct])` and `[map]` return the same order as the
  two-column chain applied with a constant second key.
- `lexsort_to_indices([decimal])` / `[interval]` sort, and match the ascending
  order of the equivalent `Int128`/`Int32` primitives.
- Every existing sort test still passes; multi-column output is unchanged
  (byte-identical, equal keys in ascending index order).
