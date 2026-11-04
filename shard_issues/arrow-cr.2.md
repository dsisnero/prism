# `arrow-cr`: a single-column `lexsort_to_indices` raises for struct/map keys

## Status: fixed upstream, consumer simplified (2026-11-03)

Fixed in `arrow-cr` commit `3023eac` ("port: gate the single-column lexsort fast
path with `can_sort_to_indices`"), merged to `main`. `Arrow.can_sort_to_indices?`
now exists (`sort.cr`, with the `can_rank?` recursion in `rank.cr`),
`lexsort_to_indices` uses its single-column fast path only for the dtypes
`sort_to_indices` can express, and the dispatch behind it grew the decimal and
interval primitives. Prism was updated to that commit and dropped its workaround;
no `lib/` changes were ever made.

Re-verified from Prism against `arrow-cr` `3023eac` (Apple M4, Crystal 1.22.2):

```text
struct:   can_sort_to_indices?=false
  sort_to_indices   raises (as upstream)
  lexsort single    [1, 3, 0, 2]     ascending x, ties in input order
  lexsort limit 2   [1, 3]           bounded selection
decimal:  can_sort_to_indices?=true
  sort_to_indices   [2, 1, 3, 0]     nulls first under default SortOptions
  lexsort single    [2, 1, 3, 0]
interval: can_sort_to_indices?=true
  sort_to_indices   [2, 1, 3, 0]
```

The bounded case is worth keeping: a single struct key over 1e6 rows costs
405.1 ms for the full order and 18.5 ms for `limit: 10`, so `top_n(10)` on such
a key now selects instead of sorting everything (Prism's former workaround
sorted in full and then truncated).

Originally verified against `arrow-cr` `728ce51` and upstream `arrow-rs`
`60.0.0`, which the port pins.

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

Prism carried a temporary workaround between Phase 23 and the fix:
`Prism::SortEngine.comparator_sort` compared the index array directly and
`per_key_sortable?` mirrored the old dispatch. Both are now deleted —
`SortEngine` defers a single column to `Arrow.lexsort_to_indices` (which reaches
the chain through `can_sort_to_indices?`) and asks `Arrow.can_sort_to_indices?`
when choosing the multi-key shape, so there is no parallel dtype dialect left in
Prism.

The specs written for the workaround stay: they assert Order/`limit` behaviour
through `Prism::SortEngine` and `DataFrame#sort_by`/`top_n`/`group_by`, and now
pin the dependency's comparator path instead of Prism's copy.

## What landed upstream

1. `can_sort_to_indices?` ported, with `can_rank?` handling the primitive,
   boolean, byte-array and byte-view families and the list/dictionary/run-end
   recursion.
2. The single-column fast path in `lexsort_to_indices` gated on it, so the
   comparator path below is reachable for one column as well.
3. The dispatch extended with the decimal and interval primitives, so
   `can_sort_to_indices?` and what `sort_to_indices` can actually sort agree.

## Acceptance

- `lexsort_to_indices([struct])` and `[map]` return the same order as the
  two-column chain applied with a constant second key.
- `lexsort_to_indices([decimal])` / `[interval]` sort, and match the ascending
  order of the equivalent `Int128`/`Int32` primitives.
- Every existing sort test still passes; multi-column output is unchanged
  (byte-identical, equal keys in ascending index order).
