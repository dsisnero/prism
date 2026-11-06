# `arrow-cr`: `can_rank?` says yes to decimals that `rank` cannot rank

## Status: fixed upstream (2026-11-06)

Fixed in `arrow-cr` commit `c1c5db8` ("port: rank the Decimal128/256 storage
like upstream"), merged to `main`. Consumers see it after
`shards update arrow-cr` (the pin here was `ca106f2`). `shard_issues/arrow-cr.4.md`
is the other open report.

The symptom was real and is gone: `Arrow.can_rank?` still answers `true` for
every decimal width, and `Arrow.rank` now ranks all of them. The report's
premise was wrong in one place, though — upstream does **not** exclude decimals
from `can_rank`: `can_rank` uses `DataType::is_primitive` (which includes
`Decimal32/64/128/256`) and `downcast_primitive_array!` downcasts all four
decimal widths, so upstream `rank` orders the **physical integer** (the scale is
ignored). The port's `Decimal32`/`Decimal64` arms already matched; only
`Decimal128`/`Decimal256` raised because their `Int128`/`I256` storage had no
`rank` branch.

Resolution: option 2 in the report (rank by physical value), not option 1.
`src/arrow/rank.cr` now dispatches `PrimitiveArray(Int128)` and
`PrimitiveArray(I256)`, and `spec/arrow/rank_spec.cr` pins the scale-blind order
for all four widths plus `can_rank?`.

The Prism guard (`Arrow.can_rank?(dtype) && !dtype.decimal?`) is now only a
Polars-semantics choice, not a workaround for a kernel bug: if `col("d").rank`
should rank decimal columns, drop the `!dtype.decimal?` clause and update the
spec that pins the `DtypeError`.

The rest of the report (the analysis below) is kept for context but its
"Expected"/"Why" sections describe the pre-fix state.

## Failure

`Arrow.can_rank?` answers true for `Decimal128` (and presumably `Decimal256`),
but `Arrow.rank` has no branch for either and raises, so the guard and the
kernel disagree:

```crystal
decimal = Arrow::PrimitiveArray(Int128)
  .from_values([123_i128, -456_i128])
  .with_data_type(Arrow::DataType.decimal128(10, 2))

Arrow.can_rank?(decimal.data_type)  # => true
Arrow.rank(decimal)                 # => Arrow::ArrowError:
                                    #    Compute error: Decimal128(10, 2) not supported in rank
```

## Why

`rank.cr`:

- `can_rank?` returns `true if data_type.primitive?`, and a decimal's storage
  (`PrimitiveArray(Int32/Int64/Int128)`) is primitive, so the decimal dtypes pass
  the guard.
- `rank` dispatches on the **array class**: `PrimitiveArray(Int8)` …
  `PrimitiveArray(UInt64)`, `Float16/32/64`, `BooleanArray`,
  `GenericStringArray`, `GenericBinaryArray`, `StringViewArray`,
  `BinaryViewArray`. `PrimitiveArray(Int128)` reaches the `else` branch and
  raises.

So the pair disagrees for `Decimal32`/`Decimal64` (whose storage *does* match an
allowed class and therefore ranks as the raw integer, ignoring the scale — a
second, quieter problem) and `Decimal128`/`Decimal256` (which raise).

## Expected

Either

1. `can_rank?` returns false for every decimal — a decimal's order is
   scale-aware, and ranking the storage integer is wrong for
   `Decimal128(10, 2)` versus `Decimal128(10, 3)` alike — so callers get a
   consistent "not rankable"; or
2. `rank` grows a decimal branch that ranks by value (the `num_cast`/decimal
   comparison path already exists), and `can_rank?` stays true.

Option 1 is the smaller, safer change and matches `arrow-rs`'s `can_rank`, which
excludes the decimal types from its primitive list. Either way the two functions
should answer the same question the same way.

## Downstream

`Prism::Expr#rank` is a public expression (`col("x").rank`) added in Phase 37; it
guards with `Arrow.can_rank?(dtype) && !dtype.decimal?` until this is fixed, and
its spec pins the Prism-level `DtypeError` for a decimal column.
