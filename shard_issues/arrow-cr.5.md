# `arrow-cr`: `can_rank?` says yes to decimals that `rank` cannot rank

## Status: open (2026-11-05)

Still present at the current pin `arrow-cr` `ca106f2`: `Arrow.can_rank?` answers
`true` for `Decimal128(10, 2)` and `Arrow.rank` still raises.

Prism works around it: `Prism::KernelArgs.require_rankable` excludes decimals
before asking `Arrow.can_rank?`, so `col("d").rank` raises
`Prism::DtypeError` naming the column instead of failing inside the kernel.
`shard_issues/arrow-cr.4.md` is the other open report.

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
