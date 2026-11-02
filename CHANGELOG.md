# Changelog

All notable changes to this project are documented here. This project adheres to
[Semantic Versioning](https://semver.org/) and the structure of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `Prism::Series` comparison operators: `==`, `!=`, `<`, `<=`, `>`, `>=` against
  another series or a scalar, producing a boolean column. Scalars follow the same
  coercion rule expressions use — the literal adopts the column's dtype when the
  cast is lossless, otherwise both sides widen — so `series > 10` on an `Int32`
  column stays `Int32`, and `series > 1.5` on an integer column widens instead of
  truncating the literal.
- `DataFrame#filter(mask : Series)`: a boolean column can be the filter directly
  (`df.filter(df["price"] > 10)`). Masks derived from the frame's own columns
  keep the chunking; the three fast paths (all true, none true, otherwise) are
  shared with the expression form.
- `DataFrame#remove` (expression, mask, or block), the inverse of `filter`: rows
  are kept where the predicate is false **or** null, which is deliberately not
  `filter(pred.not)` — a null predicate would otherwise drop a row the predicate
  could not judge. `LazyFrame#remove` matches.
- `sort_by("-col")` descending shorthand (RedAmber), resolved by a shared
  `Prism.sort_key` so eager and lazy verbs agree; `nulls:` still applies to the
  prefixed key.
- Range row slicing: `df[1..3]`, `df[1...3]`, `df[..1]`, `df[2..]`, `df[-2..]`,
  plus `df.slice(range)`. Negative endpoints count from the end and bounds clamp,
  so an out-of-range range yields an empty frame rather than `nil`.
- Column indexing: `df[["region", "price"]]`, equivalent to `select`.
- One-call `GroupBy` aggregates: `sum`, `mean`, `min`, `max`, `count`,
  `n_unique`, `median`, `std`, `var`. Output columns keep their input names
  (Polars' rule); with no column names the aggregate covers every eligible
  column and never a grouping key; `count` with no columns is Polars' `count`
  (one `count` column of row counts per group).

### Changed

- `Coerce` exposes `with_literal(column_dtype, literal_dtype)`, the column-vs-
  literal half of `#target`, so `Series` comparisons and expressions cannot drift.
- `Prism::DuplicateColumn` takes an optional reason, used when a one-call
  aggregate names a grouping key.

## [0.1.1] - 2026-11-02

A small follow-up to `0.1.0`: the two type families that were still bottoming
out in "unsupported dtype" now work end to end — decimals in arithmetic, and
nested (`list`/`struct`/`map`) columns as Crystal values.

### Added

- Decimal arithmetic: `+`, `-`, `*`, `/`, `%` and unary `-` on `Decimal32/64/128/256`
  columns now run on `arrow-cr`'s kernels, declaring exactly the dtype those
  kernels return (Hive's rules: `add`/`sub` take `max(s1, s2)` and one extra
  digit, `mul` adds both scales and both precisions plus one, `div` scales to
  `s1 + 4`, `rem` keeps the wider scale). Integer operands are promoted to
  decimal with scale 0, integer literals adopt the column's scale, a float
  operand falls back to `Float64`, and mixed decimal widths are promoted to the
  wider storage kind instead of failing inside the kernel. A product whose scale
  would exceed the kind's maximum raises `Prism::DtypeError` at planning time.
- A decimal column combined with a `Null` column (or `nil` literal) now yields an
  all-null result of the right dtype, for arithmetic and comparisons alike,
  instead of raising `DtypeError`.
- Nested materialization: `List`/`LargeList`/`ListView`/`LargeListView`/
  `FixedSizeList` become `Array(Prism::Value)`, `Struct` becomes a field-ordered
  `Hash(String, Prism::Value)`, and `Map` becomes `Hash(Prism::Value,
  Prism::Value)`, matching how Polars converts them (`to_list` semantics: a null
  list is `nil`, distinct from `[]` and from a list holding a null). `Value` is
  now a recursive alias through those containers, so nested columns also work as
  `distinct`/`n_unique`/`group_by` keys via the existing `arrow-row` encoding,
  and the table renders containers with nested strings quoted.

### Tests

- `spec/prism/decimal_arithmetic_spec.cr` pins every rule above against the
  kernel's own result dtype (the `dtype`/`eval` agreement check), and the coverage
  spec no longer asserts that decimal arithmetic is unimplemented.
- `spec/prism/nested_spec.cr` covers list/fixed-size-list/struct/map extraction,
  nulls at both levels, display, nested keys through `distinct`/`n_unique`, and
  arithmetic rejection; `values_spec` moved its "unsupported dtype" case from
  structs to intervals, which are still unsupported.

## [0.1.0] - 2026-11-01

The first release. Prism is a DataFrame library over Apache Arrow: chunked
record batches, an inspectable expression language, eager verbs, a lazy planner
that pushes work into the Parquet reader, grouped aggregation, joins, and
optional compile-time typed schemas.

### Added

**Tables and columns**

- `Prism::DataFrame` — chunked `Array(Arrow::RecordBatch)` plus one
  `Arrow::Schema`, with `from_arrow`, `from_batches`, `from_columns`, `[]`/`[]?`,
  `shape`, `column_names`, `dtypes`, `to_arrow`, `each_batch`, `head`, `tail`,
  `slice` and `row_count`.
- `Prism::Series` — a chunked column with null-aware `[]`/`[]?`, `to_a`,
  `each_chunk`, `sum`, `mean`, `min`, `max`, `unique`, `n_unique`, `is_null`,
  `fill_null`, `cast`, element-wise `==` and structural `same_as?`.
- `Prism::Values` — the single dtype-dispatch point for materializing Arrow
  values, so a null can never leak out as a zero.
- `Prism::Error` hierarchy — `ColumnNotFound`, `DuplicateColumn`, `SchemaError`,
  `DtypeError`, `UnsupportedOperation`, `MissingAlias`, each naming the column.

**Expressions**

- `Prism::Expr` AST (`Col`, `Lit`, `Binary`, `Neg`, `Not`, `IsNull`,
  `IsNotNull`, `Cast`, `Alias`, `FillNull`, `Coalesce`, aggregations) with
  `key`, `columns`, `same_as?`, and a total `dtype(schema)`.
- `Prism.col`/`Prism.lit` plus `Prism::DSL`; operator overloads and the
  precedence-safe `and`/`or`/`not` methods.
- Numeric coercion with literal-first casting, losslessly: a float literal
  against an integer column widens the column instead of truncating.
- Kleene boolean logic, `fill_null`, `coalesce`, and null-propagating
  comparisons.
- `Prism::TypeCache` and `Prism::Cse` — dtypes and coercion resolve once per
  execution, and repeated subexpressions evaluate once per chunk.

**Eager verbs**

- `select`, `filter`, `with_column`/`with_columns`, `drop`, `rename`, `cast`,
  `sort_by` (multi-key, `asc`/`desc`, null placement), `distinct`, `drop_nulls`,
  `fill_null`, `with_row_index`.
- `filter` fast paths: all-true returns the batch untouched, all-false returns an
  empty view, and `x & false` / `x | true` short-circuit without evaluating `x`.
- `group_by(...).aggregate(...)` and `#agg` over one or more keys with `sum`,
  `mean`, `min`, `max`, `count`, `count_all`, `n_unique`, `first`, `last`, `std`,
  `variance` and `median`, plus `null_keys: :first|:last|:drop` and
  `maintain_order`.
- `join` with `:inner`, `:left`, `:right`, `:full` (`:outer`), `:semi` and
  `:anti`, on one or several keys via `on:` or `left_on:`/`right_on:`, with
  `suffix`, `nulls_equal` and `coalesce`.
- `Prism.concat` (vertical) and `DataFrame#hstack` (horizontal).

**Lazy execution**

- `Prism.scan_parquet(path)` and `DataFrame#lazy` build a `Prism::Plan`;
  `#collect` executes it and `#explain(optimized:)` renders it.
- Plan nodes with schema propagation: `ScanParquet`, `ScanBatches`, `Filter`,
  `Select`, `WithColumn`, `GroupBy`, `Sort`, `Distinct`, `Join`, `Limit`, `Tail`,
  `Empty`.
- Optimizer passes: constant folding, predicate pushdown (conjunct splitting and
  `RowFilter` absorption), projection pruning (through `Select`, `WithColumn`,
  aggregations and joins), dead-column elimination, and limit pushdown.
- A window pinned to `Sort`/`GroupBy`/`Distinct` (`Plan::Slice`), with nested
  limits composing instead of stacking.

**IO**

- `read_parquet` / `write_parquet` with projection pushdown, row-group pruning,
  `batch_size`, `columns`, `predicate`, `limit`/`offset`, compression (`zstd`
  default at level 3), row-group size and statistics.
- `read_csv` / `write_csv` with schema inference or an explicit schema, header
  and delimiter control; `read_ipc` / `write_ipc` in file and stream formats.
- In-memory variants over `IO` for every reader and writer.

**Typed schemas and display**

- `Prism.define_schema` generates an Arrow schema, per-field expression
  accessors and a typed frame (`Name::Frame`) with block verbs, so a misspelt
  field is a compile error; `Name.from_dataframe` is the validated re-entry point.
- `DataFrame#to_s` / `#inspect` render an aligned table with a shape line, dtype
  row, `null` rendering and head/tail truncation, overridable via
  `to_s(max_rows:, max_cols:, max_string:)`.
- Optional styled tables via `require "prism/lipgloss"` (`Prism::Styled`,
  `DataFrame#to_styled`); `to_s`/`inspect` stay ANSI-free.

**Tooling and docs**

- `bench/` — a 1e6-row harness (`make bench`) covering verbs, row-keyed paths
  and Parquet pushdown, with results in `docs/benchmarks.md`.
- `examples/` — runnable README workflows (`make examples`) and an interactive
  `bubbletea` + `bubbles` table viewer, checked by `spec/examples_spec.cr`.
- `spec/prism/property_spec.cr` — seeded oracles comparing every verb against a
  hand-written reference, mutation-checked.
- `docs/` — architecture, benchmarks, testing, development, coding guidelines,
  PR workflow and resuming notes; `plans/plan.md` records every design decision.

### Performance

- `distinct`, `unique` and `n_unique` are O(n): key columns are encoded with
  `arrow-row` (`Prism::RowKeys`) and walked as a byte seen-set, replacing a
  sort-plus-partition (163 → 54 ms for `distinct` on 1e5 keys over 1e6 rows).
- Joins hash the same encoded rows instead of boxed `Array(Value)` keys
  (284 → 219 ms for a 1e6 → 1e5 join).
- `sort_by` + `head(n)` gathers only the window's indices instead of copying
  every row (229 → 130 ms through the planner).
- `n_unique` inside `group_by` counts distinct encodings rather than sorting each
  group.

### Changed

- `n_unique` counts null as one distinct value, matching Polars, so
  `n_unique == unique.size`.
- Float keys are canonicalized before encoding (`-0.0` becomes `0.0`, every NaN
  becomes one quiet NaN, as in Polars' `canonical_f64`), so `distinct`, joins and
  `group_by` agree on what one key is.
- `group_by` sorts *canonical* keys while emitting the original key values.
- `select` keeps a renamed bare column's nullability, so `rename` produces the
  same schema eagerly and through the planner.
- Parquet predicate pushdown is enabled again after `parquet-cr` `5883ff1` fixed
  its row filter shifting null-bearing columns.

### Breaking

- `Prism.scan(frame)` is gone; `DataFrame#lazy` is the only in-memory lazy entry,
  matching Polars' `df.lazy()` / `pl.scan_*` split (`Prism.scan_parquet` reads
  from a file).
- `shard.yml` now depends on `arrow-cr` and `parquet-cr` from GitHub rather than
  sibling `path:` checkouts, so `shards install` works for consumers.
- Decimal arithmetic raises `UnsupportedOperation` with a cast hint: `arrow-cr`'s
  decimal kernels grow precision per the Arrow spec, so a per-operator result
  dtype is needed before Prism can declare one.

### Known limitations

- Grouping is sort-based (`O(n log n)`) on purpose: it yields contiguous group
  views and deterministic order; a hash path is deferred until benchmarks
  justify it.
- Nested (`list`/`struct`) columns do not materialize as Crystal values yet; a
  tagged `Prism::Value` struct is the way in.
- A dictionary-encoded *float* key is compared by value but not canonicalized.
- Row-group pruning only pays off when the predicate correlates with the
  physical layout; on unclustered data a row filter costs more than it saves,
  which `docs/benchmarks.md` documents with numbers.

[0.1.1]: https://github.com/dsisnero/prism/releases/tag/v0.1.1
[0.1.0]: https://github.com/dsisnero/prism/releases/tag/v0.1.0
