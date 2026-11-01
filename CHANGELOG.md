# Changelog

All notable changes to this project are documented here. This project adheres
to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Phase 5 joins: `DataFrame#join` with `:inner`, `:left`, `:right`, `:full`
  (`:outer`), `:semi`, and `:anti`, on one or several keys via `on:` or
  `left_on:`/`right_on:`, plus `suffix`, `nulls_equal`, and `coalesce` options.
- `Prism.concat` for vertical stacking and `DataFrame#hstack` for horizontal.
- Phase 4 `group_by` + aggregations: `GroupBy#aggregate` (named arguments or a
  Hash) and `GroupBy#agg` (aliased expressions) over one or more keys, with
  `sum`, `mean`, `min`, `max`, `count`, `count_all`, `n_unique`, `first`,
  `last`, `std`, `variance`, and `median`.
- Grouping is sort-based on Polars' contiguous-slice strategy: groups are
  `RecordBatch#slice` views, one sort serves every aggregation in a call, and
  `null_keys` (`:first`/`:last`/`:drop`) plus `maintain_order` make the result
  order explicit.
- Expression execution optimizations mirroring Polars: `Prism::TypeCache`
  resolves dtypes and coercion targets once per execution instead of once per
  chunk, and `Prism::Cse` memoizes repeated subexpressions per chunk.
- `DataFrame#filter` fast paths: an all-true predicate returns the batch
  untouched, an all-false predicate returns an empty view without running the
  kernel, and boolean identities (`x & false`, `x | true`) short-circuit to a
  constant bitmap.
- Phase 3 eager verbs on `Prism::DataFrame`: `select` (names, projections,
  aliased expressions), `filter`, `with_column`/`with_columns`, `drop`,
  `rename`, `cast`, `sort_by` (multi-key with `asc`/`desc` and null placement),
  `distinct`, `drop_nulls`, `fill_null`, and `with_row_index`.
- Chunk-preserving execution for element-wise verbs, with specs asserting chunk
  counts; `sort_by` and `distinct` collapse to a single batch by design.
- Phase 2 expression engine: `Prism::Expr` AST (`Col`, `Lit`, `Binary`, `Neg`,
  `Not`, `IsNull`, `IsNotNull`, `Cast`, `Alias`, `FillNull`, `Coalesce`),
  `Prism.col`/`Prism.lit` plus `Prism::DSL`, operator overloads, and the
  precedence-safe `and`/`or`/`not` methods.
- `Prism::Coerce`: dtype unification and literal-first casting, matching Polars'
  rule of casting the literal instead of the column.
- Kleene boolean logic for `and`/`or`, and null handling via `is_null`,
  `is_not_null`, `fill_null`, and `coalesce`.
- Phase 1 foundation: `Prism::DataFrame` (chunked Arrow batches, `from_arrow`,
  `from_batches`, `from_columns`, `[]`/`[]?`, `to_arrow`, `each_batch`, `head`,
  `tail`, `slice`, `shape`, `dtypes`), `Prism::Series` (chunked column with
  null-aware access and aggregates), and `Prism::Values` (the single
  dtype-dispatch point for materializing Arrow values).
- `Prism::Error` hierarchy: `ColumnNotFound`, `DuplicateColumn`, `SchemaError`,
  `DtypeError`, `UnsupportedOperation`.
- Zero-copy policy (plan §14): data stays in Arrow buffers until an edge;
  aggregates reduce per chunk instead of consolidating columns.
- Project scaffolding: `CLAUDE.md`/`AGENTS.md`, `docs/`, `Makefile`,
  `.ameba.yml`, `.rumdl.toml`, and the implementation plan in `plans/plan.md`.
- Dependencies on the sibling `arrow-cr` and `parquet-cr` shards, wired as path
  dependencies for local development.
- Verified Arrow/Parquet capability constraints (chunked table model,
  free-function kernels, dtype coercion, projection and predicate pushdown) and
  recorded them as design decisions in the plan.
- Recorded the design influences for the hybrid architecture (Polars, Kotlin
  DataFrame, RedAmber, Arquero, Explorer) in `CLAUDE.md` and plan §4.1.

### Changed

- `n_unique` no longer uses `ArrayBase#uniq` (a Crystal `DynComparator` sort that
  allocates an index per row). It now counts distinct values with the typed
  `sort_to_indices` + `Arrow.partition` path, the same machinery behind
  `DataFrame#distinct`.
- Added `Series#unique` and `Series#n_unique`, sharing that path.

### Fixed

- Literal-first casting is now lossless: a float literal against an integer
  column widens the column (`Int64 * 1.5` was truncating to `Int64` and losing
  the fractional part), and a `Float64` literal no longer narrows into a
  `Float32` column.

### Changed

- The library module is `Prism`, the shard is `prism`, and the entrypoint is
  `src/prism.cr`. The repository folder and GitHub repository are still named
  `dataframe` until the remaining rename work lands.
