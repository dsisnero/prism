# Changelog

All notable changes to this project are documented here. This project adheres
to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

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

- The library module is `Prism`, the shard is `prism`, and the entrypoint is
  `src/prism.cr`. The repository folder and GitHub repository are still named
  `dataframe` until the remaining rename work lands.
