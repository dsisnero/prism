# Changelog

All notable changes to this project are documented here. This project adheres
to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

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
