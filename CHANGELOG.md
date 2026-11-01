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

### Changed

- The library module is now `Prism`. The shard name and entrypoint
  (`src/dataframe.cr`) are unchanged until the rename phase.
