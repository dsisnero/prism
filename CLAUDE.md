# Prism

> **How to resume:** read [`plans/plan.md`](plans/plan.md) and use its **Where
> we are** block (current focus + ordered "Next up"), then run `make lint` and
> `make test`. Details: [docs/resuming.md](docs/resuming.md). Do not re-derive
> state from git history.

`Prism` (v0.1.0) is a Crystal DataFrame library built on
[`dsisnero/arrow-cr`](https://github.com/dsisnero/arrow-cr) (Apache Arrow) and
[`dsisnero/parquet-cr`](https://github.com/dsisnero/parquet-cr) (Apache Parquet). Tables are chunked Arrow record
batches; transformations are inspectable expressions; Parquet reads push
projections and predicates down into the reader.

The shard is `prism` (`shard.yml`), the entrypoint is `src/prism.cr`, and
consumers `require "prism"`. The GitHub repository is
[`dsisnero/prism`](https://github.com/dsisnero/prism); dependencies come from
GitHub, not sibling checkouts (see `docs/development.md`). A local checkout folder
may still be named `dataframe/` — cosmetic, referenced by nothing.

## Commands

```bash
make install   # shards install
make update    # shards update
make format    # crystal tool format src spec bench examples
make lint      # ameba src spec bench examples
make test      # crystal spec
make examples  # run examples/quickstart.cr and examples/parquet.cr
make bench     # crystal run --release bench/main.cr
make docs      # crystal docs -o api
make clean     # remove temp/, api/, .crystal-cache
```

`make` targets strip macOS `._*` AppleDouble sidecars first; on this exFAT
volume they can otherwise be mistaken for `*_spec.cr` files. See
[docs/development.md](docs/development.md#environment-notes).

## Docs

- [Resuming work](docs/resuming.md)
- [Architecture](docs/architecture.md)
- [Development](docs/development.md)
- [Coding Guidelines](docs/coding-guidelines.md)
- [Testing](docs/testing.md)
- [Benchmarks](docs/benchmarks.md)
- [PR Workflow](docs/pr-workflow.md)
- [Implementation plan](plans/plan.md)

## Principles

1. **`arrow-cr` and `parquet-cr` own Arrow and Parquet semantics.** Reuse
   `Arrow::Schema`, `Arrow::RecordBatch`, `Arrow::DataType` directly; never
   define a parallel schema dialect.
2. **Never patch `lib/`.** If a dependency is wrong or missing, reproduce it,
   report it upstream, and record the workaround in `plans/plan.md`.
3. **No monkey-patching `Arrow` or `Parquet`.** All wrapping lives in `Prism`.
4. **Keep data in Arrow buffers; materialize only at edges.** Verbs reduce
   chunk-by-chunk and never call `to_arrow`; only `to_a`, `to_arrow`, `each`,
   display, and writers materialize. See
   [plan §14](plans/plan.md#14-zero-copy-policy-and-performance).
5. **Expressions are values.** No side effects, no callbacks deferred to
   execution; everything must be inspectable and typeable before data is read.
6. **Fail with context.** Errors name the offending column.

## Design influences

Crystal forces a hybrid: Ruby-like block syntax, static types, native code, and
a real macro system. No single library is the model — borrow one axis from
each. DeepWiki pages for the indexed ones:

| Library | What we borrow | Reference |
| --- | --- | --- |
| [Polars](https://deepwiki.com/pola-rs/polars) (Rust) | Expression engine and lazy plan: projection pruning, predicate pushdown, slice pushdown, sort collapse | [DeepWiki](https://deepwiki.com/pola-rs/polars) |
| [Kotlin DataFrame](https://deepwiki.com/Kotlin/dataframe) (Kotlin) | Compile-time schemas and typed column accessors (`df.col.price`) | [DeepWiki](https://deepwiki.com/Kotlin/dataframe) |
| [RedAmber](https://github.com/heronshoes/red_amber) (Ruby) | Block-first, immutable verbs on Arrow columnar data | GitHub (not indexed on DeepWiki) |
| [Arquero](https://deepwiki.com/uwdata/arquero) (JavaScript) | Minimal relational verb vocabulary: `derive`, `filter`, `groupby`, `rollup`, `join`, `orderby` | [DeepWiki](https://deepwiki.com/uwdata/arquero) |
| [Explorer](https://deepwiki.com/elixir-explorer/explorer) (Elixir) | dplyr-style verbs over a lazy Polars plan — proof the API and engine layers stay separable | [DeepWiki](https://deepwiki.com/elixir-explorer/explorer) |

The working split: **API** = RedAmber/dplyr surface, **execution** = Polars-style
plan, **macros** = Kotlin-style typed accessors. The concrete mapping, including
what we deliberately do *not* copy, is in
[`plans/plan.md` §4.1](plans/plan.md#41-design-influences). Consult these when
designing a new verb or optimizer pass, then record the decision in the plan.

## Session continuity (read this first)

[`plans/plan.md`](plans/plan.md) is the single source of truth for where we are.
At the start of every session:

1. Read `plans/plan.md` and use its **Where we are** block to find the current
   focus and the ordered "Next up" list. Do not re-derive state from git
   history.
2. Skim `git log --oneline -10` and `git status` to confirm the working tree.
3. Run `make lint` and `make test` to confirm a clean baseline.

See [docs/resuming.md](docs/resuming.md) for the full checklist.

### Keeping the plan resumable

- The plan starts with a **Where we are** block (`Current focus`, `Next up`,
  per-phase status). Keep it accurate after every feature.
- Work items are feature-sized (roughly one branch/commit). Per-file detail
  lives in the phase checklists, not in the status block.
- If the plan outgrows one screen, split phase detail into `plans/<phase>.md`
  files, link each from `plans/plan.md`, and mark each complete/in-progress.

### Definition of done for every feature (required)

A feature is complete only when all of the following are true, and all of it
must be committed as **one** atomic commit before starting the next feature:

1. the public API is implemented and documented in code comments;
2. specs cover the happy path, null handling, dtype behaviour, and error paths;
3. gates pass: `make lint` and `make test`;
4. the phase checklist and the **Where we are** block in `plans/plan.md` are
   updated;
5. any intentional divergence from Arrow/Parquet semantics is documented;
6. nothing under `lib/` was modified.

### Commit message

Start with `feat:` (or `fix:` / `docs:` / `chore:` / `perf:` / `test:`) and a
one-line description, then bullets naming the phases, files, semantics
decisions, and gate results, so a new session can resume from the log alone.

```text
feat: group_by with multi-key aggregation

- add Prism::GroupBy and Prism::Agg (Phase 4)
- group via lexsort -> take -> partition; null keys first
- gates: make lint + make test green
```
