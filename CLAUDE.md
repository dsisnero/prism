# Prism

> **How to resume:** read [`plans/plan.md`](plans/plan.md) and use its **Where
> we are** block (current focus + ordered "Next up"), then run `make lint` and
> `make test`. Details: [docs/resuming.md](docs/resuming.md). Do not re-derive
> state from git history.

`Prism` is a Crystal DataFrame library built on
[`dsisnero/arrow-cr`](https://github.com/dsisnero/arrow-cr) (Apache Arrow) and
`dsisnero/parquet-cr` (Apache Parquet). Tables are chunked Arrow record
batches; transformations are inspectable expressions; Parquet reads push
projections and predicates down into the reader.

The shard is still named `dataframe` and the entrypoint is still
`src/dataframe.cr` so that `require "dataframe"` resolves. The module is already
`Prism`. The rename is tracked as Phase 10 in the plan — do not rename anything
piecemeal.

## Commands

```bash
make install   # shards install
make update    # shards update
make format    # crystal tool format src spec
make lint      # ameba src spec
make test      # crystal spec
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
- [PR Workflow](docs/pr-workflow.md)
- [Implementation plan](plans/plan.md)

## Principles

1. **`arrow-cr` and `parquet-cr` own Arrow and Parquet semantics.** Reuse
   `Arrow::Schema`, `Arrow::RecordBatch`, `Arrow::DataType` directly; never
   define a parallel schema dialect.
2. **Never patch `lib/`.** If a dependency is wrong or missing, reproduce it,
   report it upstream, and record the workaround in `plans/plan.md`.
3. **No monkey-patching `Arrow` or `Parquet`.** All wrapping lives in `Prism`.
4. **Element-wise operations preserve chunking.** Do not concatenate batches
   unless the operation genuinely reorders rows.
5. **Expressions are values.** No side effects, no callbacks deferred to
   execution; everything must be inspectable and typeable before data is read.
6. **Fail with context.** Errors name the offending column.

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
