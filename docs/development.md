# Development

## Checkout layout

`Prism` depends on two sibling checkouts. All three live next to each other:

```text
repos/github.com/dsisnero/
├── arrow-cr/      # Apache Arrow for Crystal
├── parquet-cr/    # Apache Parquet for Crystal (depends on ../arrow-cr)
└── dataframe/     # this repo (shard name "dataframe", module Prism)
```

`shards install` symlinks the dependencies into `lib/`:

```text
lib/arrow-cr   -> ../../arrow-cr
lib/parquet-cr -> ../../parquet-cr
```

Because they are symlinks, editing a dependency's source is immediately visible
here. That is convenient for *reading*, and forbidden for *patching*:
see [Coding Guidelines](coding-guidelines.md#never-patch-lib).

## Dependencies

`shard.yml` uses path dependencies so three in-progress shards can move
together:

```yaml
dependencies:
  arrow-cr:
    path: ../arrow-cr
  parquet-cr:
    path: ../parquet-cr
```

For a consumer (or after publishing), swap to the git form:

```yaml
dependencies:
  dataframe:
    github: dsisnero/dataframe
```

A published `${name}` shard will not resolve `path: ../arrow-cr` on a consumer's
machine, so the published `shard.yml` must use `github:` entries. This is a
Phase 10 item alongside the shard rename.

## Prerequisites

- Crystal **1.22.2** or newer (`shard.yml` pins `>= 1.22.2`)
- `shards`
- `ameba` for lint, `rumdl` for Markdown lint (optional but used in review)
- A C toolchain for `snappy`, `brotli`, `zinc`, `flint` (Parquet compression)

## First run

```bash
make install   # shards install
make test      # crystal spec
```

The first compile is slow: `arrow-cr` is ~42k lines and `parquet-cr` ~52k, and
Crystal compiles both into every spec binary. A warm run of the full suite is
typically 20–60 s including the dependency compile. Iterating on one file with
`crystal spec spec/prism/expr_spec.cr` does not avoid the dependency compile;
budget for it.

## Repository layout

```text
src/dataframe.cr        # entrypoint (renamed in Phase 10)
src/prism/              # all library code, one directory per concern
spec/                   # mirrors src/prism/
plans/plan.md           # implementation plan — the resume source of truth
docs/                   # operator docs, linked from README
temp/                   # gitignored scratch; generated fixtures live here
Makefile                # the only supported command surface
```

`plans/inventory/`, `vendor/`, and upstream submodules do not exist here — this
shard wraps two ports rather than porting upstream source itself.

## Adding a verb

1. Decide whether it is **element-wise** (maps over chunks) or **ordering**
   (must collapse to one batch). That choice dictates the implementation shape;
   see [architecture.md](architecture.md#chunking).
2. Validate column names first and raise `Prism::ColumnNotFound` with the name.
3. Spec it with: values, resulting schema, null handling, and a chunk-count
   assertion for element-wise verbs.
4. Add the method to the API table in `plans/plan.md` if it is user-facing.

## Adding an aggregate

1. Add the method to `Expr` returning an `Agg` node.
2. Add a `dtype` rule (this is where `mean`'s integer promotion lives).
3. Add the executor case, including the all-null and empty-group behaviour.
4. Assert that grouped results equal a naive per-key reference loop.

## Working with the dependencies

Read their sources directly under `lib/`:

- `lib/arrow-cr/src/arrow/record_batch.cr`, `filter.cr`, `take.cr`, `sort.cr`,
  `partition.cr`, `numeric.cr`, `cmp.cr`, `aggregate.cr`
- `lib/parquet-cr/src/parquet/arrow/arrow_reader.cr`,
  `arrow_writer.cr`, `mod.cr` (projection), `filter.cr`

Do not rely on memory for signatures. The real ones differ from the obvious
guess in several places worth re-checking every time:

- `Arrow` kernels are module functions (`Arrow.filter_record_batch`), not batch
  methods.
- `RecordBatch` exposes `num_rows` **and** `row_count`; `column_by_name` returns
  `ArrayBase?`.
- Verify the dependency API before designing around it. Two accessors that look
  interchangeable are not: `value?` is bounds-only, `[]?` is null-aware.
- `Parquet::Arrow::ArrowWriter#write` — *not* `write_batch`.
- `ArrowWriter` takes an `IO`, never a path; the reader builder takes a
  `File::FileReader`, never a path.
- `Parquet::Compression` is a struct: `Parquet::Compression::SNAPPY`,
  `Parquet::Compression.zstd(3)`.
- `ArrayBase` has no `to_a`; typed leaf arrays do. Use `Prism::Values`.

## Reporting an upstream problem

If a kernel is missing or returns a wrong result:

1. Reproduce it in `temp/repro_<topic>.cr` with the smallest possible input.
2. Check whether the same input works in `arrow-rs` semantics (the port is
   behaviour-faithful, so a mismatch is a dependency bug).
3. File it against `dsisnero/arrow-cr` or `dsisnero/parquet-cr`.
4. Record the workaround and the issue link in `plans/plan.md` §15.

Never edit `lib/*/src/` and commit — the symlink means the change lands in the
dependency's working tree, not this repo, and it will silently disappear.

## Environment notes

This checkout is on an exFAT volume, so macOS writes AppleDouble `._*` sidecars.
Two symptoms:

- A `._foo_spec.cr` sidecar matches `spec/**/*_spec.cr` and breaks compilation.
  The `Makefile` deletes sidecars before `format`, `lint`, and `test`.
- Git may print `non-monotonic index .git/objects/pack/._pack-*.idx`. Fix with
  `find .git -name '._*' -delete`.

## Releasing

Not yet. Phase 9 produces the v0.1.0 tag; Phase 10 handles the shard rename and
publishing. `CHANGELOG.md` is updated per release, not per commit.

## The shard rename (Phase 10, owner-gated)

The module is `Prism` today, but the shard is `dataframe` and the entrypoint is
`src/dataframe.cr` so `require "dataframe"` resolves. Renaming is a single
coordinated change, because a half-rename breaks `require`:

- [ ] `shard.yml`: `name: prism`
- [ ] `src/dataframe.cr` → `src/prism.cr`
- [ ] `spec/spec_helper.cr` and any internal `require "../src/dataframe"`
- [ ] README/`CLAUDE.md`/`docs/` `require "prism"`
- [ ] `.ameba.yml`, `Makefile` references if any
- [ ] Repo folder and GitHub repo rename
- [ ] `spec/` filename `dataframe_spec.cr` → `prism_spec.cr`

Do not start it until the owner asks.
