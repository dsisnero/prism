# Development

## Checkout layout

`Prism` depends on two sibling checkouts. All three live next to each other:

```text
repos/github.com/dsisnero/
├── arrow-cr/      # Apache Arrow for Crystal
├── parquet-cr/    # Apache Parquet for Crystal (depends on ../arrow-cr)
└── dataframe/     # this repo (shard "prism"; folder rename pending)
```

The shard is `prism` and the entrypoint is `src/prism.cr`, so consumers write
`require "prism"`. The repository folder and the GitHub repository are still
named `dataframe`; renaming those is the remaining Phase 10 item.

`shards install` symlinks the dependencies into `lib/`:

```text
lib/arrow-cr   -> ../../arrow-cr
lib/parquet-cr -> ../../parquet-cr
```

Because they are symlinks, editing a dependency's source is immediately visible
here. That is convenient for *reading*, and forbidden for *patching*:
see [Coding Guidelines](coding-guidelines.md#never-patch-lib).

## Dependencies

The published `shard.yml` depends on GitHub, so `shards install` works for
everyone:

```yaml
dependencies:
  prism:
    github: dsisnero/prism
# resolves transitively:
#   arrow-cr:    github: dsisnero/arrow-cr
#   parquet-cr:  github: dsisnero/parquet-cr
```

To hack on `arrow-cr` or `parquet-cr` themselves, point the two dependencies at
your sibling checkouts instead (`path: ../arrow-cr`, `path: ../parquet-cr`);
`lib/` is gitignored either way, and `shard.lock` is too, so the swap never
lands in a commit. Note that shards refuses one shard name from two sources, so
`parquet-cr`'s own `arrow-cr` dependency must point at the same place.

`development_dependencies` add three things that are not needed to *use* Prism:
`ameba`, `lipgloss` (for `require "prism/lipgloss"` styled tables) and
`bubbletea` + `bubbles` (for `examples/viewer.cr`). `lipgloss` comes from GitHub
rather than a sibling checkout because `bubbletea` depends on it and shards
refuses two sources for one name.

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

`make bench` compiles the same dependency graph in `--release` (the first build
can take a while — the whole surface is optimized at O3, and considerably longer
while the machine is busy) and prints the 1e6-row table; the scenarios and
current numbers are in [benchmarks.md](benchmarks.md). Its fixtures are generated
into `temp/bench/`, so nothing is committed.

## Repository layout

```text
src/prism.cr            # entrypoint: requires everything, Prism::VERSION
src/prism/              # all library code, one directory per concern
spec/                   # mirrors src/prism/
examples/               # runnable README workflows (make examples)
bench/                  # benchmarks (make bench); see docs/benchmarks.md
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
4. Write the report to `shard_issues/<shard>.<n>.md` — a self-contained failure
   description with a runnable reproducer, expected vs actual, and the analysis.
   Numbers are per shard and continue across repos (for example
   `parquet-cr.1` lives in `amazon_data`, so this repo starts at
   `parquet-cr.2`).
5. Record the workaround and a link to that file in `plans/plan.md`.

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

Not yet. Phase 9 produces the v0.1.0 tag; the remaining Phase 10 work handles
the repository rename and publishing. `CHANGELOG.md` is updated per release, not
per commit.

## The repository rename (Phase 10, owner-gated)

Done: the shard is `prism` and the entrypoint is `src/prism.cr`.

Remaining, all outside this repository or requiring owner action:

- [ ] Rename the repo folder `dataframe/` → `prism/` and the GitHub repository
- [ ] Update sibling checkouts that reference the old path
- [ ] Switch `shard.yml` dependencies from `path:` to `github:` on publish
- [ ] Update the README install block to the published `github:` form

Do not start the folder/GitHub rename until the owner asks.
