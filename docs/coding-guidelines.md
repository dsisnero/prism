# Coding Guidelines

## Language and formatting

- Crystal **1.22.2+**. Run `make format` before committing; CI runs
  `crystal tool format --check src spec`.
- Two-space indent, LF line endings, final newline (enforced by
  `.editorconfig`).
- Prefer `snake_case` for methods and variables, `PascalCase` for types,
  `SCREAMING_SNAKE_CASE` for constants.
- Keep files under roughly 400 lines. Split by concern under `src/prism/`.

## Names

- Verbs are imperative and read as sentences: `df.filter`, `df.select`,
  `df.sort_by`, `df.group_by(...).aggregate(...)`.
- Mutating-looking verbs do not mutate: `with_column` returns a new frame. If a
  method mutates, its name must end with `!` and it must be justified in review.
- `DataFrame` is immutable. Treat every verb as returning a new frame that
  shares buffers where Arrow allows it.
- Name the column in every error, e.g.
  `Prism::ColumnNotFound.new("name: no such column (have: a, b, c)")`.

## Types

- Reuse `Arrow::DataType`, `Arrow::Schema`, `Arrow::Field`, `Arrow::RecordBatch`
  directly. Do not introduce parallel wrappers for these.
- Restrict parameters to the narrowest useful type. Prefer
  `Enumerable(String | Symbol)` over `Array(String)` for name lists so callers
  can pass either.
- Expressions are structs over an abstract struct. Do not add mutable state to
  an `Expr`.

## Never patch `lib/`

`lib/arrow-cr` and `lib/parquet-cr` are symlinks to sibling checkouts. Editing
files there:

- does not commit to this repository;
- silently changes the dependency for every project on this machine;
- will be lost on `shards update`.

If a dependency is wrong, reproduce it, file upstream, and record the workaround
in `plans/plan.md` §15.

## Never reopen `Arrow` or `Parquet`

Do not add `module Arrow` blocks in this shard. `arrow-cr` owns that namespace,
and reopening it makes `Prism` only half-requireable (its types would exist only
if the caller also required the right files in the right order). All wrapping
also avoids name collisions when `arrow-cr` adds types.

## Errors

- Raise `Prism::Error` subclasses for dataframe-level misuse: unknown column,
  duplicate name, unsupported dtype combination, bad argument mix.
- Let `Arrow::ArrowError` and `Parquet::ParquetError` propagate when the failure
  is genuinely in that layer. They carry precise context and callers may match
  them.
- Do not rescue and re-raise as a generic `Exception`.
- Every public method that can fail documents which errors it raises.

## Documentation

- Public classes and methods get a comment starting with the thing it does, not
  with the signature repeated. Use `#` doc comments so `make docs` renders them.
- Reference the dependency function being wrapped when it matters, e.g.
  "wraps `Arrow.filter_record_batch`".
- Semantics that differ from Arrow get an explicit note in the doc comment and
  in `plans/plan.md` §8.
- Keep `README.md` about *what the library is and how to use it*; development and
  dependency notes belong in [development.md](development.md).

## Crystal idioms worth knowing here

- `Expr#==` deliberately returns `Expr`, not `Bool`. Never rely on `==` for
  truthiness, never use `Expr` as a `Hash` key, and never write `should eq` on
  an expression (any non-nil object is truthy). Use `same_as?` or `key`.
- `&` binds tighter than `==` in Crystal. Boolean expression composition needs
  parentheses; prefer `Expr#and` / `Expr#or` in new code.
- `ArrayBase` is not `Enumerable` and has no `to_a`. Use `Prism::Values` for
  dtype-dispatched extraction; do not add a dtype `case` anywhere else.
- `value`/`value?` on Arrow arrays return the raw physical slot (`0.0` for a
  null float). Use `[]` / `[]?` for null-aware access.
- `Arrow::DataType::Kind` is a plain enum; use `kind.int64?` predicates rather
  than comparing to symbols.
- Avoid `not_nil!` on Arrow results. Kernels return `ArrayBase?` in some paths;
  handle the `nil` explicitly.

## Performance

- Do not copy buffers the operation does not need to change. Element-wise verbs
  map over chunks; `select` projects.
- Prefer `Arrow.take_record_batch` over per-row Crystal loops for any row
  selection.
- Build arrays with Arrow builders, not by boxing Crystal values into `Array` and
  re-wrapping.
- Measure before optimizing; put measurements in `bench/` (Phase 9), not in
  comments.

## Review checklist

- [ ] New public API is documented and listed in the plan's API section if
      user-facing.
- [ ] Specs cover values, schema, nulls, and error paths.
- [ ] Chunking behaviour is deliberate and tested.
- [ ] No `lib/` changes; no `module Arrow`/`module Parquet` blocks.
- [ ] `make format-check lint test` passes.
