# Prism — implementation plan

> **How to resume:** read the **Where we are** block below, then run `make lint`
> and `make test`. Full checklist: [docs/resuming.md](../docs/resuming.md).

`Prism` is a Crystal DataFrame library built on
[`dsisnero/arrow-cr`](../docs/development.md#dependencies) (Apache Arrow) and
`dsisnero/parquet-cr` (Apache Parquet). It provides an expression-based,
column-oriented table type over Arrow record batches, with eager and lazy
execution, Parquet/CSV/IPC interchange, and optional compile-time typed
schemas.

The proposed sketch that started this project lives at the bottom of this file
under [Appendix A](#appendix-a--the-original-proposal). Where the implemented
design differs, the difference is recorded in
[§7 Deviations](#7-deviations-from-the-proposed-sample-api) rather than left
implicit.

---

## Where we are

*Updated 2026-11-03.*

**Start here:** `git status` (must be clean), then `make lint && make test`
(498 examples, ~30 s warm). The table below is the only place that tracks status;
do not re-derive it from `git log`.

**State:** `0.2.0` is prepared on `main` — version, changelog and README are
bumped for Phases 12–18 — but **not tagged**: tagging and publishing are
owner-gated, so the last published release is still `v0.1.1`. `main` carries the
Phase 12 ergonomics work, the Phase 13 row layer, the Phase 14 value-coverage
work, the Phase 15 vectorized aggregation, the Phase 16 eager-grouping pruning,
the Phase 17 nested-key canonicalization, the Phase 18 bounded sort, the Phase 20
`top_n`, the Phase 21 multi-key passes, the Phase 22 dtype-aware
`Prism::SortEngine` that `group_by` shares, the Phase 23 fix for single nested
keys — which `arrow-cr` now owns, with Prism's workaround deleted — the Phase 24
row-encoded multi-key sort and the Phase 25 conditionals. Phases 0–18 are closed
apart from the cosmetic local folder rename.

**Dependencies verified against:** `arrow-cr` `3023eac` (bounded sort `5b4b958`,
the single-column `can_sort_to_indices?` guard in Phase 23) and `parquet-cr`
`25a889e` (both `branch: main`, resolved from GitHub). `shard.lock` and `lib/` are
gitignored, so this line is the record of what the suite below was run against.

**Next up, in order** (each is one atomic commit):

1. **Nothing is queued.** Phase 24 closed the last measured item on the sort path;
   what is left there is the floor, not a missing strategy: a multi-key sort pays
   one gather of the output columns plus one pass over the keys, and Phase 20
   already bounded the window cases. A new item needs a measurement or a failing
   case.
2. **Release is the owner's call.** `0.2.0` is prepared on `main` and untagged:
   on the word, push `main`, tag `v0.2.0`, then check that a consumer resolves
   `prism: github: dsisnero/prism` at `0.2.0`.

**Closed, do not re-open:** hash-based grouping. Measured twice. Before Phase 15
the sort was under 4% of a 1e5-key group-by; after Phase 15 the aggregates are
nearly free and the sort plus the row gather *is* the floor — and the contiguous
ranges it produces are exactly what lets one pass reduce every group (§14). A
hash table would trade one sort for a per-group gather, which is the work
vectorization just removed.

**Gates:** `make lint` + `make test` green, **498 examples**.
`PRISM_COMPILE_SPECS=1` adds the compile-failure spec (**499**, ~13 s) and
`PRISM_EXAMPLE_SPECS=1` adds the four example specs (**502**, ~1 min); with both
set the suite is **503**. Run both before a release.

**Last commits:** `66d11a8` is the `0.2.0` release prep (version, changelog,
README) and is **untagged** — the tag is the owner's call, and the local `v0.2.0`
tag created during prep was removed on request. Per phase, on `main`: `326a0b4`
ergonomics, `a7f1cd7` rows,
`2eac6e4` intervals and dictionary float keys, `414cf68` vectorized aggregation,
`cd473ea` group_by pruning, `b308373` nested float keys, `36aa01b` list-view keys
plus the `arrow-cr` report, `30f28a9` the bounded sort. Everything is on `main`
and `v0.2.0` is the current release, so a new feature starts from a clean tree
with nothing pending.

**Owner actions, not code:**

- Optional: rename the local checkout folder `dataframe/` → `prism/` (Phase 10).
  The repository is already `dsisnero/prism`; nothing references the folder name.

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Project scaffolding, dependency wiring, gates | **done** |
| 1 | Foundation: `DataFrame`, `Series`, schema, construction | **done** |
| 2 | Expressions: AST, DSL, evaluation, coercion | **done** |
| 3 | Eager verbs: `select`, `filter`, `with_column`, `sort_by`, … | **done** |
| 4 | `group_by` + aggregations | **done** |
| 5 | Joins and row/column concatenation | **done** |
| 6 | IO: Parquet, CSV, IPC (read + write, streaming) | **done** |
| 7 | `LazyFrame` + optimizer (projection/predicate pushdown) | **done** |
| 8 | Compile-time typed schemas | **done** |
| 9a | Display: plain table, optional `lipgloss` styling | **done** |
| 9b | API parity: lazy verbs, `Series` element-wise methods | **done** |
| 9c | Expression coverage: literal folding, decimals, dictionaries | **done** |
| 9d | Optimizer and performance parity (joins, slice, benchmarks, `arrow-row`) | **done** |
| 9e | Examples, memory notes, property oracles, v0.1.0 tag | **done** |
| 10 | Repo/GitHub rename `dataframe` → `prism` | **done** (local folder optional) |
| 11 | Post-v0.1.0: decimal arithmetic, nested materialization, `v0.1.1` | **done** |
| 12 | RedAmber ergonomics: masks, `remove`, ranges, `-` sorts, one-call aggregates | **done** |
| 13 | Rows: `DataFrame#rows` and `define_schema`-generated `S::Row` | **done** |
| 14 | Value coverage: interval materialization and dictionary float keys | **done** |
| 15 | Vectorized grouped aggregation: one pass across all groups | **done** |
| 16 | Eager `group_by` prunes unread columns before the sort and gather | **done** |
| 17 | Nested float keys canonicalized (struct, list, list-view, fixed-size list, map) | **done** |
| 18 | Bounded sort: a pinned window passes its limit to `lexsort_to_indices` | **done** |
| 19 | `v0.2.0` release prep: changelog for Phases 12–18, benchmark refresh | **done** (tag owner-gated) |
| 20 | Eager `top_n`: the fast spelling of `sort_by(...).head(n)` | **done** |
| 21 | Multi-key sort as stable single-key passes | **done** |
| 22 | Sort strategy by dtype, shared by `group_by` (`Prism::SortEngine`) | **done** |
| 23 | Single nested keys sort, so `group_by` accepts them (fix landed in `arrow-cr`) | **done** |
| 24 | Row-encoded multi-key sort for two or more variable-length keys | **done** |
| 25 | Conditional expressions: `if_else` and `case_when` | **done** |

---

## 1. What we are building

A library that makes Arrow-backed tabular data pleasant in Crystal:

- **Column-oriented.** A table is a logical sequence of named, typed columns;
  physical values live in Arrow arrays.
- **Expression-based.** Transformations are values (`col("price") * col("qty")`),
  not callbacks, so they can be inspected, type-checked, composed, and pushed
  down into readers.
- **Streaming-friendly.** Chunked storage means a multi-gigabyte Parquet file
  can be filtered and projected without materialising it — Parquet's
  `ProjectionMask` and predicate pushdown do real work.
- **Interoperable by construction.** Every boundary type is an `arrow-cr` type.
  There is no separate DataFrame schema dialect and no row-conversion tax.

## 2. Non-goals for v0.1

- No index/row-label semantics (pandas `set_index`). Rows are positional.
- No `pivot`, `melt`, window functions, or time-series resampling.
- No SQL surface, no query-string parser.
- No multi-threaded execution engine; the optimizer is rule-based and
  single-threaded.
- No async/object-store IO (`parquet-cr` has it; we expose only the sync path).
- No Arrow Flight.
- No pandas/Polars API emulation beyond the verbs listed in §6.

## 3. Ground rules from the dependencies (verified)

These were confirmed by compiling and running against the sibling checkouts, not
read off documentation. They drive several design decisions.

### 3.1 What `arrow-cr` already gives us

| Need | Available | Notes |
| --- | --- | --- |
| Typed columns | `Arrow::Int64Array`, `Float64Array`, `BooleanArray`, `StringArray`, … | `PrimitiveArray(T)` aliases |
| Schema | `Arrow::Schema`, `Arrow::Field`, `Arrow::Fields`, `Arrow::Metadata` | field lookup by name/index |
| Type introspection | `Arrow::DataType` + `Kind` enum, `#integer?`, `#floating?`, … | one class, not a union |
| Row chunk | `Arrow::RecordBatch` | `#column`, `#[]`, `#select_columns`, `#slice`, `#project` |
| Predicate filter | `Arrow.filter_record_batch(batch, mask)` | kernel, not a batch method |
| Row gather | `Arrow.take_record_batch(batch, indices)` | |
| Sort indices | `Arrow.lexsort_to_indices([SortColumn, …])` | multi-key, per-key options |
| Group boundaries | `Arrow.partition(columns) : Partitions` | requires pre-sorted input |
| Aggregates | `Arrow.sum_array/min_array/max_array`, `sum/product/min/max` | numeric; no `mean`/`count` kernel |
| Arithmetic | `Arrow.add/sub/mul/div/rem/neg` | **same dtype only** |
| Comparison | `Arrow.eq/neq/lt/lt_eq/gt/gt_eq/distinct/not_distinct` | propagates nulls |
| Cast | `Arrow.cast(array, dtype)`, `Arrow.cast_with_options` | |
| Combine chunks | `Arrow.concat_batches(schema, batches)` | identical dtypes required |
| Display | `Arrow.pretty_format_batches`, `Arrow.print_batches` | usable as a starting point |

### 3.2 What `parquet-cr` gives us

| Need | Available |
| --- | --- |
| Read batches | `Parquet::Arrow::ParquetRecordBatchReaderBuilder.try_new(file_reader).build` → `Iterator(Arrow::RecordBatch)` |
| File handle | `Parquet::File::SerializedFileReader.new(Parquet::File::FileChunkReader.new(path))` |
| In-memory handle | `…BytesChunkReader.new(bytes)` |
| Projection pushdown | `Parquet::Arrow::ProjectionMask.columns(builder.parquet_schema, names)` |
| Predicate pushdown | `with_row_filter(RowFilter.new(ArrowPredicateFn.new(mask) { |batch| … }))` |
| Slice pushdown | `with_offset(n)`, `with_limit(n)` |
| Row-group pruning | `with_row_groups([0, 2])`, `with_row_selection(...)` |
| Batch sizing | `with_batch_size(n)` (default 1024) |
| Write batches | `Parquet::Arrow::ArrowWriter.try_new(io, schema, props)`, `#write`, `#close` |
| Compression | `Parquet::Compression::SNAPPY`, `.zstd(3)`, `.gzip(9)`, … |
| Writer props | `Parquet::File::Properties::WriterProperties.builder` |

### 3.3 Constraints this imposes (and how we handle each)

#### C1. There is no `Arrow::Table`

`arrow-cr` exposes `RecordBatch` only. A multi-chunk table must be built here.
**Decision:** `Prism::DataFrame` owns `Array(Arrow::RecordBatch)` plus one
`Arrow::Schema`; `#to_arrow` concatenates on demand.

#### C2. Kernels are free functions, batches have no verbs

`RecordBatch` has `slice`/`project`/`select_columns` but no `filter`, `take`,
`sort`, or `concat`. **Decision:** `Prism::DataFrame` verbs wrap the kernels;
no monkey-patching of `Arrow` anywhere (see
[docs/coding-guidelines.md](../docs/coding-guidelines.md)).

#### C3. Mixed-dtype arithmetic raises

`Arrow.mul(Float64Array, Int64Array)` raises
`ArrowError: Invalid arithmetic operation`. **Decision:** the expression engine
performs explicit numeric coercion (§8.2) and inserts `Arrow.cast` before
calling kernels. Users never see the raw error.

#### C4. `ArrayBase` has no generic value extraction

`to_a`, `size`, `count` live on the *typed* leaf classes (`Collection(V)`), not
on `ArrayBase`, and the return type depends on the dtype. **Decision:** add
`Prism::Values.extract(array : Arrow::ArrayBase) : Array(...)` with a
`DataType::Kind` dispatch mirroring `Arrow.make_array`, used by `Series#to_a`
and by display formatting. This is a Phase 1 deliverable, and the single place
where dtype dispatch is allowed to live.

#### C5. `Arrow.partition` needs pre-sorted, pre-grouped input

It returns row ranges only when equal keys are adjacent, and `SortOptions`
defaults to *ascending, nulls first*. **Decision:** `group_by` is
`lexsort_to_indices` → `take_record_batch` → `partition` → per-range aggregate.
Null keys form their own group; placement is configurable
(`null_keys: :first | :last | :drop`, §8.1).

Polars has the same two representations and picks by data shape: hash-based
(`GroupsType::Idx`, first index + index list) by default, sort-based
(`GroupsType::Slice`, contiguous `[start, len]` pairs) when keys are sorted. We
always take the slice path because it yields `RecordBatch#slice` views instead
of index lists, and because it makes output order deterministic instead of
unspecified. Two measurements have since closed the hash alternative on
evidence: before Phase 15 the key's sort was under 4% of a 1e5-group `group_by`,
and after it the aggregates are nearly free, so the contiguous ranges are what
let one pass reduce every group — a hash table would trade the sort for a
per-group gather (§14).

#### C6. No `mean`, `count`, or `count_distinct` kernels

**Decision:** compose them. `mean = sum / non_null_count` with the result
promoted to `Float64` (integers included); `count` uses `ArrayBase#count`
(non-null rows) with an explicit `count_all` for `len`.

#### C7. Named arguments become a `NamedTuple`, not a macro

Crystal methods accept a double splat: `def f(**named)` collects call-site
named arguments into a `NamedTuple` (verified). A **macro** with `**named` also
works, but is the wrong tool here: a macro defined inside a class expands at the
call site *without the receiver*, so `df.with_columns(a: x)` would expand to a
receiverless call. **Decision:** named-argument APIs (`with_columns`,
`GroupBy#aggregate`) are plain methods using `**named`, each also offering a
`Hash` overload for runtime-built names.

```crystal
def with_columns(**named) : DataFrame
  named.each { |name, expr| ... }
end
```

#### C8. A macro parameter named `name` breaks named arguments

`macro define_schema(name, **fields)` called as
`define_schema(User, name: String)` fails to compile ("undefined constant
User") because `name:` binds to the positional parameter. **Decision:** the
schema macro's first parameter is `schema`, never `name`; this is recorded as a
comment in the macro source.

#### C9. `&` binds tighter than `==` in Crystal

Verified: `a == b & c > d` parses as `((b & c) > d)`. **Decision:** boolean
expression composition must use parentheses, and `Expr#and` / `Expr#or` are
provided as the safe spelling. Documented as the number-one user gotcha (§8.5).

#### C10. `Parquet` and `Arrow` writers/readers are strict about types

`ArrowWriter#write` (not `write_batch`) rejects mismatched schemas;
`with_projection` does not coerce. **Decision:** `write_parquet` validates the
target schema first and raises `Prism::DtypeError` with the column name, rather
than letting `Parquet::ArrowError` surface without context.

#### C11. `value`/`value?` are not null-aware

Verified in `arrow-cr`: `PrimitiveArray#value(i)` returns the *physical* slot
(`0.0` for a null float) and `value?(i)` only guards the bounds. The null-aware
accessors are the `Collection` ones: `arr[i]` → `T?` (raises `IndexError` out of
range) and `arr[i]?` → `T?` (returns `nil` out of range). **Decision:**
`Prism::Series#value`/`#[]` use the null-aware path, and `Prism::Values` — the
single dtype-dispatch point (C4) — reads through `[]?` so a null can never leak
out as a zero. Specs assert this explicitly.

#### C12. `arrow-cr` has no `ChunkedArray`, and that is deliberate

`arrow-rs` does not implement a chunked-array type; it recommends
`Vec<ArrayRef>` and leaves rechunking to the consumer. Verified: no
`ChunkedArray` exists anywhere in `arrow-cr`. **Decision:** `Prism::Series` holds
`Array(Arrow::ArrayBase)` — one entry per frame batch — and every aggregate
reduces each chunk with the Arrow kernel, then folds the partials (Polars does
the same with its own `ChunkedArray`). **Verbs never call `#to_arrow`.** Only the
edges do: `#to_a`, `#to_arrow`, `#each`, display, and file writers.

#### C13. `Arrow.merge` compacts its inputs; `Arrow.zip` is row-aligned

Verified by running it: `merge(mask, truthy, falsy)` consumes `truthy` only at
`true` positions, in order — the arrow-rs contract. Passing a full-length array
to fill nulls silently shifts values and drops the tail (a 3-row fill produced
`[1.5, 0.0, nil]`). `zip` requires full-length arrays or length-1 scalars and
selects per row, which is what expressions produce. **Decision:**
`fill_null`/`coalesce` use `Arrow.zip`; `merge` is only for compacted inputs.

#### C14. `!` is a Crystal pseudo-method and cannot be overloaded

`def !` fails to compile ("'!' is a pseudo-method and can't be redefined"), so
`!expr` can never build an expression. **Decision:** boolean negation is
`.not`; `&`/`|` operators remain available for composition with mandatory
parentheses (C9).

#### C15. Arrow kernels reject a `Datum` union argument

The kernels are untyped methods that resolve `numeric_datum`/`zip_datum`
overloads against static types, so passing `ArrayBase | Scalar(ArrayBase)`
fails to compile even though both members are supported. **Decision:**
`Prism::Kernels.apply_op` narrows the union, then calls a generic `apply` that
Crystal instantiates once per operand-type pair — one written op table, four
compiled variants, and scalars still broadcast inside Arrow.

#### C16. Arrow's `Field#with_*` mutate, and `with_schema` only adds fields

`Field#with_name`, `with_metadata` and `with_nullable` assign to the receiver and
return `self` — they are not functional copies. `RecordBatch#with_schema`
requires the new schema to be a **superset** of the current one, so it cannot
express a rename. **Decision:** rename does `field.copy.with_name(...)` and
rebuilds each batch with `Arrow::RecordBatch.new(schema, batch.columns,
batch.num_rows)`; that keeps the arrays and copies nothing.

#### C17. `select` is a Crystal keyword

`select` starts the channel-select expression, so a receiverless `select(...)`
inside `DataFrame` is parsed as the keyword and fails with "expecting when, else
or end". **Decision:** internal calls use `self.select(...)`; the public method
name is unchanged because `df.select(...)` parses fine.

#### C18. There is no dedupe kernel, and `uniq` is the slow path

Verified in `arrow-cr`: no `unique`/`count_distinct`/`value_counts` kernel exists
(nor in `arrow-rs`). What exists is:

| API | What it actually is |
| --- | --- |
| `Arrow.partition` | typed boundary detection via `cmp::distinct` on adjacent rows — the Arrow-native primitive |
| `Arrow.sort_to_indices` | typed per-dtype sorts (`sort_primitive`, `sort_byte_items`, …) |
| `ArrayBase#uniq` | a **Crystal-level** sort driven by a `DynComparator` closure, allocating one `Int32` per row |
| `Arrow.distinct(lhs, rhs)` | element-wise comparison of two arrays, *not* dedupe |

**Decision:** distinct goes through sort → `partition` → `take`, and `n_unique`
uses `Aggregates.count_distinct` (typed sort + partition) rather than
`array.uniq`. Polars' `unique_stable` uses a Rust hash set instead; its
non-stable `unique` sorts, which is the shape used here. The stable path is
therefore O(n log n) rather than O(n).

**Correction after checking `arrow-rs` directly:** the missing piece is not a
*hashing* kernel. `arrow-rs` has none either — hashing is not part of the Arrow
specification, which defines memory layout and IPC only, and `arrow-rs` uses
`ahash` internally for dictionary interning. What `arrow-rs` does provide is the
**`arrow-row` crate**: `RowConverter`/`Row`, a canonical byte encoding of one or
more columns (1-byte null sentinel, big-endian values, sign bit flipped for
signed integers, IEEE `totalOrder` for floats, block encoding for strings) built
so that `memcmp` gives the correct ordering. That — not a hash function — is the
primitive that makes O(n) distinct and unboxed join keys possible.

**Resolved 2026-11-01:** `arrow-row` landed in `arrow-cr` (Phase 13 there), so
`Prism::RowKeys` uses `Arrow::RowConverter` for joins, `distinct`, `unique` and
`n_unique`. The earlier sort-based distinct path and the boxed `Array(Value)`
join keys are gone; see [§14](#native-row-encoding-for-keys). Because the row
format is `totalOrder`, float key columns are canonicalized before encoding, so
keying follows `==` (`-0.0 == 0.0`, every NaN one key) and `group_by` — which
sorts canonical keys — agrees with `distinct`.

#### C19. `Arrow.take_record_batch` reuses the source schema

`take_record_batch` gathers columns and then validates them against the *source*
schema, so taking with a null index into a non-nullable column raises
`ArrowError: Column 'id' is declared as non-nullable but contains null values`.
Outer joins need exactly that. **Decision:** `Prism::Join` gathers column by
column with `Arrow.take` and rebuilds the batch against a relaxed (all-nullable)
schema, then `compose` restores the precise nullability on the final schema.

#### C20. `parquet-cr`'s row filter corrupted null-bearing columns (fixed)

**Resolved 2026-11-01** in `parquet-cr` `5883ff1` ("port: Align row-filter
skips with definition levels"), merged to `main`, which aligns the skip with
definition levels in `src/parquet/column/reader.cr`. Prism re-enabled
`with_row_filter`, so Parquet predicates prune per row group again, and
`spec/prism/io/parquet_spec.cr` keeps the reproduction as a permanent guard.
The report lives at
[`shard_issues/parquet-cr.2.md`](../shard_issues/parquet-cr.2.md).

Original finding, kept because it explains the guard: a row filter used to shift
columns containing nulls by one row per null while leaving null-free columns
correct. Write a 5-row frame (`region` Utf8 with
a null, `price` Float64 with a null, `qty` Int64) and read it with
`with_row_filter(RowFilter.new([ArrowPredicateFn.new(qty_mask) { ... }]))` for
`qty > 2`, where the mask logged by the predicate is `[F,F,T,T,T]`:

| Column | Result | Correct |
| --- | --- | --- |
| `qty` (Int64) | `[3, 4, 5]` | yes |
| `flag` (Boolean) | `[true, false, true]` | yes |
| `region` (Utf8) | `["east", "west", "east"]` (rows 0,1,2) | no |
| `price` (Float64) | `[30.0, 40.0, 10.0]` (rows 2,3 then row 0) | no |

The predicate's own mask is correct, so the fault is in how the reader applies
the selection to the other column readers, not in predicate evaluation.
**Decision:** Prism does not call `with_row_filter`; the `predicate:` argument
filters in memory after a projected read. Projection pushdown, the other half of
the win, still applies. `spec/prism/io/parquet_spec.cr` keeps the reproduction as
a regression guard, and re-enabling pruning is one commented block in
`src/prism/io/parquet.cr`.

**Upstream action:** the report is written as
[`shard_issues/parquet-cr.2.md`](../shard_issues/parquet-cr.2.md) (numbered per
shard, continuing from `parquet-cr.1` in `amazon_data`), with a standalone
reproducer verified against the pinned checkout. Root cause narrowed: selections
that span a null in a column shift that column's values by one row per null,
while columns without nulls in the same batch are correct.

#### C21. `def select` cannot be written inside a macro body

Reproduced while generating the typed frame: a macro body containing

```crystal
def select(& : {{schema}} -> ::Prism::Expr)
```

fails with "unterminated macro", because the parser reads `select` as the
channel-select keyword whenever it appears in macro-expanded code. A named block
parameter (`&fn`) fails the same way; renaming the method (`pick`) works, and the
same `def select` in ordinary code is fine. **Decision:** `Prism::TypedVerbs(S)`
declares the shared typed-frame verbs in ordinary code, and the generated struct
only `include`s that module plus the per-field accessors. `define_schema` also
lives inside `module Prism` (a top-level macro cannot be called as
`Prism.define_schema`).

#### C22. A bare `Range` restriction loses dispatch to `Enumerable(T)`

Verified while adding row slicing: with both overloads present,

```crystal
def [](range : Range) : DataFrame            # loses
def [](names : Enumerable(T)) forall T       # wins for Range(Int, Int)
```

`df[1..3]` dispatched to the column-list overload, because a `Range` *is* an
`Enumerable` and the bare generic restriction is not ranked more specific. Naming
the type parameters fixes the ordering: `def [](range : Range(B, E)) forall B, E`.
(The reverse order of the definitions does not help.) **Decision:** the row
overload carries explicit type parameters, with a comment in
`src/prism/dataframe.cr` so nobody "simplifies" it back.

#### C23. A type-restricted splat requires at least one argument

`def sum(*columns : String | Symbol)` cannot be called as `sum` — the compiler
reports "wrong number of arguments (given 0, expected 1+)". An *unrestricted*
splat (`*columns`) accepts zero, but then the element type is unconstrained and
a typo becomes a runtime `ColumnNotFound` instead of a compile error.
**Decision:** one-call aggregates declare the no-argument form as a second
overload, generated alongside the splat by a macro in
`src/prism/group_by.cr`.

### 3.4 Verified end-to-end sketch

This runs today against the sibling checkouts (it is the basis for Phase 1–4
specs):

```crystal
require "arrow-cr"
require "parquet-cr"

region = Arrow::StringArray.from_nullable(["east", "west", "east", nil, "west"])
price  = Arrow::Float64Array.from_nullable([10.0, 20.0, 30.0, 40.0, 50.0])
qty    = Arrow::Int64Array.from_nullable([1_i64, 2_i64, 3_i64, 4_i64, nil])
batch  = Arrow::RecordBatch.from_columns({region: region, price: price, qty: qty})

# predicate -> filter
mask     = Arrow.gt(batch["price"], Arrow::Float64Array.new_scalar(15.0))
filtered = Arrow.filter_record_batch(batch, mask)

# mixed-dtype arithmetic requires an explicit cast (C3)
promoted = Arrow.cast(batch["qty"], Arrow::DataType.float64)
total    = Arrow.mul(batch["price"], promoted).as(Arrow::Float64Array)

# group-by machinery (C5)
idx    = Arrow.lexsort_to_indices([Arrow::SortColumn.new(batch["region"])])
sorted = Arrow.take_record_batch(batch, idx)
Arrow.partition([sorted["region"]]).ranges.each do |range|
  slice = sorted.slice(range.begin, range.size)
  Arrow.sum(slice["price"].as(Arrow::Float64Array))
end

# parquet round trip
sink = IO::Memory.new
props = Parquet::File::Properties::WriterProperties.builder
  .set_compression(Parquet::Compression.zstd(3)).build
writer = Parquet::Arrow::ArrowWriter.try_new(sink, batch.schema, props)
writer.write(batch)
meta = writer.close
```

## 4. Naming and namespace decisions

| Decision | Choice | Rationale |
| --- | --- | --- |
| Top-level module | `Prism` | Owner's choice; short, no clash with Crystal stdlib |
| Table type | `Prism::DataFrame` | Explicit, greppable |
| Lazy table | `Prism::LazyFrame` | Mirrors Polars; unambiguous |
| Column type | `Prism::Series` | Distinguishes a 1-D column from `Arrow::ArrayBase` |
| Expression type | `Prism::Expr` | |
| Entry-point functions | `Prism.read_parquet`, `Prism.read_csv`, `Prism.from_arrow`, `Prism.concat`, `Prism.scan_parquet` | Constructors do not hang off the table type |
| Lazy entry points | `df.lazy` for an in-memory frame, `Prism.scan_parquet` for a file | One per source, matching Polars: `df.lazy()` and `pl.scan_*`. A `Prism.scan(df)` alias existed and was removed (Phase 9e) — "scan" means "read a source lazily", so it was both redundant and misleading; there is no `pl.scan(df)` either, only `pl.LazyFrame(df)`. |
| Bare expression builders | `Prism::DSL` (`include Prism::DSL`) | Keeps `col`/`lit` out of the global namespace |
| Errors | `Prism::Error` and subclasses | See §8.4 |
| Column projection verb | `select` | Relational π. SQL `SELECT` also lists columns, and dplyr, Polars and Arquero all call it `select` |
| Row restriction verb | `filter` | Relational σ. `select` never filters rows in this API |

**`select` means projection, not filtering.** `df.select("a", "b")` keeps rows
and picks columns; `df.filter { ... }` keeps columns and picks rows. The two are
deliberately disjoint, matching SQL/Polars/dplyr. Note the collision with
RedAmber, which uses `select` for row filtering and `pick` for column selection:
a RedAmber habit maps to `filter` and `select` respectively here. The plan node
for projection is `Plan::Select`, and for restriction `Plan::Filter`. Because
`select` is a Crystal keyword (C17), receiverless calls inside the class spell
`self.select`.

**No `Arrow::*` reopening.** `arrow-cr` owns that module. Extending it from a
downstream shard would make `Prism` requireable-but-broken (types defined only
after a second `require`) and would couple us to another shard's names.

**Shard name:** `shard.yml` declares `name: prism` and the entrypoint is
`src/prism.cr`, so consumers write `require "prism"`. The GitHub repository is
`dsisnero/prism` (Phase 10); only the local checkout folder may still be named
`dataframe/`, which is cosmetic and deliberately not part of any commit.

## 4.1 Design influences

Crystal is Ruby-shaped syntax on a statically typed, LLVM-compiled language with
macros, so no single reference library fits. Each contributes one axis:

| Library | Axis | Adopted as | Deliberately not adopted |
| --- | --- | --- | --- |
| [Polars](https://deepwiki.com/pola-rs/polars) | Execution | Expression engine; lazy plan; pass ordering (projection pruning, predicate pushdown, slice pushdown, constant folding, dead-column elimination); contiguous-slice groups (`GroupsType::Slice`) reduced in one pass across all groups (`agg_first`/`agg_last` as a single take, `agg_valid_count` over validity bits) | Common-subplan elimination via `Cache` nodes, join reordering, sort collapse — no payoff at v0.1 scale, and they need plan-level cost modelling |
| [Kotlin DataFrame](https://deepwiki.com/Kotlin/dataframe) | Type safety | Declared schema → generated typed accessors and compile-time column names | Pipeline-wide schema *refinement* (see below) |
| [RedAmber](https://github.com/heronshoes/red_amber) | API feel | Block verbs returning new frames; Arrow as the storage substrate | Ruby-only dynamic typing; we pair blocks with typed `Expr` values |
| [Arquero](https://deepwiki.com/uwdata/arquero) | Verb vocabulary | Small relational verb set (`select`, `filter`, `derive`, `groupby`+`rollup`, `orderby`, `join`) | Its expression-string parser and its "no-copy" claims |
| [Explorer](https://deepwiki.com/elixir-explorer/explorer) | Layering | dplyr-style verbs over a lazy backend, with `collect` as the explicit execution boundary | Backend NIF boundary; Crystal has no runtime boundary to cross |

### The Kotlin limitation, stated plainly

Kotlin DataFrame's typed columns work because of a **compiler plugin**: it hooks
the FIR frontend, uses `@Refine`/`@Interpretable`/`@DataSchema`, and rewrites
return types so that `df.filter { … }.select { … }` tracks the schema through
every call.

Crystal macros have no equivalent. A macro expands at the call site, but it
cannot refine the return type of an arbitrary method based on the schema flowing
through it. Therefore:

- `UserSchema::Frame` is typed **at the declaration boundary**. A misspelt
  field is a compile error, and accessors are typed.
- A verb that changes the schema (`with_column`, `select`, `join`) returns a
  plain `DataFrame`, not a refined frame. Re-declare or re-wrap with
  `S.from_dataframe(...)` to regain typed access.
- This is a hard limitation, not a missing feature to be "fixed later" — it
  would require compiler-plugin support in Crystal. Recorded here so nobody
  plans Phase 8 around it.

Designing a new verb or optimizer pass: read the relevant DeepWiki page in
[`CLAUDE.md`](../CLAUDE.md#design-influences), then add the decision to this
plan rather than leaving it implicit.

## 5. Architecture

### 5.1 Layers

```text
  user code            df.filter { col("a") > 1 }
                              |
  verbs (eager)        DataFrame#filter, #select, #group_by, #join
                              |
  expressions          Expr AST -> eval(batch) -> Arrow::ArrayBase
                              |
  kernels              Arrow.filter_record_batch, take_record_batch,
                       lexsort_to_indices, partition, cast, add/mul, aggregates
                              |
  storage chunks       Array(Arrow::RecordBatch) + Arrow::Schema
                              |
  IO                   parquet-cr (ArrowWriter, ParquetRecordBatchReaderBuilder)
```

The lazy layer wraps the same verbs: `LazyFrame` builds a plan of the same
logical operations and executes it through the same kernels after optimizing.

### 5.2 Module layout

```text
src/prism.cr                # entrypoint: requires everything, Prism::VERSION
src/prism/error.cr
src/prism/dtype.cr          # dtype predicates/helpers over Arrow::DataType
src/prism/values.cr         # C4: dtype-dispatched value extraction
src/prism/row_keys.cr       # arrow-row keys for joins, distinct, n_unique
src/prism/series.cr
src/prism/dataframe.cr
src/prism/construction.cr   # Prism.from_arrow / from_batches / from_hash
src/prism/expression/expr.cr
src/prism/expression/dsl.cr
src/prism/expression/eval.cr
src/prism/expression/coerce.cr
src/prism/verbs/select.cr
src/prism/verbs/filter.cr
src/prism/verbs/mutate.cr
src/prism/verbs/sort.cr
src/prism/verbs/distinct.cr
src/prism/verbs/concat.cr
src/prism/group_by.cr
src/prism/grouped_agg.cr      # Phase 15: one pass across all groups
src/prism/agg.cr
src/prism/join.cr
src/prism/lazy/frame.cr
src/prism/lazy/plan.cr
src/prism/lazy/optimizer.cr
src/prism/lazy/executor.cr
src/prism/io/parquet.cr
src/prism/io/csv.cr
src/prism/io/ipc.cr
src/prism/display.cr
src/prism/typed.cr
```

Splitting by verb keeps files under ~400 lines and mirrors the phase list, so a
resuming session can jump straight to the relevant file.

### 5.3 Type map

| Prism | Representation |
| --- | --- |
| `Prism::DataFrame` | `@batches : Array(Arrow::RecordBatch)`, `@schema : Arrow::Schema` |
| `Prism::Series` | `@chunks : Array(Arrow::ArrayBase)`, `@name : String`, `@dtype : Arrow::DataType` |
| `Prism::Expr` | immutable AST (§5.4) |
| `Prism::GroupBy` | `@frame : DataFrame`, `@keys : Array(String)`, `@options` |
| `Prism::LazyFrame` | `@plan : Plan::Node` |
| `Prism::Schema` (typed) | macro-generated struct with `Expr` accessors, plus a `Row` struct |
| `Prism::RowHash` | `Hash(String, Value)`; one materialized row |

### 5.4 Expression model

```crystal
module Prism
  abstract struct Expr
    # structural identity for `same_as?` / plan dedup
    abstract def key : String
    # static type inference; raises Prism::ColumnNotFound upstream
    abstract def dtype(schema : Arrow::Schema) : Arrow::DataType
    # evaluate against one chunk
    abstract def eval(ctx : EvalContext) : Arrow::ArrayBase
  end

  struct Col    < Expr; getter name : String; end
  struct Lit    < Expr; getter value : Literal; end
  struct Unary  < Expr; getter op : Op; getter operand : Expr; end
  struct Binary < Expr
    getter op : Op
    getter left : Expr
    getter right : Expr
  end
  struct Agg    < Expr; getter fn : AggFn; getter operand : Expr; end
  struct Cast   < Expr
    getter operand : Expr
    getter target : Arrow::DataType
  end
  struct Alias  < Expr; getter operand : Expr; getter name : String; end
end
```

Design rules:

- `Expr` is a **struct hierarchy over an abstract struct**, so values are cheap
  to copy and cannot be mutated; no reference cycles in a plan.
- `dtype(schema)` is total: every expression can report its output dtype before
  any data is read. This is what makes typed schemas, planning, and coercion
  possible without evaluating.
- `eval` never sees a whole table, only one `RecordBatch` plus an optional
  group range. Aggregating expressions return a length-1 array; the group
  executor broadcasts them.
- `Expr#key` gives a canonical string (`"col:price"`, `"bin:mul(col:price,col:qty)"`)
  used for deduplication and `explain` output.

`Literal` is a small union of the Crystal values we accept:
`Int64 | Float64 | Bool | String | Time | Nil | Bytes`.

### 5.5 Chunk model and invariants

- All batches in `@batches` share the same `@schema` (verified at construction).
- A `DataFrame` may be empty (`batches == []`, `schema` still meaningful) or
  zero-column (`schema` empty). No separate row-count field is needed:
  `Arrow.concat_batches` already sums `num_rows` for an empty field list and
  returns `RecordBatch.new_empty(schema)` for an empty chunk list (verified in
  `lib/arrow-cr/src/arrow/concat.cr`). `#to_arrow` therefore just delegates.
- Element-wise verbs (`filter`, `select`, `with_column`, `cast`, `drop`) map
  over batches and **preserve chunking** — no concatenation, no copy of
  untouched buffers.
- Aggregates on a `Series` (`sum`, `mean`, `min`, `max`) reduce each chunk with
  the Arrow kernel and fold the partials, so they never build a consolidated
  column (C12, §14).
- Row-reordering verbs (`sort_by`, `group_by`, `distinct`, `join`) concatenate
  to a single batch first. Phase 9 may revisit this if benchmarks justify it.
- `#each_batch` yields stored chunks; `#to_arrow` returns one concatenated
  batch. Both are public so users can choose memory behaviour.

## 6. Public API (target for v0.1)

```crystal
require "prism"
include Prism::DSL       # optional: enables bare col(...) / lit(...)

# --- construction -------------------------------------------------------
df = Prism.from_arrow(record_batch)
df = Prism.from_batches(batches)
df = Prism.from_columns({"id" => Arrow::Int64Array.from_values([1, 2, 3])})
df = Prism.read_parquet("data/sales.parquet")
df = Prism.read_csv("data/sales.csv")
df = Prism.scan_parquet("data/sales.parquet")   # LazyFrame, pushdown-ready
lazy = df.lazy                                 # the in-memory lazy entry (no Prism.scan)

# --- inspection ---------------------------------------------------------
df.shape            # => {rows: 3, columns: 4}
df.column_names     # => ["id", "region", ...]
df.dtypes           # => {"id" => Arrow::DataType.int64, ...}
df.schema           # => Arrow::Schema
df["region"]        # => Prism::Series
df.to_arrow         # => Arrow::RecordBatch
df.to_s             # aligned text table
df.head(5); df.tail(5); df.slice(1, 10)
df.rows             # => [{"id" => 1, ...}] one Prism::RowHash per row
df.row(0)           # => Prism::RowHash; IndexError outside the frame
df.each_row { |row| ... }   # one hash per row, one chunk at a time

# --- expressions --------------------------------------------------------
col("price") * col("qty")
col("price").sum
col("status").eq("completed")                        # method form
(col("status") == "completed") & (col("price") > 10) # operator form (parens!)
col("name").cast(Arrow::DataType.large_utf8)

# --- eager verbs --------------------------------------------------------
df.filter { (col("status") == "completed") & (col("price") > 10.0) }
df.filter(col("age") >= 21)
df.select("order_id", "region")
df.select(col("region"), (col("price") * col("qty")).alias("total"))
df.with_column("total_amount", col("price") * col("quantity"))
df.with_columns(total: col("price") * col("qty"))     # macro, named args
df.drop("tmp")
df.rename("old" => "new")
df.sort_by("total_revenue", order: :desc)
df.sort_by(col("a").desc, col("b").asc, nulls: :last)
df.sort_by_slice([col("a").desc], 0, 10)  # sorts, gathers only rows 0..10
df.distinct
df.distinct("region")
df.drop_nulls("region")
df.fill_null(0)
df.cast("qty" => Arrow::DataType.float64)

# --- ergonomics (RedAmber-derived, §17 / Phase 12) ----------------------
df.filter(df["price"] > 15)        # a boolean Series is a mask
df.remove(col("region") == "west") # keeps rows the predicate rejects or can't judge
df.sort_by("-total_revenue")       # descending shorthand
df[1..3]                           # rows by range; df[..2], df[-2..], df[1...3]
df[["region", "price"]]            # columns by name

# --- grouping -----------------------------------------------------------
df.group_by("region", "category").aggregate(
  total_revenue: col("price").sum,
  avg_price:     col("price").mean,
  order_count:   col("order_id").count,
).sort_by("total_revenue", order: :desc)

df.group_by("region").agg(col("price").sum.alias("total"))

df.group_by("region").sum("price", "qty")  # => region, price, qty
df.group_by("region").mean                 # every numeric column
df.group_by("region").count                # one `count` column of row counts

# --- combining ----------------------------------------------------------
df.join(other, on: "id", how: :inner)
df.join(other, left_on: "id", right_on: "user_id", how: :left)
Prism.concat([df1, df2])       # vertical
df.hstack(other)               # horizontal

# --- lazy ---------------------------------------------------------------
result = Prism.scan_parquet("data/sales.parquet")
  .filter(col("created_at") >= Time.utc(2026, 1, 1))
  .select("user_id", "department", "salary")
  .group_by("department")
  .aggregate(mean_salary: col("salary").mean)
  .collect

puts Prism.scan_parquet("data/sales.parquet").filter(col("price") > 10).explain

# --- typed schemas ------------------------------------------------------
Prism.define_schema UserSchema, id: Int64, name: String, active: Bool, salary: Float64

typed = UserSchema.from_arrow(record_batch)   # Prism::TypedFrame(UserSchema)
typed.filter(&.active).select(&.salary)       # compile error on a misspelt field
typed.salary                                  # => Arrow::PrimitiveArray(Float64)
typed.rows                                    # => Array(UserSchema::Row)
typed.row(0).salary                           # => Float64? (nullable)
UserSchema::Row.from_hash(df.row(0))         # dynamic -> nominal row
UserSchema.schema                             # => Arrow::Schema
```

### 6.1 `Expr` surface

| Category | Methods |
| --- | --- |
| Arithmetic | `+ - * / // % ** abs round floor ceil sqrt pow` |
| Comparison | `== != < <= > >= eq neq lt lt_eq gt gt_eq` |
| Boolean | `&` / `\|` (operators, parenthesise), `and` / `or` / `not` (methods; `!` is not overloadable) |
| Null | `is_null`, `is_not_null`, `fill_null(v)`, `coalesce(*exprs)` |
| Cast | `cast(dtype)` |
| String | `str_length`, `str_contains`, `str_starts_with`, `str_to_uppercase`, `str_to_lowercase` |
| Aggregate | `sum min max mean count count_all n_unique first last std var median` |
| Naming | `alias(name)` |
| Introspection | `dtype(schema)`, `columns : Array(String)`, `key`, `same_as?(other)` |

### 6.2 `Series` surface

`name`, `dtype`, `size`, `count`, `null_count`, `to_a`, `to_arrow`, `each`,
`each_chunk`, `sum`, `mean`, `min`, `max`, `unique`, `n_unique`, `[]`/`[]?`,
`value(i)`, plus element-wise `== != < <= > >=` against another series or a
scalar.

All of the above is implemented (Phases 1–4 for the aggregates and access,
Phase 9b for `is_null`, `fill_null`, `cast` and element-wise `==`, phase 12 for
the remaining comparison operators and scalar coercion).

## 7. Deviations from the proposed sample API

| # | Proposed | Implemented | Why |
| --- | --- | --- | --- |
| D1 | `Arrow::DataFrame` | `Prism::DataFrame` | Owner chose the `Prism` namespace; reopening `module Arrow` couples this shard to `arrow-cr` (§4) |
| D2 | `Arrow::DataFrame.from_arrow(batch)` | `Prism.from_arrow(batch)` | Constructors read better at module level; avoids a type-level factory for every entry point |
| D3 | `Arrow.define_schema UserSchema, id: Int64, name: String` | `Prism.define_schema` (same shape) | namespace only; macro signature verified (C7, C8) |
| D4 | `col(&.active)` | `typed.filter(&.active)` / `typed.select(&.salary)` | `col(&.x)` needs a receiver of unknown schema type at the call site. Passing the schema to the *frame verb* gives the same compile-time guarantee with a type Crystal can infer |
| D5 | `aggregate(name: expr, ...)` as a method | instance **macro** with `**named` (+ hash overload) | Crystal has no keyword splat on methods (C7) |
| D6 | `df.filter { col("age") >= 21 }` works bare | requires `include Prism::DSL` | Blocks do not change the receiver; a global `col` would pollute the namespace |
| D7 | `_ & _` without parentheses | parentheses required (or `.and`/`.or`) | `&` binds tighter than `==` (C9) — silent wrong parse otherwise |
| D8 | `.mean` "just works" | `mean` = `sum / non-null count`, promoted to `Float64` | no Arrow mean kernel (C6) |
| D9 | `df.lazy` on any frame | plus `Prism.scan_parquet(path)` | `df.lazy` cannot push anything into a file it never read; the scan node is what makes pushdown real |
| D10 | "zero-copy slicing" for `filter`/`select` | Slices are zero-copy; `filter`/`sort`/`take` allocate new buffers | Arrow semantics: `filter`/`take` produce new arrays. Only `slice`/`project` are view-level. Documented honestly in `docs/architecture.md` |

## 8. Semantics

### 8.1 Nulls

- Arrow validity bitmaps are the only null representation. There is no NaN-as-
  null, no sentinel values, no per-column "missing" enum.
- Comparison kernels propagate nulls: `null > 5` is `null`, not `false`.
  `filter` drops null-predicate rows (Arrow kernel behaviour). `drop_nulls`
  and `fill_null` exist for explicit control.
- `group_by` groups null keys together; policy is `null_keys: :first` (default,
  matches Arrow's ASC NULLS FIRST), `:last`, or `:drop`.
- Aggregates skip nulls; a group whose values are all null yields a null
  aggregate value, never `0`.
- `count` counts non-null rows; `count_all` counts rows.
- `n_unique` is the exception: null counts as one distinct value (Polars'
  contract, so `n_unique == unique.size`), and an all-null column reports `1`.
  `first`/`last` and `median` still follow the skip-nulls rule.

### 8.2 Numeric coercion matrix

Applied by the expression engine before calling a kernel (C3). `Int` literals
default to `Int64`, `Float` literals to `Float64`.

| Left | Right | Result |
| --- | --- | --- |
| `Int8/16/32/64` | `Int8/16/32/64` | widest of the two |
| `UInt*` | `UInt*` | widest of the two |
| `UInt*` | `Int*` | `Int64` if the unsigned width is narrower, else `Int128` |
| integer | float | widest float (`Float32` widens to `Float32`; `Float64` wins) |
| decimal | decimal | higher precision/scale per Arrow rules |
| decimal | integer | decimal |
| decimal | float | `Float64` (documented precision loss) |
| anything | `Null` | the non-null side |

Before unifying, the engine checks whether one side is a literal. If it is, the
literal is cast to the column's dtype instead of the column being widened —
`col("i32") + 1` stays `Int32` and copies nothing. This mirrors Polars'
`modify_supertype`. The cast only happens when both dtypes are in the same
family, so `col("name") == 1` raises instead of comparing a string to "1".

The cast must also be **lossless**: a float literal in an integer column widens
the column instead (`col("i64") * 1.5` is `Float64`, not `Int64` with `1.5`
truncated to `1`), and a `Float64` literal does not narrow into a `Float32`
column. Without that guard the "cast the literal" rule silently changes values.

Decimal ⊕ decimal does **not** unify to comparison precision: Arrow's kernels
implement Hive's rules and derive the result dtype from both operands, so Prism
declares exactly what the kernel returns.

| Operation | Result scale | Result precision |
| --- | --- | --- |
| `+`, `-` | `max(s1, s2)` | `max(s1, s2) + max(p1 - s1, p2 - s2) + 1` |
| `*` | `s1 + s2` | `p1 + p2 + 1` |
| `/` | `s1 + 4` | `(s1 + 4 - s1 + s2) + p1` |
| `%` | `max(s1, s2)` | `max(s1, s2) + min(p1 - s1, p2 - s2)` |
| `-x` | `s1` | `p1` |

Precision caps at the kind's maximum (38 for `Decimal128`); a product whose scale
would exceed it raises `Prism::DtypeError` before any data is read. Integer
operands are promoted to scale 0 with their width's digit count (19 for `Int64`),
integer *literals* adopt the column's scale, and mixed decimal widths are
promoted to the wider storage kind (a lossless cast).

Non-numeric operands (`Utf8 + Int64`) raise `Prism::DtypeError` at
`dtype(schema)` time — before any data is touched.

### 8.3 Temporals

- Timestamps compare against `Time` literals; the `Lit` node converts `Time` to
  the column's `TimeUnit` and timezone.
- `date32`/`date64` compare against `Time::Span`-free `Time` values at UTC
  midnight; naive wall-clock comparison is documented as UTC-only for v0.1.
- Duration arithmetic follows `arrow-cr`'s `duration`/`timestamp` kernels.

### 8.4 Errors

```crystal
class Prism::Error < Exception; end
class Prism::ColumnNotFound < Prism::Error; end
class Prism::DuplicateColumn < Prism::Error; end
class Prism::MissingAlias < Prism::Error; end
class Prism::DtypeError < Prism::Error; end
class Prism::UnsupportedOperation < Prism::Error; end
```

Rules: raise `Prism::*` for dataframe-level misuse (unknown column, bad
argument combination, dtype mismatch we can detect. Let `Arrow::ArrowError` and
`Parquet::ParquetError` propagate untouched when the failure is genuinely in
that layer — they carry precise context and users may match on them. Every
raised `Prism::Error` names the offending column.

### 8.5 Deliberate hazards, documented

- **`Expr#==` returns an `Expr`, not a `Bool`.** This is required for
  `col("a") == 1`. Consequences: never use `expected == actual` in specs on
  `Expr` (any non-nil object is truthy, so `should eq` would pass vacuously),
  and never use `Expr` as a `Hash` key by `==` semantics. Specs use
  `Expr#same_as?` or compare `Expr#key` (see
  [docs/testing.md](../docs/testing.md#expressions)).
- **`&` precedence** (C9): parentheses are mandatory. `and`/`or` are the safe
  spelling.
- **`!` cannot be overloaded** (C14): `!expr` is always a `Bool`, so negation is
  `.not`.
- **`and`/`or` are Kleene**, not bitwise: null AND false is false. This matches
  SQL and Polars' logical operators, and differs from `Arrow.and`/`or`.
- **`Series#==`/`!=`/`<`/`<=`/`>`/`>=` are element-wise** and return a boolean
  series, so comparing two series with `==` is always truthy, and `series == nil`
  builds an all-null mask rather than testing for `nil`. Use `Series#same_as?`
  for structural comparison and `Series#is_null` for null tests, exactly as with
  `Expr`.
- **`Value` is a recursive alias**, so an *inferred* container of values is not
  always the same type as the alias: build tuples/arrays of values with an
  annotation (`[a, b] of Prism::Value`, `.as(Prism::Value)`), or Crystal will
  infer the one-level-expanded union and refuse the assignment. `to_a` and
  `Values.extract` return the alias itself, so ordinary iteration is unaffected.
- **Nested values are containers, not cells.** A `List` row is an `Array(Value)`
  and a `Struct`/`Map` row is a `Hash`, so `Value` can no longer be assumed
  scalar: arithmetic, `to_f64`, and aggregation kernels reject containers with a
  `Prism::DtypeError`, and display renders them as `[1, 2]` / `{x: 1, y: "a"}`.
- **Intervals lose their unit when materialized.** An interval row is an
  `Arrow::IntervalValue` (`months`, `days`, `nanos`) whatever the column's Arrow
  unit, so a year-month interval displays as months (`14 mons`) instead of
  Arrow's `1 years 2 mons`. Cast the column to `Utf8` for the unit-specific
  text; day-time values are nanoseconds by then (`millis × 1e6`).

## 9. Lazy execution and optimization

### 9.1 Plan nodes

```crystal
Prism::Plan::ScanParquet  # path, projection, predicate, batch_size, limit
Prism::Plan::ScanBatches  # in-memory Array(RecordBatch), projection, predicate
Prism::Plan::Filter       # Expr
Prism::Plan::Select       # Array(Expr) (aliased or bare column)
Prism::Plan::WithColumn   # name + Expr
Prism::Plan::GroupBy      # keys + Array({name, agg Expr})
Prism::Plan::Sort         # Array({Expr, descending, nulls_first})
Prism::Plan::Distinct     # keys
Prism::Plan::Join         # other plan, keys, how
Prism::Plan::Limit        # offset + n
```

Each node exposes `schema(upstream : Arrow::Schema) : Arrow::Schema` so the
whole plan is typed before execution.

### 9.2 Optimizer passes (in order)

1. **Predicate pushdown** — move `Filter` below `Select`/`WithColumn`, split on
   `&` into conjuncts, push each conjunct as far down as its dependencies
   allow. For `ScanParquet`, conjuncts whose columns are all in the scan's
   output become an `ArrowPredicateFn` on a `RowFilter`; the rest stay as a
   post-scan `Filter`.
2. **Projection pruning** — collect the columns each node actually needs
   (Polars also prunes unused aggregation expressions out of a `GroupBy` node)
   (`Expr#columns`), intersect down to the scan. For `ScanParquet`, emit
   `ProjectionMask.columns(parquet_schema, needed)`. `Join` attributes each
   output name to its side (keys always kept; a suffixed name pins the right
   column, and the shadowed left column is retained so the suffix stays valid),
   so both join inputs prune.
3. **Limit pushdown** — `Limit` merges into a scan as `with_limit` +
   `with_offset` when no intervening `Sort`/`GroupBy`/`Join` reorders or grows
   rows. A limit that meets `Sort`/`GroupBy`/`Distinct` is pinned to that node
   instead of staying above it (Polars' "limit 10 after the sort"); nested
   limits compose on the node.
4. **Constant folding** — evaluate literal-only subtrees at plan time.
5. **Dead column elimination** — drop `WithColumn` nodes nobody reads (after
   pruning), keeping order semantics if the removed expression is pure (it
   always is; expressions have no side effects).
6. **Common subexpression elimination** — Polars' CSEE counts occurrences and
   substitutes temp columns. Prism already memoizes duplicate subexpressions per
   chunk (§14); the plan-level version is deferred to this phase so `explain`
   shows the rewrite.
7. **Boolean simplification** — collapse literal predicates and identities
   (`x & false` → false). Already applied at eval time (§14); at plan level it
   lets a `Filter` with a constant false predicate become an empty scan.

`explain` prints the unoptimized and optimized plans side by side, including
which predicates and projections reached the scan — this is the observable proof
that pushdown happened, and the optimizer specs assert on it.

A frame becomes a plan through exactly one call per source: `df.lazy` for what is
already in memory, `Prism.scan_parquet` for a file, then verbs, then `collect`.
No verb is implicitly lazy, and no block form is involved — blocks in this
library are expression sugar (`filter { … }`), typed-schema accessors
(`typed.filter(&.active)`) and iteration, never a laziness switch.

### 9.3 Executor

A plan node yields an `Array(RecordBatch)` (the scan streams via `Iterator`;
`DataFrame` materializes per node). Grouped aggregation sorts once, then reduces
each aggregate across **all** ranges in one pass (`Prism::GroupedAgg`, Phase 15);
joins build a hash table on the right side keyed by each row's
`arrow-row` encoding (`Prism::RowKeys`), and the same encoding answers `distinct`
and `n_unique`.

## 10. Typed schemas

```crystal
Prism.define_schema UserSchema, id: Int64, name: String, active: Bool, salary: Float64
```

`define_schema` expands to a **struct** with:

- `UserSchema.schema : Arrow::Schema` — the Arrow schema, built once at load.
- per-field `def salary : Prism::Expr` returning `Col.new("salary")`.
- `UserSchema.from_arrow(batch) : Prism::TypedFrame(UserSchema)` and
  `from_dataframe(df)`.
- `Prism::TypedFrame(S)` verbs whose blocks receive an `S` instance, enabling
  `typed.select(&.salary)` and `typed.filter(&.active)`; a misspelt field is a
  compile error (`undefined method 'salry'`).
- `TypedFrame#salary : Arrow::PrimitiveArray(Float64)` for typed array access.

Type mapping: `Int8/16/32/64`, `UInt8/16/32/64`, `Float32/64`, `Bool`, `String`,
`Bytes`, `Time`, `Nil` (null column) → the matching `Arrow::DataType`. Anything
else is a compile error naming the supported set.

**Scope limit (see §4.1):** typing holds at the declaration boundary only.
`schema`-changing verbs on a `TypedFrame` return a plain `DataFrame`; call
`S.from_dataframe(...)` to re-enter the typed layer. Kotlin achieves
pipeline-wide refinement with a compiler plugin; Crystal macros cannot.

### Why `Values` returns containers instead of structs

A struct row materializes as `Hash(String, Value)` (field-ordered) rather than a
generated `struct`/`class`, and that is a boundary constraint, not an omission:

- **The schema is runtime data.** `Values.extract(array)` sees an
  `Arrow::DataType` it learns at run time. A generated struct would have to be one
  type *per nested schema*, so the return type of `extract` would become an open
  union (`Value | Point | Address | …`) — every consumer (`to_a`, display, joins,
  specs) would have to handle arbitrary user types, and `Values`, the single
  dtype-dispatch point (C4), would need a macro per schema. A closed alias is what
  keeps one representation for every frame.
- **`NamedTuple` is not an option either.** Its keys are compile-time
  literals, so
  a runtime field list cannot build one.
- **Containers match Polars.** `to_list`/`to_dicts` convert a struct to a
  dict, a
  map to a dict and a list to a list, so "struct row → `Hash`" is the same shape
  other tools hand back, and it keeps field *order* and per-level nulls.
- **What a struct would buy** — nominal typing (`row.x`), no string keys, fewer
  allocations — only exists where the schema is known at compile time. That is
  exactly the `define_schema` boundary, so the honest split is: dynamic
  boundary → containers; static boundary → typed accessors. A generated `S::Row`
  struct is therefore possible *for a declared schema*, with the same caveat as
  typed frames: it stops applying the moment a verb changes the schema.
  **Implemented in Phase 13** (`typed.rows`, `S::Row.from_hash`), alongside the
  dynamic `DataFrame#rows` the typed row mirrors; the runtime path still returns
  containers, exactly as argued here.

## 11. IO

- `Prism.read_parquet(path, columns:, batch_size:, predicate: nil) → DataFrame`
  — streams the reader, so memory stays bounded by `batch_size`.
- `Prism.scan_parquet(path) → LazyFrame` — no reads until `collect`.
- `df.write_parquet(path, compression: :zstd, row_group_size:, ...)`
  — writes each chunk with `ArrowWriter#write`, validates schema first (C10),
  always calls `#close` (a `begin/ensure` — a dropped writer loses the footer).
- `Prism.read_csv(path, infer_schema: true)` via `arrow-cr`'s CSV reader.
- `read_ipc` / `write_ipc` via `arrow-cr`'s IPC file/stream.
- `write_csv(path)`.
- In-memory variants (`read_parquet(io : IO)` / `write_parquet(io : IO)`) exist
  because tests and pipelines use `IO::Memory`.

## 12. Phases

Every phase ends with: gates green, specs added, this file's **Where we are**
block updated, one commit.

### Phase 0 — scaffolding (done)

- [x] `shard.yml` with sibling `arrow-cr` / `parquet-cr` path dependencies
- [x] Module renamed to `Prism`; baseline spec green
- [x] `CLAUDE.md`/`AGENTS.md`, `docs/`, `Makefile`, `.ameba.yml`, `.rumdl.toml`
- [x] Verified dependency API end to end (C1–C10 recorded above)

### Phase 1 — foundation (done)

- [x] `Prism::Error` hierarchy (§8.4), plus `SchemaError` for batch/schema
      disagreement
- [x] `Prism::Values` dtype dispatch (C4): one place that switches on
      `DataType::Kind`, null-aware via `[]?` (C11), unsupported dtypes raise
- [x] `Prism::Series`: name, dtype, size, count, null_count, `to_a`, `[]`/`[]?`,
      `sum`/`mean`/`min`/`max`, `to_arrow`
- [x] `Prism::DataFrame`: `from_arrow`, `from_batches`, `from_columns`,
      `schema`, `shape`, `column_names`, `dtypes`, `[]`/`[]?`, `to_arrow`,
      `each_batch`, `head`, `tail`, `slice`, `row_count`, `==`
- [x] Chunk invariants enforced at construction (schema equality, dtype
      agreement)
- [x] Zero-column and empty frames behave

**Acceptance:** a batch can round-trip `Arrow → DataFrame → Arrow` unchanged
(schema and values), including nullability and nulls. Specs:
`spec/prism/dataframe_spec.cr`, `spec/prism/series_spec.cr`,
`spec/prism/values_spec.cr` — 54 examples.

**Notes for later phases:**

- Single-value access on a series is `[]`/`[]?`, never Arrow's `value?` (C11).
- Aggregates reduce chunk-by-chunk and fold the partial scalars, so they never
  consolidate a column (C12). The zero-chunk spec pins this: `#to_arrow` raises
  for that shape, so an aggregate that rechunked could not return `nil`.
- A series over zero chunks cannot be materialized to a typed empty array; it
  raises `Prism::UnsupportedOperation` and points at `#to_a`. A zero-row frame
  produced by `head(0)`/`filter` keeps its chunks, so this only affects frames
  built from an empty batch list.

### Phase 2 — expressions (done)

- [x] AST nodes + `key`, `columns`, `same_as?`: `Col`, `Lit`, `Binary`, `Neg`,
      `Not`, `IsNull`, `IsNotNull`, `Cast`, `Alias`, `FillNull`, `Coalesce`
- [x] `dtype(schema)` for every node, including coercion (§8.2)
- [x] `eval(ctx)` for column, literal, arithmetic, comparison, boolean, cast
- [x] `Prism::DSL` with `col`, `lit`, and `Prism.col`/`Prism.lit` entry points
- [x] Operator overloads with the C9 caveat documented
- [x] `and`/`or`/`not` methods as the precedence-safe form
- [x] Null predicates and `fill_null`/`coalesce`

**Acceptance:** expressions evaluate identically to the equivalent Arrow kernel
calls, and `dtype` matches `eval(...).data_type` for every spec case (asserted
via the `checked` helper in `spec/prism/expr_spec.cr`).

**Notes carried forward:**

- Literals evaluate to `Arrow::Scalar`, not to a broadcast array, so
  `col("price") * 2` never builds an n-element array of 2s (C12 policy).
- Coercion prefers casting **the literal** to the column dtype when the cast is
  possible and both are in the same family, matching Polars' `modify_supertype`
  behaviour. Out-of-range literals therefore raise from the safe cast rather
  than widening the column; Polars folds them to a constant instead (open, §15).
- `and`/`or` use Arrow's **Kleene** kernels (`and_kleene`/`or_kleene`), matching
  SQL and Polars' logical operators: a known `false` wins over null in AND.
- Boolean negation is `.not`, because `!` is a Crystal pseudo-method and cannot
  be overloaded (C14).
- Arrow's `merge` compacts its inputs, so `fill_null`/`coalesce` use `zip`,
  which is row-aligned (C13).

### Phase 3 — eager verbs (done)

- [x] `select` (names, exprs, aliased exprs), `filter`, `with_column`,
      `with_columns`, `drop`, `rename`, `cast`, `sort_by`, `distinct`,
      `drop_nulls`, `fill_null`, `with_row_index`
- [x] Chunk-preserving execution for element-wise verbs (§5.5)
- [x] `sort_by` multi-key with per-key direction/null placement via
      `lexsort_to_indices` + `take_record_batch`, with `Expr#asc`/`#desc`
- [x] Column-name validation everywhere (`Prism::ColumnNotFound`)

**Acceptance:** each verb has a spec that checks values *and* schema, plus
chunk-count assertions: element-wise verbs keep their chunks, `sort_by` and
`distinct` collapse to one. Specs: `spec/prism/verbs_spec.cr` (39 examples).

**Notes carried forward:**

- `rename` cannot use `RecordBatch#with_schema`: it requires the new schema to
  be a superset, so a rename is rejected. Batches are rebuilt around the same
  arrays instead, which stays copy-free (C16).
- `Field#with_name` mutates in place, so rename copies the field first — otherwise
  it would rename the source frame's schema too (C16).
- `select` is a Crystal keyword (channel select). `df.select(...)` is fine,
  but a receiverless `select(...)` inside the class parses as the keyword;
  internal calls use `self.select` (C17).
- `distinct` keeps the *smallest original row index* per key group rather than
  trusting sort stability, then restores the original order (C18).
- `Series#unique` and `Series#n_unique` reuse the same machinery for a single
  column, and `n_unique` in `group_by` uses `Aggregates.count_distinct` instead
  of the slow `ArrayBase#uniq` (C18). Phase 9d replaced that machinery with
  `arrow-row` keys, so both are O(n) now.

### Phase 4 — group_by + aggregation (done)

- [x] `GroupBy` with multi-key grouping (C5)
- [x] Aggregate functions: `sum mean min max count count_all n_unique first last
      std variance median`
- [x] `null_keys` policy (§8.1): `:first`, `:last`, `:drop`
- [x] `maintain_order`: `true` (default) returns discovery order, `false` sorts
      by key
- [x] `aggregate(**named)` (C7) + Hash overload, and `agg(*aliased_exprs)`
- [x] Aggregation result schema derived from `Expr#dtype`, not from data

**Acceptance:** grouped results match a naive per-key reference implementation
over the same data, including all-null groups and empty input (zero rows in →
zero rows out, correct schema). Specs: `spec/prism/group_by_spec.cr`
(20 examples).

**Notes carried forward:**

- Grouping is **sort-based**, which is Polars' `GroupsType::Slice` path: keys are
  lexsorted, `Arrow.partition` yields contiguous ranges, and each range is a
  `RecordBatch#slice` view. No group is copied out and no index list is built.
  Polars hashes by default and sorts only when keys are already sorted; we always
  sort because it gives deterministic output order and view-based groups, at
  O(n log n). Phase 15 kept this shape and made it pay: contiguous ranges let one
  pass reduce every group, and only `n_unique`/`median`/`std`/`var` still walk
  per group.
- All aggregations in one `aggregate` call walk the same ranges, so several
  aggregates cost one sort, not one per aggregate — and since Phase 15 they also
  cost one pass each rather than one evaluation per group.
- `first`/`last` are a single `Arrow.take` of the group's edge rows (Phase 15 —
  before that they were one slice per group).
- `std`/`variance`/`median` use Arrow kernels on the group (`cast` to Float64,
  `sub`/`mul`/`sum` for variance, `sort` + middle row for median) rather than
  materializing Crystal values.
- `n_unique` counts **null as one value**, which is Polars' contract and makes
  `n_unique == unique.size`. It is the one aggregate that does not skip nulls
  (`sum`/`mean`/`min`/`max`/`count` do), and it therefore diverges from SQL's
  `COUNT(DISTINCT …)`; that was fixed in Phase 9d after checking upstream.
- Null keys form **one** group, not one per null row. Polars' wiki text is
  ambiguous on this; our behavior is pinned by a spec.
- `maintain_order` defaults to `true` (discovery order), matching dplyr/pandas
  intuition. Polars leaves order unspecified by default and offers
  `maintain_order: true` for the same result.

### Phase 5 — joins and concatenation (done)

- [x] `join` inner/left/right/full/semi/anti on one or more keys
- [x] `left_on`/`right_on` with differing names; `suffix` handling for collisions
- [x] `Prism.concat` (vertical, schema-compatible) and `hstack` (horizontal)
- [x] `:outer` accepted as an alias for `:full`; no `vstack` alias (would shadow
      `concat` for no benefit)

**Acceptance:** join cardinality and null-fill behaviour asserted per join type,
including duplicate keys (many-to-many) and empty inputs. Specs:
`spec/prism/join_spec.cr` (24 examples).

**Notes carried forward:**

- Hash join with the **right side as the build side**, matching Polars' model.
  Polars samples both sides and picks the smaller; we always build right, which
  keeps the output order = left order + unmatched right rows. Choosing the
  smaller side is a later optimization (it needs the output order reversed back).
- **Null keys never match** unless `nulls_equal: true`, matching Polars and SQL.
  `nulls_equal` applies to every join type.
- **Coalescing:** key columns are merged (right copy dropped) for inner/left/
  right, and kept apart for `full` unless `coalesce: true` — the same defaults as
  Polars, where `coalesce=None` coalesces except for full joins.
- **Column order** is left columns then right columns (suffixed where they
  collide), which is Polars' rule and is why a full join reads
  `[id, name, score, id_right, label]`.
- **Semi/anti are membership filters**: they emit left rows and left columns
  only, so repeated right keys cannot duplicate rows.
- Only the key columns are encoded (as `arrow-row` bytes, Phase 9d), so joins
  hash canonical byte rows instead of boxed values. Payload columns stay in
  Arrow and are gathered with `Arrow.take`.

### Phase 6 — IO (done)

- [x] `read_parquet` / `read_parquet(io)` with `columns`, `batch_size`, and
      `predicate` (projection and row-group pruning both pushed down)
- [x] `write_parquet` / `write_parquet(io)` with compression, row-group size,
      statistics, and `close` on the success path (C10)
- [x] `read_csv` / `write_csv` with schema inference (`infer_rows`), explicit
      schema, header and delimiter control
- [x] `read_ipc` / `write_ipc` in both file and stream formats
- [x] Round-trip specs through `IO::Memory`
- [x] File-based specs reading and writing under `temp/` (gitignored)

**Acceptance:** `write_parquet` → `read_parquet` preserves schema (including
nullability and timestamp units), values, and row count for a table with nils
and mixed column types. Specs: `spec/prism/io/{parquet,csv,ipc}_spec.cr`
(21 examples).

**Notes carried forward:**

- Defaults follow Polars, not the Parquet crate: **zstd level 3** and page
  statistics on. `:uncompressed`, `:snappy`, `:gzip`, `:brotli`, `:zstd`,
  `:lz4`, `:lz4_raw` are accepted.
- Projection pushdown goes through
  `ProjectionMask.columns(parquet_schema, names)`, and **predicate columns are
  added to the read set then dropped** afterwards, which is Polars' rule.
- `batch_size` decides the chunk count of the returned frame: the reader's
  batches become the frame's chunks, so a 5-row file with `batch_size: 2` yields
  3 chunks.
- **Row-group pruning is enabled**: `predicate:` is pushed through
  `with_row_filter`, evaluated per row group over only the columns it
  references. It was disabled while C20 was open and re-enabled once
  `parquet-cr` `5883ff1` landed.
- IPC writers use `ScopedWriter#open`, which finishes the writer on normal block
  completion, so the footer or end-marker is not something callers can forget.
- CSV `infer_rows` mirrors Polars' `infer_schema_length`; `0` types every column
  as string.

### Phase 7 — lazy + optimizer (done)

- [x] Plan nodes with schema propagation: `ScanBatches`, `ScanParquet`,
      `Filter`, `Select`, `WithColumn`, `GroupBy`, `Sort`, `Distinct`, `Join`,
      `Limit`, `Empty`
- [x] `collect`, `explain(optimized:)`
- [x] Optimizer passes: constant folding, predicate pushdown, projection
      pruning (with dead-column elimination), limit pushdown
- [x] Equivalence specs: lazy results match the eager pipeline for
      filter+select, group_by, sort+limit, distinct and join
- [x] Pushdown specs assert `explain` output: `predicate:`, `projection:`,
      `limit:`, and that a sort blocks a limit, a computed column blocks a
      predicate, and `&`-conjuncts split

**Acceptance:** a scan with `filter` + `select` + `head` produces a plan whose
scan node carries a projection, a predicate and a limit — asserted via `explain`
— and the result equals the eager pipeline's. Specs:
`spec/prism/lazy_spec.cr` (15 examples).

**Notes carried forward:**

- Nodes execute by delegating to the eager verbs, so there is exactly one
  implementation of each operation's semantics.
- `GroupBy` and `Join` derive their output schema by running the eager verb
  against zero-row frames. Joins recompute the suffix/coalesce layout otherwise,
  and this keeps the two paths in agreement.
- Projection pruning originally stopped at joins: mapping a suffixed output name
  back to the side that produced it was not implemented in this phase, so both
  join inputs kept full schemas. Phase 9d closed it.
- Limit pushdown crosses only `Select`/`WithColumn`. Polars additionally pins a
  slice *to* `Sort`/`GroupBy`/`Distinct` instead of pushing below them; Phase 9d
  added that (`Plan::Slice`).
- Plan-level CSE is deliberately absent: repeated subexpressions are already
  memoized per chunk (§14), so a plan rewrite would add complexity without a new
  saving.
- `read_parquet` grew `limit:`/`offset:` so the scan node can push a window.
- Keyword landmines hit again: `select` cannot be a parameter name, `Enumerable`
  cannot appear in an ivar union (join keys are normalized to `Array(String)`),
  and leaf `rebuild` must be the identity rather than raising.

### Phase 8 — typed schemas (done)

- [x] `define_schema` macro (C8: first parameter `schema`, and C21: verbs live
      outside the macro)
- [x] Typed frame (`S::Frame`) with block verbs and per-field `Series` accessors
- [x] Schema-changing verbs return a plain `DataFrame`; `S.from_dataframe` is the
      re-entry point (§4.1)
- [x] Compile-time safety proven by a `crystal build` spec that asserts a
      *failure* naming the field

**Acceptance:** the happy path compiles and runs; a misspelt field fails to
compile with a message naming it; `typed.with_column(...)` returns a
`DataFrame` and `S.from_dataframe` restores typed access. Specs:
`spec/prism/typed_spec.cr` (15 examples, 16 with `PRISM_COMPILE_SPECS=1`).

**Notes carried forward:**

- The generated frame is `UserSchema::Frame`, not `TypedFrame(UserSchema)` as
  §10 originally had it. A generic wrapper cannot carry per-field accessors, and
  the generated struct keeps `typeof` honest (`"UserSchema::Frame"`), which the
  specs assert.
- Shared verbs live in `Prism::TypedVerbs(S)`, included by the generated struct,
  because `def select` cannot be written inside a macro body (C21).
- `Typed.dtype_for` overloads map Crystal types to Arrow dtypes; the `forall T`
  fallback uses `{% raise %}`, so `id: Int65` is a compile error naming the type.
- `from_dataframe` validates every **declared** field (present and correct dtype)
  and accepts a superset of columns, so a typed view can wrap a wider frame.
- Per-field accessors return `Prism::Series` rather than a typed Arrow array:
  columns are chunked, and `Series` is this library's column type. Typed array
  access is `typed.salary.to_arrow`.
- The compile-failure spec is gated behind `PRISM_COMPILE_SPECS=1` because it
  shells out to the compiler; run it before a release.

### Phase 9 — parity, performance, display, release

Everything deferred during Phases 1–7 lands here, grouped so each item has an
owner phase rather than living in prose. §17 lists the same items with their
origin.

#### 9a. Display (done)

- [x] `DataFrame#to_s` / `#inspect` aligned table with a dtype row, head/tail
      truncation for wide and tall frames, and `null` rendering
- [x] `DataFrame#to_s(max_rows:, max_cols:, max_string:)` for explicit limits
- [x] `Series#to_s` summary line and `LazyFrame#explain` were already adequate
- [x] Optional styled CLI table through `lipgloss`
      (`require "prism/lipgloss"`, `DataFrame#to_styled`)

**Notes:** defaults follow Polars — 10 rows, 8 columns, 30 characters per string
cell, with `max/2` head and `max/2` tail plus an ellipsis row or column. The plan
originally said "dtype footer"; the dtype sits in a row under the header
(Polars' layout), which reads better than repeating every column name in a
footer. The core table is plain ASCII: `ljust` pads by bytes, so box-drawing
characters would misalign with any wide value.

**Styled output is opt-in.** `Prism::Styled` renders a bordered table through
`dsisnero/lipgloss`, sharing selection and formatting with the plain renderer
via `Display.grid`. It is a separate require (`require "prism/lipgloss"`) and
`lipgloss` is only a *development* dependency here, so consumers who want boxes
add it to their own `shard.yml`. `to_s`/`inspect` stay plain on purpose: logs,
pipes and specs must not receive ANSI escapes. `Table#headers` takes a splat
only, so the call is macro-generated for up to 16 columns; wider tables fall back
to a header row in the body.

#### 9b. API completeness (done)

- [x] `LazyFrame` verb parity with eager: `drop`, `rename`, `cast`,
      `fill_null`, `drop_nulls`, `with_columns`, `tail`, `slice(offset, count)`
- [x] `Series`: `is_null`, `fill_null`, `cast`, element-wise `==`, and
      `same_as?` for structural comparison (§6.2)
- [ ] Plan-level CSE — still not implemented, deliberately: per-chunk
      memoization (§14) already removes the recompute, and Phase 9d decides with
      benchmarks whether a plan rewrite buys anything

**Notes:** most lazy verbs are built from existing nodes rather than new ones —
`drop` and `rename` become a `Select` (rename uses `Alias`), `cast` and
`fill_null` become `WithColumn` per column, and `drop_nulls` becomes a `Filter`.
Only `tail` needed a node, because a trailing window cannot be pushed into a
scan: the row count is unknown until the source runs.

`Series#==` is element-wise and returns a boolean series, so comparing two
series with `==` is always truthy — `same_as?` exists for structural comparison
(§8.5).

This phase also fixed a real bug: `select` marked every aliased column nullable,
so `rename` through the planner produced a different schema from the eager
verb. Aliasing a bare column now keeps its nullability (copying the field first,
C16).

#### 9c. Expression coverage (done, two deferrals)

- [x] Decimal **comparison**: decimals unify at the wider scale with
      `Decimal128(38, s)`, and `Prism::Values` materializes them as
      `BigDecimal` so the scale survives (the raw scaled integer would drop it)
- [x] Out-of-range literal folds to a constant boolean, as Polars does
- [x] Dictionary arrays materialize as their decoded values
- [x] Decimal **arithmetic**: `Coerce.arithmetic_operands` keeps each side's
      precision and scale, `Coerce.arithmetic_dtype` applies Arrow's Hive rules
      (the same rules the kernels implement, so `dtype` equals `eval`'s dtype),
      integers promote to scale 0, floats fall back to `Float64`, and a scale
      overflow raises at planning time. Done post-v0.1; see
      `spec/prism/decimal_arithmetic_spec.cr`.
- [x] Nested (`list`/`struct`/`map`) materialization: `Value` gained
      `Array(Value) | Hash(String, Value) | Hash(Value, Value)`, which is a
      recursive alias **through containers** and is legal in Crystal (`JSON::Any`
      is built the same way — the earlier claim in §17 that aliases cannot
      recurse was wrong, and this needed no new value type). Done post-v0.1; see
      `spec/prism/nested_spec.cr`.

**Notes:** the fold is Polars' rewrite
`when(col.is_not_null()).then(constant).otherwise(null)`. It matters because
`arrow-cr`'s *safe* cast turns an overflowing literal into null, so without it
`Int32 col > 3_000_000_000` quietly compares null on every row. Arithmetic with
such a literal raises `Prism::DtypeError` instead, rather than nulling silently.

`Value` gained `BigDecimal`, which is why the "unsupported dtype" spec now uses
a struct array: decimals materialize instead of raising.

#### 9d. Performance

- [x] Projection pruning through joins: map suffixed output names back to the
      side that produced them
- [x] Slice attached to `Sort`/`GroupBy`/`Distinct` as Polars does (`Plan::Slice`),
      with nested limits composing instead of stacking, and `DataFrame#sort_by_slice`
      gathering only the window's indices
- [x] Benchmarks: `filter`, `select`, `group_by` on 1e6 rows, Parquet read/write
      with and without pushdown — `make bench`, results in
      [`docs/benchmarks.md`](../docs/benchmarks.md) (§14)
- [x] Window gather for a pinned slice: `DataFrame#sort_by_slice` collects only
      the window's indices, so `sort_by + head(10)` no longer copies every row
      (229 ms → ≈160 ms per the benchmarks). A bounded-heap top-k stays an
      `arrow-cr` kernel, not a Prism change.
- [x] Switch join/distinct keys to `arrow-row` (`Prism::RowKeys`): joins hash
      canonical byte rows from `Arrow::RowConverter` instead of boxed values,
      `distinct`/`unique`/`n_unique` became O(n), and float keys are
      canonicalized (`-0.0`/`0.0` one key, every NaN one key) so keying follows
      `==` and `group_by` agrees (C18, §14)

#### 9e. Release

- [x] `examples/` for the README workflows: `quickstart.cr` (expressions,
      `group_by`, join), `parquet.cr` (write/read, projection + predicate
      pushdown, `explain`) and `viewer.cr`, an interactive table browser on
      `dsisnero/bubbletea` + `dsisnero/bubbles` (table viewport) with
      `dsisnero/ultraviolet` — the TUI counterpart to `to_styled`. All three are
      development-dependency only, and compile-or-run checked by
      `spec/examples_spec.cr` (`PRISM_EXAMPLE_SPECS=1`).
- [x] Memory notes: `#to_arrow` copy cost documented in
      [docs/architecture.md](../docs/architecture.md#memory-what-to_arrow-costs),
      including what is free (one chunk, zero rows), what copies (one chunked
      concatenation), and how to avoid it (`each_batch`, larger `batch_size`).
- [x] Property checks: `spec/prism/property_spec.cr` compares `distinct`,
      `group_by` (one and two keys), `join` (inner/left/semi/anti), `sort_by` on
      nullable floats, float keys, lazy-vs-eager and chunked-vs-single-batch
      against hand-written references over seeded random frames
- [x] API cleanup: removed `Prism.scan(frame)`; `df.lazy` is the only in-memory
      lazy entry (one per source, matching Polars' `df.lazy()` / `pl.scan_*`),
      with all 22 call sites migrated and the rule recorded in §4 and §9
- [x] v0.1.0 tag — published on `dsisnero/prism` after the opt-in suites went
      green (340 examples at the time; a version-consistency spec followed)
- [x] v0.1.1 tag — the two post-0.1.0 features (decimal arithmetic, nested
      materialization) released on `2026-11-02` after
      `PRISM_COMPILE_SPECS=1 PRISM_EXAMPLE_SPECS=1 make test` went green (365),
      and verified as a consumer: a fresh project with
      `prism: github: dsisnero/prism` resolves `0.1.1` and runs `group_by`
- [ ] v0.2.0 tag — **prepared, not tagged** (owner says when). Phases 12–18 are
      release-ready as of `2026-11-03` after
      `PRISM_COMPILE_SPECS=1 PRISM_EXAMPLE_SPECS=1 make test` went green (457;
      452 without the opt-in suites). `shard.yml` and `Prism::VERSION` both say
      `0.2.0` and the spec suite checks them against each other, so the release
      prep bumped one number instead of three — the literal in `spec/prism_spec.cr`
      that used to repeat it now asserts the shape only. The benchmark table was
      refreshed against `arrow-cr` `728ce51` / `parquet-cr` `25a889e` in Phase 18,
      which is the same run that produced the bounded-sort A/B. Consumer check
      (`prism: github: dsisnero/prism` resolves `0.2.0`) follows the push.

**Notes:** the examples are run, not eyeballed: `spec/examples_spec.cr` compiles
and executes each one and asserts the summary lines it prints (`rows: 5, columns:
4`, `rows read: 5`, `projection: [region, qty]`), so a README snippet that stops
working fails a spec. Compiling one example costs a full dependency build, hence
the `PRISM_EXAMPLE_SPECS=1` gate, matching `PRISM_COMPILE_SPECS`.

The viewer needed a dependency change: `lipgloss` moved from `path: ../lipgloss`
to `github: dsisnero/lipgloss` because `bubbletea` depends on it and shards
refuses one shard name from two sources (the sibling checkout is at the same
commit, `cdf2115`). `bubbletea` and `bubbles` are development dependencies, so a
consumer installs none of it.

### Phase 10 — repository rename (done)

- [x] `shard.yml` `name: prism`; `src/dataframe.cr` → `src/prism.cr`
- [x] `spec/spec_helper.cr` require path; README/docs `require "prism"`
- [x] GitHub repository is `dsisnero/prism` (created at v0.1.0)
- [x] `shard.yml` dependencies switched from `path:` to `github:` so
      `shards install` works for consumers, and the README install block uses the
      published form
- [x] The sibling `parquet-cr` had to stop pointing at `path: ../arrow-cr` too:
      shards refuses one shard name from two sources, so a transitive path
      dependency made the whole graph unresolvable (fixed in `parquet-cr`
      `25a889e`)
- [ ] Local checkout folder `dataframe/` → `prism/` — cosmetic, and moving the
      working tree mid-session buys nothing. Do it whenever convenient; nothing
      in the repo references the folder name.

### Phase 12 — RedAmber ergonomics (done)

The RedAmber-derived surface from §17, landed after `v0.1.1`. Every item is sugar
over existing verbs; none adds a new execution path.

- [x] `Series` comparison operators (`== != < <= > >=`, series or scalar), with
      the column-vs-literal dtype rule factored out of `Coerce#target` into
      `Coerce.with_literal` so `series > 10` coerces exactly like
      `col("x") > 10`
- [x] `DataFrame#filter(mask : Series)` — boolean masks keep the same three fast
      paths and the chunking when the mask comes from the frame's own columns
- [x] `DataFrame#remove(expr)` / `#remove(mask)` and `LazyFrame#remove(expr)`:
      keep rows where the predicate is false **or null**, which is not
      `filter(pred.not)`
- [x] `sort_by("-col")` descending shorthand through `Prism.sort_key`, shared by
      eager and lazy verbs
- [x] `slice(range)` plus `df[range]` and `df[[names]]`: negative and omitted
      endpoints, `...` exclusive, bounds clamp instead of returning `nil`
- [x] One-call `GroupBy` aggregates — `sum mean min max count n_unique median std
      var` — named columns or every eligible column, input names kept, keys never
      aggregated (`Prism::DuplicateColumn` if asked explicitly)

**Acceptance:** each shorthand produces the rows the long form produces: mask
filtering and `remove` are compared against `filter` on frames with nulls and
across chunk boundaries, range slicing is compared against `slice(offset,
length)`, `-col` against `order: :desc`, and one-call aggregates against the
equivalent `aggregate(...)`. Specs: `spec/prism/{verbs,dataframe,group_by,
series,lazy}_spec.cr` (405 examples total).

**Notes carried forward:**

- A **bare `Range` restriction is not more specific than `Enumerable(T)`** (C22),
  so `def [](range : Range)` silently loses dispatch to the column-list overload
  and `df[1..3]` is read as column names. The row overload must declare its type
  parameters: `def [](range : Range(B, E)) : DataFrame forall B, E`.
- A **type-restricted splat requires at least one argument** (C23), so
  `def sum(*columns : String | Symbol)` cannot be called as `sum`. The empty form
  is a separate overload, generated by a macro with the no-argument twin.
- `Enumerable(T)` is invariant in `T`, so `Array(String)` does not satisfy
  `Enumerable(String | Symbol)`. The column-list overload is
  `Enumerable(T) forall T` and maps each name through `Prism.col`, which is where
  a non-name type becomes a compile error.
- `select` is a keyword (C17) and bites inside `[]` too: the projection overload
  spells `self.select(...)`.
- Preferring a `Series` overload of `filter` means the block form must stay
  overload-free (`filter(&)`), because the block's return type decides between
  the expression and mask paths.

### Phase 13 — Row materialization (done)

The §17 "typed row structs" candidate, plus the dynamic row API it is the typed
counterpart of. Rows are an edge (§14), so nothing here changes execution: it is
the `Value` shape that `Series#to_a` already produces, one row at a time.

- [x] `DataFrame#each_row` / `#rows` / `#row` — the dynamic boundary: one
      `Prism::RowHash` (`Hash(String, Value)`) per row, columns materialized once
      per batch, values following `Value` (nulls `nil`, nested cells containers,
      timestamps `Time`), `IndexError` outside the frame
- [x] `Prism::RowHash` alias names the dynamic row type, so a hand-built row can
      be annotated without repeating the `Value` union
- [x] `define_schema` generates `S::Row`: typed nullable fields, positional
      `initialize`, `to_h` back to `RowHash`, `S::Row.from_hash`, and
      `S::Frame#each_row` / `#rows` / `#row`; field-wise `==`/`hash` come from
      Crystal's struct defaults, so a row is usable as a `Hash`/`Set` key
- [x] Typed rows decode every declared dtype: `Int`/`UInt` widths, `Float32/64`,
      `Bool`, `String`, `Bytes`, `Time` (microsecond timestamp)

**Acceptance:** the same frame of values, reached both ways, yields the same row:
`typed.rows` equals `typed.row(i)`, `Row#to_h` equals `DataFrame#row(i)`, rows
stream in frame order across chunk boundaries, nulls stay `nil` in typed fields,
and `row` raises `IndexError` out of range. Empty, zero-column and zero-chunk
frames are covered. Specs: `spec/prism/dataframe_spec.cr` (37 examples, 7 new)
and `spec/prism/typed_spec.cr` (25 examples, 10 new); the suite is 422.

**Notes carried forward:**

- **Two row shapes, one rule.** The dynamic row is a `Hash(String, Value)`; the
  nominal row exists only where the schema is compile-time data. That is §10's
  boundary restated at row granularity: `Values.extract` learns its dtype at run
  time and must return one closed type, while `S::Row` is generated per
  declaration.
- **Typed fields are nullable** (`T?`) because `define_schema` declares every
  Arrow field nullable, so a decoded row must be able to say "null". A non-null
  declaration would need either a schema-level `nullable:` option or a runtime
  check; both are out of scope here.
- **`each_row` extracts each column once per batch**, then indexes into those
  arrays — no per-cell dtype dispatch. `S::Frame#row(i)` delegates to
  `DataFrame#row(i)` and re-types the hash through `S::Row.from_hash`, so the two
  views cannot disagree about order, nulls or bounds.
- **A literal partial hash is a compile error.** `S::Row.from_hash({"id" => 1_i64})`
  infers `Hash(String, Int64)` and fails where a field's cast cannot apply, which
  is the right outcome (a row must cover every declared field) but reads oddly;
  the runtime `Prism::DtypeError` for a missing field only fires for a hash
  already typed as `Prism::RowHash`. Build the hash with the alias when testing
  that path.
- **`Row` needs no generated `==`/`hash`.** Crystal already compares and hashes
  structs field by field (verified with heap strings and `Bytes`, not just
  literals), so emitting them would only restate an inherited default. A spec
  pins the behaviour — equality, `uniq` and hashing — because the row API
  promises rows work as `Hash`/`Set` keys.
- **`DataFrame#row` takes no negative index**, matching `Series#[]`; Ruby-style
  negative positions live in `#slice(range)` only.

### Phase 14 — Value coverage: intervals (done)

Closes the two gaps the Phase 13 resume block named: interval columns could not
materialize, and a dictionary-encoded float key was compared by value but not
canonicalized.

- [x] `Value` gains `Arrow::IntervalValue`, so `Values.extract`/`extract_at`,
      `Series#to_a`/`#[]` and `DataFrame#rows`/`#row`/`#each_row` all handle
      interval columns (a `define_schema` schema still cannot *declare* one:
      an interval dtype needs a unit, which the macro has no syntax for)
- [x] All three Arrow interval units decode to the same shape: year-month keeps
      its months, day-time converts its milliseconds to nanoseconds, and
      month-day-nano is already `(months, days, nanos)`; packing goes through
      `Arrow.interval_*` rather than a Prism copy
- [x] Display renders intervals in Arrow's `IntervalMonthDayNano` style
      (`1 mons 2 days 0.000000003 secs`, `2 hours 2 mins`, `0 secs`)
- [x] `RowKeys.canonical` canonicalizes a dictionary's float *values* and keeps
      the keys and dtype, so `distinct`, `unique`, `n_unique`, joins and
      `group_by` agree on `-0.0`/`0.0` and NaN payloads for dictionary-encoded
      float keys too

**Acceptance:** every interval unit extracts consistently (`extract` equals
`extract_at` per index) with nulls intact; display and `rows` agree on the
canonical text; a dictionary-encoded float column yields 2 distinct values for
`[0.0, -0.0, NaN, NaN]` instead of 3, keeps its dtype through `Series#unique`,
and groups like the equivalent plain float column. Specs:
`spec/prism/{values,display,dataframe,distinct,group_by}_spec.cr`; the suite is
429.

**Notes carried forward:**

- **One interval shape, no unit.** `Arrow::IntervalValue` is `(months, days,
  nanos)`, so the original interval unit is not part of a materialized value: a
  year-month interval of 14 months renders `14 mons`, not Arrow's
  `1 years 2 mons`. That is the price of one closed `Value` alias for every
  frame (§10 "Why `Values` returns containers"); `Series#cast` to `Utf8` gives
  the unit-specific Arrow text. A per-unit value type would make a row's shape
  depend on the column's dtype, which is exactly what the alias avoids.
- **Day-time intervals become nanoseconds** because that is the narrower
  representation: every millisecond value is exactly representable as
  nanoseconds in `Int64`, so the conversion loses nothing and keeps the three
  units comparable.
- **`Value#to_f64` still rejects intervals**, and no arithmetic kernel accepts
  them. Interval arithmetic (the `arrow-cr` kernels for adding intervals to
  timestamps) is not exposed by Prism yet; materialization is a row-edge
  feature, not a numeric one.
- **Dictionary float keys canonicalize values, not keys.** A rebuild keeps the
  original keys, the key type and `ordered?`, so `Series#unique` can gather rows
  from the canonical array without the dtype shifting under it.
- **Floats nested in list/struct/map keys are still total-order.** Reaching a
  float child means rebuilding the parent layout; the gap is documented in
  `Prism::RowKeys` and in `docs/architecture.md` rather than silently fixed for
  one container type.

### Phase 15 — Vectorized grouped aggregation (done)

The §17 candidate the Phase 12 follow-up measured and could not justify
touching until the numbers said where the time went: ~95% of a 1e5-key
`group_by` was per-group overhead, not the sort.

- [x] `Prism::GroupedAgg` reduces one operand across **every** group in one pass
      instead of evaluating the aggregate expression per group: the operand is
      evaluated once over the whole sorted batch (and shared between aggregates
      that name it), and the result is one typed array
- [x] Vectorized tier: `count` (validity-bitmap popcount per range, as Polars'
      `agg_valid_count` does), `count_all` (range lengths), `first`/`last` (one
      `Arrow.take` of the boundary rows), `sum`/`mean` (one fold per group,
      wrapping integers and IEEE floats), `min`/`max` (one fold per group, IEEE
      total order for floats, `bool_and`/`bool_or` for booleans, byte order for
      strings)
- [x] `n_unique`/`std`/`variance`/`median` keep the per-group Arrow kernel but
      start from the already-evaluated operand and build one result array, so no
      expression is re-evaluated and no `Arrow.concat` happens
- [x] A nested aggregation (`col("a").sum.mean`) and any dtype `Series#min` itself
      rejects keep the original per-group evaluation, so error messages and
      undefined shapes are unchanged
- [x] `GroupBy#execute` no longer keeps `Arrow.concat`-of-pieces on the common
      path; `concatenate_groups` survives only as that last resort

**Acceptance:** `make bench` with the same harness before and after: on a
1e6-row/1e5-group frame, `count` 419 → 235 ms, one `sum` 326 → 235 ms, three
aggregates 544 → 237 ms, `n_unique` 469 → 329 ms. One aggregate and three cost
the same, which is the whole point — the aggregate layer is no longer where a
group-by spends its time. Results are unchanged: the full suite is green and
`group_by_spec.cr` gained parity specs for wrapping integer sums, IEEE total-order
float extremes, boolean extremes, null-at-the-edge `first`/`last`, all-null
groups, and a counting expression that proves the operand is evaluated **once**
for all groups (shared by `sum` and `count` over the same operand).
`property_spec.cr` adds a per-key oracle for `first`/`last`/`count`/`count_all`.
Suite: 436 examples.

**Notes carried forward:**

- **Contiguous ranges are what make this possible**, and that is the strongest
  argument yet for sort-based grouping: `first`/`last` collapse to one `take`,
  `count` to a popcount, and each numeric fold to one pass. A hash grouping
  (`GroupsType::Idx`) would need a gather per group — the exact cost this phase
  deleted. Polars draws the same line: its `GroupsType::Slice` path is the fast
  one, and its per-group `_agg_helper_slice` fallback preallocates the output,
  which is the shape the remaining per-group kernels here use.
- **`count` reads validity bits, not rows.** `ArrayBase#count` is
  `len - logical_null_count`, so counting valid bits in the group's sliced
  validity buffer gives the same answer with one bit operation per eight rows.
  Using `logical_nulls` (not `nulls`) keeps dictionary arrays agreeing with
  `Series#count`.
- **Empty and all-null groups are pinned.** `first`/`last` return the group's
  edge row even when it is null (they are views, not "first non-null"), and an
  all-null group's `sum`/`min`/`max`/`mean` is null, not zero.
- **The remaining cost is the sort and the full-width gather**, not the
  aggregation, which is what moves column pruning in eager `group_by` to the top
  of the next-up list.

### Phase 25 — Conditional expressions (done)

The one expression family the library was missing: derived columns could be
arithmetic, comparisons, `fill_null` or `coalesce`, but there was no per-row
choice, so "label these rows" needed a `with_column` per case or a mask filter.

- [x] `Prism.if_else(condition, then_value, else_value)` — one node, `IfElse`,
      evaluated with `Arrow.zip`
- [x] `Prism.case_when([{cond, value}, ...], otherwise)` — folded into nested
      `if_else` calls, first match wins, so it reads like SQL `CASE` or Polars'
      `when().then().when().then().otherwise()`
- [x] Non-expression arguments are wrapped with `Prism.lit`, so both branches can
      be plain values; the `Prism::DSL` mixin exposes both functions
- [x] Branch dtypes coerce through `Coerce.target`, the rule arithmetic uses: a
      literal adopts the column's dtype when the cast is lossless
      (`if_else(cond, int32_col, 7)` stays `Int32`), and mismatched families widen
      (`if_else(cond, int_col, 1.5)` runs in `Float64` instead of truncating)
- [x] `if_else(cond, a, b).to_s` is its canonical key, and `Coalesce` gained the
      same `to_s` — it printed an object address until this phase, which
      contradicted "expressions are values"

**Acceptance:** a null condition takes the else branch, pinned from both sides —
`Arrow.zip` treats a null mask as false (verified directly: mask
`[true, false, nil]` over `[1,2,3]`/`[10,20,30]` returns `[1, 20, 30]`) and
Polars' `when/then/otherwise` does the same, so `case_when` means "first branch
that matches" without a special null rule. Specs cover per-row selection, null
conditions, nulls inside the selected branch, scalar branches, dtype coercion
(`Int64`+`Float64` widens, `Int32`+`Int64` literal stays `Int32`), use inside
`filter`/`select`, nesting, two
chunks, the lazy plan, `key`/`columns`/`dtype` without data, and the error paths
(non-boolean condition, branches with no common dtype, unknown column).
`spec/prism/conditional_spec.cr` plus the `Coalesce` printing spec. Suite: 498
examples.

**Notes carried forward:**

- **The kernel already had the semantics.** `Arrow.zip`'s null-mask rule is
  Polars' rule, so the conditional needed no divergence note — worth checking
  before writing one, since the intuition ("null condition → null result", which
  is what R's `if_else` does) is wrong for both Arrow and Polars.
- **Nesting is the implementation, not a second semantic.** `case_when` is
  `reverse_each` over `if_else`, so the two can never drift; a null condition
  falls through because the inner call takes the else branch.
- **`filter` needs no null special case.** `if_else(...) > 5` on a row whose
  selected branch is null yields a null comparison, and `filter` already drops
  those — the same rule as any other expression.

## 13. Testing strategy

- Spec layout mirrors `src/prism/`. See
  [docs/testing.md](../docs/testing.md) for the full policy.
- **Compile cost matters:** requiring `arrow-cr` + `parquet-cr` costs ~20 s per
  build. Prefer one spec run over many; use `crystal spec spec/prism/x_spec.cr`
  while iterating.
- No fixture blobs in git: generate Parquet/CSV/IPC data in `temp/` at spec time
  via writers, then read it back.
- Property-style checks where cheap: `dtype` vs `eval` agreement, `sort_by`
  stability, and the verb oracles in `spec/prism/property_spec.cr` (see
  [docs/testing.md](../docs/testing.md#property-checks)). They are seeded, so a
  failure reproduces exactly, and they were mutation-checked: disabling
  `RowKeys.canonical` fails only the float-key checks.
- Optimization specs assert plan shape via `explain`, not just results, so a
  silently disabled pass fails CI.

## 14. Zero-copy policy and performance

The rule: **data stays in Arrow buffers until an edge forces materialization.**
Kernels are Arrow's; this library's contribution is not adding copies.

### The edges

These are the only places allowed to allocate or materialize:

| Edge | Why it allocates |
| --- | --- |
| `Series#to_a`, `Series#each`, `Series#[]` | Crystal values are not Arrow buffers |
| `DataFrame#rows`, `#row`, `#each_row` | one hash (or `S::Row`) per row |
| `Series#to_arrow`, `DataFrame#to_arrow` | one contiguous array; concat when chunked |
| `DataFrame#to_s`, display | formatting needs per-value strings |
| Parquet/CSV/IPC writers | encoding walks values |
| `filter`, `sort_by`, `group_by`, `join` | the output is a different row set (Arrow semantics) |

### What never allocates

- `DataFrame#[]` — a series is a list of references to the batch columns.
- `DataFrame#slice`, `#head`, `#tail` — `RecordBatch#slice` is a view (offset and
  length only). `head(n)`/`tail(n)` return `self` when `n` covers the frame.
- `select` / projection — `RecordBatch#project` reuses the arrays.
- `Series#size`, `#count`, `#null_count` — summed across chunk headers.
- `Series#sum`, `#mean`, `#min`, `#max` — per-chunk kernel, then fold scalars.
- `Series#each_chunk`, `DataFrame#each_batch` — streaming access to what exists.

### What the references confirmed

- **arrow-rs** has no `ChunkedArray`; `Vec<ArrayRef>` is the sanctioned model
  (C12).
- **Polars** keeps `Vec<ArrayRef>` in `ChunkedArray` and rechunks only for
  random access or SIMD work; aggregations run per chunk.
- **Arquero** goes furthest on the read side: `filter` returns a `BitSet` view
  and columns are only copied when a table is reified.
- **Kotlin DataFrame** is column-based and reuses storage across its immutable
  transforms.
- **Polars casts the literal, not the column**, in column-vs-literal binary
  operations (`modify_supertype`), which is what §8.2 specifies.
- **Polars refuses string/numeric comparison at planning time** rather than
  casting implicitly; `same_family?` reproduces that.
- **Polars' logical AND/OR are Kleene**, so `and`/`or` use Arrow's Kleene
  kernels rather than the plain ones.

### Expression execution

Polars converts an `Expr` into an arena IR, optimizes it, and compiles a physical
expression that is **reused for every chunk**. Prism keeps the AST but reproduces
the effect with two per-execution caches:

| Mechanism | Polars equivalent | Prism |
| --- | --- | --- |
| Resolve dtypes/coercion once | compiled `PhysicalExpr` | `Prism::TypeCache`, built once per verb call and shared by every chunk's `EvalContext` |
| Evaluate a repeated subtree once | CSEE (counts occurrences, substitutes temp columns) | `Prism::Cse` counts canonical keys, `EvalContext#evaluate` memoizes only those keys, per chunk |

Both are observable in specs: `spec/prism/expression_plan_spec.cr` uses counting
test nodes to assert a repeated subexpression evaluates once per chunk and that
dtype resolution does not grow with chunk count.

### Filter fast paths

Polars' `filter_with_bitmap` has three cases; `DataFrame#filter` now matches
them:

| Mask | Polars | Prism |
| --- | --- | --- |
| all true | return the array as-is | return the batch untouched (no copy) |
| all false | empty array | `batch.slice(0, 0)` (a view; kernel skipped) |
| all null | values AND validity | falls out of Arrow: `true_count` is `(values & validity).count_set_bits`, so null rows never count as matches |

Boolean identities are applied before evaluation: `x & false` and `x | true`
return a constant **bitmap** (n/8 bytes, not n) without evaluating `x`; `x & true`
and `x | false` return `x` untouched. This is Polars' boolean-simplification
rule, applied at eval time because Prism has no plan pass yet.

### How Arrow represents nulls (and what we get for free)

- Nulls are a validity bitmap: `NullBuffer` wrapping a `BooleanBuffer`, where a
  set bit means valid. `null_count` is cached on the buffer.
- `BooleanArray#true_count` is `(nulls.inner & values).count_set_bits` — a bitmap
  AND plus popcount, so the filter fast-path check is cheap.
- `Arrow.filter` already treats a null mask bit as false (it ANDs validity into
  the mask), which is the all-null case above.
- Constant boolean columns cost n/8 bytes, so the fast paths allocate almost
  nothing.

Verified in `lib/arrow-cr/src/arrow/{null_buffer,boolean_buffer,boolean_array}.cr`;
no bitmap-level work is needed in Prism beyond what these fast paths use.

### Where we deliberately differ from Polars

Polars' eager API internally builds a `LazyFrame`, optimizes, and collects. Prism
keeps eager verbs as direct kernel calls for now: a one-verb call has nothing to
optimize, and the shared pieces (TypeCache, CSE, the expression AST) are the same
ones the Phase 7 executor will use. Revisit when `LazyFrame` exists — routing
eager verbs through it would give uniform predicate pushdown.

### Native row encoding for keys

Implemented in Phase 9d. `Prism::RowKeys` is the only place Prism touches
`Arrow::RowConverter`, and it closes the divergences this section used to list:

| Divergence | Before | Now |
| --- | --- | --- |
| Join keys | one `Array(Value)` per row, boxed | one canonical row view per row, hashed as bytes |
| `distinct` / `unique` | O(n log n) sort + partition | O(n) seen-set |
| `n_unique` | O(n log n) | O(n) |
| NaN / signed-zero keys | Crystal `==`, so NaN never matched NaN | canonicalized floats: every NaN is one key, `-0.0` joins `0.0` (Polars `canonical_f64`) |

Why this shape:

- **Encoding, not hashing.** The Arrow specification covers memory layout and
  IPC, not hashing, so no Arrow implementation ships a hash function. What
  `arrow-row` provides is a canonical byte encoding built for `memcmp`; hashing
  those bytes is then Crystal's job, and `Bytes`/`Row` hash by content.
- **Floats are canonicalized, not hashed as bits.** `arrow-row` stores IEEE
  `totalOrder`, which is right for sorting and wrong for keys: it would make
  `-0.0` a different key from `0.0` and NaNs differ by payload. This is a real
  split between the two row formats — **Polars' own `polars-row` bakes
  canonicalization into the encoding** (`-inf < neg < -0.0 = 0.0 < pos < inf <
  nan`, all NaNs equal), while `arrow-row` cannot, because its bytes also have to
  express a sort order. So Prism canonicalizes the key columns first
  (`RowKeys.canonical`: `x + 0.0`, then one quiet NaN, per `canonical_f16/f32/f64`)
  and gets `polars-row` *key* equality out of `arrow-row`. Verified against
  upstream: Polars hashes `v.to_total_ord()`, and `TotalHash for f64` hashes
  `canonical_f64(x).to_bits()`, so `group_by`, join and
  `unique`/`n_unique` all agree on `-0.0`/`0.0` and NaN payloads.
  `spec/prism/{join,distinct,group_by}_spec.cr` pin it.
- **Dictionary keys are canonicalized through their values** (Phase 14).
  `arrow-row` encodes a dictionary column through its value array, so
  canonicalizing the *values* is what makes a dictionary-encoded float key agree
  with a plain float column. The rebuild keeps the keys, key type and
  `ordered?`, because `Series#unique` gathers rows from the canonical array and
  must keep the dictionary dtype. Floats nested inside a list/struct/map key are
  the remaining gap: reaching a float child means rebuilding the parent layout.
- **Nulls are a value in the encoding.** `distinct` wants exactly that (one null
  key), and so does `n_unique`: Polars counts null as a distinct value, so
  `n_unique == unique.size` (Phase 4 note, aligned in 9d). A join that must not
  match nulls (`nulls_equal: false`) checks the key columns' null bitmaps before
  indexing or probing, which costs nothing when the keys have no nulls.
- **Grouping stays sort-based on purpose, and the benchmark closed the
  question twice.** `group_by` yields contiguous `[start, len]` ranges as
  `RecordBatch#slice` views and a deterministic output order; a hash table would
  give both up. The first measurement (Phase 12 follow-up) found the key's
  `lexsort_to_indices` to be **35 ms of a 1005 ms** group-by, while ~95% of the
  time was per group (a `Series`, an `EvalContext`, a length-1 array and one
  `Arrow.concat` piece each), so hashing would have bought ~4%. That named the
  real lever: vectorize the per-group work. **Phase 15 did that**, and the answer
  strengthened rather than weakened: with one pass across all groups, `count`,
  `first`, `last` and the numeric folds are nearly free, so the sort plus the
  full-width row gather *is* the remaining cost — and the contiguous ranges are
  what make the single pass possible. A hash table would now trade one sort
  for a
  gather per group, which is precisely the cost vectorization removed (§14
  *Vectorized grouped aggregation*).

The decision trail is kept because it explains why this waited for a dependency:

**A. Prism-local key encoding (~150–250 lines).** Encode each key value to
canonical bytes — sign-flipped big-endian integers, `totalOrder` bits for floats,
length-prefixed strings, a null sentinel — concatenate per row, and use
`Hash(Bytes, …)`. Fully inside `Prism`, no dependency change, but a second
encoding to keep faithful.

**B. Port `arrow-row` into `arrow-cr` (~4.1k lines of code plus tests).** The
upstream-faithful option: `arrow-rs` has the crate, `arrow-cr`'s charter is a
behaviour-faithful port, and the encoding then matches upstream byte for byte
including the block format for variable-length data. Landed 2026-11-01
(`fixed`, `variable`, `list`, `struct`, `run`, `union` codecs plus the
`RowConverter`/`Rows`/`Row`/`RowParser`/`SortField` core).

**C. Re-open `module Arrow` from `Prism`.** Rejected: exactly what §4 forbids,
and the code would live in neither shard's ownership.

**Decision: prefer B, and B landed.** The earlier choice to defer was right at
the time — there were no benchmarks to justify a ~4k-line dependency port — and
the Phase 9d numbers (see §14 *Benchmarks*) are what turned it into an
implementation request rather than a guess.

### A multi-key sort is stable passes, not a comparator chain (Phase 21)

The two-key sort cost 900 ms on 1e6 rows while each key alone cost 100–200 ms and
the gather only 35 ms. The cost was `lexsort_to_indices`' chain: one proc call per
key per comparison, ~37 ns across ~20M comparisons. (Upstream narrows that: it
specializes 2–5 columns into `sort_fixed_column::<N>` so the comparators are
destructured, where the port keeps one generic loop. The measurement is the
port's, and so is every number in this section.)

`sort_indices` now composes stable single-key sorts for the unbounded multi-key
case — least significant key first, then refine, gathering each key through the
current order with `Arrow.take`. `Arrow.sort_to_indices` breaks ties by index, so
each pass is stable and the composition equals one lexicographic comparison
(asserted against explicit expected orders, ties and nulls included).

| scenario | before | after | speedup |
| --- | ---: | ---: | ---: |
| two keys, full sort | 900.4 ms | 360.0 ms | 2.5× |
| three keys, full sort | 964.2 ms | 482.4 ms | 2.0× |
| `top_n(10, region, price)` | 84.1 ms | 87.0 ms | unchanged |
| one key | 201.1 ms | 203.3 ms | unchanged |

Two deliberate limits:

- **Bounded windows keep the chain.** The pass sequence must order every row to
  build its least-significant ordering, which costs a full pass (~110 ms) against
  ~80 ms for the whole bounded lexsort.
- **Dtypes the per-key sort cannot express keep the chain.** `make_comparator`
  supports more dtypes than `sort_to_indices` (struct, map), so a key's dtype is
  checked with `Arrow.can_sort_to_indices?` before a pass path is chosen — as of
  Phase 23 that predicate is the dependency's, not a mirror. A struct key sorted
  next to a scalar key regressed to `Sort not supported` during this phase —
  caught by a spec written for exactly that — and now falls back.

Phase 22 kept both limits and added a third (all-fixed-width keys), after
measuring that the passes only win where a comparison itself is expensive.

### Two variable-length keys encode rows instead of gathering (Phase 24)

The Phase 21/22 pass path re-sorts by each key and gathers that key through the
current order (`Arrow.take`), and a gather of a string column copies the strings.
With two variable-length keys that means paying twice for the copy the output
already pays once — the aggregate table in §14 shows it as 293–367 ms where one
key alone is ~110 ms.

`SortEngine` now encodes the keys into `arrow-row` bytes once and sorts those
(`row_encoded_indices`), which is exactly what Polars does by default
(`arg_sort_multiple` → `_get_rows_encoded` → `argsort_multiple_row_fmt`), so the
decision has an upstream precedent rather than an invention behind it. The format
is not new to Prism either: `Arrow::RowConverter` already backs `Prism::RowKeys`
for joins, `distinct` and `group_by` keys. Per-key direction and null placement
are encoded into the bytes by `Arrow::SortField` options, so `memcmp` order is the
requested order; equal bytes mean equal keys and the index breaks the tie, which
keeps Prism's "ties keep input order" contract.

`strategy` is the decision table and is public so the spec can pin it — `:single`,
`:bounded`, `:row_encoded`, `:passes`, `:chain` — because each branch is a
measurement rather than a preference. The rule is two or more variable-length keys,
and both sides of it are in the paired table (two prebuilt binaries interleaved
over three rounds, best of five, 1e6 rows):

| scenario | Phase 22 paths | row encoded | speedup |
| --- | ---: | ---: | ---: |
| `sort_by(Utf8, Utf8)` | 519.4 ms | 377.9 ms | 1.37× |
| `group_by(Utf8, Utf8)` | 615.7 ms | 485.1 ms | 1.26× |
| `sort_by(Utf8, Int64)` (one variable key) | 451.4 ms | 441.0 ms | unchanged |
| `sort_by(Int64, Int64)` (fixed width) | 416.8 ms | 419.8 ms | unchanged |
| `top_n(10, Utf8, Utf8)` (bounded) | 154.3 ms | 154.4 ms | unchanged |

One measurement trap worth recording: a first pass at this comparison ran the two
builds once each, in sequence, and showed row encoding *losing* by 20% on scenarios
its code path never touches. Two prebuilt binaries interleaved over rounds is the
only shape that survives machine drift here.

### A single nested key sorts; the dependency owns that path (Phase 23)

`group_by(col("s"))` on a struct column raised `Sort not supported for data type
Struct(...)`. The gap was the sort, not the key: `distinct` and `n_unique`
already key structs through the `arrow-row` encoding, and a *multi-key*
`group_by` with a struct key worked, because `lexsort_to_indices` builds a
`make_comparator` chain for two or more columns. One column is different: the port
forwarded it to `sort_to_indices` unconditionally, and that dispatch has no
struct, map, decimal, interval or union arm.

That guard is a port bug, not an Arrow rule. Upstream `arrow-ord` 60.0.0 gates
the fast path — `if columns.len() == 1 && can_sort_to_indices(dtype)` — and
otherwise falls through to its `LexicographicalComparator`, so one struct key
sorts there. Filed as [`shard_issues/arrow-cr.2.md`](../shard_issues/arrow-cr.2.md)
with the reproducer and the wider dispatch gap (upstream's `can_sort_to_indices`
also admits decimal and interval primitives, which the old arms did not).

Two steps, in this order:

1. **Prism worked around it** (`2125335`) with `SortEngine.comparator_sort`, which
   sorted the index array itself and tie-broke by position. Nothing under `lib/`
   changed.
2. **`arrow-cr` `3023eac` fixed it** — `can_sort_to_indices?` ported,
   `lexsort_to_indices` gated on it, decimal and interval primitives added to the
   dispatch — so the workaround and `per_key_sortable?` are both deleted and
   `SortEngine` now asks `Arrow.can_sort_to_indices?` when choosing the multi-key
   shape. There is no parallel dtype dialect left in Prism, which is the point:
   `arrow-cr` owns Arrow semantics (§1).

Deleting the workaround also improved the bounded path, because the dependency
bounds where the workaround did not: one struct key over 1e6 rows costs 405.1 ms
for the full order and 18.5 ms for `limit: 10`, so `top_n(10)` on such a key now
selects instead of sorting everything. `Arrow.partition` already falls back to the
same comparator for struct keys (as upstream `find_boundaries` does), so
`group_by` works end to end; the emitted keys are the original values, and
grouping still canonicalizes float key columns first (Phase 17).

The specs written against the workaround stay, now pinning the dependency's path
from outside: `SortEngine` for a single unsortable key with and without a bound,
`DataFrame#sort_by`/`top_n` on a single struct key in both directions,
`group_by` on a single struct key where the group count must equal what `distinct`
reports, and a decimal pair (`SortEngine` under both null placements, `group_by`
on the key) for the second dtype family.

### The sort strategy is chosen by dtype, and `group_by` shares it (Phase 22)

Phase 21 sent every unbounded multi-key sort down the pass path, which was right
for the frame it was measured on (`Utf8` + `Float64`) and wrong for others. The
chain's cost is per comparison, so it wins whenever a single comparison settles
the order — the usual case for fixed-width keys. The passes' cost is one full
sort per key, which only pays when a comparison has to walk bytes. Measured per
dtype (best of five, 1e6 rows):

| keys | chain (ms) | passes (ms) |
| --- | ---: | ---: |
| `Utf8` (4 values) + `Int64` | 877 | 293 |
| `Utf8` (1e3 values) + `Utf8` | 1146 | 367 |
| `Utf8` (4 values) + `Float64` | 911 | 329 |
| `Int64` (1e5 values) + `Float64` | 224 | 301 |
| `Int64` (1e3 values) + `Int64` | 277 | 248 |
| `Float64` (4 values) + `Float64` | 325 | 257 |

Fixed-width keys land within ~15% either way, so they keep the chain;
`expensive_to_compare?` selects the pass path for `Utf8`/`LargeUtf8`/`Utf8View`,
`Binary`/`LargeBinary`/`BinaryView` and list types — but not `FixedSizeBinary`,
which compares fixed bytes.

The dispatch moved out of `DataFrame#sort_indices` into `Prism::SortEngine`, which
turns a list of `Arrow::SortColumn`s into indices, so `GroupBy`'s key sort is now
the same sort rather than a parallel one. That mattered: `GroupBy` called
`Arrow.lexsort_to_indices` directly, so a two-key group-by paid the chain for its
entire key sort. Paired before/after, same 1e6-row frame, minutes apart:

| scenario | before (ms) | after (ms) | speedup |
| --- | ---: | ---: | ---: |
| `group_by(region, user_id)` | 1178.8 | 528.7 | 2.2× |
| `group_by(region, qty, user_id)` | 1493.3 | 843.8 | 1.8× |
| `group_by(user_id, price)` (fixed width) | 520.4 | 514.6 | unchanged |
| `group_by(user_id)` (one key) | 179.2 | 182.1 | unchanged |

Groups and group order are unchanged by construction: the passes return the
identical indices, and those indices are what `Arrow.partition` turns into the
contiguous ranges the vectorized aggregates reduce. `spec/prism/sort_engine_spec.cr`
pins each strategy to `Arrow.lexsort_to_indices` on the same input — per dtype
family, ties, nulls, a bounded window and the struct fallback — so a later
strategy change cannot quietly split a group.

### The eager API gets the bounded sort as a verb (Phase 20)

`sort_by(...).head(n)` is two eager verbs, and the first cannot see the second:
it sorts every row and gathers all of them, so `head` trims a frame that was
already copied in full. The plan path had this solved since 9d, but only because
`Plan::Sort` + `Slice` reach `DataFrame#sort_by_slice` together.

Deferring the sort to first access would fix the cost and turn the eager surface
into a lazy one, which §14 closes: eager verbs execute when called. So the fix is
an explicit verb — `DataFrame#top_n(count, *keys, order: :desc, nulls: :last)` —
that resolves keys through the same `resolve_sort_keys` as `sort_by` and lands in
`sort_by_slice(keys, 0, count)`.

`order: :desc` is the default because the name means "largest", matching Polars'
`top_k` and dplyr's `top_n`; `sort_by` keeps its ascending default, and the two
verbs differ only in where they start. Specs pin `top_n` to
`sort_by(...).head(count)` across null placement, direction, several keys and
`SortKey`s, clamping, zero rows and the typed-frame round trip, so the fast path
cannot drift from the obvious one.

Measured (`bench/main.cr`, 1e6 rows, best of 5): `top_n(10)` 7.7 ms against
193.7 ms for `sort_by + head(10)`, which is what the lazy spelling already cost
(7.6 ms).

### A pinned window bounds the sort, not just the gather (Phase 18)

`DataFrame#sort_by_slice` already gathered only the window, but it called
`Arrow.lexsort_to_indices` with no `limit`, so the sort still ordered every row:
`arrow-cr` accepted a `limit` and only truncated the result *after* a full sort.
That half is fixed upstream (`arrow-cr` `5b4b958`: a bounded `limit` selects with
`partial_sort`, with the full sort as the fallback for a limit that covers most
of the input), and Prism now passes it.

`sort_by_keys` computes the window first and hands the sort `offset + count`:

- the bound is capped at the row count (`count` is clamped to the rows left
  after `offset`), so the output is unchanged — the bounded prefix *is* the
  prefix a full sort produces, tie-break included — and a window past the frame
  stays an empty result instead of overflowing the `Int32` cast the sort takes
  (`sort_by_slice` now carries the window in `Int64` for that reason);
- the sort still runs for an empty window, because resolving the keys is what
  rejects a bad column; returning early would swallow `ColumnNotFound`.

Paired before/after on the same 1e6-row frame (§14 numbers, `docs/benchmarks.md`):

| scenario | before | after | speedup |
| --- | ---: | ---: | ---: |
| `sort_by(qty)` → `head(10)` | 81.9 ms | 10.1 ms | 8.1× |
| `sort_by(qty desc, price)` → `head(10)` | 80.4 ms | 3.1 ms | 26× |
| `sort_by(qty)` → `slice(500_000, 10)` | 80.0 ms | 49.7 ms | 1.6× |

A mid-frame window is the case the bound cannot help: `offset + count` is half
the frame, so most of the selection is still paid. Windows near either end are
where it pays.

### Nested keys canonicalize their float children (Phase 17)

`RowKeys.canonical` reached floats only at the top level and inside dictionary
values; a struct, list, fixed-size list or map key kept `totalOrder` bits, so
`{NaN}` never matched `{NaN}` and `{0.0}` never matched `{-0.0}`.

The containers are now rebuilt around a canonicalized child, keeping offsets,
nulls and dtype: a struct from its columns, a list from `(field, offsets, values,
nulls)`, a map from its entries. A child that needs no change comes back as the
same object, so a key with no float children still copies nothing — the identity
case is asserted in the specs.

Two implementation notes that cost real debugging time:

- **Bind the child once.** `#values`/`#columns` are not guaranteed to hand back
  the same object on a second call, so comparing a canonical child against a
  fresh call reports "changed" for an untouched child.
- **Write the guard as an `if` block.** Inside the `forall O` helper,
  `return array unless values.same?(original)` returned even after the condition
  had evaluated false; the explicit `if` form behaves.

List views (`list_view`, `large_list_view`) go through the same path: their
construction does preserve offsets, sizes and dtype, so they are rebuilt around
a canonicalized child rather than left on `totalOrder` floats. The one remaining
top-k gap is upstream and written up as
[`shard_issues/arrow-cr.1.md`](../shard_issues/arrow-cr.1.md).

### Eager grouping prunes what it does not read (Phase 16)

A group-by emits keys plus aggregates, so every other column in the frame is dead
weight. `GroupBy.execute` now computes the pruning set first —
`GroupBy.required_columns(keys, aggregates, columns)`, the union of the keys'
columns and the aggregates' operands, in frame order — and projects the frame
down to it before `to_arrow`, the key sort and the row gather. The degenerate
case (a key-only aggregation such as `count_all` over the key) prunes to the key
columns; an empty set keeps the frame, because `select` needs at least one.

Measured on a 52-column, 200k-row frame with 1e5 groups and two aggregates:
**252.6 ms → 20.2 ms** best of five, and 256.4 → 20.9 median. The remaining cost
is the sort plus gathering the three columns the aggregation reads. Frames no
wider than their aggregation are unaffected, because there is nothing to prune.

### Vectorized grouped aggregation (Phase 15)

`Prism::GroupedAgg` replaced the per-group loop. Before, an aggregate was evaluated
per group: a batch slice, an `EvalContext`, a `Series`, a length-1 array and one
`Arrow.concat` piece, ~2–4 µs per group. Now the operand expression is evaluated
**once** over the whole sorted batch — shared between aggregates that name the
same operand — and one walk over it fills one result array:

| Aggregate | Kernel |
| --- | --- |
| `count` | popcount of the group's sliced validity buffer |
| `count_all` | the range length |
| `first`, `last` | one `Arrow.take` of the group-start / group-end rows |
| `sum`, `mean` | one fold per group (wrapping integers, IEEE floats) |
| `min`, `max` | one fold per group (IEEE total order for floats, `bool_and`/`bool_or` for booleans, byte order for strings) |
| `n_unique`, `std`, `variance`, `median` | the per-group Arrow kernel, from the already-evaluated operand, into one result array |

This is Polars' split, taken from the same place Polars takes it. In its
`GroupsType::Slice` path (`polars-core/frame/group_by/aggregations`),
`agg_first`/`agg_last` collect the group boundary indices and do a **single**
`take`, `agg_valid_count` counts set bits in the group's validity `BitMask`, and
the numeric aggregates either use a rolling kernel or fall back to a per-group
`_agg_helper_slice` that **preallocates** the output — which is exactly the shape
of the `n_unique`/`median`/`std`/`var` tier here. Polars' per-group fallback is
fast in a way Crystal cannot be (a Rust `par_iter` over slices, no per-group
allocations), which is why the fold-per-group kernels are one Crystal loop rather
than a call per group.

Reduction semantics stay Arrow's, so a grouped result equals the kernel
applied to
the same rows: nulls are skipped and an all-null group is null, integer sums wrap
(`&+`), and float extremes compare in IEEE total order — `-0.0` below `0.0`,
every NaN above `+inf` — matching `Arrow.min`/`max`'s `total_cmp`. Two shapes keep
the old per-group expression evaluation on purpose: an operand that contains an
aggregation (`col("a").sum.mean`, which has no defined cross-group meaning), and
a dtype `Series#min` itself rejects, so the error message a user sees is still the
one `Series` raises.

Measured with the same harness, best of five, on `group_by("user_id")` over the
1e6-row frame (1e5 groups):

| step | before (ms) | after (ms) |
| --- | ---: | ---: |
| `lexsort_to_indices` on the key alone | 118 | 151 |
| `group_by` + `count` | 419 | 235 |
| `group_by` + one `sum` | 326 | 235 |
| `group_by` + three aggregates | 544 | 237 |
| `group_by` + `n_unique` | 469 | 329 |
| `distinct` on the same keys (one hash pass) | 61 | 58 |

One aggregate and three now cost the same, which is the point: the aggregate
layer stopped being where a group-by spends its time. The floor is the sort plus
the `take_record_batch` that gathers every column, so **column pruning in eager
`group_by`** is the next item rather than another aggregate optimisation.
`n_unique` remains the one visibly-above-floor aggregate, because distinctness
per group is a hash pass per group rather than a fold.

### Where we deliberately differ from Arquero

Arquero's deferred `BitSet` mask makes eager `filter` free, but every subsequent
operation — and every edge — must honor the mask. That is a lazy engine wearing
eager clothes. Our split is explicit: eager verbs materialize (Arrow's
`filter_record_batch` semantics, documented), and `LazyFrame` is where work is
deferred and pushed into the Parquet reader. The benchmarks settled the question
they were meant to settle: a half-frame `filter` over 1e6 rows is 53 ms and a
projection is zero-copy, so filter-dominated workloads are not the bottleneck
(see [`docs/benchmarks.md`](../docs/benchmarks.md)).

### Benchmarks (Phase 9d)

`make bench` runs [`bench/main.cr`](../bench/main.cr) over one 1e6-row frame;
scenarios, numbers and the reading of them live in
[`docs/benchmarks.md`](../docs/benchmarks.md). Headlines, from the latest run
(Apple M4, Crystal 1.22.2, best of five; the machine was loaded, so every
absolute is ~1.3–1.5x an idle run and only the within-run comparisons matter):

| Path | Time |
| --- | ---: |
| `filter` on half the rows | 50 ms |
| `select` / `to_arrow` (one chunk) | 0 ms (same buffers) |
| `sort_by` one key / two keys | 212 ms / 1134 ms |
| `sort_by` + `limit 10`, eager / window gathered | 318 ms / 153 ms |
| `group_by` 4 keys / 1e5 keys | 263 ms / 293 ms |
| `distinct` / `n_unique` on 1e5 keys, before / after row keys | 163 / 144 ms → 80 / 69 ms |
| join 1e6 → 1e5 | 309 ms (219 ms in the idle run; row keys bought 284 → 219) |
| Parquet read all / 2 of 4 columns / `limit 100` | 139 ms / 83 ms / 4 ms |
| Parquet predicate, unclustered / clustered | 176 ms / 224 ms (285 ms unfiltered) |
| Lazy scan+filter+select vs eager | 159 ms vs 187 ms |

What they decide:

- **A pinned slice gathers only its window** (`DataFrame#sort_by_slice`, wired
  into `Plan::Sort#execute`): `sort + head(10)` costs 153 ms through the plan
  against 318 ms for eager sort-then-trim, and the gap is the 1e6-row copy the
  eager path still makes. The sort itself is still `O(n log n)` — a bounded-heap
  top-k is an `arrow-cr` kernel this port does not have, not a Prism change.
- **`group_by` is no longer dominated by its aggregates** (Phase 15): 4 keys and
  1e5 keys now cost about the same as the sort that precedes them, because one
  pass reduces every group. See *Vectorized grouped aggregation* above for the
  before/after decomposition.
- **`arrow-row` was worth it, and mostly for `distinct`/`n_unique`.** Those were
  sort-bound at 163/144 ms and are now 3.0x faster (54/48 ms); the join improves
  1.3x, because a 1e6-row encode, probe and gather is not dominated by key
  hashing. The "before" numbers also showed it was not the pipeline's
  bottleneck, which is why it waited for the `arrow-cr` port rather than being
  hand-rolled locally.
- **Predicate pushdown depends on the data, not the optimizer.** On unclustered
  columns the row filter costs more than it saves (155 ms vs 117 ms) because no
  row group can be skipped; on a clustered column it saves 30–45%. Worth
  documenting for users rather than optimizing here.
- The copy-count check stays a spec ("a projected `select` is the same
  buffers"), not a counter: Crystal has no per-operation allocation hook, and
  `GC.stats` deltas are too coarse to attribute.

**Follow-up probe (post-Phase 12), deciding the hash-grouping question.** A
separate probe decomposed one 1e5-key group-by on the same frame, best of five
(that run was under load, so compare ratios rather than absolutes):

| Step | Time |
| --- | ---: |
| `lexsort_to_indices` on the key alone | 35 ms |
| `sort_by(key)` (lexsort + take of 4 columns) | 183 ms |
| `group_by` + `count` only | 403 ms |
| `group_by` + one `sum` | 508 ms |
| `group_by` + three aggregates | 1005 ms |
| `distinct` on the same keys (row-keyed) | 91 ms |

The sort a hash table would remove was under 4% of that total, so hash-based
grouping was declined at the time: it would have given up contiguous group views
and deterministic ordering for ~4%. What the probe *did* show was where the time
went — ~4 µs per group for `count`, ~2.5 µs per additional aggregate, against
`distinct`'s single hash pass over the identical keys — and that is what Phase 15
removed. The same probe re-run after it (see *Vectorized grouped aggregation*)
puts one aggregate and three at the same 235 ms, so the aggregates are no longer
the cost, and hashing would now trade the sort for a per-group gather: the exact
work vectorization deleted. Declined a second time, with a sharper reason.

## 15. Risks and open questions

| Risk | Mitigation |
| --- | --- |
| `Expr#==` returning `Expr` silently breaks `should eq` | Documented; specs use `same_as?`; a spec asserts every `Expr` comparison helper |
| `&` precedence mis-parses user expressions | `.and`/`.or` provided; docs lead with the parenthesised form |
| `arrow-cr`/`parquet-cr` are young; a kernel may be missing or wrong | Never patch `lib/`; reproduce, file upstream, document the workaround in this plan |
| Multi-chunk frames complicate every verb | Element-wise verbs are chunk-agnostic by construction; ordering verbs materialize |
| Group-by via sort is O(n log n) | Measured twice: the sort was 35 ms of a 1005 ms 1e5-key group-by before vectorization, and after it the sort plus the row gather *is* the floor while the aggregates are nearly free (§14). The `O(n log n)` sort is the price of the contiguous ranges that make the single pass possible, and a hash path was measured and declined twice |
| Typed-schema macro complexity | Isolated in Phase 8 with its own spec file; API is additive |
| Users expect Kotlin-style typing through every verb | §4.1 states the boundary limit up front; `S.from_dataframe` documented as the re-entry point |
| Out-of-range literal vs a narrow column raises instead of folding | Polars rewrites such comparisons to a constant. Constant folding is Phase 7 work; documented in §8.2 |
| NaN join keys never match | **Fixed** by the `arrow-row` encoding (§14): NaNs encode canonically and match. Pinned by a join spec |
| Floats keyed by `totalOrder` bits would split `-0.0`/`0.0` and NaN payloads | `RowKeys.canonical` normalizes float key columns before encoding (Polars `canonical_f64`); `join`, `distinct` and `group_by` specs pin that they agree |
| Proving joins/distinct need hashing | Settled: Phase 9 benchmarks showed `distinct`/`n_unique` were sort-bound, and `arrow-row` (option B) landed in `arrow-cr`; §14 records the measurements |
| Compile times grow with `require` graph | Entry point requires everything; measure and split with `require`-level shards if it regresses |

**Open questions** (decide in-phase, record here):

- ~~Should `DataFrame#[]` accept a `Range`/`Slice` for row slicing, or keep rows
  out of `[]` entirely?~~ Decided in Phase 12: `df[range]` slices rows and
  `df[[names]]` projects columns, so `[]` is "pick part of the frame" while
  `df["name"]` stays a `Series`. Bounds clamp rather than returning `nil`,
  because a frame is always a frame (C22 records the dispatch trap).
- Null-key policy default for `group_by` on `Utf8` vs numeric columns: one
  policy or per-dtype defaults? (Phase 4)
- ~~`n_unique` on floats: use Arrow total order (NaN payloads distinct) or
  IEEE-equality grouping?~~ Decided in 9d: float keys are canonicalized like
  Polars' `canonical_f64`, so every NaN is one key and `-0.0`/`0.0` are one key.
- Join suffix scheme: `_right` vs `_y` vs error. (Phase 5)

## 16. Definition of done

A feature is done only when all of the following hold, committed as **one**
atomic commit:

1. Public API implemented, documented in code comments and, if user-facing,
   in `README.md` / `docs/architecture.md`.
2. Specs cover the happy path, null handling, dtype behaviour, and error paths;
   a misspelt-column spec raises `Prism::ColumnNotFound`.
3. `make lint` and `make test` pass.
4. This plan's phase checklist and **Where we are** block are updated.
5. Any intentional divergence from the Arrow/Parquet semantics is recorded in
   §7 or §8 (or in `docs/architecture.md`).
6. No changes inside `lib/`.

### Commit message

```text
feat: <phase area> <what now works>

- bullet: what changed and which files
- bullet: semantics decisions or divergences
- gates: make lint + make test green
```

Use `feat:` / `fix:` / `docs:` / `chore:` / `perf:` / `test:`. Name the phase
in the body (e.g. `Phase 3`). Keep it resumable from the log alone.

---

## 17. Deferred work

Every gap noticed during a phase, with the phase that closes it. Nothing here is
forgotten; if an item becomes urgent, it moves up rather than disappearing.

| Item | Deferred because | lands in |
| --- | --- | --- |
| Projection pruning through joins | needs output-name → side mapping for suffixed columns | 9d (done) |
| Slice pinned to `Sort`/`GroupBy`/`Distinct` | scan pushdown covered the common case first | 9d (done) |
| Physical top-k for a pinned slice | gather half in 9d; sort half needed an `arrow-cr` fix (`5b4b958`, filed as `shard_issues/arrow-cr.1.md`), now wired up | done (Phase 18) |
| `arrow-row` key encoding for joins/distinct | blocked on the `arrow-cr` port | 9d (landed; see §14) |
| Decimal arithmetic | kernels grow precision, so a per-op result dtype is needed | post-v0.1 (done) |
| Out-of-range literal folding | raising is safe, folding is Polars parity | 9c (done) |
| Nested list/struct/map in `Values` | needed a recursive `Value` alias — legal through `Array`/`Hash`, corrected the earlier assumption | post-v0.1 (done) |
| `LazyFrame` missing verbs | Phase 7 covered the planner's needs | 9b (done) |
| `Series#is_null`/`fill_null`/`cast`/`==` | verbs cover the same ground today | 9b (done) |
| Plan-level CSE | per-chunk memoization already removes the recompute | 9b (only if measured) |
| Display layer | not needed to validate semantics | 9a (done) |
| Repository folder + GitHub rename | owner-gated | 10 |
| `group_by(k).sum/mean/count/min/max/median/std/var/n_unique(*cols)` (RedAmber-derived one-call aggregates) | `aggregate(name: expr)` already covers the semantics; this is ergonomics, and it needs a naming rule (input name, Polars-style) | 12 (done) |
| `sort_by("-col")` descending shorthand (RedAmber) | `order: :desc` covers it; the `-` prefix is Ruby sugar and makes a leading-dash column name unreachable in shorthand | 12 (done) |
| `slice(1..3)`, `df[1..3]` rows, `df[["a","b"]]` columns (RedAmber indexing) | `slice(offset, count)` / `select(...)` cover it; accepting `Range`/arrays closes the §15 question below | 12 (done) |
| `remove(expr)`, the inverse of `filter` (RedAmber) | needs its own null rule — keep rows where the predicate is false **or** null, which is not `filter(pred.not)` | 12 (done) |
| Boolean-mask filtering: `df.filter(series > 10)` (RedAmber) | needs elementwise `>`/`<`/`>=`/`<=` on `Series` first; only `==` existed | 12 (done) |
| Hash-based grouping | the stated gate was "only if a benchmark shows grouping dominating"; a probe put the key sort at 35 ms of a 1005 ms 1e5-key group-by, so hashing would buy ~4% for the group views and deterministic order | 12 follow-up (measured, declined) |
| Vectorized per-group aggregation | the real cost is per group (~4 µs for `count`, ~2.5 µs per extra aggregate at 1e5 groups); it needs one kernel across all groups instead of one slice per group, which is a redesign of the aggregate layer | 15 (done) |
| Column pruning inside eager `group_by` | after Phase 15 the aggregates are nearly free, so the key sort and the full-width `take_record_batch` are the floor; the planner prunes for lazy pipelines but the eager verb still gathers every column | 16 (done) |
| Typed row structs: `define_schema` generating `S::Row` so rows are nominal | only meaningful for a declared schema, and nested fields would still be containers (§10 "Why `Values` returns containers") | 13 (done) |
| Interval materialization: `Value` had no month/day/nano member, so interval columns could not reach `to_a`/`rows`/display | it needed a value shape for all three Arrow units; `Arrow::IntervalValue` is that shape, so no new type was introduced | 14 (done) |
| Dictionary-encoded float keys were compared by value but not canonicalized | the values are what gets encoded, so canonicalizing them in place keeps the dictionary dtype and makes keying agree with plain float columns | 14 (done) |
| Floats nested inside list/struct/map key columns are still `totalOrder` | reaching a float child means rebuilding the parent layout; nested key columns are rare and the limit is documented in `Prism::RowKeys` | 17 (done) |

RedAmber's headline ergonomic is **bare column names inside blocks**
(`slice { carat > 1 }`), which works because Ruby `instance_eval`s the block
against the frame. Crystal dispatches statically, so an untyped frame cannot
resolve `carat`; `with obj yield` would need the columns as methods on a known
type, which is exactly what the typed frames give (`typed.filter(&.active)`).
Recorded here so nobody plans around copying it.

Deliberately **not** planned: Arquero-style deferred `BitSet` filters (§14),
hash-based grouping (§12 C5, measured and declined — §14), and a `vstack` alias
for `concat` (§12 Phase 5).

## Appendix A — the original proposal

Retained verbatim so future sessions can see which parts of the sketch were
adopted, which were reshaped, and why. The mapping to the worked design is in
[§7](#7-deviations-from-the-proposed-sample-api).

```crystal
require "arrow"

df = Arrow::DataFrame.from_arrow(record_batch)
df = Arrow::DataFrame.read_parquet("data/sales.parquet")

adults = df.filter { col("age") >= 21 }

sales_summary = df
  .filter { (col("status") == "completed") & (col("price") > 10.0) }
  .select("order_id", "region", "price", "quantity")
  .with_column("total_amount") { col("price") * col("quantity") }

regional_stats = df
  .group_by("region", "category")
  .aggregate(
    total_revenue: col("total_amount").sum,
    avg_price:     col("price").mean,
    order_count:   col("order_id").count
  )
  .sort_by("total_revenue", order: :desc)

optimized_result = df.lazy
  .filter(col("created_at") >= Time.utc(2026, 1, 1))
  .select("user_id", "department", "salary")
  .group_by("department")
  .aggregate(mean_salary: col("salary").mean)
  .collect

Arrow.define_schema UserSchema,
  id: Int64,
  name: String,
  active: Bool,
  salary: Float64

typed_df = UserSchema.from_arrow(record_batch)
active_salaries = typed_df.filter { col(&.active) }.select(&.salary)
```

Proposed layering, all retained: expressions → verbs → lazy plan → optional
compile-time schemas. Two claims from the sketch are corrected in §7 and
`docs/architecture.md`: `filter`/`select` are not both zero-copy, and the
C-ABI/CFFI handoff is `arrow-cr`'s concern, not this library's.
