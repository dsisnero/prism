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

**Current focus:** Phase 1 — foundation types (`DataFrame`, `Series`, schema
handling, construction and conversion).

**Next up:**

1. Phase 1: `Prism::Series` + `Prism::DataFrame` construction and round-trip
   (`from_arrow`, `to_arrow`, `each_batch`, `shape`, `column_names`, `dtypes`).
2. Phase 1: generic value extraction (`Series#to_a`) via a `make_array`-style
   dtype dispatch — see [C4](#c4-arraybase-has-no-generic-value-extraction).
3. Phase 1: `Prism::Error` hierarchy and argument validation.
4. Phase 2: `Prism::Expr` AST, DSL, and evaluation against a batch.

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Project scaffolding, dependency wiring, gates | **done** |
| 1 | Foundation: `DataFrame`, `Series`, schema, construction | in progress |
| 2 | Expressions: AST, DSL, evaluation, coercion | not started |
| 3 | Eager verbs: `select`, `filter`, `with_column`, `sort_by`, … | not started |
| 4 | `group_by` + aggregations | not started |
| 5 | Joins and row/column concatenation | not started |
| 6 | IO: Parquet, CSV, IPC (read + write, streaming) | not started |
| 7 | `LazyFrame` + optimizer (projection/predicate pushdown) | not started |
| 8 | Compile-time typed schemas | not started |
| 9 | Display, examples, benchmarks, v0.1.0 hardening | not started |
| 10 | Repo/GitHub rename `dataframe` → `prism` (shard + entrypoint done) | blocked on owner |

**Last commit:** `chore: rename shard to prism`.
**Gates:** `make lint` and `make test` — both green at Phase 0.

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
Null keys form their own group and are placed first; that policy is documented
in §8.1 and configurable (`null_keys: :first | :last | :drop`).

#### C6. No `mean`, `count`, or `count_distinct` kernels

**Decision:** compose them. `mean = sum / non_null_count` with the result
promoted to `Float64` (integers included); `count` uses `ArrayBase#count`
(non-null rows) with an explicit `count_all` for `len`.

#### C7. `serde`-style named-argument aggregation does not exist in Crystal

`aggregate(total: expr, avg: expr)` with arbitrary names requires a **macro**
with a named splat. Verified working:

```crystal
macro aggregate(**named)
  # each key/value pair is a NamedTupleLiteral member
end
```

**Decision:** `GroupBy#aggregate` is an instance macro emitting
`Array({String, Expr})`. A plain-hash overload
(`aggregate({"total" => expr})`) is kept for dynamic names.

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
| Bare expression builders | `Prism::DSL` (`include Prism::DSL`) | Keeps `col`/`lit` out of the global namespace |
| Errors | `Prism::Error` and subclasses | See §8.4 |

**No `Arrow::*` reopening.** `arrow-cr` owns that module. Extending it from a
downstream shard would make `Prism` requireable-but-broken (types defined only
after a second `require`) and would couple us to another shard's names.

**Shard name:** `shard.yml` declares `name: prism` and the entrypoint is
`src/prism.cr`, so consumers write `require "prism"`. The repository folder and
the GitHub repository are still named `dataframe`; that rename is owner-gated
(Phase 10) and must be done as one step, not piecemeal.

## 4.1 Design influences

Crystal is Ruby-shaped syntax on a statically typed, LLVM-compiled language with
macros, so no single reference library fits. Each contributes one axis:

| Library | Axis | Adopted as | Deliberately not adopted |
| --- | --- | --- | --- |
| [Polars](https://deepwiki.com/pola-rs/polars) | Execution | Expression engine; lazy plan; pass ordering (projection pruning, predicate pushdown, slice pushdown, constant folding, dead-column elimination) | Common-subplan elimination via `Cache` nodes, join reordering, sort collapse — no payoff at v0.1 scale, and they need plan-level cost modelling |
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

- `Prism::TypedFrame(S)` is typed **at the declaration boundary**. A misspelt
  field is a compile error, and accessors are typed.
- A verb that changes the schema (`with_column`, `select`, `join`) returns a
  plain `DataFrame`, not a refined `TypedFrame`. Re-declare or re-wrap with
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
| `Prism::Schema` (typed) | macro-generated struct with `Expr` accessors |

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

# --- inspection ---------------------------------------------------------
df.shape            # => {rows: 3, columns: 4}
df.column_names     # => ["id", "region", ...]
df.dtypes           # => {"id" => Arrow::DataType.int64, ...}
df.schema           # => Arrow::Schema
df["region"]        # => Prism::Series
df.to_arrow         # => Arrow::RecordBatch
df.to_s             # aligned text table
df.head(5); df.tail(5); df.slice(1, 10)

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
df.distinct
df.distinct("region")
df.drop_nulls("region")
df.fill_null(0)
df.cast("qty" => Arrow::DataType.float64)

# --- grouping -----------------------------------------------------------
df.group_by("region", "category").aggregate(
  total_revenue: col("price").sum,
  avg_price:     col("price").mean,
  order_count:   col("order_id").count,
).sort_by("total_revenue", order: :desc)

df.group_by("region").agg(col("price").sum.alias("total"))

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
UserSchema.schema                             # => Arrow::Schema
```

### 6.1 `Expr` surface

| Category | Methods |
| --- | --- |
| Arithmetic | `+ - * / // % ** abs round floor ceil sqrt pow` |
| Comparison | `== != < <= > >= eq neq lt lt_eq gt gt_eq` |
| Boolean | `& \| !` (operators), `and` / `or` / `not` (methods, precedence-safe) |
| Null | `is_null`, `is_not_null`, `fill_null(v)`, `coalesce(*exprs)` |
| Cast | `cast(dtype)` |
| String | `str_length`, `str_contains`, `str_starts_with`, `str_to_uppercase`, `str_to_lowercase` |
| Aggregate | `sum min max mean count count_all n_unique first last std var median` |
| Naming | `alias(name)` |
| Introspection | `dtype(schema)`, `columns : Array(String)`, `key`, `same_as?(other)` |

### 6.2 `Series` surface

`name`, `dtype`, `size`, `count`, `null_count`, `to_a`, `to_arrow`, `each`,
`sum`, `mean`, `min`, `max`, `unique`, `is_null`, `fill_null`, `cast`,
`value(i)`, `==` (element-wise, returns `BooleanArray` per Arrow semantics).

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
- **`&` precedence** (C9): parentheses are mandatory.

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
   (`Expr#columns`), intersect down to the scan. For `ScanParquet`, emit
   `ProjectionMask.columns(parquet_schema, needed)`.
3. **Limit pushdown** — `Limit` merges into a scan as `with_limit` +
   `with_offset` when no intervening `Sort`/`GroupBy`/`Join` reorders or grows
   rows.
4. **Constant folding** — evaluate literal-only subtrees at plan time.
5. **Dead column elimination** — drop `WithColumn` nodes nobody reads (after
   pruning), keeping order semantics if the removed expression is pure (it
   always is; expressions have no side effects).

`explain` prints the unoptimized and optimized plans side by side, including
which predicates and projections reached the scan — this is the observable proof
that pushdown happened, and the optimizer specs assert on it.

### 9.3 Executor

A plan node yields an `Array(RecordBatch)` (the scan streams via `Iterator`;
`DataFrame` materializes per node). Grouped aggregation sorts once then
processes ranges; joins build a hash table on the smaller side keyed by the join
column's Crystal values (via `Prism::Values`, C4).

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

### Phase 1 — foundation

- [ ] `Prism::Error` hierarchy (§8.4)
- [ ] `Prism::Values` dtype dispatch (C4)
- [ ] `Prism::Series`: name, dtype, size, count, null_count, `to_a`, `value`,
      aggregates, `to_arrow`
- [ ] `Prism::DataFrame`: `from_arrow`, `from_batches`, `from_columns`,
      `schema`, `shape`, `column_names`, `dtypes`, `[]`, `to_arrow`,
      `each_batch`, `head`, `tail`, `slice`, `row_count`
- [ ] Chunk invariants enforced at construction (schema equality, row counts)
- [ ] Zero-column and empty frames behave (C-invariant in §5.5)

**Acceptance:** a batch can round-trip `Arrow → DataFrame → Arrow` unchanged
(schema and values), including nullability and nulls. Specs:
`spec/prism/dataframe_spec.cr`, `spec/prism/series_spec.cr`,
`spec/prism/values_spec.cr`.

### Phase 2 — expressions

- [ ] AST nodes + `key`, `columns`, `same_as?`
- [ ] `dtype(schema)` for every node, including coercion (§8.2)
- [ ] `eval(ctx)` for column, literal, arithmetic, comparison, boolean, cast
- [ ] `Prism::DSL` with `col`, `lit`, and `Prism.col` alias
- [ ] Operator overloads with the C9 caveat documented
- [ ] `And`/`Or`/`Not` methods as the precedence-safe form
- [ ] Null predicates and `fill_null`/`coalesce`

**Acceptance:** expressions evaluate identically to the equivalent Arrow kernel
calls, and `dtype` matches `eval(...).data_type` for every spec case (asserted
with a helper so the two can never drift).

### Phase 3 — eager verbs

- [ ] `select` (names, exprs, aliased exprs), `filter`, `with_column`,
      `with_columns` (macro), `drop`, `rename`, `cast`, `sort_by`,
      `distinct`, `drop_nulls`, `fill_null`, `with_row_index`
- [ ] Chunk-preserving execution for element-wise verbs (§5.5)
- [ ] `sort_by` multi-key with per-key direction/null placement via
      `lexsort_to_indices` + `take_record_batch`
- [ ] Column-name validation everywhere (`Prism::ColumnNotFound`)

**Acceptance:** each verb has a spec that checks values *and* schema, plus one
spec proving chunks are preserved (input with 3 batches yields 3 batches).

### Phase 4 — group_by + aggregation

- [ ] `GroupBy` with multi-key grouping (C5)
- [ ] Aggregate functions: `sum min max mean count count_all n_unique first last
      std var median`
- [ ] `null_keys` policy (§8.1)
- [ ] Macro `aggregate(**named)` (C7) + hash overload
- [ ] Aggregation result schema derived from `Expr#dtype`, not from data

**Acceptance:** grouped results match a naive per-key reference implementation
over the same data, including all-null groups and empty input (zero rows in →
zero rows out, correct schema).

### Phase 5 — joins and concatenation

- [ ] `join` inner/left/right/outer/semi/anti on one or more keys
- [ ] `left_on`/`right_on` with differing names; suffix handling for collisions
- [ ] `Prism.concat` (vertical, schema-compatible) and `hstack` (horizontal)
- [ ] `vstack` alias if it reads better in docs; decide in-phase

**Acceptance:** join cardinality and null-fill behaviour asserted per join type,
including duplicate keys (many-to-many) and empty inputs.

### Phase 6 — IO

- [ ] `read_parquet` / `read_parquet(io)` with `columns`, `batch_size`,
      predicate pushdown
- [ ] `write_parquet` / `write_parquet(io)` with compression, row-group size,
      guaranteed `close` (C10)
- [ ] `read_csv` / `write_csv` via `arrow-cr`
- [ ] `read_ipc` / `write_ipc`
- [ ] Round-trip specs against in-memory `IO::Memory` (no fixture files needed)
- [ ] One fixture-based spec for a real file read from `temp/` (gitignored)

**Acceptance:** `write_parquet` → `read_parquet` preserves schema (including
nullability and timestamp units), values, and row count for a table with nils
and mixed column types.

### Phase 7 — lazy + optimizer

- [ ] Plan nodes + `schema` propagation
- [ ] `collect`, `explain`
- [ ] All five optimizer passes (§9.2), each with a spec asserting the plan
      shape
- [ ] Pushdown equivalence spec: optimized and unoptimized plans return
      identical `DataFrame`s

**Acceptance:** a scan with `filter` + `select` + `head` produces a plan whose
scan node carries a `ProjectionMask`, a `RowFilter`, and a limit — asserted via
`explain` — and the result equals the eager pipeline's result.

### Phase 8 — typed schemas

- [ ] `define_schema` macro (C8: first parameter `schema`)
- [ ] `TypedFrame(S)` with block verbs and typed column accessors
- [ ] Schema-changing verbs return a plain `DataFrame`; document
      `S.from_dataframe` as the re-entry point (§4.1)
- [ ] Compile-time safety proven by `crystal build` specs that assert a
      *failure* (a `temp/` file compiled and expected to fail, run from a spec)

**Acceptance:** the happy path compiles and runs; a misspelt field fails to
compile with a message naming the field; `typed.with_column(...)` returns a
`DataFrame` and `S.from_dataframe` restores typed access.

### Phase 9 — display and hardening

- [ ] `DataFrame#to_s` / `#inspect` aligned table with dtype footer, truncation
      for wide/tall frames, null rendering
- [ ] `examples/` for the README workflows
- [ ] `bench/` micro-benchmarks for filter/select/group_by/parquet read
- [ ] Memory notes: `#to_arrow` copy cost documented
- [ ] v0.1.0 tag

### Phase 10 — repository rename (owner-gated)

Shard-level rename is done; the remaining items are outside this repository.

- [x] `shard.yml` `name: prism`; `src/dataframe.cr` → `src/prism.cr`
- [x] `spec/spec_helper.cr` require path; README/docs `require "prism"`
- [ ] Repo folder `dataframe/` → `prism/` and the GitHub repository rename
- [ ] Update sibling checkouts that reference the old path
- [ ] Switch `shard.yml` dependencies from `path:` to `github:` on publish, and
      update the README install block to the published form
- [ ] Do it as one step, not piecemeal

## 13. Testing strategy

- Spec layout mirrors `src/prism/`. See
  [docs/testing.md](../docs/testing.md) for the full policy.
- **Compile cost matters:** requiring `arrow-cr` + `parquet-cr` costs ~20 s per
  build. Prefer one spec run over many; use `crystal spec spec/prism/x_spec.cr`
  while iterating.
- No fixture blobs in git: generate Parquet/CSV/IPC data in `temp/` at spec time
  via writers, then read it back.
- Property-style checks where cheap: for `dtype` vs `eval` agreement, and for
  `sort_by` stability.
- Optimization specs assert plan shape via `explain`, not just results, so a
  silently disabled pass fails CI.

## 14. Performance

- Kernels are Arrow's; the library's job is to *not* add copies. Rules:
  element-wise verbs map over chunks and reuse untouched arrays; `select`
  projects rather than copies; only row-reordering verbs concatenate.
- Benchmarks (Phase 9) track: filter throughput, group_by on 1e6 rows, Parquet
  read/write with and without projection pushdown.
- `#to_arrow` on a multi-chunk frame copies. It is documented, not hidden.

## 15. Risks and open questions

| Risk | Mitigation |
| --- | --- |
| `Expr#==` returning `Expr` silently breaks `should eq` | Documented; specs use `same_as?`; a spec asserts every `Expr` comparison helper |
| `&` precedence mis-parses user expressions | `.and`/`.or` provided; docs lead with the parenthesised form |
| `arrow-cr`/`parquet-cr` are young; a kernel may be missing or wrong | Never patch `lib/`; reproduce, file upstream, document the workaround in this plan |
| Multi-chunk frames complicate every verb | Element-wise verbs are chunk-agnostic by construction; ordering verbs materialize |
| Group-by via sort is O(n log n) | Acceptable for v0.1; a hash-based path is a possible Phase 9 optimization |
| Typed-schema macro complexity | Isolated in Phase 8 with its own spec file; API is additive |
| Users expect Kotlin-style typing through every verb | §4.1 states the boundary limit up front; `S.from_dataframe` documented as the re-entry point |
| Compile times grow with `require` graph | Entry point requires everything; measure and split with `require`-level shards if it regresses |

**Open questions** (decide in-phase, record here):

- Should `DataFrame#[]` accept a `Range`/`Slice` for row slicing, or keep rows out
  of `[]` entirely? (Phase 3)
- Null-key policy default for `group_by` on `Utf8` vs numeric columns: one
  policy or per-dtype defaults? (Phase 4)
- `n_unique` on floats: use Arrow total order (NaN payloads distinct) or
  IEEE-equality grouping? (Phase 4)
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
