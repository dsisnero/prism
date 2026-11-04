# Prism

**A DataFrame library for Crystal, built directly on Apache Arrow.**

[![Crystal](https://img.shields.io/badge/crystal-%E2%89%A5%201.22.2-000000.svg)](https://crystal-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Arrow](https://img.shields.io/badge/storage-Apache%20Arrow-0faa92.svg)](https://arrow.apache.org)

Prism is a column-oriented table over `Arrow::RecordBatch`, with an inspectable
expression language, grouped aggregation, Parquet/CSV/IPC interchange, and a lazy
query plan that pushes projections and predicates **into the Parquet reader**,
so
a large file can be filtered without being loaded.

It is not a wrapper around another dataframe. Every boundary type is an
`arrow-cr` type: `Prism::DataFrame` holds `Array(Arrow::RecordBatch)` and one
`Arrow::Schema`, and `#to_arrow` hands back an `Arrow::RecordBatch`. There is no
second schema dialect to keep in sync.

Coming from Polars? [docs/polars-parity.md](docs/polars-parity.md) maps the verbs
and expressions you already know, and lists the handful of semantics that differ
on purpose.

```crystal
require "prism"
include Prism::DSL

sales = Prism.read_parquet("data/sales.parquet")

sales
  .filter { (col("status") == "completed") & (col("price") > 10.0) }
  .group_by("region")
  .aggregate(revenue: (col("price") * col("qty")).sum, orders: col("order_id").count)
  .sort_by("revenue", order: :desc)
```

## Why Prism

- **Arrow all the way down.** No pandas/Polars-style copy layer. Columns are
  Arrow arrays, kernels are Arrow kernels, and Parquet reads decode only the
  columns and row groups a query needs.
- **Expressions are values.** `col("price") * col("qty")` is data you can
  inspect, alias, compose and type-check before any row is read. `explain` shows
  the plan, so an optimization that stops firing fails a spec instead of
  silently getting slower.
- **A lazy planner that does real work.** Predicate pushdown (including
  `RowFilter` absorption and conjunct splitting), projection pruning through
  joins and aggregations, slice pushdown, and a window pinned to
  `Sort`/`GroupBy`/`Distinct`. Lazy results are asserted equal to the eager
  pipeline, so the two cannot drift.
- **O(n) keys, not sort-based guesses.** `arrow-row` encodes key columns into
  canonical bytes, which makes `distinct`, `n_unique` and joins hash lookups
  instead of sorts — and makes `NaN` keys match while `-0.0`/`0.0` collapse,
  matching Polars' `canonical_f64`.
- **Honest about copies.** Element-wise verbs reuse buffers; `filter`, `sort`,
  `join` and `group_by` allocate because the output is a different row set.
  `#to_arrow` copies when the frame is chunked, and
  [the docs say exactly when](docs/architecture.md#memory-what-to_arrow-costs).
- **Typed columns when you want them.** `define_schema` generates compile-time
  accessors, so `typed.select(&.salary)` with a misspelt field is a build error,
  and `typed.rows` yields nominal rows with those fields.
- **Nested data, not a flat-only API.** List, fixed-size-list, struct and map
  columns materialize as arrays and hashes the way Polars' `to_list` does, and
  they work as `distinct` / `n_unique` / `group_by` keys through the same
  `arrow-row` encoding as scalar keys. Interval columns materialize as
  `Arrow::IntervalValue` (months, days, nanoseconds), so every dtype has a value
  shape.

## Installation

```yaml
# shard.yml
dependencies:
  prism:
    github: dsisnero/prism
```

```bash
shards install
```

Requires Crystal 1.22.2 or newer. `arrow-cr` and `parquet-cr` come with it.

## Quick start

```crystal
require "prism"

include Prism::DSL   # enables bare col(...) / lit(...)

orders = Prism.from_columns({
  order_id: Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64]),
  region:   Arrow::StringArray.from_nullable(["east", "west", nil]),
  price:    Arrow::Float64Array.from_nullable([10.0, 20.0, 30.0]),
  qty:      Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64]),
})

orders.shape          # => {rows: 3, columns: 4}
orders.column_names   # => ["order_id", "region", "price", "qty"]
orders["price"]       # => Prism::Series
puts orders.head(2)   # aligned table with dtypes and nulls
```

**Mind the parentheses.** `&` binds tighter than `==` in Crystal, so
`(col("a") == 1) & (col("b") == 2)` is required. `col("a").eq(1).and(col("b").eq(2))`
is the precedence-safe spelling, and `!` cannot be overloaded — negation is
`.not`.

### Rows

Columns are the fast path; rows are a materialization edge. `rows`, `row` and
`each_row` hand back the same `Prism::Value` shapes as `Series#to_a` — nulls are
`nil`, nested cells are containers — in one name-keyed hash per row:

```crystal
orders.rows.first              # => {"order_id" => 1, "region" => "east", ...}
orders.row(1)["region"]        # => "west"
orders.each_row { |row| ... }  # one hash per row, one chunk at a time
```

A declared schema gives the same rows nominal fields instead of string keys
(see [Compile-time typed columns](#compile-time-typed-columns)).

### Masks from columns

`Series` comparison operators build a boolean column, and `filter` takes one
directly — the RedAmber spelling, without leaving the expression API:

```crystal
orders.filter(orders["price"] > 15)   # same rows as filter(col("price") > 15)
orders["price"] > 15                  # => boolean Series, nulls propagate
```

A scalar adopts the column's dtype when the cast is lossless (`Int32` column,
`Int64` literal) and widens both sides when it is not (`1.5` against an integer
column), which is the rule expressions follow too.

### Conditionals

`if_else` picks per row and `case_when` chains branches, so derived columns do
not need a callback verb:

```crystal
orders.with_column("label", Prism.if_else(col("qty") > 10, "bulk", "single"))
orders.with_column("band",
  Prism.case_when(
    [{col("price") >= 90, lit("high")}, {col("price") >= 50, lit("mid")}],
    lit("low"),
  ))
```

A null condition takes the else branch (Arrow's `zip` reads a null mask as
false, and Polars' `when/then/otherwise` does the same), so a null flag never
invents a null result — and a null condition in `case_when` matches no branch.
Branches unify through the same coercion rule as arithmetic (`Int64` against
`1.5` runs in `Float64`), and a non-boolean condition is a `Prism::DtypeError`
before any data is read.

### RedAmber-style ergonomics

Filters and sorts accept the shapes RedAmber made familiar, without leaving the
expression API:

```crystal
orders.filter(orders["price"] > 15)     # a boolean Series is a mask
orders.remove(col("region").is_null)    # keep rows rejected *or* unjudgeable
orders.sort_by("-price")                # descending shorthand
orders.top_n(10, "price")               # 10 largest, bounded sort (fast path)
orders[1..2]                            # rows by range; negatives and .. work
orders[["region", "price"]]             # columns by name
orders.group_by("region").sum("price")  # one-call aggregate, input name kept
orders.group_by("region").mean          # every numeric column
```

`remove` is not `filter(predicate.not)`: a row is kept when the predicate is
false **or null**, so rows the predicate cannot judge survive.

`top_n` is the eager spelling of `sort_by(...).head(n)`: it defaults to
`order: :desc` (top means largest, as in Polars' `top_k`), keeps nulls last
unless told otherwise, and sorts only as far as the window — 8 ms against 194 ms
for the two-call form on 1e6 rows, because `sort_by` cannot see the `head` that
follows it. Through a plan, `lazy.sort_by(...).head(n)` already fuses.

One-call aggregates keep the input column's name, which means no aliasing;
`group_by(k).count` with no columns is Polars' `count` — one `count` column of
row counts per group.

### Lazy queries and pushdown

```crystal
plan = Prism.scan_parquet("data/sales.parquet")
  .filter(col("qty") > 2)
  .select("region")
  .explain

# Plan (optimized):
#   Select [region]
#     ScanParquet "data/sales.parquet"
#       projection: [region, qty]
#       predicate: gt(col:qty,lit:2)

rows = Prism.scan_parquet("data/sales.parquet")
  .filter(col("qty") > 2)
  .select("region")
  .collect
```

`df.lazy` is the in-memory entry (`Prism.scan_parquet` reads from a file), verbs
build plan nodes, and `collect` is the only execution point.

### Compile-time typed columns

```crystal
Prism.define_schema UserSchema, id: Int64, name: String, active: Bool, salary: Float64

typed = UserSchema.from_arrow(batch)
typed.filter(&.active).select(&.salary)   # a misspelt field is a compile error

typed.rows.first.salary                   # => Float64? — a nominal row
typed.row(1)                              # => UserSchema::Row
typed.row(1).to_h                         # => {"id" => 2, "name" => "bob", ...}
```

### Interoperability

```crystal
batch = Arrow::RecordBatch.from_columns({id: ids, price: prices})
df = Prism.from_arrow(batch)
df.to_arrow == batch   # schema, nullability and values preserved
```

## Performance, measured

`make bench` on one 1e6-row frame (Apple M4, Crystal 1.22.2, best of five). Full
table and caveats: [docs/benchmarks.md](docs/benchmarks.md).

| Path | Time |
| --- | ---: |
| `filter` on half the rows | 53 ms |
| `select` / `to_arrow` (one chunk) | 0 ms — same buffers |
| `sort_by` one key | 229 ms |
| `sort_by` two keys | 402 ms — stable pass per key when one is `Utf8` |
| `sort_by` + `head(10)` | 225 ms — two verbs, both full-width |
| `top_n(10)` / `lazy.sort_by(...).head(10)` | 9 ms — bounded sort + window |
| `group_by` 4 groups / 1e5 keys / 2 keys | 238 ms / 205 ms / 558 ms |
| `distinct` / `n_unique` on 1e5 keys | 61 ms / 52 ms — `arrow-row` keys |
| join 1e6 → 1e5 | 224 ms |
| Parquet read all / 2 of 4 columns / `limit 100` | 134 ms / 84 ms / 4 ms |
| Lazy scan+filter+select vs eager | 135 ms vs 159 ms |

## Documentation

| | |
| --- | --- |
| [Coming from Polars](docs/polars-parity.md) | Verb and expression map, plus the semantics that differ on purpose |
| [Architecture](docs/architecture.md) | Chunking, zero-copy rules, expressions, grouping, joins, IO, memory |
| [Benchmarks](docs/benchmarks.md) | Scenarios, numbers, and what they decide |
| [Testing](docs/testing.md) | Spec layout, oracles, compile-time and example specs |
| [Development](docs/development.md) | Dependencies, layout, environment notes |
| [Coding guidelines](docs/coding-guidelines.md) | Style and review expectations |
| [PR workflow](docs/pr-workflow.md) | Branch, gates, commit shape |
| [Resuming work](docs/resuming.md) | Start here after a break |
| [Changelog](CHANGELOG.md) | What changed, release by release |
| [Implementation plan](plans/plan.md) | Phase status and recorded design decisions |

## Examples

Runnable versions of the snippets above live in [`examples/`](examples/):

```bash
make examples                        # quickstart + Parquet pushdown; prints tables
crystal run examples/quickstart.cr   # in-memory: expressions, group_by, join
crystal run examples/parquet.cr      # write/read, projection + predicate pushdown
crystal run examples/viewer.cr       # interactive table browser; `q` quits
```

The viewer needs the development dependencies (`make install`). Every example is
compiled or run by `spec/examples_spec.cr` (`PRISM_EXAMPLE_SPECS=1`), so a README
snippet that stops working fails a spec.

## Status

`0.2.0` is the current release (`v0.2.0`). It adds row materialization
(`DataFrame#rows` and a generated typed row per schema), interval columns,
conditionals (`if_else`/`case_when`), vectorized grouped aggregation, a rewritten
sort engine with a bounded window, and a [Polars mapping](docs/polars-parity.md).
The API in this README is implemented, tested (498 specs plus opt-in compile and
example suites) and measured.
Deliberately out of scope: no index/row labels, no pivot/melt, no window
functions, no SQL or string-expression parser, no multi-threaded engine, and no
async/object store IO. Every Arrow dtype the library accepts materializes into a
`Prism::Value`, though nested cells come back as containers (`Array(Value)`,
`Hash`) rather than per-schema structs unless you use a typed schema. The
[plan's deferred table](plans/plan.md#17-deferred-work) lists every known gap with
the phase that would close it.

## Contributing

1. Read [`plans/plan.md`](plans/plan.md) and pick the next unchecked item (or an
   item from the deferred table).
2. Follow [`docs/pr-workflow.md`](docs/pr-workflow.md): one feature, one atomic
   commit, gates green.
3. `make lint && make test` before opening a pull request; `make examples` and
   `make bench` if you touched those paths.

## Acknowledgements

The design borrows one axis from each of: [Polars](https://github.com/pola-rs/polars)
(expression engine, lazy plan, pass ordering), [Kotlin
DataFrame](https://github.com/Kotlin/dataframe) (compile-time schemas),
[RedAmber](https://github.com/heronshoes/red_amber) (block-first verbs over
Arrow), [Arquero](https://github.com/uwdata/arquero) (verb vocabulary) and
[Explorer](https://github.com/elixir-explorer/explorer) (dplyr verbs over a lazy
backend). What was adopted from each — and what was deliberately not — is in
[plan §4.1](plans/plan.md#41-design-influences).

## License

MIT. See [LICENSE](LICENSE).
