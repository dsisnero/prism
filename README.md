# Prism

**A DataFrame library for Crystal, built directly on Apache Arrow.**

[![Crystal](https://img.shields.io/badge/crystal-%E2%89%A5%201.22.2-000000.svg)](https://crystal-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Arrow](https://img.shields.io/badge/storage-Apache%20Arrow-0faa92.svg)](https://arrow.apache.org)

Prism is a column-oriented table over `Arrow::RecordBatch`, with an inspectable
expression language, grouped aggregation, Parquet/CSV/IPC interchange, and a lazy
query plan that pushes projections and predicates **into the Parquet reader** — so
a large file can be filtered without being loaded.

It is not a wrapper around another dataframe. Every boundary type is an
`arrow-cr` type: `Prism::DataFrame` holds `Array(Arrow::RecordBatch)` and one
`Arrow::Schema`, and `#to_arrow` hands back an `Arrow::RecordBatch`. There is no
second schema dialect to keep in sync.

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
  accessors, so `typed.select(&.salary)` with a misspelt field is a build error.

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
| `filter` on half the rows | 48 ms |
| `select` / `to_arrow` (one chunk) | 0 ms — same buffers |
| `sort_by` one key | 202 ms |
| `sort_by` + `head(10)` | 130 ms — gathers only the window |
| `group_by` 4 keys / 1e5 keys | 220 ms / 381 ms |
| `distinct` / `n_unique` on 1e5 keys | 54 ms / 48 ms — `arrow-row` keys |
| join 1e6 → 1e5 | 219 ms |
| Parquet read all / 2 of 4 columns / `limit 100` | 117 ms / 74 ms / 3 ms |
| Lazy scan+filter+select vs eager | 119 ms vs 142 ms |

## Documentation

| | |
| --- | --- |
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

`v0.1.0` is the first release: the API in this README is implemented, tested
(335 specs plus opt-in compile and example suites) and measured. Deliberately out
of scope for v0.1: no index/row labels, no pivot/melt, no window functions, no
SQL or string-expression parser, no multi-threaded engine, and no async/object
store IO. The [plan's deferred table](plans/plan.md#17-deferred-work) lists every
known gap with the phase that would close it.

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
