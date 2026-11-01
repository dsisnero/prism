# Prism

A DataFrame library for Crystal, built on
[`arrow-cr`](https://github.com/dsisnero/arrow-cr) (Apache Arrow) and
[`parquet-cr`](https://github.com/dsisnero/parquet-cr) (Apache Parquet).

`Prism` gives you a column-oriented table over Arrow record batches, with an
inspectable expression language, grouped aggregation, Parquet/CSV/IPC
interchange, and a lazy query plan that pushes projections and predicates into
the Parquet reader — so a large file can be filtered without being loaded.

> **Status: pre-alpha.** The API in this README is the target design and is not
> implemented yet. See [`plans/plan.md`](plans/plan.md) for the phase-by-phase
> status.

## Installation

```yaml
dependencies:
  dataframe:
    github: dsisnero/dataframe
```

```bash
shards install
```

Requires Crystal 1.22.2 or newer.

## Usage

```crystal
require "dataframe"

include Prism::DSL   # enables bare col(...) / lit(...)

df = Prism.read_parquet("data/sales.parquet")

df.shape           # => {rows: 120_000, columns: 7}
df.column_names    # => ["order_id", "region", "status", "price", ...]
df["price"]        # => Prism::Series
df.head(3)

summary = df
  .filter { (col("status") == "completed") & (col("price") > 10.0) }
  .select("order_id", "region", "price", "quantity")
  .with_column("total_amount", col("price") * col("quantity"))

regional = df
  .group_by("region", "category")
  .aggregate(
    total_revenue: col("total_amount").sum,
    avg_price:     col("price").mean,
    order_count:   col("order_id").count,
  )
  .sort_by("total_revenue", order: :desc)

# Lazily: nothing is read until collect, and the filter/select reach the
# Parquet reader as a row filter and a projection mask.
report = Prism.scan_parquet("data/sales.parquet")
  .filter(col("created_at") >= Time.utc(2026, 1, 1))
  .select("user_id", "department", "salary")
  .group_by("department")
  .aggregate(mean_salary: col("salary").mean)
  .collect

puts Prism.scan_parquet("data/sales.parquet").filter(col("price") > 10).explain
```

Boolean expressions need parentheses: `&` binds tighter than `==` in Crystal, so
`(col("a") == 1) & (col("b") == 2)` is required. `col("a").eq(1).and(col("b").eq(2))`
is the precedence-safe alternative.

### Compile-time typed columns

```crystal
Prism.define_schema UserSchema, id: Int64, name: String, active: Bool, salary: Float64

typed = UserSchema.from_arrow(record_batch)
typed.filter(&.active).select(&.salary)   # a misspelt field is a compile error
```

### Interoperability

Every boundary is an Arrow type: `Prism::DataFrame` wraps
`Array(Arrow::RecordBatch)` with one `Arrow::Schema`, and `#to_arrow` returns a
`Arrow::RecordBatch`. There is no separate schema dialect.

```crystal
batch = Arrow::RecordBatch.from_columns({id: ids, price: prices})
df = Prism.from_arrow(batch)
df.to_arrow == batch   # schema and values preserved
```

## Documentation

- [Implementation plan](plans/plan.md) — current phase and design decisions
- [Architecture](docs/architecture.md)
- [Development](docs/development.md)
- [Coding Guidelines](docs/coding-guidelines.md)
- [Testing](docs/testing.md)
- [Resuming work](docs/resuming.md)
- [PR Workflow](docs/pr-workflow.md)
- [Changelog](CHANGELOG.md)

## Development

```bash
make install
make test
```

See [docs/development.md](docs/development.md) for the sibling-checkout layout,
dependency wiring, and environment notes.

## Contributing

1. Read [`plans/plan.md`](plans/plan.md) and pick the next unchecked item.
2. Follow [docs/pr-workflow.md](docs/pr-workflow.md).
3. Fork, branch, and open a pull request.

## Contributors

- [Dominic Sisneros](https://github.com/dsisnero) — creator and maintainer

## License

MIT. See [LICENSE](LICENSE).
