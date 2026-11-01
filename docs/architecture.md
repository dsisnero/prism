# Architecture

`Prism` is a thin, opinionated layer over `arrow-cr` and `parquet-cr`. It adds
three things those shards do not have: a multi-chunk table type, an inspectable
expression language, and a query plan that can push work into Parquet readers.
Everything else — types, kernels, encodings, file formats — belongs to the
dependencies.

## Layering

```text
  user code            df.filter { (col("a") > 1) & (col("b") == "x") }
                              |
  verbs                DataFrame#filter, #select, #group_by, #join, ...
                              |
  expressions          Expr AST -> dtype(schema) / eval(batch)
                              |
  kernels              Arrow.filter_record_batch, take_record_batch,
                       lexsort_to_indices, partition, cast, add/mul, aggregates
                              |
  storage              Array(Arrow::RecordBatch) + Arrow::Schema
                              |
  IO                   parquet-cr: ArrowWriter, ParquetRecordBatchReaderBuilder
```

Each layer only knows about the one below it. Verbs never touch Parquet; the
lazy optimizer never touches buffers.

## Type map

| Prism | Representation | Notes |
| --- | --- | --- |
| `DataFrame` | `Array(Arrow::RecordBatch)` + `Arrow::Schema` | Arrow has no table type |
| `Series` | `Array(Arrow::ArrayBase)` + name + `Arrow::DataType` | 1-D column, one chunk per batch |
| `Expr` | immutable AST (abstract struct) | typeable without data |
| `GroupBy` | `DataFrame` + key names + options | executes on `aggregate` |
| `LazyFrame` | plan node tree | nothing read until `collect` |
| `Schema` (typed) | macro-generated struct | `define_schema` |

There is no `Prism::DataType`. Data types are `Arrow::DataType` values, schemas
are `Arrow::Schema` values, and columns are `Arrow::ArrayBase` values. This is
deliberate: any conversion layer would be a second dialect to keep in sync, and
it would tax every interop path.

## Chunking

A `DataFrame` holds one or more batches that all share a schema. Chunking is
preserved or destroyed on purpose:

| Operation class | Chunking | Cost |
| --- | --- | --- |
| `select`, `filter`, `with_column`, `cast`, `drop`, `rename` | preserved (map per batch) | no concatenation; untouched buffers shared |
| `sort_by`, `group_by`, `distinct`, `join`, `concat` | collapsed to one batch | `Arrow.concat_batches` |
| `head`, `tail`, `slice` | trims leading/trailing chunks | `RecordBatch#slice` is a view |
| `to_arrow` | collapses | one allocation |

`#each_batch` exposes the chunks for streaming and `#to_arrow` collapses; users
choose. Zero-column and empty frames are legal; `Arrow.concat_batches` handles
both (it sums `num_rows` for an empty field list and returns
`RecordBatch.new_empty(schema)` for an empty chunk list).

## What is actually zero-copy

Arrow's own rules, which the plan's original sketch overstated:

- `RecordBatch#slice` and `#project` are **views** — no buffer copy.
- `Arrow.filter` and `Arrow.take` **allocate** new value buffers (they must:
  the output is a different row set). They do not copy *untouched columns*,
  which is what `filter_record_batch` exploits.
- `Arrow.concat_batches` allocates.

So `select` on a filter result copies only the selected arrays, and `select`
alone copies nothing. This library adds no copies of its own beyond `#to_arrow`.

## Expressions

```crystal
abstract struct Expr
  abstract def key : String                                 # canonical form
  abstract def columns : Array(String)                       # referenced names
  abstract def dtype(schema : Arrow::Schema) : Arrow::DataType
  abstract def eval(ctx : EvalContext) : Arrow::ArrayBase
end
```

Nodes: `Col`, `Lit`, `Unary`, `Binary`, `Agg`, `Cast`, `Alias`.

- `dtype(schema)` is total and cheap. It is what makes coercion, typed schemas,
  and plan-time schema propagation possible without reading any data.
- `eval` sees a single `RecordBatch` (plus optional group ranges), never a whole
  table. Aggregates return length-1 arrays that the group executor broadcasts.
- Expressions have no side effects, so the optimizer may reorder, duplicate, or
  drop them freely.

Numeric coercion lives in `expression/coerce.cr` and is applied before kernels
are called, because Arrow's kernels require matching dtypes.

## Execution

Eager verbs execute immediately against the chunks. Lazy execution builds a plan
and runs optimizer passes before executing:

1. predicate pushdown (splitting `&`-conjuncts, converting scan-compatible
   conjuncts into `Parquet::Arrow::RowFilter` predicates);
2. projection pruning (into `ProjectionMask.columns`);
3. limit/offset pushdown (`with_limit`, `with_offset`);
4. constant folding;
5. dead `WithColumn` elimination.

`explain` shows both plans and marks which projections and predicates reached
the scan. Tests assert on that output so a silently disabled pass fails CI.

## Grouping

Arrow provides sort and partition, not group-by:

```text
lexsort_to_indices(keys) -> take_record_batch -> partition(keys) -> ranges
  -> per-range aggregate Expr evaluation -> assemble result columns
```

Consequences worth knowing:

- Group order follows the sort, so results are deterministic (unlike a hash
  group). `group_by` output is sorted by keys unless documented otherwise.
- Null keys form one group; the default policy places it first, matching Arrow's
  `ASC NULLS FIRST` default.
- Cost is O(n log n) per grouping, not O(n). A hash-based path is a possible
  later optimization.

## Parity and divergence

`arrow-cr` is a behaviour-faithful port of `arrow-rs`; `parquet-cr` likewise for
the `parquet` crate. This library therefore inherits upstream semantics wherever
it forwards a call, including null propagation, IEEE total ordering for floats,
and overflow behaviour. Where the DataFrame layer must choose (null-key policy,
join suffixing, dtype coercion), the choice is documented in
[`plans/plan.md` §8](../plans/plan.md#8-semantics) and covered by a spec.

## Extension points

Adding a verb means: a method on `DataFrame` (chunk-wise or collapsing), an
`Expr`-friendly signature, a spec, and — if it should be lazy — a plan node plus
`schema` propagation. Adding an aggregate means: a method returning an `Agg`
node, a `dtype` rule, and an executor case. Neither requires touching the
dependency shards.
