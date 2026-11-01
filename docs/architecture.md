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

## What stays in Arrow, and where it stops

The rule: **data stays in Arrow buffers until an edge forces materialization.**
Kernels belong to Arrow; this library's job is to not add copies.

### Edges (allowed to allocate)

| Edge | Why |
| --- | --- |
| `Series#to_a`, `Series#each`, `Series#[]` | Crystal values are not Arrow buffers |
| `Series#to_arrow`, `DataFrame#to_arrow` | one contiguous array; concat when chunked |
| `DataFrame#to_s` | formatting needs per-value strings |
| file writers | encoding walks values |
| `filter`, `sort_by`, `group_by`, `join` | the output is a different row set |

### Never allocates

- `DataFrame#[]` — a series is a list of references to batch columns.
- `DataFrame#slice` / `#head` / `#tail` — `RecordBatch#slice` adjusts offset and
  length only; `head(n)` and `tail(n)` return `self` when `n` covers the frame.
- `select` / projection — `RecordBatch#project` reuses the columns.
- `Series#size`, `#count`, `#null_count` — summed from chunk headers.
- `Series#sum`, `#mean`, `#min`, `#max` — per-chunk kernel, folded scalars.
- `Series#each_chunk`, `DataFrame#each_batch` — streaming access to what exists.

`Series` holds `Array(Arrow::ArrayBase)` rather than a chunked-array type because
`arrow-rs` deliberately has none and recommends exactly this representation.

### What is actually zero-copy (Arrow's rules, not ours)

- `RecordBatch#slice` and `#project` are **views** — no buffer copy.
- `Arrow.filter` and `Arrow.take` **allocate** new value buffers; they must,
  because the output is a different row set. Untouched columns are not copied,
  which is what `filter_record_batch` exploits.
- `Arrow.concat_batches` allocates.

So `select` on a filter result copies only the selected arrays, and `select`
alone copies nothing. This library adds no copies of its own beyond the edges.

### Why not Arquero-style deferred filtering

Arquero's `filter` returns a `BitSet` mask over shared columns, so it is
effectively lazy without saying so. Every later operation then has to honor the
mask. We keep the boundary explicit instead: eager verbs materialize (Arrow's
semantics), and `LazyFrame` is where deferral and Parquet pushdown live. Revisit
if benchmarks show filter-dominated workloads.

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
are called, because Arrow's kernels require matching dtypes. When one side is a
literal, the literal is cast to the column's dtype (matching Polars) instead of
widening the column, so a narrow column is never copied into a wider buffer.

Boolean `and`/`or` use Arrow's Kleene kernels: a known `false` wins over null in
AND, matching SQL and Polars. Negation is `.not` — Crystal's `!` is a
pseudo-method and cannot be overloaded. `&`/`|` are available but bind tighter
than comparison operators, so operands must be parenthesised.

Literals evaluate to a length-1 `Arrow::Scalar` rather than a broadcast array, so
`col("price") * 2` does not build an n-element array of 2s. Arrow's kernels
reject a union of array and scalar, so `Prism::Kernels` narrows the union and
calls a generic `apply` instantiated per operand pair.

Schema-dependent work happens **once per execution**, not once per chunk.
`Prism::TypeCache` resolves output dtypes and coercion targets keyed by
`Expr#key`; verbs build one and share it across every chunk's `EvalContext`.
Polars achieves the same by compiling a physical expression once.

Repeated subexpressions are computed once per chunk. `Prism::Cse` counts canonical
keys across a verb's expressions, and `EvalContext#evaluate` memoizes only the
keys that occur more than once — the same saving as Polars' CSEE without
rewriting the plan.

## Filtering

`DataFrame#filter` mirrors the three cases in Polars' `filter_with_bitmap`:

- all rows match → the batch is returned untouched;
- no rows match → an empty view, kernel skipped;
- otherwise → `Arrow.filter_record_batch`.

Boolean identities short-circuit before evaluation: `x & false` and `x | true`
produce a constant bitmap (n/8 bytes) without evaluating `x`.

Nulls are Arrow validity bitmaps (`NullBuffer` over `BooleanBuffer`), and
`BooleanArray#true_count` is a bitmap AND plus popcount, so deciding which case
applies is cheap. Arrow's filter already treats a null mask bit as false, which
is why an all-null predicate drops every row without special handling.

## Verbs

Eager verbs split into two groups by cost:

| Group | Verbs | Effect |
| --- | --- | --- |
| Element-wise | `select`, `filter`, `with_column(s)`, `drop`, `rename`, `cast`, `fill_null`, `drop_nulls`, `with_row_index` | map over chunks, preserve chunking, reuse untouched arrays |
| Row-reordering | `sort_by`, `distinct` | collapse to one batch, since the output row set differs |

`select` projects a bare column without copying it; only computed expressions
produce new arrays. `rename` changes only the schema and re-wraps the same
arrays — Arrow's `RecordBatch#with_schema` cannot express a rename (it demands a
superset), so the batch is rebuilt around the existing columns.

Named-argument verbs (`with_columns`) use Crystal's `**named` parameter, not a
macro: a macro expands at the call site without the receiver. `select` is a
Crystal keyword, so internal calls spell `self.select`.

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
  -> per-range aggregate on a slice view -> Arrow.concat the scalars
```

This is Polars' sort-based path (`GroupsType::Slice`, contiguous `[start, len]`
ranges). Polars hashes by default and sorts only when keys are already sorted; we
always sort, which buys:

- **View-based groups.** Each group is `RecordBatch#slice(begin, size)` — offset
  and length only. No index list, no subarray copy.
- **One sort per `aggregate` call.** Every aggregation walks the same ranges.
- **Deterministic order.** `maintain_order: true` (default) restores discovery
  order by tracking each group's smallest original row index; `false` leaves the
  sorted order.

Cost is O(n log n) per grouping rather than O(n). A hash path is possible later
(Polars' `Idx` representation); the tradeoff would be losing zero-copy group
views.

Aggregations run through `Prism::Series` and Arrow kernels on the group slice:
`sum`/`mean`/`min`/`max` via `arrow-cr` aggregate kernels, `first`/`last` as
array slices (never materialized), `variance`/`std` as
`sum((x - mean)^2) / (n - ddof)` with `sub`/`mul`/`sum`, and `median` via
`sort` plus the middle row. In-memory aggregation specializations that Polars
uses for rolling windows are out of scope.

Null keys form one group. `null_keys: :first | :last` controls placement
(:first matches Arrow's `ASC NULLS FIRST` default); `:drop` filters null-key rows
before grouping.

## Distinct

`arrow-cr` has no dedupe kernel, and `ArrayBase#uniq` is a Crystal-level sort
driven by a `DynComparator` closure — correct, but it allocates an index per row
and pays an indirect call per comparison. `DataFrame#distinct`, `Series#unique`,
and the `n_unique` aggregate therefore use the Arrow-native primitives:
`sort_to_indices` (typed per-dtype sorts), `Arrow.partition` (typed
`cmp::distinct` on adjacent rows), and `Arrow.take`.

Keeping the *first* occurrence is not free: Arrow's sort is not guaranteed
stable, so a run's first sorted position is not necessarily its earliest row.
`Prism::Distinct.first_occurrence_indices` takes the smallest original index per
run and sorts those ascending to restore input order.

Cost is O(n log n). Polars' `unique_stable` is O(n) with a Rust hash set; matching
that would need Arrow-native hashing, which this dependency does not expose.

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
