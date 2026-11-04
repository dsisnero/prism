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
| `RowHash` | `Hash(String, Value)` | one materialized row, column order kept |
| `S::Row` (typed) | macro-generated struct | nominal row for a declared schema |
| `Value` | recursive alias: scalars (including `Time`, `Time::Span`, `BigDecimal`, `Arrow::IntervalValue`) and `Array(Value)` / `Hash` containers | nested and interval columns materialize at the row edge |

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
| `DataFrame#rows`, `#row`, `#each_row` | one hash per row; one chunk of values at a time |
| `S::Frame#rows`, `#row`, `#each_row` | same, into the declared row struct |
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

### Rows: two boundaries, one shape

Columns are the primary representation; rows are a convenience at the edge, and
there are exactly two row shapes:

| Boundary | Shape | Why |
| --- | --- | --- |
| Dynamic (`DataFrame#rows`, `#row`, `#each_row`) | `Prism::RowHash` = `Hash(String, Value)` | the schema is runtime data, so the row type cannot be nominal |
| Declared (`define_schema`) | `S::Row`, fields typed `T?` | here the schema *is* compile-time data, so the row can have real accessors |

A generated `S::Row` is possible only because the field list is known at compile
time; `Values.extract` sees an `Arrow::DataType` at run time and must return one
closed type for every frame (that is what keeps `Value` a single alias rather
than an open union of user structs). Fields are nullable because
`define_schema` declares every Arrow field nullable, so a null row value has
somewhere to go. `S::Row.from_hash` re-enters the typed row from the dynamic
one, and `S::Frame#rows` streams chunk by chunk — a column is materialized once
per batch, never the whole frame.

### Memory: what `#to_arrow` costs

`#to_arrow` is the one place a chunked frame becomes one contiguous batch, and it
is where an unwary pipeline pays:

- **One chunk: free.** `Arrow.concat_batches` returns a batch over the existing
  arrays, so the columns are the *same* objects (pinned by a spec). A `Series`
  over one chunk returns that chunk outright.
- **Many chunks: one full copy.** Values and validity buffers for every column
  are concatenated into fresh allocations. A 1e6-row frame of four columns that
  arrived as 1k-row batches allocates a second copy of the whole table (for the
  bench frame, ~24 MB: 8 MB `Int64` + 8 MB `Float64` + offsets/payloads for two
  string columns), and both copies are live until the frame is dropped.
- **Zero rows: an empty batch,** no allocation.

Row-reordering verbs (`sort_by`, `group_by`, `distinct`, `join`, `concat`) call
it, because the output is a different row set anyway. Element-wise verbs never
do. So the way to stay inside one chunk is to read with a large `batch_size`,
and the way to avoid the copy is to consume `#each_batch` in a loop instead of
building a `DataFrame` over many chunks.

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

Nodes: `Col`, `Lit`, `Binary`, `Neg`, `Not`, `IsNull`, `IsNotNull`, `Cast`,
`Alias`, `FillNull`, `Coalesce`, `IsIn`, `Agg` and `IfElse`.

- `dtype(schema)` is total and cheap. It is what makes coercion, typed schemas,
  and plan-time schema propagation possible without reading any data.
- `eval` returns a `Datum` (array or length-1 scalar) for a single
  `RecordBatch` (plus optional group ranges), never a whole table. Aggregates
  return length-1 arrays that the group executor broadcasts.
- Expressions have no side effects, so the optimizer may reorder, duplicate, or
  drop them freely.

Numeric coercion lives in `expression/coerce.cr` and is applied before kernels
are called, because Arrow's kernels require matching dtypes. When one side is a
literal, the literal is cast to the column's dtype (matching Polars) instead of
widening the column, so a narrow column is never copied into a wider buffer.

Set membership is one node, `IsIn`, holding a **constant** set encoded once at
construction (`Arrow.array(values)`), so a 12k-element list is one array in the
expression rather than a 12k-term `|` chain. `key` carries the set's length and
a hash of its values instead of the values themselves, which keeps plans,
`explain` and CSE keys short while still distinguishing two sets of the same
size. The node reads only its operand's columns, so predicate pushdown projects
them and nothing else; a null cell yields null unless `nulls_equal: true`,
matching Polars.

`Series#is_in` is the mask form of the same operation and shares
`Prism::Membership` with the node, so coercion, the null rule and the kernel
(`ArrayBase#is_in`, a hash probe since `arrow-cr` `78b0cd4`) cannot drift between
`df.filter(col("x").is_in(set))` and `df.filter(df["x"].is_in(set))`. One kernel
for one operation is the point: Prism has no membership loop of its own.

A dictionary column compares through its **values**: `Membership.target` unifies
the decoded dtypes and the cast decodes each side, which is the same rule the
keyed verbs follow (`Prism::RowKeys` encodes a dictionary's values). Without it,
`col("cat").is_in(["a"])` would report `Dictionary(Int32, Utf8)` vs `Utf8` and
tell the caller to cast one side — advice a set literal cannot take.

Conditionals are one node, `IfElse`, and a chain of them: `Prism.if_else` maps
straight onto `Arrow.zip`, and `Prism.case_when` folds its branches into nested
`if_else` calls with the `otherwise` value at the bottom. A null condition
selects the else branch — `zip` reads a null mask as false, and Polars'
`when/then/otherwise` agrees — so nesting means "first branch that matches" rather
than propagating nulls. Branch dtypes unify through `Prism::Coerce`, which is the
same rule arithmetic uses, and a non-boolean condition is rejected by
`dtype(schema)` before any data is read.

`fill_null` follows the same family rule as literal coercion, through
`Coerce.fillable?`: a replacement fills a column only when the two are in one
family and the value does not narrow (`fill_null(0)` fills integer and float
columns and leaves strings, booleans and lists alone; `fill_null(1.5)` leaves an
integer column alone). `Arrow.can_cast_types` is *not* that rule — it says yes to
an `Int64` into `Utf8` and to a scalar into `List<Int64>`, which is how a
frame-wide fill once turned a null string into `"0"` and a null list into `[0]`.
A named replacement that cannot fill its column raises `Prism::DtypeError`
instead of being coerced.

Boolean `and`/`or` use Arrow's Kleene kernels: a known `false` wins over null in
AND, matching SQL and Polars. Negation is `.not` — Crystal's `!` is a
pseudo-method and cannot be overloaded. `&`/`|` are available but bind tighter
than comparison operators, so operands must be parenthesised.

Comparisons against a literal that cannot fit the column's dtype fold to a
constant: `is_null ? null : constant`. Without that fold the safe cast turns the
overflowing literal into null and every row compares as null. Arithmetic with
such a literal raises instead of nulling rows.

Decimals compare at a common scale and materialize as `BigDecimal`, preserving
scale; decimal arithmetic follows Arrow's Hive rules, with each kernel's result
precision and scale declared in `Prism::Coerce` so the planned and produced
dtypes agree.

Intervals materialize as `Arrow::IntervalValue` (`months`, `days`, `nanos`) for
all three Arrow units: year-month keeps its months, day-time converts its
milliseconds to nanoseconds, and month-day-nano is already that shape. The unit
is not part of the value, so a year-month interval renders as months (`14 mons`)
rather than Arrow's `1 years 2 mons`; cast the column to `Utf8` when the
unit-specific Arrow text is needed. Rows and display therefore stay one closed
type for every frame (§10 of the plan).

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

### Mask filtering, and `remove`

`filter` also takes a `Prism::Series` of booleans, which is what the `Series`
comparison operators build (`df.filter(df["price"] > 10)`). The same three fast
paths apply: when the mask's chunks line up with the frame's — which they do for
a mask derived from the frame's own columns — each chunk is filtered in place
and the chunk count survives; otherwise the frame is consolidated once so the
mask aligns. A mask is validated for dtype and length up front, so a wrong mask
fails with a `Prism::DtypeError` naming the column or an `ArgumentError` naming
both lengths.

`remove` is the inverse of `filter`, and it needs its own null rule: a row is
kept when the predicate is false **or null**. `filter(expr.not)` is not
equivalent, because `not(null)` is null and a null predicate drops its row, so
rows the predicate cannot judge would disappear. `remove` therefore evaluates
`expr.not.or(expr.is_null)`; the two references to `expr` share a canonical key,
so per-chunk memoization evaluates it once. The mask form inverts the mask and
turns its null bits into `true`, for the same reason.

## Verbs

Eager verbs split into two groups by cost:

| Group | Verbs | Effect |
| --- | --- | --- |
| Element-wise | `select`, `filter`, `with_column(s)`, `drop`, `rename`, `cast`, `fill_null`, `drop_nulls`, `with_row_index` | map over chunks, preserve chunking, reuse untouched arrays |
| Row-reordering | `sort_by`, `sort_by_slice`, `distinct` | collapse to one batch, since the output row set differs |

A frame's column names are unique, and every verb that builds a schema by name
enforces it: `select` (a repeated name or a colliding alias), `rename` (on the
result, so a swap stays legal), `group_by` (the same key twice, or an aggregate
named after a key), `hstack` and join suffixing. Each raises
`Prism::DuplicateColumn` naming the column, because a frame with two columns of
one name loses one of them to `df[name]` while `to_s`, `to_arrow` and a Parquet
write all succeed.

`select` projects a bare column without copying it; only computed expressions
produce new arrays. `rename` changes only the schema and re-wraps the same
arrays — Arrow's `RecordBatch#with_schema` cannot express a rename (it demands a
superset), so the batch is rebuilt around the existing columns. `sort_by_slice`
is the windowed form used by a pinned slice: it sorts, then gathers only the
requested range of the sort indices.

Named-argument verbs (`with_columns`) use Crystal's `**named` parameter, not a
macro: a macro expands at the call site without the receiver. `select` is a
Crystal keyword, so internal calls spell `self.select`.

### Ergonomics borrowed from RedAmber

Four spellings from RedAmber's surface exist without breaking the expression API:

| Spelling | Equivalent | Note |
| --- | --- | --- |
| `df[1..3]`, `df[..1]`, `df[-2..]` | `df.slice(...)` | Ruby `Array#[]` index rules: negative endpoints count from the end and `...` excludes its end. Bounds clamp instead of returning `nil` — a frame is always a frame. |
| `df[["a", "b"]]` | `df.select("a", "b")` | A `Range` is `Enumerable`, so the row overload has to declare its type parameters (`Range(B, E) forall B, E`) to outrank the column-list one in dispatch. |
| `df.sort_by("-price")` | `df.sort_by("price", order: :desc)` | The prefix is read in `Prism.sort_key`, so eager and lazy verbs resolve it identically. A column whose name really starts with `-` is reachable only as an expression, the documented cost of the sugar. |
| `df.remove(pred)` | `df.filter(pred.not.or(pred.is_null))` | Not `filter(pred.not)`; see the null rule above. |

`group_by` also gained the one-call aggregates (`sum`, `mean`, `min`, `max`,
`count`, `n_unique`, `median`, `std`, `var`). Output columns keep their **input**
names, which is Polars' rule, so no aliasing is needed; with no column names the
aggregate covers every eligible column and never a grouping key. `count` with no
columns is Polars' `count`: one `count` column holding row counts per group.

Bare column names inside blocks — RedAmber's biggest ergonomic win — cannot be
copied: it relies on Ruby's `instance_eval`, and Crystal dispatches statically.
The typed frames (`typed.filter(&.active)`) are this library's answer, because
there the schema is known at compile time.

## Execution

Eager verbs execute immediately against the chunks. `DataFrame#lazy` and
`Prism.scan_parquet` build a `Prism::Plan` instead, and `#collect` optimizes then
executes it.

Plan nodes (`ScanBatches`, `ScanParquet`, `Filter`, `Select`, `WithColumn`,
`GroupBy`, `Sort`, `Distinct`, `Join`, `Limit`, `Empty`) each know their output
schema, which is what makes pruning possible before any data is read. Execution
**delegates to the eager verbs**, so lazy and eager results cannot drift, and
`GroupBy`/`Join` derive their schema by running the eager verb against zero-row
frames — one implementation of the layout rules.

Pass order, mirroring Polars:

1. **Constant folding** — `filter(lit(true))` disappears, `filter(lit(false))`
   becomes an `Empty` source.
2. **Predicate pushdown** — `&`-conjuncts are split, each is pushed as far as it
   can go, and scan-compatible ones become `RowFilter` predicates. A projection
   is crossed only when every referenced column passes through as a bare `Col`;
   a `WithColumn` blocks a predicate that reads the column it computes.
3. **Projection pruning** — required columns flow down from the root; the scan
   keeps only those plus the columns its pushed predicates need. Unused
   `WithColumn` nodes disappear entirely, and unused aggregations are dropped.
   Joins map each output name back to the side that produced it, so both inputs
   prune too: keys are always kept, a suffix is honoured (`price_right` pins the
   right `price`), and the left column that *forces* a suffix is retained so the
   right name does not silently change. Semi/anti joins read only keys from the
   right side. A name that cannot be attributed leaves both inputs untouched.
4. **Limit pushdown** — a limit reaches the scan through `Select`/`WithColumn`
   only. A limit that meets `Sort`/`GroupBy`/`Distinct` is not pushed below it,
   but it does not stay a separate node either: the window is pinned to that
   node (`Plan::Slice`), Polars' "limit 10 after the sort". Nested limits
   compose there (offsets add, counts clamp). Anything else that reorders or
   changes the row count (`Join`, a leftover `Filter`) leaves the `Limit` where
   it is.

Plan-level CSE is deferred, because repeated subexpressions are already memoized
per chunk during evaluation. A pinned slice on `Sort` gathers only its window
(`DataFrame#sort_by_slice`), so `sort + head(10)` copies 10 rows instead of every
row; the sort itself stays `O(n log n)`, because a bounded-heap top-k is an
`arrow-cr` kernel that this port does not model. `GroupBy` and `Distinct` apply
the window to their own output.

`explain` renders the plan and lists the pushdowns that reached each scan
(`projection:`, `predicate:`, `limit:`), so a silently disabled pass fails a
spec rather than degrading quietly.

## Grouping

Arrow provides sort and partition, not group-by:

```text
lexsort_to_indices(keys) -> take_record_batch -> partition(keys) -> ranges
  -> one vectorized pass per aggregate over the ranges
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
- **Vectorized aggregation.** Contiguous ranges are what let one pass reduce
  every group (below). A hash representation (`GroupsType::Idx`) would need a
  gather per group, so the sort now pays for itself rather than merely ordering
  the output.

Cost is O(n log n) per grouping rather than O(n), which the benchmarks keep in
perspective: at 1e5 keys the aggregates are ~free and the sort plus the row
gather dominate.

### Vectorized aggregation

`Prism::GroupedAgg` reduces the operand **once** for all groups instead of once
per group, which is what made a 1e5-key `group_by` slow: per group it cost a
batch slice, an `EvalContext`, a `Series`, a length-1 array and one
`Arrow.concat` piece (~4 µs each). The operand expression is now evaluated once
over the whole sorted batch — shared between aggregates that name the same
operand — and the result is one array:

| Aggregate | How |
| --- | --- |
| `count` | popcount of the group's validity bits, like Polars' `agg_valid_count` |
| `count_all` | the range length |
| `first`, `last` | one `Arrow.take` of the group-start/group-end rows |
| `sum`, `mean` | one fold per group (wrapping integers, IEEE floats) |
| `min`, `max` | one fold per group (IEEE total order for floats, `bool_and`/`bool_or` for booleans) |
| `n_unique`, `std`, `variance`, `median` | per-group Arrow kernel, but the operand is already evaluated and the result is built once |

That split matches Polars: it takes group boundary indices for `agg_first`/
`agg_last`, counts validity bits for `agg_valid_count`, and falls back to a
per-group kernel (`_agg_helper_slice`) for the rest — while preallocating the
output buffer, which is why its fallback is fast. Reductions keep Arrow's
semantics, so a grouped result equals the kernel applied to the same rows:
nulls are skipped and an all-null group is null, integer sums wrap, and float
extremes use total order (`-0.0` below `0.0`, every NaN above `+inf`). A nested
aggregation (`col("a").sum.mean`) has no defined cross-group meaning, so it keeps
the per-group evaluation rather than acquiring one silently.

`variance`/`std` are `sum((x - mean)^2) / (n - ddof)` with `sub`/`mul`/`sum`, and
`median` is `sort` plus the middle row — both per group, on the already-evaluated
operand.

Null keys form one group. `null_keys: :first | :last` controls placement
(:first matches Arrow's `ASC NULLS FIRST` default); `:drop` filters null-key rows
before grouping.

One-call aggregates (`group_by(k).sum`, `.mean`, `.count`, …) are thin sugar over
`#aggregate`: each name becomes `col(name).<agg>` and the output keeps the input
name. The no-argument form picks columns by dtype — integers and floats for
`sum`/`mean`/`median`/`std`/`var`, plus booleans and strings for `min`/`max`, and
everything for `count`/`n_unique` — and skips grouping keys, which are already in
the output. Asking for a key column explicitly raises `Prism::DuplicateColumn`
rather than emitting two columns with one name.

## IO

| Format | Read | Write |
| --- | --- | --- |
| Parquet | `Prism.read_parquet(path_or_io, columns:, batch_size:, predicate:)` | `write_parquet(..., compression:, row_group_size:, statistics:)` |
| CSV | `Prism.read_csv(path_or_io, schema:, header:, delimiter:, infer_rows:, batch_size:)` | `write_csv(..., header:)` |
| Arrow IPC | `Prism.read_ipc(path_or_io, stream:)` | `write_ipc(..., stream:)` |

Parquet reads push both halves down:

- **Projection.** `ProjectionMask.columns` means unrequested columns are never
  decoded. Predicate columns are added to the read set and dropped from the
  result afterwards, matching Polars.
- **Predicate.** `with_row_filter` evaluates the expression per row group over
  only the columns it references, so non-matching rows are never materialized.

The reader's batches become the frame's chunks, so `batch_size` controls chunking
instead of forcing one large batch.

Row-group pruning was disabled for a while: `parquet-cr` used to shift
null-bearing columns by one row per null when a filter was combined with a
projection (`shard_issues/parquet-cr.2.md`). Fixed upstream in `5883ff1`, and the
reproduction is kept as a spec guard.

Writing defaults follow Polars rather than the Parquet crate: zstd level 3 with
page statistics. The footer is written by `ArrowWriter#close` on the success
path only; a failed write leaves a file without a footer rather than a
misleading valid one. IPC writers use `ScopedWriter#open`, which finishes on
normal block completion.

CSV inference mirrors Polars' `infer_schema_length` (`infer_rows`). `0` types
every column as string, and Prism builds that schema itself from the header:
`arrow-cr`'s `infer_schema(bytes, 0)` is upstream-faithful and infers *no* types,
so it returns every field as `Null`, and passing that through read the right
number of rows with every value null (reported downstream as
`shard_issues/prism.2.md`, fixed in Phase 26).

## Joins

Hash join with the **right side as the build side**, following Polars' model:

```text
right keys -> RowConverter -> Hash(Row, [row indices])   # build
left keys  -> RowConverter -> probe, emitting (left row, right row) pairs
pairs      -> Arrow.take per column -> compose -> suffix/coalesce -> schema
```

Keys are encoded as `arrow-row` rows (`Prism::RowKeys`): one canonical byte
string per key, hashed and compared as bytes with no per-row boxing, and the
same arena also answers `distinct`, `n_unique` and `group_by`. Float key columns
are canonicalized first (`-0.0 → 0.0`, every NaN → one quiet NaN), because the
row format is `totalOrder` and keying must follow `==` instead. Nulls encode like
values, so `nulls_equal: false` is enforced by testing the key columns' null
bitmaps before indexing or probing, not by special-casing the encoding.

Payload columns stay in Arrow and are gathered by index.

Semantics match Polars: null keys do not match unless `nulls_equal: true`; key
columns are coalesced for inner/left/right and kept apart for `full` unless
`coalesce: true`; colliding non-key columns from the right get `suffix`
(`_right`); column order is left then right. Semi/anti are membership filters
that emit left rows only.

Outer joins gather rows through `Arrow.take` with a nullable index array.
`Arrow.take_record_batch` cannot be used for that: it validates against the
source schema, which declares non-nullable columns that the join nulls. The join
therefore rebuilds the batch against a relaxed schema and restores precise
nullability when composing the result.

`Prism.concat` stacks frames vertically (names and dtypes must match; nullability
only for a frame that carries rows, since a zero-row frame cannot violate a
non-null column) and `hstack` places columns side by side (row counts must match,
names must not collide). Both collapse to a single batch, since combining is the
point.

## Distinct

`arrow-cr` has no dedupe kernel, and `ArrayBase#uniq` is a Crystal-level sort
driven by a `DynComparator` closure — correct, but it allocates an index per row
and pays an indirect call per comparison. `DataFrame#distinct`, `Series#unique`
and the `n_unique` aggregate therefore encode the key columns into
`arrow-row` rows and walk them once:

```text
keys -> RowConverter -> Row per row -> Set(Row) of seen bytes -> keep first index
```

Because the encoding is canonical, "seen" is a byte-set lookup: O(n), no sort,
and the *first* occurrence is simply the first time the encoding is seen, so
input order comes for free (the previous sort-plus-partition path had to scan
each run for its smallest original index).

Floats need one normalizing pass before they are encoded. `arrow-row` stores IEEE
`totalOrder` bits — correct for sorting, wrong for keys — so `-0.0` would encode
differently from `0.0` and NaNs would differ by payload. This is where the two
row formats part company: **`polars-row` canonicalizes floats inside the
encoding** (`-inf < neg < -0.0 = 0.0 < pos < inf < nan`, all NaNs equal), and
`arrow-row` cannot, because its bytes must also express a sort order.
`RowKeys.canonical` therefore maps `-0.0` to `0.0` and every NaN to one quiet
NaN before encoding (Polars' `canonical_f16/f32/f64`: `x + 0.0`, then the
canonical quiet NaN), which is how Prism gets `polars-row` key equality out of
`arrow-row`. Keying then follows `==` plus "all NaNs are one key", as SQL, Polars
and pandas do.

A dictionary column is keyed through its *values*, so `RowKeys.canonical`
rebuilds it with canonicalized values and keeps the keys and the dictionary
dtype; callers such as `Series#unique` gather rows from the canonical array and
must not see a different dtype. Floats nested inside a list/struct/map key are
not canonicalized — reaching a float child means rebuilding the parent layout,
which nested key columns have not justified yet. That remaining limit is stated
in the `Prism::RowKeys` docs.

Nulls form one key of their own, which is what both `distinct` and `n_unique`
want: Polars counts null as a distinct value (`ChunkUnique` adds one when
`null_count > 0`), so `n_unique == unique.size`. That makes `n_unique` the one
aggregate that does not skip nulls — `sum`, `mean`, `min`, `max` and `count` all
do, because "the average of a null and 5" has no answer. It also means `n_unique`
diverges from SQL's `COUNT(DISTINCT …)`, which drops nulls.

Grouping is still sort-based on purpose: it yields contiguous `[start, len]`
ranges as `RecordBatch#slice` views and a deterministic output order, which a
hash table would give up. It sorts *canonical* keys but emits the original key
values, so `group_by` and `distinct` agree on what one key is. See
[Grouping](#grouping).

## Decimals

Decimals are fixed-point, so their arithmetic cannot simply promise "the wider
dtype": Arrow's kernels implement Hive's scale rules, and the *result* dtype
depends on both operands. Prism declares exactly what the kernel returns, which
is what makes `expr.dtype(schema)` trustworthy before any row is read:

| Operation | Result scale | Result precision |
| --- | --- | --- |
| `+`, `-` | `max(s1, s2)` | `max(s1, s2) + max(p1 - s1, p2 - s2) + 1` |
| `*` | `s1 + s2` | `p1 + p2 + 1` |
| `/` | `s1 + 4` | `(s1 + 4 - s1 + s2) + p1` |
| `%` | `max(s1, s2)` | `max(s1, s2) + min(p1 - s1, p2 - s2)` |
| `-x` | `s1` | `p1` |

Precision caps at the kind's maximum (38 for `Decimal128`); a product whose scale
would exceed it raises `Prism::DtypeError` at planning time rather than
overflowing inside the kernel. Operand promotion is deliberately narrow:

- an **integer** operand becomes a decimal with scale 0 and its width's digit
  count (`Int64` → `decimal(19, 0)`), so the Hive rule then widens precision;
- an integer **literal** adopts the column's scale instead, so
  `decimal(10, 2) + 1` stays at scale 2;
- a **float** operand falls back to `Float64`, losing the scale (documented, not
  silent — comparisons against floats still raise);
- mixed decimal **widths** (`Decimal32`/`64`/`128`/`256`) are promoted to the
  wider kind first, because the kernels reject two kinds in one operation;
- a `Null` column or `nil` literal adopts the other side's dtype, so the result
  is all null with the dtype the rule produces.

`Prism::Values` materializes decimals as `BigDecimal`, so the scale survives all
the way to `to_a` instead of appearing as a scaled integer.

## Display

`DataFrame#to_s` and `#inspect` render an aligned table with a shape line, a dtype
row, and head/tail truncation:

```text
shape: (4, 4)
region | price | qty | note
------ | ----- | --- | ------------------------------
str    | f64   | i64 | str
east   | 10.0  | 1   | a very long note that shoul...
west   | 20.5  | 2   | short
```

Defaults follow Polars: 10 rows, 8 columns, 30 characters per string cell.
Larger frames keep `max/2` rows (or columns) from each end with an ellipsis
between them, so the shape stays readable. `to_s(max_rows:, max_cols:,
max_string:)` overrides them.

Plain ASCII is deliberate: `ljust` pads by bytes, so box-drawing characters would
misalign beside any non-ASCII value. Nulls render as `null`, and floats use
Crystal's formatting (`NaN`, `Infinity`).

For CLI output there is an optional bordered renderer:

```crystal
require "prism/lipgloss"   # after adding lipgloss to your shard.yml

puts df.to_styled
```

```text
shape: (3, 3)
┌──────┬─────┬───┐
│region│price│qty│
├──────┼─────┼───┤
│str   │f64  │i64│
│east  │10.0 │1  │
└──────┴─────┴───┘
```

It shares `Display.grid` with the plain renderer, so both show the same cells and
truncation. Styling is opt-in because `to_s` must stay free of ANSI escapes —
logs, pipes and specs consume it.

## Typed schemas

```crystal
Prism.define_schema UserSchema, id: Int64, name: String, active: Bool, salary: Float64

typed = UserSchema.from_arrow(batch)
typed.filter(&.active).select(&.salary)   # misspelt field fails to compile
typed.salary                              # => Prism::Series
```

`define_schema` generates three things: the Arrow schema, a fields struct whose
properties are `Prism::Expr` references, and `UserSchema::Frame`, a typed view.

The frame is a **generated struct**, not a generic wrapper, because per-field
accessors cannot be added to an instantiated generic type. Shared behaviour lives
in `Prism::TypedVerbs(S)`, which the struct includes — partly for reuse, and
partly because `def select` cannot be written inside a macro body: the parser
reads `select` as the channel-select keyword (plan C21).

Typing holds at the **declaration boundary**. Schema-preserving verbs (`filter`,
`sort_by`, `head`, `slice`) return the same frame type. Anything that changes the
schema (`select`, `with_column`) returns a plain `DataFrame`, because Crystal has
no way to refine a return type from the schema flowing through a call — that
needs a compiler plugin, as Kotlin DataFrame uses. `UserSchema.from_dataframe`
is the explicit way back in; it validates every declared field and accepts a
superset of columns.

Field types map to Arrow dtypes through `Prism::Typed.dtype_for`; an unsupported
type is a compile error naming it, raised from the fallback overload.

## Parity and divergence

Polars is the execution model this library borrows, so the verb-by-verb mapping
and the deliberate differences live in
[Coming from Polars](polars-parity.md); this section covers the design decisions
behind them.

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
