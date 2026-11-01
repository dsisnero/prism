# `parquet-cr`: `with_row_filter` mis-reads columns that contain nulls

## Status: fixed upstream and workaround removed (2026-11-01)

Fixed in `parquet-cr` commit `5883ff1` ("port: Align row-filter skips with
definition levels", branch `codex/stream-large-file-reads`). The fix aligns the
row-filter skip with definition levels in `src/parquet/column/reader.cr`, and
adds `spec/parquet/arrow/row_filter_spec.cr` plus a record-reader regression.

Verified from `prism`: the reproducer below now prints `[nil, 40]`, and the
mixed-type frame reads `region=["east", nil, "west"]`,
`price=[30.0, 40.0, nil]`, `qty=[3, 4, 5]` across one row group, across three
row groups (`row_group_size: 2`), and for a selection spanning groups.

Downstream: `prism` re-enabled `with_row_filter`, so Parquet predicates prune per
row group again. `spec/prism/io/parquet_spec.cr` keeps this reproduction as a
permanent guard. This file remains as the record of the defect.

### Original report

Reproduced against the then-current checkout (`parquet-cr` 0.1.0, ported from
`parquet` 60.0.0). The predicate mask itself was correct; the fault was in how
the reader applied the resulting selection to value buffers containing nulls.

## Failure

Building a reader with **both** a row filter and an output projection returns
wrong values whenever a selected range spans a null in a column:

- the row count is right;
- columns without nulls read correctly;
- a column with a null inside the selected range is shifted by one row per null,
  and can pull values from outside the selection.

No error is raised. The corruption is silent, so downstream code stores bad data.

## Reproducer

Self-contained; requires only `arrow-cr` and `parquet-cr`.

```crystal
require "arrow-cr"
require "parquet-cr"

def roundtrip(payload : Arrow::ArrayBase) : Bytes
  batch = Arrow::RecordBatch.from_columns({
    key:     Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64, 5_i64]),
    payload: payload,
  })
  sink = IO::Memory.new
  props = Parquet::File::Properties::WriterProperties.builder.build
  writer = Parquet::Arrow::ArrowWriter.try_new(sink, batch.schema, props)
  writer.write(batch)
  writer.close
  sink.to_slice
end

# Rows with key in [low, high]; predicate reads only `key`.
def selected(bytes : Bytes, low : Int64, high : Int64) : Array(Int64?)
  file = Parquet::File::SerializedFileReader.new(
    Parquet::File::BytesChunkReader.new(bytes)
  )
  builder = Parquet::Arrow::ParquetRecordBatchReaderBuilder.try_new(file)
  mask = Parquet::Arrow::ProjectionMask.columns(builder.parquet_schema, ["key"])

  predicate = Parquet::Arrow::ArrowPredicateFn.new(mask) do |batch|
    key = batch["key"].as(Arrow::Int64Array)
    lower = Arrow.gt_eq(key, Arrow::Int64Array.new_scalar(low))
    upper = Arrow.lt_eq(key, Arrow::Int64Array.new_scalar(high))
    Arrow.and_kleene(lower, upper)
  end

  reader = builder.with_row_filter(
    Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate)
  ).build

  out = [] of Int64?
  while batch = reader.next_batch
    out.concat(batch["payload"].as(Arrow::Int64Array).to_a)
  end
  out
end

with_null = Arrow::Int64Array
  .from_nullable([10_i64, 20_i64, nil, 40_i64, 50_i64]).as(Arrow::ArrayBase)
no_null = Arrow::Int64Array
  .from_values([10_i64, 20_i64, 30_i64, 40_i64, 50_i64]).as(Arrow::ArrayBase)

# Rows 2..3: payload is [nil, 40] with a null, [30, 40] without.
p selected(roundtrip(with_null), 3_i64, 4_i64) # => [40, 50]  WRONG
p selected(roundtrip(no_null), 3_i64, 4_i64)   # => [30, 40]  correct
p selected(roundtrip(with_null), 1_i64, 2_i64) # => [10, 20]  correct
```

Output on the pinned checkout:

```text
[40, 50]
[30, 40]
[10, 20]
```

## Expected vs actual

| Payload | Selected rows | Expected | Actual |
| --- | --- | --- | --- |
| `[10, 20, nil, 40, 50]` | 2..3 | `[nil, 40]` | `[40, 50]` |
| `[10, 20, 30, 40, 50]` | 2..3 | `[30, 40]` | `[30, 40]` |
| `[10, 20, nil, 40, 50]` | 0..1 | `[10, 20]` | `[10, 20]` |

Mixed-type frame (`region` Utf8 with a null at row 3, `price` Float64 with a
null at row 4, `qty` Int64 with no nulls), predicate `qty > 2` (rows 2,3,4,
mask `[F,F,T,T,T]`):

| Column | Expected | Actual |
| --- | --- | --- |
| `qty` (no nulls) | `[3, 4, 5]` | `[3, 4, 5]` |
| `region` (null at row 3) | `["east", nil, "west"]` | `["east", "west", "east"]` |
| `price` (null at row 4) | `[30.0, 40.0, nil]` | `[30.0, 40.0, 10.0]` |
| `flag` Boolean (no nulls) | `[true, false, true]` | `[true, false, true]` |

## Analysis

Observations that narrow it down:

1. The mask handed back by `ArrowPredicateFn` is correct — it was logged from
   inside the predicate and is `[F,F,T,T,T]` for `qty > 2`.
2. Projection only, no filter: correct.
3. Filter only, no projection: still wrong for the null-bearing columns.
4. Selections that avoid every null (`rows 0..1`) are correct.
5. Columns with no nulls are correct in the same batch, so this is per column
   reader, not a global row misalignment.
6. The result length is right; the values are shifted, which reads like the
   null row being *skipped* in the value buffer rather than emitted as null.

Suggested place to look: how the read plan's selection reaches each output
column reader (the record reader / array readers under
`src/parquet/arrow/array_reader/`). Specifically whether the skip/selection
advances the definition-level (null) positions and the value positions
consistently for byte-array and primitive readers, and whether
`set_selection`-style offsets are applied to the null buffer as well as to the
values buffer.

## Downstream impact (`prism`)

- `plans/plan.md` records this as constraint **C20**.
- `prism` does not call `with_row_filter`. Projection pushdown is still used,
  and the `predicate:` argument filters in memory after the projected read, so
  results are correct but row-group pruning is unavailable.
- `spec/prism/io/parquet_spec.cr` keeps the mixed-type reproduction as a
  regression guard ("keeps every column consistent when a predicate is
  applied"), so re-enabling pushdown before this is fixed fails the suite.
- `src/prism/io/parquet.cr` carries the pushdown call as one commented block,
  ready to restore.

## Suggested fix direction (not a patch)

Whichever function folds the predicate selection into the decode path should
apply it uniformly to values **and** definition levels, per column reader, and
should be covered by a test with a null inside the selected range for:

- a primitive column,
- a byte-array (Utf8) column,
- a column whose last selected row is null,
- a selection that starts immediately after a null.

Until then, consumers should treat `with_row_filter` as unsafe for data
containing nulls.
