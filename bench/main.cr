# Prism benchmarks.
#
# Run with `make bench` (or `crystal run --release bench/main.cr`). Results are
# printed as a markdown table so they can be pasted into
# [`docs/benchmarks.md`](../docs/benchmarks.md).
#
# The workload is deliberately boring: one 1e6-row frame, a handful of verbs,
# and a Parquet round trip. The point is to compare **paths against each other**
# (pushed down vs not, chunked vs concatenated, sort-based keys vs nothing) on
# the same data, not to win a shootout with another library. Each scenario runs
# once to warm up, then `REPEATS` times; the table shows the best and the median
# so one noisy run cannot dominate.
require "../src/prism"

include Prism::DSL

module Bench
  extend self

  ROWS     = 1_000_000
  USERS    =   100_000
  REPEATS  =         5
  REGIONS  = ["north", "south", "east", "west"]
  PARQUET  = "temp/bench/sales.parquet"
  QTY      =   1_000 # qty is drawn from 0...QTY
  HIGH_QTY = 900_i64 # the parquet predicate keeps ~10%
  FILES    = [] of {String, Float64}

  def run : Nil
    Dir.mkdir_p("temp/bench")

    puts "Prism benchmarks — #{ROWS} rows, best of #{REPEATS} (ms)"
    puts
    puts "| scenario | best | median | out rows | note |"
    puts "| --- | ---: | ---: | ---: | --- |"

    frame = sales
    dimension = segments

    filter(frame)
    projection(frame)
    sorting(frame)
    grouping(frame)
    wide_grouping
    keys(frame, dimension)
    parquet(frame)
    clustered(frame)
    csv(frame)
    lazy(frame)

    puts
    FILES.each { |(path, mebibytes)| puts "File: #{path} (#{mebibytes.round(1)} MiB)" }
    puts "Sanity: frame=#{frame.shape}, dimension=#{dimension.shape}"
  end

  private def filter(frame : Prism::DataFrame) : Nil
    median = (QTY // 2).to_i64
    timed("filter col > lit", "≈ half the rows") do
      frame.filter(col("qty") > lit(median))
    end

    timed("filter on two conjuncts", "≈ a quarter") do
      frame.filter((col("qty") > lit(median)) & (col("region") == lit("north")))
    end
  end

  private def projection(frame : Prism::DataFrame) : Nil
    timed("select 2 of 4 columns", "projection, no copy") do
      frame.select("region", "price")
    end

    timed("with_column (price * qty)", "casts qty to Float64") do
      frame.with_column("total", col("price") * col("qty"))
    end

    timed("to_arrow (rechunk)", "single chunk already") do
      frame.to_arrow
    end
  end

  private def sorting(frame : Prism::DataFrame) : Nil
    timed("sort_by one key", "lexsort_to_indices + take") do
      frame.sort_by("price", order: :desc)
    end

    timed("sort_by two keys", "second key is `Utf8`") do
      frame.sort_by(col("region").asc, col("price").desc)
    end

    timed("sort_by + limit 10 (eager)", "sorts every row, then trims") do
      frame.sort_by("price", order: :desc).head(10)
    end

    timed("top_n(10)", "bounded sort + window, eager") do
      frame.top_n(10, "price")
    end

    timed("scan -> sort_by -> head(10)", "bounded selection + window") do
      frame.lazy
        .sort_by("price", order: :desc)
        .head(10)
        .collect
    end
  end

  private def grouping(frame : Prism::DataFrame) : Nil
    timed("group_by region (4 groups)", "sort-based grouping") do
      frame.group_by("region").aggregate(
        total: col("qty").sum,
        average: col("price").mean,
        count: col("qty").count,
      )
    end

    timed("group_by region + user_id (2 keys)", "two-key sort") do
      frame.group_by("region", "user_id").aggregate(
        total: col("qty").sum,
        average: col("price").mean,
      )
    end

    timed("group_by 1e5 keys", "one sort, three aggregates") do
      frame.group_by("user_id").aggregate(
        total: col("qty").sum,
        average: col("price").mean,
      )
    end
  end

  # A wide frame: only the key and the two aggregated columns should be gathered.
  def wide_grouping : Nil
    frame = wide

    timed("group_by 1e5 keys, 48 unused columns", "Phase 16 pruning") do
      frame.group_by("user_id").aggregate(
        total: col("qty").sum,
        average: col("price").mean,
      )
    end
  end

  # The row-keyed paths: joins, distinct and n_unique, all keyed by the
  # `arrow-row` encoding (Prism::RowKeys).
  private def keys(frame : Prism::DataFrame, dimension : Prism::DataFrame) : Nil
    timed("distinct on 1e5 keys", "row-keyed seen-set") do
      frame.distinct("user_id")
    end

    timed("series n_unique", "row-keyed seen-set") do
      frame["user_id"].n_unique
    end

    timed("join 1e6 -> 1e5 on user_id", "row-encoded keys") do
      frame.join(dimension, on: "user_id", how: :left)
    end
  end

  private def parquet(frame : Prism::DataFrame) : Nil
    timed("write_parquet zstd(3)", "row groups of 100k") do
      frame.write_parquet(PARQUET, row_group_size: 100_000)
    end

    size = File.size(PARQUET)
    timed("read_parquet (all columns)", "baseline") do
      Prism.read_parquet(PARQUET)
    end

    timed("read_parquet projection (2 of 4)", "pushdown") do
      Prism.read_parquet(PARQUET, columns: ["region", "price"])
    end

    timed("read_parquet predicate", "row filter") do
      Prism.read_parquet(PARQUET, predicate: col("qty") > lit(HIGH_QTY))
    end

    timed("read_parquet projection + predicate", "pushdown") do
      Prism.read_parquet(
        PARQUET,
        columns: ["region", "price"],
        predicate: col("qty") > lit(HIGH_QTY),
      )
    end

    timed("read_parquet limit 100", "slice pushdown") do
      Prism.read_parquet(PARQUET, limit: 100)
    end

    timed("scan -> filter -> select -> collect", "lazy, pushed down") do
      Prism.scan_parquet(PARQUET)
        .filter(col("qty") > lit(HIGH_QTY))
        .select("region", "price")
        .collect
    end

    timed("eager read + filter + select", "same work, no pushdown") do
      Prism.read_parquet(PARQUET)
        .filter(col("qty") > lit(HIGH_QTY))
        .select("region", "price")
    end

    FILES << {PARQUET, size / 1_048_576.0}
  end

  # Row-group statistics only pay off when the predicate is clustered. `seq` is
  # ascending, so `seq > 900_000` prunes whole row groups instead of only rows —
  # the case Parquet's min/max stats are built for.
  private def clustered(frame : Prism::DataFrame) : Nil
    path = "temp/bench/seq.parquet"
    frame
      .with_row_index("seq")
      .write_parquet(path, row_group_size: 100_000)

    timed("read clustered (all rows)", "no predicate") do
      Prism.read_parquet(path)
    end

    timed("read clustered predicate (last 10%)", "row groups pruned") do
      Prism.read_parquet(path, predicate: col("seq") > lit(900_000_i64))
    end

    timed("read clustered predicate + projection", "row groups pruned") do
      Prism.read_parquet(
        path,
        columns: ["region", "price"],
        predicate: col("seq") > lit(900_000_i64),
      )
    end

    FILES << {path, File.size(path) / 1_048_576.0}
  end

  # The CSV surface, which `shard_issues/arrow-cr.4.md` reports on: the file is
  # written once (outside the measurement) and then read with and without a
  # schema, because inference decodes the file twice — once to infer, once to
  # read — while a schema read skips that pass.
  private def csv(frame : Prism::DataFrame) : Nil
    path = "temp/bench/sales.csv"
    Prism.write_csv(path, frame) unless File.exists?(path)
    size = File.size(path)

    timed("write_csv", "1e6 rows, 4 columns") do
      Prism.write_csv(IO::Memory.new(size + 1_024), frame)
    end

    timed("read_csv (all columns)", "schema supplied, no inference") do
      Prism.read_csv(path, schema: frame.schema)
    end

    timed("read_csv (inferred)", "infer_rows=100, two passes") do
      Prism.read_csv(path)
    end

    FILES << {path, size / 1_048_576.0}
  end

  private def lazy(frame : Prism::DataFrame) : Nil
    timed("lazy group_by -> sort -> head", "slice pinned to the sort") do
      frame.lazy
        .group_by("user_id")
        .aggregate(total: col("qty").sum)
        .sort_by("total", order: :desc)
        .head(10)
        .collect
    end
  end

  # --- fixtures -----------------------------------------------------------

  def sales : Prism::DataFrame
    rng = Random.new(42)
    Prism.from_columns({
      "region"  => Arrow::StringArray.from_values(Array.new(ROWS) { REGIONS[rng.rand(REGIONS.size)] }),
      "user_id" => Arrow::Int64Array.from_values(Array.new(ROWS) { rng.rand(USERS).to_i64 }),
      "qty"     => Arrow::Int64Array.from_values(Array.new(ROWS) { rng.rand(1_000).to_i64 }),
      "price"   => Arrow::Float64Array.from_values(Array.new(ROWS) { rng.next_float * 100.0 }),
    })
  end

  # 52 columns where the aggregation reads three. Phase 16 prunes the rest before
  # the key sort and the row gather, so this scenario tracks that pruning.
  WIDE_ROWS    = 200_000
  WIDE_PAYLOAD =      48

  def wide : Prism::DataFrame
    rng = Random.new(44)

    columns = {} of String => Arrow::ArrayBase
    columns["user_id"] = Arrow::Int64Array.from_values(Array.new(WIDE_ROWS) { rng.rand(USERS).to_i64 })
    columns["region"] = Arrow::StringArray.from_values(Array.new(WIDE_ROWS) { REGIONS[rng.rand(REGIONS.size)] })
    columns["qty"] = Arrow::Int64Array.from_values(Array.new(WIDE_ROWS) { rng.rand(1_000).to_i64 })
    columns["price"] = Arrow::Float64Array.from_values(Array.new(WIDE_ROWS) { rng.next_float * 100.0 })
    WIDE_PAYLOAD.times do |index|
      columns["payload_#{index}"] =
        Arrow::Int64Array.from_values(Array.new(WIDE_ROWS) { rng.rand(1_000).to_i64 })
    end

    Prism.from_columns(columns)
  end

  def segments : Prism::DataFrame
    rng = Random.new(43)
    names = ["retail", "wholesale", "online"]

    Prism.from_columns({
      "user_id" => Arrow::Int64Array.from_values(Array.new(USERS, &.to_i64)),
      "segment" => Arrow::StringArray.from_values(Array.new(USERS) { names[rng.rand(names.size)] }),
    })
  end

  # --- harness ------------------------------------------------------------

  # Runs the block once to warm up, then `REPEATS` times. Prints best and median
  # milliseconds, plus the output row count, which also proves the work ran.
  def timed(label : String, note : String, &) : Nil
    rows = row_count_of(yield)

    times = [] of Float64
    REPEATS.times do
      GC.collect
      started = Time.instant
      rows = row_count_of(yield)
      times << (Time.instant - started).total_milliseconds
    end

    times.sort!
    annotated = note.empty? ? "" : note
    puts "| #{label} | #{times.first.round(1)} | #{times[times.size // 2].round(1)} | " \
         "#{rows} | #{annotated} |"
  end

  private def row_count_of(result) : Int64
    case result
    when Prism::DataFrame     then result.row_count
    when Arrow::RecordBatch   then result.num_rows.to_i64
    when Int32, Int64, UInt64 then result.to_i64
    else                           0_i64
    end
  end
end

Bench.run
