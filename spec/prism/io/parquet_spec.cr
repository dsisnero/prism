require "../../spec_helper"

include Prism::DSL

private def parquet_io(frame : Prism::DataFrame) : IO::Memory
  io = IO::Memory.new
  Prism.write_parquet(io, frame)
  io.rewind
  io
end

describe "Parquet IO" do
  it "round-trips a frame through IO::Memory" do
    original = SpecFrames.sales
    restored = Prism.read_parquet(parquet_io(original))

    restored.column_names.should eq(original.column_names)
    restored.schema.should eq(original.schema)
    restored["region"].to_a.should eq(["east", "west", "east", nil, "west"])
    restored["price"].to_a.should eq([10.0, 20.0, 30.0, 40.0, nil])
    restored["qty"].to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64, 5_i64])
  end

  it "round-trips through a file path" do
    path = SpecPaths.temp("round_trip.parquet")
    SpecFrames.sales.write_parquet(path)

    restored = Prism.read_parquet(path)

    restored.row_count.should eq(5)
    restored["qty"].to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64, 5_i64])
  end

  it "reads only the requested columns, in the requested order" do
    read = Prism.read_parquet(parquet_io(SpecFrames.sales), columns: ["qty", "region"])

    read.column_names.should eq(["qty", "region"])
    read["qty"].to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64, 5_i64])
  end

  it "pushes a predicate into the reader and drops its helper column" do
    read = Prism.read_parquet(
      parquet_io(SpecFrames.sales),
      columns: ["region"],
      predicate: col("qty") > lit(2_i64),
    )

    # `qty` was read to evaluate the predicate, but is not part of the result.
    read.column_names.should eq(["region"])
    read["region"].to_a.should eq(["east", nil, "west"])
  end

  # Regression guard for shard_issues/parquet-cr.done.2.md (fixed upstream in
  # 5883ff1): a row filter used to shift columns containing nulls by one row per
  # null, so price returned [30.0, 40.0, 10.0] for rows 2,3,4.
  # The consumer's case: a set-membership predicate over a large file must reach
  # the reader, not materialize the file and filter afterwards.
  it "pushes a set-membership predicate into the reader" do
    read = Prism.read_parquet(
      parquet_io(SpecFrames.sales),
      columns: ["qty"],
      predicate: col("region").is_in(["east"]),
    )

    read.column_names.should eq(["qty"])
    # Rows 1 and 3 are "east"; row 4's region is null, so a null predicate row
    # is dropped exactly as `filter` would drop it.
    read["qty"].to_a.should eq([1_i64, 3_i64])
  end

  # The downstream shape (amazon_data's Open Library scan): one title set tested
  # against two columns, OR'd, pushed into the reader.
  it "pushes a set-membership predicate over two columns" do
    titles = ["east", "west"]
    predicate = col("region").is_in(titles) | col("region").is_in(["west"])

    read = Prism.read_parquet(
      parquet_io(SpecFrames.sales),
      columns: ["qty"],
      predicate: predicate,
    )

    # Rows 1, 2, 3 and 5 match; row 4's region is null, so Kleene `or` leaves the
    # row unjudged and it is dropped, exactly as the eager `filter` drops it.
    read["qty"].to_a.should eq([1_i64, 2_i64, 3_i64, 5_i64])
  end

  it "keeps a null key when the predicate treats null as a value" do
    read = Prism.read_parquet(
      parquet_io(SpecFrames.sales),
      predicate: col("region").is_in(["east", nil], nulls_equal: true),
    )

    read["qty"].to_a.should eq([1_i64, 3_i64, 4_i64])
  end

  it "keeps every column consistent when a predicate is applied" do
    read = Prism.read_parquet(
      parquet_io(SpecFrames.sales),
      predicate: col("qty") > lit(2_i64),
    )

    read["region"].to_a.should eq(["east", nil, "west"])
    read["price"].to_a.should eq([30.0, 40.0, nil])
    read["qty"].to_a.should eq([3_i64, 4_i64, 5_i64])
  end

  it "returns the same rows with and without the pushed predicate" do
    original = SpecFrames.sales
    pushed = Prism.read_parquet(
      parquet_io(original),
      predicate: col("price") > lit(15.0),
    )
    in_memory = original.filter(col("price") > lit(15.0))

    pushed["qty"].to_a.should eq(in_memory["qty"].to_a)
  end

  it "filters correctly across several row groups" do
    # Small row groups force the predicate to run per group, which is where the
    # null-shift bug above lived.
    io = IO::Memory.new
    Prism.write_parquet(io, SpecFrames.sales, row_group_size: 2)
    io.rewind

    read = Prism.read_parquet(io, predicate: col("qty") > lit(2_i64))

    read["region"].to_a.should eq(["east", nil, "west"])
    read["price"].to_a.should eq([30.0, 40.0, nil])
    read["qty"].to_a.should eq([3_i64, 4_i64, 5_i64])
  end

  it "honours batch_size when chunking the result" do
    read = Prism.read_parquet(parquet_io(SpecFrames.sales), batch_size: 2)

    chunks = 0
    read.each_batch { chunks += 1 }
    read.row_count.should eq(5)
    chunks.should eq(3)
  end

  it "writes the requested compression" do
    io = IO::Memory.new
    Prism.write_parquet(io, SpecFrames.sales, compression: :uncompressed)
    io.rewind

    Prism.read_parquet(io)["qty"].to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64, 5_i64])
  end

  it "writes a readable file for an empty frame" do
    restored = Prism.read_parquet(parquet_io(SpecFrames.empty_sales))

    restored.row_count.should eq(0)
    restored.column_names.should eq(["region", "price", "qty"])
  end

  it "rejects an unknown column" do
    expect_raises(Prism::ColumnNotFound, /nope/) do
      Prism.read_parquet(parquet_io(SpecFrames.sales), columns: ["nope"])
    end
  end

  it "rejects an unknown predicate column" do
    expect_raises(Prism::ColumnNotFound, /nope/) do
      Prism.read_parquet(parquet_io(SpecFrames.sales), predicate: col("nope") > lit(1))
    end
  end

  it "exposes write_parquet on the frame as well" do
    io = IO::Memory.new
    SpecFrames.sales.write_parquet(io)
    io.rewind

    Prism.read_parquet(io).row_count.should eq(5)
  end
end
