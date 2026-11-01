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

  # Regression guard for plan C20: enabling parquet-cr's row filter corrupted
  # non-predicate columns (price returned [30.0, 40.0, 10.0] for rows 2,3,4).
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
