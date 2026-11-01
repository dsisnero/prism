require "../../spec_helper"

describe "CSV IO" do
  it "round-trips a frame through IO::Memory with an inferred schema" do
    io = IO::Memory.new
    SpecFrames.sales.write_csv(io)
    io.rewind

    restored = Prism.read_csv(io)

    restored.column_names.should eq(["region", "price", "qty"])
    restored.dtypes["price"].should eq(Arrow::DataType.float64)
    restored.dtypes["qty"].should eq(Arrow::DataType.int64)
    restored["qty"].to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64, 5_i64])
  end

  it "reads with an explicit schema" do
    io = IO::Memory.new
    SpecFrames.sales.write_csv(io)
    io.rewind

    schema = Arrow::Schema.new(Arrow::Fields.from([
      Arrow::Field.new("region", Arrow::DataType.utf8, true),
      Arrow::Field.new("price", Arrow::DataType.float64, true),
      Arrow::Field.new("qty", Arrow::DataType.int64, false),
    ]))

    restored = Prism.read_csv(io, schema: schema)

    restored.column_names.should eq(["region", "price", "qty"])
    restored["qty"].to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64, 5_i64])
  end

  it "writes a header row" do
    io = IO::Memory.new
    SpecFrames.sales.write_csv(io)

    io.to_s.lines.first.strip.should eq("region,price,qty")
  end

  it "round-trips through a file path" do
    path = SpecPaths.temp("round_trip.csv")
    SpecFrames.sales.write_csv(path)

    Prism.read_csv(path).row_count.should eq(5)
  end

  it "honours batch_size" do
    io = IO::Memory.new
    SpecFrames.sales.write_csv(io)
    io.rewind

    restored = Prism.read_csv(io, batch_size: 2)

    restored.row_count.should eq(5)
    chunks = 0
    restored.each_batch { chunks += 1 }
    chunks.should be > 1
  end
end
