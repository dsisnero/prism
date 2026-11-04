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

  # shard_issues/prism.1.md in the consumer repo: `infer_rows: 0` is documented as
  # "every column is a string" (Polars' `infer_schema_length=0`), but it used to
  # hand 0 straight to Arrow's inferer, which infers nothing and leaves every
  # dtype as `Null` — a quiet all-null frame.
  it "reads every column as a string when infer_rows is 0" do
    csv = <<-CSV
    kind,name,year
    award,pen,1989
    win,parable,1993
    CSV
    io = IO::Memory.new(csv)

    read = Prism.read_csv(io, infer_rows: 0)

    read.column_names.should eq(["kind", "name", "year"])
    read.dtypes.values.uniq.should eq([Arrow::DataType.utf8])
    read["year"].to_a.should eq(["1989", "1993"])
    read["kind"].to_a.should eq(["award", "win"])
  end

  it "lets an explicit schema win over infer_rows: 0" do
    io = IO::Memory.new("qty
1
2
")
    schema = Arrow::Schema.new(Arrow::Fields.from([
      Arrow::Field.new("qty", Arrow::DataType.int64, true),
    ]))

    read = Prism.read_csv(io, schema: schema, infer_rows: 0)

    read.dtypes["qty"].should eq(Arrow::DataType.int64)
    read["qty"].to_a.should eq([1_i64, 2_i64])
  end

  it "rejects a negative infer_rows" do
    expect_raises(ArgumentError, /infer_rows/) do
      Prism.read_csv(IO::Memory.new("a\n1\n"), infer_rows: -1)
    end
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
