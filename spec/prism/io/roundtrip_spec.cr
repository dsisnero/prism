require "../../spec_helper"

include Prism::DSL

# The IO surface's oracle: Parquet and IPC carry a frame exactly, and CSV — a
# text format with inferred types — never changes a value even though it cannot
# carry every dtype. Each matrix entry runs every format, so a dtype one writer
# or reader mishandles fails here.
private def parquet_round_trip(frame : Prism::DataFrame) : Prism::DataFrame
  io = IO::Memory.new
  Prism.write_parquet(io, frame)
  io.rewind
  Prism.read_parquet(io)
end

private def ipc_round_trip(frame : Prism::DataFrame, stream : Bool = false) : Prism::DataFrame
  io = IO::Memory.new
  Prism.write_ipc(io, frame, stream: stream)
  io.rewind
  Prism.read_ipc(io, stream: stream)
end

private def csv_round_trip(frame : Prism::DataFrame, schema : Arrow::Schema? = nil, infer_rows : Int32 = 100) : Prism::DataFrame
  io = IO::Memory.new
  Prism.write_csv(io, frame)
  io.rewind
  Prism.read_csv(io, schema: schema, infer_rows: infer_rows)
end

private def csv_text(frame : Prism::DataFrame) : String
  io = IO::Memory.new
  Prism.write_csv(io, frame)
  io.to_s
end

# Labels whose dtype a CSV file cannot carry at all: bytes have no text form
# (the writer would emit hex, which reads back as a different value) and a
# nested value has no single text form.
private CSV_UNWRITABLE = ["binary", "binary_view", "list", "fixed_list", "struct", "map"]

# Labels `arrow-cr`'s CSV reader cannot parse back even with the right schema.
private CSV_UNREADABLE = ["large_utf8", "binary", "duration", "interval_ym"]

describe "IO round-trip" do
  it "Parquet gives back every dtype, schema and values alike" do
    SpecDtypes.columns.each do |(label, array)|
      original = SpecDtypes.frame(array)
      restored = parquet_round_trip(original)

      restored.schema.should eq(original.schema), "parquet #{label}: schema"
      restored["v"].to_a.should eq(original["v"].to_a), "parquet #{label}: values"
    end
  end

  it "IPC gives back every dtype, in the file and the stream format" do
    SpecDtypes.columns.each do |(label, array)|
      original = SpecDtypes.frame(array)

      [false, true].each do |stream|
        restored = ipc_round_trip(original, stream)
        restored.schema.should eq(original.schema), "ipc #{label} (stream=#{stream}): schema"
        restored["v"].to_a.should eq(original["v"].to_a), "ipc #{label} (stream=#{stream}): values"
      end
    end
  end

  it "carries a zero-row frame, schema included, through each binary format" do
    empty = SpecFrames.sales.filter(col("qty") > lit(100_i64))
    empty.row_count.should eq(0)

    [parquet_round_trip(empty), ipc_round_trip(empty)].each do |restored|
      restored.row_count.should eq(0)
      restored.schema.should eq(empty.schema)
    end
  end

  it "keeps every row of a chunked frame, in order" do
    row_count = 300
    region = Arrow::StringArray.from_nullable(Array.new(row_count) { |i| i.even? ? "east" : nil })
    qty = Arrow::PrimitiveArray(Int64).from_values(Array.new(row_count, &.to_i64))
    frame = Prism.from_columns({region: region, qty: qty})
    batch = frame.to_arrow
    quarter = row_count // 4
    chunked = Prism.from_batches([
      batch.slice(0, quarter),
      batch.slice(quarter, quarter),
      batch.slice(quarter * 2, row_count - quarter * 2),
    ])

    [parquet_round_trip(chunked), ipc_round_trip(chunked)].each do |restored|
      restored.row_count.should eq(row_count)
      restored["qty"].to_a.should eq((0...row_count).map(&.to_i64))
      restored["region"].to_a.should eq(frame["region"].to_a)
    end
  end

  it "keeps names that need quoting, unicode and empty names" do
    frame = Prism.from_columns({
      "a,b"    => Arrow::Int64Array.from_values([1_i64]).as(Arrow::ArrayBase),
      "привет" => Arrow::Int64Array.from_values([2_i64]).as(Arrow::ArrayBase),
      ""       => Arrow::Int64Array.from_values([3_i64]).as(Arrow::ArrayBase),
    })

    [parquet_round_trip(frame), ipc_round_trip(frame), csv_round_trip(frame)].each do |restored|
      restored.column_names.should eq(["a,b", "привет", ""])
      restored.rows.should eq([{"a,b" => 1_i64, "привет" => 2_i64, "" => 3_i64}])
    end
  end

  it "keeps an all-null column typed in the binary formats" do
    frame = SpecDtypes.frame(
      Arrow::PrimitiveArray(Int64).from_nullable([nil, nil, nil]).as(Arrow::ArrayBase)
    )

    [parquet_round_trip(frame), ipc_round_trip(frame)].each do |restored|
      restored.dtypes["v"].should eq(Arrow::DataType.int64)
      restored["v"].to_a.should eq([nil, nil, nil])
    end
  end

  it "round-trips CSV exactly when the schema is supplied" do
    SpecDtypes.columns.each do |(label, array)|
      next if (CSV_UNREADABLE + CSV_UNWRITABLE).includes?(label)

      original = SpecDtypes.frame(array)
      restored = csv_round_trip(original, schema: original.schema)

      restored.schema.should eq(original.schema), "csv #{label}: schema"
      restored["v"].to_a.should eq(original["v"].to_a), "csv #{label}: values"
    end
  end

  it "reads CSV inference without inventing a column type that loses a value" do
    # The first 100 records are empty, so inference sees no values at all. A
    # `Null` column would then read the 101st row's 5 as null; falling back to a
    # string keeps the value and the dtype is the documented "nothing was
    # guessed" one.
    csv = String.build do |io|
      io << "qty,label\n"
      100.times { io << ",\n" }
      io << "5,late\n"
    end

    read = Prism.read_csv(IO::Memory.new(csv), infer_rows: 100)

    read.dtypes["qty"].should eq(Arrow::DataType.utf8)
    read.dtypes["label"].should eq(Arrow::DataType.utf8)
    read["qty"].to_a.last.should eq("5")
    read["label"].to_a.last.should eq("late")
    read.row_count.should eq(101)
  end

  it "reads a header-only CSV as strings, not as Null" do
    empty = SpecFrames.sales.filter(col("qty") > lit(100_i64))
    read = Prism.read_csv(IO::Memory.new(csv_text(empty)))

    read.row_count.should eq(0)
    read.dtypes.values.uniq.should eq([Arrow::DataType.utf8])
  end

  it "writes every dtype CSV can carry, and nothing it cannot" do
    SpecDtypes.columns.each do |(label, array)|
      frame = SpecDtypes.frame(array)

      if CSV_UNWRITABLE.includes?(label)
        error = expect_raises(Prism::UnsupportedOperation, /column "v"/) do
          Prism.write_csv(IO::Memory.new, frame)
        end
        error.message.to_s.should contain("CSV")
        error.message.to_s.should contain(array.data_type.to_s)
      else
        Prism.write_csv(IO::Memory.new, frame)
      end
    end
  end

  it "names the column it refuses, and only that one" do
    frame = Prism.from_columns({
      qty:  Arrow::Int64Array.from_values([1_i64, 2_i64]).as(Arrow::ArrayBase),
      blob: Arrow::BinaryArray.from_nullable([Bytes[1_u8], nil]).as(Arrow::ArrayBase),
    })

    error = expect_raises(Prism::UnsupportedOperation, /column "blob"/) do
      Prism.write_csv(IO::Memory.new, frame)
    end
    error.message.to_s.should contain("Binary")
    error.message.to_s.should contain("Parquet") # points at the format that can carry it
  end

  it "refuses before it opens the path, leaving an existing file intact" do
    path = SpecPaths.temp("refused.csv")
    File.write(path, "keep me\n")

    frame = SpecDtypes.frame(
      Arrow::BinaryArray.from_nullable([Bytes[1_u8], nil]).as(Arrow::ArrayBase)
    )

    expect_raises(Prism::UnsupportedOperation, /column "v"/) do
      Prism.write_csv(path, frame)
    end
    File.read(path).should eq("keep me\n")
  end

  it "writes CSV text that never re-reads as a different value" do
    # The documented lossiness of inference: text loses numeric width and every
    # dtype CSV cannot name. The values themselves must survive as text, which
    # is what this pins.
    cases = {
      "i32"        => {SpecDtypes.frame(Arrow::PrimitiveArray(Int32).from_nullable([1_i32, nil, -3_i32]).as(Arrow::ArrayBase)), Arrow::DataType.int64},
      "f32"        => {SpecDtypes.frame(Arrow::PrimitiveArray(Float32).from_nullable([1.5_f32, nil, -2.5_f32]).as(Arrow::ArrayBase)), Arrow::DataType.float64},
      "dict"       => {SpecDtypes.frame(Arrow::DictionaryArray(Int32).from_options(["x", "y", nil]).as(Arrow::ArrayBase)), Arrow::DataType.utf8},
      "large_utf8" => {SpecDtypes.frame(Arrow::LargeStringArray.from_nullable(["a", nil, "cc"]).as(Arrow::ArrayBase)), Arrow::DataType.utf8},
      "duration"   => {SpecDtypes.frame(Arrow::PrimitiveArray(Int64).from_nullable([0_i64, nil, 2_000_i64]).with_data_type(Arrow::DataType.duration(Arrow::TimeUnit::Millisecond)).as(Arrow::ArrayBase)), Arrow::DataType.utf8},
    }

    cases.each do |label, (frame, expected_dtype)|
      read = csv_round_trip(frame)
      read.dtypes["v"].should eq(expected_dtype), "csv #{label}: inferred dtype"
    end
  end

  it "keeps a string cell that looks numeric as text when asked to" do
    frame = SpecDtypes.frame(Arrow::StringArray.from_values(["0012", "13", "1e5"]).as(Arrow::ArrayBase))

    read = Prism.read_csv(IO::Memory.new(csv_text(frame)), infer_rows: 0)

    read.dtypes["v"].should eq(Arrow::DataType.utf8)
    read["v"].to_a.should eq(["0012", "13", "1e5"])
  end
end
