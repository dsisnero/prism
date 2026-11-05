require "../../spec_helper"

# The reader parses by position, so a supplied schema is only safe if Prism
# matches it to the header by name first. These two same-typed columns would
# swap their values silently if the fields were taken in order.
private def csv_two_text : String
  "a,b\n1,x\n2,y\n"
end

private def csv_utf8_schema(*names : String) : Arrow::Schema
  fields = names.map { |name| Arrow::Field.new(name, Arrow::DataType.utf8, true) }
  Arrow::Schema.new(Arrow::Fields.from(fields.to_a))
end

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

  describe "a supplied schema" do
    it "is matched to the file by name, not by position" do
      read = Prism.read_csv(IO::Memory.new(csv_two_text), schema: csv_utf8_schema("b", "a"))

      read.column_names.should eq(["a", "b"])
      read.rows.should eq([{"a" => "1", "b" => "x"}, {"a" => "2", "b" => "y"}])
    end

    it "keeps the file's column order whatever order the schema lists" do
      read = Prism.read_csv(IO::Memory.new(csv_two_text), schema: csv_utf8_schema("b", "a"))

      read.column_names.should eq(["a", "b"])
    end

    it "uses the schema's dtypes through the match" do
      read = Prism.read_csv(
        IO::Memory.new("a,b\n1,2\n"),
        schema: Arrow::Schema.new(Arrow::Fields.from([
          Arrow::Field.new("b", Arrow::DataType.int64, true),
          Arrow::Field.new("a", Arrow::DataType.int64, true),
        ])),
      )

      read.rows.should eq([{"a" => 1_i64, "b" => 2_i64}])
    end

    it "names a column the file does not have" do
      error = expect_raises(Prism::SchemaError, /column "y"/) do
        Prism.read_csv(IO::Memory.new(csv_two_text), schema: csv_utf8_schema("a", "y"))
      end
      error.message.to_s.should contain("does not have")
      error.message.to_s.should contain("a, b")
    end

    it "names a file column the schema is missing" do
      error = expect_raises(Prism::SchemaError, /column "b"/) do
        Prism.read_csv(IO::Memory.new(csv_two_text), schema: csv_utf8_schema("a"))
      end
      error.message.to_s.should contain("missing")
    end

    it "reports both directions at once" do
      error = expect_raises(Prism::SchemaError) do
        Prism.read_csv(IO::Memory.new(csv_two_text), schema: csv_utf8_schema("a", "c", "d"))
      end
      error.message.to_s.should contain(%(column "b"))
      error.message.to_s.should contain(%(column "c"))
    end

    it "rejects a schema with two columns of one name" do
      expect_raises(Prism::DuplicateColumn, /"a"/) do
        Prism.read_csv(IO::Memory.new(csv_two_text), schema: csv_utf8_schema("a", "a"))
      end
    end

    it "is positional when the file has no header" do
      read = Prism.read_csv(
        IO::Memory.new("1,x\n2,y\n"),
        header: false,
        schema: csv_utf8_schema("first", "second"),
      )

      read.column_names.should eq(["first", "second"])
      read.rows.should eq([{"first" => "1", "second" => "x"}, {"first" => "2", "second" => "y"}])
    end

    it "rejects a schema whose width does not match a headerless file" do
      expect_raises(Prism::SchemaError, /2 columns/) do
        Prism.read_csv(
          IO::Memory.new("1,x\n"),
          header: false,
          schema: csv_utf8_schema("only"),
        )
      end
    end
  end
end
