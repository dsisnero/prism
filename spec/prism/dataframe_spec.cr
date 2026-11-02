require "../spec_helper"

private def sample_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    region: Arrow::StringArray.from_nullable(["east", "west", "east", nil]),
    price:  Arrow::Float64Array.from_nullable([10.0, 20.0, 30.0, nil]),
    qty:    Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64]),
  })
end

private def split_batch(batch : Arrow::RecordBatch) : Array(Arrow::RecordBatch)
  [batch.slice(0, 2), batch.slice(2, 2)]
end

private def frame : Prism::DataFrame
  Prism.from_arrow(sample_batch)
end

describe Prism::DataFrame do
  describe ".from_arrow / #to_arrow" do
    it "round-trips a batch unchanged, including schema and nulls" do
      batch = sample_batch
      df = Prism.from_arrow(batch)

      df.to_arrow.should eq(batch)
      df.to_arrow.schema.should eq(batch.schema)
      df.to_arrow.schema.fields.map(&.nullable?).should eq([true, true, false])
    end

    it "returns the original batch when there is a single chunk" do
      batch = sample_batch
      Prism.from_arrow(batch).to_arrow.same?(batch).should be_true
    end
  end

  describe ".from_batches" do
    it "keeps chunks and reports the total row count" do
      batches = split_batch(sample_batch)
      df = Prism.from_batches(batches)

      df.row_count.should eq(4)
      df.shape.should eq({rows: 4_i64, columns: 3_i32})
      chunks = [] of Arrow::RecordBatch
      df.each_batch { |b| chunks << b }
      chunks.size.should eq(2)
    end

    it "concatenates chunks in order for #to_arrow" do
      df = Prism.from_batches(split_batch(sample_batch))

      df.to_arrow.num_rows.should eq(4)
      df.to_arrow["region"].as(Arrow::StringArray).to_a.should eq(
        ["east", "west", "east", nil]
      )
    end

    it "accepts an empty batch list with an explicit schema" do
      schema = sample_batch.schema
      df = Prism.from_batches([] of Arrow::RecordBatch, schema: schema)

      df.row_count.should eq(0)
      df.column_names.should eq(["region", "price", "qty"])
      df.to_arrow.num_rows.should eq(0)
    end

    it "raises when a batch has a different schema" do
      other = Arrow::RecordBatch.from_columns({
        region: Arrow::StringArray.from_values(["east"]),
      })

      expect_raises(Prism::SchemaError, /schema/) do
        Prism.from_batches([sample_batch, other])
      end
    end
  end

  describe ".from_columns" do
    it "builds a frame from a Hash of Arrow arrays" do
      df = Prism.from_columns({
        "id"   => Arrow::Int64Array.from_values([1_i64, 2_i64]),
        "name" => Arrow::StringArray.from_nullable(["a", nil]),
      })

      df.column_names.should eq(["id", "name"])
      df.row_count.should eq(2)
    end

    it "builds a frame from a NamedTuple" do
      df = Prism.from_columns({id: Arrow::Int64Array.from_values([1_i64])})

      df.column_names.should eq(["id"])
    end
  end

  describe "schema introspection" do
    it "exposes column names and dtypes in schema order" do
      df = frame

      df.column_names.should eq(["region", "price", "qty"])
      df.dtypes.keys.should eq(["region", "price", "qty"])
      df.dtypes["price"].should eq(Arrow::DataType.float64)
      df.schema.field_with_name("qty").data_type.should eq(Arrow::DataType.int64)
    end
  end

  describe "#[]" do
    it "returns a series for a column name" do
      series = frame["price"]

      series.should be_a(Prism::Series)
      series.name.should eq("price")
      series.dtype.should eq(Arrow::DataType.float64)
      series.to_a.should eq([10.0, 20.0, 30.0, nil])
    end

    it "accepts a Symbol" do
      frame[:qty].name.should eq("qty")
    end

    it "raises ColumnNotFound naming the column and listing the available ones" do
      expect_raises(Prism::ColumnNotFound, /salry/) do
        frame["salry"]
      end
    end

    it "returns nil from #[]? for a missing column" do
      frame["salry"]?.should be_nil
      frame["qty"]?.try(&.name).should eq("qty")
    end
  end

  describe "#head / #tail / #slice" do
    it "returns the first rows without collapsing other chunks" do
      df = Prism.from_batches(split_batch(sample_batch))

      head = df.head(3)
      head.row_count.should eq(3)
      head["region"].to_a.should eq(["east", "west", "east"])
      head.schema.should eq(df.schema)
    end

    it "returns the last rows" do
      tail = frame.tail(2)

      tail.row_count.should eq(2)
      tail["region"].to_a.should eq(["east", nil])
    end

    it "clamps beyond the end and reports fewer rows" do
      df = frame

      df.head(99).row_count.should eq(4)
      df.tail(99).row_count.should eq(4)
      df.slice(10, 5).row_count.should eq(0)
      df.slice(2, 99).row_count.should eq(2)
    end

    it "slices across chunk boundaries" do
      df = Prism.from_batches(split_batch(sample_batch))

      sliced = df.slice(1, 2)
      sliced.row_count.should eq(2)
      sliced["region"].to_a.should eq(["west", "east"])
      sliced["qty"].to_a.should eq([2_i64, 3_i64])
    end

    it "rejects a negative offset" do
      expect_raises(ArgumentError) { frame.slice(-1, 2) }
    end
  end

  describe "#slice(range) and bracket indexing" do
    it "slices an inclusive range" do
      sliced = frame[1..2]

      sliced["qty"].to_a.should eq([2_i64, 3_i64])
      sliced.schema.should eq(frame.schema)
    end

    it "slices an exclusive range" do
      frame[1...3]["qty"].to_a.should eq([2_i64, 3_i64])
    end

    it "counts a negative endpoint from the end" do
      frame[-2..]["qty"].to_a.should eq([3_i64, 4_i64])
      frame[0..-2]["qty"].to_a.should eq([1_i64, 2_i64, 3_i64])
    end

    it "accepts open-ended ranges" do
      frame[..1]["qty"].to_a.should eq([1_i64, 2_i64])
      frame[3..]["qty"].to_a.should eq([4_i64])
    end

    it "clamps to the frame instead of returning nil" do
      frame[10..20].row_count.should eq(0)
      frame[-99..1]["qty"].to_a.should eq([1_i64, 2_i64])
      frame[0..99].row_count.should eq(4)
    end

    it "returns an empty frame for an inverted range" do
      frame[3..1].row_count.should eq(0)
      frame[2...2].row_count.should eq(0)
    end

    it "keeps chunks when the range stays inside one" do
      df = Prism.from_batches(split_batch(sample_batch))

      df[0..1].batches.size.should eq(1)
      df[1..2].batches.size.should eq(2)
    end

    it "projects a list of columns" do
      selected = frame[["region", "qty"]]

      selected.column_names.should eq(["region", "qty"])
      selected["qty"].to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64])
      frame[[:qty]].column_names.should eq(["qty"])
    end

    it "rejects an unknown column in a list" do
      expect_raises(Prism::ColumnNotFound) { frame[["nope"]] }
    end
  end

  describe "row materialization" do
    it "returns one name-keyed hash per row, in schema order" do
      rows = frame.rows

      rows.size.should eq(4)
      rows.first.should eq(
        {"region" => "east", "price" => 10.0, "qty" => 1_i64}
      )
      rows.map(&.keys).uniq.should eq([["region", "price", "qty"]])
    end

    it "keeps nulls as nil rather than a zero value" do
      last = frame.rows.last

      last["region"].should be_nil
      last["price"].should be_nil
      last["qty"].should eq(4_i64)
    end

    it "yields rows in frame order across chunk boundaries" do
      df = Prism.from_batches(split_batch(sample_batch))
      seen = [] of Prism::Value

      df.each_row { |row| seen << row["region"] }

      seen.should eq(["east", "west", "east", nil])
      df.rows.should eq(frame.rows)
    end

    it "reads a single row by position, from any chunk" do
      frame.row(0)["region"].should eq("east")
      Prism.from_batches(split_batch(sample_batch)).row(3)["qty"].should eq(4_i64)
    end

    it "raises IndexError outside the frame, negative indices included" do
      expect_raises(IndexError, /row 4/) { frame.row(4) }
      expect_raises(IndexError, /row -1/) { frame.row(-1) }
    end

    it "gives every row an empty hash when the frame has no columns" do
      batch = Arrow::RecordBatch.from_columns(
        {} of String => Arrow::ArrayBase, row_count: 2
      )
      df = Prism.from_arrow(batch)

      df.rows.should eq([{} of String => Prism::Value, {} of String => Prism::Value])
      df.row(1).should be_empty
    end

    it "returns no rows for an empty frame" do
      Prism.from_batches([] of Arrow::RecordBatch, schema: sample_batch.schema)
        .rows.should be_empty
    end
  end

  describe "empty and zero-column frames" do
    it "reports a zero-column frame's row count" do
      batch = Arrow::RecordBatch.from_columns(
        {} of String => Arrow::ArrayBase, row_count: 5
      )
      df = Prism.from_arrow(batch)

      df.shape.should eq({rows: 5_i64, columns: 0_i32})
      df.column_names.should be_empty
      df.to_arrow.num_rows.should eq(5)
    end

    it "has no columns and no rows when built from nothing" do
      df = Prism.from_batches([] of Arrow::RecordBatch)

      df.column_names.should be_empty
      df.row_count.should eq(0)
      df.empty?.should be_true
    end
  end

  describe "#==" do
    it "compares schema and values" do
      frame.should eq(frame)
      frame.head(2).should_not eq(frame)
    end
  end
end
