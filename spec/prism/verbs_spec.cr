require "../spec_helper"

include Prism::DSL

private def verbs_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    region: Arrow::StringArray.from_nullable(["east", "west", "east", "west"]),
    price:  Arrow::Float64Array.from_nullable([10.0, 20.0, 30.0, nil]),
    qty:    Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64]),
    status: Arrow::StringArray.from_values(["ok", "bad", "ok", "ok"]),
  })
end

private def verbs_frame : Prism::DataFrame
  Prism.from_arrow(verbs_batch)
end

# Same rows, two chunks — every element-wise verb must preserve this.
private def chunked_frame : Prism::DataFrame
  Prism.from_batches([verbs_batch.slice(0, 2), verbs_batch.slice(2, 2)])
end

private def chunk_count(df : Prism::DataFrame) : Int32
  count = 0
  df.each_batch { count += 1 }
  count
end

describe Prism::DataFrame do
  describe "#filter" do
    it "keeps rows matching an expression" do
      filtered = verbs_frame.filter(col("status") == "ok")

      filtered.column_names.should eq(["region", "price", "qty", "status"])
      filtered.row_count.should eq(3)
      filtered["region"].to_a.should eq(["east", "east", "west"])
    end

    it "drops rows where the predicate is null" do
      filtered = verbs_frame.filter(col("price") > lit(15.0))

      filtered["price"].to_a.should eq([20.0, 30.0])
    end

    it "accepts a block when Prism::DSL is included" do
      verbs_frame.filter { col("qty") > 2 }["qty"].to_a.should eq([3_i64, 4_i64])
    end

    it "rejects a non-boolean predicate" do
      expect_raises(Prism::DtypeError) { verbs_frame.filter(col("qty")) }
    end

    it "preserves chunks" do
      chunked = chunked_frame

      chunked.filter(col("qty") > 1).row_count.should eq(3)
      chunk_count(chunked.filter(col("qty") > 1)).should eq(2)
    end
  end

  describe "#select" do
    it "selects columns by name" do
      selected = verbs_frame.select("region", "price")

      selected.column_names.should eq(["region", "price"])
      selected["price"].to_a.should eq([10.0, 20.0, 30.0, nil])
      selected.schema.field_with_name("price").nullable?.should be_true
    end

    it "projects without copying the underlying array" do
      batch = verbs_batch
      frame = Prism.from_arrow(batch)

      frame.select("price").to_arrow["price"].same?(batch["price"]).should be_true
    end

    it "selects expressions with aliases" do
      selected = verbs_frame.select(
        col("region"),
        (col("price") * lit(2)).alias("double_price"),
      )

      selected.column_names.should eq(["region", "double_price"])
      selected.dtypes["double_price"].should eq(Arrow::DataType.float64)
      selected["double_price"].to_a.should eq([20.0, 40.0, 60.0, nil])
    end

    it "requires an alias for a computed expression" do
      expect_raises(Prism::MissingAlias, /alias/) do
        verbs_frame.select(col("qty") + lit(1))
      end
    end

    it "raises ColumnNotFound for an unknown name" do
      expect_raises(Prism::ColumnNotFound, /nope/) { verbs_frame.select("nope") }
    end

    it "preserves chunks" do
      chunk_count(chunked_frame.select("qty", "price")).should eq(2)
    end
  end

  describe "#with_column" do
    it "appends a computed column" do
      extended = verbs_frame.with_column("total", col("price") * col("qty"))

      extended.column_names.should eq(["region", "price", "qty", "status", "total"])
      extended.dtypes["total"].should eq(Arrow::DataType.float64)
      extended["total"].to_a.should eq([10.0, 40.0, 90.0, nil])
    end

    it "replaces an existing column in place" do
      replaced = verbs_frame.with_column("qty", col("qty") * lit(10))

      replaced.column_names.should eq(["region", "price", "qty", "status"])
      replaced["qty"].to_a.should eq([10_i64, 20_i64, 30_i64, 40_i64])
    end

    it "accepts a block" do
      extended = verbs_frame.with_column("double") { col("qty") * lit(2) }

      extended["double"].to_a.should eq([2_i64, 4_i64, 6_i64, 8_i64])
    end

    it "adds several columns with with_columns" do
      extended = verbs_frame.with_columns(
        doubled: col("qty") * lit(2),
        flagged: col("qty") > lit(2),
      )

      extended["doubled"].to_a.should eq([2_i64, 4_i64, 6_i64, 8_i64])
      extended["flagged"].to_a.should eq([false, false, true, true])
    end

    it "preserves chunks" do
      chunk_count(chunked_frame.with_column("t", col("qty") + lit(1))).should eq(2)
    end
  end

  describe "#drop" do
    it "removes columns and keeps the rest" do
      dropped = verbs_frame.drop("status", :region)

      dropped.column_names.should eq(["price", "qty"])
      dropped["qty"].to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64])
    end

    it "raises ColumnNotFound for an unknown column" do
      expect_raises(Prism::ColumnNotFound, /nope/) { verbs_frame.drop("nope") }
    end

    it "preserves chunks" do
      chunk_count(chunked_frame.drop("status")).should eq(2)
    end
  end

  describe "#rename" do
    it "renames columns without copying data" do
      batch = verbs_batch
      frame = Prism.from_arrow(batch)
      renamed = frame.rename({"price" => "cost", :qty => "quantity"})

      renamed.column_names.should eq(["region", "cost", "quantity", "status"])
      renamed["cost"].to_a.should eq([10.0, 20.0, 30.0, nil])
      renamed.to_arrow["cost"].same?(batch["price"]).should be_true
    end

    it "accepts a single old/new pair" do
      verbs_frame.rename("status", "state").column_names.should eq(
        ["region", "price", "qty", "state"]
      )
    end

    it "raises ColumnNotFound for an unknown source name" do
      expect_raises(Prism::ColumnNotFound, /nope/) do
        verbs_frame.rename({"nope" => "x"})
      end
    end
  end

  describe "#cast" do
    it "casts selected columns" do
      casted = verbs_frame.cast({"qty" => Arrow::DataType.float64})

      casted.dtypes["qty"].should eq(Arrow::DataType.float64)
      casted["qty"].to_a.should eq([1.0, 2.0, 3.0, 4.0])
    end

    it "casts every column to one dtype" do
      frame = Prism.from_columns({
        a: Arrow::PrimitiveArray(Int32).from_values([1, 2]),
        b: Arrow::PrimitiveArray(Int32).from_values([3, 4]),
      })

      casted = frame.cast(Arrow::DataType.int64)

      casted.dtypes.values.uniq.should eq([Arrow::DataType.int64])
      casted["b"].to_a.should eq([3_i64, 4_i64])
    end
  end

  describe "#sort_by" do
    it "sorts ascending with nulls last" do
      sorted = verbs_frame.sort_by("price")

      sorted["price"].to_a.should eq([10.0, 20.0, 30.0, nil])
    end

    it "sorts descending with nulls last" do
      sorted = verbs_frame.sort_by("price", order: :desc)

      sorted["price"].to_a.should eq([30.0, 20.0, 10.0, nil])
    end

    it "sorts by several keys with per-key direction" do
      sorted = verbs_frame.sort_by(col("region").asc, col("qty").desc)

      sorted["region"].to_a.should eq(["east", "east", "west", "west"])
      sorted["qty"].to_a.should eq([3_i64, 1_i64, 4_i64, 2_i64])
    end

    it "places nulls first when asked" do
      verbs_frame.sort_by("price", nulls: :first)["price"].to_a
        .should eq([nil, 10.0, 20.0, 30.0])
    end

    it "collapses chunks because it reorders rows" do
      chunk_count(chunked_frame.sort_by("qty")).should eq(1)
    end
  end

  describe "#distinct" do
    it "keeps the first row of each key, preserving order" do
      unique = verbs_frame.distinct("region")

      unique.row_count.should eq(2)
      unique["region"].to_a.should eq(["east", "west"])
      unique["qty"].to_a.should eq([1_i64, 2_i64])
    end

    it "uses every column as the key by default" do
      verbs_frame.distinct.row_count.should eq(4)
    end

    it "collapses chunks" do
      chunk_count(chunked_frame.distinct("region")).should eq(1)
    end
  end

  describe "#drop_nulls and #fill_null" do
    it "drops rows with nulls in the named columns" do
      cleaned = verbs_frame.drop_nulls("price")

      cleaned.row_count.should eq(3)
      cleaned["price"].to_a.should eq([10.0, 20.0, 30.0])
    end

    it "requires every named column to be present" do
      expect_raises(Prism::ColumnNotFound, /nope/) { verbs_frame.drop_nulls("nope") }
    end

    it "fills nulls in every column" do
      filled = verbs_frame.fill_null(0)

      filled["price"].to_a.should eq([10.0, 20.0, 30.0, 0.0])
    end

    it "fills nulls per column" do
      filled = verbs_frame.fill_null({"price" => -1.0})

      filled["price"].to_a.should eq([10.0, 20.0, 30.0, -1.0])
    end

    it "preserves chunks" do
      chunk_count(chunked_frame.drop_nulls("price")).should eq(2)
    end
  end

  describe "#with_row_index" do
    it "adds a zero-based index column" do
      indexed = verbs_frame.with_row_index

      indexed.column_names.first.should eq("index")
      indexed.dtypes["index"].should eq(Arrow::DataType.uint32)
      indexed["index"].to_a.should eq([0_u32, 1_u32, 2_u32, 3_u32])
    end

    it "continues numbering across chunks" do
      indexed = chunked_frame.with_row_index("row", offset: 10)

      indexed["row"].to_a.should eq([10_u32, 11_u32, 12_u32, 13_u32])
      chunk_count(indexed).should eq(2)
    end
  end
end
