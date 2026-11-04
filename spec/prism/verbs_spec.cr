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

    it "accepts a boolean series as the mask" do
      filtered = verbs_frame.filter(verbs_frame["qty"] > 2)

      filtered.column_names.should eq(["region", "price", "qty", "status"])
      filtered["qty"].to_a.should eq([3_i64, 4_i64])
    end

    it "drops rows where the mask is null" do
      filtered = verbs_frame.filter(verbs_frame["price"] > 15.0)

      filtered["price"].to_a.should eq([20.0, 30.0])
    end

    it "preserves chunks for a mask built from the frame's own columns" do
      chunked = chunked_frame

      kept = chunked.filter(chunked["qty"] > 1)
      kept.row_count.should eq(3)
      chunk_count(kept).should eq(2)
    end

    it "aligns a mask whose chunking differs from the frame's" do
      chunked = chunked_frame
      mask = Prism::Series.new(
        "mask",
        [Arrow::BooleanArray.from_values([true, false, true, true])],
        Arrow::DataType.boolean
      )

      kept = chunked.filter(mask)
      kept["qty"].to_a.should eq([1_i64, 3_i64, 4_i64])
      chunk_count(kept).should eq(1)
    end

    it "rejects a non-boolean mask and a mismatched length" do
      expect_raises(Prism::DtypeError, /qty/) do
        verbs_frame.filter(verbs_frame["qty"])
      end
      expect_raises(ArgumentError) do
        short = Prism::Series.new(
          "mask", [Arrow::BooleanArray.from_values([true])], Arrow::DataType.boolean
        )
        verbs_frame.filter(short)
      end
    end
  end

  describe "#remove" do
    it "keeps the rows the predicate rejects" do
      verbs_frame.remove(col("region") == "east")["region"].to_a.should eq(["west", "west"])
    end

    it "keeps rows where the predicate is null, unlike filter(pred.not)" do
      # price has one null: `not(null)` is null, and a null predicate drops.
      verbs_frame.remove(col("price") > 15.0)["price"].to_a.should eq([10.0, nil])
      verbs_frame.filter((col("price") > 15.0).not)["price"].to_a.should eq([10.0])
    end

    it "accepts a boolean series and keeps its null bits" do
      verbs_frame.remove(verbs_frame["price"] > 15.0)["qty"].to_a.should eq([1_i64, 4_i64])
    end

    it "accepts a block when Prism::DSL is included" do
      verbs_frame.remove { col("qty") > 3 }["qty"].to_a.should eq([1_i64, 2_i64, 3_i64])
    end

    it "preserves chunks" do
      chunked = chunked_frame

      chunk_count(chunked.remove(col("qty") > 3)).should eq(2)
    end

    it "rejects a non-boolean series mask" do
      expect_raises(Prism::DtypeError, /qty/) do
        verbs_frame.remove(verbs_frame["qty"])
      end
    end
  end

  describe "#select" do
    it "rejects the same column twice" do
      expect_raises(Prism::DuplicateColumn, /"region"/) do
        verbs_frame.select("region", "region")
      end
    end

    it "rejects an alias that collides with a selected column" do
      expect_raises(Prism::DuplicateColumn, /"region"/) do
        verbs_frame.select(col("region"), col("price").alias("region"))
      end
    end

    it "still allows the same column under two aliases" do
      verbs_frame.select(col("qty").alias("left"), col("qty").alias("right")).column_names
        .should eq(["left", "right"])
    end

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

  # `fill_null(0)` used to cast the literal into *every* column Arrow could cast
  # it to: a null string became "0", a null boolean became false, and a null list
  # became [0]. A frame-wide fill has to stay inside the value's own family.
  describe "#fill_null" do
    it "fills only the columns the value fits" do
      frame = Prism.from_columns({
        "i" => Arrow::Int64Array.from_nullable([1_i64, nil]),
        "f" => Arrow::Float64Array.from_nullable([1.5, nil]),
        "s" => Arrow::StringArray.from_nullable(["a", nil]),
        "b" => Arrow::BooleanArray.from_nullable([true, nil]),
      })

      filled = frame.fill_null(0)

      filled["i"].to_a.should eq([1_i64, 0_i64])
      filled["f"].to_a.should eq([1.5, 0.0])
      filled["s"].to_a.should eq(["a", nil])
      filled["b"].to_a.should eq([true, nil])
      filled.dtypes["s"].should eq(Arrow::DataType.utf8)
      filled.dtypes["b"].should eq(Arrow::DataType.boolean)
    end

    it "fills strings and booleans only with their own kind" do
      frame = Prism.from_columns({
        "i"    => Arrow::Int64Array.from_nullable([1_i64, nil]),
        "s"    => Arrow::StringArray.from_nullable(["a", nil]),
        "flag" => Arrow::BooleanArray.from_nullable([true, nil]),
      })

      frame.fill_null("z")["i"].to_a.should eq([1_i64, nil])
      frame.fill_null("z")["s"].to_a.should eq(["a", "z"])
      frame.fill_null(true)["i"].to_a.should eq([1_i64, nil])
      frame.fill_null(true)["flag"].to_a.should eq([true, true])
    end

    it "does not narrow a column with a float literal" do
      frame = Prism.from_columns({
        "i" => Arrow::Int64Array.from_nullable([1_i64, nil]),
        "f" => Arrow::Float64Array.from_nullable([1.5, nil]),
      })

      filled = frame.fill_null(1.5)

      filled["i"].to_a.should eq([1_i64, nil])
      filled.dtypes["i"].should eq(Arrow::DataType.int64)
      filled["f"].to_a.should eq([1.5, 1.5])
    end

    it "keeps a narrow column narrow" do
      frame = Prism.from_columns({
        "small" => Arrow::PrimitiveArray(Int32).from_nullable([1, nil]),
      })

      filled = frame.fill_null(0)

      filled.dtypes["small"].should eq(Arrow::DataType.int32)
      filled["small"].to_a.should eq([1, 0])
    end

    it "never turns a scalar into a list" do
      frame = Prism.from_columns({
        "l" => Arrow::ListArray.from_iter_primitive(Int64, [
          [1_i64] of Int64?,
          nil,
        ] of Array(Int64?)?).as(Arrow::ArrayBase),
      })

      filled = frame.fill_null(0)

      filled.dtypes["l"].should eq(frame.dtypes["l"])
      filled["l"].to_a.should eq([[1_i64], nil])
    end

    it "rejects a named column the value cannot fill" do
      frame = Prism.from_columns({
        "i" => Arrow::Int64Array.from_nullable([1_i64, nil]),
        "l" => Arrow::ListArray.from_iter_primitive(Int64, [
          [1_i64] of Int64?,
          nil,
        ] of Array(Int64?)?).as(Arrow::ArrayBase),
      })

      expect_raises(Prism::DtypeError, /cannot fill/) do
        frame.fill_null({"l" => 0})
      end
      expect_raises(Prism::DtypeError, /cannot fill/) do
        frame.fill_null({"i" => 1.5})
      end
      frame.fill_null({"i" => 0})["i"].to_a.should eq([1_i64, 0_i64])
    end
  end

  describe "#rename" do
    # A frame with two columns of one name loses one of them to `df[name]`, and
    # nothing else complains: `to_s`, `to_arrow` and a Parquet write all succeed.
    # Renaming onto a taken name has to fail instead.
    it "rejects a rename onto an existing column name" do
      expect_raises(Prism::DuplicateColumn, /"region".*(rename|exists)/) do
        verbs_frame.rename("price", "region")
      end
    end

    it "rejects two renames onto the same name" do
      expect_raises(Prism::DuplicateColumn, /"cost"/) do
        verbs_frame.rename({"price" => "cost", "qty" => "cost"})
      end
    end

    it "still allows a swap, and renaming a column to itself" do
      verbs_frame.rename({"region" => "price", "price" => "region"}).column_names
        .should eq(["price", "region", "qty", "status"])
      verbs_frame.rename("qty", "qty").column_names
        .should eq(["region", "price", "qty", "status"])
    end

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

    it "reads a leading - on a column name as descending" do
      verbs_frame.sort_by("-price")["price"].to_a.should eq([30.0, 20.0, 10.0, nil])
    end

    it "mixes prefixed and plain keys" do
      sorted = verbs_frame.sort_by("region", "-qty")

      sorted["region"].to_a.should eq(["east", "east", "west", "west"])
      sorted["qty"].to_a.should eq([3_i64, 1_i64, 4_i64, 2_i64])
    end

    it "keeps null placement from the keyword, not the prefix" do
      verbs_frame.sort_by("-price", nulls: :first)["price"].to_a
        .should eq([nil, 30.0, 20.0, 10.0])
    end

    it "leaves a column really named with a dash reachable as an expression" do
      odd = Prism.from_columns({"-odd" => Arrow::Int64Array.from_values([2_i64, 1_i64])})

      # The shorthand strips the dash, so it looks for a column named "odd".
      expect_raises(Prism::ColumnNotFound, /column "odd"/) { odd.sort_by("-odd") }
      odd.sort_by(Prism.col("-odd").asc)["-odd"].to_a.should eq([1_i64, 2_i64])
    end

    it "sorts by several keys with per-key direction" do
      sorted = verbs_frame.sort_by(col("region").asc, col("qty").desc)

      sorted["region"].to_a.should eq(["east", "east", "west", "west"])
      sorted["qty"].to_a.should eq([3_i64, 1_i64, 4_i64, 2_i64])
    end

    # Multi-key order is a lexicographic comparison, and the engine reaches it by
    # stable single-key passes (Phase 21). This fixture is chosen so every tie
    # breaker matters: duplicate prices, nulls, three keys, mixed directions.
    it "orders three keys with ties, nulls and mixed directions" do
      frame = Prism.from_columns({
        "region" => Arrow::StringArray.from_values(["east", "east", "west", "west"]),
        "price"  => Arrow::Float64Array.from_nullable([nil, 5.0, 5.0, nil]),
        "qty"    => Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64]),
      })

      sorted = frame.sort_by(
        col("price").asc(nulls: :last),
        col("region").desc,
        col("qty").desc,
      )

      # price 5.0 first (west qty 3, then east qty 2), then the null prices
      # (west qty 4, then east qty 1).
      sorted["qty"].to_a.should eq([3_i64, 2_i64, 4_i64, 1_i64])
      sorted["region"].to_a.should eq(["west", "east", "west", "east"])
      sorted["price"].to_a.should eq([5.0, 5.0, nil, nil])
    end

    # Same dtype split as below, but with one key: `lexsort_to_indices` forwards a
    # single column to `sort_to_indices`, so there is no chain to catch it and the
    # sort has to compare the structs directly (Phase 23).
    it "sorts by a single struct key" do
      structs = Arrow::StructArray.from_pairs([
        {Arrow::Field.new("x", Arrow::DataType.int64, true),
         Arrow::Int64Array.from_values([2_i64, 1_i64, 2_i64, 1_i64]).as(Arrow::ArrayBase)},
      ])
      frame = Prism.from_columns({
        "s"   => structs.as(Arrow::ArrayBase),
        "qty" => Arrow::Int64Array.from_values([3_i64, 4_i64, 1_i64, 2_i64]),
      })

      frame.sort_by(col("s").asc)["qty"].to_a.should eq([4_i64, 2_i64, 3_i64, 1_i64])
      frame.sort_by(col("s").desc)["qty"].to_a.should eq([3_i64, 1_i64, 4_i64, 2_i64])
      frame.top_n(2, col("s").desc)["qty"].to_a.should eq([3_i64, 1_i64])
    end

    # Sorting a struct key is supported by the comparator chain but not by the
    # per-key `sort_to_indices`, so the multi-key path must fall back rather than
    # raise "Sort not supported" (Phase 21).
    it "still sorts a struct key alongside a scalar key" do
      structs = Arrow::StructArray.from_pairs([
        {Arrow::Field.new("x", Arrow::DataType.int64, true),
         Arrow::Int64Array.from_values([2_i64, 1_i64, 2_i64, 1_i64]).as(Arrow::ArrayBase)},
      ])
      frame = Prism.from_columns({
        "s"   => structs.as(Arrow::ArrayBase),
        "qty" => Arrow::Int64Array.from_values([3_i64, 4_i64, 1_i64, 2_i64]),
      })

      sorted = frame.sort_by(col("s").asc, col("qty").asc)

      # Sorted by x, then by qty: (1,2), (1,4), (2,1), (2,3).
      sorted["qty"].to_a.should eq([2_i64, 4_i64, 1_i64, 3_i64])
    end

    it "treats an all-equal multi-key sort as the identity" do
      frame = Prism.from_columns({
        "k" => Arrow::Int64Array.from_values([1_i64, 1_i64, 1_i64]),
        "v" => Arrow::Int64Array.from_values([3_i64, 1_i64, 2_i64]),
      })

      frame.sort_by(col("k").asc, col("v").asc)["v"].to_a
        .should eq([1_i64, 2_i64, 3_i64])
      # Every key ties, so the original order survives, as with a stable sort.
      frame.sort_by(col("k").asc, col("k").desc)["v"].to_a
        .should eq([3_i64, 1_i64, 2_i64])
    end

    it "places nulls first when asked" do
      verbs_frame.sort_by("price", nulls: :first)["price"].to_a
        .should eq([nil, 10.0, 20.0, 30.0])
    end

    it "collapses chunks because it reorders rows" do
      chunk_count(chunked_frame.sort_by("qty")).should eq(1)
    end

    it "returns the same rows when it gathers only a window" do
      frame = chunked_frame
      keys = [Prism::SortKey.new(col("qty"), true)]

      windowed = frame.sort_by_slice(keys, 1, 2)
      windowed.should eq(frame.sort_by("qty", order: :desc).slice(1, 2))
      windowed["qty"].to_a.should eq([3_i64, 2_i64])
    end

    it "clamps a window that runs past the end" do
      frame = verbs_frame
      keys = [Prism::SortKey.new(col("qty"))]

      frame.sort_by_slice(keys, 2, 100)["qty"].to_a.should eq([3_i64, 4_i64])
      frame.sort_by_slice(keys, 10, 2).row_count.should eq(0)
    end

    it "rejects a negative window" do
      keys = [Prism::SortKey.new(col("qty"))]

      expect_raises(ArgumentError, /offset/) { verbs_frame.sort_by_slice(keys, -1, 2) }
      expect_raises(ArgumentError, /count/) { verbs_frame.sort_by_slice(keys, 0, -2) }
    end

    # A bounded sort returns the prefix a full sort would, so every window is the
    # same rows as slicing the fully sorted frame — including null placement,
    # per-key direction and the equal-key tie-break (spec Phase 18).
    it "matches a full sort for every window, nulls included" do
      frame = chunked_frame
      key_sets = [
        [Prism::SortKey.new(col("price"))],
        [Prism::SortKey.new(col("price"), false, true)],
        [Prism::SortKey.new(col("price"), true)],
        [Prism::SortKey.new(col("region")), Prism::SortKey.new(col("qty"), true)],
      ]

      key_sets.each do |keys|
        (0..frame.row_count).each do |offset|
          (0..3).each do |count|
            frame.sort_by_slice(keys, offset, count)
              .should eq(frame.sort_by(keys).slice(offset, count))
          end
        end
      end
    end

    it "treats an offset past the frame as an empty window, whatever its size" do
      keys = [Prism::SortKey.new(col("qty"))]

      # `4 + 10**10` does not fit an Int32, and the bound is capped at the row
      # count, so a huge offset is still an empty window rather than an overflow.
      verbs_frame.sort_by_slice(keys, 10_000_000_000, 2).row_count.should eq(0)
    end

    it "still rejects an unknown column on an empty window" do
      keys = [Prism::SortKey.new(col("nope"))]

      expect_raises(Prism::ColumnNotFound) do
        verbs_frame.sort_by_slice(keys, 10, 2)
      end
    end
  end

  describe "#top_n" do
    it "keeps the same rows as sorting and taking the head" do
      frame = chunked_frame

      top = frame.top_n(2, "qty")
      top.should eq(frame.sort_by("qty", order: :desc).head(2))
      top["qty"].to_a.should eq([4_i64, 3_i64])
    end

    it "sorts ascending when asked, and keeps the smallest" do
      verbs_frame.top_n(2, "qty", order: :asc)["qty"].to_a.should eq([1_i64, 2_i64])
    end

    it "follows the null placement keyword" do
      verbs_frame.top_n(2, "price", nulls: :first)["price"].to_a.should eq([nil, 30.0])
      verbs_frame.top_n(2, "price", nulls: :last)["price"].to_a.should eq([30.0, 20.0])
    end

    it "accepts several keys and explicit SortKeys" do
      by_region = verbs_frame.top_n(2, col("region").asc, col("qty").desc)
      by_region.should eq(verbs_frame.sort_by(col("region").asc, col("qty").desc).head(2))
      by_region["region"].to_a.should eq(["east", "east"])

      ascending = verbs_frame.top_n(2, Prism::SortKey.new(col("qty")))
      ascending["qty"].to_a.should eq([1_i64, 2_i64])
    end

    it "clamps to the row count and keeps the schema" do
      frame = verbs_frame

      frame.top_n(100, "qty").row_count.should eq(4)
      empty = frame.top_n(0, "qty")
      empty.row_count.should eq(0)
      empty.column_names.should eq(frame.column_names)
      empty["qty"].dtype.should eq(frame["qty"].dtype)
    end

    it "collapses chunks because it reorders rows" do
      chunk_count(chunked_frame.top_n(2, "qty")).should eq(1)
    end

    it "rejects a negative count" do
      expect_raises(ArgumentError, /count/) { verbs_frame.top_n(-1, "qty") }
    end

    it "rejects an unknown column" do
      expect_raises(Prism::ColumnNotFound, /nope/) { verbs_frame.top_n(2, "nope") }
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
