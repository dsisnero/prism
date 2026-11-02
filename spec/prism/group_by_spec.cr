require "../spec_helper"

include Prism::DSL

# Discovery order of `region` is west, east — deliberately not alphabetical, so
# `maintain_order` and sorted output can be told apart.
private def group_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    region:   Arrow::StringArray.from_nullable(["west", "east", "west", "east", "west"]),
    category: Arrow::StringArray.from_values(["a", "a", "b", "b", "a"]),
    price:    Arrow::Float64Array.from_nullable([10.0, 20.0, 30.0, nil, 50.0]),
    qty:      Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64, 5_i64]),
  })
end

private def group_frame : Prism::DataFrame
  Prism.from_arrow(group_batch)
end

private def null_key_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    region: Arrow::StringArray.from_nullable(["east", nil, "east"]),
    price:  Arrow::Float64Array.from_values([1.0, 2.0, 3.0]),
  })
end

private def null_key_frame : Prism::DataFrame
  Prism.from_arrow(null_key_batch)
end

describe Prism::GroupBy do
  describe "single key" do
    it "returns groups in discovery order by default" do
      grouped = group_frame.group_by("region").aggregate(
        total: col("price").sum,
      )

      grouped["region"].to_a.should eq(["west", "east"])
      grouped["total"].to_a.should eq([90.0, 20.0])
    end

    it "sorts by key when maintain_order is false" do
      grouped = group_frame.group_by("region", maintain_order: false).aggregate(
        total: col("price").sum,
      )

      grouped["region"].to_a.should eq(["east", "west"])
      grouped["total"].to_a.should eq([20.0, 90.0])
    end

    it "counts non-null values and all rows separately" do
      grouped = group_frame.group_by("region").aggregate(
        present: col("price").count,
        rows: col("price").count_all,
      )

      grouped["present"].to_a.should eq([3_u32, 1_u32])
      grouped["rows"].to_a.should eq([3_u32, 2_u32])
    end

    it "computes mean, min, max, first and last" do
      grouped = group_frame.group_by("region").aggregate(
        mean: col("price").mean,
        low: col("price").min,
        high: col("price").max,
        first: col("price").first,
        last: col("price").last,
      )

      # west = rows 0,2,4 (10,30,50); east = rows 1,3 (20,nil)
      grouped["mean"].to_a.should eq([30.0, 20.0])
      grouped["low"].to_a.should eq([10.0, 20.0])
      grouped["high"].to_a.should eq([50.0, 20.0])
      grouped["first"].to_a.should eq([10.0, 20.0])
      grouped["last"].to_a.should eq([50.0, nil])
    end

    it "computes n_unique, median, variance and standard deviation" do
      grouped = group_frame.group_by("region").aggregate(
        distinct: col("price").n_unique,
        median: col("price").median,
        var: col("price").variance,
        sd: col("price").std,
      )

      # west = 10,30,50 -> median 30, sample variance 400, sd 20
      grouped["distinct"].to_a.should eq([3_u32, 1_u32])
      grouped["median"].to_a.should eq([30.0, 20.0])
      grouped["var"].to_a.should eq([400.0, nil])
      grouped["sd"].to_a.should eq([20.0, nil])
    end
  end

  describe "multiple keys" do
    it "groups by the key combination" do
      grouped = group_frame.group_by("region", "category").aggregate(
        total: col("price").sum,
        rows: col("qty").count_all,
      )

      grouped["region"].to_a.should eq(["west", "east", "west", "east"])
      grouped["category"].to_a.should eq(["a", "a", "b", "b"])
      grouped["total"].to_a.should eq([60.0, 20.0, 30.0, nil])
      grouped["rows"].to_a.should eq([2_u32, 1_u32, 1_u32, 1_u32])
    end
  end

  describe "null keys" do
    it "keeps null keys as one group, in discovery order by default" do
      grouped = null_key_frame.group_by("region").aggregate(
        total: col("price").sum,
      )

      grouped["region"].to_a.should eq(["east", nil])
      grouped["total"].to_a.should eq([4.0, 2.0])
    end

    it "sorts null keys first when maintain_order is false" do
      grouped = null_key_frame.group_by("region", maintain_order: false).aggregate(
        total: col("price").sum,
      )

      grouped["region"].to_a.should eq([nil, "east"])
      grouped["total"].to_a.should eq([2.0, 4.0])
    end

    it "pulls null keys to the end when asked" do
      grouped = null_key_frame.group_by("region", null_keys: :last).aggregate(
        total: col("price").sum,
      )

      grouped["region"].to_a.should eq(["east", nil])
    end

    it "drops null-key rows when asked" do
      grouped = null_key_frame.group_by("region", null_keys: :drop).aggregate(
        total: col("price").sum,
      )

      grouped["region"].to_a.should eq(["east"])
      grouped["total"].to_a.should eq([4.0])
    end

    it "rejects an unknown null policy" do
      expect_raises(ArgumentError) do
        null_key_frame.group_by("region", null_keys: :sideways)
      end
    end
  end

  describe "float keys" do
    it "groups signed zero and every NaN together, agreeing with distinct" do
      signaling_nan = 0x7FF0_0000_0000_0001_u64.unsafe_as(Float64)
      frame = Prism.from_columns({
        f: Arrow::Float64Array.from_values(
          [0.0, -0.0, Float64::NAN, signaling_nan, 1.0]
        ),
        v: Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64, 5_i64]),
      })

      grouped = frame.group_by("f").aggregate(total: col("v").sum)

      # {0.0, NaN, 1.0}: -0.0 joins 0.0 and both NaNs are one key.
      grouped.row_count.should eq(3)
      frame.distinct("f").row_count.should eq(3)
      grouped["total"].to_a.should eq([3_i64, 7_i64, 5_i64])
    end
  end

  describe "result shape" do
    it "puts key columns first, then aggregates, with derived dtypes" do
      grouped = group_frame.group_by("region").aggregate(
        total: col("price").sum,
        mean: col("price").mean,
        rows: col("qty").count,
      )

      grouped.column_names.should eq(["region", "total", "mean", "rows"])
      grouped.dtypes["region"].should eq(Arrow::DataType.utf8)
      grouped.dtypes["total"].should eq(Arrow::DataType.float64)
      grouped.dtypes["mean"].should eq(Arrow::DataType.float64)
      grouped.dtypes["rows"].should eq(Arrow::DataType.uint32)
    end

    it "returns one chunk" do
      grouped = Prism.from_batches(
        [group_batch.slice(0, 3), group_batch.slice(3, 2)]
      ).group_by("region").aggregate(total: col("price").sum)

      count = 0
      grouped.each_batch { count += 1 }
      count.should eq(1)
    end

    it "handles an empty frame with the right schema" do
      empty = group_frame.filter(col("qty") > lit(100))
      grouped = empty.group_by("region").aggregate(total: col("price").sum)

      grouped.row_count.should eq(0)
      grouped.column_names.should eq(["region", "total"])
      grouped.dtypes["total"].should eq(Arrow::DataType.float64)
    end

    it "returns null for a group whose aggregate input is entirely null" do
      batch = Arrow::RecordBatch.from_columns({
        region: Arrow::StringArray.from_values(["east", "east"]),
        price:  Arrow::Float64Array.from_nullable([nil, nil]),
      })
      grouped = Prism.from_arrow(batch).group_by("region").aggregate(
        total: col("price").sum,
        mean: col("price").mean,
      )

      grouped["total"].to_a.should eq([nil])
      grouped["mean"].to_a.should eq([nil])
    end
  end

  describe "aggregate forms" do
    it "accepts aliased aggregations through #agg" do
      grouped = group_frame.group_by("region").agg(
        col("price").sum.alias("total"),
        col("qty").max.alias("most"),
      )

      grouped.column_names.should eq(["region", "total", "most"])
      grouped["total"].to_a.should eq([90.0, 20.0])
      grouped["most"].to_a.should eq([5_i64, 4_i64])
    end

    it "accepts a Hash for runtime-built column names" do
      columns = {"total" => col("price").sum.as(Prism::Expr)}
      grouped = group_frame.group_by("region").aggregate(columns)

      grouped["total"].to_a.should eq([90.0, 20.0])
    end

    it "rejects a non-aggregate expression" do
      expect_raises(Prism::InvalidAggregation) do
        group_frame.group_by("region").aggregate(total: col("price"))
      end
    end

    it "requires an alias in the variadic form" do
      expect_raises(Prism::MissingAlias) do
        group_frame.group_by("region").agg(col("price").sum)
      end
    end
  end

  describe "expression keys" do
    it "groups by an aliased expression" do
      grouped = group_frame.group_by(
        (col("qty") % lit(2)).cast(Arrow::DataType.int64).alias("parity"),
      ).aggregate(rows: col("qty").count_all)

      grouped.column_names.should eq(["parity", "rows"])
      grouped.row_count.should eq(2)
    end
  end
end
