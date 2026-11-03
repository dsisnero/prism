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

# Counts evaluations, to prove a grouped aggregate reduces one operand across
# every group instead of re-evaluating the expression per group.
private class CountedOperand < Prism::Expr
  @@evals = 0

  def self.evals : Int32
    @@evals
  end

  def self.reset : Nil
    @@evals = 0
  end

  getter inner : Prism::Expr

  def initialize(@inner : Prism::Expr)
  end

  def key : String
    "counted(#{@inner.key})"
  end

  def columns : Array(String)
    @inner.columns
  end

  def dtype(schema : Arrow::Schema) : Arrow::DataType
    @inner.dtype(schema)
  end

  def eval(ctx : Prism::EvalContext) : Prism::Datum
    @@evals += 1
    @inner.eval(ctx)
  end
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
      # east = 20,null -> two distinct values, because null counts as one
      grouped["distinct"].to_a.should eq([3_u32, 2_u32])
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

    it "groups dictionary-encoded floats by value, like plain float keys" do
      # Grouping encodes the *values*, so a dictionary column canonicalizes the
      # same way a plain float column does: one key for `±0.0`, one for NaN.
      dict = Arrow::DictionaryArray(Int32).new(
        Arrow::PrimitiveArray(Int32).from_values([0, 1, 2, 3]),
        Arrow::Float64Array.from_values([0.0, -0.0, Float64::NAN, Float64::NAN]),
      ).as(Arrow::ArrayBase)
      frame = Prism.from_columns({
        f: dict,
        v: Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64]),
      })

      grouped = frame.group_by("f").aggregate(total: col("v").sum)

      grouped.row_count.should eq(2)
      frame.distinct("f").row_count.should eq(2)
      grouped["total"].to_a.should eq([3_i64, 7_i64])
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

  describe "one-call aggregates" do
    it "sums the named columns, keeping their names" do
      grouped = group_frame.group_by("region").sum("price", "qty")

      grouped.column_names.should eq(["region", "price", "qty"])
      grouped["price"].to_a.should eq([90.0, 20.0])
      grouped["qty"].to_a.should eq([9_i64, 6_i64])
    end

    it "defaults to every eligible column and never aggregates a key" do
      grouped = group_frame.group_by("region").sum

      # `category` is a string, so sum skips it; `region` is the key.
      grouped.column_names.should eq(["region", "price", "qty"])
      grouped["qty"].to_a.should eq([9_i64, 6_i64])
    end

    it "computes mean, median, std and var" do
      frame = group_frame.group_by("region")

      frame.mean("price")["price"].to_a.should eq([30.0, 20.0])
      frame.median("qty")["qty"].to_a.should eq([3.0, 3.0])
      frame.std("price")["price"].to_a.should eq([20.0, nil])
      frame.var("price")["price"].to_a.should eq([400.0, nil])
    end

    it "takes min and max over comparable columns" do
      grouped = group_frame.group_by("region").min

      grouped.column_names.should eq(["region", "category", "price", "qty"])
      grouped["category"].to_a.should eq(["a", "a"])
      grouped["price"].to_a.should eq([10.0, 20.0])
    end

    it "counts rows per group when no column is named" do
      grouped = group_frame.group_by("region").count

      grouped.column_names.should eq(["region", "count"])
      grouped["count"].to_a.should eq([3_u32, 2_u32])
    end

    it "counts non-null values when columns are named" do
      group_frame.group_by("region").count("price")["price"].to_a
        .should eq([3_u32, 1_u32])
    end

    it "counts distinct values with null as one, for every column by default" do
      grouped = group_frame.group_by("region").n_unique

      grouped.column_names.should eq(["region", "category", "price", "qty"])
      # east holds price 20.0 and one null, which is a value of its own.
      grouped["price"].to_a.should eq([3_u32, 2_u32])
    end

    it "matches the equivalent aggregate call" do
      one_call = group_frame.group_by("region", "category").std("price")
      explicit = group_frame.group_by("region", "category")
        .aggregate(price: col("price").std)

      one_call["price"].to_a.should eq(explicit["price"].to_a)
    end

    it "rejects a grouping key as an aggregate column" do
      expect_raises(Prism::DuplicateColumn, /grouping key/) do
        group_frame.group_by("region").sum("region")
      end
    end

    it "rejects an unknown column" do
      expect_raises(Prism::ColumnNotFound) do
        group_frame.group_by("region").sum("nope")
      end
    end

    it "raises when no column supports the aggregate" do
      frame = Prism.from_columns({
        k: Arrow::StringArray.from_values(["a"]),
        v: Arrow::StringArray.from_values(["x"]),
      })

      expect_raises(Prism::DtypeError, /sum/) { frame.group_by("k").sum }
    end
  end

  describe "vectorized aggregation" do
    # Counts evaluations, to prove the operand is reduced once for all groups.
    it "evaluates an aggregate operand once, sharing it between aggregates" do
      frame = Prism.from_columns({
        k: Arrow::Int64Array.from_values(Array.new(40) { |row| (row % 2).to_i64 }),
        v: Arrow::Int64Array.from_values(Array.new(40, &.to_i64)),
      })
      operand = CountedOperand.new(col("v"))

      CountedOperand.reset
      grouped = frame.group_by("k").aggregate(total: operand.sum, present: operand.count)

      # Two groups, but the operand is evaluated once for both.
      CountedOperand.evals.should eq(1)
      grouped["total"].to_a.should eq([380_i64, 400_i64])
      grouped["present"].to_a.should eq([20_u32, 20_u32])
    end

    it "sums narrow integers in the column dtype, wrapping like Arrow" do
      column = Arrow::PrimitiveArray(Int16)
        .from_values([30_000_i16, 30_000_i16, 30_000_i16, 7_i16])
      frame = Prism.from_columns({
        k: Arrow::StringArray.from_values(["a", "a", "a", "b"]),
        v: column,
      })

      grouped = frame.group_by("k").aggregate(total: col("v").sum)

      grouped.dtypes["total"].should eq(Arrow::DataType.int16)
      # 30_000 + 30_000 + 30_000 overflows Int16; the kernel wraps, and so must
      # the grouped fold.
      wrapped = Arrow.sum(column.slice(0, 3).as(Arrow::PrimitiveArray(Int16)))
      grouped["total"].to_a.map(&.as(Int16?)).should eq([wrapped, 7_i16])
    end

    it "takes float extremes in IEEE total order, like Arrow" do
      values = [0.0, -0.0, -1.5, Float64::NAN, 3.0]
      frame = Prism.from_columns({
        k: Arrow::StringArray.from_values(["a", "a", "a", "b", "b"]),
        v: Arrow::Float64Array.from_values(values),
      })

      grouped = frame.group_by("k").aggregate(low: col("v").min, high: col("v").max)

      # -0.0 sorts below 0.0, and NaN sorts above +inf, so it is never the min.
      grouped["low"].to_a.should eq([-1.5, 3.0])
      grouped["high"].to_a[0].as(Float64).should eq(0.0)
      grouped["high"].to_a[1].as(Float64).nan?.should be_true
    end

    it "keeps a null at the group edge for first and last" do
      batch = Arrow::RecordBatch.from_columns({
        k: Arrow::StringArray.from_values(["a", "a", "a"]),
        v: Arrow::Float64Array.from_nullable([nil, 2.0, nil]),
      })

      grouped = Prism.from_arrow(batch).group_by("k").aggregate(
        first: col("v").first,
        last: col("v").last,
      )

      grouped["first"].to_a.should eq([nil])
      grouped["last"].to_a.should eq([nil])
    end

    it "counts zero for a group whose values are all null" do
      frame = Prism.from_columns({
        k: Arrow::StringArray.from_values(["a", "a", "b"]),
        v: Arrow::Int64Array.from_nullable([nil, nil, 5_i64]),
      })

      grouped = frame.group_by("k").aggregate(
        present: col("v").count,
        rows: col("v").count_all,
        total: col("v").sum,
      )

      grouped["present"].to_a.should eq([0_u32, 1_u32])
      grouped["rows"].to_a.should eq([2_u32, 1_u32])
      grouped["total"].to_a.should eq([nil, 5_i64])
    end

    it "takes boolean extremes like Arrow's bool_and/bool_or" do
      frame = Prism.from_columns({
        k:    Arrow::StringArray.from_values(["a", "a", "b", "b", "c"]),
        flag: Arrow::BooleanArray.from_nullable([true, false, true, true, nil]),
      })

      grouped = frame.group_by("k").aggregate(low: col("flag").min, high: col("flag").max)

      grouped["low"].to_a.should eq([false, true, nil])
      grouped["high"].to_a.should eq([true, true, nil])
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
