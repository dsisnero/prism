require "../spec_helper"

private def numeric_series : Prism::Series
  Prism::Series.new(
    "price",
    [Arrow::Float64Array.from_nullable([10.0, 20.0, nil, 30.0])],
    Arrow::DataType.float64
  )
end

private def chunked_series : Prism::Series
  Prism::Series.new(
    "qty",
    [
      Arrow::Int64Array.from_values([1_i64, 2_i64]),
      Arrow::Int64Array.from_values([3_i64, 4_i64]),
    ],
    Arrow::DataType.int64
  )
end

describe Prism::Series do
  describe "metadata" do
    it "exposes name, dtype, size, count and null_count" do
      series = numeric_series

      series.name.should eq("price")
      series.dtype.should eq(Arrow::DataType.float64)
      series.size.should eq(4)
      series.count.should eq(3)
      series.null_count.should eq(1)
    end

    it "aggregates size and count across chunks" do
      series = chunked_series

      series.size.should eq(4)
      series.count.should eq(4)
      series.null_count.should eq(0)
    end
  end

  describe "#to_a" do
    it "returns Crystal values with nulls as nil" do
      numeric_series.to_a.should eq([10.0, 20.0, nil, 30.0])
    end

    it "concatenates chunks in order" do
      chunked_series.to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64])
    end
  end

  describe "#[] and #[]?" do
    it "addresses values across chunks" do
      series = chunked_series

      series[0].should eq(1_i64)
      series[1].should eq(2_i64)
      series[2].should eq(3_i64)
      series[3].should eq(4_i64)
    end

    it "returns nil for a null value and for an out-of-range index" do
      series = numeric_series

      series[2].should be_nil
      series[4]?.should be_nil
      series[-1]?.should be_nil
    end

    it "raises IndexError for an out-of-range #[]" do
      expect_raises(IndexError) { numeric_series[9] }
    end
  end

  describe "#to_arrow" do
    it "returns the single chunk unchanged" do
      chunk = Arrow::Float64Array.from_values([1.0])
      series = Prism::Series.new("x", [chunk] of Arrow::ArrayBase, Arrow::DataType.float64)

      series.to_arrow.same?(chunk).should be_true
    end

    it "concatenates multiple chunks" do
      array = chunked_series.to_arrow.as(Arrow::Int64Array)

      array.to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64])
    end

    it "raises for a series with no chunks" do
      series = Prism::Series.new("x", [] of Arrow::ArrayBase, Arrow::DataType.float64)

      expect_raises(Prism::UnsupportedOperation, /to_a/) { series.to_arrow }
    end
  end

  describe "#each_chunk" do
    it "yields the underlying chunks without combining them" do
      widths = [] of Int32
      chunked_series.each_chunk { |chunk| widths << chunk.len }

      widths.should eq([2, 2])
    end
  end

  describe "aggregates" do
    it "sums across chunks, skipping nulls" do
      numeric_series.sum.should eq(60.0)
      chunked_series.sum.should eq(10_i64)
    end

    it "reduces per chunk instead of rechunking" do
      # A series with no chunks cannot aggregate if the aggregate went through
      # #to_arrow, which raises for this shape. Nil results prove it does not.
      empty = Prism::Series.new("x", [] of Arrow::ArrayBase, Arrow::DataType.int64)

      empty.sum.should be_nil
      empty.min.should be_nil
      empty.max.should be_nil
      empty.mean.should be_nil
    end

    it "matches aggregating the concatenated column" do
      chunked = chunked_series
      combined = Prism::Series.new(
        "qty",
        [chunked.to_arrow] of Arrow::ArrayBase,
        Arrow::DataType.int64
      )

      chunked.sum.should eq(combined.sum)
      chunked.min.should eq(combined.min)
      chunked.max.should eq(combined.max)
      chunked.mean.should eq(combined.mean)
    end

    it "reduces unsigned and half-precision columns" do
      unsigned = Prism::Series.new(
        "u8",
        [Arrow::PrimitiveArray(UInt8).from_values([1_u8, 2_u8]),
         Arrow::PrimitiveArray(UInt8).from_values([3_u8])] of Arrow::ArrayBase,
        Arrow::DataType.uint8
      )
      unsigned.sum.should eq(6_u8)
      unsigned.min.should eq(1_u8)
      unsigned.max.should eq(3_u8)

      half = Prism::Series.new(
        "f16",
        [Arrow::PrimitiveArray(Arrow::Float16).from_values([
          Arrow::Float16.from_f32(1.5_f32),
          Arrow::Float16.from_f32(2.5_f32),
        ])] of Arrow::ArrayBase,
        Arrow::DataType.float16
      )
      half.max.as(Arrow::Float16).to_f32.should eq(2.5_f32)
    end

    it "averages, skipping nulls" do
      numeric_series.mean.should eq(20.0)
    end

    it "returns nil from mean when every value is null" do
      series = Prism::Series.new(
        "x",
        [Arrow::Float64Array.from_nullable([nil, nil])],
        Arrow::DataType.float64
      )

      series.mean.should be_nil
    end

    it "computes min and max for numbers" do
      numeric_series.min.should eq(10.0)
      numeric_series.max.should eq(30.0)
      chunked_series.min.should eq(1_i64)
      chunked_series.max.should eq(4_i64)
    end

    it "computes min and max for strings" do
      series = Prism::Series.new(
        "region",
        [Arrow::StringArray.from_nullable(["east", nil, "west"])],
        Arrow::DataType.utf8
      )

      series.min.should eq("east")
      series.max.should eq("west")
    end

    it "computes min and max for booleans" do
      series = Prism::Series.new(
        "flag",
        [Arrow::BooleanArray.from_nullable([true, false])],
        Arrow::DataType.boolean
      )

      series.min.should eq(false)
      series.max.should eq(true)
    end

    it "rejects sum on a non-numeric column" do
      series = Prism::Series.new(
        "region",
        [Arrow::StringArray.from_values(["east"])],
        Arrow::DataType.utf8
      )

      expect_raises(Prism::DtypeError, /sum/) { series.sum }
    end
  end

  describe "comparisons" do
    it "compares element-wise against a scalar of the same width" do
      series = numeric_series
      mask = series > 15

      mask.dtype.should eq(Arrow::DataType.boolean)
      mask.to_a.should eq([false, true, nil, true])
    end

    it "keeps every comparison operator" do
      series = numeric_series

      (series >= 20).to_a.should eq([false, true, nil, true])
      (series < 20).to_a.should eq([true, false, nil, false])
      (series <= 20).to_a.should eq([true, true, nil, false])
      (series == 20.0).to_a.should eq([false, true, nil, false])
      (series != 20.0).to_a.should eq([true, false, nil, true])
    end

    it "casts the literal to the column dtype when that is lossless" do
      # Int32 column, Int64 literal: the literal adopts Int32, no column copy.
      series = Prism::Series.new(
        "qty",
        [Arrow::Int32Array.from_values([1, 2, 3])],
        Arrow::DataType.int32
      )

      mask = series > 1
      mask.to_a.should eq([false, true, true])
      mask.to_arrow.as(Arrow::BooleanArray).len.should eq(3)
    end

    it "widens an integer column for a fractional literal instead of truncating" do
      series = Prism::Series.new(
        "qty",
        [Arrow::Int64Array.from_values([1_i64, 2_i64])],
        Arrow::DataType.int64
      )

      # 1.5 must not be truncated to 1: `1 > 1.5` is false, `2 > 1.5` true.
      (series > 1.5).to_a.should eq([false, true])
    end

    it "compares two series of the same dtype" do
      left = Prism::Series.new(
        "a", [Arrow::Int64Array.from_values([1_i64, 5_i64, 9_i64])], Arrow::DataType.int64
      )
      right = Prism::Series.new(
        "b", [Arrow::Int64Array.from_values([2_i64, 5_i64, 4_i64])], Arrow::DataType.int64
      )

      (left < right).to_a.should eq([true, false, false])
      (left == right).to_a.should eq([false, true, false])
    end

    it "compares chunk-by-chunk" do
      series = chunked_series

      mask = series > 2
      mask.chunks.size.should eq(2)
      mask.to_a.should eq([false, false, true, true])
    end

    it "rejects a series comparison with a different dtype or length" do
      other = Prism::Series.new(
        "x", [Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64])], Arrow::DataType.int64
      )

      expect_raises(Prism::DtypeError, /price/) { numeric_series > other }

      shorter = Prism::Series.new(
        "x", [Arrow::Int64Array.from_values([1_i64])], Arrow::DataType.int64
      )
      expect_raises(ArgumentError) { chunked_series > shorter }
    end

    it "rejects comparing a string column with a number" do
      series = Prism::Series.new(
        "region", [Arrow::StringArray.from_values(["east"])], Arrow::DataType.utf8
      )

      expect_raises(Prism::DtypeError) { series == 1 }
    end
  end

  describe "chunk invariants" do
    it "rejects chunks whose dtype does not match" do
      expect_raises(Prism::DtypeError) do
        Prism::Series.new(
          "x",
          [Arrow::Int64Array.from_values([1_i64]),
           Arrow::Float64Array.from_values([1.0]).as(Arrow::ArrayBase)] of Arrow::ArrayBase,
          Arrow::DataType.int64
        )
      end
    end
  end
end
