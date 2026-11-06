require "../spec_helper"

include Prism::DSL

# `std`/`var` delegate to `Arrow.stddev`/`variance`, which is a one-pass Welford
# kernel; these pin the contract the four-pass version had, so the delegation is a
# measured refactor rather than a semantic change.
private def floats(*values : Float64?) : Arrow::ArrayBase
  Arrow::PrimitiveArray(Float64).from_nullable(values.to_a).as(Arrow::ArrayBase)
end

private def close(actual : Float64?, expected : Float64, tolerance : Float64 = 1e-12) : Nil
  actual.should_not be_nil
  (actual.not_nil! - expected).abs.should be <= tolerance
end

describe Prism::Aggregates do
  describe ".variance and .standard_deviation" do
    it "computes the sample statistics, ddof 1 by default" do
      values = floats(1.0, 2.0, 3.0, 4.0)

      close(Prism::Aggregates.variance(values), 5.0 / 3.0)
      close(Prism::Aggregates.standard_deviation(values), Math.sqrt(5.0 / 3.0))
    end

    it "computes the population statistics with ddof 0" do
      values = floats(1.0, 2.0, 3.0, 4.0)

      close(Prism::Aggregates.variance(values, ddof: 0), 1.25)
      close(Prism::Aggregates.standard_deviation(values, ddof: 0), Math.sqrt(1.25))
    end

    it "skips nulls without changing the count" do
      values = floats(1.0, nil, 3.0)

      close(Prism::Aggregates.variance(values), 2.0)
      close(Prism::Aggregates.standard_deviation(values), Math.sqrt(2.0))
    end

    it "is nil when there are not enough values" do
      Prism::Aggregates.variance(floats(5.0)).should be_nil
      Prism::Aggregates.standard_deviation(floats(5.0)).should be_nil
      Prism::Aggregates.standard_deviation(floats(nil, nil)).should be_nil
      Prism::Aggregates.standard_deviation(Arrow::PrimitiveArray(Float64).from_values([] of Float64).as(Arrow::ArrayBase)).should be_nil
    end

    it "is zero for one value with ddof 0" do
      close(Prism::Aggregates.variance(floats(5.0), ddof: 0), 0.0)
      close(Prism::Aggregates.standard_deviation(floats(5.0), ddof: 0), 0.0)
    end

    it "widens narrower dtypes to Float64" do
      ints = Arrow::PrimitiveArray(Int32).from_values([1_i32, 3_i32]).as(Arrow::ArrayBase)
      halves = Arrow::PrimitiveArray(Arrow::Float16)
        .from_values([Arrow::Float16.from_f32(1.0_f32), Arrow::Float16.from_f32(3.0_f32)])
        .as(Arrow::ArrayBase)
      booleans = Arrow::BooleanArray.from_values([true, false]).as(Arrow::ArrayBase)

      close(Prism::Aggregates.variance(ints), 2.0)
      close(Prism::Aggregates.variance(halves), 2.0)
      close(Prism::Aggregates.variance(booleans), 0.5)
    end

    it "reads decimals at their scale" do
      decimals = Arrow::PrimitiveArray(Int128)
        .from_values([100_i128, 300_i128])
        .with_data_type(Arrow::DataType.decimal128(10, 2))
        .as(Arrow::ArrayBase)

      close(Prism::Aggregates.variance(decimals), 2.0)
      close(Prism::Aggregates.standard_deviation(decimals), Math.sqrt(2.0))
    end

    it "matches a two-pass reference over random data" do
      rng = Random.new(17)

      20.times do
        values = Array(Float64?).new(50) { rng.rand(4).zero? ? nil : rng.next_float * 100 }
        array = Arrow::PrimitiveArray(Float64).from_nullable(values).as(Arrow::ArrayBase)
        present = values.compact
        mean = present.sum / present.size
        squared = present.sum { |value| (value - mean) ** 2 }

        close(Prism::Aggregates.variance(array), squared / (present.size - 1), 1e-9)
        close(Prism::Aggregates.standard_deviation(array, ddof: 0), Math.sqrt(squared / present.size), 1e-9)
      end
    end

    it "is the same through a grouped aggregation" do
      frame = Prism.from_columns({
        g: Arrow::StringArray.from_values(["a", "a", "a", "b", "b"]).as(Arrow::ArrayBase),
        v: floats(1.0, 2.0, 3.0, 10.0, 14.0),
      })

      grouped = frame.group_by("g").aggregate(s: col("v").std, var: col("v").variance)
      by_group = grouped.sort_by("g")

      by_group["s"].to_a.first.as(Float64).should be_close(Math.sqrt(1.0), 1e-12)
      by_group["var"].to_a.first.as(Float64).should be_close(1.0, 1e-12)
      by_group["s"].to_a.last.as(Float64).should be_close(Math.sqrt(8.0), 1e-12)
      by_group["var"].to_a.last.as(Float64).should be_close(8.0, 1e-12)
    end
  end

  describe ".mean" do
    it "stays sum/count, which is faster than the kernel" do
      Prism.from_columns({v: floats(1.0, 2.0, nil, 3.0)})["v"].mean.should eq(2.0)
    end
  end
end
