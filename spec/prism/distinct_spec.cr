require "../spec_helper"

include Prism::DSL

private def distinct_frame_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    tag:   Arrow::StringArray.from_nullable(["b", "a", "b", nil, "a", nil]),
    value: Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64, 5_i64, 6_i64]),
  })
end

private def distinct_frame : Prism::DataFrame
  Prism.from_arrow(distinct_frame_batch)
end

describe Prism::Series do
  describe "#unique" do
    it "keeps first occurrences in original order" do
      unique = distinct_frame["tag"].unique

      unique.to_a.should eq(["b", "a", nil])
      unique.name.should eq("tag")
      unique.dtype.should eq(Arrow::DataType.utf8)
    end

    it "deduplicates across chunks" do
      frame = Prism.from_batches([
        distinct_frame_batch.slice(0, 3),
        distinct_frame_batch.slice(3, 3),
      ])

      frame["tag"].unique.to_a.should eq(["b", "a", nil])
    end

    it "returns the same series when there is nothing to deduplicate" do
      series = Prism.from_columns({x: Arrow::Int64Array.from_values([1_i64])})["x"]

      series.unique.should be(series)
    end

    it "distinguishes signed zero and NaN payloads, like Arrow's total order" do
      zeros = Prism::Series.new(
        "z",
        [Arrow::Float64Array.from_values([0.0, -0.0, Float64::NAN, Float64::NAN])] of Arrow::ArrayBase,
        Arrow::DataType.float64
      )

      zeros.unique.size.should eq(3)
    end

    it "keeps one NaN key, unlike Crystal's ==" do
      series = Prism::Series.new(
        "f",
        [Arrow::Float64Array.from_values(
          [Float64::NAN, Float64::NAN, 1.0, 0.0, -0.0]
        )] of Arrow::ArrayBase,
        Arrow::DataType.float64
      )

      # NaN encodes to one canonical key; 0.0 and -0.0 stay apart.
      series.unique.size.should eq(4)
      series.n_unique.should eq(4)
    end

    it "deduplicates dictionary-encoded values by value, not by key index" do
      array = Arrow::DictionaryArray(Int32)
        .from_values(["west", "east", "west"])
        .as(Arrow::ArrayBase)
      series = Prism::Series.new("d", [array] of Arrow::ArrayBase, array.data_type)

      series.unique.to_a.should eq(["west", "east"])
      series.n_unique.should eq(2)
    end
  end

  describe "#n_unique" do
    it "counts distinct non-null values" do
      distinct_frame["tag"].n_unique.should eq(2)
      distinct_frame["value"].n_unique.should eq(6)
    end

    it "returns zero for an all-null column" do
      series = Prism::Series.new(
        "x",
        [Arrow::Float64Array.from_nullable([nil, nil])] of Arrow::ArrayBase,
        Arrow::DataType.float64
      )

      series.n_unique.should eq(0)
    end

    it "agrees with the aggregate used inside group_by" do
      frame = distinct_frame
      grouped = frame.group_by("value").aggregate(distinct_tags: col("tag").n_unique)

      # One row per value, so each group holds a single tag. Groups whose only
      # row has a null tag count zero, because n_unique skips nulls.
      grouped["distinct_tags"].to_a.should eq([1_u32, 1_u32, 1_u32, 0_u32, 1_u32, 0_u32])
    end
  end
end

describe Prism::Aggregates do
  describe ".count_distinct" do
    it "matches Series#n_unique" do
      array = distinct_frame_batch["tag"]

      Prism::Aggregates.count_distinct(array)
        .should eq(distinct_frame["tag"].n_unique)
    end

    it "counts nulls out and handles empty input" do
      Prism::Aggregates.count_distinct(Arrow::NullArray.new(3)).should eq(0)
      Prism::Aggregates.count_distinct(
        Arrow::StringArray.from_values([] of String)
      ).should eq(0)
    end
  end
end

describe "DataFrame#distinct" do
  it "keeps the first occurrence of each key combination" do
    unique = distinct_frame.distinct("tag")

    unique["tag"].to_a.should eq(["b", "a", nil])
    unique["value"].to_a.should eq([1_i64, 2_i64, 4_i64])
  end

  it "treats a null in any key column as part of the key" do
    frame = Prism.from_columns({
      a: Arrow::Int64Array.from_nullable([1_i64, 1_i64, 1_i64, nil, nil]),
      b: Arrow::StringArray.from_nullable([nil, nil, "x", "y", "y"]),
    })

    unique = frame.distinct

    # (1, null) twice, (1, "x"), (null, "y") twice.
    unique.row_count.should eq(3)
    unique["b"].to_a.should eq([nil, "x", "y"])
  end

  it "returns the same frame when every row is already distinct" do
    frame = Prism.from_columns({x: Arrow::Int64Array.from_values([1_i64, 2_i64])})

    frame.distinct.should be(frame)
  end
end
