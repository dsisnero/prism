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

    it "canonicalizes floats before keying, like Polars: one NaN, one zero" do
      zeros = Prism::Series.new(
        "z",
        [Arrow::Float64Array.from_values([0.0, -0.0, Float64::NAN, Float64::NAN])] of Arrow::ArrayBase,
        Arrow::DataType.float64
      )

      # `-0.0 == 0.0`, and every NaN is one key, so two distinct values remain.
      zeros.unique.to_a.size.should eq(2)
      zeros.n_unique.should eq(2)
    end

    it "keeps one NaN key even across payloads, unlike Crystal's ==" do
      signaling_nan = 0x7FF0_0000_0000_0001_u64.unsafe_as(Float64)
      series = Prism::Series.new(
        "f",
        [Arrow::Float64Array.from_values(
          [Float64::NAN, signaling_nan, 1.0, 0.0, -0.0]
        )] of Arrow::ArrayBase,
        Arrow::DataType.float64
      )

      # NaN → canonical quiet NaN, -0.0 → 0.0: {NaN, 1.0, 0.0}.
      series.unique.size.should eq(3)
      series.n_unique.should eq(3)
    end

    it "deduplicates dictionary-encoded values by value, not by key index" do
      array = Arrow::DictionaryArray(Int32)
        .from_values(["west", "east", "west"])
        .as(Arrow::ArrayBase)
      series = Prism::Series.new("d", [array] of Arrow::ArrayBase, array.data_type)

      series.unique.to_a.should eq(["west", "east"])
      series.n_unique.should eq(2)
    end

    it "canonicalizes dictionary-encoded floats through their values" do
      # The keys are distinct, so keying by key index would keep four values;
      # the values are `0.0`, `-0.0` and two NaNs, which are two keys.
      array = Arrow::DictionaryArray(Int32).new(
        Arrow::PrimitiveArray(Int32).from_values([0, 1, 2, 3]),
        Arrow::Float64Array.from_values([0.0, -0.0, Float64::NAN, Float64::NAN]),
      ).as(Arrow::ArrayBase)
      series = Prism::Series.new("d", [array] of Arrow::ArrayBase, array.data_type)

      series.unique.size.should eq(2)
      series.n_unique.should eq(2)
      # Canonicalizing the dictionary values must not change the dtype.
      series.unique.dtype.should eq(array.data_type)
    end

    it "canonicalizes float32 keys as well" do
      series = Prism::Series.new(
        "f",
        [Arrow::Float32Array.from_values(
          [0.0_f32, -0.0_f32, Float32::NAN, Float32::NAN]
        )] of Arrow::ArrayBase,
        Arrow::DataType.float32
      )

      series.unique.size.should eq(2)
    end

    it "canonicalizes float16 keys as well" do
      array = Arrow::Float16Array.from_values([
        Arrow::Float16.from_f32(0.0_f32),
        Arrow::Float16.from_f32(-0.0_f32),
        Arrow::Float16::NAN,
      ])
      series = Prism::Series.new("f", [array.as(Arrow::ArrayBase)], array.data_type)

      series.unique.size.should eq(2)
    end

    it "keeps nulls as one key while canonicalizing floats" do
      series = Prism::Series.new(
        "f",
        [Arrow::Float64Array.from_nullable([0.0, -0.0, nil])] of Arrow::ArrayBase,
        Arrow::DataType.float64
      )

      series.unique.to_a.should eq([0.0, nil])
    end
  end

  describe "#n_unique" do
    it "counts distinct values, with null as one value like Polars" do
      # tag is [b, a, b, nil, a, nil]: two names plus the null.
      distinct_frame["tag"].n_unique.should eq(3)
      distinct_frame["value"].n_unique.should eq(6)
    end

    it "returns one for an all-null column" do
      series = Prism::Series.new(
        "x",
        [Arrow::Float64Array.from_nullable([nil, nil])] of Arrow::ArrayBase,
        Arrow::DataType.float64
      )

      series.n_unique.should eq(1)
    end

    it "agrees with the aggregate used inside group_by" do
      frame = distinct_frame
      grouped = frame.group_by("value").aggregate(distinct_tags: col("tag").n_unique)

      # One row per value, so each group holds a single tag — including a null
      # tag, which counts as one value.
      grouped["distinct_tags"].to_a.should eq([1_u32, 1_u32, 1_u32, 1_u32, 1_u32, 1_u32])
    end

    it "equals the length of #unique, which is Polars' contract" do
      frame = Prism.from_columns({
        f: Arrow::Float64Array.from_nullable([0.0, -0.0, Float64::NAN, nil]),
        s: Arrow::StringArray.from_nullable(["a", "a", nil, "b"]),
        i: Arrow::Int64Array.from_nullable([1_i64, 1_i64, nil, 2_i64]),
      })

      frame.column_names.each do |name|
        column = frame[name]
        column.n_unique.should eq(column.unique.size)
      end
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

    it "treats a Null column as one value and an empty column as none" do
      Prism::Aggregates.count_distinct(Arrow::NullArray.new(3)).should eq(1)
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

# Floats nested inside a composite key must key by `==`, like top-level floats,
# or two logically equal keys stay apart (plan Phase 17).
describe "nested key canonicalization" do
  it "canonicalizes a float inside a struct key" do
    structs = Arrow::StructArray.from_pairs([
      {Arrow::Field.new("x", Arrow::DataType.float64, true),
       Arrow::Float64Array.from_values(
         [Float64::NAN, Float64::NAN, 0.0, -0.0, 1.0]
       ).as(Arrow::ArrayBase)},
    ])
    series = Prism::Series.new("s", [structs.as(Arrow::ArrayBase)], structs.data_type)

    # {NaN}, {NaN} -> one; {0.0}, {-0.0} -> one; {1.0} -> one.
    series.n_unique.should eq(3)
    series.unique.size.should eq(3)
    # Canonicalizing children must not change the struct's own dtype.
    series.unique.dtype.should eq(structs.data_type)
  end

  it "canonicalizes a float inside a list key" do
    lists = Arrow::ListArray.from_iter_primitive(Float64, [
      [Float64::NAN],
      [Float64::NAN],
      [0.0],
      [-0.0],
      [1.0],
    ])
    series = Prism::Series.new("l", [lists.as(Arrow::ArrayBase)], lists.data_type)

    series.n_unique.should eq(3)
    series.unique.size.should eq(3)
  end

  it "canonicalizes a float inside a fixed-size list key" do
    values = Arrow::Float64Array.from_values([Float64::NAN, Float64::NAN, 0.0, -0.0])
    fixed = Arrow::FixedSizeListArray.new(
      Arrow::Field.new("item", Arrow::DataType.float64, true),
      1,
      values,
    )
    series = Prism::Series.new("f", [fixed.as(Arrow::ArrayBase)], fixed.data_type)

    series.n_unique.should eq(2)
  end

  it "canonicalizes a float inside a map key" do
    map = Arrow::MapArray.new_from_strings(
      ["a", "a", "a", "a"],
      Arrow::Float64Array.from_values(
        [Float64::NAN, Float64::NAN, 0.0, -0.0]
      ).as(Arrow::ArrayBase),
      [0_u32, 1_u32, 2_u32, 3_u32, 4_u32],
    )
    series = Prism::Series.new("m", [map.as(Arrow::ArrayBase)], map.data_type)

    # {a: NaN} twice and {a: 0.0} / {a: -0.0} give two keys.
    series.n_unique.should eq(2)
  end

  it "canonicalizes a float inside a list-view key" do
    values = Arrow::Float64Array.from_values([0.0, -0.0, Float64::NAN, Float64::NAN])
    field = Arrow::Field.new("item", Arrow::DataType.float64, true)
    view = Arrow::ListViewArray.new(
      field,
      Arrow::ScalarBuffer(Int32).from([0, 1, 2, 3]),
      Arrow::ScalarBuffer(Int32).from([1, 1, 1, 1]),
      values.as(Arrow::ArrayBase),
    )
    series = Prism::Series.new("v", [view.as(Arrow::ArrayBase)], view.data_type)

    series.n_unique.should eq(2)
    series.unique.dtype.should eq(view.data_type)
  end

  it "canonicalizes a float inside a large list-view key" do
    values = Arrow::Float64Array.from_values([0.0, -0.0])
    field = Arrow::Field.new("item", Arrow::DataType.float64, true)
    view = Arrow::LargeListViewArray.new(
      field,
      Arrow::ScalarBuffer(Int64).from([0_i64, 1_i64]),
      Arrow::ScalarBuffer(Int64).from([1_i64, 1_i64]),
      values.as(Arrow::ArrayBase),
    )
    series = Prism::Series.new("v", [view.as(Arrow::ArrayBase)], view.data_type)

    series.n_unique.should eq(1)
  end

  it "leaves a nested key with no float children untouched" do
    structs = Arrow::StructArray.from_pairs([
      {Arrow::Field.new("n", Arrow::DataType.int64, true),
       Arrow::Int64Array.from_values([1_i64, 1_i64, 2_i64]).as(Arrow::ArrayBase)},
    ])
    series = Prism::Series.new("s", [structs.as(Arrow::ArrayBase)], structs.data_type)

    series.n_unique.should eq(2)
    # Nothing changed, so the canonical array is the same object.
    Prism::RowKeys.canonical(structs.as(Arrow::ArrayBase)).same?(structs).should be_true
  end
end
