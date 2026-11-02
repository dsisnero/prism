require "../spec_helper"

include Prism::DSL

# Nested columns materialize the way Polars converts them: a list becomes an
# array, a struct becomes a field-ordered hash, and a map becomes a hash of its
# entries. Nulls are `nil` at every level, so a null list is distinguishable from
# an empty one and from a list holding a null.

private def int_list : Arrow::ArrayBase
  Arrow::ListArray.from_iter_primitive(Int64, [
    [1_i64, 2_i64] of Int64?,
    nil,
    [] of Int64?,
    [3_i64, nil] of Int64?,
  ] of Array(Int64?)?).as(Arrow::ArrayBase)
end

private def fixed_int_list : Arrow::ArrayBase
  Arrow::FixedSizeListArray.from_iter_primitive(Int64, [
    [1_i64, 2_i64] of Int64?,
    [3_i64, nil] of Int64?,
  ] of Array(Int64?)?, 2).as(Arrow::ArrayBase)
end

private def point_struct : Arrow::ArrayBase
  fields = Arrow::Fields.new([
    Arrow::Field.new("x", Arrow::DataType.int64, true),
    Arrow::Field.new("y", Arrow::DataType.utf8, true),
  ])
  arrays = [
    Arrow::Int64Array.from_nullable([1_i64, nil, 3_i64]).as(Arrow::ArrayBase),
    Arrow::StringArray.from_values(["a", "b", "c"]).as(Arrow::ArrayBase),
  ]

  Arrow::StructArray.new(
    fields, arrays, Arrow::NullBuffer.from_slice([true, true, false])
  ).as(Arrow::ArrayBase)
end

private def lookup_map : Arrow::ArrayBase
  entries = [] of Tuple(String, Int64?)
  first = [{"a", 1_i64.as(Int64?)}, {"b", 2_i64.as(Int64?)}] of Tuple(String, Int64?)
  third = [{"c", 3_i64.as(Int64?)}] of Tuple(String, Int64?)

  Arrow::MapArray.from_vec_of_maps(
    [first, nil, third] of Array(Tuple(String, Int64?))?, false
  ).as(Arrow::ArrayBase)
end

describe "nested values" do
  it "materializes a list recursively, keeping nulls at both levels" do
    Prism::Values.extract(int_list).should eq([
      [1_i64.as(Prism::Value), 2_i64.as(Prism::Value)].as(Prism::Value),
      nil.as(Prism::Value),
      ([] of Prism::Value).as(Prism::Value),
      [3_i64.as(Prism::Value), nil.as(Prism::Value)].as(Prism::Value),
    ])
  end

  it "materializes a fixed-size list like a list" do
    Prism::Values.extract(fixed_int_list).should eq([
      [1_i64.as(Prism::Value), 2_i64.as(Prism::Value)].as(Prism::Value),
      [3_i64.as(Prism::Value), nil.as(Prism::Value)].as(Prism::Value),
    ])
  end

  it "materializes a struct as a field-ordered hash" do
    values = Prism::Values.extract(point_struct)

    values[0].as(Hash(String, Prism::Value)).keys.should eq(["x", "y"])
    values[0].should eq({"x" => 1_i64, "y" => "a"} of String => Prism::Value)
    values[1].should eq({"x" => nil, "y" => "b"} of String => Prism::Value)
    values[2].should be_nil
  end

  it "materializes a map as a hash of entries" do
    values = Prism::Values.extract(lookup_map)

    values[0].should eq({"a" => 1_i64, "b" => 2_i64} of Prism::Value => Prism::Value)
    values[1].should be_nil
    values[2].should eq({"c" => 3_i64} of Prism::Value => Prism::Value)
  end

  it "exposes nested values through a series and the table" do
    frame = Prism.from_columns({items: int_list})

    frame["items"].to_a[0].should eq(
      [1_i64.as(Prism::Value), 2_i64.as(Prism::Value)].as(Prism::Value)
    )
    frame.to_s.should contain("[1, 2]")
    frame.to_s.should contain("[]")
  end

  it "renders a struct's fields with names and quotes" do
    frame = Prism.from_columns({point: point_struct})

    frame.to_s.should contain("x: 1, y: \"a\"")
  end

  it "keys a nested column by its row encoding for distinct and n_unique" do
    frame = Prism.from_columns({items: int_list})

    # [1,2], null, [], [3,null] are four distinct keys.
    frame["items"].n_unique.should eq(4)
    frame.distinct("items").row_count.should eq(4)

    repeated = Prism.from_columns({
      items: Arrow::ListArray.from_iter_primitive(Int64, [
        [1_i64, 2_i64] of Int64?,
        [1_i64, 2_i64] of Int64?,
        [3_i64] of Int64?,
      ] of Array(Int64?)?).as(Arrow::ArrayBase),
      tag: Arrow::StringArray.from_values(["a", "b", "c"]).as(Arrow::ArrayBase),
    })

    kept = repeated.distinct("items")
    kept.row_count.should eq(2)
    kept["tag"].to_a.should eq(["a", "c"])
  end

  it "rejects arithmetic on a nested column" do
    frame = Prism.from_columns({items: int_list})

    expect_raises(Prism::DtypeError) do
      (col("items") + lit(1)).dtype(frame.schema)
    end
  end
end
