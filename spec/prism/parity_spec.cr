require "../spec_helper"

include Prism::DSL

private def parity_frame : Prism::DataFrame
  SpecFrames.sales
end

describe "lazy verb parity" do
  it "drops columns" do
    lazy = Prism.scan(parity_frame).drop("price")
    eager = parity_frame.drop("price")

    lazy.schema.should eq(eager.schema)
    lazy.collect.should eq(eager)
  end

  it "renames columns" do
    lazy = Prism.scan(parity_frame).rename({"price" => "cost", :qty => "quantity"})
    eager = parity_frame.rename({"price" => "cost", :qty => "quantity"})

    lazy.schema.should eq(eager.schema)
    lazy.collect.should eq(eager)
  end

  it "casts a named column" do
    lazy = Prism.scan(parity_frame).cast({"qty" => Arrow::DataType.float64})

    lazy.schema.fields.find_field("qty").not_nil!.data_type
      .should eq(Arrow::DataType.float64)
    lazy.collect["qty"].to_a.should eq([1.0, 2.0, 3.0, 4.0, 5.0])
  end

  it "fills nulls" do
    lazy = Prism.scan(parity_frame).fill_null(0)
    eager = parity_frame.fill_null(0)

    lazy.collect["price"].to_a.should eq(eager["price"].to_a)
  end

  it "drops null rows" do
    lazy = Prism.scan(parity_frame).drop_nulls("price")
    eager = parity_frame.drop_nulls("price")

    lazy.collect.row_count.should eq(eager.row_count)
    lazy.collect["qty"].to_a.should eq(eager["qty"].to_a)
  end

  it "adds several columns" do
    lazy = Prism.scan(parity_frame).with_columns(
      doubled: col("qty") * lit(2),
      flagged: col("qty") > lit(2),
    )

    lazy.collect["doubled"].to_a.should eq([2_i64, 4_i64, 6_i64, 8_i64, 10_i64])
    lazy.collect["flagged"].to_a.should eq([false, false, true, true, true])
  end

  it "takes the last rows" do
    lazy = Prism.scan(parity_frame).tail(2)
    eager = parity_frame.tail(2)

    lazy.collect["qty"].to_a.should eq(eager["qty"].to_a)
  end

  it "slices by offset and count" do
    lazy = Prism.scan(parity_frame).slice(1, 2)
    eager = parity_frame.slice(1, 2)

    lazy.collect["qty"].to_a.should eq(eager["qty"].to_a)
  end

  it "rejects an unknown column" do
    expect_raises(Prism::ColumnNotFound, /nope/) do
      Prism.scan(parity_frame).drop("nope")
    end
  end
end

private def price_series : Prism::Series
  SpecFrames.sales["price"]
end

describe "Series element-wise verbs" do
  it "reports nulls" do
    nulls = price_series.is_null

    nulls.dtype.should eq(Arrow::DataType.boolean)
    nulls.to_a.should eq([false, false, false, false, true])
  end

  it "fills nulls without changing the dtype" do
    filled = price_series.fill_null(0)

    filled.dtype.should eq(Arrow::DataType.float64)
    filled.to_a.should eq([10.0, 20.0, 30.0, 40.0, 0.0])
  end

  it "casts to another dtype" do
    casted = price_series.cast(Arrow::DataType.float32)

    casted.dtype.should eq(Arrow::DataType.float32)
    casted.to_a.should eq([10.0_f32, 20.0_f32, 30.0_f32, 40.0_f32, nil])
  end

  it "compares element-wise, propagating nulls" do
    compared = price_series == price_series

    compared.dtype.should eq(Arrow::DataType.boolean)
    compared.to_a.should eq([true, true, true, true, nil])
  end

  it "compares unequal values to false" do
    doubled = price_series.cast(Arrow::DataType.float64)

    (price_series == doubled.cast(Arrow::DataType.float64)).to_a
      .should eq([true, true, true, true, nil])
  end

  it "rejects comparing different dtypes" do
    other = SpecFrames.sales["qty"]

    expect_raises(Prism::DtypeError, /dtype/) { price_series == other }
  end

  it "rejects comparing different lengths" do
    short = Prism.from_columns({x: Arrow::Float64Array.from_values([1.0])})["x"]

    expect_raises(ArgumentError) { price_series == short }
  end
end
