require "../spec_helper"

include Prism::DSL

# One value the target cannot represent, so the two cast modes can be compared on
# the same column: `strict` raises, `strict: false` nulls it (Polars' `strict=False`).
private def dirty_frame : Prism::DataFrame
  Prism.from_columns({
    nn:    Arrow::PrimitiveArray(Int64).from_values([1_i64, 300_i64, 3_i64]).as(Arrow::ArrayBase),
    dirty: Arrow::StringArray.from_nullable(["1", "abc", nil]).as(Arrow::ArrayBase),
  })
end

describe "cast(strict: false)" do
  it "nulls a value that does not fit instead of raising" do
    casted = dirty_frame.cast({"dirty" => Arrow::DataType.int64}, strict: false)

    casted["dirty"].to_a.should eq([1_i64, nil, nil])
  end

  it "is still the default that raises" do
    expect_raises(Prism::DtypeError, /cannot cast column "dirty"/) do
      dirty_frame.cast({"dirty" => Arrow::DataType.int64})
    end
  end

  it "nulls a narrowing overflow" do
    casted = dirty_frame.cast({"nn" => Arrow::DataType.int8}, strict: false)

    casted["nn"].to_a.should eq([1_i8, nil, 3_i8])
  end

  it "declares the column nullable, because it can now invent nulls" do
    casted = dirty_frame.cast({"nn" => Arrow::DataType.int8}, strict: false)

    casted.schema.field_with_name("nn").nullable?.should be_true
    casted["nn"].null_count.should eq(1)
  end

  it "declares nullable even when every value fits" do
    # The claim is about what the cast *can* produce, not what this chunk happens
    # to contain.
    casted = dirty_frame.cast({"nn" => Arrow::DataType.float64}, strict: false)

    casted.schema.field_with_name("nn").nullable?.should be_true
    casted["nn"].null_count.should eq(0)
  end

  it "works frame-wide on a mixed frame" do
    casted = dirty_frame.cast(Arrow::DataType.float64, strict: false)

    casted.dtypes.values.uniq.should eq([Arrow::DataType.float64])
    casted["dirty"].to_a.should eq([1.0, nil, nil])
  end

  it "is on the expression and the plan too" do
    expr = col("dirty").cast(Arrow::DataType.int64, strict: false)
    expr.nullable?(dirty_frame.schema).should be_true

    eager = dirty_frame.with_column("n", expr)
    lazy = dirty_frame.lazy.with_column("n", expr).collect

    eager["n"].to_a.should eq([1_i64, nil, nil])
    lazy.schema.should eq(eager.schema)
    lazy.should eq(eager)
  end

  it "is on Series" do
    dirty_frame["dirty"].cast(Arrow::DataType.int64, strict: false)
      .to_a.should eq([1_i64, nil, nil])
  end

  it "is on the lazy verb, both forms" do
    named = dirty_frame.lazy.cast({"dirty" => Arrow::DataType.int64}, strict: false).collect
    named["dirty"].to_a.should eq([1_i64, nil, nil])

    everything = dirty_frame.lazy.cast(Arrow::DataType.float64, strict: false).collect
    everything.dtypes.values.uniq.should eq([Arrow::DataType.float64])
  end

  it "keeps a strict cast's field non-null, so the two modes differ in schema" do
    strict = dirty_frame.cast({"nn" => Arrow::DataType.float64})
    loose = dirty_frame.cast({"nn" => Arrow::DataType.float64}, strict: false)

    strict.schema.field_with_name("nn").nullable?.should be_false
    loose.schema.field_with_name("nn").nullable?.should be_true
  end

  it "is a different expression from the strict one, so CSE cannot merge them" do
    strict = col("dirty").cast(Arrow::DataType.int64)
    loose = col("dirty").cast(Arrow::DataType.int64, strict: false)

    strict.same_as?(loose).should be_false
    strict.key.should_not eq(loose.key)
  end
end
