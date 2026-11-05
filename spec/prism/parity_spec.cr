require "../spec_helper"

include Prism::DSL

private def parity_frame : Prism::DataFrame
  SpecFrames.sales
end

describe "lazy verb parity" do
  it "drops columns" do
    lazy = parity_frame.lazy.drop("price")
    eager = parity_frame.drop("price")

    lazy.schema.should eq(eager.schema)
    lazy.collect.should eq(eager)
  end

  it "renames columns" do
    lazy = parity_frame.lazy.rename({"price" => "cost", :qty => "quantity"})
    eager = parity_frame.rename({"price" => "cost", :qty => "quantity"})

    lazy.schema.should eq(eager.schema)
    lazy.collect.should eq(eager)
  end

  it "casts a named column" do
    lazy = parity_frame.lazy.cast({"qty" => Arrow::DataType.float64})

    lazy.schema.fields.find_field("qty").not_nil!.data_type
      .should eq(Arrow::DataType.float64)
    lazy.collect["qty"].to_a.should eq([1.0, 2.0, 3.0, 4.0, 5.0])
  end

  it "fills nulls" do
    lazy = parity_frame.lazy.fill_null(0)
    eager = parity_frame.fill_null(0)

    lazy.collect["price"].to_a.should eq(eager["price"].to_a)
  end

  it "drops null rows" do
    lazy = parity_frame.lazy.drop_nulls("price")
    eager = parity_frame.drop_nulls("price")

    lazy.collect.row_count.should eq(eager.row_count)
    lazy.collect["qty"].to_a.should eq(eager["qty"].to_a)
  end

  it "adds several columns" do
    lazy = parity_frame.lazy.with_columns(
      doubled: col("qty") * lit(2),
      flagged: col("qty") > lit(2),
    )

    lazy.collect["doubled"].to_a.should eq([2_i64, 4_i64, 6_i64, 8_i64, 10_i64])
    lazy.collect["flagged"].to_a.should eq([false, false, true, true, true])
  end

  it "takes the last rows" do
    lazy = parity_frame.lazy.tail(2)
    eager = parity_frame.tail(2)

    lazy.collect["qty"].to_a.should eq(eager["qty"].to_a)
  end

  it "slices by offset and count" do
    lazy = parity_frame.lazy.slice(1, 2)
    eager = parity_frame.slice(1, 2)

    lazy.collect["qty"].to_a.should eq(eager["qty"].to_a)
  end

  it "rejects an unknown column" do
    expect_raises(Prism::ColumnNotFound, /nope/) do
      parity_frame.lazy.drop("nope")
    end
  end

  # The eager verbs accept every one of these call shapes; the lazy ones used to
  # require an argument because a Crystal typed splat cannot be empty, so
  # `df.lazy.distinct` was a compile error while `df.distinct` was not. The table
  # is the guard: both spellings must compile *and* produce the same frame.
  describe "call shapes" do
    it "answers every shape the eager verb answers" do
      frame = parity_frame
      # `cast` without a column list needs every column to fit the target.
      numeric = frame.select(col("price"), col("qty"))
      keys = [Prism::SortKey.new(col("region"), descending: true, nulls_first: true)]

      cases = {
        "distinct (all columns)"   => {frame.distinct, frame.lazy.distinct},
        "distinct (names)"         => {frame.distinct("region"), frame.lazy.distinct("region")},
        "drop_nulls (all columns)" => {frame.drop_nulls, frame.lazy.drop_nulls},
        "drop_nulls (names)"       => {frame.drop_nulls("region"), frame.lazy.drop_nulls("region")},
        "sort_by (splat)"          => {frame.sort_by("price", order: :desc), frame.lazy.sort_by("price", order: :desc)},
        "sort_by (key list)"       => {frame.sort_by(keys), frame.lazy.sort_by(keys)},
        "select (names)"           => {frame.select("region", "qty"), frame.lazy.select("region", "qty")},
        "select (exprs)"           => {frame.select(col("region"), col("qty").alias("q")), frame.lazy.select(col("region"), col("qty").alias("q"))},
        "filter (expr)"            => {frame.filter(col("qty") > lit(2_i64)), frame.lazy.filter(col("qty") > lit(2_i64))},
        "filter (block)"           => {frame.filter { col("qty") > lit(2_i64) }, frame.lazy.filter { col("qty") > lit(2_i64) }},
        "remove (expr)"            => {frame.remove(col("qty") > lit(2_i64)), frame.lazy.remove(col("qty") > lit(2_i64))},
        "with_column"              => {frame.with_column("double", col("qty") * lit(2_i64)), frame.lazy.with_column("double", col("qty") * lit(2_i64))},
        "with_columns"             => {frame.with_columns(double: col("qty") * lit(2_i64)), frame.lazy.with_columns(double: col("qty") * lit(2_i64))},
        "rename (hash)"            => {frame.rename({"qty" => "count"}), frame.lazy.rename({"qty" => "count"})},
        "rename (pair)"            => {frame.rename("qty", "count"), frame.lazy.rename("qty", "count")},
        "drop"                     => {frame.drop("price"), frame.lazy.drop("price")},
        "cast (every column)"      => {numeric.cast(Arrow::DataType.float64), numeric.lazy.cast(Arrow::DataType.float64)},
        "cast (named)"             => {frame.cast({"qty" => Arrow::DataType.float64}), frame.lazy.cast({"qty" => Arrow::DataType.float64})},
        "fill_null (value)"        => {frame.fill_null(0), frame.lazy.fill_null(0)},
        "fill_null (hash)"         => {frame.fill_null({"price" => 0.0}), frame.lazy.fill_null({"price" => 0.0})},
        "head"                     => {frame.head(2), frame.lazy.head(2)},
        "tail"                     => {frame.tail(2), frame.lazy.tail(2)},
        "slice"                    => {frame.slice(1, 2), frame.lazy.slice(1, 2)},
        "group_by + aggregate"     => {frame.group_by("region").aggregate(n: col("qty").sum), frame.lazy.group_by("region").aggregate(n: col("qty").sum)},
      }

      cases.each do |label, (eager, lazy)|
        collected = lazy.collect
        collected.schema.should eq(eager.schema), "#{label}: schema"
        collected.should eq(eager), "#{label}: frame"
      end
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
