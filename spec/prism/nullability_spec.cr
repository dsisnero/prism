require "../spec_helper"

include Prism::DSL

# One frame that can answer the nullability question from both sides: `nn` cannot
# hold a null, `holes` does, and `dirty` holds a value no integer dtype can.
private def nullability_frame : Prism::DataFrame
  Prism.from_columns({
    nn:    Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64]),
    holes: Arrow::Int64Array.from_nullable([1_i64, nil, 3_i64]),
    dirty: Arrow::StringArray.from_nullable(["1", "abc", nil]),
  })
end

private def nullable_names(frame : Prism::DataFrame) : Array(String)
  frame.schema.fields.select(&.nullable?).map(&.name)
end

private def schema_of(frame : Prism::DataFrame) : String
  frame.schema.fields.map do |field|
    "#{field.name}=#{field.data_type}#{field.nullable? ? ":null" : ":nn"}"
  end.join("  ")
end

describe "Expr#nullable?" do
  schema = nullability_frame.schema

  it "reports a column's own flag" do
    col("nn").nullable?(schema).should be_false
    col("holes").nullable?(schema).should be_true
  end

  it "knows a literal is null only when it is nil" do
    lit(7).nullable?(schema).should be_false
    lit("x").nullable?(schema).should be_false
    lit(nil).nullable?(schema).should be_true
  end

  it "propagates nulls through arithmetic, comparisons and negation" do
    (col("nn") + lit(1_i64)).nullable?(schema).should be_false
    (col("nn") * col("holes")).nullable?(schema).should be_true
    (col("nn") > lit(1_i64)).nullable?(schema).should be_false
    (col("holes") > lit(1_i64)).nullable?(schema).should be_true
    (-col("nn")).nullable?(schema).should be_false
    (-col("holes")).nullable?(schema).should be_true
    (col("nn") > lit(1_i64)).not.nullable?(schema).should be_false
  end

  it "knows a null predicate answers every row" do
    col("holes").is_null.nullable?(schema).should be_false
    col("holes").is_not_null.nullable?(schema).should be_false
  end

  it "keeps a cast's operand flag, because a cast never invents nulls" do
    col("nn").cast(Arrow::DataType.int32).nullable?(schema).should be_false
    col("holes").cast(Arrow::DataType.int32).nullable?(schema).should be_true
  end

  it "drops the flag when the nulls are filled" do
    col("holes").fill_null(0_i64).nullable?(schema).should be_false
    col("nn").fill_null(0_i64).nullable?(schema).should be_false
  end

  it "is non-null as soon as one coalesce operand cannot be null" do
    # `a.coalesce(b)` is null only where both are, so a non-null operand on
    # either side settles it — position does not matter.
    col("holes").coalesce(col("nn")).nullable?(schema).should be_false
    col("nn").coalesce(col("holes")).nullable?(schema).should be_false
    col("holes").coalesce(col("holes")).nullable?(schema).should be_true
    col("holes").coalesce(lit(nil)).nullable?(schema).should be_true
  end

  it "is as nullable as the branches an if_else can pick" do
    condition = col("nn") > lit(1_i64)
    if_else(condition, col("nn"), col("nn")).nullable?(schema).should be_false
    if_else(condition, col("holes"), col("nn")).nullable?(schema).should be_true
    if_else(condition, col("nn"), col("holes")).nullable?(schema).should be_true
  end

  it "follows the membership null rule" do
    col("nn").is_in([1_i64, 2_i64]).nullable?(schema).should be_false
    col("holes").is_in([1_i64, 2_i64]).nullable?(schema).should be_true
    col("holes").is_in([1_i64, 2_i64], nulls_equal: true).nullable?(schema).should be_false
  end

  it "assumes null for anything it cannot prove, aggregates included" do
    col("nn").sum.nullable?(schema).should be_true
    col("nn").count.nullable?(schema).should be_true
  end

  it "names a column that does not exist" do
    expect_raises(Prism::ColumnNotFound, /"nope"/) { col("nope").nullable?(schema) }
  end
end

describe "one nullability rule for eager verbs and the plan" do
  frame = nullability_frame

  it "agrees on a cast, a fill and a derived column" do
    cases = {
      "cast"              => {frame.cast({"nn" => Arrow::DataType.int32}), frame.lazy.cast({"nn" => Arrow::DataType.int32})},
      "fill_null"         => {frame.fill_null(0), frame.lazy.fill_null(0)},
      "fill_null named"   => {frame.fill_null({"holes" => 0}), frame.lazy.fill_null({"holes" => 0})},
      "with_column col"   => {frame.with_column("copy", col("nn")), frame.lazy.with_column("copy", col("nn"))},
      "with_column arith" => {frame.with_column("sum", col("nn") + lit(1_i64)), frame.lazy.with_column("sum", col("nn") + lit(1_i64))},
      "select alias"      => {frame.select(col("nn").alias("x")), frame.lazy.select(col("nn").alias("x"))},
      "select arith"      => {frame.select((col("nn") * lit(2_i64)).alias("x")), frame.lazy.select((col("nn") * lit(2_i64)).alias("x"))},
    }

    cases.each do |label, (eager, lazy)|
      collected = lazy.collect
      collected.schema.should eq(eager.schema), "#{label}: collected schema"
      lazy.schema.should eq(eager.schema), "#{label}: declared schema"
    end
  end

  it "drops nullability on the column it fills, eagerly and lazily" do
    frame.fill_null(0).schema.field_with_name("holes").nullable?.should be_false
    frame.lazy.fill_null(0).collect.schema.field_with_name("holes").nullable?.should be_false
    frame.fill_null({"holes" => 0}).schema.field_with_name("holes").nullable?.should be_false
    frame.lazy.fill_null({"holes" => 0}).collect.schema.field_with_name("holes").nullable?.should be_false
  end

  it "keeps nullability on the columns it does not touch" do
    filled = frame.fill_null(0)
    filled.schema.fields.map(&.name).should eq(["nn", "holes", "dirty"])
    filled.schema.field_with_name("dirty").nullable?.should be_true
  end

  it "keeps a non-null column non-null through a cast that fits" do
    casted = frame.cast({"nn" => Arrow::DataType.int32})
    casted.schema.field_with_name("nn").nullable?.should be_false
    casted["nn"].null_count.should eq(0)
  end

  it "keeps group_by conservative, as documented" do
    # Grouping does not add nulls, but the aggregate layer builds every output
    # field as nullable in one place, and a nullable field may hold no nulls. The
    # plan runs the eager verb on a zero-row frame, so the two cannot disagree.
    grouped = frame.group_by("nn").aggregate(total: col("holes").sum)
    grouped.schema.fields.map(&.nullable?).should eq([true, true])
    grouped.lazy.collect.schema.should eq(grouped.schema)
  end
end

describe "a declared non-null column holds no nulls" do
  frame = nullability_frame

  it "is true of every verb that builds a schema from expressions" do
    pipelines = {
      "cast"     => frame.cast({"holes" => Arrow::DataType.float64}),
      "fill"     => frame.fill_null(0),
      "select"   => frame.select(col("nn"), (col("nn") + lit(1_i64)).alias("sum"), col("holes").fill_null(-1_i64).alias("filled")),
      "with"     => frame.with_column("copy", col("nn")).with_column("sum", col("nn") + lit(2_i64)),
      "coalesce" => frame.with_column("c", col("holes").coalesce(col("nn"))),
      "if_else"  => frame.with_column("c", if_else(col("nn") > lit(1_i64), col("nn"), col("nn"))),
    }

    pipelines.each do |label, result|
      result.schema.fields.each do |field|
        next if field.nullable?

        result[field.name].null_count.should eq(0), "#{label}: #{field.name} is declared non-null"
      end
    end
  end
end

describe "#cast refuses to invent nulls" do
  frame = nullability_frame

  it "raises eagerly, naming the column, the count and a sample value" do
    error = expect_raises(Prism::DtypeError) do
      frame.cast({"dirty" => Arrow::DataType.int64})
    end
    error.message.to_s.should contain(%(cannot cast column "dirty" from Utf8 to Int64))
    error.message.to_s.should contain("1 of 3 values do not fit and would become null")
    error.message.to_s.should contain(%((e.g. "abc")))
  end

  it "shows at most three of the failed values" do
    many = Prism.from_columns({s: Arrow::StringArray.from_values(["1", "a", "b", "c", "d"])})
    error = expect_raises(Prism::DtypeError) { many.cast({"s" => Arrow::DataType.int64}) }

    error.message.to_s.should contain("4 of 5 values")
    error.message.to_s.should contain(%((e.g. "a", "b", "c", …)))
  end

  it "raises through the plan instead of nulling at collect" do
    lazy = frame.lazy.cast({"dirty" => Arrow::DataType.int64})
    error = expect_raises(Prism::DtypeError) { lazy.collect }
    error.message.to_s.should contain(%(cannot cast column "dirty" from Utf8 to Int64))
  end

  it "raises from the expression, a Series, and a narrowing cast" do
    expect_raises(Prism::DtypeError, /cannot cast column "dirty"/) do
      frame.with_column("n", col("dirty").cast(Arrow::DataType.int64)).to_arrow
    end

    expect_raises(Prism::DtypeError, /cannot cast series "dirty"/) do
      frame["dirty"].cast(Arrow::DataType.int64)
    end

    narrow = Prism.from_columns({n: Arrow::Int64Array.from_values([1_i64, 300_i64])})
    expect_raises(Prism::DtypeError, /cannot cast column "n" from Int64 to Int8/) do
      narrow.cast({"n" => Arrow::DataType.int8})
    end
  end

  it "still casts a column that holds nulls when every value fits" do
    frame.cast({"dirty" => Arrow::DataType.utf8})
    fits = Prism.from_columns({s: Arrow::StringArray.from_nullable(["1", nil, "3"])})
    casted = fits.cast({"s" => Arrow::DataType.int64})

    casted["s"].to_a.should eq([1_i64, nil, 3_i64])
    casted.schema.field_with_name("s").nullable?.should be_true
  end

  it "is a no-op for the same dtype, dirty values and all" do
    frame.cast({"dirty" => Arrow::DataType.utf8})["dirty"].to_a.should eq(["1", "abc", nil])
  end
end

describe "eager and lazy results of one pipeline compose" do
  it "concatenates after a cast, which used to disagree on nullability" do
    frame = nullability_frame
    eager = frame.cast({"nn" => Arrow::DataType.int32})
    lazy = frame.lazy.cast({"nn" => Arrow::DataType.int32}).collect

    stacked = Prism.concat([eager, lazy])
    stacked.row_count.should eq(6)
    stacked.schema.field_with_name("nn").nullable?.should be_false
  end

  it "concatenates after a fill, which used to disagree on nullability" do
    frame = nullability_frame
    eager = frame.fill_null(0)
    lazy = frame.lazy.fill_null(0).collect

    Prism.concat([eager, lazy]).row_count.should eq(6)
  end
end

describe "schema_of" do
  it "keeps every eager and plan schema printable" do
    # A guard on the test helper itself: the labels above are compared as
    # schemas, not strings, so this only checks the helper still renders.
    schema_of(nullability_frame).should contain("holes=Int64:null")
    nullable_names(nullability_frame).should eq(["holes", "dirty"])
  end
end
