require "../spec_helper"

include Prism::DSL

# Decimal arithmetic follows Arrow's Hive-derived rules, and `arrow-cr`'s kernels
# already implement them: the *result* precision and scale are derived from both
# operands, so the expression has to declare exactly what the kernel returns.
#
#   add / sub: scale = max(s1, s2), precision = scale + max(p1 - s1, p2 - s2) + 1
#   mul:       scale = s1 + s2,     precision = p1 + p2 + 1
#   div:       scale = s1 + 4,      precision = (scale - s1 + s2) + p1
#   rem:       scale = max(s1, s2), precision = scale + min(p1 - s1, p2 - s2)
#
# all capped at the kind's maximum (38/38 for Decimal128).

private def decimal_array(
  values : Array(Int128?),
  precision : Int32,
  scale : Int32,
) : Arrow::ArrayBase
  Arrow::PrimitiveArray(Int128)
    .from_nullable(values)
    .with_data_type(Arrow::DataType.decimal128(precision, scale))
end

private def decimal64_array(
  values : Array(Int64?),
  precision : Int32,
  scale : Int32,
) : Arrow::ArrayBase
  Arrow::PrimitiveArray(Int64)
    .from_nullable(values)
    .with_data_type(Arrow::DataType.decimal64(precision, scale))
end

private def decimal_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    # 1.25, 2.50, null
    amount: decimal_array([125_i128, 250_i128, nil], 10, 2),
    # 1.500, 2.000, 0.500
    rate: decimal_array([1500_i128, 2000_i128, 500_i128], 6, 3),
    # 1.25, 2.50, null, stored as Decimal64
    narrow: decimal64_array([125_i64, 250_i64, nil], 8, 2),
    qty:    Arrow::Int64Array.from_values([2_i64, 3_i64, 4_i64]),
    price:  Arrow::Float64Array.from_values([2.5, 3.5, 4.5]),
  })
end

private def decimal_schema : Arrow::Schema
  decimal_batch.schema
end

private def decimal_ctx : Prism::EvalContext
  Prism::EvalContext.new(decimal_batch)
end

# Evaluates and asserts that the declared dtype is the one the kernel produces.
private def decimal_values(expr : Prism::Expr) : Array(Prism::Value)
  array = case datum = expr.eval(decimal_ctx)
          when Arrow::ArrayBase then datum
          else                       datum.as(Arrow::Scalar).array.as(Arrow::ArrayBase)
          end

  expr.dtype(decimal_schema).should eq(array.data_type)
  Prism::Values.extract(array)
end

describe "decimal arithmetic" do
  it "adds at the wider scale and widens the precision by one" do
    expr = col("amount") + col("amount")

    expr.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(11, 2))
    decimal_values(expr).should eq([
      BigDecimal.new(250, 2), BigDecimal.new(500, 2), nil,
    ])
  end

  it "aligns two scales for add and subtract" do
    sum = col("amount") + col("rate")
    difference = col("amount") - col("rate")

    sum.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(12, 3))
    decimal_values(sum).should eq([
      BigDecimal.new(2750, 3), BigDecimal.new(4500, 3), nil,
    ])

    difference.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(12, 3))
    decimal_values(difference).should eq([
      BigDecimal.new(-250, 3), BigDecimal.new(500, 3), nil,
    ])
  end

  it "adds the scales and precisions for multiply" do
    expr = col("amount") * col("rate")

    expr.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(17, 5))
    decimal_values(expr).should eq([
      BigDecimal.new(187500, 5), BigDecimal.new(500000, 5), nil,
    ])
  end

  it "divides at s1 + 4, truncating toward zero" do
    expr = col("amount") / col("rate")

    expr.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(17, 6))
    decimal_values(expr).should eq([
      BigDecimal.new(833333, 6), BigDecimal.new(1250000, 6), nil,
    ])
  end

  it "keeps the wider scale for remainder" do
    expr = col("amount") % col("rate")

    expr.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(6, 3))
    decimal_values(expr).should eq([
      BigDecimal.new(1250, 3), BigDecimal.new(500, 3), nil,
    ])
  end

  it "keeps the dtype for unary minus" do
    expr = -col("amount")

    expr.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(10, 2))
    decimal_values(expr).should eq([
      BigDecimal.new(-125, 2), BigDecimal.new(-250, 2), nil,
    ])
  end

  it "promotes an integer column to scale zero" do
    expr = col("amount") + col("qty")

    # Int64 has 19 digits, so the result needs 2 + max(8, 19) + 1 = 22 digits.
    expr.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(22, 2))
    decimal_values(expr).should eq([
      BigDecimal.new(325, 2), BigDecimal.new(550, 2), nil,
    ])
  end

  it "casts an integer literal into the column's dtype first" do
    expr = col("amount") + lit(2)

    expr.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(11, 2))
    decimal_values(expr).should eq([
      BigDecimal.new(325, 2), BigDecimal.new(450, 2), nil,
    ])
  end

  it "widens a narrower decimal kind instead of failing" do
    expr = col("amount") + col("narrow")

    expr.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(11, 2))
    decimal_values(expr).should eq([
      BigDecimal.new(250, 2), BigDecimal.new(500, 2), nil,
    ])
  end

  it "falls back to Float64 when a float is involved" do
    expr = col("amount") + col("price")

    expr.dtype(decimal_schema).should eq(Arrow::DataType.float64)
    decimal_values(expr).should eq([3.75, 6.0, nil])
  end

  it "treats a null operand as the other side's dtype, all null" do
    sum = col("amount") + lit(nil)
    mirrored = lit(nil) + col("amount")

    sum.dtype(decimal_schema).should eq(Arrow::DataType.decimal128(11, 2))
    decimal_values(sum).should eq([nil, nil, nil])
    decimal_values(mirrored).should eq([nil, nil, nil])
  end

  it "treats a null column as the other side's dtype" do
    batch = Arrow::RecordBatch.from_columns({
      amount:  decimal_array([125_i128, nil], 10, 2),
      nothing: Arrow::NullArray.new(2).as(Arrow::ArrayBase),
    })
    expr = Prism.col("amount") + Prism.col("nothing")

    array = expr.eval(Prism::EvalContext.new(batch)).as(Arrow::ArrayBase)

    expr.dtype(batch.schema).should eq(array.data_type)
    array.data_type.should eq(Arrow::DataType.decimal128(11, 2))
    Prism::Values.extract(array).should eq([nil, nil])
  end

  it "compares a decimal with a null column as all null" do
    batch = Arrow::RecordBatch.from_columns({
      amount:  decimal_array([125_i128, nil], 10, 2),
      nothing: Arrow::NullArray.new(2).as(Arrow::ArrayBase),
    })
    expr = Prism.col("amount") >= Prism.col("nothing")

    expr.dtype(batch.schema).should eq(Arrow::DataType.boolean)
    Prism::Values.extract(expr.eval(Prism::EvalContext.new(batch)).as(Arrow::ArrayBase))
      .should eq([nil, nil])
  end

  it "rejects a product whose scale would exceed the kind's maximum" do
    batch = Arrow::RecordBatch.from_columns({
      a: decimal_array([1_i128], 30, 20),
      b: decimal_array([1_i128], 30, 20),
    })
    schema = batch.schema

    # 20 + 20 = 40 scales cannot be represented in Decimal128.
    expect_raises(Prism::DtypeError, /scale/) do
      (Prism.col("a") * Prism.col("b")).dtype(schema)
    end
  end

  it "propagates the kernel's division by zero" do
    batch = Arrow::RecordBatch.from_columns({
      a:    decimal_array([125_i128], 10, 2),
      zero: decimal_array([0_i128], 10, 2),
    })
    ctx = Prism::EvalContext.new(batch)

    expect_raises(Arrow::ArrowError, /Divide by zero/) do
      (Prism.col("a") / Prism.col("zero")).eval(ctx)
    end
  end
end
