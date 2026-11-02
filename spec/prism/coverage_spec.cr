require "../spec_helper"

include Prism::DSL

private def narrow_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    small: Arrow::PrimitiveArray(Int32).from_nullable([1, 2, nil]),
    byte:  Arrow::PrimitiveArray(UInt8).from_values([1_u8, 2_u8, 3_u8]),
  })
end

private def narrow_ctx : Prism::EvalContext
  Prism::EvalContext.new(narrow_batch)
end

private def narrow_schema : Arrow::Schema
  narrow_batch.schema
end

private def evaluate(expr : Prism::Expr) : Array(Prism::Value)
  datum = expr.eval(narrow_ctx)
  array = case datum
          when Arrow::ArrayBase then datum
          else                       datum.as(Arrow::Scalar).array.as(Arrow::ArrayBase)
          end
  Prism::Values.extract(array)
end

private def price_in_cents : Arrow::ArrayBase
  Arrow::PrimitiveArray(Int128)
    .from_nullable([125_i128, 250_i128, nil])
    .with_data_type(Arrow::DataType.decimal128(10, 2))
end

private def decimal_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    amount: price_in_cents,
    qty:    Arrow::Int64Array.from_values([2_i64, 3_i64, 4_i64]),
  })
end

private def decimal_ctx : Prism::EvalContext
  Prism::EvalContext.new(decimal_batch)
end

private def decimal_schema : Arrow::Schema
  decimal_batch.schema
end

describe "out-of-range literal folding" do
  it "folds a greater-than against a too-large literal to false" do
    expr = col("small") > lit(3_000_000_000_i64)

    expr.dtype(narrow_schema).should eq(Arrow::DataType.boolean)
    evaluate(expr).should eq([false, false, nil])
  end

  it "folds a less-than against a too-large literal to true" do
    evaluate(col("small") < lit(3_000_000_000_i64)).should eq([true, true, nil])
  end

  it "folds equality and inequality" do
    evaluate(col("small") == lit(3_000_000_000_i64)).should eq([false, false, nil])
    evaluate(col("small") != lit(3_000_000_000_i64)).should eq([true, true, nil])
  end

  it "folds when the literal is below the dtype's minimum" do
    evaluate(col("small") < lit(-3_000_000_000_i64)).should eq([false, false, nil])
    evaluate(col("small") > lit(-3_000_000_000_i64)).should eq([true, true, nil])
  end

  it "folds when the literal is on the left" do
    # 3e9 is above every `small` value, so `lit > col` holds and `lit < col` does not.
    evaluate(lit(3_000_000_000_i64) > col("small")).should eq([true, true, nil])
    evaluate(lit(3_000_000_000_i64) < col("small")).should eq([false, false, nil])
  end

  it "folds for unsigned columns" do
    evaluate(col("byte") > lit(300_i64)).should eq([false, false, false])
    evaluate(col("byte") < lit(300_i64)).should eq([true, true, true])
  end

  it "leaves in-range literals on the normal path" do
    evaluate(col("small") > lit(1_i64)).should eq([false, true, nil])
  end

  it "raises for out-of-range literals in arithmetic instead of nulling rows" do
    expect_raises(Prism::DtypeError, /does not fit/) do
      (col("small") * lit(3_000_000_000_i64)).eval(narrow_ctx)
    end
  end
end

describe "decimal expressions" do
  it "materializes decimals as BigDecimal, keeping the scale" do
    Prism::Values.extract_at(price_in_cents, 0).should eq(BigDecimal.new(125, 2))
    Prism::Values.extract_at(price_in_cents, 1).should eq(BigDecimal.new(250, 2))
    Prism::Values.extract_at(price_in_cents, 2).should be_nil
  end

  it "unifies two decimals at the wider scale for comparison" do
    expr = col("amount") == col("amount")

    expr.dtype(decimal_schema).should eq(Arrow::DataType.boolean)
    Prism::Values.extract(expr.eval(decimal_ctx).as(Arrow::ArrayBase))
      .should eq([true, true, nil])
  end

  it "compares a decimal with an integer literal" do
    # 1.25 and 2.50 against 2.00.
    compared = (col("amount") > lit(2)).eval(decimal_ctx).as(Arrow::ArrayBase)

    Prism::Values.extract(compared).should eq([false, true, nil])
  end

  it "runs decimal arithmetic with the kernels' Hive scale rules" do
    # Full coverage of the rules lives in spec/prism/decimal_arithmetic_spec.cr.
    sum = (col("amount") + lit(2)).dtype(decimal_schema)

    sum.should eq(Arrow::DataType.decimal128(11, 2))
  end

  it "rejects unifying a decimal column with a float column" do
    expect_raises(Prism::DtypeError, /decimal/) do
      Prism::Coerce.unify(
        Arrow::DataType.decimal128(10, 2),
        Arrow::DataType.float64,
      )
    end
  end
end

describe "dictionary columns" do
  it "materializes decoded values rather than keys" do
    dictionary = Arrow::DictionaryArray(Int32)
      .from_values(["east", "west", "east"])
      .as(Arrow::ArrayBase)

    Prism::Values.extract(dictionary).should eq(["east", "west", "east"])
    Prism::Values.extract_at(dictionary, 1).should eq("west")
  end
end
