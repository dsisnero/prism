require "../spec_helper"

include Prism::DSL

private def expr_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    a:     Arrow::Int64Array.from_nullable([1_i64, 2_i64, 3_i64]),
    b:     Arrow::Int64Array.from_values([10_i64, 20_i64, 30_i64]),
    small: Arrow::PrimitiveArray(Int32).from_values([1, 2, 3]),
    big:   Arrow::PrimitiveArray(UInt64).from_values([1_u64, 2_u64, 3_u64]),
    price: Arrow::Float64Array.from_nullable([1.5, nil, 3.0]),
    flag:  Arrow::BooleanArray.from_nullable([true, false, nil]),
    name:  Arrow::StringArray.from_nullable(["x", "y", nil]),
  })
end

private def expr_ctx : Prism::EvalContext
  Prism::EvalContext.new(expr_batch)
end

private def expr_schema : Arrow::Schema
  expr_batch.schema
end

# Evaluates to an array, unwrapping a broadcast scalar if the expression is a
# bare literal.
private def evaluate(expr : Prism::Expr) : Arrow::ArrayBase
  case datum = expr.eval(expr_ctx)
  when Arrow::ArrayBase
    datum
  else
    datum.as(Arrow::Scalar).array.as(Arrow::ArrayBase)
  end
end

private def values(expr : Prism::Expr) : Array(Prism::Value)
  Prism::Values.extract(evaluate(expr))
end

# The Phase 2 acceptance property: inferred dtype equals evaluated dtype.
private def checked(expr : Prism::Expr) : Prism::Expr
  inferred = expr.dtype(expr_schema)
  evaluate(expr).data_type.should eq(inferred)
  expr
end

describe Prism::Expr do
  describe "columns and literals" do
    it "types and evaluates a column" do
      column = col("b")

      column.dtype(expr_schema).should eq(Arrow::DataType.int64)
      values(checked(column)).should eq([10_i64, 20_i64, 30_i64])
      column.key.should eq("col:b")
      column.columns.should eq(["b"])
      column.literal?.should be_false
    end

    it "accepts a Symbol column name" do
      col(:price).key.should eq("col:price")
    end

    it "normalizes integer and float literals" do
      lit(1).dtype(expr_schema).should eq(Arrow::DataType.int64)
      lit(1_u8).dtype(expr_schema).should eq(Arrow::DataType.uint64)
      lit(1.5).dtype(expr_schema).should eq(Arrow::DataType.float64)
      lit(true).dtype(expr_schema).should eq(Arrow::DataType.boolean)
      lit("x").dtype(expr_schema).should eq(Arrow::DataType.utf8)
      lit(nil).dtype(expr_schema).should eq(Arrow::DataType.null)
      lit(1).literal?.should be_true
    end

    it "reports no columns for a literal" do
      lit(1).columns.should be_empty
    end
  end

  describe "arithmetic" do
    it "adds two columns" do
      sum = checked(col("a") + col("b"))

      sum.dtype(expr_schema).should eq(Arrow::DataType.int64)
      values(sum).should eq([11_i64, 22_i64, 33_i64])
      sum.key.should eq("add(col:a,col:b)")
    end

    it "supports -, *, / and %" do
      values(checked(col("b") - col("a"))).should eq([9_i64, 18_i64, 27_i64])
      values(checked(col("a") * col("b"))).should eq([10_i64, 40_i64, 90_i64])
      values(checked(col("b") / col("a"))).should eq([10_i64, 10_i64, 10_i64])
      values(checked(col("b") % col("a"))).should eq([0_i64, 0_i64, 0_i64])
    end

    it "keeps a narrow column narrow when the literal fits" do
      # The literal is cast to the column's dtype instead of widening the
      # column to Int64, which would copy every value.
      sum = checked(col("small") + lit(1))

      sum.dtype(expr_schema).should eq(Arrow::DataType.int32)
      values(sum).should eq([2, 3, 4])
    end

    it "promotes to the float column's dtype for mixed arithmetic" do
      doubled = checked(col("price") * lit(2))

      doubled.dtype(expr_schema).should eq(Arrow::DataType.float64)
      values(doubled).should eq([3.0, nil, 6.0])
    end

    it "negates" do
      values(checked(-col("a"))).should eq([-1_i64, -2_i64, -3_i64])
    end

    it "rejects arithmetic on strings" do
      expect_raises(Prism::DtypeError) { (col("name") + lit(1)).dtype(expr_schema) }
    end

    it "rejects mixing UInt64 with signed integers, asking for a cast" do
      expect_raises(Prism::DtypeError, /cast/) { (col("big") + col("a")).dtype(expr_schema) }
    end
  end

  describe "comparison" do
    it "compares a column with a literal" do
      greater = checked(col("a") > lit(1))

      greater.dtype(expr_schema).should eq(Arrow::DataType.boolean)
      values(greater).should eq([false, true, true])
    end

    it "supports ==, !=, <, <=, > and >=" do
      values(checked(col("a") == lit(2))).should eq([false, true, false])
      values(checked(col("a") != lit(2))).should eq([true, false, true])
      values(checked(col("a") < lit(2))).should eq([true, false, false])
      values(checked(col("a") <= lit(2))).should eq([true, true, false])
      values(checked(col("a") >= lit(2))).should eq([false, true, true])
    end

    it "propagates nulls in comparisons" do
      values(checked(col("price") > lit(1.0))).should eq([true, nil, true])
      values(checked(col("price") == lit(1.5))).should eq([true, nil, false])
    end

    it "compares strings" do
      values(checked(col("name") == lit("x"))).should eq([true, false, nil])
    end
  end

  describe "boolean logic" do
    it "combines predicates with and/or/not" do
      predicate = checked((col("a") > lit(1)).and(col("b") < lit(30)))
      values(predicate).should eq([false, true, false])

      either = checked((col("a") > lit(1)).or(col("b") < lit(30)))
      values(either).should eq([true, true, true])

      values(checked(col("flag").not)).should eq([false, true, nil])
    end

    it "supports operator form, where parentheses are mandatory" do
      # `&` binds tighter than `==`/`>` in Crystal, so unparenthesized
      # expressions parse incorrectly (C9).
      values(checked((col("a") > lit(1)) & (col("b") < lit(30))))
        .should eq([false, true, false])
      values(checked((col("a") > lit(1)) | (col("b") < lit(30))))
        .should eq([true, true, true])
      values(checked(col("flag").not)).should eq([false, true, nil])
    end

    it "uses Kleene logic so a known value wins over null" do
      # flag is [true, false, nil]
      values(checked(col("flag").and(lit(false))))
        .should eq([false, false, false])
      values(checked(col("flag").or(lit(true))))
        .should eq([true, true, true])
    end

    it "rejects boolean operators on non-boolean expressions" do
      expect_raises(Prism::DtypeError) { col("a").and(col("b")).dtype(expr_schema) }
    end
  end

  describe "null handling" do
    it "finds nulls" do
      values(checked(col("price").is_null)).should eq([false, true, false])
      values(checked(col("price").is_not_null)).should eq([true, false, true])
    end

    it "fills nulls with the column dtype" do
      filled = checked(col("price").fill_null(0))

      filled.dtype(expr_schema).should eq(Arrow::DataType.float64)
      values(filled).should eq([1.5, 0.0, 3.0])
    end

    it "coalesces expressions" do
      coalesced = checked(col("price").coalesce(lit(9.0)))

      values(coalesced).should eq([1.5, 9.0, 3.0])
      # Expressions are values (§ principle 5), so printing one is its canonical
      # key, not an object address.
      coalesced.to_s.should eq(coalesced.key)
    end
  end

  describe "cast and alias" do
    it "casts to a target dtype" do
      cast = checked(col("a").cast(Arrow::DataType.float64))

      cast.dtype(expr_schema).should eq(Arrow::DataType.float64)
      values(cast).should eq([1.0, 2.0, 3.0])
    end

    it "keeps the dtype through an alias and records the name" do
      aliased = checked(col("a").alias("total"))

      aliased.dtype(expr_schema).should eq(Arrow::DataType.int64)
      aliased.key.should eq("alias(col:a,total)")
    end
  end

  describe "structure" do
    it "compares structurally, not by expression identity" do
      (col("a") + lit(1)).same_as?(col("a") + lit(1)).should be_true
      (col("a") + lit(1)).same_as?(col("a") + lit(2)).should be_false
      (col("a") + lit(1)).same_as?(col("b") + lit(1)).should be_false
    end

    it "collects referenced columns once each" do
      (col("a") + col("b") - col("a")).columns.should eq(["a", "b"])
    end
  end

  describe "errors" do
    it "raises ColumnNotFound from dtype inference" do
      expect_raises(Prism::ColumnNotFound, /salry/) do
        col("salry").dtype(expr_schema)
      end
    end

    it "raises ColumnNotFound at evaluation time" do
      expect_raises(Prism::ColumnNotFound, /salry/) do
        col("salry").eval(expr_ctx)
      end
    end
  end
end
