require "../spec_helper"

include Prism::DSL

# Counts evaluations, to prove common-subexpression elimination.
private class CountingExpr < Prism::Expr
  @@evals = 0

  def self.evals : Int32
    @@evals
  end

  def self.reset : Nil
    @@evals = 0
  end

  getter inner : Prism::Expr

  def initialize(@inner : Prism::Expr)
  end

  def key : String
    "counted(#{@inner.key})"
  end

  def columns : Array(String)
    @inner.columns
  end

  def dtype(schema : Arrow::Schema) : Arrow::DataType
    @inner.dtype(schema)
  end

  def eval(ctx : Prism::EvalContext) : Prism::Datum
    @@evals += 1
    @inner.eval(ctx)
  end
end

# Counts dtype inference, to prove the per-execution type cache.
private class CountingDtype < Prism::Expr
  @@calls = 0

  def self.calls : Int32
    @@calls
  end

  def self.reset : Nil
    @@calls = 0
  end

  def initialize(@inner : Prism::Expr)
  end

  def key : String
    "typed(#{@inner.key})"
  end

  def columns : Array(String)
    @inner.columns
  end

  def dtype(schema : Arrow::Schema) : Arrow::DataType
    @@calls += 1
    @inner.dtype(schema)
  end

  def eval(ctx : Prism::EvalContext) : Prism::Datum
    @inner.eval(ctx)
  end
end

private def plan_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    a:   Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64]),
    b:   Arrow::Int64Array.from_values([10_i64, 20_i64, 30_i64, 40_i64]),
    tag: Arrow::StringArray.from_values(["x", "y", "x", "y"]),
  })
end

private def plan_frame : Prism::DataFrame
  Prism.from_arrow(plan_batch)
end

# Two chunks, so per-chunk work is observable.
private def chunked_plan_frame : Prism::DataFrame
  Prism.from_batches([plan_batch.slice(0, 2), plan_batch.slice(2, 2)])
end

describe "expression execution" do
  describe "common subexpression elimination" do
    it "evaluates a repeated subexpression once per chunk" do
      frame = plan_frame
      shared = CountingExpr.new(col("a"))

      CountingExpr.reset
      result = frame.select(
        (shared + lit(1)).alias("plus_one"),
        (shared + lit(2)).alias("plus_two"),
      )

      result["plus_one"].to_a.should eq([2_i64, 3_i64, 4_i64, 5_i64])
      result["plus_two"].to_a.should eq([3_i64, 4_i64, 5_i64, 6_i64])
      CountingExpr.evals.should eq(1)
    end

    it "evaluates once per chunk across several chunks" do
      frame = chunked_plan_frame
      shared = CountingExpr.new(col("a"))

      CountingExpr.reset
      frame.select(
        (shared * lit(2)).alias("doubled"),
        (shared * lit(3)).alias("tripled"),
      )

      CountingExpr.evals.should eq(2)
    end

    it "still evaluates distinct subexpressions separately" do
      frame = plan_frame
      left = CountingExpr.new(col("a"))
      right = CountingExpr.new(col("b"))

      CountingExpr.reset
      frame.with_column("sum", left + right)

      CountingExpr.evals.should eq(2)
    end

    it "does not change results" do
      frame = plan_frame
      shared = col("a") + col("b")

      frame.with_column("x", shared * lit(2))["x"].to_a
        .should eq([22_i64, 44_i64, 66_i64, 88_i64])
    end
  end

  describe "type cache" do
    it "resolves an expression dtype once per execution, not once per chunk" do
      tracked = CountingDtype.new(col("a"))
      expression = (tracked + lit(1)).alias("plus_one")

      CountingDtype.reset
      Prism.from_arrow(plan_batch).select(expression)
      single_chunk_calls = CountingDtype.calls

      CountingDtype.reset
      chunked_plan_frame.select(expression)
      chunked_calls = CountingDtype.calls

      single_chunk_calls.should be > 0
      chunked_calls.should eq(single_chunk_calls)
    end

    it "keeps coercion stable across chunks" do
      frame = chunked_plan_frame

      frame.with_column("scaled", col("a") * lit(1.5))["scaled"].to_a
        .should eq([1.5, 3.0, 4.5, 6.0])
    end
  end

  describe "filter fast paths" do
    it "returns the same batch when every row matches" do
      batch = plan_batch
      frame = Prism.from_arrow(batch)

      filtered = frame.filter(col("a") > lit(0))

      filtered.row_count.should eq(4)
      filtered.to_arrow.same?(batch).should be_true
    end

    it "skips the filter kernel when no row matches" do
      filtered = plan_frame.filter(col("a") > lit(100))

      filtered.row_count.should eq(0)
      filtered.column_names.should eq(["a", "b", "tag"])
    end

    it "handles a constant true predicate" do
      plan_frame.filter(lit(true)).row_count.should eq(4)
    end

    it "handles a constant false predicate" do
      plan_frame.filter(lit(false)).row_count.should eq(0)
      plan_frame.filter(lit(false)).column_names.should eq(["a", "b", "tag"])
    end

    it "keeps chunking when no row matches" do
      count = 0
      chunked_plan_frame.filter(col("a") > lit(100)).each_batch { count += 1 }

      count.should eq(2)
    end
  end
end
