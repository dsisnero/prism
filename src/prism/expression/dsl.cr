module Prism
  # Builds a column reference.
  def self.col(name : String | Symbol) : Expr
    Col.new(name.to_s)
  end

  # Builds a constant.
  #
  # Integer and float literals are normalized to `Int64`/`UInt64`/`Float64` so
  # queries do not depend on the width Crystal inferred.
  def self.lit(value : Int8 | Int16 | Int32 | Int64 | Int128) : Expr
    Lit.new(value.to_i64)
  end

  def self.lit(value : UInt8 | UInt16 | UInt32 | UInt64 | UInt128) : Expr
    Lit.new(value.to_u64)
  end

  def self.lit(value : Float) : Expr
    Lit.new(value.to_f64)
  end

  def self.lit(value : Bool | String | Bytes | Time | Nil) : Expr
    Lit.new(value)
  end

  # Row-aligned conditional: `then_value` where `condition` is true, `else_value`
  # otherwise.
  #
  # A null condition takes the else branch: `Arrow.zip` reads a null mask as
  # false, which is also what Polars' `when/then/otherwise` does, so a null flag
  # never invents a null result. Non-expression arguments are wrapped with `lit`,
  # so both branches can be plain values.
  #
  # ```
  # Prism.if_else(col("status") == "ok", col("price"), 0.0)
  # Prism.if_else(col("qty") > 10, "big", "small")
  # ```
  def self.if_else(condition : Expr, then_value, else_value) : Expr
    IfElse.new(condition, as_expr(then_value), as_expr(else_value))
  end

  # First matching branch, with `otherwise` when none matches — SQL `CASE` /
  # Polars `when().then().when().then().otherwise()` as one call.
  #
  # Each branch is a `{condition, value}` tuple, tried in order, so the first
  # true condition wins even if a later one is also true. A null condition
  # matches nothing and falls through, which is what makes this read like `CASE`.
  #
  # ```
  # Prism.case_when(
  #   [{col("score") >= 90, lit("A")}, {col("score") >= 80, lit("B")}],
  #   lit("C"),
  # )
  # ```
  def self.case_when(
    branches : Enumerable(Tuple(Expr, Expr)),
    otherwise : Expr,
  ) : Expr
    pairs = branches.to_a
    if pairs.empty?
      raise ArgumentError.new("case_when needs at least one {condition, value} branch")
    end

    result = otherwise
    pairs.reverse_each { |(condition, value)| result = if_else(condition, value, result) }
    result
  end

  private def self.as_expr(value : Expr) : Expr
    value
  end

  private def self.as_expr(value) : Expr
    Prism.lit(value)
  end

  # Mix this into your own scope to write `col("x")` and `lit(1)` unqualified.
  #
  # ```
  # include Prism::DSL
  # df.filter { (col("status") == "ok") & (col("price") > 10) }
  # ```
  module DSL
    def col(name : String | Symbol) : Prism::Expr
      Prism.col(name)
    end

    def lit(value) : Prism::Expr
      Prism.lit(value)
    end

    def if_else(condition : Prism::Expr, then_value, else_value) : Prism::Expr
      Prism.if_else(condition, then_value, else_value)
    end

    def case_when(
      branches : Enumerable(Tuple(Prism::Expr, Prism::Expr)),
      otherwise : Prism::Expr,
    ) : Prism::Expr
      Prism.case_when(branches, otherwise)
    end
  end
end
