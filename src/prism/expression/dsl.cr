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
  end
end
