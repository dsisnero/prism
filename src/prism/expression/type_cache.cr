module Prism
  # Per-execution memo for schema-dependent expression facts.
  #
  # Polars converts `Expr` into a physical plan once and reuses the compiled
  # expression for every chunk. This is the same idea at smaller scale: resolve
  # output dtypes and coercion targets once, then reuse them across chunks
  # instead of re-walking the schema (and re-running `can_cast_types`) per
  # batch.
  class TypeCache
    def initialize(@schema : Arrow::Schema)
      @dtypes = {} of String => Arrow::DataType
      @targets = {} of String => Arrow::DataType
      @coins = {} of String => Bool
    end

    def schema : Arrow::Schema
      @schema
    end

    # Output dtype of `expr`, computed once per key.
    def dtype(expr : Expr) : Arrow::DataType
      key = expr.key
      cached = @dtypes[key]?
      return cached if cached

      resolved = expr.dtype(@schema)
      @dtypes[key] = resolved
      resolved
    end

    # Dtype a binary operation between `left` and `right` runs in.
    def coercion_target(left : Expr, right : Expr) : Arrow::DataType
      key = "#{left.key}|#{right.key}"
      cached = @targets[key]?
      return cached if cached

      resolved = Coerce.target(left, right, @schema, self)
      @targets[key] = resolved
      resolved
    end
  end
end
