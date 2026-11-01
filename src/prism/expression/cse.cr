require "set"

module Prism
  # Finds subexpressions that appear more than once in a verb's expressions.
  #
  # Polars' `CommonSubExprOptimizer` counts occurrences, then materializes
  # repeated subexpressions once and substitutes temp columns. Prism keeps the
  # AST intact and instead memoizes the *result* of duplicate keys for the
  # duration of one chunk (see `EvalContext#evaluate`), which is the same saving
  # without plan rewriting.
  module Cse
    extend self

    # Canonical keys that occur more than once across `exprs`.
    def duplicate_keys(exprs : Enumerable(Expr)) : Set(String)
      counts = Hash(String, Int32).new(0)
      exprs.each { |expr| count(expr, counts) }

      duplicates = Set(String).new
      counts.each do |key, occurrences|
        duplicates << key if occurrences > 1
      end
      duplicates
    end

    private def count(expr : Expr, counts : Hash(String, Int32)) : Nil
      key = expr.key
      counts[key] = counts[key] + 1
      expr.children.each { |child| count(child, counts) }
    end
  end
end
