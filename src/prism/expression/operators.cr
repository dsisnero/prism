module Prism
  # Operators and verb-like methods on expressions.
  #
  # ## Hazards, by design
  #
  # - `==` returns an `Expr`, not a `Bool`. Never use it for truthiness or in
  #   a spec matcher; use `same_as?` for structural equality.
  # - `&` binds tighter than `==` and `>` in Crystal, so boolean composition
  #   needs parentheses: `(col("a") > 1) & (col("b") < 2)`. The `and`/`or`
  #   methods are the precedence-safe form.
  # - `!` cannot be overloaded in Crystal (it is a pseudo-method), so boolean
  #   negation is the `.not` method.
  abstract class Expr
    def +(other : Expr) : Expr
      Binary.new(BinaryOp::Add, self, other)
    end

    def +(other : Number) : Expr
      self + Prism.lit(other)
    end

    def -(other : Expr) : Expr
      Binary.new(BinaryOp::Sub, self, other)
    end

    def -(other : Number) : Expr
      self - Prism.lit(other)
    end

    def *(other : Expr) : Expr
      Binary.new(BinaryOp::Mul, self, other)
    end

    def *(other : Number) : Expr
      self * Prism.lit(other)
    end

    def /(other : Expr) : Expr
      Binary.new(BinaryOp::Div, self, other)
    end

    def /(other : Number) : Expr
      self / Prism.lit(other)
    end

    def %(other : Expr) : Expr
      Binary.new(BinaryOp::Rem, self, other)
    end

    def %(other : Number) : Expr
      self % Prism.lit(other)
    end

    def ==(other : Expr) : Expr
      Binary.new(BinaryOp::Eq, self, other)
    end

    def ==(other : Number | String | Bool | Time | Nil) : Expr
      self == Prism.lit(other)
    end

    def !=(other : Expr) : Expr
      Binary.new(BinaryOp::Neq, self, other)
    end

    def !=(other : Number | String | Bool | Time | Nil) : Expr
      self != Prism.lit(other)
    end

    def <(other : Expr) : Expr
      Binary.new(BinaryOp::Lt, self, other)
    end

    def <(other : Number | String | Bool | Time) : Expr
      self < Prism.lit(other)
    end

    def <=(other : Expr) : Expr
      Binary.new(BinaryOp::LtEq, self, other)
    end

    def <=(other : Number | String | Bool | Time) : Expr
      self <= Prism.lit(other)
    end

    def >(other : Expr) : Expr
      Binary.new(BinaryOp::Gt, self, other)
    end

    def >(other : Number | String | Bool | Time) : Expr
      self > Prism.lit(other)
    end

    def >=(other : Expr) : Expr
      Binary.new(BinaryOp::GtEq, self, other)
    end

    def >=(other : Number | String | Bool | Time) : Expr
      self >= Prism.lit(other)
    end

    # Boolean and. Parenthesise operands when using `&`.
    def &(other : Expr) : Expr
      Binary.new(BinaryOp::And, self, other)
    end

    def &(other : Bool) : Expr
      self & Prism.lit(other)
    end

    # Boolean or. Parenthesise operands when using `|`.
    def |(other : Expr) : Expr
      Binary.new(BinaryOp::Or, self, other)
    end

    def |(other : Bool) : Expr
      self | Prism.lit(other)
    end

    def - : Expr
      Neg.new(self)
    end

    # Precedence-safe boolean and.
    def and(other : Expr | Bool) : Expr
      Binary.new(BinaryOp::And, self, as_expr(other))
    end

    # Precedence-safe boolean or.
    def or(other : Expr | Bool) : Expr
      Binary.new(BinaryOp::Or, self, as_expr(other))
    end

    def not : Expr
      Not.new(self)
    end

    # Named for the operation, not as a Crystal predicate: this builds an
    # expression, so `null?` would misrepresent it (and reads oddly).
    # ameba:disable Naming/PredicateName
    def is_null : Expr
      IsNull.new(self)
    end

    # ameba:disable Naming/PredicateName
    def is_not_null : Expr
      IsNotNull.new(self)
    end

    # Whether each row's value is in `values`, a constant set.
    #
    # The set is encoded once (`Arrow.array(values.to_a)`), so the argument can be
    # a large collection without becoming a huge expression tree:
    #
    # ```
    # df.filter(col("title").is_in(["the dispossessed", "parable of the sower"]))
    # ```
    #
    # A null cell yields null, like every other comparison, so `filter` drops it
    # and `remove` keeps it. Pass `nulls_equal: true` to treat null as a value
    # instead: null then matches only when the set contains null.
    # ameba:disable Naming/PredicateName
    def is_in(values : Enumerable(T), *, nulls_equal : Bool = false) : Expr forall T
      IsIn.new(self, Arrow.array(values.to_a), nulls_equal)
    end

    # The same test against an Arrow array, for a set that is already encoded
    # (a `Series#to_arrow`, a dictionary, or a set carrying nulls).
    # ameba:disable Naming/PredicateName
    def is_in(values : Arrow::ArrayBase, *, nulls_equal : Bool = false) : Expr
      IsIn.new(self, values, nulls_equal)
    end

    def fill_null(value : Number | String | Bool | Time | Nil) : Expr
      FillNull.new(self, Prism.lit(value))
    end

    def coalesce(*others : Expr) : Expr
      # `[self] + others.to_a` infers `Array(Col)` when every operand is a `Col`,
      # which `Coalesce` cannot take; widening the first element fixes the type
      # for the common `col("a").coalesce(col("b"))` case.
      Coalesce.new([self.as(Expr)] + others.to_a)
    end

    # Explicit dtype conversion.
    #
    # `strict: false` is Polars' `strict=False`: a value the target dtype cannot
    # represent becomes null instead of raising (`"abc"` to `Int64`, `300` to
    # `Int8`). The column is then nullable even when every value in this chunk
    # happens to fit, because nullability describes what the expression can
    # produce (`Prism::Cast#nullable?`).
    def cast(target : Arrow::DataType, *, strict : Bool = true) : Expr
      Cast.new(self, target, strict)
    end

    def alias(name : String | Symbol) : Expr
      Alias.new(self, name.to_s)
    end

    private def as_expr(value : Expr | Bool) : Expr
      value.is_a?(Expr) ? value : Prism.lit(value)
    end
  end
end
