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

    # --- strings ---------------------------------------------------------
    #
    # Polars' `str` namespace over the kernels `arrow-cr` already ships
    # (`arrow-string`): `str.len_bytes()` is `str_length`, `str.contains` with a
    # literal is `str_contains`, `str.slice` is `str_slice`. They count *bytes*,
    # like the kernels, so a byte offset works on any UTF-8 text.

    # Byte length of a string or binary value.
    def str_length : Expr
      StrLength.new(self)
    end

    # Whether the value contains `needle` as a literal substring.
    def str_contains(needle : String) : Expr
      StrPredicate.new(StrPredicateKind::Contains, self, needle)
    end

    # Whether the value starts with `needle`.
    def str_starts_with(needle : String) : Expr
      StrPredicate.new(StrPredicateKind::StartsWith, self, needle)
    end

    # Whether the value ends with `needle`.
    def str_ends_with(needle : String) : Expr
      StrPredicate.new(StrPredicateKind::EndsWith, self, needle)
    end

    # Upper-cases the value (Polars' `str.to_uppercase()`), Unicode-aware.
    def str_to_uppercase : Expr
      StrTransform.new(StrTransformKind::Upper, self)
    end

    # Lower-cases the value (Polars' `str.to_lowercase()`), Unicode-aware.
    def str_to_lowercase : Expr
      StrTransform.new(StrTransformKind::Lower, self)
    end

    # Strips whitespace from both ends (Polars' `str.strip_chars()`), or from one.
    def str_trim : Expr
      StrTransform.new(StrTransformKind::Trim, self)
    end

    def str_trim_start : Expr
      StrTransform.new(StrTransformKind::TrimStart, self)
    end

    def str_trim_end : Expr
      StrTransform.new(StrTransformKind::TrimEnd, self)
    end

    # A byte slice: `offset` may be negative (from the end), and a `nil` length
    # runs to the end of the value.
    def str_slice(offset : Int, length : Int? = nil) : Expr
      StrSlice.new(self, offset.to_i64, length.try(&.to_i64))
    end

    # Regular-expression match (Polars' `str.contains` with a pattern; separate
    # from `str_contains`, which is literal).
    def matches(pattern : String) : Expr
      RegexMatch.new(self, pattern)
    end

    # --- temporal --------------------------------------------------------

    # One calendar part of a date/time/timestamp column, as `Int32`.
    def date_part(part : Arrow::DatePart) : Expr
      TemporalPart.new(self, part)
    end

    # The accessors Polars spells `dt.<part>()`. `weekday` is 1 = Monday .. 7 =
    # Sunday, and `week` is ISO.
    def year : Expr
      date_part(Arrow::DatePart::Year)
    end

    def quarter : Expr
      date_part(Arrow::DatePart::Quarter)
    end

    def month : Expr
      date_part(Arrow::DatePart::Month)
    end

    def week : Expr
      date_part(Arrow::DatePart::WeekISO)
    end

    def day : Expr
      date_part(Arrow::DatePart::Day)
    end

    def day_of_year : Expr
      date_part(Arrow::DatePart::DayOfYear)
    end

    def weekday : Expr
      date_part(Arrow::DatePart::DayOfWeekMonday1)
    end

    def hour : Expr
      date_part(Arrow::DatePart::Hour)
    end

    def minute : Expr
      date_part(Arrow::DatePart::Minute)
    end

    def second : Expr
      date_part(Arrow::DatePart::Second)
    end

    # --- position --------------------------------------------------------

    # Moves values `offset` rows forward (negative moves them back), leaving
    # nulls where rows enter or leave the window.
    def shift(offset : Int) : Expr
      Shift.new(self, offset.to_i64)
    end

    # Difference from `offset` rows back: `self - self.shift(offset)`.
    def diff(offset : Int = 1) : Expr
      self - shift(offset)
    end

    # Rank of each row in sorted order, ties sharing the highest rank. Every row
    # gets a rank, so the column is never null.
    def rank(*, descending : Bool = false) : Expr
      Rank.new(self, descending)
    end

    def alias(name : String | Symbol) : Expr
      Alias.new(self, name.to_s)
    end

    private def as_expr(value : Expr | Bool) : Expr
      value.is_a?(Expr) ? value : Prism.lit(value)
    end
  end
end
