module Prism
  # Expressions that wrap kernels `arrow-cr` already ships.
  #
  # The rule from the plan is "keep data in Arrow buffers, materialize only at
  # edges", so none of these loops over rows in Crystal: each node's `eval`
  # forwards one chunk to one `Arrow.*` kernel, which is where null slots,
  # dictionary/run-end/view dispatch and offsets are already handled. What the
  # nodes add is the expression contract — a `key`, a `dtype` computed before any
  # data is read, and `nullable?` — so a plan can be built and typed without
  # touching a buffer.
  #
  # Naming follows Polars: `str.len_bytes()` is `str_length`, `str.contains` is
  # `str_contains`, `str.slice` is `str_slice`, `dt.year()` is `.year`, and
  # `shift`/`diff`/`rank` keep their names. See `docs/polars-parity.md`.
  module KernelArgs
    extend self

    # A dictionary column goes through its values, the rule every keyed verb,
    # `is_in` and the aggregations already follow: two dictionaries can map one
    # code to different values, so only the decoded dtype means anything here.
    def decode(array : Arrow::ArrayBase) : Arrow::ArrayBase
      value_type = array.data_type.value
      return array unless array.data_type.kind.dictionary? && value_type

      Arrow.cast(array, value_type)
    end

    def decoded_dtype(dtype : Arrow::DataType) : Arrow::DataType
      return dtype unless dtype.kind.dictionary?

      dtype.value || dtype
    end

    # The operand's dtype (decoded), or a `DtypeError` naming it.
    def require_string(operand : Expr, schema : Arrow::Schema) : Arrow::DataType
      dtype = decoded_dtype(operand.dtype(schema))
      return dtype if dtype.string? || dtype.binary?

      raise DtypeError.new(
        "#{subject(operand)} must be a string or binary column for a string " \
        "operation; it is #{dtype}"
      )
    end

    def require_temporal(operand : Expr, schema : Arrow::Schema) : Arrow::DataType
      dtype = decoded_dtype(operand.dtype(schema))
      return dtype if temporal?(dtype)

      raise DtypeError.new(
        "#{subject(operand)} must be a temporal column for a date part; it is #{dtype}"
      )
    end

    def require_rankable(operand : Expr, schema : Arrow::Schema) : Arrow::DataType
      dtype = decoded_dtype(operand.dtype(schema))
      # `Arrow.can_rank?` says yes to a decimal (its storage is a primitive) while
      # `Arrow.rank`'s dispatch has no branch for it and raises, so decimals are
      # excluded here and the inconsistency is filed as
      # `shard_issues/arrow-cr.5.md`.
      return dtype if Arrow.can_rank?(dtype) && !dtype.decimal?

      raise DtypeError.new(
        "#{subject(operand)} cannot be ranked (#{dtype}); " \
        "rank supports numeric, boolean, string and binary columns"
      )
    end

    private def temporal?(dtype : Arrow::DataType) : Bool
      case dtype.kind
      when .date32?, .date64?, .time32?, .time64?, .timestamp?,
           .interval?, .duration?
        true
      else
        false
      end
    end

    # `column "x"` for a bare column, the canonical key otherwise, so the error
    # names what the user wrote.
    private def subject(operand : Expr) : String
      operand.is_a?(Col) ? "column #{operand.name.inspect}" : operand.key
    end
  end

  # Byte length of a string or binary value (`str.len_bytes()` in Polars).
  #
  # Bytes rather than characters, which is what the kernel counts: `"héllo"` is
  # 6 bytes and 5 characters, and a byte offset is what `str_slice` takes.
  class StrLength < Expr
    getter operand : Expr

    def initialize(@operand : Expr)
    end

    def key : String
      "str_length(#{@operand.key})"
    end

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      dtype = KernelArgs.require_string(@operand, schema)
      case dtype.kind
      when .large_utf8?, .large_binary? then Arrow::DataType.int64
      else                                   Arrow::DataType.int32
      end
    end

    def nullable?(schema : Arrow::Schema) : Bool
      @operand.nullable?(schema)
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.length(KernelArgs.decode(Datums.array(ctx.evaluate(@operand), ctx)))
    end

    def to_s(io : IO) : Nil
      io << key
    end
  end

  # The three substring predicates `arrow-string` exposes as one kernel.
  enum StrPredicateKind
    Contains
    StartsWith
    EndsWith

    def key_name : String
      case self
      in .contains?    then "str_contains"
      in .starts_with? then "str_starts_with"
      in .ends_with?   then "str_ends_with"
      end
    end
  end

  # `str_contains` / `str_starts_with` / `str_ends_with` with a constant needle.
  class StrPredicate < Expr
    getter kind : StrPredicateKind
    getter operand : Expr
    getter needle : String

    def initialize(@kind : StrPredicateKind, @operand : Expr, @needle : String)
    end

    def key : String
      "#{@kind.key_name}(#{@operand.key},#{@needle.inspect})"
    end

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      KernelArgs.require_string(@operand, schema)
      Arrow::DataType.boolean
    end

    def nullable?(schema : Arrow::Schema) : Bool
      @operand.nullable?(schema)
    end

    def eval(ctx : EvalContext) : Datum
      haystack = KernelArgs.decode(Datums.array(ctx.evaluate(@operand), ctx))
      # The kernel takes a scalar so the needle is not materialized per row.
      needle = Arrow::Scalar(Arrow::ArrayBase).new(
        Arrow::StringArray.from_values([@needle]).as(Arrow::ArrayBase)
      )

      case @kind
      in .contains?    then Arrow.contains(haystack, needle)
      in .starts_with? then Arrow.starts_with(haystack, needle)
      in .ends_with?   then Arrow.ends_with(haystack, needle)
      end
    end

    def to_s(io : IO) : Nil
      io << key
    end
  end

  # Byte slice of a string or binary value (`str.slice` in Polars).
  #
  # `length` of `nil` means "to the end". Offsets are bytes and may be negative
  # (counted from the end), which is the kernel's rule.
  class StrSlice < Expr
    getter operand : Expr
    getter offset : Int64
    getter length : Int64?

    def initialize(@operand : Expr, @offset : Int64, @length : Int64?)
    end

    def key : String
      "str_slice(#{@operand.key},#{@offset},#{@length || "end"})"
    end

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      KernelArgs.require_string(@operand, schema)
    end

    def nullable?(schema : Arrow::Schema) : Bool
      @operand.nullable?(schema)
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.substring(
        KernelArgs.decode(Datums.array(ctx.evaluate(@operand), ctx)),
        @offset,
        @length,
      )
    end

    def to_s(io : IO) : Nil
      io << key
    end
  end

  # Regular-expression match against a constant pattern (`str.contains` with a
  # pattern, in Polars; `matches` here so a literal `str_contains` stays literal).
  class RegexMatch < Expr
    getter operand : Expr
    getter pattern : String

    def initialize(@operand : Expr, @pattern : String)
    end

    def key : String
      "matches(#{@operand.key},#{@pattern.inspect})"
    end

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      KernelArgs.require_string(@operand, schema)
      Arrow::DataType.boolean
    end

    def nullable?(schema : Arrow::Schema) : Bool
      @operand.nullable?(schema)
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.regexp_is_match_scalar(
        KernelArgs.decode(Datums.array(ctx.evaluate(@operand), ctx)),
        @pattern,
      )
    end

    def to_s(io : IO) : Nil
      io << key
    end
  end

  # One calendar part of a temporal column (`dt.year()` and friends in Polars).
  #
  # Every part is the same kernel with a different `Arrow::DatePart`, so the
  # dtype is always `Int32` and the null slots follow the operand.
  class TemporalPart < Expr
    getter operand : Expr
    getter part : Arrow::DatePart

    def initialize(@operand : Expr, @part : Arrow::DatePart)
    end

    def key : String
      "date_part(#{@operand.key},#{@part.to_s.downcase})"
    end

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      KernelArgs.require_temporal(@operand, schema)
      Arrow::DataType.int32
    end

    def nullable?(schema : Arrow::Schema) : Bool
      @operand.nullable?(schema)
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.date_part(KernelArgs.decode(Datums.array(ctx.evaluate(@operand), ctx)), @part)
    end

    def to_s(io : IO) : Nil
      io << key
    end
  end

  # Moves values `offset` rows forward (or back, for a negative offset), leaving
  # nulls where rows enter or leave the window.
  class Shift < Expr
    getter operand : Expr
    getter offset : Int64

    def initialize(@operand : Expr, @offset : Int64)
    end

    def key : String
      "shift(#{@operand.key},#{@offset})"
    end

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      @operand.dtype(schema)
    end

    # A non-zero shift opens a gap at the boundary, so the column is nullable
    # even when every input value is present; a shift of zero changes nothing.
    def nullable?(schema : Arrow::Schema) : Bool
      @offset != 0 || @operand.nullable?(schema)
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.shift(Datums.array(ctx.evaluate(@operand), ctx), @offset)
    end

    def to_s(io : IO) : Nil
      io << key
    end
  end

  # Rank of each row's value in sorted order (`rank` in Polars).
  #
  # Ties share the highest of their ranks and leave gaps, nulls take the last
  # rank (the kernel's `nulls_first: false`), and every row gets a rank — so the
  # column is never null whatever the operand's nullability.
  class Rank < Expr
    getter operand : Expr
    getter? descending : Bool

    def initialize(@operand : Expr, @descending : Bool = false)
    end

    def key : String
      "rank(#{@operand.key}#{@descending ? ",desc" : ""})"
    end

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      KernelArgs.require_rankable(@operand, schema)
      Arrow::DataType.uint32
    end

    def nullable?(schema : Arrow::Schema) : Bool
      false
    end

    def eval(ctx : EvalContext) : Datum
      array = KernelArgs.decode(Datums.array(ctx.evaluate(@operand), ctx))
      options = Arrow::SortOptions.new(@descending, false)
      Arrow::PrimitiveArray(UInt32).from_values(
        Arrow.rank(array, options)
      ).as(Arrow::ArrayBase)
    end

    def to_s(io : IO) : Nil
      io << key
    end
  end
end
