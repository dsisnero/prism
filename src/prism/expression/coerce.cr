module Prism
  # Decides the dtype an operation runs in, and casts datums into it.
  #
  # Two rules, in priority order:
  #
  # 1. **Prefer the column's dtype.** When one side is a literal and the other
  #    is a column, the literal is cast instead of the column, so
  #    `col("i32") + 1` stays `Int32` rather than copying the column into an
  #    `Int64` buffer. This only happens when the cast is possible and the two
  #    dtypes are in the same family, so `col("name") == 1` still errors.
  # 2. **Otherwise unify.** Both sides are cast to the widest common type.
  #
  # Arrow's kernels require identical dtypes on both sides, so one of these
  # always runs before a kernel is called.
  module Coerce
    extend self

    # Casts a datum to `target`, returning it unchanged when it already matches.
    def cast(datum : Datum, target : Arrow::DataType) : Datum
      case value = datum
      when Arrow::ArrayBase
        return value if value.data_type == target

        Arrow.cast(value, target)
      else
        array = value.array.as(Arrow::ArrayBase)
        return value if array.data_type == target

        Arrow::Scalar(Arrow::ArrayBase).new(Arrow.cast(array, target))
      end
    end

    # The dtype an operation between `left` and `right` runs in.
    #
    # `cache` is the per-execution `Prism::TypeCache`; when present, dtype
    # inference happens once per expression instead of once per chunk.
    def target(
      left : Expr,
      right : Expr,
      schema : Arrow::Schema,
      cache : TypeCache? = nil,
    ) : Arrow::DataType
      left_dtype = cache ? cache.dtype(left) : left.dtype(schema)
      right_dtype = cache ? cache.dtype(right) : right.dtype(schema)
      return left_dtype if left_dtype == right_dtype

      if left.literal? != right.literal?
        column_dtype = left.literal? ? right_dtype : left_dtype
        literal_dtype = left.literal? ? left_dtype : right_dtype
        if same_family?(column_dtype, literal_dtype) &&
           literal_cast_safe?(literal_dtype, column_dtype) &&
           Arrow.can_cast_types(literal_dtype, column_dtype)
          return column_dtype
        end
      end

      unify(left_dtype, right_dtype)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def unify(left : Arrow::DataType, right : Arrow::DataType) : Arrow::DataType
      return left if left == right

      left_kind = left.kind
      right_kind = right.kind
      return right if left_kind.null?
      return left if right_kind.null?

      if left.integer? && right.integer?
        return unify_integer(left, right)
      end
      if left.floating? && right.floating?
        return width(left) >= width(right) ? left : right
      end
      if (left.integer? && right.floating?) || (left.floating? && right.integer?)
        number = left.integer? ? left : right
        float = left.floating? ? left : right
        return unify_integer_and_float(number, float)
      end
      if left.string? && right.string?
        fused = left_kind.large_utf8? || right_kind.large_utf8?
        return fused ? Arrow::DataType.large_utf8 : Arrow::DataType.utf8
      end
      if left.binary? && right.binary?
        fused = left_kind.large_binary? || right_kind.large_binary?
        return fused ? Arrow::DataType.large_binary : Arrow::DataType.binary
      end
      if left_kind.timestamp? && right_kind.timestamp?
        return unify_timestamp(left, right)
      end
      if left.decimal? || right.decimal?
        raise UnsupportedOperation.new(
          "decimal coercion is not implemented yet; cast one side explicitly"
        )
      end

      raise DtypeError.new(
        "cannot combine #{left} and #{right}; cast one side explicitly"
      )
    end

    # Casting the literal into the column's dtype must not lose information.
    #
    # A float literal in an integer column would be truncated (`1.5` -> `1`), so
    # that case widens the *column* instead. A Float64 literal in a Float32
    # column is the same problem in reverse.
    private def literal_cast_safe?(
      literal : Arrow::DataType,
      column : Arrow::DataType,
    ) : Bool
      return false if literal.floating? && column.integer?
      return false if literal.kind.float64? && column.kind.float32?

      true
    end

    private def same_family?(left : Arrow::DataType, right : Arrow::DataType) : Bool
      return true if left.kind.null? || right.kind.null?
      return true if left.numeric? && right.numeric?
      return true if left.temporal? && right.temporal?
      return true if left.kind.boolean? && right.kind.boolean?
      return true if left.string? && right.string?

      left.binary? && right.binary?
    end

    private def unify_integer(
      left : Arrow::DataType,
      right : Arrow::DataType,
    ) : Arrow::DataType
      if left.signed_integer? == right.signed_integer?
        width = Math.max(width(left), width(right))
        return left.signed_integer? ? signed(width) : unsigned(width)
      end

      unsigned_side = left.signed_integer? ? right : left
      if width(unsigned_side) < 64
        return Arrow::DataType.int64
      end

      raise DtypeError.new(
        "cannot mix UInt64 with signed integers; cast one side explicitly"
      )
    end

    private def unify_integer_and_float(
      number : Arrow::DataType,
      float : Arrow::DataType,
    ) : Arrow::DataType
      float_width = width(float)
      number_width = width(number)

      if float_width >= 64
        Arrow::DataType.float64
      elsif float_width >= 32
        number_width <= 32 ? float : Arrow::DataType.float64
      else
        number_width <= 16 ? float : Arrow::DataType.float32
      end
    end

    private def unify_timestamp(
      left : Arrow::DataType,
      right : Arrow::DataType,
    ) : Arrow::DataType
      left_unit = left.time_unit
      right_unit = right.time_unit
      unit = if unit_rank(left_unit) >= unit_rank(right_unit)
               left_unit
             else
               right_unit
             end
      unit ||= raise UnsupportedOperation.new("timestamp comparison without a time unit")

      Arrow::DataType.timestamp(unit, left.timezone || right.timezone)
    end

    private def unit_rank(unit : Arrow::TimeUnit?) : Int32
      case unit
      when Arrow::TimeUnit::Nanosecond  then 3
      when Arrow::TimeUnit::Microsecond then 2
      when Arrow::TimeUnit::Millisecond then 1
      else                                   0
      end
    end

    # Bit width of a numeric dtype.
    private def width(data_type : Arrow::DataType) : Int32
      case data_type.kind
      when .int8?, Arrow::DataType::Kind::UInt8   then 8
      when .int16?, Arrow::DataType::Kind::UInt16 then 16
      when .int32?, Arrow::DataType::Kind::UInt32 then 32
      when .int64?, Arrow::DataType::Kind::UInt64 then 64
      when .float16?                              then 16
      when .float32?                              then 32
      when .float64?                              then 64
      else
        raise UnsupportedOperation.new("no numeric width for #{data_type}")
      end
    end

    private def signed(width : Int32) : Arrow::DataType
      case width
      when  8 then Arrow::DataType.int8
      when 16 then Arrow::DataType.int16
      when 32 then Arrow::DataType.int32
      else         Arrow::DataType.int64
      end
    end

    private def unsigned(width : Int32) : Arrow::DataType
      case width
      when  8 then Arrow::DataType.uint8
      when 16 then Arrow::DataType.uint16
      when 32 then Arrow::DataType.uint32
      else         Arrow::DataType.uint64
      end
    end
  end
end
