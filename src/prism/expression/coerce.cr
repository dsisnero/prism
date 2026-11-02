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
      if left.literal? != right.literal?
        column_dtype = left.literal? ? right_dtype : left_dtype
        literal_dtype = left.literal? ? left_dtype : right_dtype
        return with_literal(column_dtype, literal_dtype)
      end

      unify_pair(left_dtype, right_dtype)
    end

    # The dtype a comparison between a column and a literal runs in.
    #
    # This is the literal half of `#target`, callable without expressions: the
    # literal adopts the column's dtype when that cast is lossless, otherwise
    # both sides widen. `Prism::Series` uses it so `series > 10` coerces exactly
    # like `col("x") > 10` does.
    def with_literal(column : Arrow::DataType, literal : Arrow::DataType) : Arrow::DataType
      if same_family?(column, literal) &&
         literal_cast_safe?(literal, column) &&
         Arrow.can_cast_types(literal, column)
        return column
      end

      unify_pair(column, literal)
    end

    # Widening both sides, with the decimal rule ahead of the equality shortcut:
    # decimals compare at a common scale, and a literal must not drag the column
    # into a wider precision first.
    private def unify_pair(left_dtype : Arrow::DataType, right_dtype : Arrow::DataType) : Arrow::DataType
      return unify_decimal(left_dtype, right_dtype) if left_dtype.decimal? || right_dtype.decimal?
      return left_dtype if left_dtype == right_dtype

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
      if left.decimal? || right.decimal?
        return unify_decimal(left, right)
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

    # Decimals unify at the wider scale and full Decimal128 precision, which is
    # what comparisons need. Integers are promoted into the decimal; floats are
    # rejected because the conversion is lossy.
    private def unify_decimal(
      left : Arrow::DataType,
      right : Arrow::DataType,
    ) : Arrow::DataType
      if left.decimal? && right.decimal?
        scale = Math.max(left.scale || 0, right.scale || 0)
        return Arrow::DataType.decimal128(38, scale)
      end

      decimal = left.decimal? ? left : right
      other = left.decimal? ? right : left
      return decimal if other.kind.null?

      if other.integer?
        return Arrow::DataType.decimal128(38, decimal.scale || 0)
      end

      raise DtypeError.new(
        "cannot combine #{left} with #{right}; cast the decimal to a float explicitly"
      )
    end

    # Operand dtypes for arithmetic when a decimal is involved, or `nil` when the
    # ordinary rules apply.
    #
    # Decimals do not unify to comparison precision here: Arrow's kernels
    # implement Hive's decimal rules and derive the *result* dtype from both
    # operands, so each side keeps its own precision and scale. What they do not
    # accept is two different decimal widths, so both are promoted to the wider
    # kind (a lossless cast: only the storage width changes).
    def arithmetic_operands(
      left : Expr,
      right : Expr,
      schema : Arrow::Schema,
      cache : TypeCache? = nil,
    ) : Tuple(Arrow::DataType, Arrow::DataType)?
      left_dtype = cache ? cache.dtype(left) : left.dtype(schema)
      right_dtype = cache ? cache.dtype(right) : right.dtype(schema)
      return nil unless left_dtype.decimal? || right_dtype.decimal?

      # A float anywhere makes the operation float; the scale is lost, which
      # §8.2 documents.
      if left_dtype.floating? || right_dtype.floating?
        return {Arrow::DataType.float64, Arrow::DataType.float64}
      end

      # A null or a literal adopts the decimal side wholesale, so
      # `decimal(10, 2) + 1` stays at scale 2 rather than promoting to scale 0.
      if adopted = adopted_decimal(left, right, left_dtype, right_dtype)
        return {adopted, adopted}
      end

      decimal_pair(left_dtype, right_dtype)
    end

    # The decimal dtype a null or literal side takes on, or `nil`.
    private def adopted_decimal(
      left : Expr,
      right : Expr,
      left_dtype : Arrow::DataType,
      right_dtype : Arrow::DataType,
    ) : Arrow::DataType?
      decimal = left_dtype.decimal? ? left_dtype : right_dtype
      return decimal if left_dtype.kind.null? || right_dtype.kind.null?
      return nil if left.literal? == right.literal?

      literal = left.literal? ? left_dtype : right_dtype
      Arrow.can_cast_types(literal, decimal) ? decimal : nil
    end

    # Both sides as decimals, promoted to the wider storage kind.
    private def decimal_pair(
      left_dtype : Arrow::DataType,
      right_dtype : Arrow::DataType,
    ) : Tuple(Arrow::DataType, Arrow::DataType)?
      left_operand = decimal_operand(left_dtype)
      right_operand = decimal_operand(right_dtype)
      return nil unless left_operand && right_operand

      rank = Math.max(decimal_rank(left_operand.kind), decimal_rank(right_operand.kind))
      {with_decimal_rank(left_operand, rank), with_decimal_rank(right_operand, rank)}
    end

    # The dtype an arithmetic operation produces from its operands.
    #
    # These are the Hive rules `arrow-cr`'s kernels implement, so the declared
    # dtype is exactly the dtype the kernel returns:
    #
    #   add/sub: scale = max(s1, s2), precision = scale + max(p1 - s1, p2 - s2) + 1
    #   mul:     scale = s1 + s2,     precision = p1 + p2 + 1
    #   div:     scale = s1 + 4,      precision = (scale - s1 + s2) + p1
    #   rem:     scale = max(s1, s2), precision = scale + min(p1 - s1, p2 - s2)
    def arithmetic_dtype(
      op : BinaryOp,
      operands : Tuple(Arrow::DataType, Arrow::DataType),
    ) : Arrow::DataType
      left, right = operands
      return left unless left.decimal? && right.decimal?

      rank = Math.max(decimal_rank(left.kind), decimal_rank(right.kind))
      max_precision = decimal_max_precision(rank)
      max_scale = decimal_max_scale(rank)
      p1, s1 = precision(left), scale(left)
      p2, s2 = precision(right), scale(right)

      case
      when op.add? || op.sub?
        result_scale = Math.max(s1, s2)
        result_precision = cap(Math.max(p1 - s1, p2 - s2) + result_scale + 1, max_precision)
      when op.mul?
        result_scale = s1 + s2
        if result_scale > max_scale
          raise DtypeError.new(
            "a decimal product of #{left} and #{right} needs scale #{result_scale}, " \
            "but the maximum is #{max_scale}; scale an operand down first"
          )
        end
        result_precision = cap(p1 + p2 + 1, max_precision)
      when op.div?
        result_scale = cap(s1 + 4, max_scale)
        result_precision = cap(result_scale - s1 + s2 + p1, max_precision)
      else
        result_scale = Math.max(s1, s2)
        result_precision = cap(Math.min(p1 - s1, p2 - s2) + result_scale, max_precision)
      end

      decimal_data_type(rank, result_precision, result_scale)
    end

    # Integers become decimals with scale 0 and the digits their width can hold;
    # anything else answers `nil` so the caller falls back to the normal rules.
    private def decimal_operand(dtype : Arrow::DataType) : Arrow::DataType?
      return dtype if dtype.decimal?

      precision = integer_precision(dtype.kind)
      precision && Arrow::DataType.decimal128(precision, 0)
    end

    private def integer_precision(kind : Arrow::DataType::Kind) : Int32?
      case kind
      when .int8?, Arrow::DataType::Kind::UInt8   then 3
      when .int16?, Arrow::DataType::Kind::UInt16 then 5
      when .int32?, Arrow::DataType::Kind::UInt32 then 10
      when .int64?                                then 19
      when Arrow::DataType::Kind::UInt64          then 20
      else                                             nil
      end
    end

    # 32/64/128/256: how many bits the decimal's storage takes.
    private def decimal_rank(kind : Arrow::DataType::Kind) : Int32
      case kind
      when .decimal32?  then 32
      when .decimal64?  then 64
      when .decimal256? then 256
      else                   128
      end
    end

    private def decimal_max_precision(rank : Int32) : Int32
      case rank
      when  32 then Arrow::DECIMAL32_MAX_PRECISION.to_i
      when  64 then Arrow::DECIMAL64_MAX_PRECISION.to_i
      when 256 then Arrow::DECIMAL256_MAX_PRECISION.to_i
      else          Arrow::DECIMAL128_MAX_PRECISION.to_i
      end
    end

    private def decimal_max_scale(rank : Int32) : Int32
      case rank
      when  32 then Arrow::DECIMAL32_MAX_SCALE.to_i
      when  64 then Arrow::DECIMAL64_MAX_SCALE.to_i
      when 256 then Arrow::DECIMAL256_MAX_SCALE.to_i
      else          Arrow::DECIMAL128_MAX_SCALE.to_i
      end
    end

    private def decimal_data_type(rank : Int32, precision : Int32, scale : Int32) : Arrow::DataType
      case rank
      when  32 then Arrow::DataType.decimal32(precision, scale)
      when  64 then Arrow::DataType.decimal64(precision, scale)
      when 256 then Arrow::DataType.decimal256(precision, scale)
      else          Arrow::DataType.decimal128(precision, scale)
      end
    end

    private def with_decimal_rank(dtype : Arrow::DataType, rank : Int32) : Arrow::DataType
      return dtype if decimal_rank(dtype.kind) == rank

      decimal_data_type(rank, precision(dtype), scale(dtype))
    end

    private def precision(dtype : Arrow::DataType) : Int32
      (dtype.precision || 0).to_i
    end

    private def scale(dtype : Arrow::DataType) : Int32
      (dtype.scale || 0).to_i
    end

    private def cap(value : Int32, maximum : Int32) : Int32
      value < maximum ? value : maximum
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
