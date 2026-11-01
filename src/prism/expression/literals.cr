module Prism
  # A constant an expression can hold.
  #
  # Crystal's numeric literals are normalized so a query does not depend on the
  # width the compiler happened to infer: `lit(1)` is `Int64`, `lit(1_u8)` is
  # `UInt64`, `lit(1.5)` is `Float64`. A `Time` literal becomes a
  # microsecond-precision timestamp.
  alias LiteralValue = Int64 | UInt64 | Float64 | Bool | String | Bytes | Time | Nil

  # Builds the Arrow dtype and the length-1 array for a `LiteralValue`.
  module Literals
    extend self

    def dtype_of(value : LiteralValue) : Arrow::DataType
      case value
      when Nil     then Arrow::DataType.null
      when Bool    then Arrow::DataType.boolean
      when Int64   then Arrow::DataType.int64
      when UInt64  then Arrow::DataType.uint64
      when Float64 then Arrow::DataType.float64
      when String  then Arrow::DataType.utf8
      when Bytes   then Arrow::DataType.binary
      when Time    then Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond)
      else
        raise UnsupportedOperation.new("unsupported literal #{value.inspect}")
      end
    end

    def array(value : LiteralValue) : Arrow::ArrayBase
      case value
      when Nil     then Arrow::NullArray.new(1)
      when Bool    then Arrow::BooleanArray.from_values([value])
      when Int64   then Arrow::Int64Array.from_values([value])
      when UInt64  then Arrow::PrimitiveArray(UInt64).from_values([value])
      when Float64 then Arrow::Float64Array.from_values([value])
      when String  then Arrow::StringArray.from_values([value])
      when Bytes   then Arrow::BinaryArray.from_values([value])
      when Time    then timestamp_array(value)
      else
        raise UnsupportedOperation.new("unsupported literal #{value.inspect}")
      end
    end

    # Microseconds since the epoch, matching the literal timestamp dtype.
    def micros(value : Time) : Int64
      value.to_unix.to_i64 * 1_000_000_i64 + (value.nanosecond // 1000).to_i64
    end

    private def timestamp_array(value : Time) : Arrow::ArrayBase
      Arrow::PrimitiveArray(Int64)
        .from_values([micros(value)])
        .with_data_type(dtype_of(value))
    end
  end
end
