module Prism
  # The Crystal-level representation of an Arrow value.
  #
  # Nulls are `nil`. Temporal columns become `Time` (`Time::Span` for
  # durations). Nested, dictionary, decimal and view types are not materialized
  # yet; see `Prism::Values`.
  alias Value = Int8 | Int16 | Int32 | Int64 |
                UInt8 | UInt16 | UInt32 | UInt64 |
                Arrow::Float16 | Float32 | Float64 |
                Bool | String | Bytes | Time | Time::Span | Nil

  # Materializes Arrow arrays into Crystal values and reads single values.
  #
  # This is the one place in the library allowed to switch on
  # `Arrow::DataType::Kind`. Two rules keep it honest:
  #
  # - Null-aware access goes through `Collection#[]?`, never `#value?`. The
  #   latter is bounds-only and would turn a null float into `0.0`.
  # - Unsupported dtypes raise `Prism::UnsupportedOperation` naming the dtype
  #   rather than returning a plausible-looking wrong answer.
  module Values
    extend self

    # ameba:disable Metrics/CyclomaticComplexity
    def extract(array : Arrow::ArrayBase) : Array(Value)
      # Crystal specializes on the call-site type, which may be a concrete
      # array class; widen so sibling `as` casts are legal.
      source = array.as(Arrow::ArrayBase)

      case source.data_type.kind
      when .null?                        then Array(Value).new(source.len) { nil }
      when .boolean?                     then collect_values(source.as(Arrow::BooleanArray))
      when .int8?                        then collect_values(source.as(Arrow::PrimitiveArray(Int8)))
      when .int16?                       then collect_values(source.as(Arrow::PrimitiveArray(Int16)))
      when .int32?                       then collect_values(source.as(Arrow::PrimitiveArray(Int32)))
      when .int64?                       then collect_values(source.as(Arrow::PrimitiveArray(Int64)))
      when Arrow::DataType::Kind::UInt8  then collect_values(source.as(Arrow::PrimitiveArray(UInt8)))
      when Arrow::DataType::Kind::UInt16 then collect_values(source.as(Arrow::PrimitiveArray(UInt16)))
      when Arrow::DataType::Kind::UInt32 then collect_values(source.as(Arrow::PrimitiveArray(UInt32)))
      when Arrow::DataType::Kind::UInt64 then collect_values(source.as(Arrow::PrimitiveArray(UInt64)))
      when .float16?                     then collect_values(source.as(Arrow::PrimitiveArray(Arrow::Float16)))
      when .float32?                     then collect_values(source.as(Arrow::PrimitiveArray(Float32)))
      when .float64?                     then collect_values(source.as(Arrow::PrimitiveArray(Float64)))
      when .utf8?                        then collect_values(source.as(Arrow::StringArray))
      when .large_utf8?                  then collect_values(source.as(Arrow::LargeStringArray))
      when .binary?                      then collect_values(source.as(Arrow::BinaryArray))
      when .large_binary?                then collect_values(source.as(Arrow::LargeBinaryArray))
      when .fixed_size_binary?           then collect_values(source.as(Arrow::FixedSizeBinaryArray))
      when .timestamp?                   then collect_datetime(source.as(Arrow::PrimitiveArray(Int64)))
      when .date32?                      then collect_date(source.as(Arrow::PrimitiveArray(Int32)))
      when .date64?                      then collect_date(source.as(Arrow::PrimitiveArray(Int64)))
      when .time32?                      then collect_time(source.as(Arrow::PrimitiveArray(Int32)))
      when .time64?                      then collect_time(source.as(Arrow::PrimitiveArray(Int64)))
      when .duration?                    then collect_duration(source.as(Arrow::PrimitiveArray(Int64)))
      else
        raise UnsupportedOperation.new(
          "cannot materialize #{source.data_type} values; " \
          "see Prism::Values for the supported types"
        )
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def extract_at(array : Arrow::ArrayBase, index : Int) : Value
      source = array.as(Arrow::ArrayBase)
      return nil unless index >= 0 && index < source.len

      case source.data_type.kind
      when .null?                        then nil
      when .boolean?                     then source.as(Arrow::BooleanArray)[index]?
      when .int8?                        then source.as(Arrow::PrimitiveArray(Int8))[index]?
      when .int16?                       then source.as(Arrow::PrimitiveArray(Int16))[index]?
      when .int32?                       then source.as(Arrow::PrimitiveArray(Int32))[index]?
      when .int64?                       then source.as(Arrow::PrimitiveArray(Int64))[index]?
      when Arrow::DataType::Kind::UInt8  then source.as(Arrow::PrimitiveArray(UInt8))[index]?
      when Arrow::DataType::Kind::UInt16 then source.as(Arrow::PrimitiveArray(UInt16))[index]?
      when Arrow::DataType::Kind::UInt32 then source.as(Arrow::PrimitiveArray(UInt32))[index]?
      when Arrow::DataType::Kind::UInt64 then source.as(Arrow::PrimitiveArray(UInt64))[index]?
      when .float16?                     then source.as(Arrow::PrimitiveArray(Arrow::Float16))[index]?
      when .float32?                     then source.as(Arrow::PrimitiveArray(Float32))[index]?
      when .float64?                     then source.as(Arrow::PrimitiveArray(Float64))[index]?
      when .utf8?                        then source.as(Arrow::StringArray)[index]?
      when .large_utf8?                  then source.as(Arrow::LargeStringArray)[index]?
      when .binary?                      then source.as(Arrow::BinaryArray)[index]?
      when .large_binary?                then source.as(Arrow::LargeBinaryArray)[index]?
      when .fixed_size_binary?           then source.as(Arrow::FixedSizeBinaryArray)[index]?
      when .timestamp?                   then datetime_at(source.as(Arrow::PrimitiveArray(Int64)), index)
      when .date32?                      then date_at(source.as(Arrow::PrimitiveArray(Int32)), index)
      when .date64?                      then date_at(source.as(Arrow::PrimitiveArray(Int64)), index)
      when .time32?                      then time_at(source.as(Arrow::PrimitiveArray(Int32)), index)
      when .time64?                      then time_at(source.as(Arrow::PrimitiveArray(Int64)), index)
      when .duration?                    then duration_at(source.as(Arrow::PrimitiveArray(Int64)), index)
      else
        raise UnsupportedOperation.new("cannot materialize #{source.data_type} values")
      end
    end

    private def self.collect_values(array) : Array(Value)
      out = [] of Value
      array.each { |value| out << value }
      out
    end

    private def self.collect_datetime(array : Arrow::PrimitiveArray(T)) : Array(Value) forall T
      out = [] of Value
      array.len.times do |i|
        out << (array.null?(i) ? nil : array.value_as_datetime(i))
      end
      out
    end

    private def self.collect_date(array : Arrow::PrimitiveArray(T)) : Array(Value) forall T
      out = [] of Value
      array.len.times do |i|
        out << (array.null?(i) ? nil : array.value_as_date(i))
      end
      out
    end

    private def self.collect_time(array : Arrow::PrimitiveArray(T)) : Array(Value) forall T
      out = [] of Value
      array.len.times do |i|
        out << (array.null?(i) ? nil : array.value_as_time(i))
      end
      out
    end

    private def self.collect_duration(array : Arrow::PrimitiveArray(T)) : Array(Value) forall T
      out = [] of Value
      array.len.times do |i|
        out << (array.null?(i) ? nil : array.value_as_duration(i))
      end
      out
    end

    private def self.datetime_at(array : Arrow::PrimitiveArray(T), index : Int) : Value forall T
      array.null?(index) ? nil : array.value_as_datetime(index)
    end

    private def self.date_at(array : Arrow::PrimitiveArray(T), index : Int) : Value forall T
      array.null?(index) ? nil : array.value_as_date(index)
    end

    private def self.time_at(array : Arrow::PrimitiveArray(T), index : Int) : Value forall T
      array.null?(index) ? nil : array.value_as_time(index)
    end

    private def self.duration_at(array : Arrow::PrimitiveArray(T), index : Int) : Value forall T
      array.null?(index) ? nil : array.value_as_duration(index)
    end
  end
end
