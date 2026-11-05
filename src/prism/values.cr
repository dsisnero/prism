require "big"

module Prism
  # The Crystal-level representation of an Arrow value.
  #
  # Nulls are `nil`. Temporal columns become `Time` (`Time::Span` for
  # durations), decimals become `BigDecimal`, intervals become
  # `Arrow::IntervalValue`, and nested columns become containers, matching how
  # Polars converts them: a list is an `Array`, a struct is a `Hash` keyed by
  # field name (in schema order), and a map is a `Hash` keyed by its entries.
  # Nulls stay `nil` at every level, so a null list is distinguishable from an
  # empty one and from a list containing a null.
  #
  # An interval is one shape (`months`, `days`, `nanos`) for all three Arrow
  # interval units: year-month keeps its months, day-time converts its
  # milliseconds to nanoseconds, and month-day-nano is already that shape. The
  # unit itself is not part of the value, so a year-month interval renders as
  # months (see `Prism::Display`); pass the column through `Series#cast` to a
  # string dtype if the unit-specific Arrow text is needed.
  #
  # The recursion is through `Array`/`Hash`, which Crystal allows in an alias
  # (`JSON::Any` is built the same way), and every member is hashable, so a
  # `Value` can still be a `Hash`/`Set` key.
  alias Value = Int8 | Int16 | Int32 | Int64 |
                UInt8 | UInt16 | UInt32 | UInt64 |
                Arrow::Float16 | Float32 | Float64 |
                Bool | String | Bytes | Time | Time::Span |
                BigDecimal | Arrow::IntervalValue |
                Array(Value) | Hash(String, Value) | Hash(Value, Value) |
                Nil

  # One materialized row: column name to value, in schema order.
  #
  # This is the dynamic row shape (`DataFrame#row`, `#rows`, `#each_row`) and
  # the input `Row.from_hash` on a generated typed schema accepts. The name
  # exists so callers can annotate a hand-built row without repeating the union.
  alias RowHash = Hash(String, Value)

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
      when .utf8_view?                   then collect_values(source.as(Arrow::StringViewArray))
      when .binary?                      then collect_values(source.as(Arrow::BinaryArray))
      when .large_binary?                then collect_values(source.as(Arrow::LargeBinaryArray))
      when .binary_view?                 then collect_values(source.as(Arrow::BinaryViewArray))
      when .fixed_size_binary?           then collect_values(source.as(Arrow::FixedSizeBinaryArray))
      when .run_end_encoded?             then collect_run_end(source)
      when .dictionary?                  then extract(decoded_dictionary(source))
      when .decimal32?                   then collect_decimal(source.as(Arrow::PrimitiveArray(Int32)))
      when .decimal64?                   then collect_decimal(source.as(Arrow::PrimitiveArray(Int64)))
      when .decimal128?                  then collect_decimal(source.as(Arrow::PrimitiveArray(Int128)))
      when .decimal256?                  then collect_decimal256(source)
      when .timestamp?                   then collect_datetime(source.as(Arrow::PrimitiveArray(Int64)))
      when .date32?                      then collect_date(source.as(Arrow::PrimitiveArray(Int32)))
      when .date64?                      then collect_date(source.as(Arrow::PrimitiveArray(Int64)))
      when .time32?                      then collect_time(source.as(Arrow::PrimitiveArray(Int32)))
      when .time64?                      then collect_time(source.as(Arrow::PrimitiveArray(Int64)))
      when .duration?                    then collect_duration(source.as(Arrow::PrimitiveArray(Int64)))
      when .interval?                    then collect_interval(source)
      when .list?, .large_list?          then collect_list(source.as(Arrow::ListArray))
      when .list_view?                   then collect_list(source.as(Arrow::ListViewArray))
      when .large_list_view?             then collect_list(source.as(Arrow::LargeListViewArray))
      when .fixed_size_list?             then collect_list(source.as(Arrow::FixedSizeListArray))
      when .struct?                      then collect_struct(source.as(Arrow::StructArray))
      when .map?                         then collect_map(source.as(Arrow::MapArray))
      else
        raise UnsupportedOperation.new(
          "cannot materialize #{source.data_type} values; " \
          "see Prism::Values for the supported types"
        )
      end
    end

    # List, large list, list view and fixed-size list share one shape: a row is
    # a child-array view, or null.
    # The list-like classes differ only in offset width (and fixed vs
    # offset-pair), so each forwards its own `#value` into one shared shape.
    private def self.collect_list(array : Arrow::GenericListArray(O)) : Array(Value) forall O
      collect_rows(array) { |index| array.value(index) }
    end

    private def self.collect_list(array : Arrow::GenericListViewArray(O)) : Array(Value) forall O
      collect_rows(array) { |index| array.value(index) }
    end

    private def self.collect_list(array : Arrow::FixedSizeListArray) : Array(Value)
      collect_rows(array) { |index| array.value(index) }
    end

    private def self.collect_rows(
      array : Arrow::ArrayBase,
      &block : Int32 -> Arrow::ArrayBase
    ) : Array(Value)
      Array(Value).new(array.len) do |index|
        array.null?(index) ? nil : extract(block.call(index))
      end
    end

    # A struct is a field-ordered hash, like a Python dict built from `to_list`.
    private def self.collect_struct(array : Arrow::StructArray) : Array(Value)
      names = array.column_names
      columns = array.columns

      Array(Value).new(array.len) do |index|
        if array.null?(index)
          nil
        else
          row = {} of String => Value
          names.each_with_index { |name, position| row[name] = extract_at(columns[position], index) }
          row
        end
      end
    end

    # A map is a hash of its entries. Duplicate keys collapse, last one wins,
    # which is what a Python dict does with the same entries.
    private def self.collect_map(array : Arrow::MapArray) : Array(Value)
      Array(Value).new(array.len) do |index|
        if array.null?(index)
          nil
        else
          entries = array.value(index)
          keys = entries.column(0)
          values = entries.column(1)
          pairs = {} of Value => Value
          entries.len.times do |position|
            pairs[extract_at(keys, position)] = extract_at(values, position)
          end
          pairs
        end
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
      when .utf8_view?                   then source.as(Arrow::StringViewArray)[index]?
      when .binary?                      then source.as(Arrow::BinaryArray)[index]?
      when .large_binary?                then source.as(Arrow::LargeBinaryArray)[index]?
      when .binary_view?                 then source.as(Arrow::BinaryViewArray)[index]?
      when .fixed_size_binary?           then source.as(Arrow::FixedSizeBinaryArray)[index]?
      when .run_end_encoded?             then run_end_at(source, index)
      when .dictionary?
        decoded = decoded_dictionary(source.slice(index, 1))
        extract_at(decoded, 0)
      when .list?, .large_list?, .list_view?, .large_list_view?,
           .fixed_size_list?, .struct?, .map?
        return nil if source.null?(index)

        extract(source.slice(index, 1)).first
      when .decimal32?  then decimal_at(source.as(Arrow::PrimitiveArray(Int32)), index)
      when .decimal64?  then decimal_at(source.as(Arrow::PrimitiveArray(Int64)), index)
      when .decimal128? then decimal_at(source.as(Arrow::PrimitiveArray(Int128)), index)
      when .decimal256? then decimal256_at(source, index)
      when .timestamp?  then datetime_at(source.as(Arrow::PrimitiveArray(Int64)), index)
      when .date32?     then date_at(source.as(Arrow::PrimitiveArray(Int32)), index)
      when .date64?     then date_at(source.as(Arrow::PrimitiveArray(Int64)), index)
      when .time32?     then time_at(source.as(Arrow::PrimitiveArray(Int32)), index)
      when .time64?     then time_at(source.as(Arrow::PrimitiveArray(Int64)), index)
      when .duration?   then duration_at(source.as(Arrow::PrimitiveArray(Int64)), index)
      when .interval?   then interval_at(source, index)
      else
        raise UnsupportedOperation.new("cannot materialize #{source.data_type} values")
      end
    end

    # Numeric value as Float64, for means/medians and other float outputs.
    def to_f64(value : Value) : Float64
      case value
      when Arrow::Float16 then value.to_f64
      when Int8, Int16, Int32, Int64,
           UInt8, UInt16, UInt32, UInt64,
           Float32, Float64
        value.to_f
      when BigDecimal
        value.to_f
      else
        raise DtypeError.new("expected a number, got #{value.inspect}")
      end
    end

    private def self.collect_values(array) : Array(Value)
      out = [] of Value
      array.each { |value| out << value }
      out
    end

    # Dictionary arrays are decoded through their value type, so callers see
    # values rather than dictionary keys.
    private def self.decoded_dictionary(array : Arrow::ArrayBase) : Arrow::ArrayBase
      value_type = array.data_type.value
      unless value_type
        raise UnsupportedOperation.new("#{array.data_type} has no dictionary value type")
      end

      Arrow.cast(array, value_type)
    end

    # Decimals materialize as `BigDecimal` so the scale is preserved; returning
    # the raw scaled integer would silently drop it.
    private def self.collect_decimal(array : Arrow::PrimitiveArray(T)) : Array(Value) forall T
      scale = array.data_type.scale || 0
      out = [] of Value
      array.len.times do |i|
        out << (array.null?(i) ? nil : BigDecimal.new(array.value(i).to_i128, scale))
      end
      out
    end

    private def self.decimal_at(array : Arrow::PrimitiveArray(T), index : Int) : Value forall T
      return nil if array.null?(index)

      BigDecimal.new(array.value(index).to_i128, array.data_type.scale || 0)
    end

    # `Decimal256` is stored as an `I256`, which does not fit `Int128` in
    # general, so the value goes through Arrow's decimal formatter (the same one
    # `to_s` uses) rather than the integer constructor.
    private def self.collect_decimal256(array : Arrow::ArrayBase) : Array(Value)
      source = array.as(Arrow::PrimitiveArray(Arrow::I256))
      scale = (source.data_type.scale || 0).to_i32

      Array(Value).new(source.len) do |index|
        source.null?(index) ? nil : decimal256_value(source.value(index), scale)
      end
    end

    private def self.decimal256_at(array : Arrow::ArrayBase, index : Int) : Value
      source = array.as(Arrow::PrimitiveArray(Arrow::I256))
      return nil if source.null?(index)

      decimal256_value(source.value(index), (source.data_type.scale || 0).to_i32)
    end

    private def self.decimal256_value(value : Arrow::I256, scale : Int32) : Value
      BigDecimal.new(Arrow.format_decimal_str(value.to_s, 0, scale))
    end

    # Run-end encoded arrays have three possible run-end widths, so the generic
    # class is dispatched rather than cast. `RunEndBuffer#len` is the *logical*
    # (row) length, and `RunArray#value` maps a logical index to its run, so each
    # row reads the value it belongs to.
    private def self.collect_run_end(array : Arrow::ArrayBase) : Array(Value)
      case array
      when Arrow::RunArray(Int16), Arrow::RunArray(Int32), Arrow::RunArray(Int64)
        logical = array.run_ends.len.to_i32
        values = Array(Value).new(logical)
        logical.times { |index| values << run_end_at(array, index) }
        values
      else
        raise UnsupportedOperation.new("cannot materialize #{array.data_type} values")
      end
    end

    private def self.run_end_at(array : Arrow::ArrayBase, index : Int) : Value
      case array
      when Arrow::RunArray(Int16),
           Arrow::RunArray(Int32),
           Arrow::RunArray(Int64)
        extract_at(array.value(index), 0)
      else
        raise UnsupportedOperation.new("cannot materialize #{array.data_type} values")
      end
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

    # All three interval units decode to `Arrow::IntervalValue`: Arrow's own
    # value shape (`months`, `days`, `nanos`, with the packing helpers in
    # `Arrow.interval_*`), so no parallel interval type is introduced here.
    private def self.collect_interval(array : Arrow::ArrayBase) : Array(Value)
      Array(Value).new(array.len) { |index| interval_at(array, index) }
    end

    private def self.interval_at(array : Arrow::ArrayBase, index : Int) : Value
      return nil if array.null?(index)

      case array.data_type.interval_unit
      when Arrow::IntervalUnit::YearMonth
        months = array.as(Arrow::PrimitiveArray(Int32)).value(index)
        Arrow::IntervalValue.new(months, 0, 0)
      when Arrow::IntervalUnit::DayTime
        days, milliseconds =
          Arrow.interval_day_time_parts(array.as(Arrow::PrimitiveArray(Int64)).value(index))
        Arrow::IntervalValue.new(0, days, milliseconds.to_i64 * Arrow::NANOS_PER_MILLIS)
      when Arrow::IntervalUnit::MonthDayNano
        months, days, nanoseconds =
          Arrow.interval_month_day_nano_parts(array.as(Arrow::PrimitiveArray(Int128)).value(index))
        Arrow::IntervalValue.new(months, days, nanoseconds)
      else
        raise UnsupportedOperation.new("cannot materialize #{array.data_type} values")
      end
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
