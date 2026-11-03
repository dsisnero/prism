require "set"

module Prism
  # Key handling shared by every keyed operation: joins, `distinct`, `unique`,
  # `n_unique` and `group_by`.
  #
  # `Arrow::RowConverter` encodes one or more columns into big-endian bytes whose
  # `memcmp` order matches the sort order (null sentinel, sign-flipped integers,
  # IEEE `totalOrder` floats, block-encoded strings). Equal keys encode to equal
  # bytes, so a `Hash`/`Set` over encoded rows turns joins and `distinct` into
  # O(n) passes with no per-row boxing. This is why Arrow has no hashing kernel:
  # `arrow-row` *is* the primitive, and this module is the only place Prism
  # touches it.
  #
  # Two things differ between "equal under `==`" and "equal bytes":
  #
  # - **Floats.** The row format is `totalOrder`, so `-0.0` and `0.0` encode
  #   differently and NaNs encode by payload. Keying must follow `==`, not
  #   ordering — `-0.0 == 0.0` is true and `NaN != NaN`, while for keys every NaN
  #   is one value (SQL, Polars and pandas all agree). `#canonical` normalizes
  #   float key columns first, exactly as Polars' `canonical_f64` does, and also
  #   passes them to `group_by`, so every keyed verb agrees on what one key is.
  #   A dictionary column is keyed through its *values*, so its values are
  #   canonicalized in place and the dictionary dtype (and its keys) survive.
  # - **Nulls.** A null encodes like a value, i.e. all nulls are one key. That is
  #   what `distinct` and `group_by` want; a join that must *not* match nulls
  #   (`nulls_equal: false`) checks the key columns' null bitmaps instead.
  #
  # Nested keys are handled by rebuilding the containers: a struct, list,
  # fixed-size list or map whose child changed is re-created around the
  # canonicalized child, keeping offsets, nulls and dtype. Children that need no
  # change come back as the same object, so a key with no float children copies
  # nothing. List-view keys are still not covered (their constructor is not
  # ported in a form that preserves every buffer).
  module RowKeys
    extend self

    # Canonical quiet-NaN bit patterns and the sign bits of the three float
    # widths (`totalOrder` would otherwise keep payloads and signed zero apart).
    F16_SIGN          = 0x8000_u16
    F16_EXPONENT      = 0x7C00_u16
    F16_MANTISSA      = 0x03FF_u16
    F16_CANONICAL_NAN = 0x7E00_u16

    F32_SIGN          = 0x8000_0000_u32
    F32_EXPONENT      = 0x7F80_0000_u32
    F32_MANTISSA      = 0x007F_FFFF_u32
    F32_CANONICAL_NAN = 0x7FC0_0000_u32

    F64_SIGN          = 0x8000_0000_0000_0000_u64
    F64_EXPONENT      = 0x7FF0_0000_0000_0000_u64
    F64_MANTISSA      = 0x000F_FFFF_FFFF_FFFF_u64
    F64_CANONICAL_NAN = 0x7FF8_0000_0000_0000_u64

    # Key columns in canonical form: `-0.0` becomes `0.0` and every NaN becomes
    # one quiet NaN. Arrays of other dtypes (and float columns that already need
    # no change) are returned as-is, so the common path copies nothing.
    def canonical(arrays : Array(Arrow::ArrayBase)) : Array(Arrow::ArrayBase)
      arrays.map { |array| canonical(array) }
    end

    def canonical(array : Arrow::ArrayBase) : Arrow::ArrayBase
      # Widen first: the method is specialized per call-site type, and a
      # narrower one (a `NullArray`, say) has no float array to cast to.
      source = array.as(Arrow::ArrayBase)
      case source.data_type.kind
      when .float64?         then canonical_f64(source.as(Arrow::PrimitiveArray(Float64)))
      when .float32?         then canonical_f32(source.as(Arrow::PrimitiveArray(Float32)))
      when .float16?         then canonical_f16(source.as(Arrow::PrimitiveArray(Arrow::Float16)))
      when .dictionary?      then canonical_dictionary(source)
      when .struct?          then canonical_struct_array(source.as(Arrow::StructArray))
      when .list?            then canonical_list(source.as(Arrow::GenericListArray(Int32)))
      when .large_list?      then canonical_list(source.as(Arrow::GenericListArray(Int64)))
      when .fixed_size_list? then canonical_fixed_size_list(source.as(Arrow::FixedSizeListArray))
      when .map?             then canonical_map(source.as(Arrow::MapArray))
      else                        source
      end
    end

    # Dictionaries are keyed through their values, so canonicalize those. The
    # keys and the data type stay, which matters because callers such as
    # `Series#unique` gather rows from the canonical array and must keep the
    # original dtype.
    private def canonical_dictionary(array : Arrow::ArrayBase) : Arrow::ArrayBase
      case array.data_type.key.try(&.kind)
      when Arrow::DataType::Kind::Int8   then canonical_dictionary(array.as(Arrow::DictionaryArray(Int8)))
      when Arrow::DataType::Kind::Int16  then canonical_dictionary(array.as(Arrow::DictionaryArray(Int16)))
      when Arrow::DataType::Kind::Int32  then canonical_dictionary(array.as(Arrow::DictionaryArray(Int32)))
      when Arrow::DataType::Kind::Int64  then canonical_dictionary(array.as(Arrow::DictionaryArray(Int64)))
      when Arrow::DataType::Kind::UInt8  then canonical_dictionary(array.as(Arrow::DictionaryArray(UInt8)))
      when Arrow::DataType::Kind::UInt16 then canonical_dictionary(array.as(Arrow::DictionaryArray(UInt16)))
      when Arrow::DataType::Kind::UInt32 then canonical_dictionary(array.as(Arrow::DictionaryArray(UInt32)))
      when Arrow::DataType::Kind::UInt64 then canonical_dictionary(array.as(Arrow::DictionaryArray(UInt64)))
      else                                    array
      end
    end

    private def canonical_dictionary(array : Arrow::DictionaryArray(K)) : Arrow::ArrayBase forall K
      values = canonical(array.values)
      return array if values.same?(array.values)

      Arrow::DictionaryArray(K).new(array.data_type, array.keys, values, array.ordered?)
    end

    # Rebuilds a struct only when a child changed, so a struct key without float
    # children costs one identity check per child.
    private def canonical_struct_array(array : Arrow::StructArray) : Arrow::StructArray
      columns = array.columns
      children = columns.map { |child| canonical(child) }
      changed = children.each_with_index.any? do |child, index|
        !child.same?(columns[index])
      end
      return array if changed == false

      Arrow::StructArray.new(array.fields, children, array.nulls)
    end

    # Contract for these helpers: bind the child once, because `#values` is not
    # guaranteed to hand back the same object on a second call (comparing against
    # a fresh call reports "changed" for an untouched child), and write the guard
    # as an `if` block — the `return x unless cond` modifier form returned in this
    # generic method after `cond` had already evaluated false.
    private def canonical_list(
      array : Arrow::GenericListArray(O),
    ) : Arrow::ArrayBase forall O
      original = array.values
      values = canonical(original)
      return array if values.same?(original)

      field = array.data_type.field
      return array unless field

      Arrow::GenericListArray(O).new(field, array.offsets, values, array.nulls)
    end

    private def canonical_fixed_size_list(
      array : Arrow::FixedSizeListArray,
    ) : Arrow::ArrayBase
      original = array.values
      values = canonical(original)
      return array if values.same?(original)

      field = array.data_type.field
      size = array.data_type.size
      return array unless field && size

      Arrow::FixedSizeListArray.new(field, size, values, array.nulls)
    end

    private def canonical_map(array : Arrow::MapArray) : Arrow::ArrayBase
      original = array.entries
      entries = canonical_struct_array(original)
      return array if entries.same?(original)

      field = array.data_type.field
      return array unless field

      Arrow::MapArray.new(
        field,
        array.offsets,
        entries,
        array.nulls,
        array.data_type.is_sorted || false,
      )
    end

    # A converter for `arrays`, one key column each.
    #
    # `names` is only used to name the offending columns if `arrow-row` cannot
    # encode a dtype (dictionaries of nested values are the one gap upstream
    # leaves open), which raises `Prism::DtypeError` rather than an opaque
    # `Arrow::ArrowError`.
    def converter(
      arrays : Array(Arrow::ArrayBase),
      names : Array(String),
    ) : Arrow::RowConverter
      fields = arrays.map { |array| Arrow::SortField.new(array.data_type) }
      unless Arrow::RowConverter.supports_fields?(fields)
        dtypes = arrays.map(&.data_type.to_s).uniq!.join(", ")
        raise DtypeError.new(
          "row keys #{names.join(", ")} cannot be encoded (unsupported dtype: #{dtypes})"
        )
      end

      Arrow::RowConverter.new(fields)
    end

    # Null bitmaps for the columns that actually carry nulls, so a per-row
    # completeness check costs nothing when the keys have no nulls.
    def null_guards(arrays : Array(Arrow::ArrayBase)) : Array(Arrow::NullBuffer)
      guards = [] of Arrow::NullBuffer
      arrays.each do |array|
        nulls = array.nulls
        guards << nulls if nulls
      end
      guards
    end

    # True when `row` has a value in every guarded column.
    def complete?(guards : Array(Arrow::NullBuffer), row : Int32) : Bool
      guards.all?(&.valid?(row))
    end

    # Row indices of the first occurrence of each distinct key, in input order.
    def first_occurrences(rows : Arrow::Rows) : Array(UInt32)
      seen = Set(Arrow::Row).new(rows.num_rows)
      kept = [] of UInt32
      rows.num_rows.times do |row|
        next unless seen.add?(rows.row(row))

        kept << row.to_u32
      end
      kept
    end

    # Number of distinct keys, counting the null key if there is one.
    def distinct_count(rows : Arrow::Rows) : Int32
      seen = Set(Arrow::Row).new(rows.num_rows)
      rows.num_rows.times { |row| seen.add?(rows.row(row)) }
      seen.size
    end

    # --- float canonicalization -------------------------------------------

    # Floats are normalized in two passes: scan for anything to change (no
    # allocation, and the common case of a clean column copies nothing), then
    # rebuild only when it found something.
    private def canonical_f64(
      array : Arrow::PrimitiveArray(Float64),
    ) : Arrow::PrimitiveArray(Float64)
      changed = false
      index = 0
      while index < array.len
        bits = array.values[index].unsafe_as(UInt64)
        if canonical_f64_bits(bits) != bits
          changed = true
          break
        end
        index += 1
      end
      return array unless changed

      values = Array(Float64).new(array.len) do |position|
        canonical_f64_bits(array.values[position].unsafe_as(UInt64)).unsafe_as(Float64)
      end
      Arrow::PrimitiveArray(Float64).new(
        Arrow::ScalarBuffer(Float64).from(values), array.nulls, array.data_type
      )
    end

    private def canonical_f32(
      array : Arrow::PrimitiveArray(Float32),
    ) : Arrow::PrimitiveArray(Float32)
      changed = false
      index = 0
      while index < array.len
        bits = array.values[index].unsafe_as(UInt32)
        if canonical_f32_bits(bits) != bits
          changed = true
          break
        end
        index += 1
      end
      return array unless changed

      values = Array(Float32).new(array.len) do |position|
        canonical_f32_bits(array.values[position].unsafe_as(UInt32)).unsafe_as(Float32)
      end
      Arrow::PrimitiveArray(Float32).new(
        Arrow::ScalarBuffer(Float32).from(values), array.nulls, array.data_type
      )
    end

    private def canonical_f16(
      array : Arrow::PrimitiveArray(Arrow::Float16),
    ) : Arrow::PrimitiveArray(Arrow::Float16)
      changed = false
      index = 0
      while index < array.len
        bits = array.values[index].bits
        if canonical_f16_bits(bits) != bits
          changed = true
          break
        end
        index += 1
      end
      return array unless changed

      values = Array(Arrow::Float16).new(array.len) do |position|
        Arrow::Float16.from_bits(canonical_f16_bits(array.values[position].bits))
      end
      Arrow::PrimitiveArray(Arrow::Float16).new(
        Arrow::ScalarBuffer(Arrow::Float16).from(values), array.nulls, array.data_type
      )
    end

    private def canonical_f64_bits(bits : UInt64) : UInt64
      return 0_u64 if bits == F64_SIGN
      return F64_CANONICAL_NAN if nan?(bits, F64_EXPONENT, F64_MANTISSA)

      bits
    end

    private def canonical_f32_bits(bits : UInt32) : UInt32
      return 0_u32 if bits == F32_SIGN
      return F32_CANONICAL_NAN if nan?(bits, F32_EXPONENT, F32_MANTISSA)

      bits
    end

    private def canonical_f16_bits(bits : UInt16) : UInt16
      return 0_u16 if bits == F16_SIGN
      return F16_CANONICAL_NAN if nan?(bits, F16_EXPONENT, F16_MANTISSA)

      bits
    end

    # A NaN has an all-ones exponent and a non-zero mantissa (the sign bit is
    # irrelevant: `-NaN` and `NaN` are the same key).
    private def nan?(bits, exponent, mantissa) : Bool
      (bits & exponent) == exponent && (bits & mantissa) != 0
    end
  end
end
