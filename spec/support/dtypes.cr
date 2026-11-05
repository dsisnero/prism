require "./frames"

# One column per dtype Prism can hold, each with a null in the middle so a
# round-trip has to preserve the validity bitmap as well as the values.
#
# This is the IO surface's version of the seeded property frames: every format
# spec reads it, so a dtype a writer or reader mishandles fails in one place.
# Time, interval and nested types are built the way `arrow-cr` spells them (a
# primitive array plus `.with_data_type`), because it has no typed aliases.
module SpecDtypes
  def self.columns : Array(Tuple(String, Arrow::ArrayBase))
    [
      {"bool", Arrow::BooleanArray.from_nullable([true, nil, false]).as(Arrow::ArrayBase)},
      {"i8", Arrow::PrimitiveArray(Int8).from_nullable([1_i8, nil, -3_i8]).as(Arrow::ArrayBase)},
      {"i16", Arrow::PrimitiveArray(Int16).from_nullable([1_i16, nil, -3_i16]).as(Arrow::ArrayBase)},
      {"i32", Arrow::PrimitiveArray(Int32).from_nullable([1_i32, nil, -3_i32]).as(Arrow::ArrayBase)},
      {"i64", Arrow::PrimitiveArray(Int64).from_nullable([1_i64, nil, -3_i64]).as(Arrow::ArrayBase)},
      {"u8", Arrow::PrimitiveArray(UInt8).from_nullable([1_u8, nil, 3_u8]).as(Arrow::ArrayBase)},
      {"u16", Arrow::PrimitiveArray(UInt16).from_nullable([1_u16, nil, 3_u16]).as(Arrow::ArrayBase)},
      {"u32", Arrow::PrimitiveArray(UInt32).from_nullable([1_u32, nil, 3_u32]).as(Arrow::ArrayBase)},
      {"u64", Arrow::PrimitiveArray(UInt64).from_nullable([1_u64, nil, 3_u64]).as(Arrow::ArrayBase)},
      {"f32", Arrow::PrimitiveArray(Float32).from_nullable([1.5_f32, nil, -2.5_f32]).as(Arrow::ArrayBase)},
      {"f64", Arrow::PrimitiveArray(Float64).from_nullable([1.5, nil, -2.5]).as(Arrow::ArrayBase)},
      {"utf8", Arrow::StringArray.from_nullable(["a", nil, "cc"]).as(Arrow::ArrayBase)},
      {"utf8_view", Arrow::StringViewArray.from_nullable(["a", nil, "cc"]).as(Arrow::ArrayBase)},
      {"large_utf8", Arrow::LargeStringArray.from_nullable(["a", nil, "cc"]).as(Arrow::ArrayBase)},
      {"binary", Arrow::BinaryArray.from_nullable([Bytes[1_u8], nil, Bytes[2_u8, 3_u8]]).as(Arrow::ArrayBase)},
      {"binary_view", Arrow::BinaryViewArray.from_nullable([Bytes[1_u8], nil, Bytes[2_u8, 3_u8]]).as(Arrow::ArrayBase)},
      {"date32", Arrow::PrimitiveArray(Int32).from_nullable([0_i32, nil, 18_000_i32]).with_data_type(Arrow::DataType.date32).as(Arrow::ArrayBase)},
      {"date64", Arrow::PrimitiveArray(Int64).from_nullable([0_i64, nil, 86_400_000_i64]).with_data_type(Arrow::DataType.date64).as(Arrow::ArrayBase)},
      {"time32", Arrow::PrimitiveArray(Int32).from_nullable([0_i32, nil, 3_600_i32]).with_data_type(Arrow::DataType.time32(Arrow::TimeUnit::Millisecond)).as(Arrow::ArrayBase)},
      {"time64", Arrow::PrimitiveArray(Int64).from_nullable([0_i64, nil, 3_600_000_i64]).with_data_type(Arrow::DataType.time64(Arrow::TimeUnit::Microsecond)).as(Arrow::ArrayBase)},
      {"timestamp", Arrow::PrimitiveArray(Int64).from_nullable([0_i64, nil, 1_700_000_000_000_000_i64]).with_data_type(Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond)).as(Arrow::ArrayBase)},
      {"timestamp_tz", Arrow::PrimitiveArray(Int64).from_nullable([0_i64, nil, 1_700_000_000_000_000_i64]).with_data_type(Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond, "UTC")).as(Arrow::ArrayBase)},
      {"duration", Arrow::PrimitiveArray(Int64).from_nullable([0_i64, nil, 2_000_i64]).with_data_type(Arrow::DataType.duration(Arrow::TimeUnit::Millisecond)).as(Arrow::ArrayBase)},
      {"interval_ym", Arrow::PrimitiveArray(Int32).from_nullable([0_i32, nil, 14_i32]).with_data_type(Arrow::DataType.interval(Arrow::IntervalUnit::YearMonth)).as(Arrow::ArrayBase)},
      {"decimal128", Arrow::PrimitiveArray(Int128).from_nullable([123_i128, nil, -456_i128]).with_data_type(Arrow::DataType.decimal128(10, 2)).as(Arrow::ArrayBase)},
      {"decimal256", Arrow::PrimitiveArray(Arrow::I256).from_nullable([Arrow::I256.from_i128(123_i128), nil, Arrow::I256.from_i128(-456_i128)]).with_data_type(Arrow::DataType.decimal256(40, 2)).as(Arrow::ArrayBase)},
      {"null", Arrow::NullArray.new(3).as(Arrow::ArrayBase)},
      {"dict", Arrow::DictionaryArray(Int32).from_options(["x", "y", nil]).as(Arrow::ArrayBase)},
      {"list", Arrow::ListArray.from_iter_primitive(Int64, [[1_i64, 2_i64], nil, [3_i64]]).as(Arrow::ArrayBase)},
      {"fixed_list", Arrow::FixedSizeListArray.from_iter_primitive(Int64, [[1_i64, 2_i64], nil, [3_i64, 4_i64]], 2).as(Arrow::ArrayBase)},
      {"struct", Arrow::StructArray.new(
        Arrow::Fields.from([Arrow::Field.new("s", Arrow::DataType.utf8, true)]),
        [Arrow::StringArray.from_nullable(["a", nil, "b"]).as(Arrow::ArrayBase)],
        Arrow::NullBuffer.from_slice([true, true, false]),
      ).as(Arrow::ArrayBase)},
      {"map", Arrow::MapArray.from_vec_of_maps(
        [[{"a", 1_i64.as(Int64?)}, {"b", 2_i64.as(Int64?)}], nil, [{"c", 3_i64.as(Int64?)}]] of Array(Tuple(String, Int64?))?,
        false,
      ).as(Arrow::ArrayBase)},
    ]
  end

  # Run-end encoded columns are deliberately absent: Parquet has no encoding for
  # them, so a write widens the column to its values dtype (`Int64`) and the
  # round-trip is lossy by the format's rule, not Prism's. IPC carries it exactly;
  # the value path is covered in `spec/prism/values_spec.cr`.
  #
  # A one-column frame for a dtype, so a spec can round-trip a single kind.
  def self.frame(array : Arrow::ArrayBase) : Prism::DataFrame
    Prism.from_columns({v: array})
  end
end
