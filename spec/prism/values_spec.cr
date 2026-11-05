require "../spec_helper"

private def extract_consistently(array : Arrow::ArrayBase)
  extracted = Prism::Values.extract(array)
  per_index = (0...array.len).map { |i| Prism::Values.extract_at(array, i) }
  extracted.should eq(per_index)
  extracted
end

describe Prism::Values do
  describe ".extract" do
    it "extracts integers and keeps nulls as nil" do
      extract_consistently(Arrow::Int64Array.from_nullable([1_i64, nil, 3_i64]))
        .should eq([1_i64, nil, 3_i64])
    end

    it "extracts nulls as nil, not the physical zero slot" do
      values = extract_consistently(Arrow::Float64Array.from_nullable([1.5, nil, 0.0]))
      values.should eq([1.5, nil, 0.0])
      values[1].should be_nil
    end

    it "extracts booleans" do
      extract_consistently(Arrow::BooleanArray.from_nullable([true, nil, false]))
        .should eq([true, nil, false])
    end

    it "extracts strings" do
      extract_consistently(Arrow::StringArray.from_nullable(["a", nil, ""]))
        .should eq(["a", nil, ""])
    end

    it "extracts binary values as Bytes" do
      array = Arrow::BinaryArray.from_nullable([Bytes[1_u8, 2_u8], nil])
      extract_consistently(array).should eq([Bytes[1_u8, 2_u8], nil])
    end

    it "extracts a null array as all nil" do
      extract_consistently(Arrow::NullArray.new(3)).should eq([nil, nil, nil])
    end

    it "extracts timestamps as Time" do
      micros = 1_700_000_000_000_000_i64
      array = Arrow::PrimitiveArray(Int64).from_values([micros])
        .with_data_type(Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond))

      extracted = extract_consistently(array)
      extracted.size.should eq(1)
      extracted[0].should eq(Time.unix(1_700_000_000))
    end

    it "extracts year-month intervals as months" do
      # 14 months is one year and two months; the unit is not kept, so the value
      # is just the month count.
      array = Arrow::PrimitiveArray(Int32)
        .from_nullable([14, -1, nil])
        .with_data_type(Arrow::DataType.interval(Arrow::IntervalUnit::YearMonth))

      extract_consistently(array).should eq([
        Arrow::IntervalValue.new(14, 0, 0),
        Arrow::IntervalValue.new(-1, 0, 0),
        nil,
      ])
    end

    it "extracts day-time intervals as days plus nanoseconds" do
      packed = [
        Arrow.interval_day_time(3, 7_200_000),
        Arrow.interval_day_time(-1, -1_000),
      ]
      array = Arrow::PrimitiveArray(Int64)
        .from_values(packed)
        .with_data_type(Arrow::DataType.interval(Arrow::IntervalUnit::DayTime))

      extract_consistently(array).should eq([
        Arrow::IntervalValue.new(0, 3, 7_200_000_i64 * 1_000_000_i64),
        Arrow::IntervalValue.new(0, -1, -1_000_i64 * 1_000_000_i64),
      ])
    end

    it "extracts month-day-nano intervals unchanged" do
      packed = [
        Arrow.interval_month_day_nano(1, 2, 3),
        Arrow.interval_month_day_nano(0, 0, 0),
        Arrow.interval_month_day_nano(-1, -2, -3),
      ]
      array = Arrow::PrimitiveArray(Int128)
        .from_values(packed)
        .with_data_type(Arrow::DataType.interval(Arrow::IntervalUnit::MonthDayNano))

      extract_consistently(array).should eq([
        Arrow::IntervalValue.new(1, 2, 3),
        Arrow::IntervalValue.new(0, 0, 0),
        Arrow::IntervalValue.new(-1, -2, -3),
      ])
    end

    it "raises for unsupported dtypes" do
      # Unions have no canonical Crystal representation; they are the remaining
      # gap now that nested values and intervals materialize.
      builder = Arrow::UnionBuilder.new_dense
      builder.append("a", 1)
      builder.append("b", 2)
      union = builder.build

      expect_raises(Prism::UnsupportedOperation, /Union/) do
        Prism::Values.extract(union)
      end
    end

    it "extracts narrow numeric types" do
      extract_consistently(Arrow::PrimitiveArray(Int8).from_values([1_i8, 2_i8]))
        .should eq([1_i8, 2_i8])
      extract_consistently(Arrow::PrimitiveArray(UInt8).from_values([3_u8]))
        .should eq([3_u8])
      extract_consistently(Arrow::PrimitiveArray(Float32).from_values([1.5_f32]))
        .should eq([1.5_f32])
    end

    it "extracts Float16" do
      half = Arrow::Float16.from_f32(1.5_f32)

      extract_consistently(Arrow::PrimitiveArray(Arrow::Float16).from_values([half]))
        .should eq([half])
    end

    # These four can be built, stored, filtered, sorted, grouped and written to
    # Parquet/IPC, but had no `Values` case — so `to_a`, `rows` and `to_s` raised
    # `UnsupportedOperation` for a frame that was otherwise complete.
    it "extracts a Utf8View column" do
      extract_consistently(Arrow::StringViewArray.from_nullable(["b", nil, "a"]))
        .should eq(["b", nil, "a"])
    end

    it "extracts a BinaryView column" do
      extract_consistently(Arrow::BinaryViewArray.from_nullable([Bytes[1_u8, 2_u8], nil]))
        .should eq([Bytes[1_u8, 2_u8], nil])
    end

    it "extracts a run-end encoded column as its logical values" do
      # runs: two 5s, then one null — three logical rows from two runs.
      array = Arrow::RunArray(Int32).try_new(
        Arrow::PrimitiveArray(Int32).from_values([2_i32, 3_i32]),
        Arrow::PrimitiveArray(Int64).from_nullable([5_i64, nil]),
      )

      extract_consistently(array).should eq([5_i64, 5_i64, nil])
    end

    it "extracts a Decimal256 column at its scale" do
      array = Arrow::PrimitiveArray(Arrow::I256)
        .from_values([Arrow::I256.from_i128(123_i128), Arrow::I256.from_i128(-456_i128)])
        .with_data_type(Arrow::DataType.decimal256(40, 2))

      extract_consistently(array)
        .should eq([BigDecimal.new("1.23"), BigDecimal.new("-4.56")])
    end

    it "extracts them as nested values too, which goes through extract_at" do
      fields = Arrow::Fields.from([
        Arrow::Field.new("s", Arrow::DataType.utf8_view, true),
        Arrow::Field.new("b", Arrow::DataType.binary_view, true),
        Arrow::Field.new("d", Arrow::DataType.decimal256(40, 2), true),
        Arrow::Field.new("r", Arrow::DataType.run_end_encoded(
          Arrow::Field.new("run_ends", Arrow::DataType.int32, false),
          Arrow::Field.new("values", Arrow::DataType.int64, true),
        ), true),
      ])
      record = Arrow::StructArray.new(
        fields,
        [
          Arrow::StringViewArray.from_values(["a"]).as(Arrow::ArrayBase),
          Arrow::BinaryViewArray.from_values([Bytes[1_u8]]).as(Arrow::ArrayBase),
          Arrow::PrimitiveArray(Arrow::I256).from_values([Arrow::I256.from_i128(123_i128)])
            .with_data_type(Arrow::DataType.decimal256(40, 2)).as(Arrow::ArrayBase),
          Arrow::RunArray(Int32).try_new(
            Arrow::PrimitiveArray(Int32).from_values([1_i32]),
            Arrow::PrimitiveArray(Int64).from_values([5_i64]),
          ).as(Arrow::ArrayBase),
        ],
        nil,
      )

      extract_consistently(record).should eq([{
        "s" => "a",
        "b" => Bytes[1_u8],
        "d" => BigDecimal.new("1.23"),
        "r" => 5_i64,
      }])
    end
  end

  describe ".extract_at" do
    it "returns nil for a null slot and for an out-of-range index" do
      array = Arrow::Float64Array.from_nullable([1.0, nil])

      Prism::Values.extract_at(array, 0).should eq(1.0)
      Prism::Values.extract_at(array, 1).should be_nil
      Prism::Values.extract_at(array, 2).should be_nil
    end
  end
end
