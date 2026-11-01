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

    it "raises for unsupported dtypes" do
      decimal = Arrow::PrimitiveArray(Int32).from_values([100])
        .with_data_type(Arrow::DataType.decimal128(10, 2))
        .as(Arrow::ArrayBase)

      expect_raises(Prism::UnsupportedOperation, /Decimal128/) do
        Prism::Values.extract(decimal)
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
