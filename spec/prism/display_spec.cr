require "../spec_helper"

private def display_frame : Prism::DataFrame
  Prism.from_columns({
    region: Arrow::StringArray.from_nullable(["east", nil, "west"]),
    price:  Arrow::Float64Array.from_nullable([10.0, 20.0, nil]),
    qty:    Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64]),
  })
end

describe Prism::Display do
  describe "DataFrame#to_s" do
    it "renders shape, header, dtypes and rows as an aligned table" do
      display_frame.to_s.should eq(<<-TABLE.chomp)
        shape: (3, 3)
        region | price | qty
        ------ | ----- | ---
        str    | f64   | i64
        east   | 10.0  | 1
        null   | 20.0  | 2
        west   | null  | 3
        TABLE
    end

    it "renders an empty frame without body rows" do
      empty = display_frame.filter(Prism.col("qty") > Prism.lit(100_i64))

      empty.to_s.should eq(<<-TABLE.chomp)
        shape: (0, 3)
        region | price | qty
        ------ | ----- | ---
        str    | f64   | i64
        TABLE
    end

    it "uses the same text for inspect" do
      display_frame.inspect.should eq(display_frame.to_s)
    end
  end

  describe "truncation" do
    it "keeps head and tail rows with an ellipsis between them" do
      frame = Prism.from_columns({
        n: Arrow::Int64Array.from_values((0_i64...20_i64).to_a),
      })

      rendered = frame.to_s(max_rows: 6)
      lines = rendered.lines

      # shape + header + separator + dtypes + 3 head + ellipsis + 3 tail
      lines.size.should eq(11)
      lines[4].strip.should eq("0")
      lines[5].strip.should eq("1")
      lines[6].strip.should eq("2")
      lines[7].should contain("...")
      lines[8].strip.should eq("17")
      lines[10].strip.should eq("19")
    end

    it "keeps leading and trailing columns with an ellipsis between them" do
      columns = {} of String => Arrow::ArrayBase
      12.times { |i| columns["c#{i}"] = Arrow::Int64Array.from_values([i.to_i64]) }

      rendered = Prism.from_columns(columns).to_s(max_cols: 4)
      header = rendered.lines[1]

      header.should contain("c0")
      header.should contain("c1")
      header.should contain("...")
      header.should contain("c10")
      header.should contain("c11")
      header.includes?("c2").should be_false
    end

    it "truncates long strings with an ellipsis" do
      frame = Prism.from_columns({
        text: Arrow::StringArray.from_values(["abcdefghijklmnopqrstuvwxyz"]),
      })

      rendered = frame.to_s(max_string: 10)

      rendered.should contain("abcdefg...")
      rendered.includes?("hijklmnop").should be_false
    end

    it "rejects limits that cannot render" do
      expect_raises(ArgumentError) { display_frame.to_s(max_rows: 0) }
      expect_raises(ArgumentError) { display_frame.to_s(max_cols: 0) }
      expect_raises(ArgumentError) { display_frame.to_s(max_string: 3) }
    end
  end

  describe "value formatting" do
    it "renders booleans, floats, NaN and infinity without ambiguity" do
      frame = Prism.from_columns({
        flag:  Arrow::BooleanArray.from_values([true, false]),
        price: Arrow::Float64Array.from_values([
          Float64::NAN,
          Float64::INFINITY,
        ]),
      })

      rendered = frame.to_s

      rendered.should contain("true")
      rendered.should contain("false")
      rendered.should contain("NaN")
      rendered.should contain("Infinity")
    end

    it "renders timestamps as ISO 8601" do
      stamps = Arrow::PrimitiveArray(Int64)
        .from_values([1_700_000_000_000_000_i64])
        .with_data_type(Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond))
      frame = Prism.from_columns({at: stamps})

      frame.to_s.should contain("2023-11-14")
    end

    it "renders intervals as months, days and nanoseconds" do
      spans = Arrow::PrimitiveArray(Int128)
        .from_values([
          Arrow.interval_month_day_nano(1, 2, 3),
          Arrow.interval_month_day_nano(0, 0, 0),
          Arrow.interval_month_day_nano(0, 0, 7_320_000_000_000),
        ])
        .with_data_type(Arrow::DataType.interval(Arrow::IntervalUnit::MonthDayNano))
      # A year-month interval keeps only its month count, so it reads as months.
      months = Arrow::PrimitiveArray(Int32)
        .from_nullable([14, nil, -13])
        .with_data_type(Arrow::DataType.interval(Arrow::IntervalUnit::YearMonth))
      frame = Prism.from_columns({span: spans, ym: months})

      rendered = frame.to_s

      rendered.should contain("1 mons 2 days 0.000000003 secs")
      rendered.should contain("0 secs")
      rendered.should contain("2 hours 2 mins")
      rendered.should contain("14 mons")
      rendered.should contain("-13 mons")
      rendered.should contain("null")
    end
  end
end
