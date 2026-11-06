require "../spec_helper"

include Prism::DSL

# Expressions that wrap kernels `arrow-cr` already has. The point of this file is
# that there is no Prism-side loop: each node forwards to one `Arrow.*` kernel, so
# the null handling, dtype dispatch (dictionary, view) and offsets are the
# dependency's.
private def strings_frame : Prism::DataFrame
  Prism.from_columns({
    title: Arrow::StringArray.from_nullable(["open library", nil, "the hobbit"]).as(Arrow::ArrayBase),
    n:     Arrow::Int64Array.from_values([3_i64, 1_i64, 2_i64]).as(Arrow::ArrayBase),
  })
end

private def times_frame : Prism::DataFrame
  # 2023-11-14T22:13:20Z, so every part has a distinct value to check.
  micros = 1_700_000_000_000_000_i64
  Prism.from_columns({
    ts: Arrow::PrimitiveArray(Int64).from_nullable([micros, nil])
      .with_data_type(Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond)).as(Arrow::ArrayBase),
    date: Arrow::PrimitiveArray(Int32).from_nullable([19_675_i32, nil])
      .with_data_type(Arrow::DataType.date32).as(Arrow::ArrayBase),
  })
end

private def eager_strings(frame : Prism::DataFrame, expr : Prism::Expr) : Array(Prism::Value)
  frame.with_column("out", expr)["out"].to_a
end

describe "string kernels" do
  it "reports byte length, keeping nulls" do
    frame = strings_frame

    eager_strings(frame, col("title").str_length).should eq([12_i32, nil, 10_i32])
    frame.with_column("len", col("title").str_length).dtypes["len"].should eq(Arrow::DataType.int32)
  end

  it "is Int64 for a large-utf8 column, where the kernel is" do
    frame = Prism.from_columns({v: Arrow::LargeStringArray.from_values(["abc"]).as(Arrow::ArrayBase)})

    frame.with_column("len", col("v").str_length).dtypes["len"].should eq(Arrow::DataType.int64)
  end

  it "tests contains, starts-with and ends-with" do
    frame = strings_frame

    eager_strings(frame, col("title").str_contains("open")).should eq([true, nil, false])
    eager_strings(frame, col("title").str_starts_with("open")).should eq([true, nil, false])
    eager_strings(frame, col("title").str_ends_with("ry")).should eq([true, nil, false])
    frame.with_column("hit", col("title").str_contains("open")).dtypes["hit"].should eq(Arrow::DataType.boolean)
  end

  it "slices by byte offset, with and without a length" do
    frame = strings_frame

    eager_strings(frame, col("title").str_slice(0, 4)).should eq(["open", nil, "the "])
    eager_strings(frame, col("title").str_slice(5)).should eq(["library", nil, "obbit"])
    frame.with_column("head", col("title").str_slice(0, 4)).dtypes["head"].should eq(Arrow::DataType.utf8)
  end

  it "matches a regular expression" do
    frame = strings_frame

    eager_strings(frame, col("title").matches("^the")).should eq([false, nil, true])
    eager_strings(frame, col("title").matches("hobb.t")).should eq([false, nil, true])
  end

  it "matches a dictionary column through its values" do
    frame = Prism.from_columns({v: Arrow::DictionaryArray(Int32).from_options(["open library", "the hobbit"]).as(Arrow::ArrayBase)})

    eager_strings(frame, col("v").str_contains("hobbit")).should eq([false, true])
  end

  it "changes case with the UTF-8 kernels, not the ASCII ones" do
    frame = strings_frame

    # "héllo" -> "HÉLLO" proves the unicode kernel: an ascii-only transform would
    # leave the accent alone.
    accented = Prism.from_columns({v: Arrow::StringArray.from_nullable(["héllo", nil]).as(Arrow::ArrayBase)})

    eager_strings(accented, col("v").str_to_uppercase).should eq(["HÉLLO", nil])
    eager_strings(accented, col("v").str_to_lowercase).should eq(["héllo", nil])
    eager_strings(frame, col("title").str_to_uppercase).should eq(["OPEN LIBRARY", nil, "THE HOBBIT"])
    frame.with_column("up", col("title").str_to_uppercase).dtypes["up"].should eq(Arrow::DataType.utf8)
  end

  it "trims whitespace from both ends, or one" do
    frame = Prism.from_columns({v: Arrow::StringArray.from_nullable(["  a b  ", nil, "c"]).as(Arrow::ArrayBase)})

    eager_strings(frame, col("v").str_trim).should eq(["a b", nil, "c"])
    eager_strings(frame, col("v").str_trim_start).should eq(["a b  ", nil, "c"])
    eager_strings(frame, col("v").str_trim_end).should eq(["  a b", nil, "c"])
  end

  it "keeps a dictionary's values through a transform" do
    frame = Prism.from_columns({v: Arrow::DictionaryArray(Int32).from_options(["ab", "cd"]).as(Arrow::ArrayBase)})

    eager_strings(frame, col("v").str_to_uppercase).should eq(["AB", "CD"])
  end

  it "rejects a non-string operand for a transform" do
    frame = strings_frame

    expect_raises(Prism::DtypeError, /"n".*Int64/) do
      frame.with_column("x", col("n").str_to_uppercase)
    end
  end

  it "names the column and dtype it cannot use" do
    frame = strings_frame

    expect_raises(Prism::DtypeError, /"n".*Int64/) do
      frame.with_column("x", col("n").str_length)
    end

    expect_raises(Prism::DtypeError, /"n".*Int64/) do
      frame.with_column("x", col("n").str_contains("1"))
    end
  end
end

describe "temporal kernels" do
  it "reads the calendar parts of a timestamp and a date" do
    frame = times_frame

    ts = {
      "year"        => col("ts").year,
      "quarter"     => col("ts").quarter,
      "month"       => col("ts").month,
      "week"        => col("ts").week,
      "day"         => col("ts").day,
      "day_of_year" => col("ts").day_of_year,
      "weekday"     => col("ts").weekday,
      "hour"        => col("ts").hour,
      "minute"      => col("ts").minute,
      "second"      => col("ts").second,
    }

    expected = {
      "year"        => 2023,
      "quarter"     => 4,
      "month"       => 11,
      "week"        => 46,
      "day"         => 14,
      "day_of_year" => 318,
      "weekday"     => 2, # Tuesday, Monday = 1
      "hour"        => 22,
      "minute"      => 13,
      "second"      => 20,
    }

    ts.each do |name, expr|
      values = eager_strings(frame, expr)
      values[0].should eq(expected[name]), "ts.#{name}"
      values[1].should be_nil
      values.size.should eq(2)
    end

    frame.with_column("y", col("ts").year).dtypes["y"].should eq(Arrow::DataType.int32)
  end

  it "reads a date32 column" do
    frame = times_frame

    eager_strings(frame, col("date").year).should eq([2023, nil])
    eager_strings(frame, col("date").month).should eq([11, nil])
    eager_strings(frame, col("date").day).should eq([14, nil])
  end

  it "rejects a column that is not temporal" do
    frame = strings_frame

    expect_raises(Prism::DtypeError, /"n".*Int64/) do
      frame.with_column("y", col("n").year)
    end
  end
end

describe "positional kernels" do
  it "shifts values and fills the boundary with nulls" do
    frame = strings_frame

    shifted = frame.with_column("prev", col("n").shift(1))
    shifted["prev"].to_a.should eq([nil, 3_i64, 1_i64])
    shifted.schema.field_with_name("prev").nullable?.should be_true
  end

  it "shifts backwards and by nothing" do
    frame = strings_frame

    frame.with_column("next", col("n").shift(-1))["next"].to_a.should eq([1_i64, 2_i64, nil])
    same = frame.with_column("same", col("n").shift(0))
    same["same"].to_a.should eq([3_i64, 1_i64, 2_i64])
    same.schema.field_with_name("same").nullable?.should be_false
  end

  it "diffs against a shift" do
    frame = strings_frame

    frame.with_column("d", col("n").diff)["d"].to_a.should eq([nil, -2_i64, 1_i64])
    frame.with_column("d2", col("n").diff(2))["d2"].to_a.should eq([nil, nil, -1_i64])
  end

  it "ranks with ties sharing the highest rank" do
    frame = Prism.from_columns({v: Arrow::Int64Array.from_nullable([5_i64, 5_i64, 1_i64, nil]).as(Arrow::ArrayBase)})

    ranked = frame.with_column("r", col("v").rank)
    ranked.dtypes["r"].should eq(Arrow::DataType.uint32)
    # nulls last by default: 1 -> 1, the 5s share rank 3 (highest of 2 and 3),
    # the null takes 4.
    ranked["r"].to_a.should eq([3_u32, 3_u32, 1_u32, 4_u32])
    ranked.schema.field_with_name("r").nullable?.should be_false
  end

  it "ranks descending" do
    frame = Prism.from_columns({v: Arrow::Int64Array.from_values([10_i64, 30_i64, 20_i64]).as(Arrow::ArrayBase)})

    frame.with_column("r", col("v").rank(descending: true))["r"].to_a.should eq([3_u32, 1_u32, 2_u32])
  end

  it "rejects a dtype the rank kernel cannot order" do
    frame = Prism.from_columns({
      v: Arrow::PrimitiveArray(Int128).from_values([123_i128]).with_data_type(Arrow::DataType.decimal128(10, 2)).as(Arrow::ArrayBase),
    })

    expect_raises(Prism::DtypeError, /decimal/i) do
      frame.with_column("r", col("v").rank)
    end
  end
end

describe "kernel expressions through the plan" do
  it "gives the same frame and schema lazily" do
    frame = strings_frame

    expressions = {
      "len"   => col("title").str_length,
      "hit"   => col("title").str_contains("open"),
      "start" => col("title").str_starts_with("open"),
      "end"   => col("title").str_ends_with("ry"),
      "slice" => col("title").str_slice(0, 4),
      "regex" => col("title").matches("^the"),
      "prev"  => col("n").shift(1),
      "diff"  => col("n").diff,
      "rank"  => col("n").rank,
    }

    expressions.each do |name, expr|
      eager = frame.with_column(name, expr)
      lazy = frame.lazy.with_column(name, expr).collect

      lazy.schema.should eq(eager.schema), "#{name}: schema"
      lazy.should eq(eager), "#{name}: frame"
    end

    # Two kernel expressions in one pipeline, so a later node cannot capture an
    # earlier one's schema.
    pipeline = frame.with_column("y", col("title").str_length).with_column("k", col("n") + lit(1_i64))
    frame.lazy
      .with_column("y", col("title").str_length)
      .with_column("k", col("n") + lit(1_i64))
      .collect
      .should eq(pipeline)
  end

  it "keeps one identity per argument" do
    col("t").str_contains("a").same_as?(col("t").str_contains("b")).should be_false
    col("t").str_contains("a").same_as?(col("t").str_contains("a")).should be_true
    col("t").str_slice(0, 2).same_as?(col("t").str_slice(0, 3)).should be_false
    col("t").shift(1).same_as?(col("t").shift(2)).should be_false
    col("t").year.same_as?(col("t").month).should be_false
    col("t").rank.same_as?(col("t").rank(descending: true)).should be_false
  end
end
