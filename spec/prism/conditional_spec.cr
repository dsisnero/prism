require "../spec_helper"

include Prism::DSL

private def cond_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    # `nil` in the condition is a real case: Arrow's `zip` treats a null mask as
    # false, so the else branch wins (Polars' when/then/otherwise agrees).
    flag:  Arrow::BooleanArray.from_nullable([true, false, nil, true]),
    score: Arrow::Int64Array.from_nullable([10_i64, 20_i64, 30_i64, nil]),
    note:  Arrow::StringArray.from_nullable(["a", nil, "c", "d"]),
  })
end

private def cond_frame : Prism::DataFrame
  Prism.from_arrow(cond_batch)
end

# The same rows in two chunks: an element-wise expression must not care.
private def chunked_cond_frame : Prism::DataFrame
  Prism.from_batches([cond_batch.slice(0, 2), cond_batch.slice(2, 2)])
end

describe "Prism.if_else" do
  it "selects per row from two columns" do
    scored = cond_frame.with_column(
      "points",
      Prism.if_else(col("flag"), col("score"), lit(0_i64)),
    )

    scored["points"].to_a.should eq([10_i64, 0_i64, 0_i64, nil])
  end

  it "takes the else branch where the condition is null" do
    # Arrow's `zip` treats a null mask as false and Polars' when/then/otherwise
    # agrees, so this is the documented contract, not an accident of the kernel.
    picked = cond_frame.with_column(
      "label",
      Prism.if_else(col("flag"), lit("yes"), lit("no")),
    )

    picked["label"].to_a.should eq(["yes", "no", "no", "yes"])
  end

  it "keeps the nulls of the selected branch" do
    # Row 4 is selected and its score is null, so the result is null there.
    picked = cond_frame.with_column(
      "score_or_zero",
      Prism.if_else(col("flag"), col("score"), lit(0_i64)),
    )

    picked["score_or_zero"].to_a.should eq([10_i64, 0_i64, 0_i64, nil])
  end

  it "accepts plain scalars on either side" do
    filled = cond_frame.with_column(
      "note_or_dash",
      Prism.if_else(col("note").is_null, "-", col("note")),
    )

    filled["note_or_dash"].to_a.should eq(["a", "-", "c", "d"])
  end

  it "widens the branch dtypes instead of truncating" do
    widened = cond_frame.with_column(
      "mixed",
      Prism.if_else(col("flag"), col("score"), lit(1.5)),
    )

    widened["mixed"].dtype.should eq(Arrow::DataType.float64)
    widened["mixed"].to_a.should eq([10.0, 1.5, 1.5, nil])

    # A literal adopts the column's dtype when that cast is lossless, the same rule
    # arithmetic follows, so a narrow column is not widened for nothing.
    narrow = cond_frame.with_column(
      "small",
      Prism.if_else(col("flag"), col("score").cast(Arrow::DataType.int32), lit(7_i64)),
    )
    narrow["small"].dtype.should eq(Arrow::DataType.int32)
    narrow["small"].to_a.should eq([10, 7, 7, nil])
  end

  it "works inside filter and select" do
    # The last row's condition is true and its score is null, so the comparison
    # itself is null there and `filter` drops it — no special case needed.
    kept = cond_frame.filter(
      Prism.if_else(col("flag"), col("score"), lit(-1_i64)) > 5,
    )
    kept["score"].to_a.should eq([10_i64])

    selected = cond_frame.select(
      Prism.if_else(col("flag"), col("score"), lit(0_i64)).alias("points"),
    )
    selected.column_names.should eq(["points"])
    selected["points"].to_a.should eq([10_i64, 0_i64, 0_i64, nil])
  end

  it "nests as a multi-branch conditional" do
    graded = cond_frame.with_column(
      "grade",
      Prism.if_else(
        col("score") >= 25,
        lit("high"),
        Prism.if_else(col("score") < 15, lit("low"), lit("mid")),
      ),
    )

    # score 10 -> low, 20 -> mid, 30 -> high, null -> low? No: null compares
    # false both ways, so the null row falls to the inner else, "mid".
    graded["grade"].to_a.should eq(["low", "mid", "high", "mid"])
  end

  it "is element-wise across chunks" do
    picked = chunked_cond_frame.with_column(
      "points",
      Prism.if_else(col("flag"), col("score"), lit(0_i64)),
    )

    picked["points"].to_a.should eq([10_i64, 0_i64, 0_i64, nil])
    batch_count = 0
    picked.each_batch { batch_count += 1 }
    batch_count.should eq(2)
  end

  it "survives the lazy plan unchanged" do
    eager = cond_frame.with_column(
      "points",
      Prism.if_else(col("flag"), col("score"), lit(0_i64)),
    )
    lazy = cond_frame.lazy
      .with_column("points", Prism.if_else(col("flag"), col("score"), lit(0_i64)))
      .collect

    lazy["points"].to_a.should eq(eager["points"].to_a)
  end

  it "reports its dtype, key and columns without reading data" do
    expr = Prism.if_else(col("flag"), col("score"), lit(0_i64))

    expr.dtype(cond_batch.schema).should eq(Arrow::DataType.int64)
    expr.columns.should eq(["flag", "score"])
    expr.key.should eq("if_else(col:flag,col:score,lit:0)")
    expr.to_s.should eq("if_else(col:flag,col:score,lit:0)")
  end

  it "rejects a non-boolean condition" do
    expect_raises(Prism::DtypeError, /condition/) do
      cond_frame.with_column("bad", Prism.if_else(col("score"), lit(1), lit(2)))
    end
  end

  it "rejects branches that have no common dtype" do
    expect_raises(Prism::DtypeError, /cannot combine Int64 and Utf8/) do
      cond_frame.with_column("bad", Prism.if_else(col("flag"), col("score"), lit("x")))
    end
  end

  it "rejects a missing column with the column name" do
    expect_raises(Prism::ColumnNotFound, /nope/) do
      cond_frame.with_column("bad", Prism.if_else(col("nope"), lit(1), lit(2)))
    end
  end
end

describe "Prism.case_when" do
  it "takes the first matching branch and falls back to otherwise" do
    graded = cond_frame.with_column(
      "grade",
      Prism.case_when(
        [{col("score") >= 25, lit("high")}, {col("score") >= 15, lit("mid")}],
        lit("low"),
      ),
    )

    # Null scores match no condition, so they land on `otherwise`.
    graded["grade"].to_a.should eq(["low", "mid", "high", "low"])
  end

  it "keeps the first match when two conditions are true" do
    graded = cond_frame.with_column(
      "grade",
      Prism.case_when(
        [{col("score") >= 10, lit("any")}, {col("score") >= 25, lit("high")}],
        lit("none"),
      ),
    )

    graded["grade"].to_a.should eq(["any", "any", "any", "none"])
  end

  it "rejects an empty branch list" do
    expect_raises(ArgumentError, /branch/) do
      Prism.case_when([] of Tuple(Prism::Expr, Prism::Expr), lit("x"))
    end
  end
end
