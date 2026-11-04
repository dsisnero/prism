require "../spec_helper"

include Prism::DSL

private def join_left_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    id:    Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64]),
    name:  Arrow::StringArray.from_values(["a", "b", "c", "d"]),
    score: Arrow::Float64Array.from_values([1.0, 2.0, 3.0, 4.0]),
  })
end

# Right side repeats id 3, so joins must expand many-to-many.
private def join_right_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    id:    Arrow::Int64Array.from_values([2_i64, 3_i64, 3_i64, 5_i64]),
    label: Arrow::StringArray.from_values(["x", "y", "z", "w"]),
  })
end

private def join_left : Prism::DataFrame
  Prism.from_arrow(join_left_batch)
end

private def join_right : Prism::DataFrame
  Prism.from_arrow(join_right_batch)
end

describe Prism::DataFrame do
  describe "#join" do
    it "inner joins on a key" do
      joined = join_left.join(join_right, on: "id")

      joined.column_names.should eq(["id", "name", "score", "label"])
      joined.row_count.should eq(3)
      joined["id"].to_a.should eq([2_i64, 3_i64, 3_i64])
      joined["label"].to_a.should eq(["x", "y", "z"])
    end

    it "left joins, filling the right side with nulls" do
      joined = join_left.join(join_right, on: "id", how: :left)

      joined.row_count.should eq(5)
      joined["id"].to_a.should eq([1_i64, 2_i64, 3_i64, 3_i64, 4_i64])
      joined["label"].to_a.should eq([nil, "x", "y", "z", nil])
      joined.schema.field_with_name("label").nullable?.should be_true
    end

    it "right joins, filling the left side with nulls" do
      joined = join_left.join(join_right, on: "id", how: :right)

      joined.row_count.should eq(4)
      joined["id"].to_a.should eq([2_i64, 3_i64, 3_i64, 5_i64])
      joined["name"].to_a.should eq(["b", "c", "c", nil])
    end

    it "full joins both sides, keeping the right key column uncoalesced" do
      joined = join_left.join(join_right, on: "id", how: :full)

      joined.column_names.should eq(["id", "name", "score", "id_right", "label"])
      joined.row_count.should eq(6)
      joined["label"].to_a.should eq([nil, "x", "y", "z", nil, "w"])
      joined["id_right"].to_a.should eq([nil, 2_i64, 3_i64, 3_i64, nil, 5_i64])
    end

    it "coalesces the key column for a full join when asked" do
      joined = join_left.join(
        join_right,
        on: "id",
        how: :full,
        coalesce: true,
      )

      joined.column_names.should eq(["id", "name", "score", "label"])
      joined["id"].to_a.should eq([1_i64, 2_i64, 3_i64, 3_i64, 4_i64, 5_i64])
    end

    it "semi joins keep matching left rows only, with left columns only" do
      joined = join_left.join(join_right, on: "id", how: :semi)

      joined.column_names.should eq(["id", "name", "score"])
      joined["id"].to_a.should eq([2_i64, 3_i64])
    end

    it "anti joins keep non-matching left rows" do
      joined = join_left.join(join_right, on: "id", how: :anti)

      joined.column_names.should eq(["id", "name", "score"])
      joined["id"].to_a.should eq([1_i64, 4_i64])
    end

    it "keeps null keys unmatched, like SQL" do
      left = Prism.from_columns({id: Arrow::Int64Array.from_nullable([1_i64, nil])})
      right = Prism.from_columns({id: Arrow::Int64Array.from_nullable([nil, 1_i64])})

      left.join(right, on: "id")["id"].to_a.should eq([1_i64])

      with_nulls = left.join(right, on: "id", nulls_equal: true)
      with_nulls.row_count.should eq(2)
    end

    it "treats a null in any key of a composite join as unmatched" do
      left = Prism.from_columns({
        a: Arrow::Int64Array.from_values([1_i64, 1_i64]),
        b: Arrow::StringArray.from_nullable(["x", nil]),
      })
      right = Prism.from_columns({
        a: Arrow::Int64Array.from_values([1_i64, 1_i64]),
        b: Arrow::StringArray.from_nullable(["x", nil]),
      })

      left.join(right, on: ["a", "b"]).row_count.should eq(1)
      left.join(right, on: ["a", "b"], nulls_equal: true).row_count.should eq(2)
    end

    it "matches NaN keys, which the value-keyed path could not" do
      left = Prism.from_columns({
        f: Arrow::Float64Array.from_values([Float64::NAN, 1.0]),
      })
      right = Prism.from_columns({
        f: Arrow::Float64Array.from_values([Float64::NAN]),
      })

      joined = left.join(right, on: "f")

      joined.row_count.should eq(1)
      joined["f"].to_a.first.as(Float64).nan?.should be_true
    end

    it "matches signed zero keys, because -0.0 == 0.0" do
      left = Prism.from_columns({f: Arrow::Float64Array.from_values([0.0])})
      right = Prism.from_columns({f: Arrow::Float64Array.from_values([-0.0])})

      left.join(right, on: "f").row_count.should eq(1)
    end

    it "matches NaN keys across payloads" do
      signaling_nan = 0x7FF0_0000_0000_0001_u64.unsafe_as(Float64)

      left = Prism.from_columns({
        f: Arrow::Float64Array.from_values([Float64::NAN]),
      })
      right = Prism.from_columns({
        f: Arrow::Float64Array.from_values([signaling_nan]),
      })

      left.join(right, on: "f").row_count.should eq(1)
    end

    it "joins dictionary-encoded keys by value, not by key index" do
      # The two dictionaries intern their values in opposite orders.
      left_keys = Arrow::DictionaryArray(Int32)
        .from_values(["east", "west"])
        .as(Arrow::ArrayBase)
      right_keys = Arrow::DictionaryArray(Int32)
        .from_values(["west", "east", "west"])
        .as(Arrow::ArrayBase)

      left = Prism.from_columns({
        region: left_keys,
        l:      Arrow::Int64Array.from_values([1_i64, 2_i64]),
      })
      right = Prism.from_columns({
        region: right_keys,
        r:      Arrow::Int64Array.from_values([10_i64, 20_i64, 30_i64]),
      })

      joined = left.join(right, on: "region")

      joined["l"].to_a.should eq([1_i64, 2_i64, 2_i64])
      joined["r"].to_a.should eq([20_i64, 10_i64, 30_i64])
    end

    it "joins on differently named keys and keeps both key columns" do
      users = Prism.from_columns({
        user_id: Arrow::Int64Array.from_values([10_i64, 20_i64]),
        name:    Arrow::StringArray.from_values(["ann", "bob"]),
      })
      orders = Prism.from_columns({
        id:     Arrow::Int64Array.from_values([10_i64]),
        amount: Arrow::Float64Array.from_values([5.0]),
      })

      joined = users.join(orders, left_on: "user_id", right_on: "id", how: :left)

      joined.column_names.should eq(["user_id", "name", "id", "amount"])
      joined["user_id"].to_a.should eq([10_i64, 20_i64])
      joined["id"].to_a.should eq([10_i64, nil])
    end

    it "suffixes colliding non-key columns" do
      left = Prism.from_columns({
        id:    Arrow::Int64Array.from_values([1_i64]),
        value: Arrow::Int64Array.from_values([1_i64]),
      })
      right = Prism.from_columns({
        id:    Arrow::Int64Array.from_values([1_i64]),
        value: Arrow::Int64Array.from_values([2_i64]),
      })

      joined = left.join(right, on: "id")

      joined.column_names.should eq(["id", "value", "value_right"])
      joined["value_right"].to_a.should eq([2_i64])
    end

    it "honours a custom suffix" do
      left = Prism.from_columns({
        id:    Arrow::Int64Array.from_values([1_i64]),
        value: Arrow::Int64Array.from_values([1_i64]),
      })
      right = Prism.from_columns({
        id:    Arrow::Int64Array.from_values([1_i64]),
        value: Arrow::Int64Array.from_values([2_i64]),
      })

      left.join(right, on: "id", suffix: "_r").column_names
        .should eq(["id", "value", "value_r"])
    end

    it "joins on several keys" do
      left = Prism.from_columns({
        a: Arrow::Int64Array.from_values([1_i64, 1_i64, 2_i64]),
        b: Arrow::StringArray.from_values(["x", "y", "x"]),
        v: Arrow::Int64Array.from_values([10_i64, 20_i64, 30_i64]),
      })
      right = Prism.from_columns({
        a: Arrow::Int64Array.from_values([1_i64, 1_i64]),
        b: Arrow::StringArray.from_values(["x", "z"]),
        w: Arrow::Int64Array.from_values([100_i64, 200_i64]),
      })

      joined = left.join(right, on: ["a", "b"])

      joined.row_count.should eq(1)
      joined["v"].to_a.should eq([10_i64])
      joined["w"].to_a.should eq([100_i64])
    end

    it "returns an empty result with the right schema for no matches" do
      empty_right = join_right.filter(col("id") > lit(100))
      none = join_left.join(empty_right, on: "id")

      none.row_count.should eq(0)
      none.column_names.should eq(["id", "name", "score", "label"])
    end

    it "keeps every left row when the right side is empty" do
      empty_right = join_right.filter(col("id") > lit(100))
      joined = join_left.join(empty_right, on: "id", how: :left)

      joined.row_count.should eq(4)
      joined["label"].to_a.should eq([nil, nil, nil, nil])
    end

    it "raises ColumnNotFound for an unknown key" do
      expect_raises(Prism::ColumnNotFound, /nope/) do
        join_left.join(join_right, on: "nope")
      end
    end

    it "raises for incompatible key dtypes" do
      left = Prism.from_columns({id: Arrow::Int64Array.from_values([1_i64])})
      right = Prism.from_columns({id: Arrow::StringArray.from_values(["1"])})

      expect_raises(Prism::DtypeError, /key/) { left.join(right, on: "id") }
    end

    it "rejects an unknown join type" do
      expect_raises(ArgumentError) do
        join_left.join(join_right, on: "id", how: :sideways)
      end
    end

    it "requires both left_on and right_on together" do
      expect_raises(ArgumentError) do
        join_left.join(join_right, left_on: "id")
      end
    end
  end

  describe "Prism.concat" do
    it "stacks frames vertically" do
      combined = Prism.concat([join_left, join_left])

      combined.row_count.should eq(8)
      combined.column_names.should eq(["id", "name", "score"])
      combined["id"].to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64] * 2)
    end

    it "rejects frames with different schemas" do
      other = Prism.from_columns({id: Arrow::Int64Array.from_values([9_i64])})

      expect_raises(Prism::SchemaError) { Prism.concat([join_left, other]) }
    end

    # A zero-row frame contributes no rows, so its nullability cannot violate the
    # others' non-null columns. Building a seed frame from an empty array used to
    # fail with "schema differs (["id", "name", "score"] vs ["id", "name",
    # "score"])", which is self-contradictory.
    # An empty seed frame's columns come out non-nullable (nullability is derived
    # from the data), while a frame that holds nulls is nullable; the mismatch
    # used to fail with "schema differs (["id", "name", "score"] vs ["id", "name",
    # "score"])". A zero-row frame carries no rows, so it cannot violate the
    # nullable contract either way.
    it "accepts a zero-row frame whose columns differ only in nullability" do
      nullable = Prism.from_columns({
        id:    Arrow::Int64Array.from_nullable([1_i64, nil]),
        name:  Arrow::StringArray.from_nullable(["a", nil]),
        score: Arrow::Float64Array.from_nullable([1.0, nil]),
      })
      seed = Prism.from_columns({
        id:    Arrow::Int64Array.from_values([] of Int64),
        name:  Arrow::StringArray.from_values([] of String),
        score: Arrow::Float64Array.from_values([] of Float64),
      })
      seed.schema.fields[0].nullable?.should be_false
      nullable.schema.fields[0].nullable?.should be_true

      combined = Prism.concat([seed, nullable])

      combined.row_count.should eq(2)
      combined.schema.should eq(nullable.schema)
      combined["name"].to_a.should eq(["a", nil])

      # Order must not matter, and an all-empty input still carries a schema.
      Prism.concat([nullable, seed]).schema.should eq(nullable.schema)
      Prism.concat([seed]).row_count.should eq(0)
    end

    it "still rejects a zero-row frame with a different dtype" do
      seed = Prism.from_columns({
        id:    Arrow::PrimitiveArray(Int32).from_nullable([] of Int32?),
        name:  Arrow::StringArray.from_nullable([] of String?),
        score: Arrow::Float64Array.from_nullable([] of Float64?),
      })

      expect_raises(Prism::SchemaError, /id: Int64 vs Int32/) do
        Prism.concat([join_left, seed])
      end
    end

    it "names the difference instead of repeating the column list" do
      # Same names, different dtypes: the old message said the schemas differed
      # and then printed two identical name lists.
      other = Prism.from_columns({
        id:    Arrow::Int64Array.from_values([9_i64]),
        name:  Arrow::StringArray.from_values(["w"]),
        score: Arrow::Int64Array.from_values([7_i64]),
      })

      expect_raises(Prism::SchemaError, /score: Float64 vs Int64/) do
        Prism.concat([join_left, other])
      end

      wider = Prism.from_columns({
        id:    Arrow::Int64Array.from_values([9_i64]),
        name:  Arrow::StringArray.from_values(["w"]),
        score: Arrow::Float64Array.from_values([7.0]),
        extra: Arrow::Int64Array.from_values([1_i64]),
      })

      expect_raises(Prism::SchemaError, /extra/) do
        Prism.concat([join_left, wider])
      end

      narrower = Prism.from_columns({
        id:   Arrow::Int64Array.from_values([9_i64]),
        name: Arrow::StringArray.from_values(["w"]),
      })

      expect_raises(Prism::SchemaError, /missing column "score"/) do
        Prism.concat([join_left, narrower])
      end
    end

    it "rejects an empty list" do
      expect_raises(ArgumentError) { Prism.concat([] of Prism::DataFrame) }
    end
  end

  describe "#hstack" do
    it "concatenates columns side by side" do
      right = Prism.from_columns({extra: Arrow::Int64Array.from_values([9_i64, 8_i64, 7_i64, 6_i64])})
      wide = join_left.hstack(right)

      wide.column_names.should eq(["id", "name", "score", "extra"])
      wide["extra"].to_a.should eq([9_i64, 8_i64, 7_i64, 6_i64])
    end

    it "rejects mismatched row counts" do
      right = Prism.from_columns({extra: Arrow::Int64Array.from_values([9_i64])})

      expect_raises(ArgumentError) { join_left.hstack(right) }
    end

    it "rejects duplicate column names" do
      right = Prism.from_columns({name: Arrow::StringArray.from_values(["w", "x", "y", "z"])})

      expect_raises(Prism::DuplicateColumn) { join_left.hstack(right) }
    end
  end
end
