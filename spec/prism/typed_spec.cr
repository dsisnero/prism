require "../spec_helper"

include Prism::DSL

Prism.define_schema UserSchema,
  id: Int64,
  name: String,
  active: Bool,
  salary: Float64

# A second declaration pins the remaining supported types, so typed rows prove
# each Crystal type maps to the Arrow dtype it claims (and back).
Prism.define_schema MetricSchema,
  at: Time,
  blob: Bytes,
  small: Int32,
  ratio: Float32

private def user_batch : Arrow::RecordBatch
  Arrow::RecordBatch.from_columns({
    id:     Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64]),
    name:   Arrow::StringArray.from_nullable(["ann", "bob", nil]),
    active: Arrow::BooleanArray.from_values([true, false, true]),
    salary: Arrow::Float64Array.from_nullable([100.0, 200.0, nil]),
  })
end

private def typed_users : UserSchema::Frame
  UserSchema.from_arrow(user_batch)
end

describe Prism::Typed do
  describe "define_schema" do
    it "builds the Arrow schema from the declared fields" do
      schema = UserSchema.schema

      schema.fields.map(&.name).should eq(["id", "name", "active", "salary"])
      schema.fields.map(&.data_type).should eq([
        Arrow::DataType.int64,
        Arrow::DataType.utf8,
        Arrow::DataType.boolean,
        Arrow::DataType.float64,
      ])
    end

    it "wraps a record batch" do
      typed = typed_users

      typed.row_count.should eq(3)
      typed.columns.should eq(["id", "name", "active", "salary"])
      typed.to_arrow.should eq(user_batch)
    end

    it "wraps a data frame" do
      UserSchema.from_dataframe(Prism.from_arrow(user_batch)).row_count.should eq(3)
    end

    it "rejects a frame that does not match the schema" do
      other = Prism.from_columns({id: Arrow::StringArray.from_values(["x"])})

      expect_raises(Prism::DtypeError, /id/) { UserSchema.from_dataframe(other) }
    end

    it "rejects a frame with a matching name but wrong dtype" do
      other = Prism.from_columns({id: Arrow::StringArray.from_values(["x"])})

      expect_raises(Prism::DtypeError, /Int64/) { UserSchema.from_dataframe(other) }
    end
  end

  describe "typed accessors" do
    it "returns a series per field" do
      typed = typed_users

      typed.salary.to_a.should eq([100.0, 200.0, nil])
      typed.name.to_a.should eq(["ann", "bob", nil])
      typed.active.to_a.should eq([true, false, true])
    end

    it "exposes field expressions for composing verbs" do
      UserSchema.salary.should be_a(Prism::Expr)
      UserSchema.salary.key.should eq("col:salary")
    end
  end

  describe "typed verbs" do
    it "selects with a field reference" do
      selected = typed_users.select(&.salary)

      selected.column_names.should eq(["salary"])
      selected["salary"].to_a.should eq([100.0, 200.0, nil])
    end

    it "selects several fields through the schema" do
      selected = typed_users.select(UserSchema.active, UserSchema.name)

      selected.column_names.should eq(["active", "name"])
    end

    it "filters rows with a field reference" do
      filtered = typed_users.filter(&.active)

      filtered.row_count.should eq(2)
      filtered.id.to_a.should eq([1_i64, 3_i64])
    end

    it "filters with a composed expression" do
      filtered = typed_users.filter(UserSchema.salary > lit(150.0))

      filtered.id.to_a.should eq([2_i64])
    end

    it "keeps the typed frame through schema-preserving verbs" do
      typed = typed_users

      typed.head(2).row_count.should eq(2)
      typed.head(2).id.to_a.should eq([1_i64, 2_i64])
      typed.sort_by("salary", order: :desc).id.to_a.first.should eq(2_i64)
      typeof(typed.head(2)).to_s.should eq("UserSchema::Frame")
    end

    it "returns a plain DataFrame when the schema changes" do
      extended = typed_users.with_column("bonus", UserSchema.salary * lit(0.1))

      extended.column_names.should eq(["id", "name", "active", "salary", "bonus"])
      typeof(extended).to_s.should eq("Prism::DataFrame")
    end

    it "re-enters the typed layer with from_dataframe" do
      extended = typed_users.with_column("bonus", UserSchema.salary * lit(0.1))
      # A superset is accepted: every declared field is present and typed.
      UserSchema.from_dataframe(extended).row_count.should eq(3)

      # A frame missing a declared field is rejected.
      narrowed = extended.select("id", "name", "active")
      expect_raises(Prism::DtypeError, /salary/) do
        UserSchema.from_dataframe(narrowed)
      end
    end

    it "exposes the underlying frame and lazy plan" do
      typed = typed_users

      typed.frame.should be_a(Prism::DataFrame)
      typed.lazy.should be_a(Prism::LazyFrame)
      typed.lazy.filter(UserSchema.active).collect.row_count.should eq(2)
    end
  end

  describe "typed rows" do
    it "materializes one nominal row per row, with typed fields" do
      rows = typed_users.rows

      rows.size.should eq(3)
      rows.first.should be_a(UserSchema::Row)
      rows.first.id.should eq(1_i64)
      rows.first.name.should eq("ann")
      rows.first.active.should be_true
      rows.first.salary.should eq(100.0)
    end

    it "keeps nulls as nil in typed fields" do
      row = typed_users.rows.last

      row.name.should be_nil
      row.salary.should be_nil
      row.active.should be_true
    end

    it "streams rows in order across chunk boundaries" do
      batches = [user_batch.slice(0, 2), user_batch.slice(2, 1)]
      typed = UserSchema.from_dataframe(Prism.from_batches(batches))
      ids = [] of Int64?

      typed.each_row { |row| ids << row.id }

      ids.should eq([1_i64, 2_i64, 3_i64])
    end

    it "reads one row by position and re-types the dynamic row" do
      row = typed_users.row(1)

      row.should eq(UserSchema::Row.new(2_i64, "bob", false, 200.0))
      row.to_h.should eq(typed_users.frame.row(1))
    end

    it "raises IndexError outside the frame" do
      expect_raises(IndexError, /row 3/) { typed_users.row(3) }
    end

    it "has no rows when the frame has no chunks" do
      typed = UserSchema.from_dataframe(
        Prism.from_batches([] of Arrow::RecordBatch, schema: UserSchema.schema)
      )

      typed.rows.should be_empty
    end

    it "compares and hashes structurally, so rows work in sets" do
      rows = typed_users.rows

      rows[0].should eq(UserSchema::Row.new(1_i64, "ann", true, 100.0))
      rows[0].should_not eq(rows[1])
      rows.uniq.size.should eq(3)
    end

    it "rebuilds a row from a name-keyed hash" do
      hash = typed_users.frame.row(0)

      UserSchema::Row.from_hash(hash).should eq(typed_users.rows[0])
    end

    it "names a missing field instead of raising KeyError" do
      partial = Prism::RowHash.new
      partial["id"] = 1_i64
      partial["name"] = "ann"
      partial["active"] = true

      expect_raises(Prism::DtypeError, /salary/) do
        UserSchema::Row.from_hash(partial)
      end
    end

    it "decodes every declared dtype to its Crystal value" do
      micros = 1_700_000_000_000_000_i64
      batch = Arrow::RecordBatch.from_columns({
        at: Arrow::PrimitiveArray(Int64).from_values([micros, micros])
          .with_data_type(Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond)),
        blob:  Arrow::BinaryArray.from_nullable([Bytes[1_u8, 2_u8], nil]),
        small: Arrow::Int32Array.from_values([7_i32, 8_i32]),
        ratio: Arrow::Float32Array.from_nullable([1.5_f32, nil]),
      })
      typed = MetricSchema.from_arrow(batch)

      first = typed.rows.first
      first.at.should eq(Time.unix(1_700_000_000))
      first.blob.should eq(Bytes[1_u8, 2_u8])
      first.small.should eq(7_i32)
      first.ratio.should eq(1.5_f32)

      # Nulls survive the typed decode unchanged.
      last = typed.rows.last
      last.blob.should be_nil
      last.ratio.should be_nil
      last.small.should eq(8_i32)
    end
  end

  describe "compile-time safety" do
    # Only defined when explicitly enabled: it shells out to `crystal build`,
    # which re-analyses the whole dependency graph.
    if ENV["PRISM_COMPILE_SPECS"]?
      it "fails to compile a misspelt field" do
        path = SpecPaths.temp("compile_fail.cr")
        File.write(path, <<-CRYSTAL)
          require "../src/prism"

          Prism.define_schema UserSchema, id: Int64, salary: Float64

          frame = Prism.from_columns({
            id:     Arrow::Int64Array.from_values([1_i64]),
            salary: Arrow::Float64Array.from_values([1.0]),
          })
          UserSchema.from_arrow(frame.to_arrow).select(&.salry)
          CRYSTAL

        output = `crystal build --no-codegen #{path} 2>&1`
        $?.success?.should be_false
        output.should contain("salry")
      end
    end
  end
end
