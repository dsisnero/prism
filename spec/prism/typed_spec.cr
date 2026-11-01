require "../spec_helper"

include Prism::DSL

Prism.define_schema UserSchema,
  id: Int64,
  name: String,
  active: Bool,
  salary: Float64

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
