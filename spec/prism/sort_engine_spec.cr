require "../spec_helper"

# The strategy `Prism::SortEngine` picks depends on the key dtypes, so what
# needs pinning is the invariant behind the choice: every strategy returns the
# same order the reference lexsort would, ties and nulls included. `sort_by` and
# `group_by` both go through it, and `group_by` relies on the sorted keys having
# contiguous ranges, so a divergence here would silently split a group.
private def order_of(*arrays : Arrow::ArrayBase) : Array(UInt32)
  columns = arrays.map { |values| Arrow::SortColumn.new(values) }.to_a
  Prism::SortEngine.indices(columns).to_a.compact
end

private def reference_order(*arrays : Arrow::ArrayBase) : Array(UInt32)
  columns = arrays.map { |values| Arrow::SortColumn.new(values) }.to_a
  Arrow.lexsort_to_indices(columns).to_a.compact
end

describe Prism::SortEngine do
  describe ".indices" do
    it "matches the reference order for variable-length keys" do
      words = Arrow::StringArray.from_nullable(["west", "east", nil, "west", "east"])
      numbers = Arrow::Int64Array.from_values([1_i64, 1_i64, 1_i64, 0_i64, 0_i64])

      order_of(words, numbers).should eq(reference_order(words, numbers))
      order_of(words).should eq(reference_order(words))
      order_of(words, words).should eq(reference_order(words, words))
    end

    it "matches the reference order for fixed-width keys" do
      ints = Arrow::Int64Array.from_nullable([3_i64, nil, 1_i64, 1_i64, 2_i64])
      floats = Arrow::Float64Array.from_nullable([nil, 1.0, 1.0, 2.0, 2.0])

      order_of(ints, floats).should eq(reference_order(ints, floats))
      order_of(floats, ints).should eq(reference_order(floats, ints))
    end

    it "breaks ties by input order, as a stable sort does" do
      # Every key ties, so the identity is the only stable answer.
      ties = Arrow::StringArray.from_values(["a", "a", "a"])
      order_of(ties, ties).should eq([0_u32, 1_u32, 2_u32])
    end

    it "keeps the bounded window equal to the prefix of the full order" do
      words = Arrow::StringArray.from_values(["c", "a", "b", "a", "c", "b"])
      numbers = Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 1_i64, 2_i64, 0_i64])
      columns = [
        Arrow::SortColumn.new(words.as(Arrow::ArrayBase)),
        Arrow::SortColumn.new(numbers.as(Arrow::ArrayBase)),
      ]

      full = Prism::SortEngine.indices(columns).to_a.compact
      Prism::SortEngine.indices(columns, 3).to_a.compact.should eq(full[0, 3])

      # A bound at or past the row count is no bound at all.
      rows = columns.first.values.len
      Prism::SortEngine.indices(columns, rows).to_a.compact.should eq(full)
      Prism::SortEngine.indices(columns, rows + 5).to_a.compact.should eq(full)
    end

    it "falls back for a key the per-key sort cannot order" do
      structs = Arrow::StructArray.from_pairs([
        {Arrow::Field.new("x", Arrow::DataType.int64, true),
         Arrow::Int64Array.from_values([2_i64, 1_i64, 2_i64, 1_i64]).as(Arrow::ArrayBase)},
      ])
      numbers = Arrow::Int64Array.from_values([3_i64, 4_i64, 1_i64, 2_i64])

      # A struct key next to a variable-length key would otherwise take the pass
      # path and raise "Sort not supported".
      Prism::SortEngine.per_key_sortable?(structs).should be_false
      order_of(structs.as(Arrow::ArrayBase), numbers)
        .should eq(reference_order(structs.as(Arrow::ArrayBase), numbers))
    end
  end

  describe ".expensive_to_compare?" do
    it "is true for variable-length and nested keys" do
      Prism::SortEngine.expensive_to_compare?(Arrow::DataType.utf8).should be_true
      Prism::SortEngine.expensive_to_compare?(Arrow::DataType.large_utf8).should be_true
      Prism::SortEngine.expensive_to_compare?(Arrow::DataType.binary).should be_true
      Prism::SortEngine.expensive_to_compare?(
        Arrow::DataType.list(Arrow::Field.new("item", Arrow::DataType.int64, true))
      ).should be_true
    end

    it "is false for fixed-width keys" do
      Prism::SortEngine.expensive_to_compare?(Arrow::DataType.int64).should be_false
      Prism::SortEngine.expensive_to_compare?(Arrow::DataType.float64).should be_false
      Prism::SortEngine.expensive_to_compare?(Arrow::DataType.boolean).should be_false
      Prism::SortEngine.expensive_to_compare?(
        Arrow::DataType.fixed_size_binary(16)
      ).should be_false
    end
  end
end
