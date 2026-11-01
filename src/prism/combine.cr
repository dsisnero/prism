module Prism
  # Stacks frames vertically. Schemas must match exactly.
  #
  # Collapses to one batch: concatenating chunks is the point of the operation.
  def self.concat(frames : Enumerable(DataFrame)) : DataFrame
    list = frames.to_a
    raise ArgumentError.new("concat needs at least one frame") if list.empty?

    schema = list.first.schema
    list.each_with_index do |frame, index|
      next if frame.schema == schema

      raise SchemaError.new(
        "cannot concatenate frame #{index}: schema differs " \
        "(#{frame.column_names} vs #{schema.fields.map(&.name).to_a})"
      )
    end

    DataFrame.new(schema, [Arrow.concat_batches(schema, list.map(&.to_arrow))])
  end

  class DataFrame
    # Concatenates columns side by side. Row counts must match and names must
    # not collide.
    def hstack(other : DataFrame) : DataFrame
      unless row_count == other.row_count
        raise ArgumentError.new(
          "hstack needs equal row counts (got #{row_count} and #{other.row_count})"
        )
      end

      right_batch = other.to_arrow
      clashes = right_batch.schema.fields.to_a.select do |field|
        column_names.includes?(field.name)
      end
      raise DuplicateColumn.new(clashes.first.name) unless clashes.empty?

      schema = Arrow::Schema.new(
        @schema.fields.to_a + right_batch.schema.fields.to_a
      )
      batch = Arrow::RecordBatch.try_new(schema, to_arrow.columns + right_batch.columns)

      DataFrame.new(schema, [batch])
    end
  end
end
