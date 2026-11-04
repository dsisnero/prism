module Prism
  # Stacks frames vertically.
  #
  # Column names and dtypes must match exactly. Nullability only has to match for
  # a frame that carries rows: a zero-row frame contributes nothing, so it cannot
  # violate a non-null column, and requiring it made the common "concat the
  # results, starting from an empty seed" pattern fail on a nullable seed.
  #
  # Collapses to one batch: concatenating chunks is the point of the operation.
  # A failure names the column and the difference — the names-only message this
  # replaced printed two identical lists when the schemas differed in dtype.
  def self.concat(frames : Enumerable(DataFrame)) : DataFrame
    list = frames.to_a
    raise ArgumentError.new("concat needs at least one frame") if list.empty?

    # The first frame with rows decides the result schema, so a nullable seed does
    # not become the output schema; with only empty frames the first one does.
    schema = (list.find { |frame| frame.row_count > 0 } || list.first).schema
    batches = [] of Arrow::RecordBatch

    list.each_with_index do |frame, index|
      if difference = concat_dtype_difference(schema, frame.schema)
        raise SchemaError.new("cannot concatenate frame #{index}: #{difference}")
      end
      next if frame.row_count == 0

      if difference = concat_row_difference(schema, frame.schema)
        raise SchemaError.new("cannot concatenate frame #{index}: #{difference}")
      end
      batches << frame.to_arrow
    end

    DataFrame.new(schema, batches.empty? ? [] of Arrow::RecordBatch : [Arrow.concat_batches(schema, batches)])
  end

  # How `actual` differs from `reference` in names or dtypes, or `nil` when it
  # does not. This is the part every frame must satisfy, rows or not.
  private def self.concat_dtype_difference(
    reference : Arrow::Schema,
    actual : Arrow::Schema,
  ) : String?
    expected = reference.fields.to_a
    got = actual.fields.to_a

    expected.each_with_index do |field, index|
      other = got[index]?
      return "missing column #{field.name.inspect}" if other.nil?

      if other.name != field.name
        return "column #{index}: expected #{field.name.inspect}, got #{other.name.inspect}"
      end
      if other.data_type != field.data_type
        return "column #{field.name}: #{field.data_type} vs #{other.data_type}"
      end
    end
    return "unexpected column #{got[expected.size].name.inspect}" if got.size > expected.size

    nil
  end

  # How `actual` differs from `reference` beyond names and dtypes, for a frame
  # that carries rows: nullability, then field metadata.
  private def self.concat_row_difference(
    reference : Arrow::Schema,
    actual : Arrow::Schema,
  ) : String?
    reference.fields.to_a.zip(actual.fields.to_a).each do |(field, other)|
      next if field.nullable? == other.nullable?

      return "column #{field.name}: expected #{nullability(field)}, got #{nullability(other)}"
    end

    unless reference == actual
      reference.fields.to_a.zip(actual.fields.to_a).each do |(field, other)|
        next if field == other

        return "column #{field.name}: field metadata differs"
      end
    end

    nil
  end

  private def self.nullability(field : Arrow::Field) : String
    field.nullable? ? "nullable" : "non-null"
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
