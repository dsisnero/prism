module Prism
  class DataFrame
    # Drops duplicate rows, keeping the first occurrence of each key.
    #
    # Keys are compared as `arrow-row` bytes (`Prism::RowKeys`), so this is
    # O(n): NaNs match each other, `-0.0`/`0.0` stay apart, and nulls are a key
    # of their own. With no arguments every column is part of the key.
    def distinct : DataFrame
      distinct_columns(column_names)
    end

    # Drops duplicates considering only the named columns.
    def distinct(*names : String | Symbol) : DataFrame
      distinct_columns(names.map(&.to_s).to_a)
    end

    # Key-list form, used by the planner.
    def distinct(names : Array(String)) : DataFrame
      distinct_columns(names)
    end

    # Drops rows with a null in any column. Element-wise, so chunks survive.
    def drop_nulls : DataFrame
      drop_null_columns(column_names)
    end

    # Drops rows with a null in any of the named columns.
    def drop_nulls(*names : String | Symbol) : DataFrame
      drop_null_columns(names.map(&.to_s).to_a)
    end

    # Encode the key columns as `arrow-row` bytes and keep the first row of each
    # distinct encoding, in input order. O(n): no sort, and no column values are
    # materialized into Crystal objects.
    private def distinct_columns(keys : Array(String)) : DataFrame
      keys.each { |name| require_column!(name) }
      return self if keys.empty?

      batch = to_arrow
      return self if batch.num_rows <= 1

      key_arrays = keys.map do |name|
        batch.column_by_name(name) || raise ColumnNotFound.new(name, batch.column_names)
      end

      rows = RowKeys.converter(key_arrays, keys).convert_columns(key_arrays)
      kept = RowKeys.first_occurrences(rows)
      return self if kept.size == batch.num_rows

      selected = Arrow::PrimitiveArray(UInt32).from_values(kept)
      DataFrame.new(@schema, [Arrow.take_record_batch(batch, selected)])
    end

    private def drop_null_columns(targets : Array(String)) : DataFrame
      targets.each { |name| require_column!(name) }
      return self if targets.empty?

      predicate = targets
        .map { |name| Prism.col(name).is_not_null }
        .reduce { |left, right| left.and(right) }

      filter(predicate)
    end
  end
end
