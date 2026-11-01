module Prism
  class DataFrame
    # Drops duplicate rows, keeping the first occurrence of each key.
    #
    # With no arguments every column is part of the key.
    def distinct : DataFrame
      distinct_columns(column_names)
    end

    # Drops duplicates considering only the named columns.
    def distinct(*names : String | Symbol) : DataFrame
      distinct_columns(names.map(&.to_s).to_a)
    end

    # Drops rows with a null in any column. Element-wise, so chunks survive.
    def drop_nulls : DataFrame
      drop_null_columns(column_names)
    end

    # Drops rows with a null in any of the named columns.
    def drop_nulls(*names : String | Symbol) : DataFrame
      drop_null_columns(names.map(&.to_s).to_a)
    end

    # Sort by the key columns, find each run of equal keys, keep the smallest
    # original row index from each run, then restore the original order. No
    # column values are materialized.
    private def distinct_columns(keys : Array(String)) : DataFrame
      keys.each { |name| require_column!(name) }
      return self if keys.empty?

      batch = to_arrow
      return self if batch.num_rows <= 1

      key_arrays = keys.map do |name|
        batch.column_by_name(name) || raise ColumnNotFound.new(name, batch.column_names)
      end

      order = Arrow.lexsort_to_indices(
        key_arrays.map { |array| Arrow::SortColumn.new(array) }.to_a
      )
      sorted_keys = key_arrays.map { |array| Arrow.take(array, order) }.to_a
      positions = order.as(Arrow::PrimitiveArray(UInt32))

      kept = Arrow.partition(sorted_keys).ranges.map do |range|
        (range.begin...range.end).min_of { |position| positions.value(position) }
      end

      selected = Arrow::PrimitiveArray(UInt32).from_values(kept.sort!)
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
