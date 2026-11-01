module Prism
  # An immutable, column-oriented table backed by Arrow record batches.
  #
  # A frame owns one schema and one or more batches, all of which must share
  # that schema. Element-wise operations map over the batches and keep the
  # chunking; operations that reorder rows collapse them later.
  class DataFrame
    getter schema : Arrow::Schema

    # The backing batches. Treat as read-only; all verbs return new frames.
    getter batches : Array(Arrow::RecordBatch)

    # Raises `Prism::SchemaError` unless every batch matches `schema`.
    def initialize(@schema : Arrow::Schema, @batches : Array(Arrow::RecordBatch))
      @batches.each_with_index do |batch, index|
        next if batch.schema == @schema

        raise SchemaError.new(
          "batch #{index} schema does not match the frame schema " \
          "(batch: #{batch.schema.fields.map(&.name).to_a}, " \
          "frame: #{@schema.fields.map(&.name).to_a})"
        )
      end
    end

    # Total rows across all chunks.
    def row_count : Int64
      @batches.sum(&.num_rows.to_i64)
    end

    def column_count : Int32
      @schema.fields.len
    end

    def shape : NamedTuple(rows: Int64, columns: Int32)
      {rows: row_count, columns: column_count}
    end

    def column_names : Array(String)
      @schema.fields.map(&.name)
    end

    def dtypes : Hash(String, Arrow::DataType)
      @schema.fields.each_with_object({} of String => Arrow::DataType) do |field, memo|
        memo[field.name] = field.data_type
      end
    end

    def empty? : Bool
      row_count == 0
    end

    # Returns the column `name`.
    #
    # Raises `Prism::ColumnNotFound`, naming the column and listing the columns
    # that do exist.
    def [](name : String | Symbol) : Series
      self[name]? || raise ColumnNotFound.new(name.to_s, column_names)
    end

    # Returns the column `name`, or `nil` when it does not exist.
    def []?(name : String | Symbol) : Series?
      found = @schema.fields.find(name.to_s)
      return nil unless found

      index, field = found
      Series.new(field.name, @batches.map(&.column(index)), field.data_type)
    end

    def each_batch(& : Arrow::RecordBatch ->) : Nil
      @batches.each { |batch| yield batch }
    end

    # Returns the frame as one Arrow batch, concatenating chunks if needed.
    #
    # A single-chunk frame returns its batch without copying.
    def to_arrow : Arrow::RecordBatch
      return Arrow::RecordBatch.new_empty(@schema) if @batches.empty?
      return @batches.first if @batches.size == 1

      Arrow.concat_batches(@schema, @batches)
    end

    # First `n` rows. Returns `self` when `n` covers the whole frame.
    def head(n : Int) : DataFrame
      raise ArgumentError.new("n must be >= 0 (got #{n})") if n < 0
      return self if n >= row_count

      slice(0, n)
    end

    # Last `n` rows. Returns `self` when `n` covers the whole frame.
    def tail(n : Int) : DataFrame
      raise ArgumentError.new("n must be >= 0 (got #{n})") if n < 0
      return self if n >= row_count

      slice((row_count - n.to_i64).to_i, n)
    end

    # `length` rows starting at `offset`, clamped to the available rows.
    #
    # Slices that fall inside one chunk stay a view of that chunk.
    def slice(offset : Int, length : Int) : DataFrame
      raise ArgumentError.new("offset must be >= 0 (got #{offset})") if offset < 0
      raise ArgumentError.new("length must be >= 0 (got #{length})") if length < 0

      selected = [] of Arrow::RecordBatch
      position = 0_i64
      remaining = length.to_i64

      @batches.each do |batch|
        break if remaining <= 0

        width = batch.num_rows.to_i64
        next_position = position + width

        if next_position <= offset
          position = next_position
          next
        end

        start = Math.max(offset.to_i64 - position, 0_i64)
        take = Math.min(width - start, remaining)
        if take > 0
          selected << batch.slice(start.to_i32, take.to_i32)
          remaining -= take
        end

        position = next_position
      end

      DataFrame.new(@schema, selected)
    end

    def ==(other : DataFrame) : Bool
      @schema == other.schema && @batches == other.batches
    end

    def to_s(io : IO) : Nil
      io << "DataFrame(" << row_count << " rows x " << column_count << " columns)"
    end
  end
end
