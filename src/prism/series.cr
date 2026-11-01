module Prism
  # A single named column, stored as one or more Arrow chunks.
  #
  # A column borrows the chunking of its frame: a frame with three batches
  # yields a series with three chunks. `Array` was chosen over introducing a
  # chunked-array type because `arrow-rs` deliberately has no `ChunkedArray` and
  # recommends `Vec<ArrayRef>` for exactly this: one buffer set per chunk, and
  # no consolidated copy until someone asks for one.
  #
  # Everything here works chunk-at-a-time. Materialization happens only at the
  # edges — `#to_a`, `#to_arrow`, and `#each` — never inside an aggregate.
  class Series
    getter name : String
    getter chunks : Array(Arrow::ArrayBase)
    getter dtype : Arrow::DataType

    # The reduction an aggregate applies, chosen once and run per chunk.
    enum Reduce
      Sum
      Min
      Max
    end

    # Builds a series from already-aligned Arrow chunks.
    #
    # Raises `Prism::DtypeError` if any chunk's dtype differs from `dtype`.
    def initialize(@name : String, chunks : Enumerable, @dtype : Arrow::DataType)
      @chunks = chunks.map(&.as(Arrow::ArrayBase)).to_a
      @chunks.each do |chunk|
        next if chunk.data_type == @dtype

        raise DtypeError.new(
          "#{@name.inspect}: chunk dtype #{chunk.data_type} does not match #{@dtype}"
        )
      end
    end

    # Total rows across all chunks, including nulls.
    def size : Int64
      @chunks.sum(&.len.to_i64)
    end

    # Non-null rows across all chunks.
    def count : Int64
      @chunks.sum(&.count.to_i64)
    end

    def null_count : Int64
      @chunks.sum(&.null_count.to_i64)
    end

    def empty? : Bool
      size == 0
    end

    # Edge: yields each Arrow chunk without combining them.
    def each_chunk(& : Arrow::ArrayBase ->) : Nil
      @chunks.each { |chunk| yield chunk }
    end

    # Edge: materializes every value as a Crystal value, nulls as `nil`.
    def to_a : Array(Value)
      @chunks.flat_map { |chunk| Values.extract(chunk) }
    end

    # Edge: yields each value. Materializes one chunk at a time, not the column.
    def each(& : Value ->) : Nil
      @chunks.each do |chunk|
        Values.extract(chunk).each { |value| yield value }
      end
    end

    # Returns the value at `index`, which may be `nil` for a null slot.
    #
    # Raises `IndexError` when `index` is outside the series.
    def [](index : Int) : Value
      chunk, local = locate(index)
      Values.extract_at(chunk, local)
    end

    # Returns the value at `index`, or `nil` when it is null or out of range.
    def []?(index : Int) : Value
      return nil if index < 0 || index >= size

      chunk, local = locate(index)
      Values.extract_at(chunk, local)
    end

    # Distinct values, keeping the first occurrence of each in original order.
    # Nulls are a value, so at most one null survives.
    def unique : Series
      return self if size <= 1

      array = to_arrow
      order = Arrow.lexsort_to_indices([Arrow::SortColumn.new(array)])
      sorted = Arrow.take(array, order)
      ranges = Arrow.partition([sorted]).ranges
      indices = Distinct.first_occurrence_indices(order, ranges)
      selected = Arrow::PrimitiveArray(UInt32).from_values(indices)

      Series.new(@name, [Arrow.take(array, selected)] of Arrow::ArrayBase, @dtype)
    end

    # Number of distinct non-null values.
    def n_unique : Int64
      Aggregates.count_distinct(to_arrow)
    end

    # Edge: returns the column as one Arrow array, concatenating chunks.
    #
    # A single-chunk series returns its chunk without copying; multi-chunk
    # concatenation is the one place this type allocates. Aggregates do not use
    # this path.
    def to_arrow : Arrow::ArrayBase
      if @chunks.empty?
        raise UnsupportedOperation.new(
          "#{@name.inspect}: series has no chunks; use #to_a instead of #to_arrow"
        )
      end

      return @chunks.first if @chunks.size == 1

      Arrow.concat(@chunks)
    end

    # Sum of the non-null values, reduced chunk-by-chunk.
    #
    # Raises `Prism::DtypeError` for non-numeric columns.
    def sum : Value
      ensure_numeric!("sum")
      reduce_numeric(Reduce::Sum)
    end

    # Arithmetic mean of the non-null values, or `nil` when there are none.
    #
    # Raises `Prism::DtypeError` for non-numeric columns.
    def mean : Float64?
      ensure_numeric!("mean")
      total = reduce_numeric(Reduce::Sum)
      rows = count
      return nil if total.nil? || rows == 0

      to_f64(total) / rows.to_f64
    end

    # Smallest non-null value, reduced chunk-by-chunk.
    #
    # Supports numeric, boolean, and string columns; anything else raises
    # `Prism::UnsupportedOperation`.
    def min : Value
      reduce(Reduce::Min)
    end

    # Largest non-null value, reduced chunk-by-chunk. See `#min`.
    def max : Value
      reduce(Reduce::Max)
    end

    def to_s(io : IO) : Nil
      io << "Series(" << @name << ": " << @dtype << ", " << size << " rows, "
      io << null_count << " null)"
    end

    private def reduce(op : Reduce) : Value
      if @dtype.integer? || @dtype.floating?
        reduce_numeric(op)
      elsif @dtype.kind.boolean?
        reduce_boolean(op)
      elsif @dtype.string?
        reduce_string(op)
      else
        raise UnsupportedOperation.new(
          "#{@name.inspect}: #{op.to_s.downcase} is not supported for #{@dtype}"
        )
      end
    end

    private def reduce_numeric(op : Reduce) : Value
      case @dtype.kind
      when .int8?                        then reduce_primitive(Int8, op)
      when .int16?                       then reduce_primitive(Int16, op)
      when .int32?                       then reduce_primitive(Int32, op)
      when .int64?                       then reduce_primitive(Int64, op)
      when Arrow::DataType::Kind::UInt8  then reduce_primitive(UInt8, op)
      when Arrow::DataType::Kind::UInt16 then reduce_primitive(UInt16, op)
      when Arrow::DataType::Kind::UInt32 then reduce_primitive(UInt32, op)
      when Arrow::DataType::Kind::UInt64 then reduce_primitive(UInt64, op)
      when .float16?                     then reduce_primitive(Arrow::Float16, op)
      when .float32?                     then reduce_primitive(Float32, op)
      when .float64?                     then reduce_primitive(Float64, op)
      else
        raise UnsupportedOperation.new(
          "#{@name.inspect}: #{op.to_s.downcase} is not supported for #{@dtype}"
        )
      end
    end

    # Reduces one chunk at a time and folds the partial results, so an aggregate
    # never allocates a consolidated column.
    private def reduce_primitive(klass : T.class, op : Reduce) : T? forall T
      result = nil.as(T?)
      @chunks.each do |chunk|
        array = chunk.as(Arrow::PrimitiveArray(T))
        partial = case op
                  in .sum? then Arrow.sum(array)
                  in .min? then Arrow.min(array)
                  in .max? then Arrow.max(array)
                  end
        next if partial.nil?

        result = if current = result
                   case op
                   in .sum? then current + partial
                   in .min? then Math.min(current, partial)
                   in .max? then Math.max(current, partial)
                   end
                 else
                   partial
                 end
      end
      result
    end

    private def reduce_boolean(op : Reduce) : Bool?
      result = nil.as(Bool?)
      @chunks.each do |chunk|
        array = chunk.as(Arrow::BooleanArray)
        partial = case op
                  in .sum? then raise UnsupportedOperation.new("sum is not supported for Boolean")
                  in .min? then Arrow.min_boolean(array)
                  in .max? then Arrow.max_boolean(array)
                  end
        next if partial.nil?

        result = if current = result
                   case op
                   in .sum? then raise UnsupportedOperation.new("sum is not supported for Boolean")
                   in .min? then current && partial
                   in .max? then current || partial
                   end
                 else
                   partial
                 end
      end
      result
    end

    private def reduce_string(op : Reduce) : String?
      result = nil.as(String?)
      @chunks.each do |chunk|
        partial = string_extreme(chunk, op)
        next if partial.nil?

        result = if current = result
                   better = op.min? ? partial < current : partial > current
                   better ? partial : current
                 else
                   partial
                 end
      end
      result
    end

    private def string_extreme(chunk : Arrow::ArrayBase, op : Reduce) : String?
      kind = chunk.data_type.kind
      if kind.large_utf8?
        array = chunk.as(Arrow::LargeStringArray)
        op.min? ? Arrow.min_string(array) : Arrow.max_string(array)
      elsif kind.utf8_view?
        array = chunk.as(Arrow::StringViewArray)
        op.min? ? Arrow.min_string_view(array) : Arrow.max_string_view(array)
      else
        array = chunk.as(Arrow::StringArray)
        op.min? ? Arrow.min_string(array) : Arrow.max_string(array)
      end
    end

    private def locate(index : Int) : Tuple(Arrow::ArrayBase, Int32)
      if index < 0 || index >= size
        raise IndexError.new(
          "index #{index} is outside series #{@name.inspect} of size #{size}"
        )
      end

      remaining = index.to_i64
      @chunks.each do |chunk|
        width = chunk.len.to_i64
        return {chunk, remaining.to_i32} if remaining < width

        remaining -= width
      end

      raise IndexError.new("index #{index} is outside series #{@name.inspect}")
    end

    private def ensure_numeric!(operation : String) : Nil
      return if @dtype.integer? || @dtype.floating?

      raise DtypeError.new(
        "#{operation} requires a numeric column; #{@name.inspect} is #{@dtype}"
      )
    end

    private def to_f64(value : Value) : Float64
      Values.to_f64(value)
    end
  end
end
