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

    # Element-wise null test, one boolean per row.
    #
    # Named for the operation rather than as a Crystal predicate: it builds a
    # column, so `null?` would misrepresent it (same reasoning as `Expr#is_null`).
    # ameba:disable Naming/PredicateName
    def is_null : Series
      chunks = @chunks.map { |chunk| Arrow.is_null(chunk).as(Arrow::ArrayBase) }

      Series.new(@name, chunks, Arrow::DataType.boolean)
    end

    # Replaces nulls, keeping this column's dtype.
    def fill_null(value : Value) : Series
      # The same rule the frame-wide verb applies: a replacement has to fit the
      # column's family, or a string column would be "filled" with 0 and become
      # "0". Values that only materialize from Arrow (decimals, intervals,
      # containers) keep the old path, which rejects them deeper down.
      if literal = literal_dtype_of(value)
        unless Coerce.fillable?(@dtype, literal)
          raise DtypeError.new(
            "cannot fill #{@name.inspect} (#{@dtype}) with #{literal}; " \
            "cast the replacement explicitly"
          )
        end
      end

      filler = Arrow::Scalar(Arrow::ArrayBase).new(
        Aggregates.array(value, @dtype)
      )
      chunks = @chunks.map do |chunk|
        Datums.zip(Arrow.is_not_null(chunk), chunk, filler).as(Arrow::ArrayBase)
      end

      Series.new(@name, chunks, @dtype)
    end

    # Whether each value is in `values`, as a boolean column — the mask form of
    # `Expr#is_in`, for `DataFrame#filter`:
    #
    # ```
    # df.filter(df["title"].is_in(titles))
    # ```
    #
    # Both forms run the same code (`Prism::Membership`), so the coercion rule,
    # the null rule and the kernel cannot drift between them. Nulls follow
    # comparisons: a null cell yields a null mask bit and `filter` drops that row.
    # `nulls_equal: true` treats null as a value instead, matching only when the
    # set contains null. The set may be any `Enumerable` of Crystal values or an
    # already-encoded `Arrow::ArrayBase`, and it is unified with the column's
    # dtype, so `Int64` columns accept `Int32` literals; a set with no common
    # dtype raises `Prism::DtypeError` before any chunk is read.
    # ameba:disable Naming/PredicateName
    def is_in(values : Enumerable(T), *, nulls_equal : Bool = false) : Series forall T
      is_in(Arrow.array(values.to_a), nulls_equal: nulls_equal)
    end

    # ameba:disable Naming/PredicateName
    def is_in(values : Arrow::ArrayBase, *, nulls_equal : Bool = false) : Series
      # Validate up front: an empty series has no chunk to fail on.
      Membership.target(@dtype, values.data_type)
      chunks = @chunks.map do |chunk|
        Membership.matches(chunk, values, nulls_equal).as(Arrow::ArrayBase)
      end

      Series.new(@name, chunks, Arrow::DataType.boolean)
    end

    # Converts to `target`, casting each chunk.
    # The dtype of a `Value` that is also a literal (everything `Prism.lit`
    # accepts), or nil for the values that only exist after materialization.
    private def literal_dtype_of(value : Value) : Arrow::DataType?
      # Normalized the way `Prism.lit` normalizes: a width is not part of the
      # literal's meaning, so `0_i32` and `0_i64` both mean `Int64`.
      case value
      when Nil                                    then Arrow::DataType.null
      when Bool                                   then Arrow::DataType.boolean
      when String                                 then Arrow::DataType.utf8
      when Bytes                                  then Arrow::DataType.binary
      when Time                                   then Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond)
      when Int8, Int16, Int32, Int64, Int128      then Arrow::DataType.int64
      when UInt8, UInt16, UInt32, UInt64, UInt128 then Arrow::DataType.uint64
      when Float32, Float64                       then Arrow::DataType.float64
      else                                             nil
      end
    end

    def cast(target : Arrow::DataType) : Series
      return self if target == @dtype

      chunks = @chunks.map { |chunk| Arrow.cast(chunk, target).as(Arrow::ArrayBase) }

      Series.new(@name, chunks, target)
    end

    # Element-wise comparisons, producing a boolean series.
    #
    # `==`, `!=`, `<`, `<=`, `>`, `>=` accept another series or a scalar. A
    # series comparison requires the same dtype and length; a scalar comparison
    # coerces through `Prism::Coerce.with_literal`, so `series > 10` casts `10`
    # to the column's dtype when that is lossless and widens both sides
    # otherwise — the same rule expressions follow (§8.2). Comparing a numeric
    # series with a string raises `Prism::DtypeError`.
    #
    # The result is built for `DataFrame#filter`, which takes a boolean mask:
    #
    # ```
    # df.filter(df["price"] > 10)
    # ```
    #
    # Nulls propagate, so a null comparison yields a null mask bit and that row
    # is dropped, exactly as an expression predicate behaves.
    #
    # Never use `==` to compare two series for structural equality: it returns a
    # series, which is always truthy. Use `#same_as?`.
    def ==(other : Series) : Series
      compare(BinaryOp::Eq, other)
    end

    def ==(other : LiteralInput) : Series
      compare(BinaryOp::Eq, other)
    end

    def !=(other : Series) : Series
      compare(BinaryOp::Neq, other)
    end

    def !=(other : LiteralInput) : Series
      compare(BinaryOp::Neq, other)
    end

    def <(other : Series) : Series
      compare(BinaryOp::Lt, other)
    end

    def <(other : LiteralInput) : Series
      compare(BinaryOp::Lt, other)
    end

    def <=(other : Series) : Series
      compare(BinaryOp::LtEq, other)
    end

    def <=(other : LiteralInput) : Series
      compare(BinaryOp::LtEq, other)
    end

    def >(other : Series) : Series
      compare(BinaryOp::Gt, other)
    end

    def >(other : LiteralInput) : Series
      compare(BinaryOp::Gt, other)
    end

    def >=(other : Series) : Series
      compare(BinaryOp::GtEq, other)
    end

    def >=(other : LiteralInput) : Series
      compare(BinaryOp::GtEq, other)
    end

    # Structural equality: name, dtype and chunk contents.
    def same_as?(other : Series) : Bool
      @name == other.name && @dtype == other.dtype && @chunks == other.chunks
    end

    # Distinct values, keeping the first occurrence of each in original order.
    # Nulls are a value, so at most one null survives. Floats are canonicalized
    # before keying (`Prism::RowKeys`), so `-0.0` and `0.0` are one value and so
    # is every NaN.
    def unique : Series
      return self if size <= 1

      array = RowKeys.canonical(to_arrow)
      rows = RowKeys.converter([array], [@name]).convert_columns([array])
      kept = RowKeys.first_occurrences(rows)
      return self if kept.size == array.len

      selected = Arrow::PrimitiveArray(UInt32).from_values(kept)
      Series.new(@name, [Arrow.take(array, selected)] of Arrow::ArrayBase, @dtype)
    end

    # Number of distinct values, with null counted as one value (Polars'
    # contract, so `n_unique == unique.size`).
    def n_unique : Int64
      Aggregates.count_distinct(to_arrow, @name)
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

    # Chunk pairs for an element-wise operation. Series whose chunking differs
    # are consolidated first; equal chunking is used as-is.
    private def comparable_chunks(other : Series) : Array(Tuple(Arrow::ArrayBase, Arrow::ArrayBase))
      if @chunks.size == other.chunks.size
        @chunks.zip(other.chunks).map { |(left, right)| {left, right} }
      else
        [{to_arrow, other.to_arrow}]
      end
    end

    # Series-versus-series comparison: both sides must already agree.
    private def compare(op : BinaryOp, other : Series) : Series
      unless other.dtype == @dtype
        raise DtypeError.new(
          "#{@name.inspect}: cannot compare dtype #{@dtype} with #{other.dtype}"
        )
      end
      unless other.size == size
        raise ArgumentError.new(
          "#{@name.inspect}: cannot compare #{size} rows with #{other.size}"
        )
      end

      chunks = comparable_chunks(other).map do |(left, right)|
        Kernels.apply_op(op, left, right)
      end

      Series.new(@name, chunks, Arrow::DataType.boolean)
    end

    # Series-versus-scalar comparison, coerced by the same rule expressions use.
    private def compare(op : BinaryOp, other : LiteralInput) : Series
      array = literal_array(other)
      target = Coerce.with_literal(@dtype, array.data_type)
      scalar = Coerce.cast(Arrow::Scalar(Arrow::ArrayBase).new(array), target)

      chunks = @chunks.map do |chunk|
        column = chunk.data_type == target ? chunk : Arrow.cast(chunk, target)
        Kernels.apply_op(op, column, scalar)
      end

      Series.new(@name, chunks, Arrow::DataType.boolean)
    end

    # Normalizes a scalar the way `Prism.lit` does, so a comparison does not
    # depend on the width Crystal inferred for the literal.
    private def literal_array(value : LiteralInput) : Arrow::ArrayBase
      normalized = case value
                   when Int8, Int16, Int32, Int64, Int128      then value.to_i64
                   when UInt8, UInt16, UInt32, UInt64, UInt128 then value.to_u64
                   when Float32, Float64                       then value.to_f64
                   else                                             value
                   end

      Literals.array(normalized)
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
