module Prism
  # Compute helpers for aggregations.
  #
  # These run on a single array (one group's slice) using Arrow kernels, so a
  # group is never copied out: `Agg` receives a `RecordBatch#slice` view and the
  # kernel walks it in place. Only the scalar result is materialized.
  module Aggregates
    extend self

    # Builds a length-1 array of `target` from an aggregated Crystal value.
    #
    # Reuses the literal constructors and casts, rather than duplicating a
    # dtype switch: an `Int64` sum of an `Int32` column becomes `Int64` then
    # casts back to `Int32`.
    def array(value : Value, target : Arrow::DataType) : Arrow::ArrayBase
      built = Literals.array(normalize(value))
      return built if built.data_type == target

      Arrow.cast(built, target)
    end

    def normalize(value : Value) : LiteralValue
      case value
      when Int8, Int16, Int32, Int64      then value.to_i64
      when UInt8, UInt16, UInt32, UInt64  then value.to_u64
      when Arrow::Float16                 then value.to_f64
      when Float32, Float64               then value.to_f64
      when Nil, Bool, String, Bytes, Time then value
      else
        raise UnsupportedOperation.new(
          "cannot build an aggregate from #{value.class}"
        )
      end
    end

    # Number of distinct non-null values.
    #
    # Uses `lexsort_to_indices` (typed per-dtype sort) plus `Arrow.partition`
    # (which compares adjacent rows with the typed `cmp::distinct` kernel), not
    # `ArrayBase#uniq`, which sorts through a `DynComparator` closure and
    # allocates an index array for every row.
    def count_distinct(array : Arrow::ArrayBase) : Int64
      # A null column has no non-null values, and sorting is unsupported for it.
      return 0_i64 if array.len == 0 || array.data_type.kind.null?

      order = Arrow.lexsort_to_indices([Arrow::SortColumn.new(array)])
      sorted = Arrow.take(array, order)
      runs = Arrow.partition([sorted]).ranges.size

      # `partition` gives nulls a run of their own; the other aggregates skip
      # nulls, so this one does too.
      (runs - (array.null_count > 0 ? 1 : 0)).to_i64
    end

    # Sample variance (`ddof` = 1), or `nil` when there are too few values.
    #
    # Computed as sum((x - mean)^2) / (n - ddof) with Arrow kernels; the column
    # is cast to Float64 once and never materialized into Crystal values.
    def variance(array : Arrow::ArrayBase, ddof : Int32 = 1) : Float64?
      floats = float_values(array)
      count = floats.count
      return nil if count <= ddof

      total = Arrow.sum(floats)
      return nil if total.nil?

      mean = total / count.to_f64
      mean_scalar = Arrow::Scalar(Arrow::ArrayBase).new(
        Arrow::Float64Array.from_values([mean]).as(Arrow::ArrayBase)
      )
      deviations = Arrow.sub(floats, mean_scalar)
      squares = Arrow.mul(deviations, deviations)
      total_squares = Arrow.sum(squares.as(Arrow::PrimitiveArray(Float64)))
      return nil if total_squares.nil?

      total_squares / (count - ddof).to_f64
    end

    def standard_deviation(array : Arrow::ArrayBase, ddof : Int32 = 1) : Float64?
      value = variance(array, ddof)
      value ? Math.sqrt(value) : nil
    end

    # Median of the non-null values, averaging the middle two when even.
    def median(array : Arrow::ArrayBase) : Float64?
      valid = Arrow.filter(array, Arrow.is_not_null(array))
      count = valid.len
      return nil if count == 0

      sorted = Arrow.sort(valid)
      middle = count // 2
      if count.odd?
        Values.to_f64(Values.extract_at(sorted, middle))
      else
        lower = Values.to_f64(Values.extract_at(sorted, middle - 1))
        upper = Values.to_f64(Values.extract_at(sorted, middle))
        (lower + upper) / 2.0
      end
    end

    private def float_values(array : Arrow::ArrayBase) : Arrow::PrimitiveArray(Float64)
      return array.as(Arrow::PrimitiveArray(Float64)) if array.data_type.kind.float64?

      Arrow.cast(array, Arrow::DataType.float64)
        .as(Arrow::PrimitiveArray(Float64))
    end
  end
end
