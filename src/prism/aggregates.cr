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

    # Number of distinct values, counting null as a value of its own.
    #
    # This is Polars' contract (`ChunkUnique`'s implementations add one when
    # `null_count > 0`, and `NullChunked` reports 1 for any non-empty column), so
    # `n_unique == len(unique())`. It differs from SQL's `COUNT(DISTINCT x)`,
    # which skips nulls; that is the one place this library follows Polars over
    # SQL.
    #
    # The column is encoded as `arrow-row` bytes (`Prism::RowKeys`) and the
    # distinct encodings are counted, so this is O(n) rather than the
    # sort-plus-partition it used to be.
    def count_distinct(array : Arrow::ArrayBase, name : String = "value") : Int64
      return 0_i64 if array.len == 0
      # A Null column carries no values to encode, but it is one value.
      return 1_i64 if array.data_type.kind.null?

      array = RowKeys.canonical(array)
      rows = RowKeys.converter([array], [name]).convert_columns([array])
      RowKeys.distinct_count(rows).to_i64
    end

    # Sample variance (`ddof` = 1), or `nil` when there are too few values.
    #
    # This is `Arrow.variance`, a one-pass Welford kernel over the chunk. It
    # applies exactly the rule this used to apply by hand — nil when
    # `count <= ddof` — so the four kernel passes and the two full-length
    # temporaries behind the old `sum((x - mean)^2)` are gone (16.5 ms against
    # 50.2 ms on 1e6 Float64 with 20% nulls).
    def variance(array : Arrow::ArrayBase, ddof : Int32 = 1) : Float64?
      Arrow.variance(stats_array(array), Arrow::VarianceOptions.new(ddof: ddof))
    end

    # Square root of `#variance`, from the same kernel (`Arrow.stddev`).
    def standard_deviation(array : Arrow::ArrayBase, ddof : Int32 = 1) : Float64?
      Arrow.stddev(stats_array(array), Arrow::VarianceOptions.new(ddof: ddof))
    end

    # The variance kernel dispatches the numeric dtypes and the decimals. The two
    # shapes `std` used to accept by widening — `Float16` and boolean — are cast
    # to Float64 first, which is what the old implementation did for every dtype.
    private def stats_array(array : Arrow::ArrayBase) : Arrow::ArrayBase
      case array.data_type.kind
      when .int8?, .int16?, .int32?, .int64?,
           .u_int8?, .u_int16?, .u_int32?, .u_int64?,
           .float32?, .float64?,
           .decimal32?, .decimal64?, .decimal128?, .decimal256?
        array
      else
        float_values(array)
      end
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
