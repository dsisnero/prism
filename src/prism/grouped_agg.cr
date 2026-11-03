module Prism
  # Vectorized grouped aggregation: one scan across every group.
  #
  # `GroupBy` hands this module a contiguous partition of the sorted frame, so an
  # aggregate can be computed once for all groups instead of once per group. The
  # per-group path cost, for each of 1e5 groups, a `RecordBatch` slice, an
  # `EvalContext`, a `Series`, a length-1 array and one `Arrow.concat` piece —
  # ~95% of a group-by (see [`docs/benchmarks.md`](../docs/benchmarks.md)). Here
  # the operand expression is evaluated **once** over the whole sorted batch and
  # one walk over it fills one result array.
  #
  # | Aggregate | How |
  # | --- | --- |
  # | `count`, `count_all` | range lengths / a validity-bitmap popcount |
  # | `first`, `last` | one `Arrow.take` of the group-start or group-end rows |
  # | `sum`, `mean` | one fold per group over the operand |
  # | `min`, `max` | one fold per group |
  # | `n_unique`, `std`, `variance`, `median` | per-group Arrow kernel, but the operand is already evaluated and the result is built once |
  #
  # `vectorizable?` and `per_group_kernel?` pick the tier; anything else (a
  # nested aggregation, or a dtype `Series#min` itself rejects) keeps the
  # original per-group expression evaluation in `GroupBy`.
  #
  # Reduction semantics are Arrow's, so a grouped result equals what the kernel
  # returns for the same rows: nulls are skipped and an all-null group is null,
  # integer sums wrap (`&+`, as `Arrow.sum` does), and float extremes compare in
  # IEEE total order, so `-0.0` sorts below `0.0` and every NaN above `+inf`.
  module GroupedAgg
    extend self

    # Aggregates `reduce` computes in one scan for this input dtype.
    def vectorizable?(fn : AggFn, dtype : Arrow::DataType) : Bool
      case fn
      in .count?, .count_all?, .first?, .last? then true
      in .sum?, .mean?                         then numeric?(dtype)
      in .min?, .max?                          then extreme_capable?(dtype)
      in .n_unique?, .std?, .variance?, .median?
        false
      end
    end

    # Aggregates that need a group's whole slice, and so keep the Arrow kernel
    # per group. Their operand is still evaluated once, outside the loop.
    def per_group_kernel?(fn : AggFn) : Bool
      fn.n_unique? || fn.std? || fn.variance? || fn.median?
    end

    # Applies `fn` across all `ranges` of `operand`, one pass, one result array.
    #
    # `ranges` are contiguous, non-empty group boundaries over `operand`, which
    # must cover all rows of the frame being aggregated.
    def reduce(
      fn : AggFn,
      operand : Arrow::ArrayBase,
      ranges : Array(Range(Int32, Int32)),
    ) : Arrow::ArrayBase
      case fn
      in .count_all? then row_counts(ranges)
      in .count?     then value_counts(operand, ranges)
      in .first?     then gather(operand, ranges, from_end: false)
      in .last?      then gather(operand, ranges, from_end: true)
      in .sum?       then sums(operand, ranges)
      in .mean?      then means(operand, ranges)
      in .min?       then extrema(operand, ranges, minimum: true)
      in .max?       then extrema(operand, ranges, minimum: false)
      in .n_unique?, .std?, .variance?, .median?
        raise UnsupportedOperation.new(
          "#{fn.key_name} is not vectorized; use GroupedAgg.reduce_per_group"
        )
      end
    end

    # Aggregates that need each group's whole slice.
    #
    # The win over evaluating the expression per group is that the operand is
    # already a column here and the pieces are collected into one typed array
    # rather than concatenated.
    def reduce_per_group(
      fn : AggFn,
      operand : Arrow::ArrayBase,
      ranges : Array(Range(Int32, Int32)),
    ) : Arrow::ArrayBase
      case fn
      in .n_unique?
        counted = ranges.map { |range| Aggregates.count_distinct(group(operand, range)).to_u32 }
        Arrow::PrimitiveArray(UInt32).from_nullable(counted)
      in .std?
        Arrow::Float64Array.from_nullable(
          ranges.map { |range| Aggregates.standard_deviation(group(operand, range)) }
        )
      in .variance?
        Arrow::Float64Array.from_nullable(
          ranges.map { |range| Aggregates.variance(group(operand, range)) }
        )
      in .median?
        Arrow::Float64Array.from_nullable(
          ranges.map { |range| Aggregates.median(group(operand, range)) }
        )
      in .sum?, .mean?, .min?, .max?, .count?, .count_all?, .first?, .last?
        raise UnsupportedOperation.new(
          "#{fn.key_name} is vectorized; use GroupedAgg.reduce"
        )
      end
    end

    # --- aggregates -------------------------------------------------------

    # One row per group, so the range length is the count.
    private def row_counts(ranges : Array(Range(Int32, Int32))) : Arrow::ArrayBase
      Arrow::PrimitiveArray(UInt32).from_values(ranges.map(&.size.to_u32))
    end

    # Non-null values per group, counted as popcounts over the validity bitmap.
    #
    # This mirrors Polars' `agg_valid_count` for `GroupsType::Slice`, which
    # counts set bits in the group's validity `BitMask`: one bit operation per
    # eight rows beats a null check per row, and the answer is the same because
    # `ArrayBase#count` is `len - logical_null_count`.
    private def value_counts(
      operand : Arrow::ArrayBase,
      ranges : Array(Range(Int32, Int32)),
    ) : Arrow::ArrayBase
      nulls = operand.as(Arrow::ArrayBase).logical_nulls
      return row_counts(ranges) unless nulls

      counts = Array(UInt32).new(ranges.size) do |position|
        range = ranges[position]
        nulls.slice(range.begin, range.size).inner.count_set_bits.to_u32
      end

      Arrow::PrimitiveArray(UInt32).from_values(counts)
    end

    # `first`/`last` are a gather of one row per group, so one `Arrow.take`
    # covers every group.
    private def gather(
      operand : Arrow::ArrayBase,
      ranges : Array(Range(Int32, Int32)),
      from_end : Bool,
    ) : Arrow::ArrayBase
      indices = Array(UInt32).new(ranges.size) do |position|
        range = ranges[position]
        (from_end ? range.end - 1 : range.begin).to_u32
      end

      Arrow.take(operand, Arrow::PrimitiveArray(UInt32).from_values(indices))
    end

    private def sums(operand : Arrow::ArrayBase, ranges) : Arrow::ArrayBase
      array = operand.as(Arrow::ArrayBase)
      case array.data_type.kind
      when .int8?  then sum_array(array.as(Arrow::PrimitiveArray(Int8)), ranges)
      when .int16? then sum_array(array.as(Arrow::PrimitiveArray(Int16)), ranges)
      when .int32? then sum_array(array.as(Arrow::PrimitiveArray(Int32)), ranges)
      when .int64? then sum_array(array.as(Arrow::PrimitiveArray(Int64)), ranges)
      when Arrow::DataType::Kind::UInt8
        sum_array(array.as(Arrow::PrimitiveArray(UInt8)), ranges)
      when Arrow::DataType::Kind::UInt16
        sum_array(array.as(Arrow::PrimitiveArray(UInt16)), ranges)
      when Arrow::DataType::Kind::UInt32
        sum_array(array.as(Arrow::PrimitiveArray(UInt32)), ranges)
      when Arrow::DataType::Kind::UInt64
        sum_array(array.as(Arrow::PrimitiveArray(UInt64)), ranges)
      when .float16? then sum_array(array.as(Arrow::PrimitiveArray(Arrow::Float16)), ranges)
      when .float32? then sum_array(array.as(Arrow::PrimitiveArray(Float32)), ranges)
      when .float64? then sum_array(array.as(Arrow::PrimitiveArray(Float64)), ranges)
      else                raise DtypeError.new("sum requires a numeric column; got #{array.data_type}")
      end
    end

    private def sum_array(array : Arrow::PrimitiveArray(T), ranges) : Arrow::ArrayBase forall T
      Arrow::PrimitiveArray(T).from_nullable(sum_values(array, ranges))
    end

    private def means(operand : Arrow::ArrayBase, ranges) : Arrow::ArrayBase
      array = operand.as(Arrow::ArrayBase)
      case array.data_type.kind
      when .int8?  then mean_array(array.as(Arrow::PrimitiveArray(Int8)), ranges)
      when .int16? then mean_array(array.as(Arrow::PrimitiveArray(Int16)), ranges)
      when .int32? then mean_array(array.as(Arrow::PrimitiveArray(Int32)), ranges)
      when .int64? then mean_array(array.as(Arrow::PrimitiveArray(Int64)), ranges)
      when Arrow::DataType::Kind::UInt8
        mean_array(array.as(Arrow::PrimitiveArray(UInt8)), ranges)
      when Arrow::DataType::Kind::UInt16
        mean_array(array.as(Arrow::PrimitiveArray(UInt16)), ranges)
      when Arrow::DataType::Kind::UInt32
        mean_array(array.as(Arrow::PrimitiveArray(UInt32)), ranges)
      when Arrow::DataType::Kind::UInt64
        mean_array(array.as(Arrow::PrimitiveArray(UInt64)), ranges)
      when .float16? then mean_array(array.as(Arrow::PrimitiveArray(Arrow::Float16)), ranges)
      when .float32? then mean_array(array.as(Arrow::PrimitiveArray(Float32)), ranges)
      when .float64? then mean_array(array.as(Arrow::PrimitiveArray(Float64)), ranges)
      else                raise DtypeError.new("mean requires a numeric column; got #{array.data_type}")
      end
    end

    # `mean = sum / non-null count`, promoted to Float64, exactly as
    # `Series#mean` composes it — including summing in the column's own dtype.
    private def mean_array(array : Arrow::PrimitiveArray(T), ranges) : Arrow::ArrayBase forall T
      values = Array(Float64?).new(ranges.size)
      ranges.each do |range|
        total = nil.as(T?)
        count = 0
        index = range.begin
        while index < range.end
          unless array.null?(index)
            value = array.value(index)
            total = total ? added(total, value) : value
            count += 1
          end
          index += 1
        end
        values << (total ? total.to_f64 / count.to_f64 : nil)
      end

      Arrow::Float64Array.from_nullable(values)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def extrema(
      operand : Arrow::ArrayBase,
      ranges,
      minimum : Bool,
    ) : Arrow::ArrayBase
      array = operand.as(Arrow::ArrayBase)
      case array.data_type.kind
      when .boolean?
        Arrow::BooleanArray.from_nullable(
          extreme_booleans(array.as(Arrow::BooleanArray), ranges, minimum)
        )
      when .utf8?
        string_array(array.as(Arrow::StringArray), ranges, minimum)
      when .large_utf8?
        string_array(array.as(Arrow::LargeStringArray), ranges, minimum)
      when .utf8_view?
        Arrow::StringViewArray.from_nullable(
          extreme_strings(array.as(Arrow::StringViewArray), ranges, minimum)
        )
      when .int8?  then extreme_array(array.as(Arrow::PrimitiveArray(Int8)), ranges, minimum)
      when .int16? then extreme_array(array.as(Arrow::PrimitiveArray(Int16)), ranges, minimum)
      when .int32? then extreme_array(array.as(Arrow::PrimitiveArray(Int32)), ranges, minimum)
      when .int64? then extreme_array(array.as(Arrow::PrimitiveArray(Int64)), ranges, minimum)
      when Arrow::DataType::Kind::UInt8
        extreme_array(array.as(Arrow::PrimitiveArray(UInt8)), ranges, minimum)
      when Arrow::DataType::Kind::UInt16
        extreme_array(array.as(Arrow::PrimitiveArray(UInt16)), ranges, minimum)
      when Arrow::DataType::Kind::UInt32
        extreme_array(array.as(Arrow::PrimitiveArray(UInt32)), ranges, minimum)
      when Arrow::DataType::Kind::UInt64
        extreme_array(array.as(Arrow::PrimitiveArray(UInt64)), ranges, minimum)
      when .float16?
        extreme_array(array.as(Arrow::PrimitiveArray(Arrow::Float16)), ranges, minimum)
      when .float32?
        extreme_array(array.as(Arrow::PrimitiveArray(Float32)), ranges, minimum)
      when .float64?
        extreme_array(array.as(Arrow::PrimitiveArray(Float64)), ranges, minimum)
      else
        raise UnsupportedOperation.new(
          "min/max is not supported for #{array.data_type}"
        )
      end
    end

    private def extreme_array(
      array : Arrow::PrimitiveArray(T),
      ranges,
      minimum : Bool,
    ) : Arrow::ArrayBase forall T
      Arrow::PrimitiveArray(T).from_nullable(extreme_values(array, ranges, minimum))
    end

    private def string_array(
      array : Arrow::GenericStringArray(O),
      ranges,
      minimum : Bool,
    ) : Arrow::ArrayBase forall O
      Arrow::GenericStringArray(O).from_nullable(extreme_strings(array, ranges, minimum))
    end

    private def string_array(
      array : Arrow::StringViewArray,
      ranges,
      minimum : Bool,
    ) : Arrow::ArrayBase
      Arrow::StringViewArray.from_nullable(extreme_strings(array, ranges, minimum))
    end

    # --- per-group folds --------------------------------------------------

    # One group's rows as a view, used by the per-group kernels.
    private def group(
      operand : Arrow::ArrayBase,
      range : Range(Int32, Int32),
    ) : Arrow::ArrayBase
      operand.slice(range.begin, range.size)
    end

    # Folds each group's non-null values with wrapping (integer) or IEEE
    # (float) addition, matching `Arrow.sum`'s accumulator.
    private def sum_values(array : Arrow::PrimitiveArray(T), ranges) : Array(T?) forall T
      values = Array(T?).new(ranges.size)
      ranges.each do |range|
        total = nil.as(T?)
        index = range.begin
        while index < range.end
          unless array.null?(index)
            value = array.value(index)
            total = total ? added(total, value) : value
          end
          index += 1
        end
        values << total
      end

      values
    end

    private def extreme_values(
      array : Arrow::PrimitiveArray(T),
      ranges,
      minimum : Bool,
    ) : Array(T?) forall T
      values = Array(T?).new(ranges.size)
      ranges.each do |range|
        best = nil.as(T?)
        index = range.begin
        while index < range.end
          unless array.null?(index)
            value = array.value(index)
            best = if current = best
                     extreme_better?(value, current, minimum) ? value : current
                   else
                     value
                   end
          end
          index += 1
        end
        values << best
      end

      values
    end

    private def extreme_strings(
      array : Arrow::GenericStringArray(O),
      ranges,
      minimum : Bool,
    ) : Array(String?) forall O
      extreme_string_rows(array, ranges, minimum) { |index| array.value(index) }
    end

    private def extreme_strings(
      array : Arrow::StringViewArray,
      ranges,
      minimum : Bool,
    ) : Array(String?)
      extreme_string_rows(array, ranges, minimum) { |index| array.value(index) }
    end

    private def extreme_string_rows(
      array : Arrow::ArrayBase,
      ranges,
      minimum : Bool,
      & : Int32 -> String
    ) : Array(String?)
      values = Array(String?).new(ranges.size)
      ranges.each do |range|
        best = nil.as(String?)
        index = range.begin
        while index < range.end
          unless array.null?(index)
            value = yield index
            best = if current = best
                     extreme_better?(value, current, minimum) ? value : current
                   else
                     value
                   end
          end
          index += 1
        end
        values << best
      end

      values
    end

    # Arrow's boolean aggregates, per group: `min` is every non-null value true
    # (`bool_and`), `max` is any non-null value true (`bool_or`), and a group
    # with no non-null value is null.
    private def extreme_booleans(
      array : Arrow::BooleanArray,
      ranges,
      minimum : Bool,
    ) : Array(Bool?)
      values = Array(Bool?).new(ranges.size)
      ranges.each do |range|
        best = nil.as(Bool?)
        index = range.begin
        while index < range.end
          unless array.null?(index)
            value = array.value(index)
            best = if current = best
                     minimum ? (current && value) : (current || value)
                   else
                     value
                   end
          end
          index += 1
        end
        values << best
      end

      values
    end

    # --- comparison helpers -----------------------------------------------

    private def extreme_better?(candidate : Int, current : Int, minimum : Bool) : Bool
      minimum ? candidate < current : candidate > current
    end

    private def extreme_better?(candidate : Float64, current : Float64, minimum : Bool) : Bool
      minimum ? total_order(candidate) < total_order(current) : total_order(candidate) > total_order(current)
    end

    private def extreme_better?(candidate : Float32, current : Float32, minimum : Bool) : Bool
      minimum ? total_order(candidate) < total_order(current) : total_order(candidate) > total_order(current)
    end

    private def extreme_better?(
      candidate : Arrow::Float16,
      current : Arrow::Float16,
      minimum : Bool,
    ) : Bool
      comparison = candidate.total_cmp(current)
      minimum ? comparison < 0 : comparison > 0
    end

    # Strings compare byte-wise, like Arrow's `min_string`/`max_string`.
    private def extreme_better?(candidate : String, current : String, minimum : Bool) : Bool
      minimum ? candidate < current : candidate > current
    end

    private def added(left : Int, right : Int) : Int
      left &+ right
    end

    private def added(left : Float32, right : Float32) : Float32
      left + right
    end

    private def added(left : Float64, right : Float64) : Float64
      left + right
    end

    private def added(left : Arrow::Float16, right : Arrow::Float16) : Arrow::Float16
      left + right
    end

    # IEEE `totalOrder` key, matching `arrow-cr`'s `total_cmp`: flipping the
    # sign bit this way makes a plain integer comparison the total order.
    private def total_order(value : Float64) : Int64
      total_order_bits(value.unsafe_as(Int64))
    end

    private def total_order(value : Float32) : Int64
      total_order_bits(value.unsafe_as(Int32).to_i64)
    end

    private def total_order_bits(bits : Int64) : Int64
      bits ^ ((bits >> 63).unsafe_as(UInt64) >> 1).to_i64!
    end

    # --- dtypes -----------------------------------------------------------

    private def numeric?(dtype : Arrow::DataType) : Bool
      dtype.integer? || dtype.floating?
    end

    # The dtypes `Arrow`'s own reduction supports, so the vectorized fold and
    # the per-group kernel cannot disagree about what `min`/`max` accepts.
    private def extreme_capable?(dtype : Arrow::DataType) : Bool
      numeric?(dtype) || dtype.kind.boolean? || dtype.kind.utf8? ||
        dtype.kind.large_utf8? || dtype.kind.utf8_view?
    end
  end
end
