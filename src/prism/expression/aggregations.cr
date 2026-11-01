module Prism
  # The aggregation a grouped expression applies within each group.
  enum AggFn
    Sum
    Mean
    Min
    Max
    Count
    CountAll
    NUnique
    First
    Last
    Std
    Variance
    Median

    def key_name : String
      case self
      in .sum?       then "sum"
      in .mean?      then "mean"
      in .min?       then "min"
      in .max?       then "max"
      in .count?     then "count"
      in .count_all? then "count_all"
      in .n_unique?  then "n_unique"
      in .first?     then "first"
      in .last?      then "last"
      in .std?       then "std"
      in .variance?  then "variance"
      in .median?    then "median"
      end
    end

    # Aggregates that count rows or distinct values.
    def count_like? : Bool
      count? || count_all? || n_unique?
    end

    # Aggregates whose output is Float64 regardless of the input dtype.
    def float_output? : Bool
      mean? || std? || variance? || median?
    end
  end

  # An aggregation applied to an expression inside `GroupBy#aggregate`.
  #
  # `eval` receives one group's rows (a batch slice) and returns a length-1
  # array, which the group executor concatenates across groups.
  class Agg < Expr
    getter fn : AggFn
    getter operand : Expr

    def initialize(@fn : AggFn, @operand : Expr)
    end

    def key : String
      "#{@fn.key_name}(#{@operand.key})"
    end

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      input = @operand.dtype(schema)
      return Arrow::DataType.uint32 if @fn.count_like?
      return Arrow::DataType.float64 if @fn.float_output?

      input
    end

    def eval(ctx : EvalContext) : Datum
      array = Datums.array(ctx.evaluate(@operand), ctx)
      series = Series.new("aggregate", [array] of Arrow::ArrayBase, array.data_type)

      case @fn
      when .first?
        # A slice is a view, so no value is materialized for first/last.
        array.slice(0, Math.min(1, array.len))
      when .last?
        array.len == 0 ? array.slice(0, 0) : array.slice(array.len - 1, 1)
      else
        Aggregates.array(value_for(@fn, series, array), output_dtype(array.data_type))
      end
    end

    private def value_for(fn : AggFn, series : Series, array : Arrow::ArrayBase) : Value
      case fn
      in .sum?       then series.sum
      in .mean?      then series.mean
      in .min?       then series.min
      in .max?       then series.max
      in .count?     then series.count
      in .count_all? then series.size
      in .n_unique?
        # Distinct non-null values, consistent with the other aggregates
        # skipping nulls.
        Arrow.filter(array, Arrow.is_not_null(array)).uniq.len.to_i64
      in .std?      then Aggregates.standard_deviation(array)
      in .variance? then Aggregates.variance(array)
      in .median?   then Aggregates.median(array)
      in .first?, .last?
        raise UnsupportedOperation.new("#{fn.key_name} returns a slice, not a scalar")
      end
    end

    private def output_dtype(input : Arrow::DataType) : Arrow::DataType
      return Arrow::DataType.uint32 if @fn.count_like?
      return Arrow::DataType.float64 if @fn.float_output?

      input
    end
  end

  # Aggregation verbs on expressions.
  abstract class Expr
    # Sum of non-null values within each group.
    def sum : Expr
      Agg.new(AggFn::Sum, self)
    end

    # Arithmetic mean within each group.
    def mean : Expr
      Agg.new(AggFn::Mean, self)
    end

    def min : Expr
      Agg.new(AggFn::Min, self)
    end

    def max : Expr
      Agg.new(AggFn::Max, self)
    end

    # Non-null value count.
    def count : Expr
      Agg.new(AggFn::Count, self)
    end

    # Row count, including nulls.
    def count_all : Expr
      Agg.new(AggFn::CountAll, self)
    end

    def n_unique : Expr
      Agg.new(AggFn::NUnique, self)
    end

    def first : Expr
      Agg.new(AggFn::First, self)
    end

    def last : Expr
      Agg.new(AggFn::Last, self)
    end

    def std : Expr
      Agg.new(AggFn::Std, self)
    end

    def variance : Expr
      Agg.new(AggFn::Variance, self)
    end

    def median : Expr
      Agg.new(AggFn::Median, self)
    end
  end
end
