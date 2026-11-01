module Prism
  class DataFrame
    # Groups rows for aggregation.
    #
    # ```
    # df.group_by("region").aggregate(total: col("price").sum)
    # df.group_by("region", "category", maintain_order: false).aggregate(...)
    # ```
    #
    # `null_keys` controls where null key rows go: `:first` (default), `:last`,
    # or `:drop` to discard them. `maintain_order: true` (default) returns groups
    # in the order their key first appeared; `false` returns them sorted by key.
    def group_by(
      *keys : String | Symbol | Expr,
      null_keys : Symbol = :first,
      maintain_order : Bool = true,
    ) : GroupBy
      expressions = [] of Expr
      keys.each do |key|
        expressions << (key.is_a?(Expr) ? key : Prism.col(key))
      end

      GroupBy.new(self, expressions, null_keys, maintain_order)
    end

    # Expression-list form, used by the planner.
    def group_by(
      keys : Array(Expr),
      *,
      null_keys : Symbol = :first,
      maintain_order : Bool = true,
    ) : GroupBy
      GroupBy.new(self, keys, null_keys, maintain_order)
    end
  end

  # Grouped rows, ready for aggregation.
  #
  # Grouping is sort-based — the strategy Polars uses when keys are already
  # sorted (`GroupsType::Slice`). The keys are lexsorted, `Arrow.partition`
  # yields contiguous ranges, and each range is aggregated through a
  # `RecordBatch#slice` view, so no group is copied out and no index list is
  # built. Every aggregation in one `aggregate` call walks the same ranges.
  class GroupBy
    getter frame : DataFrame
    getter keys : Array(Expr)

    def initialize(
      @frame : DataFrame,
      @keys : Array(Expr),
      @null_keys : Symbol = :first,
      @maintain_order : Bool = true,
    )
      raise ArgumentError.new("group_by needs at least one key") if @keys.empty?

      unless {:first, :last, :drop}.includes?(@null_keys)
        raise ArgumentError.new(
          "null_keys must be :first, :last, or :drop (got #{@null_keys})"
        )
      end
    end

    # Aggregates by name: `aggregate(total: col("price").sum)`.
    def aggregate(**named) : DataFrame
      columns = {} of String => Expr
      named.each do |name, expr|
        columns[name.to_s] = expr.as(Expr)
      end

      aggregate(columns)
    end

    # Aggregates with runtime-built column names.
    def aggregate(columns : Hash(String, Expr)) : DataFrame
      columns.each do |name, expr|
        next if aggregation?(expr)

        raise InvalidAggregation.new(name, expr.key)
      end

      execute(columns)
    end

    # Aggregates aliased expressions: `agg(col("price").sum.alias("total"))`.
    def agg(*exprs : Expr) : DataFrame
      columns = {} of String => Expr
      exprs.each do |expr|
        unless expr.is_a?(Alias) && aggregation?(expr)
          raise MissingAlias.new(expr.key)
        end

        columns[expr.as(Alias).name] = expr
      end

      execute(columns)
    end

    private def aggregation?(expr : Expr) : Bool
      case expr
      when Agg   then true
      when Alias then aggregation?(expr.operand)
      else            false
      end
    end

    private def execute(columns : Hash(String, Expr)) : DataFrame
      raise ArgumentError.new("aggregate needs at least one column") if columns.empty?

      frame = @null_keys == :drop ? without_null_keys : @frame
      batch = frame.to_arrow
      types = TypeCache.new(frame.schema)

      key_context = EvalContext.new(batch, types, Cse.duplicate_keys(@keys))
      key_arrays = @keys.map { |key| Datums.array(key_context.evaluate(key), key_context) }
      schema = result_schema(key_arrays, columns, types)

      if batch.num_rows == 0
        return empty_result(schema, key_arrays, columns, batch, types)
      end

      nulls_first = @null_keys != :last
      sort_columns = key_arrays.map do |array|
        Arrow::SortColumn.new(array, Arrow::SortOptions.new(false, nulls_first))
      end.to_a
      order = Arrow.lexsort_to_indices(sort_columns)

      sorted_batch = Arrow.take_record_batch(batch, order)
      sorted_keys = key_arrays.map { |array| Arrow.take(array, order) }.to_a
      ranges = Arrow.partition(sorted_keys).ranges

      first_rows = Arrow::PrimitiveArray(UInt32).from_values(
        ranges.map(&.begin.to_u32)
      )
      key_columns = sorted_keys.map { |array| Arrow.take(array, first_rows) }.to_a
      aggregate_columns = columns.values.map do |expr|
        concatenate_groups(expr, sorted_batch, ranges, types)
      end

      if @maintain_order && ranges.size > 1
        permutation = discovery_order(order, ranges)
        indices = Arrow::PrimitiveArray(UInt32).from_values(
          permutation.map(&.to_u32)
        )
        key_columns = key_columns.map { |array| Arrow.take(array, indices) }.to_a
        aggregate_columns = aggregate_columns.map do |array|
          Arrow.take(array, indices)
        end.to_a
      end

      DataFrame.new(
        schema,
        [Arrow::RecordBatch.try_new(schema, key_columns + aggregate_columns)]
      )
    end

    # One pass over the group ranges; each group is a view into `sorted_batch`.
    private def concatenate_groups(
      expr : Expr,
      sorted_batch : Arrow::RecordBatch,
      ranges : Array(Range(Int32, Int32)),
      types : TypeCache,
    ) : Arrow::ArrayBase
      duplicates = Cse.duplicate_keys([expr])
      pieces = ranges.map do |range|
        slice = sorted_batch.slice(range.begin, range.size)
        ctx = EvalContext.new(slice, types, duplicates)
        Datums.array(ctx.evaluate(expr), ctx)
      end

      return pieces.first if pieces.size == 1

      Arrow.concat(pieces)
    end

    # Groups that share a first-seen row must keep discovery order, which the
    # sort destroyed. Each group's smallest original row index restores it.
    private def discovery_order(
      order : Arrow::ArrayBase,
      ranges : Array(Range(Int32, Int32)),
    ) : Array(Int32)
      positions = order.as(Arrow::PrimitiveArray(UInt32))
      first_seen = ranges.map do |range|
        (range.begin...range.end).min_of { |position| positions.value(position) }
      end

      (0...ranges.size).to_a.sort_by { |index| first_seen[index] }
    end

    private def without_null_keys : DataFrame
      predicate = @keys
        .map(&.is_not_null)
        .reduce { |left, right| left.and(right) }

      @frame.filter(predicate)
    end

    private def result_schema(
      key_arrays : Array(Arrow::ArrayBase),
      columns : Hash(String, Expr),
      types : TypeCache,
    ) : Arrow::Schema
      key_fields = @keys.map_with_index do |key, index|
        Arrow::Field.new(key_name(key), key_arrays[index].data_type, true)
      end
      aggregate_fields = columns.map do |name, expr|
        Arrow::Field.new(name, types.dtype(expr), true)
      end

      Arrow::Schema.new((key_fields + aggregate_fields).to_a)
    end

    # An empty frame has no groups, but still needs typed empty columns.
    private def empty_result(
      schema : Arrow::Schema,
      key_arrays : Array(Arrow::ArrayBase),
      columns : Hash(String, Expr),
      batch : Arrow::RecordBatch,
      types : TypeCache,
    ) : DataFrame
      ctx = EvalContext.new(batch, types)
      key_columns = key_arrays.map(&.slice(0, 0)).to_a
      aggregate_columns = columns.values.map do |expr|
        Datums.array(expr.eval(ctx), ctx).slice(0, 0)
      end

      DataFrame.new(
        schema,
        [Arrow::RecordBatch.try_new(schema, key_columns + aggregate_columns)]
      )
    end

    private def key_name(key : Expr) : String
      case key
      when Col   then key.name
      when Alias then key.name
      else            raise MissingAlias.new(key.key)
      end
    end
  end
end
