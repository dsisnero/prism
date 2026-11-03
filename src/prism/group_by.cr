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

    # Columns an aggregation reads: the keys' columns plus the aggregates'
    # operands, in frame order.
    #
    # Everything else in the frame is pruned before the sort and the row gather,
    # so a 50-column frame with a two-column aggregation does not pay to gather
    # 48 unused columns (plan Phase 16).
    def self.required_columns(
      keys : Array(Expr),
      aggregates : Hash(String, Expr),
      columns : Array(String),
    ) : Array(String)
      needed = Set(String).new
      keys.each { |key| key.columns.each { |name| needed << name } }
      aggregates.each_value do |expr|
        expr.columns.each { |name| needed << name }
      end

      columns.select { |name| needed.includes?(name) }
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

    # One-call aggregates, RedAmber style.
    #
    # The output keeps each input column's name — Polars' naming rule — so
    # `group_by("region").sum("price")` produces `region, price`. With no column
    # names the aggregate applies to every eligible column: numeric ones for
    # `sum`/`mean`/`median`/`std`/`var`, numeric/boolean/string ones for
    # `min`/`max`, and all columns for `n_unique`. Grouping keys are never
    # aggregated, since they are already in the output.
    #
    # ```
    # df.group_by("region").sum("price", "qty")
    # df.group_by("region").mean # every numeric column
    # df.group_by("region").median("price")
    # ```
    #
    # The empty form is a separate overload rather than a default on the splat:
    # Crystal requires at least one argument for a type-restricted splat.
    {% for name, member in {sum: "Sum", mean: "Mean", min: "Min", max: "Max",
                            median: "Median", std: "Std", var: "Variance",
                            n_unique: "NUnique"} %}
      def {{name.id}}(*columns : String | Symbol) : DataFrame
        one_call(AggFn::{{member.id}}, columns)
      end

      def {{name.id}} : DataFrame
        one_call(AggFn::{{member.id}}, [] of String)
      end
    {% end %}

    # Non-null counts per column. With no column names this is Polars'
    # `GroupBy#count`: one column named `count` holding the number of rows in
    # each group.
    def count(*columns : String | Symbol) : DataFrame
      one_call(AggFn::Count, columns)
    end

    def count : DataFrame
      verify_output_name!("count")
      aggregate({"count" => row_count_expr})
    end

    # Restricts the frame to what the aggregation reads. A key-only aggregation
    # (`count_all` over the key) prunes to the key columns; the degenerate case of
    # no columns at all keeps the frame, since `select` needs at least one.
    private def prune_columns(frame : DataFrame, aggregates : Hash(String, Expr)) : DataFrame
      required = GroupBy.required_columns(@keys, aggregates, frame.column_names)
      return frame if required.empty? || required.size == frame.column_count

      frame.select(required.map { |name| Prism.col(name) })
    end

    private def aggregation?(expr : Expr) : Bool
      case expr
      when Agg   then true
      when Alias then aggregation?(expr.operand)
      else            false
      end
    end

    # Shared body of the one-call aggregate verbs.
    private def one_call(fn : AggFn, columns : Enumerable(String | Symbol)) : DataFrame
      names = columns.map(&.to_s)
      names = default_columns(fn) if names.empty?

      if names.empty?
        raise DtypeError.new(
          "#{fn.key_name}: no column supports it " \
          "(columns: #{@frame.column_names.join(", ")}); name the columns explicitly"
        )
      end

      names.each do |name|
        raise ColumnNotFound.new(name, @frame.column_names) unless @frame.column_names.includes?(name)

        verify_output_name!(name)
      end

      aggregated = {} of String => Expr
      names.each { |name| aggregated[name] = aggregate_expr(fn, name) }
      aggregate(aggregated)
    end

    # Turns an `AggFn` into the expression the named form would hand-write.
    private def aggregate_expr(fn : AggFn, column : String) : Expr
      expr = Prism.col(column)
      case fn
      in .sum?       then expr.sum
      in .mean?      then expr.mean
      in .min?       then expr.min
      in .max?       then expr.max
      in .count?     then expr.count
      in .count_all? then expr.count_all
      in .n_unique?  then expr.n_unique
      in .first?     then expr.first
      in .last?      then expr.last
      in .std?       then expr.std
      in .variance?  then expr.variance
      in .median?    then expr.median
      end
    end

    # Every column the aggregate can run on when no names are given. Keys are
    # excluded because they are already emitted.
    private def default_columns(fn : AggFn) : Array(String)
      keys = group_key_names
      @frame.schema.fields.each_with_object([] of String) do |field, memo|
        next if keys.includes?(field.name)
        next unless default_candidate?(fn, field.data_type)

        memo << field.name
      end
    end

    # Which dtypes each aggregate can reduce. `Series#sum`/`#mean` and the
    # variance helpers accept integers and floats only, so a boolean `sum` is
    # excluded here rather than raising mid-execution.
    private def default_candidate?(fn : AggFn, dtype : Arrow::DataType) : Bool
      numeric = dtype.integer? || dtype.floating?

      case fn
      in .sum?, .mean?, .std?, .variance?, .median?
        numeric
      in .min?, .max?
        numeric || dtype.kind.boolean? || dtype.string?
      in .count?, .count_all?, .n_unique?, .first?, .last?
        true
      end
    end

    # The key columns' output names, where they are plain columns.
    private def group_key_names : Array(String)
      @keys.compact_map do |key|
        case key
        when Col   then key.name
        when Alias then key.name
        else            nil
        end
      end
    end

    # One row per group, so any column would do.
    private def row_count_expr : Expr
      name = group_key_names.first? || @frame.column_names.first?
      raise UnsupportedOperation.new("count needs at least one column to count rows") unless name

      Prism.col(name).count_all
    end

    # A one-call aggregate takes its output name from the input column, so it
    # would collide with a grouping key of the same name.
    private def verify_output_name!(name : String) : Nil
      return unless group_key_names.includes?(name)

      raise DuplicateColumn.new(
        name,
        "#{name.inspect} is a grouping key; aggregating it would duplicate the column"
      )
    end

    private def execute(columns : Hash(String, Expr)) : DataFrame
      raise ArgumentError.new("aggregate needs at least one column") if columns.empty?

      frame = @null_keys == :drop ? without_null_keys : @frame
      frame = prune_columns(frame, columns)
      batch = frame.to_arrow
      types = TypeCache.new(frame.schema)

      key_context = EvalContext.new(batch, types, Cse.duplicate_keys(@keys))
      key_arrays = @keys.map { |key| Datums.array(key_context.evaluate(key), key_context) }
      schema = result_schema(key_arrays, columns, types)

      if batch.num_rows == 0
        return empty_result(schema, key_arrays, columns, batch, types)
      end

      # Grouping compares canonical keys, so `-0.0`/`0.0` and every NaN fall into
      # one group; the emitted key values stay the original ones.
      canonical_keys = RowKeys.canonical(key_arrays)
      recanonicalized = canonical_keys.zip(key_arrays).any? { |(left, right)| !left.same?(right) }

      nulls_first = @null_keys != :last
      sort_columns = canonical_keys.map do |array|
        Arrow::SortColumn.new(array, Arrow::SortOptions.new(false, nulls_first))
      end.to_a
      order = Arrow.lexsort_to_indices(sort_columns)

      sorted_batch = Arrow.take_record_batch(batch, order)
      sorted_keys = key_arrays.map { |array| Arrow.take(array, order) }.to_a
      partition_keys = if recanonicalized
                         canonical_keys.map { |array| Arrow.take(array, order) }.to_a
                       else
                         sorted_keys
                       end
      ranges = Arrow.partition(partition_keys).ranges

      first_rows = Arrow::PrimitiveArray(UInt32).from_values(
        ranges.map(&.begin.to_u32)
      )
      key_columns = sorted_keys.map { |array| Arrow.take(array, first_rows) }.to_a
      eval_context = EvalContext.new(sorted_batch, types)
      operands = {} of String => Arrow::ArrayBase
      aggregate_columns = columns.values.map do |expr|
        aggregate_column(expr, sorted_batch, ranges, types, eval_context, operands)
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

    # One aggregate column of the output.
    #
    # The common path evaluates the aggregate's operand once over the whole
    # sorted frame and lets `GroupedAgg` reduce every group in one pass. Two
    # shapes keep the original per-group evaluation: an operand that itself
    # contains an aggregation (`col("a").sum.mean`, which has no defined
    # cross-group meaning), and a dtype the vectorized tier does not cover, so
    # the error a user sees is still the one `Series` raises.
    private def aggregate_column(
      expr : Expr,
      sorted_batch : Arrow::RecordBatch,
      ranges : Array(Range(Int32, Int32)),
      types : TypeCache,
      ctx : EvalContext,
      operands : Hash(String, Arrow::ArrayBase),
    ) : Arrow::ArrayBase
      node = expr.is_a?(Alias) ? expr.as(Alias).operand : expr
      return concatenate_groups(expr, sorted_batch, ranges, types) unless node.is_a?(Agg)

      agg = node.as(Agg)
      return concatenate_groups(expr, sorted_batch, ranges, types) if aggregation_inside?(agg.operand)

      vectorized = GroupedAgg.vectorizable?(agg.fn, types.dtype(agg.operand))
      unless vectorized || GroupedAgg.per_group_kernel?(agg.fn)
        return concatenate_groups(expr, sorted_batch, ranges, types)
      end

      # Two aggregates over the same operand (`col("qty").sum` and
      # `col("qty").count`) share one evaluation.
      operand = operands[agg.operand.key]? || begin
        array = Datums.array(ctx.evaluate(agg.operand), ctx)
        operands[agg.operand.key] = array
        array
      end

      if vectorized
        GroupedAgg.reduce(agg.fn, operand, ranges)
      else
        GroupedAgg.reduce_per_group(agg.fn, operand, ranges)
      end
    end

    private def aggregation_inside?(expr : Expr) : Bool
      expr.is_a?(Agg) || expr.children.any? { |child| aggregation_inside?(child) }
    end

    # Last resort: evaluate the aggregate expression once per group. Reached
    # only for the shapes `aggregate_column` declines, because the per-group
    # cost (~4 µs a group) is what `GroupedAgg` exists to remove.
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
