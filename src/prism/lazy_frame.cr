module Prism
  # A deferred plan over the same expression AST the eager verbs use.
  #
  # Nothing is read until `#collect`. `#explain` shows what the optimizer did, so
  # pushdown is observable rather than assumed.
  class LazyFrame
    getter plan : Plan::Node

    def initialize(@plan : Plan::Node)
    end

    def schema : Arrow::Schema
      @plan.schema
    end

    def columns : Array(String)
      schema.fields.map(&.name)
    end

    def filter(expr : Expr) : LazyFrame
      LazyFrame.new(Plan::Filter.new(@plan, expr))
    end

    def filter(&) : LazyFrame
      filter(yield)
    end

    # Drops rows where `expr` is true; a null predicate keeps its row, matching
    # the eager verb.
    def remove(expr : Expr) : LazyFrame
      filter(expr.not.or(expr.is_null))
    end

    def remove(&) : LazyFrame
      remove(yield)
    end

    def select(*names : String | Symbol) : LazyFrame
      self.select(names.map { |name| Prism.col(name) })
    end

    def select(*exprs : Expr) : LazyFrame
      self.select(exprs.map(&.as(Expr)))
    end

    def select(exprs : Enumerable(Expr)) : LazyFrame
      # `Enumerable(Expr)` accepts `Array(Col)` here, so widen explicitly.
      list = exprs.map(&.as(Expr)).to_a
      LazyFrame.new(Plan::Select.new(@plan, list))
    end

    def with_column(name : String | Symbol, expr : Expr) : LazyFrame
      LazyFrame.new(Plan::WithColumn.new(@plan, name.to_s, expr))
    end

    def with_column(name : String | Symbol, &) : LazyFrame
      with_column(name, yield)
    end

    def sort_by(
      *keys : String | Symbol | SortKey,
      order : Symbol = :asc,
      nulls : Symbol = :last,
    ) : LazyFrame
      resolved = keys.map do |key|
        case key
        when SortKey
          key
        else
          Prism.sort_key(key, order, nulls)
        end
      end.to_a

      LazyFrame.new(Plan::Sort.new(@plan, resolved))
    end

    def distinct(*names : String | Symbol) : LazyFrame
      LazyFrame.new(Plan::Distinct.new(@plan, names.map(&.to_s).to_a))
    end

    # Removes columns, raising `Prism::ColumnNotFound` for an unknown name.
    def drop(*names : String | Symbol) : LazyFrame
      dropped = names.map(&.to_s)
      dropped.each do |name|
        raise ColumnNotFound.new(name, columns) unless columns.includes?(name)
      end

      kept = columns.reject { |name| dropped.includes?(name) }
      self.select(kept.map { |name| Prism.col(name) })
    end

    # Renames columns; unknown source names raise `Prism::ColumnNotFound`.
    def rename(pairs : Hash(String | Symbol, String | Symbol)) : LazyFrame
      mapping = {} of String => String
      pairs.each { |source, target| mapping[source.to_s] = target.to_s }
      mapping.each_key do |from|
        raise ColumnNotFound.new(from, columns) unless columns.includes?(from)
      end

      self.select(
        columns.map do |name|
          replacement = mapping[name]?
          replacement ? Prism.col(name).alias(replacement) : Prism.col(name)
        end
      )
    end

    def rename(from : String | Symbol, to : String | Symbol) : LazyFrame
      rename({from => to})
    end

    # Casts every column.
    def cast(target : Arrow::DataType) : LazyFrame
      rename_targets = {} of String => Arrow::DataType
      columns.each { |name| rename_targets[name] = target }

      cast(rename_targets)
    end

    # Casts the named columns.
    def cast(targets : Hash(String | Symbol, Arrow::DataType)) : LazyFrame
      targets.reduce(self) do |frame, (name, target)|
        column = name.to_s
        raise ColumnNotFound.new(column, columns) unless columns.includes?(column)

        frame.with_column(column, Prism.col(column).cast(target))
      end
    end

    # Replaces nulls in every column that can accept `value`.
    def fill_null(value : LiteralInput) : LazyFrame
      replacement = Prism.lit(value)
      literal_dtype = replacement.dtype(schema)

      targets = {} of String => Expr
      schema.fields.each do |field|
        next unless Arrow.can_cast_types(literal_dtype, field.data_type)

        targets[field.name] = FillNull.new(Prism.col(field.name), replacement)
      end

      fill_replacements(targets)
    end

    # Replaces nulls per column.
    def fill_null(values : Hash(String | Symbol, LiteralInput)) : LazyFrame
      targets = {} of String => Expr
      values.each do |name, value|
        column = name.to_s
        raise ColumnNotFound.new(column, columns) unless columns.includes?(column)

        targets[column] = FillNull.new(Prism.col(column), Prism.lit(value))
      end

      fill_replacements(targets)
    end

    # Drops rows with a null in any of the named columns (all columns by
    # default).
    def drop_nulls(*names : String | Symbol) : LazyFrame
      targets = names.empty? ? columns : names.map(&.to_s)
      targets.each do |name|
        raise ColumnNotFound.new(name, columns) unless columns.includes?(name)
      end
      return self if targets.empty?

      predicate = targets
        .map { |name| Prism.col(name).is_not_null }
        .reduce { |left, right| left.and(right) }

      filter(predicate)
    end

    # Adds several columns in one call.
    def with_columns(**named) : LazyFrame
      result = self
      named.each do |name, expr|
        result = result.with_column(name.to_s, expr.as(Expr))
      end
      result
    end

    def limit(count : Int) : LazyFrame
      raise ArgumentError.new("limit must be >= 0 (got #{count})") if count < 0

      LazyFrame.new(Plan::Limit.new(@plan, 0, count))
    end

    def head(count : Int) : LazyFrame
      limit(count)
    end

    # Keeps the last `count` rows. Cannot be pushed into a scan.
    def tail(count : Int) : LazyFrame
      raise ArgumentError.new("tail must be >= 0 (got #{count})") if count < 0

      LazyFrame.new(Plan::Tail.new(@plan, count.to_i32))
    end

    # `count` rows starting at `offset`.
    def slice(offset : Int, count : Int) : LazyFrame
      raise ArgumentError.new("offset must be >= 0 (got #{offset})") if offset < 0
      raise ArgumentError.new("count must be >= 0 (got #{count})") if count < 0

      LazyFrame.new(Plan::Limit.new(@plan, offset.to_i32, count.to_i32))
    end

    def group_by(
      *keys : String | Symbol | Expr,
      null_keys : Symbol = :first,
      maintain_order : Bool = true,
    ) : LazyGroupBy
      expressions = [] of Expr
      keys.each do |key|
        expressions << (key.is_a?(Expr) ? key : Prism.col(key))
      end

      LazyGroupBy.new(@plan, expressions, null_keys, maintain_order)
    end

    def join(
      other : LazyFrame,
      *,
      on : String | Symbol | Enumerable | Nil = nil,
      left_on : String | Symbol | Enumerable | Nil = nil,
      right_on : String | Symbol | Enumerable | Nil = nil,
      how : Symbol = :inner,
      suffix : String = "_right",
      nulls_equal : Bool = false,
      coalesce : Bool? = nil,
    ) : LazyFrame
      LazyFrame.new(
        Plan::Join.new(
          @plan,
          other.plan,
          on: on,
          left_on: left_on,
          right_on: right_on,
          how: how,
          suffix: suffix,
          nulls_equal: nulls_equal,
          coalesce: coalesce,
        )
      )
    end

    # Accepts a materialized frame by scanning it.
    def join(other : DataFrame, **options) : LazyFrame
      join(other.lazy, **options)
    end

    private def fill_replacements(targets : Hash(String, Expr)) : LazyFrame
      targets.reduce(self) do |frame, (name, expr)|
        frame.with_column(name, expr)
      end
    end

    def collect : DataFrame
      Optimizer.optimize(@plan).execute
    end

    # Renders the plan. Scan lines list the pushdowns that reached the source.
    def explain(optimized : Bool = true) : String
      node = optimized ? Optimizer.optimize(@plan) : @plan
      io = IO::Memory.new
      io << (optimized ? "Plan (optimized):" : "Plan (unoptimized):") << '\n'
      node.describe(io, 1)
      io.to_s
    end

    def to_s(io : IO) : Nil
      io << "LazyFrame(" << columns.size << " columns)"
    end
  end

  # Grouped lazy frame, mirroring `Prism::GroupBy`.
  class LazyGroupBy
    def initialize(
      @input : Plan::Node,
      @keys : Array(Expr),
      @null_keys : Symbol,
      @maintain_order : Bool,
    )
    end

    def aggregate(**named) : LazyFrame
      columns = {} of String => Expr
      named.each { |name, expr| columns[name.to_s] = expr.as(Expr) }

      aggregate(columns)
    end

    def aggregate(columns : Hash(String, Expr)) : LazyFrame
      LazyFrame.new(
        Plan::GroupBy.new(@input, @keys, columns, @null_keys, @maintain_order)
      )
    end

    def agg(*exprs : Expr) : LazyFrame
      columns = {} of String => Expr
      exprs.each do |expr|
        raise MissingAlias.new(expr.key) unless expr.is_a?(Alias)

        columns[expr.as(Alias).name] = expr
      end

      aggregate(columns)
    end
  end

  # Plans a Parquet file. Only the footer is read, to learn the schema.
  def self.scan_parquet(
    path : String,
    *,
    batch_size : Int32 = DEFAULT_BATCH_SIZE,
  ) : LazyFrame
    LazyFrame.new(Plan::ScanParquet.open(path, batch_size: batch_size))
  end

  class DataFrame
    # Defers further work into a plan — the in-memory twin of
    # `Prism.scan_parquet`, and the only way to make a frame lazy (Polars'
    # `df.lazy()`).
    def lazy : LazyFrame
      LazyFrame.new(Plan::ScanBatches.new(@schema, @batches))
    end
  end
end
