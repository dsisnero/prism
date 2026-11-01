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
          SortKey.new(Prism.col(key), order == :desc, nulls == :first)
        end
      end.to_a

      LazyFrame.new(Plan::Sort.new(@plan, resolved))
    end

    def distinct(*names : String | Symbol) : LazyFrame
      LazyFrame.new(Plan::Distinct.new(@plan, names.map(&.to_s).to_a))
    end

    def limit(count : Int) : LazyFrame
      raise ArgumentError.new("limit must be >= 0 (got #{count})") if count < 0

      LazyFrame.new(Plan::Limit.new(@plan, 0, count))
    end

    def head(count : Int) : LazyFrame
      limit(count)
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
      join(Prism.scan(other), **options)
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

  # Wraps a materialized frame as a lazy source.
  def self.scan(frame : DataFrame) : LazyFrame
    LazyFrame.new(Plan::ScanBatches.new(frame.schema, frame.batches))
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
    # Defers further work into a plan.
    def lazy : LazyFrame
      Prism.scan(self)
    end
  end
end
