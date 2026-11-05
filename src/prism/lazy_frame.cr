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
      # The plan's schema is the contract, so a duplicate output name has to fail
      # here rather than when `collect` builds a frame with an unreachable column.
      names = list.map { |expr| Prism::LazyFrame.output_name(expr) }
      Prism.ensure_unique_names!(names, "select")
      LazyFrame.new(Plan::Select.new(@plan, list))
    end

    # The name a selected expression will carry.
    def self.output_name(expr : Expr) : String
      case expr
      when Alias then expr.name
      when Col   then expr.name
      else            raise MissingAlias.new(expr.key)
      end
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

    # Key-list form, mirroring `DataFrame#sort_by(keys : Enumerable(SortKey))`,
    # so a key list built elsewhere can be handed over without splatting.
    def sort_by(keys : Enumerable(SortKey)) : LazyFrame
      LazyFrame.new(Plan::Sort.new(@plan, keys.to_a))
    end

    # Drops duplicate rows across every column, mirroring `DataFrame#distinct`.
    def distinct : LazyFrame
      LazyFrame.new(Plan::Distinct.new(@plan, columns))
    end

    # Drops duplicate rows considering only the named columns.
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
        next unless Coerce.fillable?(field.data_type, literal_dtype)

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

    # Drops rows with a null in any column, mirroring `DataFrame#drop_nulls`.
    def drop_nulls : LazyFrame
      drop_nulls_on(columns)
    end

    # Drops rows with a null in any of the named columns.
    def drop_nulls(*names : String | Symbol) : LazyFrame
      drop_nulls_on(names.map(&.to_s).to_a)
    end

    private def drop_nulls_on(targets : Array(String)) : LazyFrame
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
      # The plan's schema is the contract: two keys with one name would emit two
      # columns of that name, and the second would be unreachable.
      Prism.ensure_unique_names!(
        expressions.map { |key| Prism::GroupBy.key_name(key) },
        "the group_by keys",
      )

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
      # Filling never changes a dtype, so the plan's schema cannot catch an
      # unfillable replacement on its own — check it when the plan is built, as
      # the eager verb does, instead of at `collect`.
      targets.each do |name, expr|
        target = schema.field_with_name(name).data_type
        replacement = expr.dtype(schema)
        next if Coerce.fillable?(target, replacement)

        raise DtypeError.new(
          "cannot fill column #{name.inspect} (#{target}) with #{replacement}; " \
          "cast the replacement explicitly"
        )
      end

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
      # An aggregate reusing a key's name would produce two columns of that name.
      key_names = @keys.map { |key| Prism::GroupBy.key_name(key) }
      columns.each_key do |name|
        next unless key_names.includes?(name)

        raise DuplicateColumn.new(
          name,
          "column #{name.inspect} is a grouping key; an aggregate cannot reuse the name",
        )
      end

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
