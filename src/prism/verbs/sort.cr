module Prism
  # One sort key: an expression, its direction, and where nulls go.
  #
  # Build them with `Expr#asc` / `Expr#desc`, or pass column names to
  # `DataFrame#sort_by`.
  struct SortKey
    getter expr : Expr
    getter? descending : Bool
    getter? nulls_first : Bool

    def initialize(
      @expr : Expr,
      @descending : Bool = false,
      @nulls_first : Bool = false,
    )
    end
  end

  abstract class Expr
    def asc(nulls : Symbol = :last) : SortKey
      SortKey.new(self, false, nulls == :first)
    end

    def desc(nulls : Symbol = :last) : SortKey
      SortKey.new(self, true, nulls == :first)
    end
  end

  class DataFrame
    # Sorts rows.
    #
    # ```
    # df.sort_by("price", order: :desc)
    # df.sort_by(col("region").asc, col("qty").desc)
    # ```
    #
    # Nulls go last unless `nulls: :first`. Sorting reorders rows, so the frame
    # collapses to a single chunk.
    def sort_by(
      *keys : String | Symbol | SortKey,
      order : Symbol = :asc,
      nulls : Symbol = :last,
    ) : DataFrame
      resolved = keys.map do |key|
        case key
        when SortKey
          key
        else
          SortKey.new(Prism.col(key), order == :desc, nulls == :first)
        end
      end

      sort_by_keys(resolved.to_a)
    end

    # Key-list form, used by the planner.
    def sort_by(keys : Enumerable(SortKey)) : DataFrame
      sort_by_keys(keys.to_a)
    end

    # Sorts and keeps `count` rows starting at `offset`.
    #
    # This is what a slice pinned to a `Plan::Sort` executes: only the window's
    # sort indices are gathered, so a `sort + head(10)` copies 10 rows instead
    # of every row. The rows are identical to
    # `sort_by(keys).slice(offset, count)`.
    def sort_by_slice(
      keys : Enumerable(SortKey),
      offset : Int,
      count : Int,
    ) : DataFrame
      raise ArgumentError.new("offset must be >= 0 (got #{offset})") if offset < 0
      raise ArgumentError.new("count must be >= 0 (got #{count})") if count < 0

      sort_by_keys(keys.to_a, offset.to_i32, count.to_i32)
    end

    private def sort_by_keys(
      keys : Array(SortKey),
      offset : Int32? = nil,
      count : Int32? = nil,
    ) : DataFrame
      raise ArgumentError.new("sort_by needs at least one key") if keys.empty?

      batch = to_arrow
      indices = if batch.num_rows <= 1
                  nil
                else
                  sort_indices(batch, keys)
                end

      unless offset || count
        return self if indices.nil?

        return DataFrame.new(@schema, [Arrow.take_record_batch(batch, indices)])
      end

      start = (offset || 0).to_i64
      available = Math.max(batch.num_rows.to_i64 - start, 0)
      length = count.nil? ? available : Math.min(count.to_i64, available)
      return DataFrame.new(@schema, [] of Arrow::RecordBatch) if length <= 0

      if indices
        window = indices.slice(start.to_i32, length.to_i32)
        DataFrame.new(@schema, [Arrow.take_record_batch(batch, window)])
      else
        slice(start.to_i32, length.to_i32)
      end
    end

    # Sort indices for one batch, resolved through the expression evaluator.
    private def sort_indices(batch : Arrow::RecordBatch, keys : Array(SortKey)) : Arrow::PrimitiveArray(UInt32)
      types = TypeCache.new(@schema)
      duplicates = Cse.duplicate_keys(keys.map(&.expr))
      ctx = EvalContext.new(batch, types, duplicates)
      columns = keys.map do |key|
        values = Datums.array(ctx.evaluate(key.expr), ctx)
        options = Arrow::SortOptions.new(key.descending?, key.nulls_first?)
        Arrow::SortColumn.new(values, options)
      end

      Arrow.lexsort_to_indices(columns)
    end
  end
end
