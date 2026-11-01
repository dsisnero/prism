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

    private def sort_by_keys(keys : Array(SortKey)) : DataFrame
      raise ArgumentError.new("sort_by needs at least one key") if keys.empty?

      batch = to_arrow
      return self if batch.num_rows <= 1

      types = TypeCache.new(@schema)
      duplicates = Cse.duplicate_keys(keys.map(&.expr))
      ctx = EvalContext.new(batch, types, duplicates)
      columns = keys.map do |key|
        values = Datums.array(ctx.evaluate(key.expr), ctx)
        options = Arrow::SortOptions.new(key.descending?, key.nulls_first?)
        Arrow::SortColumn.new(values, options)
      end

      indices = Arrow.lexsort_to_indices(columns)
      DataFrame.new(@schema, [Arrow.take_record_batch(batch, indices)])
    end
  end
end
