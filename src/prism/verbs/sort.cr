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

  # Resolves a sort key written as a column name.
  #
  # A leading `-` means descending, which is RedAmber's shorthand:
  # `sort_by("-price")`. That is the only place the prefix is read, so a column
  # whose name really starts with `-` is reachable only as a `SortKey`
  # (`sort_by(col("-weird").asc)`) — the documented cost of the sugar.
  # `order:` and `nulls:` still apply to keys without a prefix, so `-` overrides
  # direction alone and never null placement.
  def self.sort_key(
    name : String | Symbol,
    order : Symbol = :asc,
    nulls : Symbol = :last,
  ) : SortKey
    text = name.to_s
    descending = order == :desc
    if text.size > 1 && text.starts_with?('-')
      descending = true
      text = text[1..]
    end

    SortKey.new(col(text), descending, nulls == :first)
  end

  class DataFrame
    # Sorts rows.
    #
    # ```
    # df.sort_by("price", order: :desc)
    # df.sort_by("-price") # descending shorthand
    # df.sort_by(col("region").asc, col("qty").desc)
    # ```
    #
    # A leading `-` on a column name sorts that key descending; see
    # `Prism.sort_key`. Nulls go last unless `nulls: :first`. Sorting reorders
    # rows, so the frame collapses to a single chunk.
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
          Prism.sort_key(key, order, nulls)
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

      # Keep the window in `Int64`: an offset past the frame is legal and can be
      # larger than `Int32::MAX`, and only the sort bound is ever narrowed.
      sort_by_keys(keys.to_a, offset.to_i64, count.to_i64)
    end

    private def sort_by_keys(
      keys : Array(SortKey),
      offset : Int64? = nil,
      count : Int64? = nil,
    ) : DataFrame
      raise ArgumentError.new("sort_by needs at least one key") if keys.empty?

      batch = to_arrow
      windowed = !!(offset || count)
      start = offset || 0_i64
      available = Math.max(batch.num_rows.to_i64 - start, 0)
      length = count.nil? ? available : Math.min(count, available)

      # A window needs only the first `start + length` sorted rows, so hand the
      # sort a bound and let `Arrow.lexsort_to_indices` select it instead of
      # sorting every row (a bounded `limit` uses `partial_sort`; §14). The bound
      # is capped at the row count — it never asks for rows that do not exist, so
      # the output is unchanged (the bounded prefix *is* the prefix a full sort
      # produces, tie-break included) and a huge offset cannot overflow the cast.
      # The sort still runs for an empty window: resolving the keys is what
      # rejects an unknown column, and returning before it would swallow that.
      limit = windowed ? Math.min(start + length, batch.num_rows.to_i64).to_i32 : nil
      indices = if batch.num_rows <= 1
                  nil
                else
                  sort_indices(batch, keys, limit)
                end

      unless windowed
        return self if indices.nil?

        return DataFrame.new(@schema, [Arrow.take_record_batch(batch, indices)])
      end

      return DataFrame.new(@schema, [] of Arrow::RecordBatch) if length <= 0

      if indices
        window = indices.slice(start.to_i32, length.to_i32)
        DataFrame.new(@schema, [Arrow.take_record_batch(batch, window)])
      else
        slice(start.to_i32, length.to_i32)
      end
    end

    # Sort indices for one batch, resolved through the expression evaluator.
    #
    # `limit` bounds how many indices come back; the sort uses it to skip rows
    # that cannot land in the window.
    private def sort_indices(
      batch : Arrow::RecordBatch,
      keys : Array(SortKey),
      limit : Int32? = nil,
    ) : Arrow::PrimitiveArray(UInt32)
      types = TypeCache.new(@schema)
      duplicates = Cse.duplicate_keys(keys.map(&.expr))
      ctx = EvalContext.new(batch, types, duplicates)
      columns = keys.map do |key|
        values = Datums.array(ctx.evaluate(key.expr), ctx)
        options = Arrow::SortOptions.new(key.descending?, key.nulls_first?)
        Arrow::SortColumn.new(values, options)
      end

      Arrow.lexsort_to_indices(columns, limit)
    end
  end
end
