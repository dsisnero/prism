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
      sort_by_keys(resolve_sort_keys(keys, order, nulls))
    end

    # Keeps the first `count` rows after sorting — `sort_by(...).head(count)`,
    # but the sort selects only that prefix instead of ordering every row
    # (§14: 190 ms → 8 ms on 1e6 rows).
    #
    # `order: :desc` is the default because "top" means largest here, matching
    # `top_k` in Polars and `top_n` in dplyr; `sort_by` keeps its ascending
    # default, so the two verbs differ only in where they start. Pass
    # `order: :asc` for the smallest rows.
    #
    # ```
    # df.top_n(10, "revenue")              # 10 largest
    # df.top_n(10, "latency", order: :asc) # 10 smallest
    # df.top_n(5, col("region").asc, col("qty").desc)
    # ```
    #
    # This exists because eager code otherwise pays the full sort and gathers
    # every row before trimming: `sort_by(...).head(n)` is two verbs, and the
    # second one cannot undo the first. The lazy spelling
    # (`lazy.sort_by(...).head(n)`) fuses the two in the plan; `top_n` is the
    # eager equivalent, and both land in `#sort_by_slice`.
    def top_n(
      count : Int,
      *keys : String | Symbol | SortKey,
      order : Symbol = :desc,
      nulls : Symbol = :last,
    ) : DataFrame
      raise ArgumentError.new("count must be >= 0 (got #{count})") if count < 0

      sort_by_slice(resolve_sort_keys(keys, order, nulls), 0, count)
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

    # Sort keys written as names, expressions or `SortKey`s, resolved once for
    # both `sort_by` and `top_n` so the two verbs cannot drift on the `-` prefix
    # or the null-placement rule.
    private def resolve_sort_keys(
      keys : Enumerable(String | Symbol | SortKey),
      order : Symbol,
      nulls : Symbol,
    ) : Array(SortKey)
      keys.map do |key|
        case key
        when SortKey
          key
        else
          Prism.sort_key(key, order, nulls)
        end
      end.to_a
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

      # One key needs no composition, and a bounded window keeps arrow-cr's
      # bounded lexsort: that selects the prefix in one pass, while the pass
      # sequence below has to order every row to build its least-significant
      # ordering first (a full pass is ~110 ms against ~80 ms for the whole
      # bounded lexsort of two keys).
      return Arrow.lexsort_to_indices(columns, limit) if columns.size == 1
      return Arrow.lexsort_to_indices(columns, limit) if limit && limit < batch.num_rows
      # Keys the per-key sort cannot handle keep the comparator chain, which
      # supports more dtypes (nested, decimal, interval, run-end, union).
      unless columns.all? { |column| per_key_sortable?(column.values) }
        return Arrow.lexsort_to_indices(columns, limit)
      end

      stable_lexsort(columns)
    end

    # `Arrow.sort_to_indices` dispatches on the array class and covers fewer
    # dtypes than `Arrow.make_comparator`, which the multi-key chain uses: a
    # struct, map, decimal128/256, interval, run-end or union key is comparable
    # but not sortable on its own. Mirroring that dispatch here keeps those keys
    # on the comparator path instead of raising "Sort not supported".
    private def per_key_sortable?(values : Arrow::ArrayBase) : Bool
      case values
      when Arrow::PrimitiveArray(Int8), Arrow::PrimitiveArray(Int16),
           Arrow::PrimitiveArray(Int32), Arrow::PrimitiveArray(Int64),
           Arrow::PrimitiveArray(UInt8), Arrow::PrimitiveArray(UInt16),
           Arrow::PrimitiveArray(UInt32), Arrow::PrimitiveArray(UInt64),
           Arrow::PrimitiveArray(Arrow::Float16), Arrow::PrimitiveArray(Float32),
           Arrow::PrimitiveArray(Float64),
           Arrow::BooleanArray,
           Arrow::GenericStringArray(Int32), Arrow::GenericStringArray(Int64),
           Arrow::GenericBinaryArray(Int32), Arrow::GenericBinaryArray(Int64),
           Arrow::FixedSizeBinaryArray,
           Arrow::StringViewArray, Arrow::BinaryViewArray,
           Arrow::GenericListArray(Int32), Arrow::GenericListArray(Int64),
           Arrow::GenericListViewArray(Int32), Arrow::GenericListViewArray(Int64),
           Arrow::FixedSizeListArray,
           Arrow::DictionaryArray(Int8), Arrow::DictionaryArray(Int16),
           Arrow::DictionaryArray(Int32), Arrow::DictionaryArray(Int64),
           Arrow::DictionaryArray(UInt8), Arrow::DictionaryArray(UInt16),
           Arrow::DictionaryArray(UInt32), Arrow::DictionaryArray(UInt64)
        true
      else
        false
      end
    end

    # Multi-key sort as stable single-key passes, least significant key first.
    #
    # Sorting by the last key and then re-sorting stably by each earlier key
    # yields exactly the order one lexicographic comparison produces, and each
    # pass is a dtype-specialized `Arrow.sort_to_indices` (which breaks ties by
    # index, so it is stable) instead of a per-comparison chain of procs. On 1e6
    # rows that is 360 ms for two keys against 900 ms for the chain, with the same
    # indices out.
    #
    # `Arrow.sort_to_indices` sorts an array's own positions, so a refinement pass
    # first gathers the key through the current order (`Arrow.take`) and then
    # applies the resulting permutation to it.
    private def stable_lexsort(columns : Array(Arrow::SortColumn)) : Arrow::PrimitiveArray(UInt32)
      last = columns.last
      order = Arrow.sort_to_indices(last.values, last.options)
      (columns.size - 2).downto(0) do |position|
        column = columns[position]
        values = Arrow.take(column.values, order.as(Arrow::ArrayBase))
        refinement = Arrow.sort_to_indices(values, column.options)
        order = Arrow.take(order.as(Arrow::ArrayBase), refinement.as(Arrow::ArrayBase))
          .as(Arrow::PrimitiveArray(UInt32))
      end
      order
    end
  end
end
