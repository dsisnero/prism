module Prism
  # How a sort is executed, in one place.
  #
  # Two callers sort by several columns and they must produce the same order:
  # `DataFrame#sort_by` (also the path behind `top_n` and `sort_by_slice`) and
  # `GroupBy`'s key sort, which relies on the sorted keys having contiguous,
  # comparable ranges so one pass can reduce every group. Keeping the dispatch
  # here means a change to the strategy cannot leave one caller behind — and
  # `GroupBy` is where the difference shows up most, since its key sort is most
  # of a multi-key group-by's cost.
  #
  # Three strategies, chosen by what the sort has to produce:
  #
  # - **One key** goes straight to `Arrow.lexsort_to_indices`, which hands a
  #   single column to the dtype-specialized `sort_to_indices` anyway.
  # - **A bounded window** keeps the bounded lexsort: it selects the prefix in
  #   one pass (`partial_sort`), while the pass sequence below must order every
  #   row to build its least-significant ordering first — a full pass (~110 ms on
  #   1e6 rows) against ~80 ms for the whole bounded lexsort of two keys.
  # - **Multi-key** sorts split on how expensive a comparison is, which the
  #   measurements below justify: variable-length (or nested) keys compose stable
  #   single-key passes, fixed-width keys keep the comparator chain. A key's dtype
  #   that only `make_comparator` can order (`per_key_sortable?`) also keeps the
  #   chain.
  module SortEngine
    extend self

    # Sorted row indices for `columns`, compared in the order given.
    #
    # `limit` bounds how many indices come back; a caller passes it when rows
    # outside the window cannot matter. The result is a stable lexicographic
    # order either way: equal rows keep their input order.
    def indices(
      columns : Array(Arrow::SortColumn),
      limit : Int32? = nil,
    ) : Arrow::PrimitiveArray(UInt32)
      # `lexsort_to_indices` raises for an empty list and for columns of
      # different lengths; let it, so both callers see Arrow's own error.
      return Arrow.lexsort_to_indices(columns, limit) if columns.empty?

      sortable = columns.all? { |column| per_key_sortable?(column.values) }
      # One key the per-key sort cannot express has no chain to fall back on:
      # with a single column `lexsort_to_indices` forwards to `sort_to_indices`
      # and raises, so the values are compared directly (Phase 23).
      return comparator_sort(columns.first, limit) if columns.size == 1 && !sortable
      # A single sortable key already is one dtype-specialized sort.
      return Arrow.lexsort_to_indices(columns, limit) if columns.size == 1

      # A bounded window, a dtype only the chain can order, and all-fixed-width
      # keys all take the chain; only variable-length keys pay for the passes.
      # A bound at or past the row count is no bound at all, so it still counts
      # as unbounded here: the passes return the full order either way.
      row_count = columns.first.values.len
      unbounded = limit.nil? || limit >= row_count
      return Arrow.lexsort_to_indices(columns, limit) unless unbounded
      return Arrow.lexsort_to_indices(columns, limit) unless sortable
      unless columns.any? { |column| expensive_to_compare?(column.values.data_type) }
        return Arrow.lexsort_to_indices(columns, limit)
      end

      stable_lexsort(columns)
    end

    # Sorts one column with the comparator `Arrow.make_comparator` builds, for the
    # dtypes `Arrow.sort_to_indices` has no case for: struct, map, decimal,
    # interval, run-end and union keys are comparable but not sortable on their
    # own. That also unblocks `group_by` on such a key, which is legal everywhere
    # else in Arrow — `Arrow.partition` falls back to the same comparator.
    #
    # `limit` truncates after the full order rather than bounding the sort: this
    # is the rare path, and a bounded selection through `make_comparator` would
    # mean reimplementing `sort_bounded` for a comparison this module does not
    # own. The prefix it returns is the same one a bounded sort would.
    private def comparator_sort(
      column : Arrow::SortColumn,
      limit : Int32?,
    ) : Arrow::PrimitiveArray(UInt32)
      values = column.values
      compare = Arrow.make_comparator(values, values, column.options || Arrow::SortOptions.default)
      indices = Array(UInt32).new(values.len, &.to_u32)
      indices.sort! do |left, right|
        result = compare.call(left.to_i32, right.to_i32)
        # Tie-break by position, which makes this a stable order and therefore
        # the same order a stable sort of the same values would return.
        result == 0 ? left.to_i32 <=> right.to_i32 : result
      end

      length = limit ? Math.min(limit, indices.size) : indices.size
      Arrow::PrimitiveArray(UInt32).from_values(indices[0, length])
    end

    # `Arrow.sort_to_indices` dispatches on the array class and covers fewer
    # dtypes than `Arrow.make_comparator`, which the multi-key chain uses: a
    # struct, map, decimal128/256, interval, run-end or union key is comparable
    # but not sortable on its own. Mirroring that dispatch here keeps those keys
    # on the comparator path instead of raising "Sort not supported".
    def per_key_sortable?(values : Arrow::ArrayBase) : Bool
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

    # Whether comparing two values of `type` costs more than a fixed-width
    # compare, which is what decides the multi-key strategy.
    #
    # The chain's cost is per comparison, so it wins when a single comparison
    # settles the order — the common case with fixed-width keys. The passes'
    # cost is one full sort per key, and that is only worth paying when a
    # comparison has to walk bytes or elements. Measured on 1e6 rows, paired,
    # best of five (`docs/benchmarks.md`):
    #
    # | keys | chain | passes |
    # | --- | ---: | ---: |
    # | `Utf8`(4 values) + `Int64` | 877 ms | 293 ms |
    # | `Utf8`(1e3) + `Utf8` | 1146 ms | 367 ms |
    # | `Utf8`(4) + `Float64` | 911 ms | 329 ms |
    # | `Int64`(1e5) + `Float64` | 224 ms | 301 ms |
    # | `Int64`(1e3) + `Int64` | 277 ms | 248 ms |
    # | `Float64`(4) + `Float64` | 325 ms | 257 ms |
    #
    # Fixed-width keys land within ~15% of each other in either direction, so
    # they keep the chain (the cheaper of the two on high-cardinality first
    # keys); a variable-length key makes the chain 2.5–3x slower and the passes
    # win outright. `FixedSizeBinary` counts as fixed: it compares fixed bytes.
    def expensive_to_compare?(type : Arrow::DataType) : Bool
      case type.kind
      when .utf8?, .large_utf8?, .utf8_view?,
           .binary?, .large_binary?, .binary_view?,
           .list?, .list_view?, .large_list?, .large_list_view?, .fixed_size_list?
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
