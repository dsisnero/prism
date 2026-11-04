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
  # The dependency owns the per-key cases: one column, and a column whose dtype
  # `sort_to_indices` cannot express (struct, map) which `Arrow.lexsort_to_indices`
  # reaches through its comparator chain via `Arrow.can_sort_to_indices?`. What
  # this module adds is the multi-key choice, which has three shapes:
  #
  # - **A bounded window** keeps the bounded lexsort: it selects the prefix in
  #   one pass (`partial_sort`), while the pass sequence below must order every
  #   row to build its least-significant ordering first — a full pass (~110 ms on
  #   1e6 rows) against ~80 ms for the whole bounded lexsort of two keys.
  # - **Two or more variable-length keys** encode the keys into rows once and sort
  #   the encoded bytes (`row_encoded_indices`). The passes would gather each
  #   variable-length key through the current order, and a string gather copies;
  #   encoding happens once instead.
  # - **Exactly one variable-length key** composes stable single-key passes;
  #   **all fixed-width keys** keep the comparator chain. `expensive_to_compare?`
  #   documents the measurements behind that split.
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
      case strategy(columns, limit)
      when :row_encoded then row_encoded_indices(columns)
      when :passes      then stable_lexsort(columns)
      else
        # `:single`, `:bounded` and `:chain` are all the dependency's call: the
        # specialized per-key sort where the dtype fits, its comparator chain
        # otherwise. It also raises for an empty list and for columns of
        # different lengths, which is where both callers should see that error.
        Arrow.lexsort_to_indices(columns, limit)
      end
    end

    # Which shape `indices` takes for `columns`, given an optional `limit`.
    #
    # Each branch is a measurement rather than a preference, so the table is
    # pinned by `spec/prism/sort_engine_spec.cr`: a change here has to be a
    # deliberate edit to the decision, not a side effect of something else.
    def strategy(
      columns : Array(Arrow::SortColumn),
      limit : Int32? = nil,
    ) : Symbol
      return :chain if columns.empty?
      # One key needs no composition: a sortable dtype goes to the specialized
      # `sort_to_indices`, an unsortable one to the comparator chain, both inside
      # `lexsort_to_indices` (arrow-cr `3023eac`).
      return :single if columns.size == 1

      # A bound at or past the row count is no bound at all, so only a real
      # window counts as bounded.
      row_count = columns.first.values.len
      return :bounded if limit && limit < row_count

      variable_length = columns.count { |column| expensive_to_compare?(column.values.data_type) }
      return :row_encoded if variable_length >= 2 && row_encodable?(columns)
      # The pass sequence sorts each key on its own, so every key has to be
      # sortable for it; anything else falls back to the chain, which is the only
      # path that can compare such a key.
      return :chain unless columns.all? { |column| Arrow.can_sort_to_indices?(column.values.data_type) }
      return :passes if variable_length >= 1

      :chain
    end

    # Whether `Arrow::RowConverter` can encode every key, the only precondition
    # row encoding has (`arrow-row` covers both fixed and variable width, nested
    # values and run-end encoded arrays).
    private def row_encodable?(columns : Array(Arrow::SortColumn)) : Bool
      Arrow::RowConverter.supports_fields?(row_sort_fields(columns))
    end

    # One `Arrow::SortField` per key, carrying that key's direction and null
    # placement: `arrow-row` encodes those into the bytes, so `memcmp` order is
    # the requested order.
    private def row_sort_fields(columns : Array(Arrow::SortColumn)) : Array(Arrow::SortField)
      columns.map do |column|
        Arrow::SortField.new(column.values.data_type, column.options || Arrow::SortOptions.default)
      end
    end

    # Multi-key sort by encoded rows: encode each key once, sort the row bytes.
    #
    # This is what Polars does by default (`arg_sort_multiple` -> `_get_rows_encoded`
    # -> `argsort_multiple_row_fmt`), and `Prism::RowKeys` already relies on the
    # same encoding for keyed verbs, so the format is not new here — only its use
    # for ordering.
    #
    # It wins where the pass sequence is expensive, and the passes are expensive
    # when they have to *gather* a key through the current order, because a gather
    # of a string column copies the strings. Paired end-to-end on 1e6 rows, two
    # prebuilt binaries interleaved over three rounds, best of five each (the
    # aggregate 1.37x/1.26x rows repeated every round; the others are noise):
    #
    # | scenario | Phase 22 paths | row encoded |
    # | --- | ---: | ---: |
    # | `sort_by(Utf8, Utf8)` | 519.4 ms | 377.9 ms |
    # | `group_by(Utf8, Utf8)` | 615.7 ms | 485.1 ms |
    # | `sort_by(Utf8, Int64)` (one variable key) | 451.4 ms | 441.0 ms |
    # | `sort_by(Int64, Int64)` (fixed width) | 416.8 ms | 419.8 ms |
    # | `top_n(10, Utf8, Utf8)` (bounded) | 154.3 ms | 154.4 ms |
    #
    # So the rule is two or more variable-length keys: with one variable key the
    # encoding buys nothing (the same single gather the output already pays for),
    # and with none it is pure overhead. `strategy` encodes that rule; this method
    # assumes it has been applied.
    private def row_encoded_indices(columns : Array(Arrow::SortColumn)) : Arrow::PrimitiveArray(UInt32)
      rows = Arrow::RowConverter.new(row_sort_fields(columns)).convert_columns(columns.map(&.values))
      buffer = rows.buffer
      offsets = rows.offsets
      items = Array(Tuple(Bytes, UInt32)).new(rows.num_rows) do |index|
        {buffer[offsets[index], offsets[index + 1] - offsets[index]], index.to_u32}
      end
      # Equal bytes are equal keys, so the index breaks the tie: the order is the
      # stable one a stable sort of the values would give.
      items.sort! do |left, right|
        comparison = left[0] <=> right[0]
        comparison == 0 ? left[1] <=> right[1] : comparison
      end
      Arrow::PrimitiveArray(UInt32).from_values(items.map(&.[1]))
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
    #
    # The string cost is a property of this dependency, not of Arrow: the port
    # materializes a `String`/`Bytes` per row and compares full slices, where
    # upstream `sort_bytes` compares a 4-byte big-endian prefix first and never
    # copies. Upstream also special-cases 2–5 columns (`sort_fixed_column::<N>`)
    # where this port keeps a generic comparator loop. So the threshold is
    # calibrated to `arrow-cr`, and a dtype that moves to the other side should
    # move with a paired measurement, not by reasoning about Arrow semantics.
    #
    # The comparison-free alternative — encode every key into row bytes once and
    # `memcmp` them — is `row_encoded_indices`, and it takes over once two keys are
    # variable length, where the passes' gathers cost more than the encoding.
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
    # The stability each pass needs is a property of this dependency and is worth
    # being explicit about: the port builds `(key, index)` tuples and compares key
    # then index, so equal keys keep ascending input order by construction.
    # Upstream `sort_to_indices` compares values only and sorts unstably, so the
    # same assumption would not hold against arrow-rs — and Polars makes the same
    # choice dynamic (`maintain_order`: stable sort when true, unstable otherwise).
    # Prism's contract is "ties keep input order", which this dependency satisfies;
    # `spec/prism/verbs_spec.cr` pins it from the outside.
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
