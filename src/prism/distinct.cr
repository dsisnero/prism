module Prism
  # Distinct-row machinery shared by `DataFrame#distinct` and `Series#unique`.
  #
  # There is no dedupe *kernel* in `arrow-cr` (nor in `arrow-rs`). What exists is:
  # `Arrow.partition`, which compares adjacent rows with the typed
  # `cmp::distinct` kernel and returns run boundaries; `Arrow.sort_to_indices`,
  # which dispatches to typed per-dtype sorts; and `ArrayBase#uniq`, which is a
  # Crystal-level sort driven by a `DynComparator` closure — correct, but the
  # slowest of the three.
  #
  # So "first occurrence per group" is built from Arrow kernels: sort indices,
  # partition the sorted keys, then restore the original order of the chosen
  # rows. Polars' `unique_stable` uses a Rust hash set instead; the non-stable
  # `unique` sorts, which is the shape used here.
  module Distinct
    extend self

    # For each run in `ranges` (positions in sorted order), the smallest original
    # row index — i.e. the first occurrence — returned ascending so the result
    # keeps the input order.
    #
    # The minimum scan is needed because Arrow's sort is not guaranteed stable,
    # so a run's first sorted position is not necessarily its earliest row.
    def first_occurrence_indices(
      order : Arrow::ArrayBase,
      ranges : Array(Range(Int32, Int32)),
    ) : Array(UInt32)
      positions = order.as(Arrow::PrimitiveArray(UInt32))
      kept = ranges.map do |range|
        (range.begin...range.end).min_of { |position| positions.value(position) }
      end

      kept.sort!
    end
  end
end
