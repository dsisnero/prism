require "set"

module Prism
  # Canonical row keys, built with `arrow-cr`'s port of the `arrow-row` crate.
  #
  # `Arrow::RowConverter` encodes one or more columns into big-endian bytes whose
  # `memcmp` order matches the sort order (null sentinel, sign-flipped integers,
  # IEEE `totalOrder` floats, block-encoded strings). Equal keys encode to equal
  # bytes, so a `Hash`/`Set` over encoded rows turns joins, `distinct` and
  # `n_unique` into O(n) passes with no per-row boxing. This is why Arrow has no
  # hashing kernel: `arrow-row` *is* the primitive, and this module is the only
  # place Prism touches it.
  #
  # Three consequences of encoding rather than comparing Crystal values, all
  # intentional:
  #
  # - Two NaN keys **match**. `Float64#==` says otherwise, so the earlier
  #   value-keyed path could not join or dedupe NaNs at all.
  # - `-0.0` and `0.0` encode differently, so they are **distinct** keys. That is
  #   Arrow's total order, and Polars hashes the same physical bits.
  # - Nulls encode like a value, i.e. they form one key of their own. A join that
  #   must *not* match nulls (`nulls_equal: false`) therefore checks the null
  #   bitmaps rather than the encoding (`#complete?`).
  module RowKeys
    extend self

    # A converter for `arrays`, one key column each.
    #
    # `names` is only used to name the offending columns if `arrow-row` cannot
    # encode a dtype (dictionaries of nested values are the one gap upstream
    # leaves open), which raises `Prism::DtypeError` rather than an opaque
    # `Arrow::ArrowError`.
    def converter(
      arrays : Array(Arrow::ArrayBase),
      names : Array(String),
    ) : Arrow::RowConverter
      fields = arrays.map { |array| Arrow::SortField.new(array.data_type) }
      unless Arrow::RowConverter.supports_fields?(fields)
        dtypes = arrays.map(&.data_type.to_s).uniq!.join(", ")
        raise DtypeError.new(
          "row keys #{names.join(", ")} cannot be encoded (unsupported dtype: #{dtypes})"
        )
      end

      Arrow::RowConverter.new(fields)
    end

    # Null bitmaps for the columns that actually carry nulls, so a per-row
    # completeness check costs nothing when the keys have no nulls.
    def null_guards(arrays : Array(Arrow::ArrayBase)) : Array(Arrow::NullBuffer)
      guards = [] of Arrow::NullBuffer
      arrays.each do |array|
        nulls = array.nulls
        guards << nulls if nulls
      end
      guards
    end

    # True when `row` has a value in every guarded column.
    def complete?(guards : Array(Arrow::NullBuffer), row : Int32) : Bool
      guards.all?(&.valid?(row))
    end

    # Row indices of the first occurrence of each distinct key, in input order.
    def first_occurrences(rows : Arrow::Rows) : Array(UInt32)
      seen = Set(Arrow::Row).new(rows.num_rows)
      kept = [] of UInt32
      rows.num_rows.times do |row|
        next unless seen.add?(rows.row(row))

        kept << row.to_u32
      end
      kept
    end

    # Number of distinct keys, counting the null key if there is one.
    def distinct_count(rows : Arrow::Rows) : Int32
      seen = Set(Arrow::Row).new(rows.num_rows)
      rows.num_rows.times { |row| seen.add?(rows.row(row)) }
      seen.size
    end
  end
end
