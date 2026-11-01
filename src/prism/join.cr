require "set"

module Prism
  # Hash join, following Polars' model:
  #
  # - the **right** side is the build side: its keys become a hash table;
  # - **null keys never match** unless `nulls_equal` is true (SQL semantics);
  # - key columns are **coalesced** (the right copy is dropped) for inner, left
  #   and right joins, and kept apart for `full` unless `coalesce: true`;
  # - `semi`/`anti` are membership filters that emit left rows and left columns
  #   only, so repeated right keys never duplicate rows.
  #
  # Only the key columns are materialized into Crystal values; `arrow-cr`
  # exposes no Arrow-native hashing (`Arrow.partition` needs sorted input). All
  # payload columns stay in Arrow and are gathered with
  # `Arrow.take_record_batch`, which accepts a nullable index array and therefore
  # produces the null rows an outer join needs.
  module Join
    extend self

    VALID_TYPES = {:inner, :left, :right, :full, :outer, :semi, :anti}

    def execute(
      left : DataFrame,
      right : DataFrame,
      on : String | Symbol | Enumerable | Nil,
      left_on : String | Symbol | Enumerable | Nil,
      right_on : String | Symbol | Enumerable | Nil,
      how : Symbol,
      suffix : String,
      nulls_equal : Bool,
      coalesce : Bool?,
    ) : DataFrame
      how = :full if how == :outer
      unless VALID_TYPES.includes?(how)
        raise ArgumentError.new(
          "how must be one of #{VALID_TYPES.to_a.join(", ")} (got #{how})"
        )
      end

      left_keys, right_keys = resolve_keys(on, left_on, right_on)
      left_batch = left.to_arrow
      right_batch = right.to_arrow
      left_key_arrays = fetch_columns(left_batch, left_keys)
      right_key_arrays = fetch_columns(right_batch, right_keys)
      check_dtypes(left_keys, left_key_arrays, right_key_arrays)

      left_rows, right_rows = match(left_key_arrays, right_key_arrays, how, nulls_equal)

      if {:semi, :anti}.includes?(how)
        taken = take_rows(left_batch, left_rows, left_batch.schema)
        return DataFrame.new(left_batch.schema, [taken])
      end

      # Outer joins introduce nulls, so the gathered batches use a relaxed
      # schema; `compose` restores the precise nullability afterwards.
      left_taken = take_rows(left_batch, left_rows, relaxed(left_batch.schema))
      right_taken = take_rows(right_batch, right_rows, relaxed(right_batch.schema))
      should_coalesce = coalesce.nil? ? how != :full : coalesce

      compose(
        left_taken,
        right_taken,
        left_batch.schema,
        right_batch.schema,
        left_keys,
        right_keys,
        how,
        suffix,
        should_coalesce,
      )
    end

    private def match(
      left_keys : Array(Arrow::ArrayBase),
      right_keys : Array(Arrow::ArrayBase),
      how : Symbol,
      nulls_equal : Bool,
    ) : Tuple(Array(UInt32?), Array(UInt32?))
      index = Hash(Array(Value), Array(Int32)).new
      right_count = right_keys.first.len

      right_count.times do |row|
        key = key_at(right_keys, row, nulls_equal)
        next unless key

        (index[key] ||= [] of Int32) << row
      end

      left_rows = [] of UInt32?
      right_rows = [] of UInt32?
      matched_right = Set(Int32).new
      left_count = left_keys.first.len

      left_count.times do |row|
        key = key_at(left_keys, row, nulls_equal)
        candidates = key ? index[key]? : nil

        if candidates
          case how
          when :semi
            left_rows << row.to_u32
          when :anti
            # matched rows are excluded
          else
            candidates.each do |candidate|
              left_rows << row.to_u32
              right_rows << candidate.to_u32
              matched_right << candidate
            end
          end
        else
          case how
          when :anti
            left_rows << row.to_u32
          when :left, :full
            left_rows << row.to_u32
            right_rows << nil
          end
        end
      end

      if how == :right || how == :full
        right_count.times do |row|
          next if matched_right.includes?(row)

          left_rows << nil
          right_rows << row.to_u32
        end
      end

      {left_rows, right_rows}
    end

    # A key is absent (`nil`) when it contains a null and nulls are not equal.
    private def key_at(
      arrays : Array(Arrow::ArrayBase),
      row : Int32,
      nulls_equal : Bool,
    ) : Array(Value)?
      values = arrays.map { |array| Values.extract_at(array, row) }
      return nil if !nulls_equal && values.any?(Nil)

      values
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def compose(
      left_taken : Arrow::RecordBatch,
      right_taken : Arrow::RecordBatch,
      left_schema : Arrow::Schema,
      right_schema : Arrow::Schema,
      left_keys : Array(String),
      right_keys : Array(String),
      how : Symbol,
      suffix : String,
      coalesce : Bool,
    ) : DataFrame
      left_fields = left_schema.fields.to_a
      right_fields = right_schema.fields.to_a
      left_missing = how == :right || how == :full
      right_missing = how == :left || how == :full

      columns = [] of Arrow::ArrayBase
      fields = [] of Arrow::Field
      names = [] of String

      left_fields.each_with_index do |field, index|
        column = left_taken.column(index)

        # A right/full join can leave the left side empty for a row; the key
        # value then comes from the right side instead.
        if coalesce && left_missing && (pair = left_keys.index(field.name))
          right_name = right_keys[pair]
          right_index = right_fields.index { |candidate| candidate.name == right_name }
          if right_index
            other = right_taken.column(right_index)
            column = Datums.zip(Arrow.is_not_null(column), column, other)
          end
        end

        columns << column
        fields << Arrow::Field.new(field.name, column.data_type, left_missing || field.nullable?)
        names << field.name
      end

      right_fields.each_with_index do |field, index|
        next if coalesce && left_keys.includes?(field.name)

        name = field.name
        name = "#{name}#{suffix}" if names.includes?(name)
        raise DuplicateColumn.new(name) if names.includes?(name)

        columns << right_taken.column(index)
        fields << Arrow::Field.new(name, field.data_type, right_missing || field.nullable?)
        names << name
      end

      schema = Arrow::Schema.new(fields)
      DataFrame.new(schema, [Arrow::RecordBatch.try_new(schema, columns)])
    end

    # Gathers rows with a nullable index array, building the batch against
    # `schema`. `Arrow.take_record_batch` cannot be used: it reuses the source
    # schema, which may declare columns non-nullable that an outer join nulls.
    private def take_rows(
      batch : Arrow::RecordBatch,
      rows : Array(UInt32?),
      schema : Arrow::Schema,
    ) : Arrow::RecordBatch
      indices = Arrow::PrimitiveArray(UInt32).from_nullable(rows)
      columns = batch.columns.map { |column| Arrow.take(column, indices) }

      Arrow::RecordBatch.try_new(schema, columns)
    end

    private def relaxed(schema : Arrow::Schema) : Arrow::Schema
      Arrow::Schema.new(
        schema.fields.to_a.map do |field|
          Arrow::Field.new(field.name, field.data_type, true)
        end
      )
    end

    # Normalizes and validates `on:`/`left_on:`/`right_on:` into the left and
    # right key names. Raises `ArgumentError` for an unusable combination.
    def resolve_keys(
      on : String | Symbol | Enumerable | Nil,
      left_on : String | Symbol | Enumerable | Nil,
      right_on : String | Symbol | Enumerable | Nil,
    ) : Tuple(Array(String), Array(String))
      unless on.nil?
        unless left_on.nil? && right_on.nil?
          raise ArgumentError.new("pass either on:, or left_on:/right_on:, not both")
        end

        names = column_names(on)
        raise ArgumentError.new("join needs at least one key") if names.empty?

        return {names, names}
      end

      if left_on.nil? || right_on.nil?
        raise ArgumentError.new("join needs on:, or both left_on: and right_on:")
      end

      left_names = column_names(left_on)
      right_names = column_names(right_on)
      if left_names.size != right_names.size
        raise ArgumentError.new("left_on and right_on must name the same number of keys")
      end

      {left_names, right_names}
    end

    # Normalizes `on:`/`left_on:`/`right_on:` arguments to column names.
    def column_names(value : String | Symbol | Enumerable) : Array(String)
      case value
      when String then [value]
      when Symbol then [value.to_s]
      else             value.map(&.to_s).to_a
      end
    end

    private def fetch_columns(
      batch : Arrow::RecordBatch,
      names : Array(String),
    ) : Array(Arrow::ArrayBase)
      names.map do |name|
        batch.column_by_name(name) || raise ColumnNotFound.new(name, batch.column_names)
      end
    end

    private def check_dtypes(
      names : Array(String),
      left : Array(Arrow::ArrayBase),
      right : Array(Arrow::ArrayBase),
    ) : Nil
      left.each_with_index do |array, index|
        counterpart = right[index]
        next if array.data_type == counterpart.data_type

        raise DtypeError.new(
          "join key #{names[index].inspect} has dtype #{array.data_type} on the left " \
          "and #{counterpart.data_type} on the right"
        )
      end
    end
  end

  class DataFrame
    # Joins two frames on shared or matching keys.
    #
    # ```
    # df.join(other, on: "id", how: :left)
    # df.join(other, left_on: "user_id", right_on: "id", how: :inner)
    # df.join(other, on: ["region", "month"], how: :full, coalesce: true)
    # ```
    #
    # `how` is `:inner` (default), `:left`, `:right`, `:full` (alias `:outer`),
    # `:semi`, or `:anti`. Null keys do not match unless `nulls_equal: true`.
    # Colliding non-key columns from the right side get `suffix` (`_right`).
    def join(
      other : DataFrame,
      *,
      on : String | Symbol | Enumerable | Nil = nil,
      left_on : String | Symbol | Enumerable | Nil = nil,
      right_on : String | Symbol | Enumerable | Nil = nil,
      how : Symbol = :inner,
      suffix : String = "_right",
      nulls_equal : Bool = false,
      coalesce : Bool? = nil,
    ) : DataFrame
      Join.execute(
        self,
        other,
        on,
        left_on,
        right_on,
        how,
        suffix,
        nulls_equal,
        coalesce,
      )
    end
  end
end
