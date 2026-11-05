module Prism
  class DataFrame
    # Adds `name`, or replaces it in place when it already exists.
    #
    # Only the new column is computed; existing arrays are reused as-is.
    def with_column(name : String | Symbol, expr : Expr) : DataFrame
      column = name.to_s
      types = TypeCache.new(@schema)
      field = Prism.output_field(expr, column, @schema)
      index = @schema.fields.to_a.index { |existing| existing.name == column }

      fields = @schema.fields.to_a
      if index
        fields[index] = field
      else
        fields << field
      end
      schema = Arrow::Schema.new(fields)

      duplicates = Cse.duplicate_keys([expr])
      batches = @batches.map do |batch|
        ctx = EvalContext.new(batch, types, duplicates)
        values = Datums.array(ctx.evaluate(expr), ctx)
        columns = batch.columns.dup

        if index
          columns[index] = values
        else
          columns << values
        end

        Arrow::RecordBatch.try_new(schema, columns)
      end

      DataFrame.new(schema, batches)
    end

    # Block form: `df.with_column("total") { col("price") * col("qty") }`.
    def with_column(name : String | Symbol, &) : DataFrame
      with_column(name, yield)
    end

    # Adds several columns in one call, using named arguments.
    #
    # ```
    # df.with_columns(total: col("price") * col("qty"), cheap: col("price") < 5)
    # ```
    #
    # `**named` collects the arguments into a `NamedTuple`. This is a plain
    # method, not a macro: a macro defined on the class expands at the call site
    # without the receiver, so it could not chain onto this frame.
    def with_columns(**named) : DataFrame
      result = self
      named.each do |name, expr|
        result = result.with_column(name.to_s, expr.as(Expr))
      end
      result
    end

    # Dynamic variant: column names are only known at runtime.
    def with_columns(columns : Hash(String, Expr)) : DataFrame
      columns.reduce(self) do |frame, (name, expr)|
        frame.with_column(name, expr)
      end
    end

    # Removes columns. Raises `Prism::ColumnNotFound` for an unknown name.
    def drop(*names : String | Symbol) : DataFrame
      dropped = names.map(&.to_s)
      dropped.each { |name| require_column!(name) }

      kept = @schema.fields.to_a.reject { |field| dropped.includes?(field.name) }
      schema = Arrow::Schema.new(kept)
      kept_names = kept.map(&.name)

      batches = @batches.map do |batch|
        columns = kept_names.map do |name|
          batch.column_by_name(name) || raise ColumnNotFound.new(name, batch.column_names)
        end

        build_batch(schema, columns, batch.num_rows)
      end

      DataFrame.new(schema, batches)
    end

    # Renames columns. Values are not touched: only the schema changes, and each
    # batch is re-labelled, so no buffer is copied.
    def rename(pairs : Hash(String | Symbol, String | Symbol)) : DataFrame
      normalized = pairs.each_with_object({} of String => String) do |(from, to), memo|
        memo[from.to_s] = to.to_s
      end
      normalized.each_key { |name| require_column!(name) }

      fields = @schema.fields.to_a.map do |field|
        replacement = normalized[field.name]?
        # `with_name` returns a copy, so the source frame's schema is untouched
        # (C16).
        replacement ? field.with_name(replacement) : field
      end
      # A swap (`a -> b`, `b -> a`) is legal because it is checked on the result;
      # renaming onto a name that survives is not.
      Prism.ensure_unique_names!(fields.map(&.name), "rename")
      schema = Arrow::Schema.new(fields)

      # `RecordBatch#with_schema` requires the new schema to be a *superset*, so
      # it cannot express a rename. Rebuild the batch around the same arrays.
      batches = @batches.map do |batch|
        Arrow::RecordBatch.new(schema, batch.columns, batch.num_rows)
      end

      DataFrame.new(schema, batches)
    end

    # Renames one column.
    def rename(from : String | Symbol, to : String | Symbol) : DataFrame
      rename({from => to})
    end

    # Casts every column.
    def cast(target : Arrow::DataType, *, strict : Bool = true) : DataFrame
      rename_targets = column_names.each_with_object({} of String => Arrow::DataType) do |name, memo|
        memo[name] = target
      end

      cast(rename_targets, strict: strict)
    end

    # Casts the named columns, leaving the rest untouched.
    #
    # With `strict: true` (the default, and the only behaviour until Phase 35)
    # the output field keeps the input field's nullability, which is honest
    # because the cast refuses to invent nulls: a value the target dtype cannot
    # represent raises `Prism::DtypeError` naming the column rather than becoming
    # null (`Coerce.cast_exact`).
    #
    # `strict: false` is Polars' `strict=False`: an unrepresentable value becomes
    # null, and the column is declared nullable because the cast can now produce
    # nulls. It is the escape hatch for cleaning a dirty column, and it is
    # deliberately not the default.
    def cast(
      targets : Hash(String | Symbol, Arrow::DataType),
      *,
      strict : Bool = true,
    ) : DataFrame
      targets.each_key { |key| require_column!(key.to_s) }

      fields = @schema.fields.to_a.map do |field|
        target = targets[field.name]?
        next field unless target
        next field if field.data_type == target

        expr = Cast.new(Prism.col(field.name), target, strict)
        Prism.output_field(expr, field.name, @schema)
      end
      schema = Arrow::Schema.new(fields)

      batches = @batches.map do |batch|
        columns = batch.columns.map_with_index do |column, index|
          name = batch.schema.field(index).name
          target = targets[name]?
          next column unless target && target != column.data_type

          casted = if strict
                     Coerce.cast_exact(column, target, "column #{name.inspect}")
                   else
                     Coerce.cast(column, target)
                   end
          casted.as(Arrow::ArrayBase)
        end

        Arrow::RecordBatch.try_new(schema, columns)
      end

      DataFrame.new(schema, batches)
    end

    # Replaces nulls in every column that can accept `value`.
    def fill_null(value : LiteralInput) : DataFrame
      replacement = Prism.lit(value)
      literal_dtype = replacement.dtype(@schema)

      targets = @schema.fields.each_with_object({} of String => Expr) do |field, memo|
        next unless Coerce.fillable?(field.data_type, literal_dtype)

        memo[field.name] = FillNull.new(Prism.col(field.name), replacement)
      end

      fill_replacements(targets)
    end

    # Replaces nulls per column.
    def fill_null(values : Hash(String | Symbol, LiteralInput)) : DataFrame
      targets = values.each_with_object({} of String => Expr) do |(name, value), memo|
        column = name.to_s
        require_column!(column)
        memo[column] = FillNull.new(Prism.col(column), Prism.lit(value))
      end

      fill_replacements(targets)
    end

    # Prepends a zero-based row index column.
    #
    # Numbering continues across chunks, so a multi-chunk frame gets one
    # sequence rather than one per chunk.
    def with_row_index(name : String | Symbol = "index", offset : Int = 0) : DataFrame
      column = name.to_s
      raise ArgumentError.new("offset must be >= 0 (got #{offset})") if offset < 0
      raise DuplicateColumn.new(column) if @schema.fields.find_field(column)

      field = Arrow::Field.new(column, Arrow::DataType.uint32, false)
      schema = Arrow::Schema.new([field] + @schema.fields.to_a)

      running = offset.to_u64
      batches = @batches.map do |batch|
        count = batch.num_rows
        values = Array(UInt32).new(count) { |i| (running + i.to_u64).to_u32 }
        running += count.to_u64

        indices = Arrow::PrimitiveArray(UInt32).from_values(values).as(Arrow::ArrayBase)
        Arrow::RecordBatch.try_new(schema, [indices] + batch.columns)
      end

      DataFrame.new(schema, batches)
    end

    private def fill_replacements(replacements : Hash(String, Expr)) : DataFrame
      return self if replacements.empty?

      types = TypeCache.new(@schema)
      # Filling never changes a dtype, and `Prism.output_field` is what says so:
      # a `FillNull` field keeps the column's dtype and drops its nullability.
      # Building the fields first also runs `FillNull#dtype`, which is what
      # rejects an unfillable replacement (a scalar into a list, a float into an
      # integer column) before any batch is built — otherwise the coercion
      # happens in `Coerce.cast` at eval time and silently succeeds.
      fields = @schema.fields.to_a.map do |field|
        expr = replacements[field.name]?
        expr ? Prism.output_field(expr, field.name, @schema) : field
      end
      schema = Arrow::Schema.new(fields)

      duplicates = Cse.duplicate_keys(replacements.values)
      batches = @batches.map do |batch|
        ctx = EvalContext.new(batch, types, duplicates)
        columns = batch.columns.map_with_index do |column, index|
          expr = replacements[batch.schema.field(index).name]?
          next column unless expr

          Datums.array(ctx.evaluate(expr), ctx)
        end

        Arrow::RecordBatch.try_new(schema, columns)
      end

      DataFrame.new(schema, batches)
    end

    private def require_column!(name : String) : Nil
      return if @schema.fields.find_field(name)

      raise ColumnNotFound.new(name, column_names)
    end

    # `try_new` rejects zero-column batches, so carry the row count explicitly.
    private def build_batch(
      schema : Arrow::Schema,
      columns : Array(Arrow::ArrayBase),
      rows : Int32,
    ) : Arrow::RecordBatch
      if columns.empty?
        Arrow::RecordBatch.try_new_with_options(
          schema,
          columns,
          Arrow::RecordBatchOptions.new.with_row_count(rows),
        )
      else
        Arrow::RecordBatch.try_new(schema, columns)
      end
    end
  end
end
