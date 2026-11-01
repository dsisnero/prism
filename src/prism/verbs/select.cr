module Prism
  class DataFrame
    # Selects columns by name.
    #
    # ```
    # df.select("region", "price")
    # ```
    def select(*names : String | Symbol) : DataFrame
      self.select(names.map { |name| Prism.col(name) })
    end

    # Selects columns by expression (variadic form).
    def select(*exprs : Expr) : DataFrame
      self.select(exprs.map(&.as(Expr)))
    end

    # Selects columns by expression.
    #
    # A bare `Col` projects the existing array without copying it. Any other
    # expression must be aliased:
    #
    # ```
    # df.select(col("region"), (col("price") * 2).alias("double_price"))
    # ```
    def select(exprs : Enumerable(Expr)) : DataFrame
      list = exprs.to_a
      raise ArgumentError.new("select needs at least one column") if list.empty?

      types = TypeCache.new(@schema)
      duplicates = Cse.duplicate_keys(list)
      names = list.map { |expr| output_name(expr) }
      fields = list.map_with_index { |expr, index| select_field(expr, names[index], types) }
      schema = Arrow::Schema.new(fields)

      projected = @batches.map do |batch|
        ctx = EvalContext.new(batch, types, duplicates)
        columns = list.map do |expr|
          if expr.is_a?(Col)
            batch.column_by_name(expr.name) || raise ColumnNotFound.new(expr.name, batch.column_names)
          else
            Datums.array(ctx.evaluate(expr), ctx)
          end
        end

        Arrow::RecordBatch.try_new(schema, columns)
      end

      DataFrame.new(schema, projected)
    end

    private def output_name(expr : Expr) : String
      case expr
      when Alias then expr.name
      when Col   then expr.name
      else            raise MissingAlias.new(expr.key)
      end
    end

    # A selected column keeps its original field, so nullability and metadata
    # survive a projection. Renaming a column is still the same column, so an
    # `Alias` of a bare column keeps its nullability too; computed columns are
    # nullable.
    private def select_field(expr : Expr, name : String, types : TypeCache) : Arrow::Field
      case expr
      when Col
        input_field(expr.name)
      when Alias
        inner = expr.operand
        if inner.is_a?(Col)
          # `Field#with_name` mutates, so copy first (C16).
          input_field(inner.name).copy.with_name(name)
        else
          Arrow::Field.new(name, types.dtype(expr), true)
        end
      else
        Arrow::Field.new(name, types.dtype(expr), true)
      end
    end

    private def input_field(name : String) : Arrow::Field
      field = @schema.fields.find_field(name)
      return field if field

      raise ColumnNotFound.new(name, column_names)
    end
  end
end
