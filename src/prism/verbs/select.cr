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

      names = list.map { |expr| output_name(expr) }
      fields = list.map_with_index { |expr, index| select_field(expr, names[index]) }
      schema = Arrow::Schema.new(fields)

      projected = @batches.map do |batch|
        ctx = EvalContext.new(batch)
        columns = list.map do |expr|
          if expr.is_a?(Col)
            batch.column_by_name(expr.name) || raise ColumnNotFound.new(expr.name, batch.column_names)
          else
            Datums.array(expr.eval(ctx), ctx)
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
    # survive a projection. Computed columns are nullable.
    private def select_field(expr : Expr, name : String) : Arrow::Field
      if expr.is_a?(Col)
        field = @schema.fields.find_field(expr.name)
        raise ColumnNotFound.new(expr.name, column_names) unless field

        field
      else
        Arrow::Field.new(name, expr.dtype(@schema), true)
      end
    end
  end
end
