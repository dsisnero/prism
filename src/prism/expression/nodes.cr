module Prism
  # A column reference.
  class Col < Expr
    getter name : String

    def initialize(@name : String)
    end

    def key : String
      "col:#{@name}"
    end

    def columns : Array(String)
      [@name]
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      field = schema.fields.find_field(@name)
      return field.data_type if field

      raise ColumnNotFound.new(@name, schema.fields.map(&.name))
    end

    def eval(ctx : EvalContext) : Datum
      ctx.column(@name)
    end

    def to_s(io : IO) : Nil
      io << key
    end
  end

  # A constant, evaluated as a length-1 array the kernels broadcast.
  class Lit < Expr
    getter value : LiteralValue

    def initialize(@value : LiteralValue)
    end

    def key : String
      "lit:#{@value.inspect}"
    end

    def literal? : Bool
      true
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      Literals.dtype_of(@value)
    end

    def eval(ctx : EvalContext) : Datum
      Arrow::Scalar(Arrow::ArrayBase).new(Literals.array(@value))
    end

    def to_s(io : IO) : Nil
      io << key
    end
  end

  # Binary operations. Arithmetic and comparison run through `Prism::Coerce`;
  # boolean operations require boolean operands.
  enum BinaryOp
    Add
    Sub
    Mul
    Div
    Rem
    Eq
    Neq
    Lt
    LtEq
    Gt
    GtEq
    And
    Or

    def key_name : String
      case self
      in .add?   then "add"
      in .sub?   then "sub"
      in .mul?   then "mul"
      in .div?   then "div"
      in .rem?   then "rem"
      in .eq?    then "eq"
      in .neq?   then "neq"
      in .lt?    then "lt"
      in .lt_eq? then "lt_eq"
      in .gt?    then "gt"
      in .gt_eq? then "gt_eq"
      in .and?   then "and"
      in .or?    then "or"
      end
    end

    def comparison? : Bool
      eq? || neq? || lt? || lt_eq? || gt? || gt_eq?
    end

    def boolean? : Bool
      and? || or?
    end
  end

  # A binary operation over two expressions.
  class Binary < Expr
    getter op : BinaryOp
    getter left : Expr
    getter right : Expr

    def initialize(@op : BinaryOp, @left : Expr, @right : Expr)
    end

    def key : String
      "#{@op.key_name}(#{@left.key},#{@right.key})"
    end

    def columns : Array(String)
      (@left.columns + @right.columns).uniq
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      if @op.boolean?
        require_boolean(@left, schema)
        require_boolean(@right, schema)
        Arrow::DataType.boolean
      elsif @op.comparison?
        Coerce.target(@left, @right, schema)
        Arrow::DataType.boolean
      else
        Coerce.target(@left, @right, schema)
      end
    end

    def eval(ctx : EvalContext) : Datum
      if @op.boolean?
        return apply_boolean(
          Datums.boolean_array(@left.eval(ctx), ctx),
          Datums.boolean_array(@right.eval(ctx), ctx),
        )
      end

      target = Coerce.target(@left, @right, ctx.schema)
      left = Coerce.cast(@left.eval(ctx), target)
      right = Coerce.cast(@right.eval(ctx), target)

      Kernels.apply_op(@op, left, right)
    end

    def to_s(io : IO) : Nil
      io << key
    end

    # Kleene logic, matching SQL and Polars' logical operators: a known `false`
    # wins over null in AND, a known `true` wins over null in OR.
    private def apply_boolean(
      left : Arrow::BooleanArray,
      right : Arrow::BooleanArray,
    ) : Arrow::BooleanArray
      @op.and? ? Arrow.and_kleene(left, right) : Arrow.or_kleene(left, right)
    end

    private def require_boolean(expr : Expr, schema : Arrow::Schema) : Nil
      dtype = expr.dtype(schema)
      return if dtype.kind.boolean? || dtype.kind.null?

      raise DtypeError.new(
        "#{@op.key_name} requires boolean operands; got #{dtype} for #{expr.key}"
      )
    end
  end

  # Arithmetic negation.
  class Neg < Expr
    getter operand : Expr

    def initialize(@operand : Expr)
    end

    def key : String
      "neg(#{@operand.key})"
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      dtype = @operand.dtype(schema)
      unless dtype.numeric? || dtype.kind.duration?
        raise DtypeError.new("neg requires a numeric column; #{@operand.key} is #{dtype}")
      end

      dtype
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.neg(Datums.array(@operand.eval(ctx), ctx))
    end
  end

  # Logical negation. Nulls stay null.
  class Not < Expr
    getter operand : Expr

    def initialize(@operand : Expr)
    end

    def key : String
      "not(#{@operand.key})"
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      dtype = @operand.dtype(schema)
      unless dtype.kind.boolean? || dtype.kind.null?
        raise DtypeError.new("not requires a boolean expression; #{@operand.key} is #{dtype}")
      end

      Arrow::DataType.boolean
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.not(Datums.boolean_array(@operand.eval(ctx), ctx))
    end
  end

  # Null predicate: true where the value is null.
  class IsNull < Expr
    getter operand : Expr

    def initialize(@operand : Expr)
    end

    def key : String
      "is_null(#{@operand.key})"
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      @operand.dtype(schema)
      Arrow::DataType.boolean
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.is_null(Datums.array(@operand.eval(ctx), ctx))
    end
  end

  # Null predicate: true where the value is present.
  class IsNotNull < Expr
    getter operand : Expr

    def initialize(@operand : Expr)
    end

    def key : String
      "is_not_null(#{@operand.key})"
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      @operand.dtype(schema)
      Arrow::DataType.boolean
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.is_not_null(Datums.array(@operand.eval(ctx), ctx))
    end
  end

  # Explicit dtype conversion.
  class Cast < Expr
    getter operand : Expr
    getter target : Arrow::DataType

    def initialize(@operand : Expr, @target : Arrow::DataType)
    end

    def key : String
      "cast(#{@operand.key},#{@target})"
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      @operand.dtype(schema)
      @target
    end

    def eval(ctx : EvalContext) : Datum
      Coerce.cast(@operand.eval(ctx), @target)
    end
  end

  # Renames an expression's output column.
  class Alias < Expr
    getter operand : Expr
    getter name : String

    def initialize(@operand : Expr, @name : String)
    end

    def key : String
      "alias(#{@operand.key},#{@name})"
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      @operand.dtype(schema)
    end

    def eval(ctx : EvalContext) : Datum
      @operand.eval(ctx)
    end
  end

  # Replaces nulls in `operand` with `replacement`.
  class FillNull < Expr
    getter operand : Expr
    getter replacement : Expr

    def initialize(@operand : Expr, @replacement : Expr)
    end

    def key : String
      "fill_null(#{@operand.key},#{@replacement.key})"
    end

    def columns : Array(String)
      (@operand.columns + @replacement.columns).uniq
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      target = @operand.dtype(schema)
      @replacement.dtype(schema)
      target
    end

    def eval(ctx : EvalContext) : Datum
      array = Datums.array(@operand.eval(ctx), ctx)
      replacement = Coerce.cast(@replacement.eval(ctx), array.data_type)

      Datums.zip(Arrow.is_not_null(array), array, replacement)
    end
  end

  # First non-null value across the operands, in order.
  class Coalesce < Expr
    getter operands : Array(Expr)

    def initialize(@operands : Array(Expr))
    end

    def key : String
      "coalesce(#{@operands.map(&.key).join(",")})"
    end

    def columns : Array(String)
      @operands.flat_map(&.columns).uniq!
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      target = @operands.first.dtype(schema)
      @operands[1..].each { |operand| target = Coerce.unify(target, operand.dtype(schema)) }
      target
    end

    def eval(ctx : EvalContext) : Datum
      accumulator = Datums.array(@operands.first.eval(ctx), ctx)

      @operands[1..].each do |operand|
        replacement = Coerce.cast(operand.eval(ctx), accumulator.data_type)
        accumulator = Datums.zip(
          Arrow.is_not_null(accumulator),
          accumulator,
          replacement,
        )
      end

      accumulator
    end
  end
end
