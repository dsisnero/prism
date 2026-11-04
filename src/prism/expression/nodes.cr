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
      elsif operands = Coerce.arithmetic_operands(@left, @right, schema)
        Coerce.arithmetic_dtype(@op, operands)
      else
        Coerce.target(@left, @right, schema)
      end
    end

    def children : Array(Expr)
      [@left, @right]
    end

    def eval(ctx : EvalContext) : Datum
      return eval_boolean(ctx) if @op.boolean?

      if folded = fold_out_of_range(ctx)
        return folded
      end

      # Decimal arithmetic keeps each side's precision and scale: the kernel
      # derives the result dtype from both, so casting to one common dtype first
      # would declare the wrong answer.
      if operands = Coerce.arithmetic_operands(@left, @right, ctx.schema, ctx.types)
        return eval_decimal(ctx, operands)
      end

      target = ctx.coercion_target(@left, @right)
      check_literals_fit(target)

      left = Coerce.cast(ctx.evaluate(@left), target)
      right = Coerce.cast(ctx.evaluate(@right), target)

      Kernels.apply_op(@op, left, right)
    end

    private def eval_decimal(
      ctx : EvalContext,
      operands : Tuple(Arrow::DataType, Arrow::DataType),
    ) : Arrow::ArrayBase
      check_literal_fits(@left, operands[0])
      check_literal_fits(@right, operands[1])

      left = Coerce.cast(ctx.evaluate(@left), operands[0])
      right = Coerce.cast(ctx.evaluate(@right), operands[1])

      Kernels.apply_op(@op, left, right)
    end

    # A comparison against a literal that cannot fit the column's dtype compares
    # the same way for every row, so it folds to a constant:
    # `is_null ? null : constant`, which is what Polars rewrites it to.
    #
    # Without this the safe cast turns the literal into null and the whole
    # comparison silently becomes null.
    private def fold_out_of_range(ctx : EvalContext) : Arrow::BooleanArray?
      return nil unless @op.comparison?

      literal, operand, op = literal_operand
      return nil unless (literal_side = literal) && (column_expr = operand)

      column = Datums.array(ctx.evaluate(column_expr), ctx)
      range = integer_range(column.data_type)
      return nil unless range

      number = integer_literal(literal_side)
      return nil unless number

      minimum, maximum = range
      return nil if number >= minimum && number <= maximum

      above = number > maximum
      constant = case op
                 when .lt?, .lt_eq? then above
                 when .gt?, .gt_eq? then !above
                 when .eq?          then false
                 else                    true
                 end

      Arrow.nullif(
        Datums.constant_boolean(constant, ctx),
        Arrow.is_null(column),
      ).as(Arrow::BooleanArray)
    end

    # The literal side (if any), the expression on the other side, and the
    # operator oriented as `column <op> literal`.
    private def literal_operand : Tuple(Lit?, Expr?, BinaryOp)
      if @left.is_a?(Lit) && !@right.is_a?(Lit)
        {@left.as(Lit), @right, mirror(@op)}
      elsif @right.is_a?(Lit) && !@left.is_a?(Lit)
        {@right.as(Lit), @left, @op}
      else
        {nil, nil, @op}
      end
    end

    private def mirror(op : BinaryOp) : BinaryOp
      case op
      in .lt?    then BinaryOp::Gt
      in .lt_eq? then BinaryOp::GtEq
      in .gt?    then BinaryOp::Lt
      in .gt_eq? then BinaryOp::LtEq
      in .eq?    then BinaryOp::Eq
      in .neq?   then BinaryOp::Neq
      in .add?, .sub?, .mul?, .div?, .rem?, .and?, .or?
        op
      end
    end

    private def integer_literal(literal : Lit) : Int128?
      case value = literal.value
      when Int64  then value.to_i128
      when UInt64 then value.to_i128
      end
    end

    private def integer_range(data_type : Arrow::DataType) : Tuple(Int128, Int128)?
      case data_type.kind
      when .int8?                        then {-128_i128, 127_i128}
      when .int16?                       then {-32_768_i128, 32_767_i128}
      when .int32?                       then {-2_147_483_648_i128, 2_147_483_647_i128}
      when .int64?                       then {Int64::MIN.to_i128, Int64::MAX.to_i128}
      when Arrow::DataType::Kind::UInt8  then {0_i128, 255_i128}
      when Arrow::DataType::Kind::UInt16 then {0_i128, 65_535_i128}
      when Arrow::DataType::Kind::UInt32 then {0_i128, 4_294_967_295_i128}
      when Arrow::DataType::Kind::UInt64 then {0_i128, UInt64::MAX.to_i128}
      end
    end

    # Arithmetic cannot fold, so a literal that does not fit is an error rather
    # than a silently nulled row.
    private def check_literals_fit(target : Arrow::DataType) : Nil
      check_literal_fits(@left, target)
      check_literal_fits(@right, target)
    end

    private def check_literal_fits(expr : Expr, target : Arrow::DataType) : Nil
      return unless expr.is_a?(Lit)

      number = integer_literal(expr.as(Lit))
      return unless number

      range = integer_range(target)
      return unless range

      minimum, maximum = range
      return if number >= minimum && number <= maximum

      raise DtypeError.new(
        "literal #{number} does not fit #{target}; cast the column or the literal explicitly"
      )
    end

    # Boolean identities first, so `x & false` and `x | true` never evaluate
    # `x` at all. Safe under Kleene logic: a known false wins in AND, a known
    # true wins in OR, exactly as `and_kleene`/`or_kleene` define it.
    private def eval_boolean(ctx : EvalContext) : Arrow::BooleanArray
      and_op = @op.and?
      left_constant = boolean_literal(@left)
      right_constant = boolean_literal(@right)

      if left_constant == !and_op || right_constant == !and_op
        return Datums.constant_boolean(!and_op, ctx)
      end
      return Datums.boolean_array(ctx.evaluate(@left), ctx) if right_constant == and_op
      return Datums.boolean_array(ctx.evaluate(@right), ctx) if left_constant == and_op

      left = Datums.boolean_array(ctx.evaluate(@left), ctx)
      right = Datums.boolean_array(ctx.evaluate(@right), ctx)
      and_op ? Arrow.and_kleene(left, right) : Arrow.or_kleene(left, right)
    end

    private def boolean_literal(expr : Expr) : Bool?
      case expr
      when Lit
        expr.value.as?(Bool)
      else
        nil
      end
    end

    def to_s(io : IO) : Nil
      io << key
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

    def children : Array(Expr)
      [@operand]
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
      Arrow.neg(Datums.array(ctx.evaluate(@operand), ctx))
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

    def children : Array(Expr)
      [@operand]
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
      Arrow.not(Datums.boolean_array(ctx.evaluate(@operand), ctx))
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

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      @operand.dtype(schema)
      Arrow::DataType.boolean
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.is_null(Datums.array(ctx.evaluate(@operand), ctx))
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

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      @operand.dtype(schema)
      Arrow::DataType.boolean
    end

    def eval(ctx : EvalContext) : Datum
      Arrow.is_not_null(Datums.array(ctx.evaluate(@operand), ctx))
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

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      @operand.dtype(schema)
      @target
    end

    def eval(ctx : EvalContext) : Datum
      Coerce.cast(ctx.evaluate(@operand), @target)
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

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      @operand.dtype(schema)
    end

    def eval(ctx : EvalContext) : Datum
      ctx.evaluate(@operand)
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

    def children : Array(Expr)
      [@operand, @replacement]
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
      array = Datums.array(ctx.evaluate(@operand), ctx)
      replacement = Coerce.cast(ctx.evaluate(@replacement), array.data_type)

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

    def children : Array(Expr)
      @operands
    end

    def columns : Array(String)
      @operands.flat_map(&.columns).uniq!
    end

    def to_s(io : IO) : Nil
      io << key
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      target = @operands.first.dtype(schema)
      @operands[1..].each { |operand| target = Coerce.unify(target, operand.dtype(schema)) }
      target
    end

    def eval(ctx : EvalContext) : Datum
      accumulator = Datums.array(ctx.evaluate(@operands.first), ctx)

      @operands[1..].each do |operand|
        replacement = Coerce.cast(ctx.evaluate(operand), accumulator.data_type)
        accumulator = Datums.zip(
          Arrow.is_not_null(accumulator),
          accumulator,
          replacement,
        )
      end

      accumulator
    end
  end

  # Row-aligned conditional: `then_value` where `condition` is true, `else_value`
  # otherwise.
  #
  # A null condition selects `else_value`. That is what `Arrow.zip` does — a null
  # mask reads as false — and what Polars' `when/then/otherwise` does, so the
  # result never carries a null the selected branch does not have. It is also what
  # makes nesting (`Prism.case_when`) mean "first branch that matches", since a
  # null condition matches nothing.
  #
  # The branches are coerced with `Prism::Coerce.target`, so a literal follows the
  # same rule arithmetic uses: `if_else(cond, int32_col, 7)` stays `Int32`, while
  # `if_else(cond, int_col, 1.5)` widens to `Float64` instead of truncating. The
  # condition has to be boolean, checked by `#dtype` before any data is read.
  class IfElse < Expr
    getter condition : Expr
    getter then_value : Expr
    getter else_value : Expr

    def initialize(@condition : Expr, @then_value : Expr, @else_value : Expr)
    end

    def key : String
      "if_else(#{@condition.key},#{@then_value.key},#{@else_value.key})"
    end

    def children : Array(Expr)
      [@condition, @then_value, @else_value]
    end

    def columns : Array(String)
      (@condition.columns + @then_value.columns + @else_value.columns).uniq
    end

    def to_s(io : IO) : Nil
      io << key
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      require_boolean!(@condition.dtype(schema))
      # `Coerce.target` rather than a bare unify, so a literal branch follows the
      # same rule arithmetic does: it adopts the column's dtype when the cast is
      # lossless (`int32_col` + `lit(7)` stays `Int32`) and widens both sides when
      # it is not (`int_col` against `1.5` runs in `Float64`).
      Coerce.target(@then_value, @else_value, schema)
    end

    def eval(ctx : EvalContext) : Datum
      target = dtype(ctx.schema)
      mask = Datums.array(ctx.evaluate(@condition), ctx)

      Datums.zip(
        mask.as(Arrow::BooleanArray),
        Coerce.cast(ctx.evaluate(@then_value), target),
        Coerce.cast(ctx.evaluate(@else_value), target),
      )
    end

    private def require_boolean!(dtype : Arrow::DataType) : Nil
      return if dtype.kind.boolean?

      raise DtypeError.new(
        "if_else needs a boolean condition, got #{dtype}"
      )
    end
  end

  # Set membership: whether each row's value is in a constant set.
  #
  # The set is encoded once, when the expression is built, so a 12k-title set is
  # not rebuilt per chunk and does not have to be written as a 12k-term `|` chain
  # of equalities. `Prism::RowKeys`' rule applies: the set array is the constant,
  # only the operand reads data.
  #
  # Nulls follow comparisons: a null cell yields null, so `filter` and `remove`
  # behave exactly as they do for `col("x") == "y"`. `nulls_equal: true` treats
  # null as a value instead — null then matches only when the set itself contains
  # null, which is what Arrow's kernel does and what a boolean mask usually wants.
  class IsIn < Expr
    getter operand : Expr
    getter values : Arrow::ArrayBase
    getter? nulls_equal : Bool
    getter digest : String

    def initialize(
      @operand : Expr,
      @values : Arrow::ArrayBase,
      @nulls_equal : Bool = false,
    )
      # The set is a constant, so its key is a digest rather than its contents:
      # plans, `explain` and CSE keys stay short and printable, and two different
      # sets of the same size still get different keys. `Prism::Values` is the
      # one materialization point, so this works for any dtype the set can hold.
      @digest = Prism::Values.extract(@values).hash.to_s(16)
    end

    def key : String
      suffix = @nulls_equal ? ",nulls_equal" : ""
      "is_in(#{@operand.key},#{@values.len},#{@digest}#{suffix})"
    end

    def children : Array(Expr)
      [@operand]
    end

    def columns : Array(String)
      @operand.columns
    end

    def to_s(io : IO) : Nil
      io << key
    end

    def dtype(schema : Arrow::Schema) : Arrow::DataType
      # Raising here is what rejects `col("i").is_in(["a"])` before any data is
      # read; `Membership.matches` computes the same target at run time.
      Membership.target(@operand.dtype(schema), @values.data_type)
      Arrow::DataType.boolean
    end

    def eval(ctx : EvalContext) : Datum
      Membership.matches(
        Datums.array(ctx.evaluate(@operand), ctx),
        @values,
        @nulls_equal,
      )
    end
  end
end
