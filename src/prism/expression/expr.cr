require "set"

module Prism
  # A value produced by expression evaluation: a full column chunk, or a
  # length-1 array standing in for a broadcast scalar.
  #
  # Representing literals as scalars lets Arrow's kernels broadcast them
  # natively, so `col("price") * 2` does not build an n-element array of 2s.
  #
  # Note: `Arrow::Scalar(T)` cannot appear in a union without its type argument
  # (Crystal rejects a bare generic virtual type there), hence the explicit
  # `Scalar(Arrow::ArrayBase)`.
  alias Datum = Arrow::ArrayBase | Arrow::Scalar(Arrow::ArrayBase)

  # Everything an expression needs to evaluate: one chunk of rows, plus the
  # per-execution caches shared across chunks.
  class EvalContext
    getter batch : Arrow::RecordBatch
    getter types : TypeCache

    @duplicates : Set(String)
    @computed : Hash(String, Datum)?

    # `types` and `duplicates` are built once per verb call and shared by every
    # chunk's context; omitting them makes a context that owns its own cache.
    def initialize(
      @batch : Arrow::RecordBatch,
      types : TypeCache? = nil,
      duplicates : Set(String)? = nil,
    )
      @types = types || TypeCache.new(@batch.schema)
      @duplicates = duplicates || Set(String).new
      @computed = @duplicates.empty? ? nil : {} of String => Datum
    end

    def row_count : Int32
      @batch.num_rows
    end

    def schema : Arrow::Schema
      @batch.schema
    end

    # Returns a column chunk, raising `Prism::ColumnNotFound` when absent.
    def column(name : String) : Arrow::ArrayBase
      @batch[name]? || raise ColumnNotFound.new(name, @batch.column_names)
    end

    # Evaluates `expr`, memoizing subexpressions that occur more than once in
    # this execution. Nodes call this for their children, so a repeated subtree
    # is computed once per chunk rather than once per occurrence.
    def evaluate(expr : Expr) : Datum
      memo = @computed
      return expr.eval(self) unless memo

      key = expr.key
      return expr.eval(self) unless @duplicates.includes?(key)

      cached = memo[key]?
      return cached if cached

      value = expr.eval(self)
      memo[key] = value
      value
    end

    # Coercion dtype for a binary operation, resolved once per execution.
    def coercion_target(left : Expr, right : Expr) : Arrow::DataType
      @types.coercion_target(left, right)
    end
  end

  # Helpers for turning datums into arrays, shared by expression nodes.
  module Datums
    extend self

    # Arrays pass through untouched; scalars are broadcast to the row count.
    def array(datum : Datum, ctx : EvalContext) : Arrow::ArrayBase
      case value = datum
      when Arrow::ArrayBase
        value
      else
        broadcast(value.array.as(Arrow::ArrayBase), ctx.row_count)
      end
    end

    # Materializes a length-1 array to `count` rows by gathering its first row.
    def broadcast(array : Arrow::ArrayBase, count : Int32) : Arrow::ArrayBase
      return array if array.len == count

      indices = Arrow::PrimitiveArray(UInt32).from_values(
        Array(UInt32).new(count, 0_u32)
      )
      Arrow.take(array, indices)
    end

    # Narrows a datum to a boolean array, broadcasting scalars.
    def boolean_array(datum : Datum, ctx : EvalContext) : Arrow::BooleanArray
      array = array(datum, ctx)
      return array.as(Arrow::BooleanArray) if array.data_type.kind.boolean?

      raise DtypeError.new("expected a boolean expression, got #{array.data_type}")
    end

    # A constant boolean column, as a bitmap. Costs n/8 bytes, not n.
    def constant_boolean(value : Bool, ctx : EvalContext) : Arrow::BooleanArray
      buffer = if value
                 Arrow::BooleanBuffer.new_set(ctx.row_count)
               else
                 Arrow::BooleanBuffer.new_unset(ctx.row_count)
               end
      Arrow::BooleanArray.new(buffer)
    end

    # Row-aligned selection: `truthy` where `mask` is true, `falsy` otherwise.
    #
    # Uses `Arrow.zip`, not `Arrow.merge`: merge consumes its inputs *compacted*
    # (only true rows from `truthy`, in order), which silently drops rows when
    # filling nulls. zip requires full-length arrays or scalars, which is what
    # an expression produces.
    #
    # The union is narrowed explicitly because Arrow's `zip_datum` resolves
    # overloads against static types and rejects a union argument.
    def zip(
      mask : Arrow::BooleanArray,
      truthy : Datum,
      falsy : Datum,
    ) : Arrow::ArrayBase
      case yes = truthy
      when Arrow::ArrayBase
        case no = falsy
        when Arrow::ArrayBase then Arrow.zip(mask, yes, no)
        else                       Arrow.zip(mask, yes, no)
        end
      else
        case no = falsy
        when Arrow::ArrayBase then Arrow.zip(mask, yes, no)
        else                       Arrow.zip(mask, yes, no)
        end
      end
    end
  end

  # An immutable, inspectable column operation.
  #
  # - `key` is the canonical string form, used for structural comparison,
  #   deduplication, and `explain` output.
  # - `dtype` is total: it reports the output type without reading any data.
  # - `nullable?` is total too, and answers the same question for nullability:
  #   can a value this expression produces be null? It is the *only* place that
  #   question is answered, so an eager verb and the plan node mirroring it
  #   cannot disagree about a column's schema.
  # - `eval` sees one chunk and never mutates its inputs.
  #
  # Expressions are classes, not structs, because nodes contain other nodes;
  # a struct hierarchy with recursive members would not compile.
  abstract class Expr
    abstract def key : String

    abstract def dtype(schema : Arrow::Schema) : Arrow::DataType

    abstract def eval(ctx : EvalContext) : Datum

    # Whether this expression's output can hold a null.
    #
    # The default is `true`: unknown nodes are assumed to produce nulls, which
    # is always a safe claim for an Arrow field. Nodes narrow it when they can
    # prove otherwise, and the rules are structural — a null can only come from
    # an operand (or from a node that fills, predicates, or chooses):
    #
    # - `Col` reports its field's own flag, `Lit` is null only for `nil`;
    # - arithmetic, comparison, `neg` and `not` propagate their operands'
    #   nulls and introduce none (`arrow-cr` raises on overflow and division by
    #   zero instead of nulling);
    # - `is_null`/`is_not_null` never null, and `cast` keeps its operand's flag
    #   because `Prism::Coerce.cast_exact` refuses to invent nulls;
    # - `fill_null` is as nullable as its replacement, `coalesce` is non-null as
    #   soon as one operand is, and `if_else` is as nullable as its branches.
    #
    # Aggregations keep the default: a group's `sum` or `first` of a nullable
    # column is null, and the one conservative answer is cheaper than twelve
    # precise ones (§17).
    def nullable?(schema : Arrow::Schema) : Bool
      true
    end

    # Child expressions, in evaluation order. Used by `Prism::Cse`.
    def children : Array(Expr)
      [] of Expr
    end

    # Column names this expression reads, in first-seen order, deduplicated.
    def columns : Array(String)
      [] of String
    end

    # True for a constant. Used to choose which side of an operation to cast
    # (see `Prism::Coerce.target`).
    def literal? : Bool
      false
    end

    # Structural equality. Never use `==` for this: on an `Expr` it builds a
    # comparison expression (see the operator overloads).
    def same_as?(other : Expr) : Bool
      key == other.key
    end
  end

  # The field an expression produces when it becomes a column.
  #
  # One rule, shared by every verb that builds a schema from expressions —
  # `select`, `with_column`, `cast` and `fill_null` eagerly, and the `Select`
  # and `WithColumn` plan nodes that mirror them — so the same pipeline cannot
  # report two schemas. Before this existed, `cast` and the frame-wide
  # `fill_null` went through `select` and came out nullable while the eager verb
  # preserved the input field, and `concat` refused to stack the two results.
  #
  # A bare column keeps its own field, metadata included, because a projection
  # is the same column; `name` covers the alias/rename case. Any other
  # expression takes its dtype and nullability from the expression, and since
  # `nullable?` is the only nullability rule, an eager verb and its plan node
  # agree by construction.
  def self.output_field(expr : Expr, name : String, schema : Arrow::Schema) : Arrow::Field
    source = case expr
             when Alias then expr.operand
             else            expr
             end

    if source.is_a?(Col)
      field = schema.fields.find_field(source.name)
      raise ColumnNotFound.new(source.name, schema.fields.map(&.name)) unless field
      return field if field.name == name

      # `Field#with_name` mutates, so copy first (C16).
      return field.copy.with_name(name)
    end

    Arrow::Field.new(name, expr.dtype(schema), expr.nullable?(schema))
  end
end
