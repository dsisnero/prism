module Prism
  # A value produced by expression evaluation: a full column chunk, or a
  # length-1 array standing in for a broadcast scalar.
  #
  # Representing literals as scalars lets Arrow's kernels broadcast them
  # natively, so `col("price") * 2` does not build an n-element array of 2s.
  alias Datum = Arrow::ArrayBase | Arrow::Scalar(Arrow::ArrayBase)

  # Everything an expression needs to evaluate: one chunk of rows.
  #
  # Expressions never see a whole frame; aggregation and grouping will pass a
  # narrowed batch through the same type.
  class EvalContext
    getter batch : Arrow::RecordBatch

    def initialize(@batch : Arrow::RecordBatch)
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

    # Narrows a datum to a boolean array, broadcasting scalars.
    def boolean_array(datum : Datum, ctx : EvalContext) : Arrow::BooleanArray
      array = array(datum, ctx)
      return array.as(Arrow::BooleanArray) if array.data_type.kind.boolean?

      raise DtypeError.new("expected a boolean expression, got #{array.data_type}")
    end
  end

  # An immutable, inspectable column operation.
  #
  # - `key` is the canonical string form, used for structural comparison,
  #   deduplication, and `explain` output.
  # - `dtype` is total: it reports the output type without reading any data.
  # - `eval` sees one chunk and never mutates its inputs.
  #
  # Expressions are classes, not structs, because nodes contain other nodes;
  # a struct hierarchy with recursive members would not compile.
  abstract class Expr
    abstract def key : String

    abstract def dtype(schema : Arrow::Schema) : Arrow::DataType

    abstract def eval(ctx : EvalContext) : Datum

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
end
