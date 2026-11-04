module Prism
  # Set membership for one Arrow array, shared by `Expr#is_in` and
  # `Series#is_in`.
  #
  # One place, so the expression and the column API cannot drift on the coercion
  # rule, the null rule, or which kernel runs: every path here goes through
  # `arrow-cr`'s `ArrayBase#is_in`, which has been a hash probe since `78b0cd4`
  # (`shard_issues/arrow-cr.3.md`). There is deliberately no Prism-side loop —
  # a second kernel for the same operation is exactly what §1 forbids.
  module Membership
    extend self

    # A length-1 null boolean, broadcast by `Arrow.zip` to propagate nulls.
    NULL_MATCH = Arrow::Scalar(Arrow::ArrayBase).new(
      Arrow::BooleanArray.from_nullable([nil] of Bool?)
    )

    # The dtype membership runs in: the column and the set unified, so an
    # `Int32` set against an `Int64` column compares as `Int64` and a set with no
    # common dtype with the column raises `Prism::DtypeError`. Callers use this
    # to reject a bad set before any data is read — including for an empty
    # column, where `#matches` would never run.
    def target(operand : Arrow::DataType, values : Arrow::DataType) : Arrow::DataType
      Coerce.unify(operand, values)
    end

    # `operand.is_in(values)` per row, as a boolean array.
    #
    # Nulls follow comparisons: a null cell yields null, so `filter` drops that
    # row and `remove` keeps it. `nulls_equal: true` treats null as a value
    # instead — null then matches only when the set contains null, which is
    # Arrow's kernel rule and Polars' `nulls_equal`.
    def matches(
      operand : Arrow::ArrayBase,
      values : Arrow::ArrayBase,
      nulls_equal : Bool,
    ) : Arrow::BooleanArray
      target = target(operand.data_type, values.data_type)
      operand = cast(operand, target)
      set = cast(values, target)

      # `arrow-cr`'s kernel: a hash probe for the hashable dtypes, a comparator
      # search for the rest.
      matches = operand.is_in(set)
      return matches if nulls_equal || operand.logical_null_count == 0

      Arrow.zip(Arrow.is_not_null(operand), matches, NULL_MATCH)
        .as(Arrow::BooleanArray)
    end

    private def cast(array : Arrow::ArrayBase, target : Arrow::DataType) : Arrow::ArrayBase
      array.data_type == target ? array : Arrow.cast(array, target)
    end
  end
end
