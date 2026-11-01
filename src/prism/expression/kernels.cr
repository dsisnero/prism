module Prism
  # Calls Arrow's compute kernels on datums.
  #
  # Arrow's kernels are untyped methods that internally resolve overloads
  # (`numeric_datum`, `zip_datum`) against *static* types, so handing them a
  # union of array and scalar does not compile. `apply_op` narrows the union
  # first and then calls `apply`, which Crystal instantiates once per
  # (left, right) type pair. The result: one written dispatch table, four
  # compiled variants, and scalars still broadcast inside Arrow instead of being
  # materialized here.
  module Kernels
    extend self

    def apply_op(op : BinaryOp, left : Datum, right : Datum) : Arrow::ArrayBase
      case left_value = left
      when Arrow::ArrayBase
        case right_value = right
        when Arrow::ArrayBase then apply(op, left_value, right_value)
        else                       apply(op, left_value, right_value)
        end
      else
        case right_value = right
        when Arrow::ArrayBase then apply(op, left_value, right_value)
        else                       apply(op, left_value, right_value)
        end
      end
    end

    private def apply(op : BinaryOp, left : L, right : R) : Arrow::ArrayBase forall L, R
      case op
      in .add?   then Arrow.add(left, right)
      in .sub?   then Arrow.sub(left, right)
      in .mul?   then Arrow.mul(left, right)
      in .div?   then Arrow.div(left, right)
      in .rem?   then Arrow.rem(left, right)
      in .eq?    then Arrow.eq(left, right)
      in .neq?   then Arrow.neq(left, right)
      in .lt?    then Arrow.lt(left, right)
      in .lt_eq? then Arrow.lt_eq(left, right)
      in .gt?    then Arrow.gt(left, right)
      in .gt_eq? then Arrow.gt_eq(left, right)
      in .and?, .or?
        raise UnsupportedOperation.new("boolean operators are applied separately")
      end
    end
  end
end
