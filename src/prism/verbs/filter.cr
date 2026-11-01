module Prism
  class DataFrame
    # Keeps rows where `expr` evaluates to true.
    #
    # Null predicate values drop their rows, matching Arrow's filter kernel.
    # Maps over chunks, so a filter never consolidates the frame.
    def filter(expr : Expr) : DataFrame
      filtered = @batches.map do |batch|
        ctx = EvalContext.new(batch)
        predicate = Datums.boolean_array(expr.eval(ctx), ctx)
        Arrow.filter_record_batch(batch, predicate)
      end

      DataFrame.new(@schema, filtered)
    end

    # Block form: `df.filter { col("price") > 10 }` with `Prism::DSL` included.
    def filter(&) : DataFrame
      filter(yield)
    end
  end
end
