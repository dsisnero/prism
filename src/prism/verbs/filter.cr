module Prism
  class DataFrame
    # Keeps rows where `expr` evaluates to true.
    #
    # Null predicate values drop their rows, matching Arrow's filter kernel and
    # Polars' treatment of an all-null mask (values AND validity).
    #
    # Mirrors the three fast paths in Polars' `filter_with_bitmap`:
    #
    # - every row matches (mask all true, no nulls) → the batch is returned
    #   untouched, no copy;
    # - no row matches → an empty view of the batch, kernel skipped;
    # - otherwise → Arrow's boolean-mask filter kernel.
    #
    # Maps over chunks, so a filter never consolidates the frame.
    def filter(expr : Expr) : DataFrame
      case constant_predicate(expr)
      when true  then return self
      when false then return empty_rows
      end

      types = TypeCache.new(@schema)
      duplicates = Cse.duplicate_keys([expr])

      filtered = @batches.map do |batch|
        ctx = EvalContext.new(batch, types, duplicates)
        predicate = Datums.boolean_array(ctx.evaluate(expr), ctx)
        matches = predicate.true_count

        if matches == batch.num_rows
          batch
        elsif matches == 0
          batch.slice(0, 0)
        else
          Arrow.filter_record_batch(batch, predicate)
        end
      end

      DataFrame.new(@schema, filtered)
    end

    # Block form: `df.filter { col("price") > 10 }` with `Prism::DSL` included.
    def filter(&) : DataFrame
      filter(yield)
    end

    private def constant_predicate(expr : Expr) : Bool?
      case expr
      when Lit
        expr.value.as?(Bool)
      else
        nil
      end
    end

    private def empty_rows : DataFrame
      DataFrame.new(@schema, @batches.map(&.slice(0, 0)))
    end
  end
end
