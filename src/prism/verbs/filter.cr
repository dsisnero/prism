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
        filter_batch(batch, predicate)
      end

      DataFrame.new(@schema, filtered)
    end

    # Keeps rows where the boolean series `mask` is true.
    #
    # This is the RedAmber-style alternative to an expression predicate, and it
    # pairs with the comparison operators on `Prism::Series`:
    #
    # ```
    # df.filter(df["price"] > 10)
    # ```
    #
    # `mask` must be boolean and as long as the frame. When its chunking matches
    # the frame's — which it does for a mask built from the frame's own columns —
    # each chunk is filtered in place and the chunk count survives. Otherwise the
    # frame is consolidated once so the mask lines up.
    #
    # Null mask bits drop their row, matching the expression path.
    def filter(mask : Series) : DataFrame
      unless mask.dtype.kind.boolean?
        raise DtypeError.new(
          "filter requires a boolean series; #{mask.name.inspect} is #{mask.dtype}"
        )
      end
      unless mask.size == row_count
        raise ArgumentError.new(
          "filter mask #{mask.name.inspect} has #{mask.size} rows; the frame has #{row_count}"
        )
      end

      predicates, batches = aligned_mask(mask)
      filtered = batches.zip(predicates).map do |(batch, predicate)|
        filter_batch(batch, predicate)
      end

      DataFrame.new(@schema, filtered)
    end

    # Block form: `df.filter { col("price") > 10 }` with `Prism::DSL` included.
    def filter(&) : DataFrame
      filter(yield)
    end

    # Drops rows where `expr` is true — the inverse of `filter`.
    #
    # The null rule is its own: a row is kept where the predicate is false **or
    # null**. `filter(expr.not)` is not the same thing, because `not(null)` is
    # null and a null predicate drops its row, so rows the predicate cannot
    # judge would be removed too. Keeping them matches RedAmber's `remove`, and
    # it is what "everything that is not a match" means.
    #
    # ```
    # df.remove(col("region") == "west") # keeps null regions
    # ```
    def remove(expr : Expr) : DataFrame
      filter(expr.not.or(expr.is_null))
    end

    # Drops rows where the boolean series `mask` is true, with the same null
    # rule as the expression form: a null mask bit keeps its row.
    def remove(mask : Series) : DataFrame
      unless mask.dtype.kind.boolean?
        raise DtypeError.new(
          "remove requires a boolean series; #{mask.name.inspect} is #{mask.dtype}"
        )
      end

      keep = keep_where_not(mask)
      filter(Series.new("#{mask.name}_removed", keep, Arrow::DataType.boolean))
    end

    # Block form: `df.remove { col("price") > 10 }`.
    def remove(&) : DataFrame
      remove(yield)
    end

    # Inverts a mask, turning its nulls into `true` so the row survives.
    private def keep_where_not(mask : Series) : Array(Arrow::ArrayBase)
      truthy = Arrow::Scalar(Arrow::ArrayBase).new(
        Arrow::BooleanArray.from_values([true]).as(Arrow::ArrayBase)
      )

      mask.chunks.map do |chunk|
        inverted = Arrow.not(chunk.as(Arrow::BooleanArray)).as(Arrow::ArrayBase)
        Datums.zip(Arrow.is_null(chunk), truthy, inverted)
      end
    end

    # The mask's chunks when they line up with this frame's batches, otherwise
    # one consolidated mask against one consolidated batch.
    private def aligned_mask(
      mask : Series,
    ) : Tuple(Array(Arrow::BooleanArray), Array(Arrow::RecordBatch))
      aligned = mask.chunks.size == @batches.size &&
                mask.chunks.zip(@batches).all? { |(chunk, batch)| chunk.len == batch.num_rows }

      if aligned
        predicates = mask.chunks.map { |chunk| chunk.as(Arrow::BooleanArray) }
        {predicates, @batches}
      else
        {[mask.to_arrow.as(Arrow::BooleanArray)], [to_arrow]}
      end
    end

    # Arrow's kernel already treats a null mask bit as false; the all-true and
    # all-false cases skip it, matching Polars' `filter_with_bitmap` fast paths.
    private def filter_batch(batch : Arrow::RecordBatch, predicate : Arrow::BooleanArray) : Arrow::RecordBatch
      matches = predicate.true_count

      if matches == batch.num_rows
        batch
      elsif matches == 0
        batch.slice(0, 0)
      else
        Arrow.filter_record_batch(batch, predicate)
      end
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
