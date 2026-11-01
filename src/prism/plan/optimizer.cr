require "set"

module Prism
  # Rule-based optimizer over a `Prism::Plan`.
  #
  # Mirrors the order Polars uses: simplify, push predicates, prune projections,
  # push limits. Each pass is a pure tree rewrite, so `data.lazy.explain` can
  # show the before and after.
  #
  # Not implemented yet (see plan §9.2): common subexpression elimination at
  # plan level (it already happens per chunk during evaluation) and sort
  # collapse.
  module Optimizer
    extend self

    def optimize(plan : Plan::Node) : Plan::Node
      node = fold_constants(plan)
      node = push_predicates(node)
      node = prune_projections(node, required_names(node.schema))
      push_limits(node)
    end

    # `filter(lit(true))` disappears; `filter(lit(false))` becomes an empty
    # source, so nothing below it runs.
    def fold_constants(node : Plan::Node) : Plan::Node
      case node
      when Plan::Filter
        folded = fold_constants(node.input)
        case constant_boolean(node.expr)
        when true  then folded
        when false then Plan::Empty.new(folded.schema)
        else            Plan::Filter.new(folded, node.expr)
        end
      else
        node.rebuild(node.children.map { |child| fold_constants(child) })
      end
    end

    # Moves each conjuct as close to the source as it can go.
    def push_predicates(node : Plan::Node) : Plan::Node
      case node
      when Plan::Filter
        push_down_filter(push_predicates(node.input), node.expr)
      else
        node.rebuild(node.children.map { |child| push_predicates(child) })
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def prune_projections(node : Plan::Node, required : Set(String)) : Plan::Node
      case node
      when Plan::ScanParquet
        # Pushed predicates still need their columns decoded.
        needed = required | node.predicates.flat_map(&.columns).to_set
        names = node.full_schema.fields.map(&.name)
          .select { |name| needed.includes?(name) }
        # A projection over every column buys nothing, so leave the scan bare
        # and let `explain` show that nothing was pruned.
        return node if names.size == node.full_schema.fields.size

        node.with_projection(names)
      when Plan::ScanBatches, Plan::Empty
        node
      when Plan::Filter
        needed = required | node.expr.columns.to_set
        Plan::Filter.new(prune_projections(node.input, needed), node.expr)
      when Plan::Select
        kept = node.exprs.select { |expr| required.includes?(Plan.output_name(expr)) }
        return node if kept.empty?

        needed = kept.flat_map(&.columns).to_set
        Plan::Select.new(prune_projections(node.input, needed), kept)
      when Plan::WithColumn
        # Dead column elimination: an unused computed column costs a whole
        # expression evaluation, so drop the node entirely.
        if required.includes?(node.name)
          needed = (required - Set{node.name}) | node.expr.columns.to_set
          Plan::WithColumn.new(
            prune_projections(node.input, needed), node.name, node.expr
          )
        else
          prune_projections(node.input, required)
        end
      when Plan::GroupBy
        kept = node.aggregates.select { |name, _| required.includes?(name) }
        return node if kept.empty?

        keys = node.keys.map { |key| Plan.output_name(key) }.to_set
        needed = keys | kept.values.flat_map(&.columns).to_set
        Plan::GroupBy.new(
          prune_projections(node.input, needed),
          node.keys,
          kept,
          node.null_keys,
          node.maintain_order?,
          node.slice,
        )
      when Plan::Sort
        needed = required | node.keys.flat_map(&.expr.columns).to_set
        Plan::Sort.new(prune_projections(node.input, needed), node.keys, node.slice)
      when Plan::Distinct
        needed = required | node.keys.to_set
        Plan::Distinct.new(prune_projections(node.input, needed), node.keys, node.slice)
      when Plan::Limit
        Plan::Limit.new(
          prune_projections(node.input, required), node.offset, node.count
        )
      when Plan::Join
        # Map the join's output names back to the side that produced them, so
        # each input is pruned to what the output (and the keys) really need.
        requirements = node.input_requirements(required)
        return node unless requirements

        left_required, right_required = requirements
        node.rebuild([
          prune_projections(node.left, left_required),
          prune_projections(node.right, right_required),
        ])
      else
        node
      end
    end

    # Limit(Scan) becomes a scan limit. Projections in between are safe because
    # they neither reorder nor change the row count; anything else blocks it.
    #
    # A row-reordering node does not let the limit through, but it does take the
    # window itself (`Plan::Slice`), which is Polars' "limit 10 after the sort".
    def push_limits(node : Plan::Node) : Plan::Node
      case node
      when Plan::Limit
        input = push_limits(node.input)
        push_limit_into(input, node.offset, node.count) ||
          Plan::Limit.new(input, node.offset, node.count)
      else
        node.rebuild(node.children.map { |child| push_limits(child) })
      end
    end

    private def push_down_filter(input : Plan::Node, expr : Expr) : Plan::Node
      case input
      when Plan::ScanParquet
        available = input.full_schema.fields.map(&.name)
        conjuncts = Plan.conjuncts(expr)
        pushable = conjuncts.select do |conjunct|
          conjunct.columns.all? { |name| available.includes?(name) }
        end
        rest = conjuncts.reject { |conjunct| pushable.includes?(conjunct) }

        scan = pushable.empty? ? input : input.with_predicates(pushable)
        remainder = Plan.combine(rest)
        remainder ? Plan::Filter.new(scan, remainder) : scan
      when Plan::Select
        if can_push_past_select?(expr, input)
          Plan::Select.new(push_down_filter(input.input, expr), input.exprs)
        else
          Plan::Filter.new(input, expr)
        end
      when Plan::WithColumn
        if expr.columns.includes?(input.name)
          Plan::Filter.new(input, expr)
        else
          Plan::WithColumn.new(
            push_down_filter(input.input, expr), input.name, input.expr
          )
        end
      else
        Plan::Filter.new(input, expr)
      end
    end

    # A projection can only be crossed when every referenced column passes
    # through unchanged, i.e. appears as a bare `Col`.
    # `select` cannot be a parameter name: it is a Crystal keyword.
    private def can_push_past_select?(expr : Expr, projection : Plan::Select) : Bool
      passthrough = projection.exprs.compact_map do |projected|
        projected.is_a?(Col) ? projected.as(Col).name : nil
      end

      expr.columns.all? { |name| passthrough.includes?(name) }
    end

    private def push_limit_into(node : Plan::Node, offset : Int32, count : Int32) : Plan::Node?
      case node
      when Plan::ScanParquet
        node.with_limit(offset, count)
      when Plan::Select
        pushed = push_limit_into(node.input, offset, count)
        pushed ? Plan::Select.new(pushed, node.exprs) : nil
      when Plan::WithColumn
        pushed = push_limit_into(node.input, offset, count)
        pushed ? Plan::WithColumn.new(pushed, node.name, node.expr) : nil
      when Plan::Sort
        node.with_slice(compose_slice(node.slice, offset, count))
      when Plan::GroupBy
        node.with_slice(compose_slice(node.slice, offset, count))
      when Plan::Distinct
        node.with_slice(compose_slice(node.slice, offset, count))
      else
        nil
      end
    end

    # An inner window applies first, so the offsets add and the count is
    # clamped to whatever the inner window left.
    private def compose_slice(
      existing : Plan::Slice?,
      offset : Int32,
      count : Int32,
    ) : Plan::Slice
      return Plan::Slice.new(offset, count) unless existing

      available = Math.max(existing.count - offset, 0)
      Plan::Slice.new(existing.offset + offset, Math.min(available, count))
    end

    private def constant_boolean(expr : Expr) : Bool?
      expr.is_a?(Lit) ? expr.as(Lit).value.as?(Bool) : nil
    end

    private def required_names(schema : Arrow::Schema) : Set(String)
      schema.fields.map(&.name).to_set
    end
  end
end
