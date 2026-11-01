module Prism
  # Logical plan nodes for `LazyFrame`.
  #
  # Every node knows its output schema, because expressions are typeable without
  # data. That is what lets the optimizer prune projections before anything is
  # read.
  #
  # Execution deliberately delegates to the eager verbs: a plan node applies the
  # same `DataFrame` method it represents, so lazy and eager results cannot
  # drift. The optimizer is where the extra work happens.
  module Plan
    # Column name an expression will produce.
    def self.output_name(expr : Expr) : String
      case expr
      when Alias then expr.name
      when Col   then expr.name
      else            raise MissingAlias.new(expr.key)
      end
    end

    # Splits `a & b & c` into its conjuncts, so each can be pushed separately.
    def self.conjuncts(expr : Expr) : Array(Expr)
      if expr.is_a?(Binary) && expr.op.and?
        conjuncts(expr.left) + conjuncts(expr.right)
      else
        [expr]
      end
    end

    def self.combine(exprs : Array(Expr)) : Expr?
      current : Expr? = nil
      exprs.each do |expr|
        current = if previous = current
                    previous.and(expr)
                  else
                    expr
                  end
      end
      current
    end

    abstract class Node
      abstract def schema : Arrow::Schema

      abstract def execute : DataFrame

      # Input nodes, in execution order.
      def children : Array(Node)
        [] of Node
      end

      # Returns a copy with different children; used by optimizer rewrites.
      # Leaves have none, so rebuilding them is the identity.
      def rebuild(children : Array(Node)) : Node
        return self if children.empty?

        raise UnsupportedOperation.new("#{self.class} cannot be rebuilt")
      end

      abstract def describe(io : IO, indent : Int) : Nil

      def name : String
        self.class.name.split("::").last
      end

      protected def line(io : IO, indent : Int, text : String) : Nil
        io << "  " * indent << text << '\n'
      end

      protected def describe_children(io : IO, indent : Int) : Nil
        children.each(&.describe(io, indent + 1))
      end
    end

    # A source with no rows, produced by constant folding a false predicate.
    class Empty < Node
      getter schema : Arrow::Schema

      def initialize(@schema : Arrow::Schema)
      end

      def execute : DataFrame
        DataFrame.new(@schema, [] of Arrow::RecordBatch)
      end

      def describe(io : IO, indent : Int) : Nil
        line(io, indent, "Empty")
      end
    end
  end
end
