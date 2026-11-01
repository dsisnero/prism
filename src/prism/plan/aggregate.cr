module Prism
  module Plan
    # Grouped aggregation.
    #
    # The output schema must be identical to the eager `group_by(...).aggregate`
    # one, so it is computed by running that verb against a zero-row frame
    # instead of duplicating the layout rules here.
    class GroupBy < Node
      getter input : Node
      getter keys : Array(Expr)
      getter aggregates : Hash(String, Expr)
      getter null_keys : Symbol
      getter? maintain_order : Bool

      @schema : Arrow::Schema?

      def initialize(
        @input : Node,
        @keys : Array(Expr),
        @aggregates : Hash(String, Expr),
        @null_keys : Symbol = :first,
        @maintain_order : Bool = true,
      )
        raise ArgumentError.new("aggregate needs at least one column") if @aggregates.empty?
      end

      def schema : Arrow::Schema
        @schema ||= begin
          empty = DataFrame.new(@input.schema, [] of Arrow::RecordBatch)
          empty
            .group_by(@keys, null_keys: @null_keys, maintain_order: @maintain_order)
            .aggregate(@aggregates)
            .schema
        end
      end

      def children : Array(Node)
        [@input]
      end

      def rebuild(children : Array(Node)) : Node
        GroupBy.new(children[0], @keys, @aggregates, @null_keys, @maintain_order)
      end

      def execute : DataFrame
        @input.execute
          .group_by(@keys, null_keys: @null_keys, maintain_order: @maintain_order)
          .aggregate(@aggregates)
      end

      def describe(io : IO, indent : Int) : Nil
        keys = @keys.map { |key| Plan.output_name(key) }
        line(io, indent, "GroupBy [#{keys.join(", ")}] -> [#{@aggregates.keys.join(", ")}]")
        describe_children(io, indent)
      end
    end
  end
end
