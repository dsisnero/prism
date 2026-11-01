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
      getter slice : Slice?

      @schema : Arrow::Schema?

      def initialize(
        @input : Node,
        @keys : Array(Expr),
        @aggregates : Hash(String, Expr),
        @null_keys : Symbol = :first,
        @maintain_order : Bool = true,
        @slice : Slice? = nil,
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
        GroupBy.new(
          children[0], @keys, @aggregates, @null_keys, @maintain_order, @slice
        )
      end

      def with_slice(slice : Slice) : GroupBy
        GroupBy.new(@input, @keys, @aggregates, @null_keys, @maintain_order, slice)
      end

      def execute : DataFrame
        grouped = @input.execute
          .group_by(@keys, null_keys: @null_keys, maintain_order: @maintain_order)
          .aggregate(@aggregates)
        slice = @slice
        slice ? grouped.slice(slice.offset, slice.count) : grouped
      end

      def describe(io : IO, indent : Int) : Nil
        keys = @keys.map { |key| Plan.output_name(key) }
        line(io, indent, "GroupBy [#{keys.join(", ")}] -> [#{@aggregates.keys.join(", ")}]")
        if slice = @slice
          line(io, indent + 1, "slice: offset=#{slice.offset} count=#{slice.count}")
        end
        describe_children(io, indent)
      end
    end
  end
end
