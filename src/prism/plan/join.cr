module Prism
  module Plan
    # Join of two plans.
    #
    # Like `GroupBy`, the output schema is obtained by running the eager join
    # against zero-row frames, so suffixing, coalescing and nullability rules
    # have exactly one implementation.
    class Join < Node
      getter left : Node
      getter right : Node
      getter on : Array(String)?
      getter left_on : Array(String)?
      getter right_on : Array(String)?
      getter how : Symbol
      getter suffix : String
      getter? nulls_equal : Bool
      getter coalesce : Bool?

      @schema : Arrow::Schema?

      def initialize(
        @left : Node,
        @right : Node,
        *,
        on : String | Symbol | Enumerable | Nil = nil,
        left_on : String | Symbol | Enumerable | Nil = nil,
        right_on : String | Symbol | Enumerable | Nil = nil,
        @how : Symbol = :inner,
        @suffix : String = "_right",
        @nulls_equal : Bool = false,
        @coalesce : Bool? = nil,
      )
        # Normalized here because `Enumerable` cannot appear in an ivar union.
        @on = on ? Prism::Join.column_names(on) : nil
        @left_on = left_on ? Prism::Join.column_names(left_on) : nil
        @right_on = right_on ? Prism::Join.column_names(right_on) : nil
      end

      def schema : Arrow::Schema
        @schema ||= begin
          left_frame = DataFrame.new(@left.schema, [] of Arrow::RecordBatch)
          right_frame = DataFrame.new(@right.schema, [] of Arrow::RecordBatch)
          left_frame.join(
            right_frame,
            on: @on,
            left_on: @left_on,
            right_on: @right_on,
            how: @how,
            suffix: @suffix,
            nulls_equal: @nulls_equal,
            coalesce: @coalesce,
          ).schema
        end
      end

      def children : Array(Node)
        [@left, @right]
      end

      def rebuild(children : Array(Node)) : Node
        Join.new(
          children[0],
          children[1],
          on: @on,
          left_on: @left_on,
          right_on: @right_on,
          how: @how,
          suffix: @suffix,
          nulls_equal: @nulls_equal,
          coalesce: @coalesce,
        )
      end

      def execute : DataFrame
        @left.execute.join(
          @right.execute,
          on: @on,
          left_on: @left_on,
          right_on: @right_on,
          how: @how,
          suffix: @suffix,
          nulls_equal: @nulls_equal,
          coalesce: @coalesce,
        )
      end

      def describe(io : IO, indent : Int) : Nil
        line(io, indent, "Join how=#{@how} on=#{@on.inspect}")
        describe_children(io, indent)
      end
    end
  end
end
