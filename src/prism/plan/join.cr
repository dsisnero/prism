require "set"

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

      # Columns each input must still produce so that the outputs named in
      # `required` (a subset of this node's schema) can be built.
      #
      # Both sides gain their join keys, because the hash table needs them even
      # when nothing above reads them. A right column whose output name is
      # suffixed also keeps the left column that forced the suffix: the suffix
      # is computed from the *actual* left schema, so dropping the shadowed
      # column would rename the right one out from under the plan.
      #
      # Returns `nil` when a required name cannot be attributed to a side, so
      # the optimizer leaves both inputs untouched rather than pruning a column
      # something above still reads.
      def input_requirements(required : Set(String)) : Tuple(Set(String), Set(String))?
        left_names = @left.schema.fields.map(&.name)
        right_names = @right.schema.fields.map(&.name)
        left_keys, right_keys = Prism::Join.resolve_keys(@on, @left_on, @right_on)
        how = @how == :outer ? :full : @how

        # Semi/anti joins emit left rows and left columns only; the right side
        # is read for its keys and nothing else.
        if {:semi, :anti}.includes?(how)
          return nil unless required.all? { |name| left_names.includes?(name) }

          return {required | left_keys.to_set, right_keys.to_set}
        end

        # Polars' default: coalesce everywhere except an explicit full join.
        coalesce = @coalesce || how != :full
        left = Set(String).new
        right = Set(String).new

        required.each do |name|
          # A left column keeps its own name in the output.
          if left_names.includes?(name)
            left << name
            next
          end

          source = right_names.find do |candidate|
            right_output_name(candidate, left_names, left_keys, coalesce) == name
          end
          return nil unless source

          right << source
        end

        right.each do |name|
          next unless left_names.includes?(name)
          next if coalesce && left_keys.includes?(name)

          left << name
        end

        {left | left_keys.to_set, right | right_keys.to_set}
      end

      def describe(io : IO, indent : Int) : Nil
        line(io, indent, "Join how=#{@how} on=#{@on.inspect}")
        describe_children(io, indent)
      end

      # Name a right column is emitted under, or `nil` when coalescing drops it.
      # Mirrors `Prism::Join#compose` exactly.
      private def right_output_name(
        name : String,
        left_names : Array(String),
        left_keys : Array(String),
        coalesce : Bool,
      ) : String?
        return nil if coalesce && left_keys.includes?(name)

        left_names.includes?(name) ? "#{name}#{@suffix}" : name
      end
    end
  end
end
