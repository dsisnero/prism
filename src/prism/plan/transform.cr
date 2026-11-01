module Prism
  module Plan
    # Row filter. The optimizer pushes it into a scan when it can, otherwise it
    # stays here and runs eagerly.
    class Filter < Node
      getter input : Node
      getter expr : Expr

      def initialize(@input : Node, @expr : Expr)
      end

      def schema : Arrow::Schema
        @input.schema
      end

      def children : Array(Node)
        [@input]
      end

      def rebuild(children : Array(Node)) : Node
        Filter.new(children[0], @expr)
      end

      def execute : DataFrame
        @input.execute.filter(@expr)
      end

      def describe(io : IO, indent : Int) : Nil
        line(io, indent, "Filter #{@expr.key}")
        describe_children(io, indent)
      end
    end

    # Projection. A bare `Col` is a view of the input column.
    class Select < Node
      getter input : Node
      getter exprs : Array(Expr)

      def initialize(@input : Node, @exprs : Array(Expr))
        raise ArgumentError.new("select needs at least one column") if @exprs.empty?
      end

      def schema : Arrow::Schema
        Arrow::Schema.new(@exprs.map { |expr| field_for(expr) })
      end

      def children : Array(Node)
        [@input]
      end

      def rebuild(children : Array(Node)) : Node
        Select.new(children[0], @exprs)
      end

      def execute : DataFrame
        @input.execute.select(@exprs)
      end

      def describe(io : IO, indent : Int) : Nil
        names = @exprs.map { |expr| Plan.output_name(expr) }
        line(io, indent, "Select [#{names.join(", ")}]")
        describe_children(io, indent)
      end

      private def field_for(expr : Expr) : Arrow::Field
        case expr
        when Col
          input_field(expr.name)
        when Alias
          # Renaming a column must not change its nullability, otherwise a
          # rename would report a different schema from the eager verb.
          inner = expr.operand
          if inner.is_a?(Col)
            # `Field#with_name` mutates, so copy first (C16).
            input_field(inner.name).copy.with_name(expr.name)
          else
            Arrow::Field.new(expr.name, expr.dtype(@input.schema), true)
          end
        else
          Arrow::Field.new(Plan.output_name(expr), expr.dtype(@input.schema), true)
        end
      end

      private def input_field(name : String) : Arrow::Field
        field = @input.schema.fields.find_field(name)
        return field if field

        raise ColumnNotFound.new(name, @input.schema.fields.map(&.name))
      end
    end

    # Adds or replaces one column.
    class WithColumn < Node
      getter input : Node
      getter name : String
      getter expr : Expr

      def initialize(@input : Node, @name : String, @expr : Expr)
      end

      def schema : Arrow::Schema
        field = Arrow::Field.new(@name, @expr.dtype(@input.schema), true)
        fields = @input.schema.fields.to_a
        index = fields.index { |existing| existing.name == @name }

        if index
          replaced = fields.dup
          replaced[index] = field
          Arrow::Schema.new(replaced)
        else
          Arrow::Schema.new(fields + [field])
        end
      end

      def children : Array(Node)
        [@input]
      end

      def rebuild(children : Array(Node)) : Node
        WithColumn.new(children[0], @name, @expr)
      end

      def execute : DataFrame
        @input.execute.with_column(@name, @expr)
      end

      def describe(io : IO, indent : Int) : Nil
        line(io, indent, "WithColumn #{@name} = #{@expr.key}")
        describe_children(io, indent)
      end
    end

    # Reorders rows; exposes a `slice` so a limit can attach to this node
    # instead of spilling into the scan.
    class Sort < Node
      getter input : Node
      getter keys : Array(SortKey)

      def initialize(@input : Node, @keys : Array(SortKey))
      end

      def schema : Arrow::Schema
        @input.schema
      end

      def children : Array(Node)
        [@input]
      end

      def rebuild(children : Array(Node)) : Node
        Sort.new(children[0], @keys)
      end

      def execute : DataFrame
        @input.execute.sort_by(@keys)
      end

      def describe(io : IO, indent : Int) : Nil
        described = @keys.map do |key|
          "#{key.expr.key} #{key.descending? ? "desc" : "asc"}"
        end
        line(io, indent, "Sort [#{described.join(", ")}]")
        describe_children(io, indent)
      end
    end

    # Drops duplicate rows by key.
    class Distinct < Node
      getter input : Node
      getter keys : Array(String)

      def initialize(@input : Node, @keys : Array(String))
      end

      def schema : Arrow::Schema
        @input.schema
      end

      def children : Array(Node)
        [@input]
      end

      def rebuild(children : Array(Node)) : Node
        Distinct.new(children[0], @keys)
      end

      def execute : DataFrame
        @input.execute.distinct(@keys)
      end

      def describe(io : IO, indent : Int) : Nil
        line(io, indent, "Distinct [#{@keys.join(", ")}]")
        describe_children(io, indent)
      end
    end

    # Keeps the last `count` rows. A trailing window cannot be pushed into a
    # scan, because the row count is not known until the source has run.
    class Tail < Node
      getter input : Node
      getter count : Int32

      def initialize(@input : Node, @count : Int32)
      end

      def schema : Arrow::Schema
        @input.schema
      end

      def children : Array(Node)
        [@input]
      end

      def rebuild(children : Array(Node)) : Node
        Tail.new(children[0], @count)
      end

      def execute : DataFrame
        @input.execute.tail(@count)
      end

      def describe(io : IO, indent : Int) : Nil
        line(io, indent, "Tail #{@count}")
        describe_children(io, indent)
      end
    end

    # Keeps a window of rows.
    class Limit < Node
      getter input : Node
      getter offset : Int32
      getter count : Int32

      def initialize(@input : Node, @offset : Int32, @count : Int32)
      end

      def schema : Arrow::Schema
        @input.schema
      end

      def children : Array(Node)
        [@input]
      end

      def rebuild(children : Array(Node)) : Node
        Limit.new(children[0], @offset, @count)
      end

      def execute : DataFrame
        @input.execute.slice(@offset, @count)
      end

      def describe(io : IO, indent : Int) : Nil
        line(io, indent, "Limit offset=#{@offset} count=#{@count}")
        describe_children(io, indent)
      end
    end
  end
end
