module Prism
  # Type mapping for `Prism.define_schema`.
  #
  # Declaring a field's Crystal type is what makes the generated accessors typed;
  # the Arrow dtype is derived here so a typo such as `id: Int65` fails to
  # compile instead of producing a mismatched schema.
  module Typed
    extend self

    # Crystal type -> Arrow dtype used by a schema declaration.
    def dtype_for(type : Int8.class) : Arrow::DataType
      Arrow::DataType.int8
    end

    def dtype_for(type : Int16.class) : Arrow::DataType
      Arrow::DataType.int16
    end

    def dtype_for(type : Int32.class) : Arrow::DataType
      Arrow::DataType.int32
    end

    def dtype_for(type : Int64.class) : Arrow::DataType
      Arrow::DataType.int64
    end

    def dtype_for(type : UInt8.class) : Arrow::DataType
      Arrow::DataType.uint8
    end

    def dtype_for(type : UInt16.class) : Arrow::DataType
      Arrow::DataType.uint16
    end

    def dtype_for(type : UInt32.class) : Arrow::DataType
      Arrow::DataType.uint32
    end

    def dtype_for(type : UInt64.class) : Arrow::DataType
      Arrow::DataType.uint64
    end

    def dtype_for(type : Float32.class) : Arrow::DataType
      Arrow::DataType.float32
    end

    def dtype_for(type : Float64.class) : Arrow::DataType
      Arrow::DataType.float64
    end

    def dtype_for(type : Bool.class) : Arrow::DataType
      Arrow::DataType.boolean
    end

    def dtype_for(type : String.class) : Arrow::DataType
      Arrow::DataType.utf8
    end

    def dtype_for(type : Bytes.class) : Arrow::DataType
      Arrow::DataType.binary
    end

    # `Time` maps to a microsecond timestamp, matching `Prism::Literals`.
    def dtype_for(type : Time.class) : Arrow::DataType
      Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond)
    end

    def dtype_for(type : T.class) : Arrow::DataType forall T
      {% raise "Prism.define_schema does not support #{T}; supported: Int8/16/32/64, UInt8/16/32/64, Float32/64, Bool, String, Bytes, Time" %}
    end
  end

  # Verbs shared by every generated typed frame.
  #
  # `S` is the schema struct, whose properties are `Prism::Expr` field
  # references. These verbs live outside the macro because `def select` cannot
  # be written inside a macro body: the parser reads `select` as the
  # channel-select keyword and reports an unterminated macro.
  #
  # Schema-preserving verbs return `self`, so the generated frame type survives.
  # Verbs that change the schema return a plain `Prism::DataFrame`, because
  # Crystal cannot refine a return type through an arbitrary call (plan §4.1).
  module TypedVerbs(S)
    abstract def frame : ::Prism::DataFrame

    abstract def fields : S

    def schema : ::Arrow::Schema
      frame.schema
    end

    def row_count : Int64
      frame.row_count
    end

    def columns : Array(String)
      frame.column_names
    end

    def [](name : String | Symbol) : ::Prism::Series
      frame[name]
    end

    def filter(& : S -> ::Prism::Expr) : self
      self.class.new(frame.filter(yield fields))
    end

    def filter(expr : ::Prism::Expr) : self
      self.class.new(frame.filter(expr))
    end

    def select(& : S -> ::Prism::Expr) : ::Prism::DataFrame
      frame.select(yield(fields))
    end

    def select(*exprs : ::Prism::Expr) : ::Prism::DataFrame
      frame.select(exprs.map(&.as(::Prism::Expr)))
    end

    def sort_by(
      *keys : String | Symbol | ::Prism::SortKey,
      order : Symbol = :asc,
      nulls : Symbol = :last,
    ) : self
      self.class.new(frame.sort_by(*keys, order: order, nulls: nulls))
    end

    def head(count : Int) : self
      self.class.new(frame.head(count))
    end

    def limit(count : Int) : self
      self.class.new(frame.slice(0, count))
    end

    def slice(offset : Int, count : Int) : self
      self.class.new(frame.slice(offset, count))
    end

    def with_column(
      name : String | Symbol,
      expr : ::Prism::Expr,
    ) : ::Prism::DataFrame
      frame.with_column(name, expr)
    end

    def lazy : ::Prism::LazyFrame
      frame.lazy
    end

    def to_arrow : ::Arrow::RecordBatch
      frame.to_arrow
    end

    def each_batch(& : ::Arrow::RecordBatch ->) : Nil
      frame.each_batch { |batch| yield batch }
    end

    def to_s(io : IO) : Nil
      io << "TypedFrame(" << row_count << " rows)"
    end
  end

  # Declares a compile-time schema.
  #
  # ```
  # Prism.define_schema UserSchema, id: Int64, name: String, active: Bool
  #
  # typed = UserSchema.from_arrow(batch)
  # typed.filter(&.active).select(&.name)
  # typed.name # => Prism::Series
  # ```
  #
  # Generates:
  #
  # - `UserSchema.schema : Arrow::Schema` (and `SCHEMA`);
  # - `UserSchema.<field> : Prism::Expr` for composing verbs outside a block;
  # - `UserSchema.from_arrow(batch)` / `.from_dataframe(frame) : UserSchema::Frame`,
  #   which validate field names and dtypes and raise `Prism::DtypeError`;
  # - `UserSchema::Frame`, a typed view whose schema-preserving verbs keep the type
  #   and whose schema-changing verbs return a plain `Prism::DataFrame`.
  #
  # The first parameter is named `schema`, not `name`: a macro parameter called
  # `name` collides with a field of that name and fails to compile (C8).
  macro define_schema(schema, **fields)
  {% if fields.empty? %}
    {% raise "Prism.define_schema #{schema} needs at least one field" %}
  {% end %}

  struct {{schema}}
    SCHEMA = ::Arrow::Schema.new(::Arrow::Fields.from([
      {% for field, type in fields %}
        ::Arrow::Field.new(
          {{field.id.stringify}},
          ::Prism::Typed.dtype_for({{type}}),
          true,
        ),
      {% end %}
    ]))

    def self.schema : ::Arrow::Schema
      SCHEMA
    end

    {% for field, type in fields %}
      # Field expression, for composing verbs outside a block.
      def self.{{field.id}} : ::Prism::Expr
        ::Prism.col({{field.id.stringify}})
      end

      def {{field.id}} : ::Prism::Expr
        ::Prism.col({{field.id.stringify}})
      end
    {% end %}

    def self.from_arrow(batch : ::Arrow::RecordBatch) : Frame
      from_dataframe(::Prism.from_arrow(batch))
    end

    def self.from_dataframe(frame : ::Prism::DataFrame) : Frame
      SCHEMA.fields.each do |expected|
        actual = frame.schema.fields.find_field(expected.name)
        if actual.nil? || actual.data_type != expected.data_type
          found = actual ? actual.data_type.to_s : "missing"
          raise ::Prism::DtypeError.new(
            "field #{expected.name.inspect} expects #{expected.data_type}, got #{found}"
          )
        end
      end

      Frame.new(frame)
    end

    # Typed view over a frame whose schema matches this declaration.
    #
    # The shared verbs come from `Prism::TypedVerbs`; only the per-field
    # accessors are generated here.
    struct Frame
      include ::Prism::TypedVerbs({{schema}})

      getter frame : ::Prism::DataFrame
      getter fields : {{schema}}

      def initialize(@frame : ::Prism::DataFrame)
        @fields = {{schema}}.new
      end

      {% for field, type in fields %}
        def {{field.id}} : ::Prism::Series
          @frame[{{field.id.stringify}}]
        end
      {% end %}
    end
  end
end
end
