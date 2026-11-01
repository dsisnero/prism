module Prism
  module Plan
    # In-memory source over an existing frame's batches.
    class ScanBatches < Node
      getter schema : Arrow::Schema
      getter batches : Array(Arrow::RecordBatch)

      def initialize(@schema : Arrow::Schema, @batches : Array(Arrow::RecordBatch))
      end

      def execute : DataFrame
        DataFrame.new(@schema, @batches)
      end

      def describe(io : IO, indent : Int) : Nil
        line(io, indent, "ScanBatches rows=#{@batches.sum(&.num_rows)}")
      end
    end

    # Parquet source.
    #
    # `projection`, `predicates`, `limit` and `offset` are the pushdown slots.
    # They start empty and the optimizer fills them; `execute` forwards whatever
    # is set to `Prism.read_parquet`, which handles the reader wiring.
    class ScanParquet < Node
      getter path : String
      getter full_schema : Arrow::Schema
      getter projection : Array(String)?
      getter predicates : Array(Expr)
      getter limit : Int32?
      getter offset : Int32?
      getter batch_size : Int32

      def initialize(
        @path : String,
        @full_schema : Arrow::Schema,
        @projection : Array(String)? = nil,
        @predicates : Array(Expr) = [] of Expr,
        @limit : Int32? = nil,
        @offset : Int32? = nil,
        @batch_size : Int32 = DEFAULT_BATCH_SIZE,
      )
      end

      def schema : Arrow::Schema
        projection = @projection
        return @full_schema unless projection

        Arrow::Schema.new(projection.map { |name| @full_schema.field_with_name(name) })
      end

      def execute : DataFrame
        Prism.read_parquet(
          @path,
          columns: @projection,
          predicate: Plan.combine(@predicates),
          batch_size: @batch_size,
          limit: @limit,
          offset: @offset,
        )
      end

      def with_projection(names : Array(String)) : ScanParquet
        ScanParquet.new(
          @path, @full_schema, names, @predicates, @limit, @offset, @batch_size
        )
      end

      def with_predicates(predicates : Array(Expr)) : ScanParquet
        ScanParquet.new(
          @path, @full_schema, @projection, predicates, @limit, @offset, @batch_size
        )
      end

      def with_limit(offset : Int32, limit : Int32) : ScanParquet
        ScanParquet.new(
          @path, @full_schema, @projection, @predicates, limit, offset, @batch_size
        )
      end

      # Reads the file's schema without reading data (footer only).
      def self.open(path : String, *, batch_size : Int32 = DEFAULT_BATCH_SIZE) : ScanParquet
        file = Parquet::File::SerializedFileReader.new(
          Parquet::File::FileChunkReader.new(path)
        )
        schema = Parquet::Arrow::ParquetRecordBatchReaderBuilder.try_new(file).schema

        new(path, schema, batch_size: batch_size)
      end

      def describe(io : IO, indent : Int) : Nil
        line(io, indent, "ScanParquet #{@path.inspect}")
        if projection = @projection
          line(io, indent + 1, "projection: [#{projection.join(", ")}]")
        end
        @predicates.each do |predicate|
          line(io, indent + 1, "predicate: #{predicate.key}")
        end
        line(io, indent + 1, "limit: #{@limit}") if @limit
        line(io, indent + 1, "offset: #{@offset}") if @offset
      end
    end
  end
end
