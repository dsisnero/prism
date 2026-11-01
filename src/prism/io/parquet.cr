module Prism
  # Rows per batch when a Parquet file is streamed back into a frame.
  DEFAULT_BATCH_SIZE = 4_096

  # Reads a Parquet file into a frame.
  #
  # `columns` is projected into the reader: only those columns are decoded.
  # `predicate` is pushed into the reader as a Parquet row filter, so rows are
  # dropped per row group before the requested projection is materialized. The
  # columns it references are added to the read set and dropped again, exactly
  # as Polars does — so `columns: ["region"], predicate: col("qty") > 2` never
  # returns `qty`.
  #
  # The result keeps one chunk per reader batch, so `batch_size` controls the
  # chunking rather than forcing a single large batch.
  def self.read_parquet(
    path : String,
    *,
    columns : Enumerable | Nil = nil,
    batch_size : Int32 = DEFAULT_BATCH_SIZE,
    predicate : Expr | Nil = nil,
  ) : DataFrame
    file = Parquet::File::SerializedFileReader.new(
      Parquet::File::FileChunkReader.new(path)
    )

    read_parquet_file(file, columns, batch_size, predicate)
  end

  # Reads Parquet from memory. The IO is consumed in full: the reader needs
  # random access to the footer.
  def self.read_parquet(
    io : IO,
    *,
    columns : Enumerable | Nil = nil,
    batch_size : Int32 = DEFAULT_BATCH_SIZE,
    predicate : Expr | Nil = nil,
  ) : DataFrame
    file = Parquet::File::SerializedFileReader.new(
      Parquet::File::BytesChunkReader.new(io.getb_to_end)
    )

    read_parquet_file(file, columns, batch_size, predicate)
  end

  private def self.read_parquet_file(
    file : Parquet::File::FileReader,
    columns : Enumerable | Nil,
    batch_size : Int32,
    predicate : Expr | Nil,
  ) : DataFrame
    builder = Parquet::Arrow::ParquetRecordBatchReaderBuilder.try_new(file)
    available = builder.schema.fields.map(&.name)

    requested = columns.try(&.map(&.to_s).to_a)
    requested.try &.each do |name|
      raise ColumnNotFound.new(name, available) unless available.includes?(name)
    end

    predicate_columns = predicate ? predicate.columns : [] of String
    predicate_columns.each do |name|
      raise ColumnNotFound.new(name, available) unless available.includes?(name)
    end

    read_set = ((requested || available) + predicate_columns).uniq
    builder.with_projection(
      Parquet::Arrow::ProjectionMask.columns(builder.parquet_schema, read_set)
    )
    builder.with_batch_size(batch_size)

    # A predicate with no column references (a constant) has nothing to prune.
    pushdown = !predicate.nil? && !predicate_columns.empty?
    if expr = predicate
      add_row_filter(builder, predicate_columns, expr) if pushdown
    end

    reader = builder.build
    batches = [] of Arrow::RecordBatch
    while batch = reader.next_batch
      batches << batch
    end

    frame = DataFrame.new(reader.schema, batches)
    # Constant predicates cannot be pushed; everything else was filtered above.
    if expr = predicate
      frame = frame.filter(expr) unless pushdown
    end

    # `select` takes names variadically; the request is a runtime array.
    if names = requested
      frame.select(names.map { |name| Prism.col(name) })
    else
      frame
    end
  end

  # Writes a frame to a Parquet file.
  #
  # Defaults follow Polars rather than the Parquet crate: zstd level 3 and page
  # statistics on. `row_group_size` caps rows per row group.
  def self.write_parquet(
    path : String,
    frame : DataFrame,
    *,
    compression : Symbol = :zstd,
    compression_level : Int32? = nil,
    row_group_size : Int32? = nil,
    statistics : Bool = true,
  ) : Nil
    File.open(path, "w") do |io|
      write_parquet(
        io,
        frame,
        compression: compression,
        compression_level: compression_level,
        row_group_size: row_group_size,
        statistics: statistics,
      )
    end
  end

  # Writes a frame to an IO sink.
  #
  # The footer is written by `ArrowWriter#close`. If a write fails, the stream
  # is deliberately left without one: a partial file is invalid either way, and
  # closing over a broken stream would only produce a misleading "valid" file.
  def self.write_parquet(
    io : IO,
    frame : DataFrame,
    *,
    compression : Symbol = :zstd,
    compression_level : Int32? = nil,
    row_group_size : Int32? = nil,
    statistics : Bool = true,
  ) : Nil
    properties = parquet_properties(
      compression,
      compression_level,
      row_group_size,
      statistics,
    )
    writer = Parquet::Arrow::ArrowWriter.try_new(io, frame.schema, properties)

    # `close` writes the footer. It is skipped if a write raises, so a broken
    # stream never gets a footer that makes it look valid.
    frame.each_batch { |batch| writer.write(batch) }
    writer.close
  end

  # Pushes `expr` into the reader. The parameter is typed `Expr` on purpose: a
  # proc literal capturing a nilable local sends Crystal 1.22 into a compiler
  # crash ("Cast from Nil to Crystal::ProcInstanceType failed").
  private def self.add_row_filter(
    builder : Parquet::Arrow::ParquetRecordBatchReaderBuilder,
    columns : Array(String),
    expr : Expr,
  ) : Nil
    mask = Parquet::Arrow::ProjectionMask.columns(builder.parquet_schema, columns)
    filter = Parquet::Arrow::ArrowPredicateFn.new(mask) do |batch|
      ctx = EvalContext.new(batch)
      Datums.boolean_array(ctx.evaluate(expr), ctx)
    end

    builder.with_row_filter(
      Parquet::Arrow::RowFilter.new([filter] of Parquet::Arrow::ArrowPredicate)
    )
  end

  private def self.parquet_properties(
    compression : Symbol,
    compression_level : Int32?,
    row_group_size : Int32?,
    statistics : Bool,
  ) : Parquet::File::Properties::WriterProperties
    builder = Parquet::File::Properties::WriterProperties.builder
    builder.set_compression(compression_for(compression, compression_level))
    builder.set_max_row_group_row_count(row_group_size) if row_group_size
    unless statistics
      builder.set_statistics_enabled(
        Parquet::File::Properties::EnabledStatistics::None
      )
    end

    builder.build
  end

  private def self.compression_for(
    name : Symbol,
    level : Int32?,
  ) : Parquet::Compression
    case name
    when :uncompressed then Parquet::Compression::UNCOMPRESSED
    when :snappy       then Parquet::Compression::SNAPPY
    when :gzip         then Parquet::Compression.gzip(level || 6)
    when :brotli       then Parquet::Compression.brotli(level || 1)
    when :zstd         then Parquet::Compression.zstd(level || 3)
    when :lz4          then Parquet::Compression::LZ4
    when :lz4_raw      then Parquet::Compression::LZ4_RAW
    else
      raise ArgumentError.new(
        "unknown compression #{name}; use :uncompressed, :snappy, :gzip, " \
        ":brotli, :zstd, :lz4, or :lz4_raw"
      )
    end
  end

  class DataFrame
    # Writes this frame to a Parquet path. See `Prism.write_parquet`.
    def write_parquet(path : String, **options) : Nil
      Prism.write_parquet(path, self, **options)
    end

    # Writes this frame to a Parquet IO sink. See `Prism.write_parquet`.
    def write_parquet(io : IO, **options) : Nil
      Prism.write_parquet(io, self, **options)
    end
  end
end
