module Prism
  # Reads a CSV file into a frame.
  #
  # Without an explicit `schema`, the schema is inferred from the first
  # `infer_rows` records (Polars' `infer_schema_length`). `infer_rows: 0` makes
  # every column a string, which is the safe choice for messy input.
  def self.read_csv(
    path : String,
    *,
    schema : Arrow::Schema? = nil,
    header : Bool = true,
    delimiter : Char = ',',
    infer_rows : Int32 = 100,
    batch_size : Int32 = 1_024,
  ) : DataFrame
    read_csv_bytes(
      File.read(path).to_slice,
      schema,
      header,
      delimiter,
      infer_rows,
      batch_size,
    )
  end

  # Reads CSV from memory.
  def self.read_csv(
    io : IO,
    *,
    schema : Arrow::Schema? = nil,
    header : Bool = true,
    delimiter : Char = ',',
    infer_rows : Int32 = 100,
    batch_size : Int32 = 1_024,
  ) : DataFrame
    read_csv_bytes(
      io.getb_to_end,
      schema,
      header,
      delimiter,
      infer_rows,
      batch_size,
    )
  end

  private def self.read_csv_bytes(
    bytes : Bytes,
    schema : Arrow::Schema?,
    header : Bool,
    delimiter : Char,
    infer_rows : Int32,
    batch_size : Int32,
  ) : DataFrame
    format = Arrow::Csv::Format.new
    format.with_header(header)
    format.with_delimiter(delimiter)

    resolved = schema || format.infer_schema(bytes, infer_rows).first
    reader = Arrow::Csv::ReaderBuilder.new(resolved, format, batch_size).build(bytes)

    batches = [] of Arrow::RecordBatch
    while batch = reader.maybe_next
      batches << batch
    end

    DataFrame.new(reader.schema, batches)
  end

  # Writes a frame as CSV to an IO sink.
  def self.write_csv(
    io : IO,
    frame : DataFrame,
    *,
    header : Bool = true,
  ) : Nil
    writer = Arrow::Csv::WriterBuilder.new.with_header(header).build(io)
    frame.each_batch { |batch| writer.write(batch) }
  end

  # Writes a frame as CSV to a path.
  def self.write_csv(
    path : String,
    frame : DataFrame,
    *,
    header : Bool = true,
  ) : Nil
    File.open(path, "w") do |io|
      write_csv(io, frame, header: header)
    end
  end

  class DataFrame
    # Writes this frame as CSV. See `Prism.write_csv`.
    def write_csv(path : String, **options) : Nil
      Prism.write_csv(path, self, **options)
    end

    def write_csv(io : IO, **options) : Nil
      Prism.write_csv(io, self, **options)
    end
  end
end
