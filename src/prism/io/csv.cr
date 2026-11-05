module Prism
  # Reads a CSV file into a frame.
  #
  # Without an explicit `schema`, the schema is inferred from the first
  # `infer_rows` records (Polars' `infer_schema_length`). `infer_rows: 0` makes
  # every column a string, matching Polars' `infer_schema_length=0` — the safe
  # choice for messy input, because nothing is guessed from the data.
  #
  # The all-string schema is built here rather than left to Arrow: `arrow-cr`'s
  # `infer_schema(bytes, 0)` is upstream-faithful and infers *no* types, so it
  # returns the header with every field `Null`. Passing that through would read
  # the right number of rows with every value null, silently.
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
    if infer_rows < 0
      raise ArgumentError.new("infer_rows must be >= 0 (got #{infer_rows})")
    end

    format = Arrow::Csv::Format.new
    format.with_header(header)
    format.with_delimiter(delimiter)

    resolved = if supplied = schema
                 match_csv_schema(format, bytes, supplied, header)
               else
                 infer_csv_schema(format, bytes, infer_rows)
               end

    # A matched schema is in the file's own order, so the reader can check the
    # header it parses against it; a disagreement means Prism's header reader and
    # the reader's parted ways, which is a bug rather than a user error.
    format.with_header_validation(true) if schema && header

    reader = Arrow::Csv::ReaderBuilder.new(resolved, format, batch_size).build(bytes)

    batches = [] of Arrow::RecordBatch
    while batch = reader.maybe_next
      batches << batch
    end

    DataFrame.new(reader.schema, batches)
  end

  # Writes a frame as CSV to an IO sink.
  #
  # CSV is text with inferred types, so a few dtypes have no faithful text form:
  # bytes would be written as hex and read back as a different value, and a
  # nested value has no single cell. Those raise `Prism::UnsupportedOperation`
  # naming the column instead of writing a file that lies (or letting Arrow
  # reject it without saying which column), and the message points at the
  # formats that can carry them.
  #
  # Everything else is written as text and comes back exactly when the same
  # schema is supplied to `read_csv` — see `spec/prism/io/roundtrip_spec.cr`.
  def self.write_csv(
    io : IO,
    frame : DataFrame,
    *,
    header : Bool = true,
  ) : Nil
    ensure_csv_writable!(frame)

    writer = Arrow::Csv::WriterBuilder.new.with_header(header).build(io)
    frame.each_batch { |batch| writer.write(batch) }
  end

  # Writes a frame as CSV to a path.
  #
  # The frame is checked before the file is opened: a refusal must not truncate
  # an existing file on its way out.
  def self.write_csv(
    path : String,
    frame : DataFrame,
    *,
    header : Bool = true,
  ) : Nil
    ensure_csv_writable!(frame)

    File.open(path, "w") do |io|
      write_csv(io, frame, header: header)
    end
  end

  # Matches a supplied schema to the file by name.
  #
  # The reader parses by position, so handing it a schema in another order
  # silently swaps two same-typed columns, and one whose names the file does not
  # have silently relabels the columns (a frame that says `x` and holds `a`). A
  # schema names columns, so it is reordered to the file's header here, a
  # mismatch raises `Prism::SchemaError` naming the difference, and two fields of
  # one name are rejected like every other schema Prism builds (Phase 29).
  #
  # With `header: false` there are no names to match, so position is the only
  # meaning a schema can have and only the width is checked.
  private def self.match_csv_schema(
    format : Arrow::Csv::Format,
    bytes : Bytes,
    schema : Arrow::Schema,
    header : Bool,
  ) : Arrow::Schema
    schema_names = schema.fields.map(&.name)
    Prism.ensure_unique_names!(schema_names, "the CSV schema")

    first = csv_first_record(format, bytes)
    unless header
      return schema if first.empty? || first.size == schema_names.size

      raise SchemaError.new(
        "CSV has #{first.size} columns but the schema names #{schema_names.size}; " \
        "without a header a schema is matched by position"
      )
    end

    missing = first - schema_names
    unexpected = schema_names - first
    unless missing.empty? && unexpected.empty?
      raise SchemaError.new(
        "CSV schema does not match the file: " \
        "#{schema_mismatch(missing, unexpected)} (file columns: #{first.join(", ")})"
      )
    end

    # The file's order wins, so the reader parses the order its header describes
    # and each schema dtype lands on the column it names.
    Arrow::Schema.new(Arrow::Fields.from(first.map { |name| schema.field_with_name(name) }))
  end

  private def self.schema_mismatch(missing : Array(String), unexpected : Array(String)) : String
    parts = [] of String
    unless missing.empty?
      parts << "missing #{missing.map { |name| "column #{name.inspect}" }.join(", ")}"
    end
    unless unexpected.empty?
      parts << "the schema names #{unexpected.map { |name| "column #{name.inspect}" }.join(", ")}, " \
               "which the file does not have"
    end
    parts.join("; ")
  end

  # The first record, with the same decoder the reader uses, so quoting,
  # delimiters, comments and line endings are handled once. Bounded to one record
  # rather than decoding the file.
  private def self.csv_first_record(format : Arrow::Csv::Format, bytes : Bytes) : Array(String)
    decoder = Arrow::Csv::RecordDecoder.new(
      delimiter: format.delimiter || 0x2C_u8,
      quote: format.quote || 0x22_u8,
      escape: format.escape,
      terminator: format.terminator,
      comment: format.comment,
      num_columns: 0,
      truncated_rows: format.truncated_rows?,
      enforce_columns: false,
    )
    decoder.feed(bytes)
    decoder.feed(Bytes.empty)

    rows = [] of Array(Bytes)
    return [] of String if decoder.decode_into(rows, 1) == 0

    rows[0].map { |field| String.new(field) }
  end

  # The schema for a frame read from `bytes`.
  #
  # `infer_rows: 0` means "do not guess": take the names Arrow found in the
  # header and make every field a nullable string, which is Polars'
  # `infer_schema_length=0` contract. Anything else is Arrow's inference, with
  # one correction: a column whose window held no value infers `Null`, and a
  # `Null` column then reads every *later* value as null — a 101st row's `5`
  # would disappear because the first 100 records were empty. `Null` is never a
  # useful CSV inference, so it falls back to `Utf8`, the one dtype that cannot
  # lose a value. A header-only file has no rows at all and lands here too.
  private def self.infer_csv_schema(
    format : Arrow::Csv::Format,
    bytes : Bytes,
    infer_rows : Int32,
  ) : Arrow::Schema
    inferred = format.infer_schema(bytes, infer_rows).first

    fields = inferred.fields.to_a.map do |field|
      if infer_rows == 0 || field.data_type.null?
        Arrow::Field.new(field.name, Arrow::DataType.utf8, true)
      else
        field
      end
    end

    Arrow::Schema.new(Arrow::Fields.from(fields))
  end

  # Rejects a dtype CSV cannot carry, naming the column.
  #
  # `arrow-cr` writes bytes as hex (which a reader infers as an integer with a
  # different value) and raises `Csv error: Nested type List(Int64) is not
  # supported in CSV` for nested columns, which names the type but not the
  # column. Both are answered here, before a byte is written.
  private def self.ensure_csv_writable!(frame : DataFrame) : Nil
    frame.schema.fields.each do |field|
      dtype = field.data_type
      reason = if dtype.binary?
                 "bytes have no text form, so hex would read back as a different value"
               elsif dtype.nested?
                 "a nested value has no single text form"
               end
      next unless reason

      raise UnsupportedOperation.new(
        "cannot write column #{field.name.inspect} (#{dtype}) to CSV: #{reason}; " \
        "use Parquet or IPC, which carry any dtype"
      )
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
