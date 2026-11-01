module Prism
  # Renders frames as aligned text tables.
  #
  # Defaults follow Polars: at most 10 rows, 8 columns, and 30 characters per
  # string cell. When a frame is larger, head and tail slices are shown with an
  # ellipsis between them, so a wide frame stays readable without losing the
  # shape.
  #
  # The layout is plain ASCII on purpose: `ljust` measures bytes and slicing a
  # wide string would misalign the table, so no box-drawing characters are used.
  module Display
    extend self

    DEFAULT_MAX_ROWS   = 10
    DEFAULT_MAX_COLS   =  8
    DEFAULT_MAX_STRING = 30
    MIN_MAX_STRING     =  4

    # Rows shown from the head when truncating.
    ELLIPSIS = "..."

    def render(
      frame : DataFrame,
      *,
      max_rows : Int32 = DEFAULT_MAX_ROWS,
      max_cols : Int32 = DEFAULT_MAX_COLS,
      max_string : Int32 = DEFAULT_MAX_STRING,
    ) : String
      raise ArgumentError.new("max_rows must be >= 1 (got #{max_rows})") if max_rows < 1
      raise ArgumentError.new("max_cols must be >= 1 (got #{max_cols})") if max_cols < 1
      if max_string < MIN_MAX_STRING
        raise ArgumentError.new("max_string must be >= #{MIN_MAX_STRING} (got #{max_string})")
      end

      names = frame.column_names
      columns = select_columns(names, max_cols)
      rows = select_rows(frame.row_count, max_rows)

      headers = columns.map do |position|
        if index = position
          names[index]
        else
          ELLIPSIS
        end
      end
      dtypes = columns.map do |position|
        if index = position
          short_dtype(frame.schema.field(index).data_type)
        else
          ELLIPSIS
        end
      end
      cells = columns.map do |index|
        rows.map { |row| cell_text(frame, names, index, row, max_string) }
      end

      widths = headers.map_with_index do |header, position|
        candidates = [header, dtypes[position]] + cells[position]
        candidates.max_of(&.size)
      end

      lines = [] of String
      lines << "shape: (#{frame.row_count}, #{names.size})"
      lines << render_row(headers, widths)
      lines << widths.map { |width| "-" * width }.join(" | ")
      lines << render_row(dtypes, widths)
      rows.each_index do |position|
        lines << render_row(cells.map { |column| column[position] }, widths)
      end

      lines.join('\n')
    end

    # Short dtype label used in the table header, Polars style.
    # A flat label table needs many branches; splitting it would not read better.
    # ameba:disable Metrics/CyclomaticComplexity
    def short_dtype(data_type : Arrow::DataType) : String
      case data_type.kind
      when .int8?                                  then "i8"
      when .int16?                                 then "i16"
      when .int32?                                 then "i32"
      when .int64?                                 then "i64"
      when Arrow::DataType::Kind::UInt8            then "u8"
      when Arrow::DataType::Kind::UInt16           then "u16"
      when Arrow::DataType::Kind::UInt32           then "u32"
      when Arrow::DataType::Kind::UInt64           then "u64"
      when .float32?                               then "f32"
      when .float64?                               then "f64"
      when .boolean?                               then "bool"
      when .utf8?, .large_utf8?, .utf8_view?       then "str"
      when .binary?, .large_binary?, .binary_view? then "bin"
      when .null?                                  then "null"
      when .date32?                                then "date32"
      when .date64?                                then "date64"
      when .timestamp?                             then "ts[#{unit_label(data_type)}]"
      when .duration?                              then "dur[#{unit_label(data_type)}]"
      else
        data_type.to_s
      end
    end

    private def unit_label(data_type : Arrow::DataType) : String
      case data_type.time_unit
      when Arrow::TimeUnit::Second      then "s"
      when Arrow::TimeUnit::Millisecond then "ms"
      when Arrow::TimeUnit::Microsecond then "us"
      when Arrow::TimeUnit::Nanosecond  then "ns"
      else                                   "?"
      end
    end

    private def cell_text(
      frame : DataFrame,
      names : Array(String),
      column : Int32?,
      row : Int32?,
      max_string : Int32,
    ) : String
      return ELLIPSIS unless (position = column) && (index = row)

      series = frame[names[position]]
      format_value(series[index], max_string)
    end

    private def format_value(value : Value, max_string : Int32) : String
      case value
      when Nil    then "null"
      when String then truncate(value, max_string)
      when Bytes  then truncate(value.hexstring, max_string)
      else             value.to_s
      end
    end

    private def truncate(text : String, limit : Int32) : String
      return text if text.size <= limit

      text[0, limit - ELLIPSIS.size] + ELLIPSIS
    end

    private def render_row(cells : Array(String), widths : Array(Int32)) : String
      cells
        .map_with_index { |cell, index| cell.ljust(widths[index]) }
        .join(" | ")
        .rstrip
    end

    # Column positions to render; `nil` stands for the ellipsis column.
    private def select_columns(names : Array(String), max_cols : Int32) : Array(Int32?)
      selected = [] of Int32?
      if names.size <= max_cols
        (0...names.size).each { |index| selected << index.to_i32 }
        return selected
      end

      head = max_cols // 2
      tail = max_cols - head
      (0...head).each { |index| selected << index.to_i32 }
      selected << nil
      (names.size - tail...names.size).each { |index| selected << index.to_i32 }
      selected
    end

    # Row positions to render; `nil` stands for the ellipsis row.
    private def select_rows(total : Int64, max_rows : Int32) : Array(Int32?)
      selected = [] of Int32?
      if total <= max_rows
        (0...total).each { |index| selected << index.to_i32 }
        return selected
      end

      head = max_rows // 2
      tail = max_rows - head
      (0...head).each { |index| selected << index.to_i32 }
      selected << nil
      (total - tail...total).each { |index| selected << index.to_i32 }
      selected
    end
  end

  class DataFrame
    # Renders the frame as a text table. See `Prism::Display`.
    def to_s(io : IO) : Nil
      io << Display.render(self)
    end

    # Renders with explicit limits.
    #
    # ```
    # df.to_s(max_rows: 20, max_cols: 4)
    # ```
    def to_s(
      *,
      max_rows : Int32 = Display::DEFAULT_MAX_ROWS,
      max_cols : Int32 = Display::DEFAULT_MAX_COLS,
      max_string : Int32 = Display::DEFAULT_MAX_STRING,
    ) : String
      Display.render(
        self,
        max_rows: max_rows,
        max_cols: max_cols,
        max_string: max_string,
      )
    end

    def inspect(io : IO) : Nil
      io << Display.render(self)
    end
  end
end
