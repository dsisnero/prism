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

    # Pre-computed text grid, shared by the plain and styled renderers so both
    # agree on selection, truncation and value formatting.
    struct Grid
      getter shape : String
      getter headers : Array(String)
      getter dtypes : Array(String)
      getter rows : Array(Array(String))

      def initialize(
        @shape : String,
        @headers : Array(String),
        @dtypes : Array(String),
        @rows : Array(Array(String)),
      )
      end

      # Dtype labels above the data, for renderers that style rows rather than
      # columns.
      def body_rows : Array(Array(String))
        [@dtypes] + @rows
      end

      # Header and dtype labels stacked as data rows, for renderers that cannot
      # set a real header row.
      def labelled_rows : Array(Array(String))
        [@headers] + body_rows
      end
    end

    def render(
      frame : DataFrame,
      *,
      max_rows : Int32 = DEFAULT_MAX_ROWS,
      max_cols : Int32 = DEFAULT_MAX_COLS,
      max_string : Int32 = DEFAULT_MAX_STRING,
    ) : String
      grid = self.grid(
        frame,
        max_rows: max_rows,
        max_cols: max_cols,
        max_string: max_string,
      )

      widths = grid.headers.map_with_index do |header, position|
        cell_width = grid.rows.max_of?(&.[position].size) || 0
        [header.size, grid.dtypes[position].size, cell_width].max
      end

      lines = [] of String
      lines << grid.shape
      lines << render_row(grid.headers, widths)
      lines << widths.map { |width| "-" * width }.join(" | ")
      lines << render_row(grid.dtypes, widths)
      grid.rows.each { |row| lines << render_row(row, widths) }

      lines.join('\n')
    end

    def grid(
      frame : DataFrame,
      *,
      max_rows : Int32 = DEFAULT_MAX_ROWS,
      max_cols : Int32 = DEFAULT_MAX_COLS,
      max_string : Int32 = DEFAULT_MAX_STRING,
    ) : Grid
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
      body = rows.map do |row|
        columns.map { |column| cell_text(frame, names, column, row, max_string) }
      end

      Grid.new("shape: (#{frame.row_count}, #{names.size})", headers, dtypes, body)
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

    # Nested values render inside their container, where a bare string would be
    # ambiguous, so strings are quoted one level down.
    private def format_value(value : Value, max_string : Int32, nested : Bool = false) : String
      case value
      when Nil    then "null"
      when String then nested ? %("#{truncate(value, max_string)}") : truncate(value, max_string)
      when Bytes  then truncate(value.hexstring, max_string)
      when Arrow::IntervalValue
        interval_text(value)
      when Array
        "[" + value.map { |item| format_value(item, max_string, true) }.join(", ") + "]"
      when Hash(String, Value)
        pairs = value.map { |key, item| "#{key}: #{format_value(item, max_string, true)}" }
        "{" + pairs.join(", ") + "}"
      when Hash(Value, Value)
        pairs = value.map do |key, item|
          "#{format_value(key, max_string, true)}: #{format_value(item, max_string, true)}"
        end
        "{" + pairs.join(", ") + "}"
      else value.to_s
      end
    end

    # Arrow's `IntervalMonthDayNano` text (`1 mons 2 days 3.000000004 secs`),
    # with zero components omitted and a zero interval as `0 secs`. A
    # materialized interval does not carry its original unit, so a year-month
    # interval reads as months here rather than as `{years} years {months} mons`;
    # cast the column to a string dtype for the unit-specific Arrow text.
    private def interval_text(interval : Arrow::IntervalValue) : String
      return "0 secs" if interval.months == 0 && interval.days == 0 && interval.nanos == 0

      parts = [] of String
      parts << "#{interval.months} mons" unless interval.months == 0
      parts << "#{interval.days} days" unless interval.days == 0
      parts << nanoseconds_text(interval.nanos) unless interval.nanos == 0
      parts.join(' ')
    end

    # Arrow's `NanosecondsFormatter`: `{hours} hours {mins} mins {secs}.{nanos}
    # secs`, omitting zero components.
    private def nanoseconds_text(nanoseconds : Int64) : String
      secs = nanoseconds.tdiv(Arrow::NANOS_PER_SECOND)
      mins = secs.tdiv(60_i64)
      hours = mins.tdiv(60_i64)
      secs -= mins * 60_i64
      mins -= hours * 60_i64
      nanos = nanoseconds.remainder(Arrow::NANOS_PER_SECOND)

      parts = [] of String
      parts << "#{hours} hours" unless hours == 0
      parts << "#{mins} mins" unless mins == 0
      if secs != 0 || nanos != 0
        sign = (secs < 0 || nanos < 0) ? "-" : ""
        parts << "#{sign}#{secs.abs}.#{nanos.abs.to_s.rjust(9, '0')} secs"
      end
      parts.join(' ')
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
