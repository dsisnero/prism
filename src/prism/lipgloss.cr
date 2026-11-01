require "lipgloss"

module Prism
  # Styled CLI rendering through `lipgloss`.
  #
  # Opt-in by design:
  #
  # - `Prism::Display` (used by `to_s`/`inspect`) stays plain text, because logs,
  #   pipes and specs must never receive ANSI escapes;
  # - `lipgloss` is a *development* dependency of this shard, so consumers add it
  #   to their own `shard.yml` and `require "prism/lipgloss"` when they want
  #   boxed output.
  #
  # Selection, truncation and value formatting are shared with the plain
  # renderer through `Prism::Display.grid`, so both show exactly the same cells.
  module Styled
    extend self

    def render(
      frame : DataFrame,
      *,
      max_rows : Int32 = Display::DEFAULT_MAX_ROWS,
      max_cols : Int32 = Display::DEFAULT_MAX_COLS,
      max_string : Int32 = Display::DEFAULT_MAX_STRING,
      border : Lipgloss::Border = Lipgloss::Border.normal,
    ) : String
      grid = Display.grid(
        frame,
        max_rows: max_rows,
        max_cols: max_cols,
        max_string: max_string,
      )

      table = Lipgloss::StyleTable::Table.new
        .border(border)
        .border_header(true)

      if grid.headers.size <= MAX_SPLAT_HEADERS
        table = apply_headers(table, grid.headers).rows(grid.body_rows)
      else
        # Beyond the generated arities the header becomes a data row; a table
        # that wide is unreadable in a terminal anyway.
        table = table.rows(grid.labelled_rows)
      end

      "#{grid.shape}\n#{table.string}"
    end

    # Lipgloss' `Table#headers` takes a splat only, and Crystal cannot splat a
    # runtime array, so the call is generated for the arities we support.
    MAX_SPLAT_HEADERS = 16

    private def apply_headers(
      table : Lipgloss::StyleTable::Table,
      headers : Array(String),
    ) : Lipgloss::StyleTable::Table
      {% begin %}
        case headers.size
        {% for n in (1..16) %}
        when {{n}}
          table.headers({% for i in (0...n) %}{% if i > 0 %}, {% end %}headers[{{i}}]{% end %})
        {% end %}
        else
          table
        end
      {% end %}
    end
  end

  class DataFrame
    # Styled CLI rendering. Requires `require "prism/lipgloss"`.
    def to_styled(
      *,
      max_rows : Int32 = Display::DEFAULT_MAX_ROWS,
      max_cols : Int32 = Display::DEFAULT_MAX_COLS,
      max_string : Int32 = Display::DEFAULT_MAX_STRING,
      border : Lipgloss::Border = Lipgloss::Border.normal,
    ) : String
      Styled.render(
        self,
        max_rows: max_rows,
        max_cols: max_cols,
        max_string: max_string,
        border: border,
      )
    end
  end
end
