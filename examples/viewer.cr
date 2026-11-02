# Interactive table viewer — the TUI counterpart to `DataFrame#to_styled`.
#
#     crystal run examples/viewer.cr
#
# Needs the development dependencies (`make install`): `bubbletea` runs the event
# loop, `bubbles` provides the table viewport, `ultraviolet` the terminal.
# Keys: arrows / page up / page down scroll, `q` or ctrl+c quits.
require "../src/prism"
require "bubbletea"
require "bubbles"
require "ultraviolet"

include Prism::DSL

ROWS = 200

def sales : Prism::DataFrame
  rng = Random.new(7)
  regions = ["north", "south", "east", "west"]

  Prism.from_columns({
    order_id: Arrow::Int64Array.from_values(Array.new(ROWS, &.to_i64)),
    region:   Arrow::StringArray.from_values(Array.new(ROWS) { regions[rng.rand(regions.size)] }),
    price:    Arrow::Float64Array.from_values(Array.new(ROWS) { (rng.next_float * 100.0).round(2) }),
    qty:      Arrow::Int64Array.from_values(Array.new(ROWS) { rng.rand(1..9).to_i64 }),
  })
end

def cell(value : Prism::Value) : String
  case value
  when Nil     then "null"
  when Float64 then value.round(2).to_s
  else              value.to_s
  end
end

# One array of strings per row, straight out of the columns.
def table_rows(frame : Prism::DataFrame) : Array(Bubbles::Table::Row)
  columns = frame.column_names.map { |name| frame[name].to_a }
  Array(Bubbles::Table::Row).new(frame.row_count.to_i) do |row|
    columns.map { |values| cell(values[row]) }
  end
end

def table_columns(frame : Prism::DataFrame) : Array(Bubbles::Table::Column)
  frame.column_names.map do |name|
    width = Math.max(name.size, 12)
    Bubbles::Table::Column.new(title: name, width: width)
  end
end

class Viewer
  include Bubbletea::Model

  @table : Bubbles::Table::Model

  def initialize(frame : Prism::DataFrame)
    @title = "Prism viewer — #{frame.row_count} rows, #{frame.column_names.size} columns"
    @table = Bubbles::Table.new(
      Bubbles::Table.with_columns(table_columns(frame)),
      Bubbles::Table.with_rows(table_rows(frame)),
      Bubbles::Table.with_height(18),
      Bubbles::Table.with_focused(true),
    )
  end

  def init : Bubbletea::Cmd?
    nil
  end

  def update(msg : Tea::Msg)
    case msg
    when Bubbletea::KeyPressMsg
      return {self, Bubbletea.quit} if {"q", "ctrl+c"}.includes?(msg.string)

      @table = @table.update(msg).first
    when Bubbletea::WindowSizeMsg
      @table.set_height(Math.max(msg.height - 6, 3))
    end

    {self, nil}
  end

  def view : Bubbletea::View
    Bubbletea::View.new("#{@title}\n\n#{@table.view}\n\narrows/page scroll · q quits")
  end
end

program = Bubbletea.new_program(Viewer.new(sales), Bubbletea.with_alt_screen)
_model, error = program.run
if error
  STDERR.puts "viewer failed: #{error.message}"
  exit 1
end
