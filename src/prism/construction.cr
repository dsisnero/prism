module Prism
  # Wraps an existing Arrow batch as a frame without copying.
  def self.from_arrow(batch : Arrow::RecordBatch) : DataFrame
    DataFrame.new(batch.schema, [batch])
  end

  # Wraps existing Arrow batches, keeping their chunking.
  #
  # `schema` is only required when `batches` is empty; it lets an empty frame
  # still answer `#column_names` and `#dtypes`. Raises `Prism::SchemaError` when
  # the batches disagree with each other or with `schema`.
  def self.from_batches(
    batches : Enumerable(Arrow::RecordBatch),
    *,
    schema : Arrow::Schema? = nil,
  ) : DataFrame
    list = batches.to_a
    if list.empty?
      DataFrame.new(schema || Arrow::Schema.empty, [] of Arrow::RecordBatch)
    else
      DataFrame.new(schema || list.first.schema, list)
    end
  end

  # Builds a frame from a Hash of named Arrow arrays.
  def self.from_columns(
    columns : Hash(String | Symbol, Arrow::ArrayBase),
    *,
    row_count : Int32? = nil,
  ) : DataFrame
    from_arrow(Arrow::RecordBatch.from_columns(columns, row_count: row_count))
  end

  # Builds a frame from a NamedTuple of Arrow arrays.
  def self.from_columns(
    columns : NamedTuple,
    *,
    row_count : Int32? = nil,
  ) : DataFrame
    from_arrow(Arrow::RecordBatch.from_columns(columns, row_count: row_count))
  end
end
