module Prism
  # Base class for every error raised by this library.
  #
  # Errors from `arrow-cr` (`Arrow::ArrowError`) and `parquet-cr`
  # (`Parquet::ParquetError`) are deliberately not wrapped: they carry precise
  # context and callers may want to match them. `Prism::Error` is for
  # dataframe-level misuse only.
  class Error < Exception
  end

  # Raised when a column name does not exist in a frame's schema.
  class ColumnNotFound < Error
    getter column : String

    def initialize(@column : String, available : Enumerable(String)? = nil)
      if names = available
        super("column #{@column.inspect} not found (available: #{names.join(", ")})")
      else
        super("column #{@column.inspect} not found")
      end
    end
  end

  # Raised when a column name would appear twice in a frame.
  class DuplicateColumn < Error
    def initialize(column : String, reason : String? = nil)
      super(reason || "column #{column.inspect} already exists")
    end
  end

  # Raised when batches in one frame do not share a schema, or when a supplied
  # schema disagrees with the data.
  class SchemaError < Error
  end

  # Raises `DuplicateColumn` for the first repeated name.
  #
  # Every verb that builds a schema from names calls this, because a frame with
  # two columns of one name loses one of them silently: `df[name]` finds the
  # first, while `to_s`, `to_arrow` and a Parquet write all succeed. `context`
  # names the verb, so the message says what produced the collision.
  def self.ensure_unique_names!(names : Enumerable(String), context : String) : Nil
    seen = Set(String).new
    names.each do |name|
      next if seen.add?(name)

      raise DuplicateColumn.new(
        name,
        "column #{name.inspect} appears twice in #{context}",
      )
    end
  end

  # Raised when an expression is selected without a name to give its column.
  class MissingAlias < Error
    def initialize(expression : String)
      super("expression #{expression} needs an alias to be selected; use .alias(\"name\")")
    end
  end

  # Raised when a group_by aggregation column is not an aggregation.
  class InvalidAggregation < Error
    def initialize(column : String, expression : String)
      super(
        "column #{column.inspect} is not an aggregation (#{expression}); " \
        "use .sum, .mean, .count, .min, .max, and friends"
      )
    end
  end

  # Raised when an operation cannot apply to the given Arrow data type.
  class DtypeError < Error
  end

  # Raised when an operation is not implemented for the given type or shape.
  class UnsupportedOperation < Error
  end
end
