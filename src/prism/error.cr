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
    def initialize(column : String)
      super("column #{column.inspect} already exists")
    end
  end

  # Raised when batches in one frame do not share a schema, or when a supplied
  # schema disagrees with the data.
  class SchemaError < Error
  end

  # Raised when an expression is selected without a name to give its column.
  class MissingAlias < Error
    def initialize(expression : String)
      super("expression #{expression} needs an alias to be selected; use .alias(\"name\")")
    end
  end

  # Raised when an operation cannot apply to the given Arrow data type.
  class DtypeError < Error
  end

  # Raised when an operation is not implemented for the given type or shape.
  class UnsupportedOperation < Error
  end
end
