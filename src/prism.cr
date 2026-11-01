# Prism is a DataFrame library for Crystal built on Apache Arrow and Parquet.
#
# Tables are chunked `Arrow::RecordBatch` values with a single `Arrow::Schema`.
# Transformations are inspectable `Prism::Expr` values, and Parquet reads push
# projections and predicates down into `parquet-cr`'s reader.
#
# Consumers require this file as `require "prism"`, which resolves to
# `src/prism.cr` for a shard named `prism`.
require "arrow-cr"
require "parquet-cr"

module Prism
  VERSION = "0.1.0"
end
