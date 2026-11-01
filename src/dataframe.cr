# Prism is a DataFrame library for Crystal built on Apache Arrow and Parquet.
#
# Tables are chunked `Arrow::RecordBatch` values with a single `Arrow::Schema`.
# Transformations are inspectable `Prism::Expr` values, and Parquet reads push
# projections and predicates down into `parquet-cr`'s reader.
#
# The shard name and this file name (`dataframe`) are unchanged until the
# rename phase so that `require "dataframe"` keeps resolving. The module is
# already `Prism`; see plans/plan.md (Phase 10).
require "arrow-cr"
require "parquet-cr"

module Prism
  VERSION = "0.1.0"
end
