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

require "./prism/error"
require "./prism/values"
require "./prism/series"
require "./prism/dataframe"
require "./prism/construction"

require "./prism/expression/expr"
require "./prism/expression/literals"
require "./prism/expression/coerce"
require "./prism/expression/type_cache"
require "./prism/expression/cse"
require "./prism/expression/kernels"
require "./prism/expression/nodes"
require "./prism/expression/dsl"
require "./prism/expression/operators"

require "./prism/row_keys"
require "./prism/membership"
require "./prism/aggregates"
require "./prism/expression/aggregations"
require "./prism/grouped_agg"

require "./prism/sort_engine"

require "./prism/verbs/filter"
require "./prism/verbs/select"
require "./prism/verbs/mutate"
require "./prism/verbs/sort"
require "./prism/verbs/distinct"

require "./prism/group_by"

require "./prism/combine"
require "./prism/join"

require "./prism/io/parquet"
require "./prism/io/csv"
require "./prism/io/ipc"

require "./prism/plan/node"
require "./prism/plan/scans"
require "./prism/plan/transform"
require "./prism/plan/aggregate"
require "./prism/plan/join"
require "./prism/plan/optimizer"

require "./prism/lazy_frame"

require "./prism/typed"

require "./prism/display"

module Prism
  VERSION = "0.2.0"
end
