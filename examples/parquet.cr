# Parquet round trip, projection and predicate pushdown, and a lazy plan.
#
#     crystal run examples/parquet.cr
#
# Writes under `temp/` (gitignored) so the example needs no fixture files.
# The `rows read:` / `projection:` lines feed `spec/examples_spec.cr`.
require "../src/prism"

include Prism::DSL

path = "temp/examples/sales.parquet"
Dir.mkdir_p(File.dirname(path))

orders = Prism.from_columns({
  region: Arrow::StringArray.from_nullable(["east", "west", "east", nil, "west"]),
  price:  Arrow::Float64Array.from_nullable([10.0, 20.0, 30.0, 40.0, nil]),
  qty:    Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64, 5_i64]),
})

# Row groups of two, so the reader has several to prune; zstd is the default.
orders.write_parquet(path, row_group_size: 2, compression: :zstd)
puts "wrote #{File.size(path)} bytes to #{path}"
puts

full = Prism.read_parquet(path)
puts "rows read: #{full.row_count}"
puts

# Only the named columns are decoded — projection pushdown.
projected = Prism.read_parquet(path, columns: ["region", "price"])
puts "projected columns: #{projected.column_names.join(", ")}"
puts

# The predicate becomes a RowFilter inside the reader.
filtered = Prism.read_parquet(path, predicate: col("qty") > lit(2))
puts "predicate rows: #{filtered.row_count}"
puts

# The same work, deferred: `explain` shows what reached the scan. The predicate
# column `qty` is read even though the projection only keeps `region`.
puts Prism.scan_parquet(path)
  .filter(col("qty") > lit(2))
  .select("region")
  .explain
