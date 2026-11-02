# Quickstart — the workflow from the README, runnable on its own.
#
#     crystal run examples/quickstart.cr
#
# Everything is in memory: build a frame, derive a column from an expression,
# filter, group and join. The `puts` lines that look like assertions are there so
# `spec/examples_spec.cr` can check this file actually works.
require "../src/prism"

include Prism::DSL

orders = Prism.from_columns({
  order_id: Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64, 5_i64]),
  region:   Arrow::StringArray.from_nullable(["east", "west", "east", nil, "west"]),
  price:    Arrow::Float64Array.from_nullable([10.0, 20.0, 30.0, 40.0, nil]),
  qty:      Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64, 5_i64]),
})

puts "rows: #{orders.row_count}, columns: #{orders.column_names.size}"
puts
puts orders
puts

# Expressions are values, so a derived column is inspectable before it runs.
priced = orders.with_column("total", col("price") * col("qty"))
puts "with total"
puts priced.select("order_id", "total")
puts

# `&` binds tighter than `==`, so the parentheses are mandatory (or use `.and`).
east = orders.filter((col("qty") > lit(2)) & (col("region") == lit("east")))
puts "filtered rows: #{east.row_count}"
puts

# One sort per `aggregate`, however many aggregates it holds.
summary = orders
  .group_by("region")
  .aggregate(total: col("qty").sum, orders: col("order_id").count)
  .sort_by("total", order: :desc)

puts "grouped rows: #{summary.row_count}"
puts summary
puts

# A null key never matches, so the row without a region stays unmatched.
regions = Prism.from_columns({
  region:  Arrow::StringArray.from_values(["east", "west"]),
  manager: Arrow::StringArray.from_values(["ann", "bob"]),
})
joined = orders.join(regions, on: "region", how: :left)
puts "joined rows: #{joined.row_count}"
puts joined.select("order_id", "region", "manager")
puts

puts "top region total: #{summary["total"].to_a.first}"
