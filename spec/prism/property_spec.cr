require "../spec_helper"

include Prism::DSL

# Oracles, not fuzzers: seeded, small, and deterministic. Each scenario compares
# a verb against a hand-written reference over the same rows, so a regression in
# a kernel, a verb, the optimizer or a key encoding fails here even when no
# targeted spec covers that combination of nulls, keys and dtypes.
private ROWS    = 120
private REPEATS =   6
private REGIONS = ["east", "west", "north", nil]

private def random_frame(rng : Random) : Prism::DataFrame
  Prism.from_columns({
    id:     Arrow::Int64Array.from_values(Array.new(ROWS, &.to_i64)),
    region: Arrow::StringArray.from_nullable(Array.new(ROWS) { REGIONS[rng.rand(REGIONS.size)] }),
    bucket: Arrow::Int64Array.from_values(Array.new(ROWS) { rng.rand(5).to_i64 }),
    amount: Arrow::Float64Array.from_nullable(Array.new(ROWS) { rng.rand(3).zero? ? nil : (rng.next_float * 10).round(2) }),
    qty:    Arrow::Int64Array.from_values(Array.new(ROWS) { rng.rand(1..9).to_i64 }),
  })
end

# Which right rows each bucket has, in right order.
private def lookup_frame(rng : Random) : Prism::DataFrame
  Prism.from_columns({
    bucket: Arrow::Int64Array.from_values(Array.new(10) { |row| (row % 5).to_i64 }),
    label:  Arrow::StringArray.from_nullable(Array.new(10) { |row| rng.rand(4).zero? ? nil : "l#{row}" }),
  })
end

private def rows_of(frame : Prism::DataFrame, names : Array(String)) : Array(Array(Prism::Value))
  columns = names.map { |name| frame[name].to_a }
  Array(Array(Prism::Value)).new(frame.row_count.to_i) do |row|
    columns.map { |values| values[row] }
  end
end

# --- reference implementations -------------------------------------------

# First occurrence of each key, in input order; null is a value of its own.
# `keys` decide identity, `names` decide what the result carries.
private def reference_distinct(
  frame : Prism::DataFrame,
  keys : Array(String),
  names : Array(String),
) : Array(Array(Prism::Value))
  rows = rows_of(frame, names)
  key_rows = rows_of(frame, keys)
  seen = Set(Array(Prism::Value)).new

  kept = [] of Array(Prism::Value)
  rows.each_with_index do |row, index|
    kept << row if seen.add?(key_rows[index])
  end
  kept
end

# Grouped sums/min/max/mean over `amount` (nullable) and `qty` (never null), in
# discovery order.
private def reference_group_by(frame : Prism::DataFrame) : Array(Array(Prism::Value))
  regions = frame["region"].to_a
  qty = frame["qty"].to_a
  amounts = frame["amount"].to_a

  order = [] of Prism::Value
  totals = Hash(Prism::Value, Int64).new
  amount_sums = Hash(Prism::Value, Float64).new
  amount_counts = Hash(Prism::Value, Int64).new
  mins = Hash(Prism::Value, Float64?).new
  maxs = Hash(Prism::Value, Float64?).new

  regions.each_with_index do |region, row|
    unless totals.has_key?(region)
      order << region
      totals[region] = 0_i64
      amount_sums[region] = 0.0
      amount_counts[region] = 0_i64
      mins[region] = nil
      maxs[region] = nil
    end

    totals[region] += qty[row].as(Int64)
    amount = amounts[row].as(Float64?)
    next unless amount

    amount_sums[region] += amount
    amount_counts[region] += 1
    mins[region] = amount if mins[region].nil? || amount < mins[region].not_nil!
    maxs[region] = amount if maxs[region].nil? || amount > maxs[region].not_nil!
  end

  order.map do |region|
    count = amount_counts[region]
    mean = count.zero? ? nil : amount_sums[region] / count.to_f64
    [region, totals[region], mins[region], maxs[region], mean]
  end
end

# Nested-loop join on `bucket`; null keys never match.
private def reference_join(
  left : Prism::DataFrame,
  right : Prism::DataFrame,
  how : Symbol,
) : Array(Array(Prism::Value))
  ids = left["id"].to_a
  left_keys = left["bucket"].to_a
  right_keys = right["bucket"].to_a
  labels = right["label"].to_a

  out = [] of Array(Prism::Value)
  left_keys.each_with_index do |key, row|
    matches = [] of Int32
    right_keys.each_with_index do |other, position|
      matches << position if key && other && key == other
    end

    if matches.empty?
      out << [ids[row], nil] unless how == :inner
    else
      matches.each { |position| out << [ids[row], labels[position]] }
    end
  end

  out
end

# The canonical key rule for floats, as a comparable string: `-0.0` is `0.0` and
# every NaN is one key.
private def canonical_key(value : Prism::Value) : String
  return "null" if value.nil?

  float = value.as(Float64)
  return "nan" if float.nan?
  return "0.0" if float.zero?

  float.to_s
end

describe "property checks" do
  it "distinct matches first-occurrence over a set of rows" do
    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))
      distinct = frame.distinct("region", "bucket")

      rows_of(distinct, ["id", "region", "bucket"])
        .should eq(reference_distinct(frame, ["region", "bucket"], ["id", "region", "bucket"]))
    end
  end

  it "group_by matches a per-key loop" do
    REPEATS.times do |seed|
      grouped = random_frame(Random.new(seed))
        .group_by("region")
        .aggregate(
          total: col("qty").sum,
          smallest: col("amount").min,
          largest: col("amount").max,
          average: col("amount").mean,
        )

      rows_of(grouped, ["region", "total", "smallest", "largest", "average"])
        .should eq(reference_group_by(random_frame(Random.new(seed))))
    end
  end

  it "group_by on two keys matches a per-key loop" do
    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))
      grouped = frame.group_by("region", "bucket").aggregate(total: col("qty").sum)

      regions = frame["region"].to_a
      buckets = frame["bucket"].to_a
      qty = frame["qty"].to_a

      order = [] of Tuple(Prism::Value, Prism::Value)
      totals = Hash(Tuple(Prism::Value, Prism::Value), Int64).new
      regions.each_with_index do |region, row|
        key = {region, buckets[row].as(Int64)}
        unless totals.has_key?(key)
          order << key
          totals[key] = 0_i64
        end
        totals[key] += qty[row].as(Int64)
      end

      expected = order.map { |key| [key[0], key[1], totals[key]] of Prism::Value }
      rows_of(grouped, ["region", "bucket", "total"]).should eq(expected)
    end
  end

  it "join matches a nested loop" do
    REPEATS.times do |seed|
      rng = Random.new(seed)
      left = random_frame(rng)
      right = lookup_frame(rng)

      [:inner, :left].each do |how|
        joined = left.join(right, on: "bucket", how: how)
        rows_of(joined, ["id", "label"]).should eq(reference_join(left, right, how))
      end
    end
  end

  it "semi and anti joins are membership filters" do
    REPEATS.times do |seed|
      rng = Random.new(seed)
      left = random_frame(rng)
      right = lookup_frame(rng)

      matched = reference_join(left, right, :inner).map(&.[0]).to_set
      ids = rows_of(left, ["id"])

      rows_of(left.join(right, on: "bucket", how: :semi), ["id"])
        .should eq(ids.select { |row| matched.includes?(row[0]) })
      rows_of(left.join(right, on: "bucket", how: :anti), ["id"])
        .should eq(ids.reject { |row| matched.includes?(row[0]) })
    end
  end

  it "joins float keys with ==" do
    left_values = [0.0, -0.0, Float64::NAN, nil, 1.5] of Float64?
    right_values = [-0.0, Float64::NAN, 1.5, 2.5, nil] of Float64?

    REPEATS.times do |seed|
      rng = Random.new(seed)
      left = Prism.from_columns({
        id: Arrow::Int64Array.from_values(Array.new(12, &.to_i64)),
        f:  Arrow::Float64Array.from_nullable(Array.new(12) { left_values.sample(rng) }),
      })
      right = Prism.from_columns({
        f:     Arrow::Float64Array.from_nullable(Array.new(12) { right_values.sample(rng) }),
        label: Arrow::StringArray.from_values(Array.new(12) { |row| "r#{row}" }),
      })

      ids = left["id"].to_a
      left_keys = left["f"].to_a
      right_keys = right["f"].to_a
      labels = right["label"].to_a

      expected = [] of Array(Prism::Value)
      left_keys.each_with_index do |value, row|
        next if value.nil?

        key = canonical_key(value)
        right_keys.each_with_index do |other, position|
          next if other.nil?
          next unless canonical_key(other) == key

          expected << [ids[row], labels[position]] of Prism::Value
        end
      end

      rows_of(left.join(right, on: "f"), ["id", "label"]).should eq(expected)
    end
  end

  it "sorts by a nullable float with nulls last" do
    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))
      rows = rows_of(frame, ["id", "amount"])

      expected = rows.sort_by do |row|
        amount = row[1].as(Float64?)
        {amount.nil? ? 1 : 0, amount.nil? ? 0.0 : -amount.not_nil!, row[0].as(Int64)}
      end

      rows_of(frame.sort_by("amount", order: :desc), ["id", "amount"]).should eq(expected)
    end
  end

  it "lazy pipelines match the eager ones" do
    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))

      lazy = frame.lazy
        .filter((col("qty") > lit(3)) & (col("region") == lit("east")))
        .select("id", "amount")
        .sort_by("id")
        .head(10)
        .collect
      eager = frame
        .filter((col("qty") > lit(3)) & (col("region") == lit("east")))
        .select("id", "amount")
        .sort_by("id")
        .head(10)

      rows_of(lazy, ["id", "amount"]).should eq(rows_of(eager, ["id", "amount"]))
    end
  end

  it "lazy distinct, group_by and join match the eager ones" do
    REPEATS.times do |seed|
      rng = Random.new(seed)
      left = random_frame(rng)
      right = lookup_frame(rng)

      rows_of(left.lazy.distinct("region").collect, ["id"])
        .should eq(rows_of(left.distinct("region"), ["id"]))

      rows_of(left.lazy.group_by("region").aggregate(n: col("qty").sum).collect, ["region", "n"])
        .should eq(rows_of(left.group_by("region").aggregate(n: col("qty").sum), ["region", "n"]))

      rows_of(left.lazy.join(right.lazy, on: "bucket").collect, ["id", "label"])
        .should eq(rows_of(left.join(right, on: "bucket"), ["id", "label"]))
    end
  end

  it "gives the same answer for chunked and single-batch frames" do
    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))
      batch = frame.to_arrow
      third = ROWS // 3
      chunked = Prism.from_batches([
        batch.slice(0, third),
        batch.slice(third, third),
        batch.slice(2 * third, ROWS - 2 * third),
      ])

      rows_of(chunked.distinct("region", "bucket"), ["id"])
        .should eq(rows_of(frame.distinct("region", "bucket"), ["id"]))

      rows_of(chunked.sort_by("amount", order: :desc), ["id", "amount"])
        .should eq(rows_of(frame.sort_by("amount", order: :desc), ["id", "amount"]))

      rows_of(chunked.group_by("region").aggregate(total: col("qty").sum), ["region", "total"])
        .should eq(rows_of(frame.group_by("region").aggregate(total: col("qty").sum), ["region", "total"]))
    end
  end

  it "keys floats by == rather than by total order" do
    nan = Float64::NAN
    quiet_nan = 0x7FF8_0000_0000_0001_u64.unsafe_as(Float64)
    values = [0.0, -0.0, nan, quiet_nan, 1.5, 1.5, nil, -0.0] of Float64?

    REPEATS.times do |seed|
      rng = Random.new(seed)
      column = Arrow::Float64Array.from_nullable(values.sample(values.size, rng))
      frame = Prism.from_columns({f: column})

      keys = frame["f"].to_a.map { |value| canonical_key(value) }
      expected = keys.uniq

      frame["f"].n_unique.should eq(expected.size)
      frame.distinct.row_count.should eq(expected.size)
    end
  end
end
