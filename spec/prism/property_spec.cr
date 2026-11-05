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
  tags = ["alpha", "beta", nil]
  Prism.from_columns({
    id:     Arrow::Int64Array.from_values(Array.new(ROWS, &.to_i64)),
    region: Arrow::StringArray.from_nullable(Array.new(ROWS) { REGIONS[rng.rand(REGIONS.size)] }),
    # A second variable-length key, so a two-key sort reaches the row-encoded
    # path (`SortEngine.strategy` -> :row_encoded) as well as the passes and the
    # comparator chain.
    tag:    Arrow::StringArray.from_nullable(Array.new(ROWS) { tags[rng.rand(tags.size)] }),
    bucket: Arrow::Int64Array.from_values(Array.new(ROWS) { rng.rand(5).to_i64 }),
    amount: Arrow::Float64Array.from_nullable(Array.new(ROWS) { rng.rand(3).zero? ? nil : (rng.next_float * 10).round(2) }),
    qty:    Arrow::Int64Array.from_values(Array.new(ROWS) { rng.rand(1..9).to_i64 }),
  })
end

# One sort key in a reference comparator: column name, direction, and where the
# nulls go — the three things `Prism::SortKey` carries.
private alias RefKey = NamedTuple(column: String, descending: Bool, nulls_first: Bool)

# Compares two materialized values the way a `SortKey` would. Only the dtypes the
# random frame carries are supported; anything else is a test bug.
private def compare_values(left : Prism::Value, right : Prism::Value) : Int32
  case {left, right}
  when {String, String}   then (left <=> right) || 0
  when {Int64, Int64}     then (left <=> right) || 0
  when {Float64, Float64} then (left <=> right) || 0
  else                         0
  end
end

# One key's contribution, including null placement (which is absolute: a
# `nulls_first` key puts nulls first whichever way the values go).
private def compare_key(
  left : Prism::Value,
  right : Prism::Value,
  descending : Bool,
  nulls_first : Bool,
) : Int32
  left_nil = left.nil?
  right_nil = right.nil?
  if left_nil || right_nil
    return 0 if left_nil && right_nil

    return left_nil ? (nulls_first ? -1 : 1) : (nulls_first ? 1 : -1)
  end

  result = compare_values(left, right)
  descending ? -result : result
end

# The `id`s a multi-key sort must produce: a lexicographic comparison over the
# requested keys, with ties broken by input order, which is what Prism promises.
private def reference_order(frame : Prism::DataFrame, keys : Array(RefKey)) : Array(Prism::Value)
  columns = keys.map { |key| frame[key[:column]].to_a }
  ids = frame["id"].to_a
  order = (0...frame.row_count.to_i).to_a
  order.sort! do |left, right|
    result = 0
    keys.each_with_index do |key, position|
      result = compare_key(
        columns[position][left],
        columns[position][right],
        key[:descending],
        key[:nulls_first],
      )
      break unless result == 0
    end
    result == 0 ? left <=> right : result
  end
  order.map { |index| ids[index] }
end

# One reference key as a `Prism::SortKey`.
private def sort_key(key : RefKey) : Prism::SortKey
  expr = Prism.col(key[:column])
  nulls = key[:nulls_first] ? :first : :last
  key[:descending] ? expr.desc(nulls: nulls) : expr.asc(nulls: nulls)
end

# `sort_by` takes a splat, so the arity has to be static; the oracles use at most
# three keys. Returns the frame's `id` column in sorted order.
private def sorted_ids(frame : Prism::DataFrame, keys : Array(RefKey)) : Array(Prism::Value)
  case keys.size
  when 1 then frame.sort_by(sort_key(keys[0]))["id"].to_a
  when 2 then frame.sort_by(sort_key(keys[0]), sort_key(keys[1]))["id"].to_a
  when 3 then frame.sort_by(sort_key(keys[0]), sort_key(keys[1]), sort_key(keys[2]))["id"].to_a
  else        raise ArgumentError.new("oracle supports 1..3 keys, got #{keys.size}")
  end
end

# The first `count` rows of that sort, through the eager bounded verb.
private def top_ids(
  frame : Prism::DataFrame,
  count : Int32,
  keys : Array(RefKey),
) : Array(Prism::Value)
  case keys.size
  when 1 then frame.top_n(count, sort_key(keys[0]))["id"].to_a
  when 2 then frame.top_n(count, sort_key(keys[0]), sort_key(keys[1]))["id"].to_a
  when 3
    frame.top_n(count, sort_key(keys[0]), sort_key(keys[1]), sort_key(keys[2]))["id"].to_a
  else
    raise ArgumentError.new("oracle supports 1..3 keys, got #{keys.size}")
  end
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
    [region, totals[region], mins[region], maxs[region], mean] of Prism::Value
  end
end

# First and last row of each group's `amount` (nulls included, since a group's
# edge row can be null), plus non-null and total counts, in discovery order.
private def reference_group_edges(frame : Prism::DataFrame) : Array(Array(Prism::Value))
  regions = frame["region"].to_a
  amounts = frame["amount"].to_a

  order = [] of Prism::Value
  firsts = Hash(Prism::Value, Prism::Value).new
  lasts = Hash(Prism::Value, Prism::Value).new
  present = Hash(Prism::Value, UInt32).new
  rows = Hash(Prism::Value, UInt32).new

  regions.each_with_index do |region, row|
    unless rows.has_key?(region)
      order << region
      present[region] = 0_u32
      rows[region] = 0_u32
    end

    firsts[region] = amounts[row] unless firsts.has_key?(region)
    lasts[region] = amounts[row]
    present[region] += 1 unless amounts[row].nil?
    rows[region] += 1
  end

  order.map do |region|
    [
      region,
      firsts[region],
      lasts[region],
      present[region].as(Prism::Value),
      rows[region].as(Prism::Value),
    ] of Prism::Value
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
      out << [ids[row], nil] of Prism::Value unless how == :inner
    else
      matches.each { |position| out << [ids[row], labels[position]] of Prism::Value }
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

  it "group_by first/last and counts match a per-key loop" do
    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))
      grouped = frame.group_by("region").aggregate(
        first: col("amount").first,
        last: col("amount").last,
        present: col("amount").count,
        rows: col("amount").count_all,
      )

      rows_of(grouped, ["region", "first", "last", "present", "rows"])
        .should eq(reference_group_edges(frame))
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
        # Recursive aliases need the annotation: an inferred tuple of the
        # expanded union is a different type from `Tuple(Prism::Value, ...)`.
        key = {region.as(Prism::Value), buckets[row].as(Int64).as(Prism::Value)}.as(Tuple(Prism::Value, Prism::Value))
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

  # The three multi-key shapes `Prism::SortEngine.strategy` can pick: row-encoded
  # (two variable-length keys), stable passes (one), and the comparator chain
  # (none). Each is compared against one reference order, so a strategy that
  # disagrees about direction, null placement or tie order fails here.
  it "multi-key sorts match a reference order in every strategy" do
    key_sets = [
      [{column: "region", descending: false, nulls_first: false},
       {column: "tag", descending: true, nulls_first: true}],
      [{column: "region", descending: true, nulls_first: true},
       {column: "bucket", descending: false, nulls_first: false}],
      [{column: "bucket", descending: true, nulls_first: false},
       {column: "qty", descending: false, nulls_first: false}],
      [{column: "region", descending: false, nulls_first: false},
       {column: "tag", descending: false, nulls_first: false},
       {column: "amount", descending: true, nulls_first: true}],
    ]

    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))
      key_sets.each do |keys|
        sorted_ids(frame, keys).should eq(reference_order(frame, keys))
      end
    end
  end

  it "top_n keeps the prefix of the same sort" do
    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))
      keys = [{column: "region", descending: true, nulls_first: false},
              {column: "tag", descending: false, nulls_first: true}] of RefKey
      expected = reference_order(frame, keys)

      [0, 1, 7].each do |count|
        top_ids(frame, count, keys).should eq(expected[0, count])
        top_ids(frame, count, keys)
          .should eq(sorted_ids(frame, keys)[0, count])
      end
    end
  end

  it "is_in matches a Set probe, with and without nulls_equal" do
    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))
      regions = ["east", "north"]

      expected = frame["region"].to_a.map do |value|
        value.nil? ? nil : regions.includes?(value.as(String))
      end

      frame["region"].is_in(regions).to_a.should eq(expected)
      frame.with_column("m", col("region").is_in(regions))["m"].to_a.should eq(expected)

      with_null = frame["region"].to_a.map do |value|
        value.nil? || regions.includes?(value.as(String))
      end
      frame["region"].is_in(regions + [nil], nulls_equal: true).to_a
        .should eq(with_null)
    end
  end

  it "if_else matches a per-row choice" do
    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))
      rows = rows_of(frame, ["qty", "amount"])

      expected = rows.map do |row|
        row[0].as(Int64) > 4 ? row[1] : -1.0
      end

      frame.with_column(
        "chosen",
        Prism.if_else(col("qty") > 4, col("amount"), lit(-1.0)),
      )["chosen"].to_a.should eq(expected)
    end
  end

  it "keeps a declared non-null column free of nulls, eagerly and lazily" do
    REPEATS.times do |seed|
      frame = random_frame(Random.new(seed))

      # Every pipeline that builds a schema from expressions, so the rule that
      # decides `nullable?` is checked against the values it claims about. A
      # cast that would have to invent nulls is left out on purpose: it raises
      # instead (`Coerce.cast_exact`).
      pipelines = {
        "fill_null" => {frame.fill_null(0), frame.lazy.fill_null(0)},
        "cast"      => {frame.cast({"qty" => Arrow::DataType.int32}), frame.lazy.cast({"qty" => Arrow::DataType.int32})},
        "with"      => {frame.with_column("sum", col("qty") + lit(1_i64)), frame.lazy.with_column("sum", col("qty") + lit(1_i64))},
        "select"    => {frame.select(col("id"), col("qty").alias("q")), frame.lazy.select(col("id"), col("qty").alias("q"))},
        "coalesce"  => {
          frame.with_column("c", col("amount").coalesce(lit(0.0))),
          frame.lazy.with_column("c", col("amount").coalesce(lit(0.0))),
        },
        "if_else" => {
          frame.with_column("c", Prism.if_else(col("qty") > 4, col("qty"), col("qty"))),
          frame.lazy.with_column("c", Prism.if_else(col("qty") > 4, col("qty"), col("qty"))),
        },
      }

      pipelines.each do |label, (eager, lazy)|
        collected = lazy.collect
        collected.schema.should eq(eager.schema), "#{label}: the plan and the verb must agree"

        eager.schema.fields.each do |field|
          next if field.nullable?

          eager[field.name].null_count.should eq(0), "#{label}: #{field.name} is declared non-null"
          collected[field.name].null_count.should eq(0), "#{label}: #{field.name} stays non-null"
        end
      end
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
