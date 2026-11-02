require "../spec_helper"

include Prism::DSL

private def lazy_parquet_path : String
  path = SpecPaths.temp("lazy_sales.parquet")
  SpecFrames.sales.write_parquet(path, row_group_size: 2)
  path
end

# Two parquet files whose columns collide on `price`, so the join suffixes the
# right one to `price_right`.
private def lazy_join_paths : Tuple(String, String)
  left = SpecPaths.temp("lazy_join_left.parquet")
  SpecFrames.sales.write_parquet(left, row_group_size: 2)

  right = SpecPaths.temp("lazy_join_right.parquet")
  Prism.from_columns({
    region: Arrow::StringArray.from_values(["east", "west"]),
    price:  Arrow::Float64Array.from_values([1.5, 2.5]),
    weight: Arrow::Float64Array.from_values([1.0, 2.0]),
  }).write_parquet(right)

  {left, right}
end

describe Prism::LazyFrame do
  describe "#collect" do
    it "matches the eager filter + select pipeline" do
      lazy = SpecFrames.sales.lazy
        .filter(col("qty") > lit(2_i64))
        .select("region", "price")
      eager = SpecFrames.sales
        .filter(col("qty") > lit(2_i64))
        .select("region", "price")

      lazy.schema.should eq(eager.schema)
      lazy.collect.should eq(eager)
    end

    it "matches the eager group_by pipeline" do
      lazy = SpecFrames.sales.lazy
        .group_by("region")
        .aggregate(total: col("qty").sum)
      eager = SpecFrames.sales
        .group_by("region")
        .aggregate(total: col("qty").sum)

      lazy.collect["total"].to_a.should eq(eager["total"].to_a)
    end

    it "matches the eager sort + limit pipeline" do
      lazy = SpecFrames.sales.lazy.sort_by("qty", order: :desc).head(2)
      eager = SpecFrames.sales.sort_by("qty", order: :desc).head(2)

      lazy.collect["qty"].to_a.should eq(eager["qty"].to_a)
    end

    it "matches the eager distinct pipeline" do
      lazy = SpecFrames.sales.lazy.distinct("region")
      eager = SpecFrames.sales.distinct("region")

      lazy.collect["region"].to_a.should eq(eager["region"].to_a)
    end

    it "matches the eager join pipeline" do
      other = Prism.from_columns({
        region: Arrow::StringArray.from_values(["east", "west"]),
        weight: Arrow::Float64Array.from_values([1.0, 2.0]),
      })

      lazy = SpecFrames.sales.lazy.join(other.lazy, on: "region", how: :left)
      eager = SpecFrames.sales.join(other, on: "region", how: :left)

      lazy.collect.column_names.should eq(eager.column_names)
      lazy.collect["weight"].to_a.should eq(eager["weight"].to_a)
    end

    it "reads a parquet scan with the same rows as the eager reader" do
      path = lazy_parquet_path
      lazy = Prism.scan_parquet(path)
      eager = Prism.read_parquet(path)

      lazy.collect.should eq(eager)
    end
  end

  describe "#explain" do
    it "shows the unoptimized plan" do
      plan = SpecFrames.sales.lazy
        .filter(col("qty") > lit(2_i64))
        .explain(optimized: false)

      plan.should contain("Filter")
      plan.should contain("ScanBatches")
    end

    it "pushes a predicate into a parquet scan" do
      plan = Prism.scan_parquet(lazy_parquet_path)
        .filter(col("qty") > lit(2_i64))
        .select("region")
        .explain

      plan.should contain("ScanParquet")
      plan.should contain("predicate: gt(col:qty,lit:2)")
      # `qty` is read only to evaluate the predicate.
      plan.should contain("projection: [region, qty]")
    end

    it "pushes a limit into the scan" do
      plan = Prism.scan_parquet(lazy_parquet_path).head(3).explain

      plan.should contain("limit: 3")
    end

    it "pins a limit to the sort instead of pushing it into the scan" do
      plan = Prism.scan_parquet(lazy_parquet_path).sort_by("qty").head(3).explain

      plan.should contain("slice: offset=0 count=3")
      # No separate node above the sort, and nothing reached the scan.
      plan.should_not contain("Limit")
      plan.includes?("limit:").should be_false
    end

    it "pins a limit to a group_by result" do
      plan = Prism.scan_parquet(lazy_parquet_path)
        .group_by("region")
        .aggregate(total: col("qty").sum)
        .head(2)
        .explain

      plan.should contain("slice: offset=0 count=2")
      plan.should_not contain("Limit")
    end

    it "pins a limit to a distinct node" do
      plan = Prism.scan_parquet(lazy_parquet_path).distinct("region").head(1).explain

      plan.should contain("slice: offset=0 count=1")
      plan.should_not contain("Limit")
    end

    it "composes nested limits onto one node" do
      plan = Prism.scan_parquet(lazy_parquet_path)
        .sort_by("qty")
        .slice(1, 4)
        .head(2)
        .explain

      plan.scan(/slice:/).size.should eq(1)
      plan.should contain("slice: offset=1 count=2")
    end

    it "does not pin a limit across a filter" do
      plan = Prism.scan_parquet(lazy_parquet_path)
        .sort_by("qty")
        .filter(col("qty") > lit(1_i64))
        .head(2)
        .explain

      plan.should contain("Limit")
      plan.includes?("slice:").should be_false
    end

    it "splits conjunctions so each half can be pushed" do
      plan = Prism.scan_parquet(lazy_parquet_path)
        .filter((col("qty") > lit(2_i64)) & (col("region") == lit("east")))
        .explain

      plan.should contain("predicate: gt(col:qty,lit:2)")
      plan.should contain("predicate: eq(col:region,lit:\"east\")")
    end

    it "keeps a filter above a computed column" do
      plan = Prism.scan_parquet(lazy_parquet_path)
        .with_column("doubled", col("qty") * lit(2))
        .filter(col("doubled") > lit(4))
        .explain

      plan.should contain("Filter")
      plan.includes?("predicate:").should be_false
    end

    it "prunes a column the plan never reads" do
      plan = Prism.scan_parquet(lazy_parquet_path).select("region").explain

      plan.should contain("projection: [region]")
    end

    it "prunes both inputs of a join down to what the output needs" do
      left, right = lazy_join_paths
      plan = Prism.scan_parquet(left)
        .join(Prism.scan_parquet(right), on: "region", how: :left)
        .select("price", "weight")
        .explain

      # `qty` exists on the left and is read by nothing.
      plan.should contain("projection: [region, price]")
      plan.should contain("projection: [region, weight]")
      plan.includes?("qty").should be_false
    end

    it "reads only the keys from the right side of a semi join" do
      left, right = lazy_join_paths
      plan = Prism.scan_parquet(left)
        .join(Prism.scan_parquet(right), on: "region", how: :semi)
        .select("qty")
        .explain

      plan.should contain("projection: [region, qty]")
      plan.should contain("projection: [region]")
    end

    it "keeps the shadowed left column so a suffixed right name survives" do
      left, right = lazy_join_paths
      plan = Prism.scan_parquet(left)
        .join(Prism.scan_parquet(right), on: "region", how: :inner)
        .select("price_right")
        .explain

      # Without the left `price` the right one would lose its suffix, so both
      # inputs carry `region` and `price` — and `weight` is never read.
      plan.scan(/projection: \[region, price\]/).size.should eq(2)
    end

    it "leaves the inputs alone when the whole join output is required" do
      left, right = lazy_join_paths
      plan = Prism.scan_parquet(left)
        .join(Prism.scan_parquet(right), on: "region")
        .explain

      plan.includes?("projection:").should be_false
    end

    it "prunes a join whose keys are named differently on each side" do
      left, _ = lazy_join_paths
      other = SpecPaths.temp("lazy_join_uid.parquet")
      Prism.from_columns({
        uid:    Arrow::StringArray.from_values(["east", "west"]),
        weight: Arrow::Float64Array.from_values([1.0, 2.0]),
        note:   Arrow::StringArray.from_values(["a", "b"]),
      }).write_parquet(other)

      plan = Prism.scan_parquet(left)
        .join(Prism.scan_parquet(other), left_on: "region", right_on: "uid", how: :left)
        .select("qty", "weight")
        .explain

      plan.should contain("projection: [region, qty]")
      plan.should contain("projection: [uid, weight]")
    end
  end

  describe "results after optimization" do
    it "returns the same rows the eager pipeline does when the filter is pushed" do
      path = lazy_parquet_path
      lazy = Prism.scan_parquet(path)
        .filter((col("qty") > lit(2_i64)) & (col("region") == lit("east")))
        .select("region", "price")
      eager = SpecFrames.sales
        .filter((col("qty") > lit(2_i64)) & (col("region") == lit("east")))
        .select("region", "price")

      lazy.collect.should eq(eager)
    end

    it "returns the same rows when a limit is pushed" do
      path = lazy_parquet_path
      lazy = Prism.scan_parquet(path).head(2)
      eager = Prism.read_parquet(path).head(2)

      lazy.collect["qty"].to_a.should eq(eager["qty"].to_a)
    end

    it "returns the same rows as an eager join after pruning" do
      left, right = lazy_join_paths
      lazy = Prism.scan_parquet(left)
        .join(Prism.scan_parquet(right), on: "region", how: :left)
        .select("region", "price", "weight")
      eager = Prism.read_parquet(left)
        .join(Prism.read_parquet(right), on: "region", how: :left)
        .select("region", "price", "weight")

      lazy.collect.should eq(eager)
    end

    it "keeps a suffixed right column readable after pruning" do
      left, right = lazy_join_paths
      lazy = Prism.scan_parquet(left)
        .join(Prism.scan_parquet(right), on: "region", how: :inner)
        .select("region", "price", "price_right")
      eager = Prism.read_parquet(left)
        .join(Prism.read_parquet(right), on: "region", how: :inner)
        .select("region", "price", "price_right")

      lazy.collect.should eq(eager)
    end

    it "keeps a filtered right column readable after pruning" do
      left, right = lazy_join_paths
      lazy = Prism.scan_parquet(left)
        .join(Prism.scan_parquet(right), on: "region", how: :left)
        .filter(col("weight") > lit(1.5))
        .select("price")
      eager = Prism.read_parquet(left)
        .join(Prism.read_parquet(right), on: "region", how: :left)
        .filter(col("weight") > lit(1.5))
        .select("price")

      lazy.collect.should eq(eager)
    end

    it "returns the same rows for a non-coalesced full join after pruning" do
      left, right = lazy_join_paths
      lazy = Prism.scan_parquet(left)
        .join(Prism.scan_parquet(right), on: "region", how: :full, coalesce: false)
        .select("region", "region_right", "weight")
      eager = Prism.read_parquet(left)
        .join(Prism.read_parquet(right), on: "region", how: :full, coalesce: false)
        .select("region", "region_right", "weight")

      lazy.collect.should eq(eager)
    end

    it "treats the :outer alias as a non-coalesced full join while pruning" do
      left, right = lazy_join_paths
      lazy = Prism.scan_parquet(left)
        .join(Prism.scan_parquet(right), on: "region", how: :outer)
        .select("region_right", "weight")
      eager = Prism.read_parquet(left)
        .join(Prism.read_parquet(right), on: "region", how: :outer)
        .select("region_right", "weight")

      lazy.collect.should eq(eager)
      lazy.explain.should contain("projection: [region, weight]")
    end

    it "returns the same rows when a slice is pinned to the sort" do
      path = lazy_parquet_path
      lazy = Prism.scan_parquet(path).sort_by("qty", order: :desc).slice(1, 2)
      eager = Prism.read_parquet(path).sort_by("qty", order: :desc).slice(1, 2)

      lazy.collect["qty"].to_a.should eq(eager["qty"].to_a)
    end

    it "returns the same rows when a slice is pinned to a group_by" do
      path = lazy_parquet_path
      lazy = Prism.scan_parquet(path)
        .group_by("region")
        .aggregate(total: col("qty").sum)
        .head(1)
      eager = Prism.read_parquet(path)
        .group_by("region")
        .aggregate(total: col("qty").sum)
        .head(1)

      lazy.collect["total"].to_a.should eq(eager["total"].to_a)
    end

    it "returns the same rows when a slice is pinned to distinct" do
      path = lazy_parquet_path
      lazy = Prism.scan_parquet(path).distinct("region").head(1)
      eager = Prism.read_parquet(path).distinct("region").head(1)

      lazy.collect["region"].to_a.should eq(eager["region"].to_a)
    end

    it "clamps a pinned slice that starts past the end" do
      path = lazy_parquet_path
      lazy = Prism.scan_parquet(path).sort_by("qty").head(5).slice(10, 2)
      eager = Prism.read_parquet(path).sort_by("qty").head(5).slice(10, 2)

      lazy.collect.row_count.should eq(0)
      eager.row_count.should eq(0)
    end
  end
end
