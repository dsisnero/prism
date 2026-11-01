require "../spec_helper"

include Prism::DSL

private def lazy_parquet_path : String
  path = SpecPaths.temp("lazy_sales.parquet")
  SpecFrames.sales.write_parquet(path, row_group_size: 2)
  path
end

describe Prism::LazyFrame do
  describe "#collect" do
    it "matches the eager filter + select pipeline" do
      lazy = Prism.scan(SpecFrames.sales)
        .filter(col("qty") > lit(2_i64))
        .select("region", "price")
      eager = SpecFrames.sales
        .filter(col("qty") > lit(2_i64))
        .select("region", "price")

      lazy.schema.should eq(eager.schema)
      lazy.collect.should eq(eager)
    end

    it "matches the eager group_by pipeline" do
      lazy = Prism.scan(SpecFrames.sales)
        .group_by("region")
        .aggregate(total: col("qty").sum)
      eager = SpecFrames.sales
        .group_by("region")
        .aggregate(total: col("qty").sum)

      lazy.collect["total"].to_a.should eq(eager["total"].to_a)
    end

    it "matches the eager sort + limit pipeline" do
      lazy = Prism.scan(SpecFrames.sales).sort_by("qty", order: :desc).head(2)
      eager = SpecFrames.sales.sort_by("qty", order: :desc).head(2)

      lazy.collect["qty"].to_a.should eq(eager["qty"].to_a)
    end

    it "matches the eager distinct pipeline" do
      lazy = Prism.scan(SpecFrames.sales).distinct("region")
      eager = SpecFrames.sales.distinct("region")

      lazy.collect["region"].to_a.should eq(eager["region"].to_a)
    end

    it "matches the eager join pipeline" do
      other = Prism.from_columns({
        region: Arrow::StringArray.from_values(["east", "west"]),
        weight: Arrow::Float64Array.from_values([1.0, 2.0]),
      })

      lazy = Prism.scan(SpecFrames.sales).join(Prism.scan(other), on: "region", how: :left)
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
      plan = Prism.scan(SpecFrames.sales)
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

    it "does not push a limit below a sort" do
      plan = Prism.scan_parquet(lazy_parquet_path).sort_by("qty").head(3).explain

      plan.should contain("Limit")
      plan.should_not contain("limit: 3\n      projection")
      plan.includes?("limit: 3").should be_false
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
  end
end
