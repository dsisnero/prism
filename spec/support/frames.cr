require "../spec_helper"

# Scratch path under the gitignored ./temp directory.
module SpecPaths
  def self.temp(name : String) : String
    Dir.mkdir_p("temp")
    File.join("temp", name)
  end
end

# Shared sample frames for IO round-trips.
module SpecFrames
  def self.sales : Prism::DataFrame
    Prism.from_columns({
      region: Arrow::StringArray.from_nullable(["east", "west", "east", nil, "west"]),
      price:  Arrow::Float64Array.from_nullable([10.0, 20.0, 30.0, 40.0, nil]),
      qty:    Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64, 4_i64, 5_i64]),
    })
  end

  def self.empty_sales : Prism::DataFrame
    sales.filter(Prism.col("qty") > Prism.lit(100_i64))
  end
end
