require "./spec_helper"

# Phase 0 acceptance: the shard loads, and the Arrow/Parquet surface it builds
# on actually works from this project's dependency graph. These specs guard the
# dependency contract described in plans/plan.md §3.
describe Prism do
  # The number itself is not repeated here: `spec/prism/version_spec.cr` owns the
  # agreement with `shard.yml`, so a release bumps one place. A literal here was a
  # third copy that could silently drift.
  it "exposes a version" do
    Prism::VERSION.should_not be_empty
    Prism::VERSION.should match(/^\d+\.\d+\.\d+/)
  end
end

describe "dependency contract" do
  it "builds an Arrow record batch from named columns" do
    batch = Arrow::RecordBatch.from_columns({
      id:    Arrow::Int64Array.from_values([1_i64, 2_i64, 3_i64]),
      price: Arrow::Float64Array.from_nullable([10.0, nil, 30.0]),
    })

    batch.num_rows.should eq(3)
    batch.column_names.should eq(["id", "price"])
    # `value?` is bounds-only; `[]?` is the null-aware accessor.
    batch["price"].as(Arrow::Float64Array).value?(1).should eq(0.0)
    batch["price"].as(Arrow::Float64Array)[1].should be_nil
  end

  it "filters with the Arrow compute kernel" do
    prices = Arrow::Float64Array.from_values([10.0, 20.0, 30.0])
    batch = Arrow::RecordBatch.from_columns({price: prices})

    mask = Arrow.gt(batch["price"], Arrow::Float64Array.new_scalar(15.0))
    kept = Arrow.filter_record_batch(batch, mask)

    kept.num_rows.should eq(2)
  end

  it "requires an explicit cast for mixed-dtype arithmetic" do
    ints = Arrow::Int64Array.from_values([1_i64, 2_i64])
    floats = Arrow::Float64Array.from_values([1.5, 2.5])

    expect_raises(Arrow::ArrowError) { Arrow.mul(floats, ints) }

    promoted = Arrow.cast(ints, Arrow::DataType.float64)
    product = Arrow.mul(floats, promoted).as(Arrow::Float64Array)
    product.to_a.should eq([1.5, 5.0])
  end

  it "round-trips a record batch through Parquet" do
    batch = Arrow::RecordBatch.from_columns({
      region: Arrow::StringArray.from_nullable(["east", "west"]),
      price:  Arrow::Float64Array.from_values([10.0, 20.0]),
    })

    props = Parquet::File::Properties::WriterProperties.builder
      .set_compression(Parquet::Compression.zstd(3))
      .build

    sink = IO::Memory.new
    writer = Parquet::Arrow::ArrowWriter.try_new(sink, batch.schema, props)
    writer.write(batch)
    writer.close.file_metadata.num_rows.should eq(2)

    reader = Parquet::Arrow::ParquetRecordBatchReaderBuilder.try_new(
      Parquet::File::SerializedFileReader.new(
        Parquet::File::BytesChunkReader.new(sink.to_slice)
      )
    ).build

    round_tripped = reader.to_a
    round_tripped.size.should eq(1)
    round_tripped.first.num_rows.should eq(2)
    round_tripped.first.schema.fields.map(&.name).should eq(["region", "price"])
  end

  it "projects Parquet columns by name" do
    batch = Arrow::RecordBatch.from_columns({
      region: Arrow::StringArray.from_nullable(["east"]),
      price:  Arrow::Float64Array.from_values([10.0]),
    })

    sink = IO::Memory.new
    writer = Parquet::Arrow::ArrowWriter.try_new(sink, batch.schema)
    writer.write(batch)
    writer.close

    builder = Parquet::Arrow::ParquetRecordBatchReaderBuilder.try_new(
      Parquet::File::SerializedFileReader.new(
        Parquet::File::BytesChunkReader.new(sink.to_slice)
      )
    )
    mask = Parquet::Arrow::ProjectionMask.columns(builder.parquet_schema, ["price"])

    builder.with_projection(mask).build.schema.fields.map(&.name).should eq(["price"])
  end
end
