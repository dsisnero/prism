require "../../spec_helper"

describe "IPC IO" do
  it "round-trips the file format" do
    io = IO::Memory.new
    SpecFrames.sales.write_ipc(io)
    io.rewind

    restored = Prism.read_ipc(io)

    restored.column_names.should eq(["region", "price", "qty"])
    restored.schema.should eq(SpecFrames.sales.schema)
    restored["region"].to_a.should eq(["east", "west", "east", nil, "west"])
  end

  it "round-trips the stream format" do
    io = IO::Memory.new
    SpecFrames.sales.write_ipc(io, stream: true)
    io.rewind

    restored = Prism.read_ipc(io, stream: true)

    restored.row_count.should eq(5)
    restored["price"].to_a.should eq([10.0, 20.0, 30.0, 40.0, nil])
  end

  it "round-trips through a file path" do
    path = SpecPaths.temp("round_trip.arrow")
    SpecFrames.sales.write_ipc(path)

    Prism.read_ipc(path)["qty"].to_a.should eq([1_i64, 2_i64, 3_i64, 4_i64, 5_i64])
  end

  it "keeps chunk boundaries as batches" do
    io = IO::Memory.new
    SpecFrames.sales.write_ipc(io)
    io.rewind

    restored = Prism.read_ipc(io)

    chunks = 0
    restored.each_batch { chunks += 1 }
    chunks.should be > 0
  end
end
