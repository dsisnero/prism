module Prism
  # Reads an Arrow IPC file (or stream) into a frame.
  #
  # The IPC file format carries a footer for random access; the stream format is
  # append-only. Both are supported, matching the two writers.
  def self.read_ipc(path : String, *, stream : Bool = false) : DataFrame
    File.open(path, "r") { |io| read_ipc(io, stream: stream) }
  end

  # Reads Arrow IPC from memory.
  def self.read_ipc(io : IO, *, stream : Bool = false) : DataFrame
    bytes = io.getb_to_end
    reader = if stream
               Arrow::Ipc::StreamReader.new(bytes)
             else
               Arrow::Ipc::FileReader.new(bytes)
             end

    batches = [] of Arrow::RecordBatch
    while batch = reader.maybe_next
      batches << batch
    end

    DataFrame.new(reader.schema, batches)
  end

  # Writes a frame as Arrow IPC to an IO sink.
  #
  # `ScopedWriter#open` finishes the writer on normal block completion, so the
  # footer (file format) or end-of-stream marker (stream format) is always
  # written on success.
  def self.write_ipc(
    io : IO,
    frame : DataFrame,
    *,
    stream : Bool = false,
  ) : Nil
    if stream
      Arrow::Ipc::StreamWriter.open(io, frame.schema) do |writer|
        frame.each_batch { |batch| writer.write(batch) }
      end
    else
      Arrow::Ipc::FileWriter.open(io, frame.schema) do |writer|
        frame.each_batch { |batch| writer.write(batch) }
      end
    end
  end

  # Writes a frame as Arrow IPC to a path.
  def self.write_ipc(path : String, frame : DataFrame, *, stream : Bool = false) : Nil
    File.open(path, "w") do |io|
      write_ipc(io, frame, stream: stream)
    end
  end

  class DataFrame
    # Writes this frame as Arrow IPC. See `Prism.write_ipc`.
    def write_ipc(path : String, **options) : Nil
      Prism.write_ipc(path, self, **options)
    end

    def write_ipc(io : IO, **options) : Nil
      Prism.write_ipc(io, self, **options)
    end
  end
end
