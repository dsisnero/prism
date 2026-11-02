require "./spec_helper"

# The examples are the code a reader copies out of the README, so they are
# compiled and executed rather than eyeballed. Each one costs a full dependency
# compile, so this file is opt-in:
#
#     PRISM_EXAMPLE_SPECS=1 make test
#
# The markers are the lines the examples print on purpose: stable, greppable
# summaries rather than table formatting, which would make the spec brittle.
private EXAMPLE_OUTPUT = {
  "quickstart" => [
    "rows: 5, columns: 4",
    "filtered rows: 1",
    "grouped rows: 3",
    "joined rows: 5",
    "top region total: 7",
  ],
  "parquet" => [
    "rows read: 5",
    "projected columns: region, price",
    "predicate rows: 3",
    "projection: [region, qty]",
  ],
}

if ENV["PRISM_EXAMPLE_SPECS"]?
  describe "examples" do
    it "keeps every example in examples/" do
      Dir.exists?("examples").should be_true
      Dir.glob("examples/*.cr").size.should be >= 3
    end

    EXAMPLE_OUTPUT.each do |name, markers|
      it "runs examples/#{name}.cr and prints its documented output" do
        output = `crystal run --no-color examples/#{name}.cr 2>&1`
        status = $?
        status.success?.should be_true
        markers.each { |marker| output.should contain(marker) }
      end
    end

    # The viewer needs a terminal, so it is compiled but not run: a syntax or
    # API error is what this catches.
    it "compiles examples/viewer.cr" do
      output = `crystal build --no-codegen --no-color examples/viewer.cr 2>&1`
      $?.success?.should be_true
      output.should_not contain("Error")
    end
  end
end
