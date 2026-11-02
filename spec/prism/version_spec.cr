require "../spec_helper"

# Release hygiene: the version a consumer sees (`Prism::VERSION`) and the version
# shards resolves (`shard.yml`) must agree, or a tag quietly ships the wrong
# number.
describe "Prism::VERSION" do
  it "matches the version declared in shard.yml" do
    declared = File.read_lines("shard.yml")
      .find(&.starts_with?("version:"))
      .try(&.split(':', 2).last.strip)

    declared.should_not be_nil
    Prism::VERSION.should eq(declared)
  end

  it "is semver-shaped" do
    Prism::VERSION.should match(/^\d+\.\d+\.\d+$/)
  end
end
