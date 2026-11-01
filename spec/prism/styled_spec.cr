require "../spec_helper"
require "../../src/prism/lipgloss"

private def styled_frame : Prism::DataFrame
  SpecFrames.sales
end

# Force a colorless profile so the rendering is deterministic in any terminal.
private def plain_colors(&)
  renderer = Lipgloss::StyleRenderer.default
  previous = renderer.color_profile
  begin
    renderer.color_profile = Colorprofile::Profile::ASCII
    yield
  ensure
    renderer.color_profile = previous
  end
end

describe Prism::Styled do
  it "renders a bordered table with the shape line" do
    rendered = plain_colors { Prism::Styled.render(styled_frame) }

    rendered.lines.first.should eq("shape: (5, 3)")
    rendered.should contain("┌")
    rendered.should contain("└")
    rendered.should contain("region")
    rendered.should contain("east")
    rendered.should contain("f64")
  end

  it "puts a separator under the header and the dtype row first" do
    rendered = plain_colors { Prism::Styled.render(styled_frame) }
    lines = rendered.lines

    # lines[0] is the shape line, lines[1] the top border.
    lines[2].should contain("region")
    lines[3].should contain("├")
    lines[4].should contain("f64")
  end

  it "emits no ANSI escapes under a colorless profile" do
    rendered = plain_colors { Prism::Styled.render(styled_frame) }

    rendered.includes?("\e[").should be_false
  end

  it "shares selection and truncation with the plain renderer" do
    rendered = plain_colors do
      Prism::Styled.render(styled_frame, max_rows: 2, max_cols: 2)
    end

    rendered.should contain("...")
    rendered.lines.size.should be < styled_frame.to_s(max_rows: 2, max_cols: 2).lines.size + 4
  end

  it "is available as a DataFrame method" do
    plain_colors do
      styled_frame.to_styled.should eq(Prism::Styled.render(styled_frame))
    end
  end

  it "honours the border argument" do
    rendered = plain_colors do
      Prism::Styled.render(styled_frame, border: Lipgloss::Border.rounded)
    end

    rendered.should contain("╭")
  end
end
