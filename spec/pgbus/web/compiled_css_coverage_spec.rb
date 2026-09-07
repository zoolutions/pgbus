# frozen_string_literal: true

require "spec_helper"
require "pathname"

# app/frontend/pgbus/style.css is a COMMITTED Tailwind build artifact. Nothing in
# CI recompiles it, so a class added to a view after the last build silently
# resolves to no CSS at all — the element renders, unstyled. That is how the
# batch progress bar lost its fill colour (bg-green-500) while still sizing
# itself correctly. This spec fails the moment the artifact drifts from the views.
RSpec.describe "Compiled dashboard CSS covers the classes views use" do # rubocop:disable RSpec/DescribeClass
  let(:engine_root) { Pathname.new(__dir__).join("..", "..", "..").expand_path }
  let(:frontend_dir) { engine_root.join("app", "frontend", "pgbus") }
  let(:views_dir) { engine_root.join("app", "views") }
  let(:compiled_css) { File.read(frontend_dir.join("style.css")) }

  # Selectors Tailwind emitted, with CSS escapes (\:, \., \/) unwrapped so
  # ".sm\:mt-0" matches the "sm:mt-0" written in the template.
  let(:compiled_classes) do
    compiled_css.scan(/\.((?:[\w-]|\\.)+)/).flatten.to_set { |sel| sel.gsub(/\\(.)/, '\1') }
  end

  # A class token is "static" when it is a literal run of Tailwind-ish
  # characters. Tokens carrying Ruby or interpolation are skipped — guessing at
  # them would make this spec flaky.
  let(:static_class_token) { %r{\A-?[a-z][a-z0-9]*(?:[:/.-]?[a-z0-9-]+)*\z} }

  # Utilities that legitimately have no Tailwind rule: `dark` is the variant
  # toggle set on <html>, `pgbus-table` is defined by @utility in tailwind.css,
  # and `group`/`peer` are marker classes Tailwind never emits a rule for.
  let(:allowed_uncompiled) { %w[dark group peer pgbus-table] }

  let(:erb_tag) { /<%.*?%>/m }

  # Class attributes routinely interleave literal text with an ERB ternary that
  # picks between two literal class lists:
  #
  #   class="h-4 rounded-full <%= failed? ? 'bg-amber-500' : 'bg-green-500' %>"
  #
  # Both branches ship to the browser, so both must exist in the artifact. Take
  # the text outside the ERB tags verbatim and the single-quoted literals inside
  # them; anything else in the tag is Ruby and is ignored.
  def class_tokens(content)
    content.scan(/class(?:es)?(?:=|:\s*)"([^"]*)"|class(?:es)?(?:=|:\s*)'([^']*)'/)
           .map { |double, single| double || single }
           .flat_map { |value| literal_class_text(value.to_s) }
           .flat_map { |text| text.split(/\s+/) }
           .grep(static_class_token)
  end

  def literal_class_text(value)
    erb_literals = value.scan(erb_tag).flat_map { |tag| tag.scan(/'([^']*)'/).flatten }
    [value.gsub(erb_tag, " "), *erb_literals]
  end

  it "emits every static Tailwind class the ERB templates reference" do
    missing = Hash.new { |hash, key| hash[key] = [] }

    Dir[views_dir.join("**", "*.erb")].each do |file|
      relative = Pathname.new(file).relative_path_from(engine_root)
      class_tokens(File.read(file)).uniq.each do |token|
        next if allowed_uncompiled.include?(token)
        next if compiled_classes.include?(token)

        missing[token] << relative.to_s
      end
    end

    report = missing.sort.map { |token, files| "#{token} (#{files.uniq.first(3).join(", ")})" }

    expect(report).to be_empty, <<~MSG
      #{report.size} class(es) used in views are absent from app/frontend/pgbus/style.css.
      Rebuild the artifact with `bundle exec rake frontend:css` and commit it.

        #{report.join("\n  ")}
    MSG
  end
end
