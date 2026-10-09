# frozen_string_literal: true

require "spec_helper"
require "pathname"

# The dashboard toggles dark mode with `.dark` on <html>, so every light colour
# a view sets needs a `dark:` partner or it keeps its light value on a dark
# card: gray-700 text on gray-800, a bg-green-100 badge glowing on gray-900
# (issue #489). Static guard over the view sources and the badge helpers.
#
# The layout's always-dark navbar lives in app/views/layouts/, which is not
# scanned.
RSpec.describe "Dashboard dark-mode contrast" do # rubocop:disable RSpec/DescribeClass
  let(:engine_root) { Pathname.new(__dir__).join("..", "..", "..").expand_path }
  let(:view_files) { Dir[engine_root.join("app", "views", "pgbus", "**", "*.erb")].sort }
  let(:helper_file) { engine_root.join("app", "helpers", "pgbus", "application_helper.rb").to_s }

  # Light tokens that need a dark partner for the same property.
  let(:light_rules) do
    {
      /\Atext-gray-[4-9]00\z/ => "text",
      /\Abg-(white|gray-50|gray-100)\z/ => "bg",
      /\Aborder-gray-(100|200)\z/ => "border",
      /\Aring-gray-200\z/ => "ring",
      /\Abg-[a-z]+-100\z/ => "bg",
      /\Atext-(?!gray)[a-z]+-[678]00\z/ => "text"
    }
  end

  let(:erb_tag) { /<%.*?%>/m }

  # Each class attribute as one token list: the literal text outside ERB tags
  # plus the single-quoted literals inside them (both ternary branches ship).
  def class_attributes(content)
    content.scan(/class(?:=|:\s*)"([^"]*)"|class(?:=|:\s*)'([^']*)'/).map do |double, single|
      value = (double || single).to_s
      literals = value.scan(erb_tag).flat_map { |tag| tag.scan(/'([^']*)'/).flatten }
      [value.gsub(erb_tag, " "), *literals].join(" ").split(/\s+/)
    end
  end

  # Helpers also build class lists in constants and string concatenations.
  def helper_class_strings(content)
    content.scan(/"([^"\n]*)"/).flatten.grep(/\b(?:bg|text)-[a-z]+-\d{2,3}\b/).map(&:split)
  end

  def gaps(tokens)
    tokens.filter_map do |token|
      property = light_rules.find { |pattern, _| token.match?(pattern) }&.last
      next unless property
      next if tokens.any? { |t| t.start_with?("dark:#{property}-") }

      token
    end
  end

  def relative(file) = Pathname.new(file).relative_path_from(engine_root).to_s

  it "pairs every light colour with a dark variant" do
    missing = view_files.flat_map do |file|
      class_attributes(File.read(file)).flat_map { |tokens| gaps(tokens).map { |t| "#{relative(file)}: #{t}" } }
    end
    missing += helper_class_strings(File.read(helper_file)).flat_map do |tokens|
      gaps(tokens).map { |t| "#{relative(helper_file)}: #{t}" }
    end

    expect(missing.uniq).to be_empty, <<~MSG
      #{missing.uniq.size} light class token(s) have no dark: partner for the same property:

        #{missing.uniq.join("\n  ")}
    MSG
  end

  it "never paints a table row darker than its card" do
    offenders = view_files.select { |file| File.read(file).match?(/<tr\b[^>]*dark:bg-gray-900/m) }

    expect(offenders.map { |f| relative(f) }).to be_empty
  end

  # A single-cell row (the empty state) must span every <th>. A table whose
  # checkbox column is conditional renders a different count, so it passes the
  # colspan in from ERB (shared/_empty_row) instead of hard-coding one.
  it "spans every static empty-state colspan across all of its table's columns" do
    wrong = view_files.flat_map do |file|
      File.read(file).scan(%r{<table\b.*?</table>}m).flat_map do |table|
        columns = table.scan(/<th\b/).size
        next [] if columns.zero?

        table.scan(%r{<tr\b[^>]*>\s*<td\b[^>]*colspan="(\d+)"[^>]*>(?:(?!<td\b).)*?</td>\s*</tr>}m)
             .flatten.map(&:to_i).reject { |n| n == columns }
             .map { |n| "#{relative(file)}: colspan=#{n}, #{columns} columns" }
      end
    end

    expect(wrong).to be_empty
  end
end
