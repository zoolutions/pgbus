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
      /\Abg-(white|gray-50|gray-100|gray-200)\z/ => "bg",
      /\Aborder-gray-(100|200)\z/ => "border",
      /\Aring-gray-200\z/ => "ring",
      /\Abg-[a-z]+-100\z/ => "bg",
      /\Atext-(?!gray)[a-z]+-[678]00\z/ => "text"
    }
  end

  let(:erb_tag) { /<%.*?%>/m }
  # Text colours below WCAG AA (4.5:1) for body text on the dashboard's
  # surfaces: gray-400 on white is ~2.5:1, gray-500/600 on gray-800 ~3:1.
  # Disabled controls (cursor-not-allowed) are exempt, as WCAG allows.
  let(:low_contrast_tokens) { %w[text-gray-400 dark:text-gray-500 dark:text-gray-600] }

  # Each class attribute as token lists: one per ERB ternary branch (the text
  # outside ERB tags plus that branch's single-quoted literal), so a dark
  # token in one branch cannot mask an unpaired light token in another.
  def class_attributes(content)
    content.scan(/class(?:=|:\s*)"([^"]*)"|class(?:=|:\s*)'([^']*)'/).flat_map do |double, single|
      value = (double || single).to_s
      base = value.gsub(erb_tag, " ").split(/\s+/)
      branches = value.scan(erb_tag).flat_map { |tag| tag.scan(/'([^']*)'/).flatten }
      branches.empty? ? [base] : branches.map { |branch| base + branch.split(/\s+/) }
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

  def low_contrast(tokens)
    return [] if tokens.include?("cursor-not-allowed")

    tokens & low_contrast_tokens
  end

  # A light hover colour applies in dark mode too unless a dark:hover: partner
  # overrides it (hover:text-indigo-800 on a gray-800 row is unreadable).
  def hover_gaps(tokens)
    tokens.filter_map do |token|
      property = token[/\Ahover:(text|bg)-(?!white\b|transparent\b)/, 1]
      next unless property
      next if property == "bg" && !token.match?(/-(50|100|200)\z/)
      next if tokens.any? { |t| t.start_with?("dark:hover:#{property}-") }

      token
    end
  end

  def offenders(check)
    found = view_files.flat_map do |file|
      class_attributes(File.read(file)).flat_map { |tokens| send(check, tokens).map { |t| "#{relative(file)}: #{t}" } }
    end
    found += helper_class_strings(File.read(helper_file)).flat_map do |tokens|
      send(check, tokens).map { |t| "#{relative(helper_file)}: #{t}" }
    end
    found.uniq
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

  it "keeps every text colour at WCAG AA contrast in both themes" do
    found = offenders(:low_contrast)

    expect(found).to be_empty, "#{found.size} low-contrast text token(s):\n  #{found.join("\n  ")}"
  end

  it "gives every light hover colour a dark:hover: partner" do
    found = offenders(:hover_gaps)

    expect(found).to be_empty, "#{found.size} hover token(s) with no dark:hover: partner:\n  #{found.join("\n  ")}"
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
