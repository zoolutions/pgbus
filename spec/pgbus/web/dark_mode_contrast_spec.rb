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
  let(:helper_files) { Dir[engine_root.join("app", "helpers", "pgbus", "*.rb")].sort }

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

  # An ERB tag, or a Ruby interpolation inside a `class:` string literal.
  let(:erb_tag) { /<%.*?%>|#\{.*?\}/m }
  # Text colours below WCAG AA (4.5:1) for body text on the dashboard's
  # surfaces: gray-400 on white is ~2.5:1, gray-500/600 on gray-800 ~3:1.
  # Disabled controls (cursor-not-allowed) are exempt, as WCAG allows.
  let(:low_contrast_tokens) { %w[text-gray-400 dark:text-gray-500 dark:text-gray-600] }
  # Shades the axe gate (#498) measured below 4.5:1 as body text on white:
  # every -500 but gray (blue-500 3.7:1, red-500 3.8:1, indigo-500 4.5- under
  # the pointer) and the warm -600s (amber 3.2:1, yellow 3.2:1, green 3.3:1).
  # Large text (text-2xl and up) only needs 3:1, so stat numbers may keep them.
  let(:low_light_shade) { /\A(?:hover:)?text-(?:(?!gray)[a-z]+-500|(?:amber|yellow|green|lime|orange)-600)\z/ }
  let(:large_text) { /\Atext-[2-9]xl\z/ }

  # Each class attribute as token lists: one per ERB ternary branch (the text
  # outside ERB tags plus that branch's single-quoted literal), so a dark
  # token in one branch cannot mask an unpaired light token in another. A
  # `class:` built from "…" \ "…" continuations is read as one string.
  def class_attributes(content)
    content.scan(/class(?:=|:\s*)((?:"[^"]*"\s*\\\s*)*"[^"]*")|class(?:=|:\s*)'([^']*)'/).flat_map do |double, single|
      value = double ? double.scan(/"([^"]*)"/).join(" ") : single.to_s
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

  # Dark pairings the gate caught: indigo-400 on its own /30 badge tint
  # (4.4:1), and gray-400 text on a gray-700 chip (4.0:1).
  def low_contrast_pairing(tokens)
    return [] if tokens.include?("cursor-not-allowed")

    found = tokens.any? { |t| t.match?(large_text) } ? [] : tokens.grep(low_light_shade)
    found << "dark:text-indigo-400" if (tokens & %w[dark:text-indigo-400 dark:bg-indigo-900/30]).size == 2
    found << "dark:text-gray-400" if (tokens & %w[dark:text-gray-400 dark:bg-gray-700]).size == 2
    found
  end

  describe "class attribute parsing" do
    it "reads a class: built from string continuations with an interpolated ternary" do
      erb = <<~'ERB'
        <%= button_to "x", "/x",
              class: "inline-flex rounded " \
                     "#{on ? 'bg-yellow-100 text-yellow-700' : 'bg-green-100 dark:bg-green-900/30'}" %>
      ERB

      expect(class_attributes(erb).map { |tokens| gaps(tokens) }).to eq([%w[bg-yellow-100 text-yellow-700], []])
    end
  end

  describe "low-contrast shades" do
    it "flags small -500 text and warm -600 text in light mode" do
      expect(low_contrast_pairing(%w[text-xs text-blue-500 dark:text-blue-400])).to eq(%w[text-blue-500])
      expect(low_contrast_pairing(%w[text-sm text-amber-600 dark:text-amber-400])).to eq(%w[text-amber-600])
      expect(low_contrast_pairing(%w[hover:text-indigo-500 dark:hover:text-indigo-300])).to eq(%w[hover:text-indigo-500])
    end

    it "allows them on large text and allows the passing shades" do
      expect(low_contrast_pairing(%w[text-3xl text-amber-600 dark:text-amber-400])).to be_empty
      expect(low_contrast_pairing(%w[text-sm text-gray-500 text-indigo-600 text-red-600 text-amber-700])).to be_empty
      expect(low_contrast_pairing(%w[text-xs text-blue-500 cursor-not-allowed])).to be_empty
    end

    it "flags indigo-400 on an indigo-900/30 badge and gray-400 on a gray-700 chip" do
      expect(low_contrast_pairing(%w[bg-indigo-100 text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-400]))
        .to eq(%w[dark:text-indigo-400])
      expect(low_contrast_pairing(%w[bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-400]))
        .to eq(%w[dark:text-gray-400])
      expect(low_contrast_pairing(%w[dark:bg-indigo-900/30 dark:text-indigo-300 dark:bg-gray-700 dark:text-gray-200]))
        .to be_empty
    end

    it "keeps the views and badge helpers clear of them" do
      found = offenders(:low_contrast_pairing)

      expect(found).to be_empty, "#{found.size} low-contrast shade(s):\n  #{found.join("\n  ")}"
    end
  end

  def offenders(check)
    found = view_files.flat_map do |file|
      class_attributes(File.read(file)).flat_map { |tokens| send(check, tokens).map { |t| "#{relative(file)}: #{t}" } }
    end
    found += helper_files.flat_map do |file|
      helper_class_strings(File.read(file)).flat_map { |tokens| send(check, tokens).map { |t| "#{relative(file)}: #{t}" } }
    end
    found.uniq
  end

  def relative(file) = Pathname.new(file).relative_path_from(engine_root).to_s

  it "pairs every light colour with a dark variant" do
    missing = view_files.flat_map do |file|
      class_attributes(File.read(file)).flat_map { |tokens| gaps(tokens).map { |t| "#{relative(file)}: #{t}" } }
    end
    missing += helper_files.flat_map do |file|
      helper_class_strings(File.read(file)).flat_map { |tokens| gaps(tokens).map { |t| "#{relative(file)}: #{t}" } }
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
