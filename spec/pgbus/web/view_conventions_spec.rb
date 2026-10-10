# frozen_string_literal: true

require "spec_helper"
require "pathname"

require_relative "../../../app/helpers/pgbus/button_helper"

# The table and button conventions the Jobs list set (#489/#490), pinned for
# every dashboard view (issue #496): a pgbus-table on every table, a translated
# data-label on every cell (the mobile card label), empty states through
# shared/_empty_row, one table chrome, and every button or action link through
# Pgbus::ButtonHelper. Static guard over the view sources.
#
# The allowlist is per rule and per file. Every entry names the issue that removes
# it; the list only shrinks.
RSpec.describe "Dashboard view conventions" do # rubocop:disable RSpec/DescribeClass
  let(:allowlist) do
    {
      empty_row: {
        "app/views/pgbus/queues/show.html.erb" => "expanded-row colspan cells + inline empty row; #491 rewrites the messages table",
        "app/views/pgbus/dead_letter/_messages_table.html.erb" => "expanded-row colspan cells; #495 rewrites the DLQ messages table",
        "app/views/pgbus/events/_pending_table.html.erb" => "expanded-row colspan cells; #494 replaces the Events page"
      },
      chrome: {
        "app/views/pgbus/queues/show.html.erb" => "#491 keeps the py-2 Table Health block"
      }
    }.freeze
  end

  let(:engine_root) { Pathname.new(__dir__).join("..", "..", "..").expand_path }
  let(:view_files) { Dir[engine_root.join("app", "views", "pgbus", "**", "*.erb")].sort }

  let(:th_classes) do
    [
      "px-4 py-3 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400",
      "px-4 py-3 text-right text-xs font-medium uppercase text-gray-500 dark:text-gray-400",
      "w-10 px-4 py-3"
    ]
  end
  let(:thead_class) { "bg-gray-50 dark:bg-gray-900" }
  let(:tbody_class) { "divide-y divide-gray-100 dark:divide-gray-700" }

  def relative(file) = Pathname.new(file).relative_path_from(engine_root).to_s

  def allowed?(rule, file) = allowlist.fetch(rule, {}).key?(relative(file))

  # ERB tags can hold `>` (`t[:dead] > 1000`), which breaks tag-level regexes.
  # Replace each with a placeholder: ERB_T for a translation, ERB otherwise.
  def flatten_erb(content)
    content.gsub(/<%.*?%>/m) { |tag| tag.match?(/\A<%=\s*t\(/) ? "ERB_T" : "ERB" }
  end

  def tables(file) = flatten_erb(File.read(file)).scan(%r{<table\b.*?</table>}m)

  def pgbus_tables(file) = tables(file).select { |table| table[/<table\b[^>]*>/].include?("pgbus-table") }

  def tag_class(tag) = tag[/\bclass="([^"]*)"/, 1].to_s.split.join(" ")

  def report(found, what)
    "#{found.size} #{what}:\n  #{found.join("\n  ")}"
  end

  it "keeps the allowlist pointed at files that exist" do
    missing = allowlist.values.flat_map(&:keys).uniq.reject { |path| engine_root.join(path).exist? }

    expect(missing).to be_empty
  end

  it "gives every table the pgbus-table utility (cards below 1024 px)" do
    found = view_files.flat_map do |file|
      tables(file).reject { |table| table[/<table\b[^>]*>/].include?("pgbus-table") }.map { relative(file) }
    end

    expect(found).to be_empty, report(found, "table(s) without pgbus-table")
  end

  it "labels every pgbus-table cell with a translated data-label" do
    found = view_files.flat_map do |file|
      pgbus_tables(file).flat_map { |table| table.scan(/<td\b[^>]*>/) }.filter_map do |td|
        next if td.include?("colspan=") || tag_class(td).split.include?("w-10")
        next if td.include?('data-label="ERB_T"')

        "#{relative(file)}: #{td[0, 90]}"
      end
    end

    expect(found).to be_empty, report(found, "cell(s) without a translated data-label")
  end

  it "renders empty states through shared/_empty_row" do
    found = view_files.flat_map do |file|
      next [] if relative(file).end_with?("shared/_empty_row.html.erb") || allowed?(:empty_row, file)

      flatten_erb(File.read(file)).scan(%r{<tr\b[^>]*>\s*<td\b[^>]*colspan=[^>]*>(?:(?!<td\b).)*?</td>\s*</tr>}m)
                                  .reject { |row| row[/<tr\b[^>]*>/].include?("pgbus-job-detail") }
                                  .map { |row| "#{relative(file)}: #{row.gsub(/\s+/, " ")[0, 90]}" }
    end

    expect(found).to be_empty, report(found, "inline empty-state row(s)")
  end

  it "uses one table chrome for thead, tbody and th" do
    found = view_files.flat_map do |file|
      next [] if allowed?(:chrome, file)

      pgbus_tables(file).flat_map do |table|
        heads = table.scan(/<thead\b[^>]*>/).reject { |tag| tag_class(tag) == thead_class }
        bodies = table.scan(/<tbody\b[^>]*>/).reject { |tag| tag_class(tag) == tbody_class }
        ths = table.scan(/<th\b[^>]*>/).reject { |tag| th_classes.include?(tag_class(tag)) }
        (heads + bodies + ths).map { |tag| "#{relative(file)}: #{tag}" }
      end
    end

    expect(found).to be_empty, report(found, "table chrome tag(s) off the standard")
  end

  describe "buttons and action links" do
    def erb_calls(file, helper)
      File.read(file).scan(/<%=\s*#{helper}\b(.*?)%>/m).flatten
    end

    # A light text/bg colour (not a dark:/hover: variant) plus any hover state:
    # a hand-made action link. A card link (bg-white … hover:ring-…) has none.
    def hand_coloured?(call)
      classes = call[/\bclass:\s*"([^"]*)"/, 1].to_s
      classes.match?(/(?<![\w:-])(?:text|bg)-[a-z]+-\d/) && classes.include?("hover:")
    end

    it "never styles a button_to by hand" do
      found = view_files.flat_map do |file|
        next [] if allowed?(:raw_button, file)

        erb_calls(file, "button_to").grep(/\bclass:/).map { |call| "#{relative(file)}: button_to#{call.gsub(/\s+/, " ")[0, 80]}" }
      end

      expect(found).to be_empty, report(found, "hand-styled button_to call(s); use pgbus_button_to")
    end

    it "never styles a <button> by hand" do
      found = view_files.flat_map do |file|
        next [] if allowed?(:raw_button, file)

        flatten_erb(File.read(file)).scan(/<button\b[^>]*\bclass=[^>]*>/m).map { |tag| "#{relative(file)}: #{tag.gsub(/\s+/, " ")[0, 90]}" }
      end

      expect(found).to be_empty, report(found, "hand-styled <button> tag(s); use pgbus_button_tag")
    end

    it "never colours a link_to with a hover state by hand" do
      found = view_files.flat_map do |file|
        next [] if allowed?(:raw_button, file)

        erb_calls(file, "link_to").select { |call| hand_coloured?(call) }
                                  .map { |call| "#{relative(file)}: link_to#{call.gsub(/\s+/, " ")[0, 80]}" }
      end

      expect(found).to be_empty, report(found, "hand-styled link_to call(s); use pgbus_link_to")
    end

    it "gives every helper variant and size a target of at least 24 px (WCAG 2.5.8)" do
      helper = Class.new { include Pgbus::ButtonHelper }.new
      short = Pgbus::ButtonHelper::VARIANTS.keys.product(Pgbus::ButtonHelper::SIZES.keys).reject do |variant, size|
        helper.pgbus_button_classes(variant, size).split.include?("min-h-6")
      end

      expect(short).to be_empty
    end
  end
end
