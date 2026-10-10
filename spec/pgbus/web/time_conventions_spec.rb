# frozen_string_literal: true

require "spec_helper"
require "pathname"

# Every timestamp, age and duration a dashboard view prints goes through the
# TimeFormat helpers (issue #497): pgbus_time, pgbus_timestamp,
# pgbus_absolute_time, pgbus_duration, pgbus_ms_duration. RuboCop's
# Pgbus/DashboardTimeFormatting guards the Ruby code; RuboCop cannot parse ERB,
# so this static spec guards the views.
#
# Two rules:
# - no hand-rolled formatting: strftime, Rails' distance helpers, to_fs,
#   iso8601, l/localize, or the removed pgbus_time_ago / pgbus_job_eta helpers;
# - no raw time field: an output tag (<%= %>) whose whole expression is a field
#   named like a moment (*_at, vt) or an age/duration (*_age, *_sec, *_seconds,
#   *_ms), optionally with an `|| "—"` fallback. Those print Time#to_s, an ISO
#   string or bare seconds.
RSpec.describe "Dashboard time conventions" do # rubocop:disable RSpec/DescribeClass
  let(:banned_calls) do
    /\b(strftime|time_ago_in_words|distance_of_time_in_words(?:_to_now)?|to_fs|to_formatted_s|iso8601|
       pgbus_time_ago(?:_future)?|pgbus_job_eta|localize)\b|(?<![\w.])l\(/x
  end
  let(:time_field) { /(?:_at|\Avt|_age|_sec|_seconds|_ms)\z/ }
  # receiver, then any number of [:key] / ["key"] / .name / &.name accesses
  let(:accessor) { /\A@?[a-z_]\w*((?:\[:\w+\]|\["\w+"\]|\.\w+|&\.\w+)*)(?:\s*\|\|\s*"[^"]*")?\z/ }
  let(:engine_root) { Pathname.new(__dir__).join("..", "..", "..").expand_path }
  let(:view_files) { Dir[engine_root.join("app", "views", "pgbus", "**", "*.erb")] }

  def raw_time_output?(expression)
    match = expression.strip.match(accessor)
    return false unless match

    last = match[1].scan(/\[:(\w+)\]|\["(\w+)"\]|\.(\w+)/).last&.compact&.first
    time_field.match?(last || expression.strip[/\A@?(\w+)/, 1])
  end

  def output_tags(source) = source.scan(/<%=(.*?)-?%>/m).flatten

  def relative(file) = Pathname.new(file).relative_path_from(engine_root)

  describe "the raw-time-field rule" do
    it "flags a bare time field, with or without a fallback" do
      ["row[:enqueued_at]", '@job["failed_at"]', "entry.created_at", "m[:vt]",
       'q[:oldest_claimable_age_sec] || "—"', "@stats[:oldest_unpublished_age]", "row[:avg_ms]"]
        .each { |expression| expect(raw_time_output?(expression)).to be(true), expression }
    end

    it "accepts a field passed through a helper, and fields that are not times" do
      ["pgbus_time(row[:enqueued_at])", "pgbus_duration(q[:oldest_claimable_age_sec])", "row[:msg_id]",
       "m[:read_ct]", 't("pgbus.jobs.show.labels.failed_at")', "batch[:status]"]
        .each { |expression| expect(raw_time_output?(expression)).to be(false), expression }
    end
  end

  describe "the banned-call rule" do
    let(:hand_rolled) do
      ['t.strftime("%H:%M")', "time_ago_in_words(x)", "x.to_fs(:short)", "pgbus_time_ago(x)", "x.iso8601",
       "l(x, format: :short)"]
    end

    it "flags hand-rolled formatting" do
      expect(hand_rolled).to all(match(banned_calls))
    end
  end

  it "has no hand-rolled time formatting in any view" do
    found = view_files.flat_map do |file|
      File.readlines(file).each_with_index.filter_map do |line, index|
        next unless line.match?(banned_calls)

        "#{relative(file)}:#{index + 1}: #{line.strip}"
      end
    end

    expect(found).to be_empty,
                     "#{found.size} hand-rolled time format(s); use pgbus_time / pgbus_timestamp / " \
                     "pgbus_absolute_time / pgbus_duration:\n  #{found.join("\n  ")}"
  end

  it "prints no time field raw in any view" do
    found = view_files.flat_map do |file|
      output_tags(File.read(file)).select { |expression| raw_time_output?(expression) }
                                  .map { |expression| "#{relative(file)}: <%=#{expression}%>" }
    end

    expect(found).to be_empty,
                     "#{found.size} raw time field(s); wrap moments in pgbus_time / pgbus_timestamp and ages " \
                     "in pgbus_duration / pgbus_ms_duration:\n  #{found.join("\n  ")}"
  end
end
