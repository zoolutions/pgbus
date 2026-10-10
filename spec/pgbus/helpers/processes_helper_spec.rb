# frozen_string_literal: true

require "spec_helper"
require "action_view"
require "active_support/testing/time_helpers"

require_relative "../../../app/helpers/pgbus/application_helper"
require_relative "../../../app/helpers/pgbus/processes_helper"

RSpec.describe Pgbus::ProcessesHelper do
  include ActiveSupport::Testing::TimeHelpers

  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }
  let(:helper) do
    Class.new do
      include ActionView::Helpers::TagHelper
      include ActionView::Helpers::OutputSafetyHelper
      include ActionView::Helpers::TranslationHelper
      include ActionView::Helpers::NumberHelper
      include Pgbus::ApplicationHelper
      include Pgbus::ProcessesHelper
    end.new
  end

  before(:all) do # rubocop:disable RSpec/BeforeAfterAll
    I18n.load_path |= Dir[File.expand_path("../../../config/locales/*.yml", __dir__)]
    I18n.backend.reload!
  end

  around do |example|
    I18n.with_locale(:en) { Time.use_zone("UTC") { travel_to(now) { example.run } } }
  end

  def result(**attrs)
    defaults = { status: "healthy", badge_tone: :green, reason_key: "healthy_heartbeat",
                 reason_args: { ago: now - 12, limit: 300 }, next_key: nil, next_args: {}, activity: [],
                 recycle: nil, started_at: nil, uptime_seconds: nil, supervisor_pid: nil }
    Pgbus::Web::ProcessState::Result.new(**defaults, **attrs)
  end

  def text(html) = Nokogiri::HTML.fragment(html.to_s).text

  describe "#pgbus_process_status" do
    it "says the reason with the moment as <time> and the limit as a duration" do
      html = helper.pgbus_process_status(result)

      expect(html).to include("<time")
      expect(text(html)).to eq("Heartbeat 12s ago (limit 5m 0s)")
    end

    it "adds what happens next after a dash" do
      html = helper.pgbus_process_status(result(status: "stale", reason_key: "stale",
                                                reason_args: { age: 420, limit: 300 },
                                                next_key: "stale_reaped", next_args: { within: 300 }))

      expect(text(html)).to eq("No heartbeat for 7m 0s (limit 5m 0s) — probably dead; " \
                               "removed at the dispatcher's next sweep (within 5m 0s)")
    end

    it "has a sentence for every reason and next key in every locale" do
      reasons = %w[healthy_loop healthy_heartbeat healthy_detection_off stalled stale]
      nexts = %w[stale_reaped stale_unreaped stalled_supervised stalled_standalone stalled_maintenance stalled_schedule]
      keys = reasons.map { |k| "pgbus.processes.state.reasons.#{k}_html" } + nexts.map { |k| "pgbus.processes.state.next.#{k}" }

      %i[en da de es fi fr it ja nb nl pt sv].each do |locale|
        keys.each { |key| expect(I18n.exists?(key, locale)).to be(true), "#{locale}: #{key} missing" }
      end
    end

    it "prints a dash for an unknown age" do
      html = helper.pgbus_process_status(result(reason_key: "stale", reason_args: { age: nil, limit: 300 }))
      expect(text(html)).to eq("No heartbeat for — (limit 5m 0s)")
    end
  end

  describe "#pgbus_process_activity" do
    it "joins the parts with delimited numbers" do
      activity = [["drains", { queues: "default, mailers" }], ["busy_threads", { busy: 3, capacity: 5 }],
                  ["buffered", { number: 2 }], ["rate", { rate: 12.4 }], ["handled", { number: 1240 }]]

      expect(helper.pgbus_process_activity(result(activity: activity)))
        .to eq("Drains default, mailers · 3/5 threads busy · 2 buffered · 12.4 jobs/s · 1,240 handled")
    end

    it "pluralizes thread and slot counts and formats intervals" do
      expect(helper.pgbus_process_activity(result(activity: [["threads", { count: 1 }]]))).to eq("1 thread")
      expect(helper.pgbus_process_activity(result(activity: [["slots", { count: 4 }]]))).to eq("4 slots")
      expect(helper.pgbus_process_activity(result(activity: [["schedule", { count: 14, interval: 1.0 }]])))
        .to eq("Runs 14 recurring tasks, checks every 1s")
    end

    it "names a supervisor's children by kind, with an unknown kind by its name" do
      activity = [["supervises", { children: [["worker", 2], ["dispatcher", 1], ["outbox_poller", 1],
                                              ["streamer", 2]] }]]

      expect(helper.pgbus_process_activity(result(activity: activity)))
        .to eq("Supervises 2 workers, dispatcher, outbox poller, 2 streamer")
    end

    it "escapes queue names" do
      html = helper.pgbus_process_activity(result(activity: [["drains", { queues: "<b>x</b>" }]]))
      expect(html).to include("&lt;b&gt;")
    end

    it "is a dash without parts" do
      expect(helper.pgbus_process_activity(result)).to eq("—")
    end
  end

  describe "#pgbus_process_recycle" do
    it "states each limit with delimited counts and durations" do
      recycle = [["jobs", { done: 1240, limit: 10_000 }], ["memory", { used: 310, limit: 512 }],
                 ["lifetime", { uptime: 2520, limit: 3600 }]]

      expect(helper.pgbus_process_recycle(result(recycle: recycle)))
        .to eq("1,240 / 10,000 jobs · 310 / 512 MB · up 42m 0s / 1h 0m")
    end

    it "prints a dash for an unreported value and for no limits at all" do
      recycle = [["memory", { used: nil, limit: 512 }], ["lifetime", { uptime: nil, limit: 60 }]]

      expect(helper.pgbus_process_recycle(result(recycle: recycle))).to eq("— / 512 MB · up — / 1m 0s")
      expect(helper.pgbus_process_recycle(result)).to eq("—")
    end
  end

  describe "#pgbus_process_raw_metadata" do
    # Counters and RSS stay: Recycle shows them only when a limit is set.
    it "leaves out every key the row already renders" do
      metadata = { "queues" => ["default"], "execution_mode" => "threads", "capsule" => "render", "process" => "1/2",
                   "supervisor_pid" => 1, "memory_mb" => 3, "started_at" => 1.0, "capacity" => 5, "busy" => 1,
                   "buffered" => 0, "max_jobs" => 1, "max_memory_mb" => 1, "max_lifetime" => 1,
                   "loop_tick_at" => 1.0, "rates" => {}, "jobs_processed" => 1, "jobs_failed" => 0, "in_flight" => 0,
                   "topics" => ["a.*"], "tasks" => 3, "hostname" => "h", "pid" => 1 }

      expect(helper.pgbus_process_raw_metadata(metadata.merge("consumer_priority" => 0)).keys)
        .to eq(%w[execution_mode memory_mb jobs_processed jobs_failed consumer_priority])
    end

    it "is empty for non-Hash metadata" do
      expect(helper.pgbus_process_raw_metadata(nil)).to eq({})
    end
  end
end
