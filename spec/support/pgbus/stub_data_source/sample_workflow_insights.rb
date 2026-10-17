# frozen_string_literal: true

module Pgbus
  module Test
    class StubDataSource
      # The workflow insights' sample data (issue #549), in the shape
      # WorkflowInsightsQueries returns: OrderWorkflow (a fan-out step, and
      # capture failing far more on the newest revision than on the one
      # before), SignupWorkflow (a removed step two old runs still sit on,
      # and a step with too few samples for its percentiles), ShippingWorkflow
      # (a step waiting on a signal) and Billing::InvoiceWorkflow, whose class
      # no longer loads. Each funnel adds up: the runs stopped at each step
      # sum to the runs started. Part of fill_sample_data!.
      module SampleWorkflowInsights
        # Stands in for a loaded Pgbus::Workflow subclass: the report reads
        # only its name and its step list.
        SampleClass = Struct.new(:name, :workflow_steps)
        INVOICE = "Billing::InvoiceWorkflow"
        OLDEST_REVISION = "e5b7a20"

        private

        def fill_sample_workflow_insights!(now)
          @workflow_insights_samples = {
            "OrderWorkflow" => sample_order_insights(now), "SignupWorkflow" => sample_signup_insights(now),
            "ShippingWorkflow" => sample_shipping_insights(now), INVOICE => sample_invoice_insights(now)
          }
        end

        def insight_class(name, *entries) = SampleClass.new(name, Pgbus::Workflow::Steps.new(entries))

        def insight_step(name, **) = Pgbus::Workflow::StepDefinition.new(name: name, body: name.to_sym, **)

        def insight_ongoing(klass, rows)
          rows.map { |status, step, runs| { workflow_class: klass, status: status, step: step, runs: runs } }
        end

        def insight_ended(klass, finished, cancelled, p50, p95, p99)
          { workflow_class: klass, finished: finished, cancelled: cancelled,
            e2e_ms: { p50: p50, p95: p95, p99: p99, samples: finished } }
        end

        # What step_stats gives one step: run and pickup are [p50, p95(, p99)] in ms.
        def insight_stat(attempts, run:, pickup:, first_seen_at:, failed: 0, reattempted: 0, skipped: 0, cancelled: 0)
          { attempts: attempts, completed: attempts - failed - skipped - cancelled, failed: failed, reattempted: reattempted,
            skipped: skipped, cancelled: cancelled,
            run_ms: { p50: run[0], p95: run[1], p99: run[2], samples: attempts - skipped - cancelled },
            pickup_ms: { p50: pickup[0], p95: pickup[1], samples: attempts - cancelled }, first_seen_at: first_seen_at }
        end

        def insight_reached(counts) = counts.each_with_index.to_h { |(name, reached), i| [name, { reached: reached, position: i + 1.0 }] }

        # by_revision rows: the revision's total, then each step.
        def insight_revision(revision, first_seen_at, last_seen_at, total, steps)
          [nil, *steps.keys].map do |step|
            { revision: revision, first_seen_at: first_seen_at, last_seen_at: last_seen_at, step: step,
              stats: step ? steps[step] : total }
          end
        end

        def sample_order_insights(now)
          day = now - 86_000
          { klass: insight_class("OrderWorkflow", insight_step(:reserve), insight_step(:capture),
                                 Pgbus::Workflow::FanOutDefinition.new(name: :notify_vendors, body: :notify_vendors),
                                 insight_step(:confirm)),
            hourly: 18, attention: 1,
            ongoing: insight_ongoing("OrderWorkflow", [["ready", "reserve", 3], ["performing", "capture", 2],
                                                       ["paused", "capture", 1], ["waiting", "notify_vendors", 4],
                                                       ["ready", "confirm", 1]]),
            ended: insight_ended("OrderWorkflow", 412, 9, 48_200.0, 312_000.0, 905_000.0),
            stats: { "reserve" => insight_stat(434, run: [42.0, 180.0, 410.0], pickup: [35.0, 220.0], first_seen_at: day,
                                                    failed: 2, reattempted: 2),
                     "capture" => insight_stat(468, run: [820.0, 4_100.0, 9_800.0], pickup: [60.0, 1_900.0],
                                                    first_seen_at: day + 2, failed: 39, reattempted: 31),
                     "notify_vendors" => insight_stat(420, run: [41_000.0, 240_000.0, 610_000.0], pickup: [80.0, 2_400.0],
                                                           first_seen_at: day + 5, failed: 3),
                     "confirm" => insight_stat(413, run: [95.0, 400.0, 900.0], pickup: [40.0, 300.0], first_seen_at: day + 60) },
            histogram: { 1 => 12, 2 => 238, 3 => 151, 4 => 11 },
            reached: insight_reached("reserve" => 432, "capture" => 429, "notify_vendors" => 420, "confirm" => 413),
            stopped: { %w[reserve ready] => 3, %w[capture performing] => 2, %w[capture paused] => 1,
                       %w[capture cancelled] => 6, %w[notify_vendors waiting] => 4, %w[notify_vendors cancelled] => 3,
                       %w[confirm ready] => 1, %w[confirm finished] => 412 },
            revisions: sample_order_revisions(now) }
        end

        # capture fails on 18 % of the newest revision's attempts, 1 % before;
        # each total is the sum of its steps, as the by_revision query gives.
        def sample_order_revisions(now)
          stat = lambda { |attempts, failed, p95|
            insight_stat(attempts, run: [820.0, p95, p95 * 2], pickup: [60.0, 900.0], first_seen_at: now, failed: failed,
                                   reattempted: failed)
          }
          insight_revision(SampleDeployTasks::NEW_REVISION, now - 14_400, now - 60, stat.call(560, 27, 6_200.0),
                           "reserve" => stat.call(140, 0, 180.0), "capture" => stat.call(150, 27, 9_100.0),
                           "notify_vendors" => stat.call(136, 0, 240_000.0), "confirm" => stat.call(134, 0, 400.0)) +
            insight_revision(SampleDeployTasks::OLD_REVISION, now - 86_000, now - 14_500, stat.call(1_180, 4, 2_900.0),
                             "reserve" => stat.call(296, 0, 180.0), "capture" => stat.call(302, 3, 3_200.0),
                             "notify_vendors" => stat.call(292, 1, 240_000.0), "confirm" => stat.call(290, 0, 400.0))
        end

        def sample_signup_insights(now)
          klass = insight_class("SignupWorkflow", insight_step(:welcome), Pgbus::Workflow::RemovedStep.new(name: "nudge"),
                                insight_step(:reminder, wait: 172_800))
          { klass: klass, hourly: 0.7, attention: 1,
            ongoing: insight_ongoing("SignupWorkflow", [["ready", "welcome", 1], ["performing", "nudge", 2],
                                                        ["ready", "reminder", 14]]),
            ended: insight_ended("SignupWorkflow", 64, 2, 172_900_000.0, 173_400_000.0, 174_100_000.0),
            stats: { "welcome" => insight_stat(81, run: [120.0, 380.0, 900.0], pickup: [50.0, 400.0], first_seen_at: now - 86_000),
                     "nudge" => insight_stat(3, run: [nil, nil, nil], pickup: [20.0, 40.0], first_seen_at: now - 80_000,
                                                skipped: 3),
                     "reminder" => insight_stat(7, run: [210.0, 640.0, 700.0], pickup: [1_200.0, 4_800.0],
                                                   first_seen_at: now - 70_000) },
            histogram: { 7 => 64 },
            reached: insight_reached("welcome" => 15, "reminder" => 14),
            stopped: { %w[welcome ready] => 1, %w[reminder ready] => 14 },
            revisions: insight_revision(SampleDeployTasks::NEW_REVISION, now - 86_000, now - 300,
                                        insight_stat(91, run: [140.0, 420.0, 900.0], pickup: [55.0, 900.0], first_seen_at: now,
                                                         skipped: 3),
                                        "welcome" => insight_stat(81, run: [120.0, 380.0, 900.0], pickup: [50.0, 400.0],
                                                                      first_seen_at: now),
                                        "nudge" => insight_stat(3, run: [nil, nil, nil], pickup: [20.0, 40.0], first_seen_at: now,
                                                                   skipped: 3),
                                        "reminder" => insight_stat(7, run: [210.0, 640.0, 700.0], pickup: [1_200.0, 4_800.0],
                                                                      first_seen_at: now)) }
        end

        def sample_shipping_insights(now)
          klass = insight_class("ShippingWorkflow", insight_step(:label), insight_step(:ship, wait_for: :shipped),
                                insight_step(:notify))
          stat = ->(attempts, run) { insight_stat(attempts, run: run, pickup: [70.0, 650.0], first_seen_at: now - 80_000) }
          { klass: klass, hourly: 6, attention: 0,
            ongoing: insight_ongoing("ShippingWorkflow", [["waiting", "ship", 23], ["ready", "notify", 1]]),
            ended: insight_ended("ShippingWorkflow", 133, 0, 30_600_000.0, 71_000_000.0, 90_000_000.0),
            stats: { "label" => stat.call(160, [600.0, 1_500.0, 2_200.0]),
                     "ship" => stat.call(137, [28_800_000.0, 70_000_000.0, 83_000_000.0]),
                     "notify" => stat.call(133, [180.0, 520.0, 800.0]) },
            histogram: { 5 => 41, 6 => 88, 7 => 4 },
            reached: insight_reached("label" => 50, "ship" => 50, "notify" => 27),
            stopped: { %w[ship waiting] => 23, %w[notify ready] => 1, %w[notify finished] => 26 },
            revisions: insight_revision(SampleDeployTasks::NEW_REVISION, now - 14_400, now - 120, stat.call(180, [700.0, 2_000.0, 3_000.0]),
                                        "label" => stat.call(64, [600.0, 1_500.0, 2_200.0]),
                                        "ship" => stat.call(58, [28_800_000.0, 70_000_000.0, 83_000_000.0]),
                                        "notify" => stat.call(58, [180.0, 520.0, 800.0])) +
              insight_revision(OLDEST_REVISION, now - 86_000, now - 14_500, stat.call(240, [690.0, 1_900.0, 2_900.0]),
                               "label" => stat.call(96, [610.0, 1_480.0, 2_100.0]),
                               "ship" => stat.call(69, [28_500_000.0, 69_000_000.0, 82_000_000.0]),
                               "notify" => stat.call(75, [175.0, 515.0, 790.0])) }
        end

        # The class is gone from the code: the steps come from the rows.
        def sample_invoice_insights(now)
          stat = lambda { |attempts, failed|
            insight_stat(attempts, run: [2_400.0, 9_000.0, 9_500.0], pickup: [90.0, 700.0], first_seen_at: now - 40_000, failed: failed)
          }
          { klass: nil, hourly: 0.2, attention: 2,
            ongoing: insight_ongoing(INVOICE, [["paused", "collect", 2]]),
            ended: insight_ended(INVOICE, 5, 1, 3_600_000.0, 7_100_000.0, 7_200_000.0),
            stats: { "draft" => stat.call(7, 0), "send" => stat.call(7, 0), "collect" => stat.call(9, 3) },
            histogram: { 4 => 1, 5 => 4 },
            reached: insight_reached("draft" => 3, "send" => 3, "collect" => 3),
            stopped: { %w[collect paused] => 2, %w[collect finished] => 1 },
            revisions: [] }
        end
      end
    end
  end
end
