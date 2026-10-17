# frozen_string_literal: true

module Pgbus
  module Test
    class StubDataSource
      # The workflow insights (issue #549), the way DataSource::WorkflowInsights
      # answers them: workflow_insights_samples holds, per class, what
      # WorkflowInsightsQueries would return, and the real
      # Web::WorkflowInsightsReport shapes it. The throughput is drawn for
      # the window asked for; every other number is the same for any window.
      module WorkflowInsights
        def workflow_definitions(window:)
          record(:workflow_definitions, { window: window.name })
          @workflow_insights_samples.sort.map do |name, sample|
            Pgbus::Web::WorkflowInsightsReport.summary(name, sample[:klass], sample[:ongoing], sample[:ended],
                                                       sample[:attention])
          end
        end

        def workflow_definition(workflow_class, window:)
          record(:workflow_definition, workflow_class, { window: window.name })
          sample = @workflow_insights_samples[workflow_class]
          return nil unless sample

          now = Time.current
          since = window.since(now)
          Pgbus::Web::WorkflowInsightsReport.detail(
            workflow_class, sample[:klass],
            window: window, since: since, now: now, zone: Time.zone,
            throughput: sample_throughput(sample, since, now, window.bucket_seconds),
            **sample.slice(:ongoing, :ended, :attention, :stats, :histogram, :reached, :stopped, :revisions)
          )
        end

        private

        # A steady hourly rate with a daily swell, scaled to the bucket.
        def sample_throughput(sample, since, now, bucket)
          starts = Pgbus::Web::WorkflowInsightsReport.bucket_starts(since: since, now: now, bucket: bucket, zone: Time.zone)
          starts.each_with_index.to_h do |start, i|
            started = (sample[:hourly] * bucket / 3600.0 * (0.6 + (0.4 * Math.sin(i / 3.0).abs))).round
            [start, [started, (started * 0.85).round, (started * 0.05).round]]
          end
        end
      end
    end
  end
end
