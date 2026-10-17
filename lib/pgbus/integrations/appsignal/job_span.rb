# frozen_string_literal: true

require "pgbus/integrations/appsignal/span_subscriber"

module Pgbus
  module Integrations
    module Appsignal
      # The `<JobClass>#perform` transaction around pgbus.executor.execute
      # (deserialize + perform + archive), and the fallback for an error
      # pgbus.job_failed reports from outside it.
      class JobSpan < SpanSubscriber
        private

        def frame_key = :pgbus_appsignal_job_span

        def describe(transaction, payload)
          transaction.set_action_if_nil("#{payload[:job_class] || "UnknownJob"}#perform")
          apply_queue_start(transaction, payload[:enqueued_at])
          transaction.add_tags(job_tags(payload))
          transaction.add_params_if_nil { { arguments: payload[:arguments] } } if payload.key?(:arguments)
        end

        def finished(payload, duration)
          ::Appsignal.add_distribution_value(
            "pgbus_job_duration_ms",
            duration,
            { queue: payload[:queue], job_class: payload[:job_class] }
          )
        end

        def job_tags(payload)
          tags = {
            "queue" => payload[:queue],
            "job_class" => payload[:job_class],
            "attempts" => payload[:read_ct]
          }
          tags["active_job_id"] = payload[:job_id] if payload[:job_id]
          tags["provider_job_id"] = payload[:provider_job_id] if payload[:provider_job_id]
          tags["request_id"] = payload[:provider_job_id] || payload[:job_id]
          tags.compact
        end
      end
    end
  end
end
