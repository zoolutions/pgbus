# frozen_string_literal: true

# One DataSource#job_rows hash for the unified Jobs list, shared by the Jobs
# and queue pages. The including spec defines `now`.
module JobRowBuilder
  def job_row(state, **attrs)
    { source: "queue", id: attrs[:msg_id] || 1, queue_name: "pgbus_default", logical_queue: "default",
      job_class: "#{state.capitalize}Job", read_ct: 0, enqueued_at: now - 60, last_read_at: nil,
      vt: now - 1, state: state, error_class: nil, error_message: nil, failed_event_id: nil,
      concurrency_key: nil, slots_held: nil, slots_max: nil,
      payload: { job_class: "#{state.capitalize}Job", job_id: "job-#{state}", arguments: [42] }.to_json,
      headers: nil }.merge(attrs)
  end
end

RSpec.configure { |config| config.include JobRowBuilder, type: :system }
