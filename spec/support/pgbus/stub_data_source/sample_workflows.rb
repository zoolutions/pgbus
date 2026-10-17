# frozen_string_literal: true

module Pgbus
  module Test
    class StubDataSource
      # The Workflows pages' sample data (issue #542): one run per state with
      # a believable timeline — paused on an error (with backtrace and cause
      # chain), performing a reattempt, waiting on a signal, waiting on a
      # fan-out batch at 7 of 12, a reminder scheduled two days out, finished
      # and cancelled runs and a rerun pair — plus what the needs-attention
      # view groups: three runs paused on one error, a stalled run on an
      # older definition, an overdue step and a wait past the visibility
      # threshold. Events carry dashboard, mcp and system actors. Part of
      # fill_sample_data!.
      module SampleWorkflows
        PAUSED_ID = "0b8f5c2e-6d1a-4c39-9e7b-2f4a8c1d5e60"
        FAN_OUT_ID = "1c9a6d3f-7e2b-4d4a-8f8c-3a5b9d2e6f71"
        SIGNAL_ID = "2d0b7e4a-8f3c-4e5b-9a9d-4b6c0e3f7a82"
        PERFORMING_ID = "3e1c8f5b-9a4d-4f6c-8b0e-5c7d1f4a8b93"
        SCHEDULED_ID = "4f2d9a6c-0b5e-4a7d-9c1f-6d8e2a5b9ca4"
        FINISHED_ID = "5a3e0b7d-1c6f-4b8e-8d2a-7e9f3b6cadb5"
        CANCELLED_ID = "6b4f1c8e-2d7a-4c9f-9e3b-8fa04c7dbec6"
        RERUN_ID = "7c5a2d9f-3e8b-4dab-8f4c-9ab15d8ecfd7"
        STALLED_ID = "8d6b3eaa-4f9c-4ebc-9a5d-abc26e9fd0e8"
        OVERDUE_ID = "9e7c4fbb-5aad-4fcd-8b6e-bcd37fa0e1f9"
        PARKED_ID = "af8d5acc-6bbe-4ade-9c7f-cde48ab1f20a"
        TIMEOUT_IDS = %w[b09e6bdd-7ccf-4bef-8d8a-def59bc2031b c1af7cee-8dda-4cfa-9e9b-ef06acd3142c
                         d2b08dff-9eeb-4dab-8fac-f017bde4253d].freeze
        BATCH_ID = "e3c19ea0-af0c-4ebc-9abd-0128cef5364e"
        CHECKOUT_STEPS = %w[reserve capture receipt].freeze
        FRAUD = "Capture declined: the issuer flagged the card (risk score 91)"
        GATEWAY_BACKTRACE = ["app/workflows/checkout_workflow.rb:18:in 'block in <class:CheckoutWorkflow>'",
                             "app/services/payment_gateway/client.rb:42:in 'PaymentGateway::Client#capture'",
                             "lib/pgbus/workflow/step_execution.rb:61:in 'Pgbus::Workflow::StepExecution#run_body'"].join("\n")

        private

        def fill_sample_workflows!(now)
          @workflows_list = [sample_paused(now), sample_performing(now), sample_signal_wait(now), sample_fan_out(now),
                             sample_scheduled(now), sample_finished(now), sample_cancelled(now), sample_rerun(now),
                             sample_stalled(now), sample_overdue(now), sample_parked(now), *sample_timeouts(now),
                             *sample_exports(now)]
          @workflow_attention_groups_list = sample_attention_groups(now)
        end

        def wf_row(now, id, workflow_class, status, current_step, **attrs)
          { workflow_id: id, workflow_class: workflow_class, subject_type: "Order", subject_id: id[0, 4].to_i(16).to_s,
            status: status, current_step: current_step, current_attempt: 1, step_status: nil, waiting_for: nil,
            waiting_since: nil, timeout_at: nil, batch_id: nil, batch_progress: nil, scheduled_at: nil, error_class: nil,
            error_message: nil, paused_at: nil, finished_at: nil, cancelled_at: nil, rerun_of_workflow_id: nil,
            created_at: now - 3600, updated_at: now - 60, step_names: CHECKOUT_STEPS,
            arguments: { "currency" => "EUR", "amount_cents" => 4_999 } }.merge(attrs)
        end

        def wf_step(id, step_name, attempt, status, created, **attrs)
          scheduled = attrs.delete(:scheduled_at) || created
          { id: id, step_name: step_name, attempt: attempt, status: status, outcome: nil, created_at: created,
            scheduled_at: scheduled, enqueued_at: scheduled, started_at: nil, finished_at: nil,
            job_id: "job-#{id}", msg_id: 7_000 + id, worker: "app-1:4101", app_revision: SampleDeployTasks::NEW_REVISION,
            error_class: nil, error_message: nil, error_backtrace: nil, error_causes: [], output: nil,
            waiting_since: nil, wait_for: nil, timeout_at: nil, batch_id: nil, signal_id: nil }.merge(attrs)
        end

        def ran(id, step_name, attempt, from, seconds, **attrs)
          wf_step(id, step_name, attempt, "completed", from, started_at: from + 1, finished_at: from + 1 + seconds,
                                                             outcome: "completed", **attrs)
        end

        def wf_event(id, event, actor, created_at, step_name: nil, attempt: nil, reason: nil, details: {})
          { id: id, event: event, actor: actor, reason: reason, step_name: step_name, attempt: attempt,
            details: details, created_at: created_at }
        end

        # Paused by an operator (mcp) while capture waited for the gateway,
        # resumed from the dashboard, then capture timed out, was reattempted
        # a minute later and failed for good: two pauses on the timeline.
        def sample_paused(now)
          start = now - 5400
          wf_row(now, PAUSED_ID, "CheckoutWorkflow", "paused", "capture",
                 subject_id: "1042", current_attempt: 2, step_status: "failed", created_at: start, updated_at: now - 2400,
                 paused_at: now - 2400, error_class: "PaymentGateway::FraudSuspected",
                 error_message: FRAUD,
                 resume_action: :retry, resume_step: "capture",
                 steps: [wf_step(13, "capture", 2, "failed", now - 2770, scheduled_at: now - 2710, started_at: now - 2709,
                                                                         finished_at: now - 2400, outcome: "paused",
                                                                         error_class: "PaymentGateway::FraudSuspected",
                                                                         error_message: FRAUD,
                                                                         error_backtrace: GATEWAY_BACKTRACE,
                                                                         error_causes: [{ "class" => "Net::HTTPClientException",
                                                                                          "message" => "402 Payment Required" }]),
                         wf_step(12, "capture", 1, "failed", now - 5395, started_at: now - 2799, finished_at: now - 2770,
                                                                         outcome: "reattempted", error_class: "Timeout::Error",
                                                                         error_message: "execution expired",
                                                                         error_backtrace: GATEWAY_BACKTRACE),
                         ran(11, "reserve", 1, start, 4, output: { "reservation" => "R-88213" })],
                 events: [wf_event(8, "paused", "system", now - 2400, step_name: "capture", attempt: 2,
                                                                      details: { "error_class" => "PaymentGateway::FraudSuspected" }),
                          wf_event(7, "step_failed", "system", now - 2400, step_name: "capture", attempt: 2),
                          wf_event(6, "step_reattempted", "system", now - 2770, step_name: "capture", attempt: 1),
                          wf_event(5, "resumed", "dashboard:alice", now - 2800, reason: "Gateway is back"),
                          wf_event(4, "paused", "mcp", now - 5390, reason: "Holding while the gateway recovers"),
                          wf_event(3, "step_scheduled", "system", now - 5395, step_name: "capture", attempt: 1),
                          wf_event(2, "step_completed", "system", start + 5, step_name: "reserve", attempt: 1),
                          wf_event(1, "started", "app", start)])
        end

        def sample_performing(now)
          wf_row(now, PERFORMING_ID, "OrderWorkflow", "performing", "capture",
                 subject_id: "1043", current_attempt: 2, step_status: "in_progress", created_at: now - 900, updated_at: now - 20,
                 steps: [wf_step(23, "capture", 2, "in_progress", now - 60, started_at: now - 20),
                         wf_step(22, "capture", 1, "failed", now - 120, started_at: now - 119, finished_at: now - 90,
                                                                        outcome: "reattempted", error_class: "Net::OpenTimeout",
                                                                        error_message: "execution expired"),
                         ran(21, "reserve", 1, now - 900, 3)],
                 events: [wf_event(12, "step_started", "system", now - 20, step_name: "capture", attempt: 2),
                          wf_event(11, "step_reattempted", "system", now - 90, step_name: "capture", attempt: 1),
                          wf_event(10, "started", "app", now - 900)])
        end

        def sample_signal_wait(now)
          wf_row(now, SIGNAL_ID, "ShippingWorkflow", "waiting", "ship",
                 subject_id: "1044", step_status: "waiting", waiting_for: "shipped", waiting_since: now - 10_800,
                 timeout_at: now + 248_400, created_at: now - 11_000, updated_at: now - 10_800,
                 step_names: %w[label ship notify], signal_names: ["shipped"],
                 steps: [wf_step(33, "ship", 1, "waiting", now - 10_802, started_at: now - 10_801, waiting_since: now - 10_800,
                                                                         wait_for: "shipped", timeout_at: now + 248_400),
                         ran(32, "label", 1, now - 11_000, 6)],
                 signals: [{ id: 3, name: "label_printed", status: "consumed", idempotency_key: "evt_9f2c41",
                             payload: { "printer" => "warehouse-2" }, created_at: now - 10_990, claimed_at: now - 10_990,
                             consumed_at: now - 10_985 },
                           { id: 4, name: "address_changed", status: "pending", idempotency_key: nil,
                             payload: { "zip" => "10115" }, created_at: now - 3600, claimed_at: nil, consumed_at: nil }],
                 events: [wf_event(22, "signal_received", "mcp", now - 3600, details: { "name" => "address_changed" }),
                          wf_event(21, "step_waiting", "system", now - 10_800, step_name: "ship", attempt: 1,
                                                                               details: { "wait_for" => "shipped" }),
                          wf_event(20, "started", "app", now - 11_000)])
        end

        def sample_fan_out(now)
          wf_row(now, FAN_OUT_ID, "OrderWorkflow", "waiting", "notify_vendors",
                 subject_id: "1045", step_status: "waiting", waiting_since: now - 1500, batch_id: BATCH_ID,
                 batch_progress: { done: 7, total: 12 }, created_at: now - 1600, updated_at: now - 1500,
                 step_names: %w[reserve notify_vendors confirm],
                 steps: [wf_step(43, "notify_vendors", 1, "waiting", now - 1550, started_at: now - 1549,
                                                                                 waiting_since: now - 1500, batch_id: BATCH_ID),
                         ran(42, "reserve", 1, now - 1600, 2)],
                 events: [wf_event(32, "fan_out_opened", "system", now - 1500, step_name: "notify_vendors", attempt: 1,
                                                                               details: { "batch_id" => BATCH_ID, "size" => 12 }),
                          wf_event(31, "started", "app", now - 1600)])
        end

        def sample_scheduled(now)
          wf_row(now, SCHEDULED_ID, "SignupWorkflow", "ready", "reminder",
                 subject_type: "User", subject_id: "42", step_status: "scheduled", scheduled_at: now + 176_400,
                 created_at: now - 600, updated_at: now - 595, step_names: %w[welcome reminder],
                 arguments: { "plan" => "pro", "user" => "gid://shop/User/42" },
                 steps: [wf_step(53, "reminder", 1, "scheduled", now - 595, scheduled_at: now + 176_400),
                         ran(52, "welcome", 1, now - 600, 2)],
                 events: [wf_event(42, "step_scheduled", "system", now - 595, step_name: "reminder", attempt: 1),
                          wf_event(41, "started", "app", now - 600)])
        end

        def sample_finished(now)
          wf_row(now, FINISHED_ID, "SignupWorkflow", "finished", "reminder",
                 subject_type: "User", subject_id: "41", step_status: "completed", created_at: now - 260_000,
                 updated_at: now - 86_000, finished_at: now - 86_000, step_names: %w[welcome reminder],
                 steps: [ran(63, "reminder", 1, now - 86_005, 4, created_at: now - 259_990, scheduled_at: now - 86_005),
                         ran(62, "welcome", 1, now - 260_000, 2)],
                 events: [wf_event(52, "finished", "system", now - 86_000), wf_event(51, "started", "app", now - 260_000)])
        end

        def sample_cancelled(now)
          wf_row(now, CANCELLED_ID, "CheckoutWorkflow", "cancelled", "capture",
                 subject_id: "1040", step_status: "cancelled", created_at: now - 90_000, updated_at: now - 80_000,
                 cancelled_at: now - 80_000,
                 reruns: [{ workflow_id: RERUN_ID, status: "finished", created_at: now - 79_000 }],
                 steps: [wf_step(73, "capture", 1, "cancelled", now - 89_990, scheduled_at: now - 89_000, finished_at: now - 80_000,
                                                                              outcome: "cancelled"),
                         ran(72, "reserve", 1, now - 90_000, 5)],
                 events: [wf_event(63, "rerun", "dashboard:alice", now - 79_000, details: { "rerun_workflow_id" => RERUN_ID }),
                          wf_event(62, "cancelled", "dashboard:alice", now - 80_000, reason: "Customer cancelled by phone"),
                          wf_event(61, "started", "app", now - 90_000)])
        end

        def sample_rerun(now)
          wf_row(now, RERUN_ID, "CheckoutWorkflow", "finished", "receipt",
                 subject_id: "1040", step_status: "completed", created_at: now - 79_000, updated_at: now - 78_900,
                 finished_at: now - 78_900, rerun_of_workflow_id: CANCELLED_ID,
                 steps: [ran(83, "receipt", 1, now - 78_920, 3), ran(82, "capture", 1, now - 78_960, 12),
                         ran(81, "reserve", 1, now - 79_000, 4)],
                 events: [wf_event(72, "finished", "system", now - 78_900),
                          wf_event(71, "started", "dashboard:alice", now - 79_000, details: { "rerun_of" => CANCELLED_ID })])
        end

        def sample_stalled(now)
          wf_row(now, STALLED_ID, "ImportWorkflow", "paused", "parse",
                 subject_type: "Import", subject_id: "77", step_status: "failed", created_at: now - 9000,
                 updated_at: now - 5400, paused_at: now - 5400, definition_outdated: true,
                 error_class: "Pgbus::Workflow::StepStalled",
                 error_message: "parse attempt 1 stalled: it was still in_progress at its stall_at (worker app-3:812)",
                 step_names: %w[download parse load], resume_action: :retry, resume_step: "parse",
                 steps: [wf_step(93, "parse", 1, "failed", now - 9000, started_at: now - 8999, finished_at: now - 5400,
                                                                       outcome: "paused", worker: "app-3:812",
                                                                       app_revision: SampleDeployTasks::OLD_REVISION,
                                                                       error_class: "Pgbus::Workflow::StepStalled",
                                                                       error_message: "parse attempt 1 stalled")],
                 events: [wf_event(82, "paused", "system", now - 5400, step_name: "parse", attempt: 1),
                          wf_event(81, "stall_detected", "system", now - 5400, step_name: "parse", attempt: 1,
                                                                               details: { "detection" => "sweep" })])
        end

        def sample_overdue(now)
          wf_row(now, OVERDUE_ID, "SignupWorkflow", "ready", "welcome",
                 subject_type: "User", subject_id: "57", step_status: "scheduled", scheduled_at: now - 7200,
                 created_at: now - 7200, updated_at: now - 7200, step_names: %w[welcome reminder],
                 steps: [wf_step(103, "welcome", 1, "scheduled", now - 7200)],
                 events: [wf_event(92, "started", "app", now - 7200)])
        end

        def sample_parked(now)
          wf_row(now, PARKED_ID, "ShippingWorkflow", "waiting", "ship",
                 subject_id: "998", step_status: "waiting", waiting_for: "shipped", waiting_since: now - 777_600,
                 created_at: now - 778_000, updated_at: now - 777_600, step_names: %w[label ship notify],
                 signal_names: ["shipped"],
                 steps: [wf_step(113, "ship", 1, "waiting", now - 777_602, started_at: now - 777_601,
                                                                           waiting_since: now - 777_600, wait_for: "shipped")],
                 events: [wf_event(102, "started", "app", now - 778_000)])
        end

        def sample_timeouts(now)
          TIMEOUT_IDS.each_with_index.map do |id, i|
            failed_at = now - 1200 + (i * 300)
            wf_row(now, id, "CheckoutWorkflow", "paused", "capture",
                   subject_id: (1050 + i).to_s, step_status: "failed", created_at: failed_at - 30, updated_at: failed_at,
                   paused_at: failed_at, error_class: "Timeout::Error", error_message: "execution expired",
                   resume_action: :retry, resume_step: "capture",
                   steps: [wf_step(120 + i, "capture", 1, "failed", failed_at - 30, started_at: failed_at - 29,
                                                                                    finished_at: failed_at, outcome: "paused",
                                                                                    error_class: "Timeout::Error",
                                                                                    error_message: "execution expired",
                                                                                    error_backtrace: GATEWAY_BACKTRACE)],
                   events: [wf_event(110 + i, "paused", "system", failed_at, step_name: "capture", attempt: 1)])
          end
        end

        # Enough finished runs that ?status=all has a second page.
        def sample_exports(now)
          Array.new(SampleData::PAGED_ROWS - 14) do |i|
            day = now - (86_400 * (i + 2))
            wf_row(now, format("f%07d-0000-4000-8000-%012d", i, i), "NightlyExportWorkflow", "finished", "upload",
                   subject_type: nil, subject_id: nil, step_status: "completed", created_at: day, updated_at: day + 900,
                   finished_at: day + 900, step_names: %w[collect upload], arguments: { "date" => (day.to_date - 1).iso8601 },
                   steps: [ran(1_000 + (i * 2), "upload", 1, day + 600, 290), ran(999 + (i * 2), "collect", 1, day, 590)],
                   events: [wf_event(1_000 + i, "finished", "system", day + 900)])
          end
        end

        def sample_attention_groups(now)
          [
            { workflow_class: "CheckoutWorkflow", step_name: "capture", reason: "paused_error", error_class: "Timeout::Error",
              count: 3, first_seen_at: now - 1200, last_seen_at: now - 600, error_message: "execution expired" },
            { workflow_class: "CheckoutWorkflow", step_name: "capture", reason: "paused_error",
              error_class: "PaymentGateway::FraudSuspected", count: 1, first_seen_at: now - 2400, last_seen_at: now - 2400,
              error_message: FRAUD },
            { workflow_class: "ImportWorkflow", step_name: "parse", reason: "stalled", error_class: "Pgbus::Workflow::StepStalled",
              count: 1, first_seen_at: now - 5400, last_seen_at: now - 5400, error_message: "parse attempt 1 stalled" },
            { workflow_class: "SignupWorkflow", step_name: "welcome", reason: "overdue", error_class: nil, count: 1,
              first_seen_at: now - 7200, last_seen_at: now - 7200, error_message: nil },
            { workflow_class: "ShippingWorkflow", step_name: "ship", reason: "waiting_overdue", error_class: nil, count: 1,
              first_seen_at: now - 777_600, last_seen_at: now - 777_600, error_message: nil }
          ]
        end
      end
    end
  end
end
