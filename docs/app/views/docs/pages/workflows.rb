# frozen_string_literal: true

# Durable multi-step workflows: Pgbus::Workflow (issue #540).
class Views::Docs::Pages::Workflows < DocsUI::Page
  title "Workflows"
  eyebrow "Guide"

  def lead = "Sequential, durable steps that span hours or days, with waits, flow control, exception policies and an audit trail."

  def content
    install
    defining
    flow_control
    exceptions
    idempotency
    removing_steps
    housekeeping
    observability
    testing
    how_it_works
    transitions
  end

  private

  def install
    DocsUI::Section("Install") do
      md <<~'MD'
        Workflows keep their state in three tables. A fresh `pgbus:install`
        creates them; an existing install adds them with the generator (or
        `rails generate pgbus:update`, which detects the missing tables):
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:add_workflows               # or --database=pgbus for a separate database
        rails db:migrate
      SHELL
    end
  end

  def defining
    DocsUI::Section("Define and start a workflow") do
      md <<~'MD'
        A workflow is a class with declared steps. Each step runs once per
        attempt, in order, as its own job; `wait:` delays it after the one
        before. Inside a step, `subject` is the record the run was started
        for and `arguments` the keywords `start!` received.
      MD
      DocsUI::Code(<<~'RUBY', filename: "app/workflows/signup_workflow.rb")
        class SignupWorkflow < Pgbus::Workflow
          cancel_if { subject.nil? || subject.closed? }
          on_exception Net::OpenTimeout, action: :reattempt!, wait: 15.seconds, max_reattempts: 5

          step :welcome do
            WelcomeMailer.with(user: subject, plan: arguments[:plan]).welcome.deliver_later
          end

          step :reminder, wait: 2.days, skip_if: -> { subject.onboarded? } do
            ReminderMailer.with(user: subject).remind.deliver_later
          end

          step :complete, wait: 7.days do
            subject.update!(onboarding_completed: true)
          end
        end

        wf = SignupWorkflow.start!(user, plan: "pro")
        wf.status        # => "ready"
        wf.workflow_id   # => "6f1c…"
      RUBY
      md <<~'MD'
        Step forms: `step :name do … end`, an anonymous `step { … }` (named
        `step_1`, `step_2`, …), `step :name` to run the instance method of that
        name, and `step def name = …`. Options, all checked when the class
        loads:

        - `wait:` — seconds or a Duration after the previous step (for the
          first step, after `start!`). Underneath it is a PGMQ delay: the
          StepJob is sent with `wait_until:`.
        - `skip_if:` — a lambda or method name; truthy skips the step.
        - `timeout:` — how long the step may stay in progress before the sweep
          treats it as stalled (default `config.workflow_stall_threshold`).
        - `job_options:` — `queue`, `priority`, `wait`, `wait_until` for this
          step's StepJob; `set_step_job_options` sets them for the class.
          `wait`/`wait_until` only ever move a step later: they are folded
          into its `scheduled_at` when it is scheduled. Anything else raises.
        - `on_exception:` — a step-level exception policy (below).
        - `before_step:` / `after_step:` — insert the step before or after
          another (useful in a subclass).

        `cancel_if` runs before every step; truthy cancels the run. Both it
        and `skip_if` run under the workflow's row lock, before the body.

        The subject is optional and loaded through GlobalID; a record that is
        gone makes `subject` return `nil` (react in `cancel_if`). Arguments go
        through `ActiveJob::Arguments` (GlobalID-aware, never Marshal) and
        are capped at `config.workflow_max_payload_size` (128 KB). A subject
        has at most one ongoing run per workflow class: a second `start!`
        raises `Pgbus::Workflow::AlreadyRunning`, whose `#workflow` is the run
        holding the slot. Subject-less runs are never limited.

        `start!` writes the run and its first step row in one transaction; the
        StepJob is sent by the step row's after-commit callback, so calling
        `start!` inside your own transaction sends nothing until it commits
        (and nothing at all if it rolls back) when the workflow tables share
        your database. On a separate pgbus database (`connects_to`) the commit
        is the pgbus connection's: the run is written and its first step sent
        on their own, so a step can run before your transaction commits the
        subject (and even if it rolls back). Wait for the subject in the step
        body — `reattempt!(wait: 5) if subject.nil?` — rather than in
        `cancel_if`, which cancels the run.
      MD
    end
  end

  def flow_control
    DocsUI::Section("Flow control") do
      md <<~'MD'
        Inside a step body, each verb ends the body on the spot:

        - `pause!` — the step completes and the run pauses; `resume!` continues
          with the next step.
        - `cancel!` — the run is cancelled.
        - `reattempt!(wait: 30)` — this attempt fails and a new one is scheduled.
        - `skip!` — the step is skipped and the run moves on.
        - `finished!` — the step completes and the run finishes now.

        From outside (the console, your code; the dashboard later), every verb
        takes `actor:` and `reason:` for the audit trail:
      MD
      DocsUI::Code(<<~'RUBY')
        wf = SignupWorkflow.find(workflow_id)
        wf.pause!(actor: "alice", reason: "SMTP is down")
        wf.resume!(actor: "alice", reason: "SMTP fixed")   # a failed step runs again; a completed one is followed by the next
        wf.resume_at!(:reminder)                            # paused → run :reminder next
        wf.run_now!                                         # the waiting step runs now
        wf.reschedule!(at: 1.hour.from_now)
        wf.cancel!(reason: "customer asked")
        wf.rerun!                                           # a finished or cancelled run, started again
        wf.available_actions                                # => [:pause, :cancel, :run_now, :reschedule]
      RUBY
      md <<~'MD'
        A verb that is not legal in the run's state raises
        `Pgbus::Workflow::InvalidTransition` (`NotPaused` for `resume!` and
        `resume_at!`); the table at the bottom of this page is the rule.
      MD
    end
  end

  def exceptions
    DocsUI::Section("Exceptions") do
      md <<~'MD'
        An exception in a step body never reaches ActiveJob. The step row
        records the error (class, message, cleaned backtrace, cause chain,
        the worker's `hostname:pid` and the app revision), the error goes to
        `Pgbus::ErrorReporter`, and by default the run pauses. A policy says
        otherwise:
      MD
      DocsUI::Code(<<~'RUBY')
        class OrderWorkflow < Pgbus::Workflow
          on_exception PaymentGateway::Transient, action: :reattempt!, wait: 30.seconds,
                                                  max_reattempts: 5, terminal_action: :cancel!
          on_exception PaymentGateway::Declined, action: :cancel!, report: :never
          on_exception action: :pause!                           # blanket: every other StandardError

          on_exception Timeout::Error do |error|                 # imperative: call a verb, or the run pauses
            error.message.include?("read") ? reattempt!(wait: 5) : skip!
          end

          step :capture, on_exception: { matching: "Faraday::TimeoutError", action: :reattempt! } do
            PaymentGateway.capture(subject)
          end
        end
      RUBY
      md <<~'MD'
        Resolution order: the step's own policies, then class-level policies
        that name the error, then blanket ones, then `pause!`. `matching:`
        takes classes, class names or a list of either. `max_reattempts:`
        counts consecutive reattempted attempts of the step; once spent,
        `terminal_action:` (default `pause!`) applies. `report:` is `:always`
        (default), `:never`, or `:terminal_only` (not on a reattempt that
        still has attempts left). A plain `rescue` in the body bypasses the
        policies: your code decides.

        To the executor the StepJob always succeeds, whatever the step's
        outcome; job stats count it as completed. The step row and the
        `pgbus.workflow.*` events are the truth about the step.
      MD
    end
  end

  def idempotency
    DocsUI::Section("Idempotency") do
      md <<~'MD'
        A step can run more than once: a worker that dies mid-step leaves
        part of the body done, and the stall action may run it again. Four
        things make that safe:
      MD
      DocsUI::Code(<<~'RUBY')
        class CheckoutWorkflow < Pgbus::Workflow
          step :charge do
            charge = PaymentGateway.charge(subject, idempotency_key: step_idempotency_key)  # same key on every attempt
            output(charge_id: charge.id)                                                    # durable once the step completes
          end

          step :receipt do
            ReceiptMailer.with(charge_id: output_of(:charge)[:charge_id]).receipt.deliver_later
          end
        end

        CheckoutWorkflow.start!(order, idempotency_key: "checkout-#{order.id}")  # a retried webhook gets the same run back
      RUBY
      md <<~'MD'
        - **Outputs.** `output(**values)` records values on the attempt, in
          the same UPDATE that completes the step, so they exist exactly when
          the step does. `output_of(:name)` returns the latest completed
          attempt's output (with indifferent access), or `nil`; `outputs`
          returns all of them. A failed attempt keeps its output on its own
          row for debugging, but `output_of` never returns it. Outputs are
          capped like arguments; an unserialisable value raises inside the
          step and goes through the policies. The body's return value is
          ignored.
        - **Step keys.** `step_idempotency_key` is
          `"pgbus-wf:<workflow_id>:<step>"`, the same on every attempt and on a
          `resume_at!` re-run: hand it to the external API so the side effect
          happens once. `attempt_idempotency_key` adds `:<attempt>` for the
          rare call that must differ per attempt.
        - **Idempotent start.** `start!(…, idempotency_key:)` returns the run
          that already has that key, in whatever status, with
          `duplicate_start?` true and no new step. The key is a reserved
          keyword. A different key for a subject with an ongoing run still
          raises `AlreadyRunning`.
        - **Completed steps stay completed.** `resume!` and the policies never
          re-run a completed step; only `resume_at!` does, on purpose.

        The engine's own messages are idempotent too: the claim (scheduled →
        in_progress, only once due) makes a duplicate StepJob a no-op — except
        a redelivery of the very message that claimed a still-running step,
        which applies the stall action (see Housekeeping) — and the
        unique `(workflow_id, step_name, attempt)` index makes a duplicate
        attempt impossible. A step still has to be idempotent: the key is how.
      MD
    end
  end

  def removing_steps
    DocsUI::Section("Renaming and removing steps") do
      md <<~'MD'
        A run that is scheduled on a step the class no longer declares pauses
        with `Pgbus::Workflow::StepNotDefined`. To retire a step, replace it
        with a gravestone where it used to be:
      MD
      DocsUI::Code(<<~'RUBY')
        class SignupWorkflow < Pgbus::Workflow
          step(:welcome) { … }
          removed_step :reminder      # a run still on :reminder skips it and moves on to :complete
          step(:complete, wait: 7.days) { … }
        end

        SignupWorkflow.removed_steps_in_flight   # => { "reminder" => 0 }: safe to delete the gravestone
      RUBY
      md <<~'MD'
        Every run stores the SHA256 of its class's ordered step list when it
        starts (`definition_digest`); `Pgbus::WorkflowEntry#definition_outdated?`
        tells a run that began on an older definition.
      MD
    end
  end

  def housekeeping
    DocsUI::Section("Housekeeping") do
      md <<~'MD'
        The dispatcher looks after workflows on its own:

        - **A dead worker is noticed in seconds**, while
          `config.visibility_heartbeat` is on (the default). StepJob keeps the
          heartbeat, so a live worker keeps its message invisible; when the
          very message that claimed a step comes back while the row is still
          in progress, its worker died (or its heartbeat lapsed, for example
          through a long pause), and `config.workflow_stall_action` applies at
          once. With the heartbeat off, only the sweep below catches a dead
          worker.
        - **A hung step** (in progress past its `timeout:`, or past
          `config.workflow_stall_threshold`) gets the same stall action from
          the sweep, every `config.workflow_sweep_interval`. `:pause` (default)
          records `Pgbus::Workflow::StepStalled`, `:reattempt` schedules a new
          attempt, `:cancel` cancels the run.
        - **A lost message** — a scheduled step of a ready run, due more than
          the threshold ago and not sent within it — is sent again.
        - **Retention**: finished and cancelled runs older than
          `config.workflow_retention` (30 days) are deleted hourly, with their
          steps and events.

        A step that was declared stalled while it was in fact still running
        finds its row no longer in progress when it ends: its outcome and
        output are discarded and a `late_completion_ignored` event says so.
      MD
      DocsUI::Table(
        [ "Setting", "Default", "What it does" ],
        [
          [ [ :code, "workflow_queue" ], [ :code, "\"default\"" ], "Queue of the StepJob messages." ],
          [ [ :code, "workflow_retention" ], [ :code, "30.days" ], "Finished/cancelled runs kept this long; nil keeps them." ],
          [ [ :code, "workflow_sweep_interval" ], [ :code, "5.minutes" ], "How often the sweep runs." ],
          [ [ :code, "workflow_stall_threshold" ], [ :code, "1.hour" ], "When an in-progress step is stalled, or a scheduled one overdue." ],
          [ [ :code, "workflow_stall_action" ], [ :code, ":pause" ], [ :md, "`:pause`, `:reattempt` or `:cancel`." ] ],
          [ [ :code, "workflow_max_payload_size" ], [ :code, "131_072" ], "Byte cap on arguments and on each step output." ],
          [ [ :code, "workflow_error_backtrace_lines" ], [ :code, "50" ], "Backtrace lines kept on a failed step." ]
        ]
      )
    end
  end

  def observability
    DocsUI::Section("Observability") do
      md <<~'MD'
        **Audit trail.** Every transition and every outside verb writes a
        `pgbus_workflow_events` row in the same transaction, with its actor
        (`"app"` by default, `"system"` for the StepJob and the sweep, or the
        `actor:` you pass) and reason. `wf.events` returns them in order:
        `started`, `step_scheduled`, `step_started`, `step_completed`,
        `step_failed`, `step_skipped`, `step_reattempted`, `paused`,
        `resumed`, `cancelled`, `finished`, `run_now`, `rescheduled`, `rerun`,
        `stall_detected`, `late_completion_ignored`.

        **Metrics** reach Prometheus/StatsD and AppSignal with the same names:
      MD
      DocsUI::Table(
        [ "Metric", "Kind", "Labels" ],
        [
          [ [ :code, "pgbus_workflow_step_duration_seconds" ], "histogram", "workflow_class, step, outcome" ],
          [ [ :code, "pgbus_workflow_steps_total" ], "counter", "workflow_class, step, outcome" ],
          [ [ :code, "pgbus_workflows_finished_total" ], "counter", "workflow_class" ],
          [ [ :code, "pgbus_workflows_paused_total" ], "counter", "workflow_class, error_class" ],
          [ [ :code, "pgbus_workflows_cancelled_total" ], "counter", "workflow_class" ],
          [ [ :code, "pgbus_workflow_stalls_total" ], "counter", "workflow_class, step, detection" ],
          [ [ :code, "pgbus_workflows" ], "gauge", "workflow_class, status" ],
          [ [ :code, "pgbus_workflows_stuck" ], "gauge", "workflow_class, reason" ]
        ]
      )
      md <<~'MD'
        **Stuck reasons** (the `pgbus_workflows_stuck` gauge): `paused_error`
        (paused on a step that failed), `stalled` (paused on `StepStalled`),
        `paused_manual` (the step called `pause!`, or an outside `pause!`),
        `overdue` (a scheduled step nobody picked up).

        **Errors** reach `Pgbus::ErrorReporter` with
        `{ workflow_class:, workflow_id:, step_name:, attempt:, outcome: }` as
        context. **Logs**: while a step runs, Pgbus's own log lines carry
        `workflow=… step=… attempt=…` through `Pgbus::LogFormatter`, and your
        code reads `Pgbus::Workflow::StepContext.workflow_id` (and `step_name`,
        `attempt`) to tag its own. The instrumentation events are
        `pgbus.workflow.step`, `pgbus.workflow_finished`,
        `pgbus.workflow_paused`, `pgbus.workflow_cancelled`,
        `pgbus.workflow.stall_detected` and `pgbus.workflow.gauge`.
      MD
    end
  end

  def testing
    DocsUI::Section("Testing") do
      md <<~'MD'
        `Pgbus::Workflow::TestHelpers` runs steps inline against your test
        database, ignoring their `wait:`, through the real StepJob:
      MD
      DocsUI::Code(<<~'RUBY', filename: "spec/workflows/signup_workflow_spec.rb")
        RSpec.describe SignupWorkflow do
          include Pgbus::Workflow::TestHelpers

          it "onboards the user" do
            wf = SignupWorkflow.start!(user)

            perform_next_step(wf)            # runs :welcome
            run_step_inline(wf, :reminder)   # raises if another step is next
            speedrun_workflow(wf)            # runs steps until the run stops being ready

            expect(wf.status).to eq("finished")
          end
        end
      RUBY
      md <<~'MD'
        `speedrun_workflow` stops at `paused`, `finished` and `cancelled`, and
        raises on `waiting` (nothing inline can deliver what the run waits for).
      MD
    end
  end

  def how_it_works
    DocsUI::Section("How it works") do
      md <<~'MD'
        - `pgbus_workflows` — one row per run (`Pgbus::WorkflowEntry`): the
          class, the subject, the arguments, the status and the current step.
          A partial unique index keeps one ongoing run per class and subject;
          another dedupes `idempotency_key`s.
        - `pgbus_workflow_steps` — one row per step attempt
          (`Pgbus::WorkflowStep`), unique on `(workflow_id, step_name,
          attempt)`, with the timings, the worker, the app revision, the
          output and the error.
        - `pgbus_workflow_events` — the append-only audit trail
          (`Pgbus::WorkflowEvent`).

        Every StepJob is sent by `WorkflowStep`'s after-commit callback, and
        only there: a new scheduled row, a row going back to scheduled, or a
        bumped `enqueued_at` (how the sweep and `resume!` re-send). A StepJob
        claims its row (scheduled → in_progress, only once due) and the run
        (ready → performing) under the run's row lock, runs the guards and
        the body, then applies the outcome under the lock again: one step at
        a time per run, every change a guarded UPDATE from the table below.
      MD
    end
  end

  def transitions
    DocsUI::Section("The transition table") do
      md <<~'MD'
        Pgbus::Workflow::States is the one rule for which verb is legal in
        which state, for the runner, `available_actions` and this page. The
        outside verbs:
      MD
      DocsUI::Table([ "Verb", "From", "To", "Steps" ], WorkflowStatesReference.rows(WorkflowStatesReference::OUTSIDE))
      md <<~'MD'
        The StepJob's and the sweep's own moves, never offered as actions:
      MD
      DocsUI::Table([ "Verb", "From", "To", "Steps" ], WorkflowStatesReference.rows(WorkflowStatesReference::RUNNER))
      md <<~'MD'
        Workflow states: `ready`, `performing`, `waiting`, `paused` (the four
        ongoing ones), `finished`, `cancelled`. Step states: `scheduled`,
        `waiting`, `in_progress`, `completed`, `failed`, `cancelled`,
        `skipped`. Nothing enters `waiting` yet: fan-out steps and signals
        arrive with the waiting-steps release.
      MD
    end
  end
end
