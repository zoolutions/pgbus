# frozen_string_literal: true

# Durable multi-step workflows: Pgbus::Workflow (issues #540, #541, #542, #549).
class Views::Docs::Pages::Workflows < DocsUI::Page
  title "Workflows"
  eyebrow "Guide"

  def lead = "Sequential, durable steps that span hours or days, with waits, flow control, exception policies and an audit trail."

  def content
    install
    defining
    flow_control
    fan_out
    signals
    exceptions
    idempotency
    removing_steps
    housekeeping
    operating
    monitoring
    observability
    testing
    how_it_works
    transitions
  end

  private

  def install
    DocsUI::Section("Install") do
      md <<~'MD'
        Workflows keep their state in four tables. A fresh `pgbus:install`
        creates them; an existing install adds them with the generator (or
        `rails generate pgbus:update`, which detects the missing tables):
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:add_workflows               # or --database=pgbus for a separate database
        rails db:migrate
      SHELL
      md <<~'MD'
        An install that ran `pgbus:add_workflows` before waiting steps existed
        adds them (the `pgbus_workflow_signals` table and five step columns)
        with `rails generate pgbus:add_workflow_signals`; `pgbus:update`
        detects that too. `start!` of a class with a `fan_out` or `wait_for:`
        step raises `Pgbus::Workflow::WaitingStepsUnmigrated` until it has run.
      MD
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
        - `wait_for:`, `wait_timeout:`, `on_timeout:` — wait for a signal
          (below).
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
        takes `actor:` and `reason:` for the audit trail. Outside a step,
        `skip!` skips the step a waiting run waits on:
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
        wf.skip!(actor: "alice", reason: "picked up in store")  # waiting → skip the step it waits on
        wf.available_actions                                # => [:pause, :cancel, :run_now, :reschedule, :signal]
      RUBY
      md <<~'MD'
        A verb that is not legal in the run's state raises
        `Pgbus::Workflow::InvalidTransition` (`NotPaused` for `resume!` and
        `resume_at!`); the table at the bottom of this page is the rule.
      MD
    end
  end

  def fan_out
    DocsUI::Section("Fan-out steps") do
      md <<~'MD'
        A `fan_out` step adds a group of jobs and waits for all of them, as
        one batch:
      MD
      DocsUI::Code(<<~'RUBY')
        fan_out :notify_vendors, on_batch_failure: :pause! do |b|
          b.add(*subject.vendors.map { |vendor| NotifyVendorJob.new(vendor.id, "#{step_idempotency_key}:#{vendor.id}") })
        end
      RUBY
      md <<~'MD'
        The body adds job instances with `b.add`, as often as it likes (each
        keeps its own `.set(queue:, wait:, priority:)`; a block given to
        `b.add` runs inside the batch, so a `perform_later` in it joins too).
        When the body returns, one batch opens with those jobs, described
        `OrderWorkflow#notify_vendors <workflow_id>` and carrying
        `{ workflow_id, step_id }` as its properties, so it shows on the
        Batches page. The step is `waiting`, and so is the run; no message is
        sent for it.

        When every member has finished (completed or dead-lettered), the
        batch's `on_complete` callback, `Pgbus::Workflow::BatchFinishedJob`
        on the step's StepJob queue, resumes the run: with no dead-lettered
        member the step completes and the next one is scheduled; with any,
        `on_batch_failure` applies (`:pause!` by default, `:cancel!` or
        `:skip!`) and `Pgbus::Workflow::FanOutFailed` ("3 of 12 jobs
        dead-lettered") is recorded on the step. A batch that stalls is
        repaired by the batch sweep, whose finish fires the same callback; a
        callback that is lost (dead-lettered, or never sent) is made up by the
        workflow sweep once its batch has been finished for
        `config.workflow_stall_threshold`. If the batch row itself is gone
        (batch retention ran before a long-delayed callback), its outcome is
        unknown: the step fails with `FanOutFailed` and the run **pauses**,
        whatever `on_batch_failure` says, for a person to resume or skip.

        - The workflow owns the batch's callbacks: `b.on_complete`,
          `b.on_success` and `b.on_failure` raise `ArgumentError` in the body.
        - A flow-control verb (`skip!`, `cancel!`, …) or an exception in the
          body ends the step before any batch exists. Members join only
          through `b.add`: a bare `perform_later` in the body is not a member.
        - An empty batch finishes at once; the step completes without waiting.
        - **A reattempt, or `resume!` after a failure, runs the whole body
          again and opens a fresh batch.** Every member runs again, so member
          jobs must be idempotent: derive each member's key from
          `step_idempotency_key` (stable across attempts) plus something that
          tells the members apart, such as the vendor id.
        - `fan_out` takes every `step` option except `wait_for:`,
          `wait_timeout:` and `on_timeout:`, and needs the batch callback-job
          columns (`rails generate pgbus:add_batch_callback_jobs`): without
          them `start!` raises `Pgbus::Workflow::BatchCallbacksUnmigrated`.
      MD
    end
  end

  def signals
    DocsUI::Section("Signals") do
      md <<~'MD'
        A `wait_for:` step waits for an event from outside, delivered with
        `signal!`:
      MD
      DocsUI::Code(<<~'RUBY')
        class OrderWorkflow < Pgbus::Workflow
          step :capture, wait_for: :payment_confirmed, wait_timeout: 2.days, on_timeout: :pause! do
            PaymentGateway.capture(subject, amount: received_signal.payload["amount"])
          end
        end

        wf.signal!(:payment_confirmed, payload: { amount: 42 }, idempotency_key: event.id)

        # A webhook that knows the order, not the run:
        OrderWorkflow.signal!(order, :payment_confirmed, payload: { amount: 42 },
                              idempotency_key: event.id, missing: :ignore)
      RUBY
      md <<~'MD'
        The step reaches its gate after its `wait:` and its guards. It takes
        the oldest pending signal its matcher takes and runs with it as
        `received_signal`; with none it parks as `waiting` and nothing is
        sent. `signal!` stores the signal under the run's row lock and, when
        the run's step is waiting for one it matches, claims it and schedules
        the step; its StepJob is sent once the signal commits (so a `signal!`
        inside your transaction wakes nothing before it commits), into a queue
        the workers already LISTEN on, so the step runs at once.
      MD
      DocsUI::Table(
        [ "Signal status", "Means" ],
        [
          [ [ :code, "pending" ], "Stored, taken by no step: it arrived before the step waited, or no waiting step matches it." ],
          [ [ :code, "claimed" ], [ :md, "A step took it and runs with it (`claimed_by_step_id`)." ] ],
          [ [ :code, "consumed" ], "The step that took it ended." ]
        ]
      )
      md <<~'MD'
        - **Buffering.** A signal sent before its step waits stays pending
          and is taken when the step reaches its gate.
        - **Idempotency.** With an `idempotency_key`, a redelivery stores
          nothing, wakes nothing and returns the stored signal, whose
          `duplicate_delivery?` is true.
        - **Matchers.** `wait_for:` takes a signal name, a
          `Pgbus::Workflow::SignalMatcher.new(:shipped) { |payload| payload[:region] == "eu" }`,
          or any object answering `matches?(signal)`. A matcher runs under the
          run's row lock, so it must be fast and free of side effects. One
          that raises at the gate is handed to the exception policies; in
          `signal!` the error reaches the caller and nothing is stored.
        - **Payloads** go through `ActiveJob::Arguments`, are capped at
          `config.workflow_max_payload_size`, and read back with indifferent
          access (`received_signal.payload["amount"]`).
        - **A finished or cancelled run** raises `Pgbus::Workflow::NotOngoing`.
          By subject, `missing: :raise` (default) raises it when the subject
          has no ongoing run of the class; `missing: :ignore` returns `nil`. A
          subject-less run is signalled through its handle
          (`OrderWorkflow.find(workflow_id).signal!`).
        - **Lifecycle.** A claimed signal is consumed when its step ends, and
          kept when the attempt reattempts or fails into a pause: the next
          attempt (`reattempt!`, `resume!`, or `resume_at!` of that step)
          runs with the same signal, without matching again.
        - **A paused run** still takes a signal for its waiting step, but stays
          paused; `resume!` runs the step. `resume!` of a run paused while it
          waited puts it back to waiting.

        **Timeouts.** A signal that never comes is the usual way a workflow
        gets stuck, so a waiting step can carry a deadline:
        `wait_timeout:` (seconds or a Duration) and `on_timeout:`, one of `:pause!`
        (default: the step fails with `Pgbus::Workflow::WaitTimedOut`),
        `:cancel!`, `:skip!` or `:run` (the body runs with `received_signal`
        nil and `timed_out?` true). `timeout:` keeps its meaning on every
        step: it bounds the in-progress attempt from its claim, while `wait_timeout:`
        bounds the wait before it. The sweep fires timeouts, so one fires
        within `config.workflow_sweep_interval` of its deadline. A paused run's
        wait does not time out while it is paused, and the time spent paused
        does not count against it: `resume!` moves the deadline by the length
        of the pause. A signal that arrives after the timeout fired stays
        pending.

        **A wait without a timeout** has three escape hatches: `skip!` (the
        run moves on), `cancel!`, or `pause!` then `resume_at!`. One waiting
        longer than `config.workflow_waiting_visibility_threshold` (7 days)
        counts as stuck (`waiting_overdue`).
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
        - **A wait past its `wait_timeout:`** gets its `on_timeout:` (see
          Signals). A waiting step is never treated as stalled.
        - **A lost fan-out callback** — a `fan_out` step still waiting on a
          batch that finished more than the threshold ago — is resolved as
          its `BatchFinishedJob` would have. One whose batch row is gone
          pauses with `FanOutFailed` (the outcome is unknown).
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
          [ [ :code, "workflow_error_backtrace_lines" ], [ :code, "50" ], "Backtrace lines kept on a failed step." ],
          [ [ :code, "workflow_waiting_visibility_threshold" ], [ :code, "7.days" ],
            [ :md, "A wait without a `wait_timeout:` this long counts as stuck (`waiting_overdue`)." ] ]
        ]
      )
    end
  end

  def operating
    DocsUI::Section("Operating") do
      md <<~'MD'
        The dashboard's **Workflows** page (`/pgbus/workflows`) lists every
        run, newest first: the ongoing ones by default, or one state, or all.
        Each row says in words what the run is doing and what happens next:
        "Performing `capture` (attempt 2)", "Waiting for signal `shipped`
        since 3h ago", "Waiting for batch `OrderWorkflow#notify_vendors` (7 of
        12 jobs done)", "Paused on `capture`: Timeout::Error — resume or
        cancel", "Scheduled: `reminder` runs in 2d". Filter by class, current
        step, error class, subject, workflow id and a created-at range; the
        filters combine and survive paging.

        A run's page shows its arguments, a **timeline** (one range bar per
        attempt, split into waited, queued and ran, coloured by how it ended,
        with waits and pauses as their own segments; zoom for runs that span
        days), its newest 100 attempts with the same durations as text, its worker, app
        revision, error, backtrace and cause chain, the signals it received,
        and the audit trail ("Resumed by dashboard:alice: SMTP fixed"). A run
        that started on an older step list says so.

        **Actions** show only when `available_actions` allows them: Resume
        (its label says whether it retries the failed step or continues with
        the next), Resume at a chosen step, Run now (a scheduled step's wait
        cut short), Skip step (a waiting step), Send signal (a JSON object;
        the form's idempotency key makes a double submit send one signal),
        Rerun (finished or cancelled runs; the new run links back) and Cancel.
        Each takes an optional reason and reports the real outcome: when the
        run moved on first, the flash says where it is now.

        **Needs attention** (`/pgbus/workflows/attention`) groups every stuck
        run by class, step, stuck reason and error class, with a count, when
        the group was first and last seen and the newest error message (the
        200 biggest groups; past that the page is not exhaustive). Once
        the cause is fixed, Resume or Cancel the whole group (at most 500 runs
        per press, each through its own guarded transition): "Resumed 212; 2
        had already moved on". The home page counts ongoing, paused and stuck
        runs.

        Two settings connect the page to your app:
      MD
      DocsUI::Code(<<~'RUBY', filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          # Who a dashboard action is recorded as on the audit trail:
          # "dashboard:<name>", or "dashboard" when this returns nil.
          config.web_actor = ->(request) { request.env["warden"]&.user&.email }

          # Where the subject links to. Only an http(s) URL or a path on this
          # site is linked; anything else, or a lambda that raises, is text.
          config.workflow_subject_url = ->(type, id) { "/admin/#{type.underscore.pluralize}/#{id}" }
        end
      RUBY
      md <<~'MD'
        The read-only MCP server has the same views: `pgbus_workflows` (the
        list, with the page's filters), `pgbus_workflow` (one run, with
        `available_actions`) and `pgbus_workflows_attention`. Arguments, step
        outputs, signal payloads and error text are redacted unless the server
        allows payloads and the call sets `include_payloads`.
      MD
    end
  end

  def monitoring
    DocsUI::Section("Monitoring") do
      md <<~'MD'
        **Workflow insights** (`/pgbus/workflows/definitions`, linked from the
        Workflows page as "Insights by class") answers how a workflow class is
        doing, not where one run is. The list has one row per class (every
        registered `Pgbus::Workflow` subclass and every class name still in
        `pgbus_workflows`): runs going on now, paused, needing attention, the
        runs that finished or were cancelled in the window, and the p50/p95
        end-to-end duration (created → finished). A class that no longer loads
        says "definition not loaded"; one with ongoing runs on a
        `removed_step` says how many.

        A class's page (`/pgbus/workflows/definitions/OrderWorkflow`, or
        `Billing::OrderWorkflow`) shows its steps in order as a **pipeline**:
        each step with the runs sitting at it now (scheduled, running,
        waiting, paused), the p50/p95/p99 run time and the pickup latency
        (scheduled → started) of its attempts that finished in the window,
        and their failure (an error was recorded), reattempt and skip rates.
        Percentiles over fewer than 20 attempts carry their sample size and a
        "few samples" note. Below: runs started, finished and cancelled per
        bucket (5 min for 1 h, 1 h for 24 h, 6 h for 7 d, 1 d for 30 d), the
        end-to-end duration distribution, a **funnel** (of the runs started
        in the window, how many reached each step and where each one is now)
        and the same numbers for the last five app revisions that finished an
        attempt, newest first, so a regression after a deploy shows as a row
        (a failure rate more than five points above the revision before is
        marked). The window is 1 h, 24 h (default), 7 d or 30 d, capped at
        `workflow_retention`. Every count of runs links to the Workflows
        list filtered to those runs; that list gained `ended_from`/`ended_to`
        (when a run finished or was cancelled) for it.

        The numbers come from the step and run rows, on the indexes the
        tables already have: on about 1 M step rows over 20 classes a class's
        page reads in about 25 ms and the list in about 100 ms
        (`rake bench:workflow_insights`). The read-only MCP server has the same:
        `pgbus_workflow_definitions(window:)` and
        `pgbus_workflow_stats(workflow_class:, window:)`.

        **Alerts** belong in Prometheus or AppSignal, on the metrics below.
        Two starting points: a step whose attempts end in a reattempt or a
        pause (an exception, or the body's own `reattempt!`/`pause!`) more
        than 5 % of the time for 15 minutes, and runs needing a person for an
        hour:
      MD
      DocsUI::Code(<<~'YAML', filename: "prometheus/rules/pgbus_workflows.yml", lexer: :yaml)
        groups:
          - name: pgbus-workflows
            rules:
              - alert: PgbusWorkflowStepFailing
                expr: |
                  sum by (workflow_class, step) (rate(pgbus_workflow_steps_total{outcome=~"reattempted|paused"}[15m]))
                    / sum by (workflow_class, step) (rate(pgbus_workflow_steps_total[15m])) > 0.05
                for: 15m
                annotations:
                  summary: "{{ $labels.workflow_class }}#{{ $labels.step }} fails on {{ $value | humanizePercentage }} of its attempts"
              - alert: PgbusWorkflowsNeedAttention
                expr: sum by (workflow_class) (pgbus_workflows_stuck) > 0
                for: 1h
                annotations:
                  summary: "{{ $value }} {{ $labels.workflow_class }} runs need a person: /pgbus/workflows/attention"
      YAML
      md <<~'MD'
        On AppSignal, the bundled `main` dashboard (`pgbus dashboard main`) has
        a workflows row: steps per outcome, step run time, workflows by status
        and stuck workflows.
      MD
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
        `stall_detected`, `late_completion_ignored`, and for waiting steps
        `step_waiting` (details: `wait_for` or `batch_id`), `signal_received`
        (`name`, `idempotency_key`, `duplicate`), `signal_claimed`,
        `wait_timed_out`, `fan_out_opened` (`batch_id`, `size`) and
        `fan_out_finished` (`failed_jobs`, `total_jobs`). `signal!` takes
        `actor:` too.

        **Metrics** reach Prometheus, StatsD and AppSignal with the same names
        (one exception for StatsD, below the table):
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
        StatsD reads timer values as milliseconds, so it receives the step
        histogram as `pgbus_workflow_step_duration_ms` (seconds × 1000).

        **Stuck reasons** (the `pgbus_workflows_stuck` gauge): `paused_error`
        (paused on a step that failed), `stalled` (paused on `StepStalled`),
        `paused_manual` (the step called `pause!`, or an outside `pause!`),
        `overdue` (a scheduled step nobody picked up), `waiting_overdue` (a
        wait without a `wait_timeout:` older than
        `workflow_waiting_visibility_threshold`).

        **Errors** reach `Pgbus::ErrorReporter` with
        `{ workflow_class:, workflow_id:, step_name:, attempt:, outcome: }` as
        context. **Logs**: while a step runs, Pgbus's own log lines carry
        `workflow=… step=… attempt=…` through `Pgbus::LogFormatter`, and your
        code reads `Pgbus::Workflow::StepContext.workflow_id` (and `step_name`,
        `attempt`) to tag its own. The instrumentation events are
        `pgbus.workflow.step`, `pgbus.workflow_finished`,
        `pgbus.workflow_paused`, `pgbus.workflow_cancelled`,
        `pgbus.workflow.stall_detected`, `pgbus.workflow.signal`,
        `pgbus.workflow.wait_timed_out` and `pgbus.workflow.gauge`.
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

          it "ships the order" do
            wf = OrderWorkflow.start!(order)
            speedrun_workflow(wf)            # raises StepWaiting at notify_vendors
          rescue Pgbus::Workflow::StepWaiting
            complete_fan_out(wf)             # every member completes; the run moves on
            deliver_signal(wf, :shipped, payload: { at: Time.current })  # signal!, then the step it woke
            expire_wait(wf)                  # or: the waiting step's on_timeout, now
          end
        end
      RUBY
      md <<~'MD'
        `speedrun_workflow` stops at `paused`, `finished` and `cancelled`, and
        raises `Pgbus::Workflow::StepWaiting` on `waiting`, naming the signal
        or the batch the step waits for. `complete_fan_out` completes every
        member of the waiting step's batch (without running them) and runs
        its `BatchFinishedJob` inline; `deliver_signal` calls `signal!` and
        runs the step it scheduled; `expire_wait` applies the waiting step's
        `on_timeout` whatever its deadline.
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
        - `pgbus_workflow_signals` — one row per signal
          (`Pgbus::WorkflowSignal`), unique on `(workflow_id,
          idempotency_key)`.

        Every StepJob is sent by `WorkflowStep`'s after-commit callback, and
        only there: a new scheduled row, a row going back to scheduled, or a
        bumped `enqueued_at` (how the sweep and `resume!` re-send). A StepJob
        claims its row (scheduled → in_progress, only once due) and the run
        (ready → performing) under the run's row lock, runs the guards and
        the body, then applies the outcome under the lock again: one step at
        a time per run, every change a guarded UPDATE from the table below.
        A waiting step goes back to scheduled when it is woken (a signal, or a
        timeout with `:run`), so its resume is a StepJob like any other; a
        fan-out's batch, a timeout or an operator `skip!` resolves the waiting
        row directly.
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
        `skipped`. A run is `waiting` while its step waits for a signal or a
        fan-out batch.
      MD
    end
  end
end
