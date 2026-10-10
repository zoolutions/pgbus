# frozen_string_literal: true

# Mounting and securing the dashboard, and what each panel shows.
class Views::Docs::Pages::Dashboard < DocsUI::Page
  title "Dashboard"
  eyebrow "Operations"

  def lead = "Mount the engine to see queues, jobs, failures, and dead letters — auto-refreshing over Turbo Frames."

  def content
    mount
    panels
    auth
  end

  private

  def mount
    DocsUI::Section("Mount it") do
      DocsUI::Code(<<~RUBY, filename: "config/routes.rb")
        mount Pgbus::Engine => "/pgbus"
      RUBY
      md <<~'MD'
        The dashboard is a mountable Rails engine. Every table auto-refreshes over
        Turbo Frames — no WebSocket, no Action Cable. Destructive actions use styled
        confirmation dialogs and flash as toast notifications.
      MD
    end
  end

  def panels
    DocsUI::Section("The panels") do
      DocsUI::Table(
        [ "Panel", "Shows" ],
        [
          [ "Overview", [ :md, "A health verdict strip (OK / DEGRADED / STALLED with its reasons — what `/readyz` would say), then cards that each say what their number means and link to the page that explains it: queues (paused, dead-letter, undrained), enqueued, processes (healthy, stalled, stale), recurring tasks, retrying (next attempt, newest error), dead-lettered (newest reason), throughput (jobs/min over 5 minutes, from job stats) and parked jobs. The queues table says what each queue is doing; Queue Health states its thresholds." ] ],
          [ "Queues", "Per-queue metrics with purge / pause / resume / delete." ],
          [ "Jobs", "Enqueued and failed jobs, with retry / discard." ],
          [ "Dead letter", "New dead messages show why they died — the last recorded error when available, attempts, and source queue — filterable by DLQ and error class, with retry / discard and bulk actions." ],
          [ "Processes", [ :md, "Every process with its status in words: why it is Healthy, Stalled or Stale against which limit, and what happens next (the supervisor's watchdog restarts it, the dispatcher removes it within 5 minutes, or nothing will). Each row also says what the process is doing (queues or topics, busy threads, buffered claims, jobs/s), how close it is to a recycle (`1,240 / 10,000 jobs · 310 / 512 MB · up 42m 0s / 1h 0m`), its uptime and its supervisor." ] ],
          [ "Events", "Pending events with their state, the handler that runs each one and why it waits (or which attempt failed with what error); processed events say whether each claim completed, is being handled or went silent, with Replay when the claim completed, its handler is still registered and the archive still holds the event; subscribers say whether a running consumer drains them." ],
          [ "Outbox", "Transactional outbox entries pending publication." ],
          [ "Locks", "Uniqueness locks and concurrency keys — value, limit, lease, parked jobs; release a key or discard its parked jobs." ],
          [ "Insights", [ :md, "Throughput and status distribution charts, each with a caption saying what it shows and a data table behind it, themed for light and dark mode; slowest job classes. The queue latency chart and latency by queue appear once `rails generate pgbus:add_job_stats_latency` has added the latency columns." ] ]
        ]
      )
      DocsUI::Callout(:note) do
        plain "The Insights page reads "
        code { "pgbus_job_stats" }
        plain " — add it with "
        code { "rails generate pgbus:add_job_stats" }
        plain ". Stats are buffered and bulk-inserted; if the migration hasn't run, recording is silently skipped."
      end
      md <<~'MD'
        Every process registers its start time, and processes the supervisor forks
        register its pid. Workers and consumers refresh their pool's capacity and
        busy count, buffered claims, RSS, processed/handled counts and recycle
        limits (a worker uses its capsule's own limits when set) once per 60 s
        beat, never per job. Workers also report live rates; consumers do not.
        A process from before these keys existed shows "—" where a fact is
        missing. A row is grouped under a supervisor only when that supervisor's
        row is on the same host. The stall limits the page quotes are derived from
        the dashboard host's `stall_threshold`: workers get it as is, dispatchers
        and schedulers add their sleep interval, consumers add their polling
        allowance.
      MD
    end
  end

  def auth
    DocsUI::Section("Protect it in production", description: "The dashboard has no auth of its own.") do
      md <<~'MD'
        Guard the mounted engine with a `web_auth` lambda:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.web_auth = ->(request) { request.env["warden"]&.user&.admin? }
        end
      RUBY
      md <<~'MD'
        Or inherit from your own authenticated controller — then its
        `before_action` filters and helpers apply automatically, with no
        monkey-patching:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure do |config|
          config.base_controller_class = "Admin::BaseController"
          config.return_to_app_url     = "/admin" # adds a "back to app" button
        end
      RUBY
      DocsUI::Callout(:warning) do
        plain "The dashboard exposes queue contents and destructive actions "
        plain "(purge, delete, discard). Never mount it unauthenticated in production."
      end
    end
  end
end
