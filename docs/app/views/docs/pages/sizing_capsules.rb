# frozen_string_literal: true

# When to reach for processes:, a dedicated capsule or per-capsule recycle
# limits: how to tell a queue's profile, use cases taken from production
# workloads (generalized), and the memory and connection math for a job host.
class Views::Docs::Pages::SizingCapsules < DocsUI::Page
  title "Sizing capsules"
  eyebrow "Operations"

  def lead = "Tell whether a queue is CPU-bound, I/O-bound or memory-heavy, and give it threads, fibers, processes or a capsule of its own."

  def content
    one_capsule_one_process
    cpu_bound
    rendering_fan_out
    memory_heavy_export
    duplicated_capsules
    bulk_sweep
    external_binary
    sizing_a_host
    moving_off_workarounds
  end

  private

  def one_capsule_one_process
    DocsUI::Section("One capsule, one process", description: "What threads, fibers and processes each buy you.") do
      md <<~'MD'
        A capsule is one forked process by default. Ruby threads in one process
        share the GVL: only one of them runs Ruby code at a time, and the others
        run only while it waits on a socket, a file or a child process.

        - **`threads:`** buys concurrency while jobs wait: HTTP calls, mail, the database.
        - **`execution_mode: :async`** buys the same with fibers, cheaply enough to run
          dozens or hundreds per capsule.
        - **`processes:`** buys cores. Each fork has its own GVL, so N forks run
          CPU-bound jobs on up to N cores.
        - **A capsule of its own** buys isolation: its own queues, threads and
          [recycle limits](/docs/running-workers#worker-recycling), so one kind of job
          can't starve or recycle another.

        The option reference is [Threads vs processes](/docs/running-workers#threads-vs-processes).
        This page is about when to use which.
      MD
    end
  end

  def cpu_bound
    DocsUI::Section("Is my queue CPU-bound?", description: "Symptoms, one tool, and a decision table.") do
      md <<~'MD'
        Two symptoms point to CPU-bound jobs:

        - Wall time per job **grows** when you raise `threads:`. The jobs queue for
          the GVL instead of running side by side.
        - The capsule's worker process sits near **100 % of one core** (in `top`, or
          the CPU graph of its container), however many threads it has.

        To measure instead of guess, run `rake bench:worker_profile`: it drives a real
        worker and reports where each thread's time goes, including `gvl_wait`, the time
        a thread was ready to run but waiting for the GVL. A large `gvl_wait` share with
        your jobs means more threads won't help and more processes will.

        | Jobs are mostly… | Give the capsule |
        |---|---|
        | Waiting on I/O (HTTP, LLM APIs, webhooks, mail) | `threads:` or `execution_mode: :async`, `processes: 1` |
        | Burning CPU in Ruby (templates, PDFs, reports, serialization) | `processes:` up to the cores you can spare, `threads: 1`–`2` |
        | Holding a large object graph in memory | A capsule of its own with its own `max_memory_mb:` |
        | Waiting on an external binary (ffmpeg, a CLI converter) | `threads:`; budget the child processes' memory |
      MD
    end
  end

  def rendering_fan_out
    DocsUI::Section("Document rendering fan-out", description: "CPU-bound renders: give them processes.") do
      md <<~'MD'
        **The situation.** A batch fans out one PDF per account and period, a job each.
        Every render is CPU-bound Ruby. The jobs sit on a mixed low-priority capsule next
        to mail and HTTP jobs, under one global memory limit, so the renders hold the GVL
        while the I/O jobs wait for it.
      MD
      DocsUI::Code(<<~RUBY, filename: "before: config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.max_memory_mb = 512
          config.workers = "critical: 5; mailers, webhooks, documents: 10"
        end
      RUBY
      DocsUI::Code(<<~RUBY, filename: "after: config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.max_memory_mb = 512
          config.workers = "critical: 5; mailers, webhooks: 10"
          config.capsule :documents, queues: %w[documents], threads: 1, processes: 3,
                         max_memory_mb: 1_024
        end
      RUBY
      md <<~'MD'
        **Why.** Three forks render three documents at once on three cores, and each one
        recycles on its own limit. The I/O capsule gets its GVL back and keeps the tight
        global limit. Start `processes:` at 2 and raise it while the host has idle cores.
      MD
    end
  end

  def memory_heavy_export
    DocsUI::Section("Memory-heavy bulk export", description: "One hungry capsule among light ones.") do
      md <<~'MD'
        **The situation.** One job builds an XML or CSV feed of a whole product catalogue
        and holds the object graph in memory for minutes. We've seen a global 1 GB limit
        recycle that capsule about every two minutes, killing exports mid-flight: they came
        back as redeliveries and some never finished. The usual fix is a separate container
        role that runs only that capsule, with an environment variable raising the limit to
        about 4 GB.
      MD
      DocsUI::Code(<<~RUBY, filename: "before: config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.workers = [] # only the capsules below
          # 1 GB for every role, about 4 GB in the export container's env.
          config.max_memory_mb = ENV.fetch("PGBUS_MAX_MEMORY_MB", 1_024).to_i
          config.capsule :default, queues: %w[default mailers], threads: 10
          config.capsule :exports, queues: %w[exports], threads: 1
        end
        # export container: pgbus start --capsule exports
        # every other container: pgbus start --capsule default
      RUBY
      DocsUI::Code(<<~RUBY, filename: "after: config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.workers = [] # only the capsules below
          config.max_memory_mb = 1_024
          config.capsule :default, queues: %w[default mailers], threads: 10
          config.capsule :exports, queues: %w[exports], threads: 1, max_memory_mb: 4_096
        end
      RUBY
      md <<~'MD'
        **Why.** The limit belongs to the capsule, not the container. One supervisor runs
        both, the light capsule still recycles at 1 GB, and the export finishes. A worker
        that hits its limit drains its running jobs before it exits, so set the limit above
        the export's peak, not at it.
      MD
    end
  end

  def duplicated_capsules
    DocsUI::Section("Duplicated capsules as manual forks", description: "Replace copy-paste with processes:.") do
      md <<~'MD'
        **The situation.** Before `processes:`, the way to get two processes on the busiest
        queues was to list the same capsule twice. Both copies are anonymous: they can't be
        addressed by `--capsule`, and the banner and the Processes page show two unrelated
        workers.
      MD
      DocsUI::Code(<<~RUBY, filename: "before: config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.workers = "default, ai, enrichment: 5; default, ai, enrichment: 5; mailers: 3"
        end
      RUBY
      DocsUI::Code(<<~RUBY, filename: "after: config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.workers = "mailers: 3"
          config.capsule :shared, queues: %w[default ai enrichment], threads: 5, processes: 2
        end
      RUBY
      md <<~'MD'
        **Why.** Same behaviour, one named entry. The banner prints `processes=2`,
        `--capsule shared` selects both forks, and the Processes page labels them
        `process: 1/2` and `process: 2/2`. The string DSL has no syntax for `processes:`,
        so the capsule moves to `c.capsule`.
      MD
    end
  end

  def bulk_sweep
    DocsUI::Section("A bulk sweep starving latency-sensitive work", description: "Isolate it; don't throttle it.") do
      md <<~'MD'
        **The situation.** A storage audit enqueues around 1,500 batches. It shares one
        25-fiber capsule with third-party API syncs that users wait on, so the sweep fills
        every fiber. The workaround is `limits_concurrency to: 4` on the sweep job.
      MD
      DocsUI::Code(<<~RUBY, filename: "before")
        # config/initializers/pgbus.rb
        config.capsule :io, queues: %w[api_sync maintenance], threads: 25, execution_mode: :async

        # app/jobs/storage_audit_job.rb
        class StorageAuditJob < ApplicationJob
          queue_as :maintenance
          limits_concurrency to: 4, key: ->(*) { "storage_audit" }
        end
      RUBY
      DocsUI::Code(<<~RUBY, filename: "after: config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.workers = [] # only the capsules below
          config.capsule :api, queues: %w[api_sync], threads: 25, execution_mode: :async
          config.capsule :maintenance, queues: %w[maintenance], threads: 4,
                         max_memory_mb: 768, max_worker_lifetime: 2.hours
        end
      RUBY
      md <<~'MD'
        **Why.** The sweep now has four threads of its own and can't take a fiber from the
        API capsule, which keeps its fibers. The sweep also gets its own recycle limits.
        `limits_concurrency` stays the right tool for per-key limits (one job per account,
        a rate-limited API), not for isolation: the throttled sweep still shares the API
        capsule's process, its GVL and its recycle limits.
      MD
    end
  end

  def external_binary
    DocsUI::Section("Counter-example: work in an external binary", description: "Processes don't help here.") do
      md <<~'MD'
        **The situation.** Media transcoding or image work shells out to ffmpeg or a CLI
        converter. The CPU work happens in the child process; the Ruby thread waits for
        it with the GVL released.

        **What to do.** Threads are fine: four threads drive four converters on four cores.
        Extra forks add boot RSS and connections without adding throughput. Budget the
        **child processes' memory** instead: it doesn't count towards the worker's
        `max_memory_mb` (that limit reads the worker's own RSS), so a peak of four converters
        at 500 MB each is 2 GB on the host that the recycle limit never sees.

        LLM calls, webhooks and mail are the same shape: I/O-bound. Use threads or
        `execution_mode: :async`, never `processes:`.
      MD
    end
  end

  def sizing_a_host
    DocsUI::Section("Sizing a job host", description: "Memory and connection math for mixed capsules.") do
      md <<~'MD'
        A typical job host: 4 cores and 8 GB, running small capsules (realtime broadcasts,
        billing, default plus mail) under one global 512 MB limit. Another setup we've seen
        budgets about 400 MB RSS per pgbus process and caps the number of capsules per vCPU.
        Both work until one capsule needs more than its share. The math that replaces the
        rule of thumb:

        ```text
        host RSS ≈ Σ over capsules of processes × (boot RSS + peak job RSS)
                   + dispatcher + scheduler
        ```

        With about 400 MB boot RSS, a render capsule with a 1.1 GB peak job and
        `processes: 4` needs about 4 × (0.4 + 1.1) ≈ 6 GB before any other capsule. That
        doesn't fit next to anything else on an 8 GB host; `processes: 3` (about 4.5 GB)
        does. Copy-on-write sharing of the booted app usually makes the real number lower.
        Measure your own app: the dashboard's Processes page and the recycle log line give
        each worker's RSS.

        Connections: each fork has its own pool (`pool=` in the boot banner), so a capsule
        holds up to about `processes × pool_size` connections. Pool slots open lazily, so
        a `threads: 1` fork holds only the few it uses. Under the default
        `worker_notify_scope: :supervisor` the host still holds one LISTEN connection;
        under `:fork` each fork holds its own. `pgbus doctor` counts the LISTEN
        connections and the worker processes.

        The host above, sized:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.workers = [] # only the capsules below
          config.max_memory_mb       = 512 # every capsule without its own limit
          config.max_jobs_per_worker = 10_000

          # I/O: webhooks, mail and API calls on fibers, one process.
          config.capsule :io, queues: %w[default mailers webhooks], threads: 50,
                         execution_mode: :async

          # CPU: three forks, each recycled on its own limits.
          config.capsule :render, queues: %w[render], threads: 1, processes: 3,
                         max_memory_mb: 1_536, max_worker_lifetime: 6.hours

          # Realtime: small and latency-sensitive, never behind a render.
          config.capsule :realtime, queues: %w[realtime], threads: 3
        end
      RUBY
      md <<~'MD'
        That is five worker processes: at most 3 × 1.5 GB for the renders and 2 × 0.5 GB
        for the other two capsules, about 5.5 GB, plus the dispatcher and the scheduler
        at about boot RSS each. That leaves headroom on an 8 GB host, and three render
        forks leave a core for everything else.
      MD
    end
  end

  def moving_off_workarounds
    DocsUI::Section("Moving off the workarounds", description: "A checklist.") do
      md <<~'MD'
        - **Duplicated capsule strings** → one `c.capsule` with `processes: N`. Same
          number of processes, one name.
        - **A container role per capsule group with an env-var memory override** →
          per-capsule `max_memory_mb:` in one supervisor. Keep a separate container when
          the group needs different hardware, or a hard memory cap the kernel enforces
          (a cgroup limit) rather than a recycle limit pgbus enforces after the fact.
        - **A queue sharded by hand** (`render_1`, `render_2`, …) to get more workers →
          one queue with `processes:`. All forks read it with `FOR UPDATE SKIP LOCKED`.
        - **`limits_concurrency` used as isolation** → a capsule of its own. Keep
          `limits_concurrency` for per-key limits.
        - **One global memory limit raised for everyone** because one job is big → lower
          it back and give that job's capsule its own `max_memory_mb:`.
      MD
      DocsUI::Callout(:note) do
        code { "processes:" }
        plain " must be a positive Integer, set on "
        code { "c.capsule" }
        plain " or on an Array-form "
        code { "workers" }
        plain " entry. Capsule limits left unset fall back to the global ones. "
        plain "See "
        a(href: "/docs/running-workers#threads-vs-processes") { "Threads vs processes" }
        plain "."
      end
    end
  end
end
