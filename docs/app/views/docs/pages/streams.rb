# frozen_string_literal: true

# The Turbo Streams replacement over Postgres SSE: a drop-in helper, the
# correctness bugs it fixes, transactional broadcasts, backlog replay, and
# presence. The fanout diagram anchors the reconnect-replay idea.
class Views::Docs::Pages::Streams < DocsUI::Page
  title "Real-time streams"
  eyebrow "Guide"

  def lead = "A drop-in Turbo Streams transport over Postgres SSE — no Action Cable, no Redis, no lost messages on reconnect."

  def content
    usage
    what_it_fixes
    transactional
    replay
    broadcast_options
    broadcast_render_section
    typed_events
    coalescing
    presence
    stream_keys
    reconciliation
    consumer
  end

  private

  def usage
    DocsUI::Section("Swap one helper") do
      md <<~'MD'
        pgbus ships a drop-in replacement for turbo-rails' `turbo_stream_from`.
        Swap the helper in your view; everything else — the model concern, the
        broadcast helpers — stays the same:
      MD
      DocsUI::Code(<<~ERB, filename: "app/views/orders/show.html.erb", lexer: :erb)
        <%# Before %>
        <%= turbo_stream_from @order %>

        <%# After — no Action Cable, no Redis %>
        <%= pgbus_stream_from @order %>
      ERB
      DocsUI::Code(<<~RUBY, filename: "app/models/order.rb")
        class Order < ApplicationRecord
          broadcasts_to ->(order) { [order.account, :orders] }
        end
      RUBY
      render Components::Diagrams::StreamsFanout.new
      DocsUI::Callout(:warning) do
        plain "Add the Puma plugin so SSE connections drain cleanly on deploy: "
        code { "plugin :pgbus_streams" }
        plain " in config/puma.rb. Streams need Puma 6.1+ or Falcon (they use rack.hijack), "
        plain "and HTTP/2 in production to lift the 6-connection-per-origin SSE limit."
      end
      DocsUI::Callout(:tip) do
        plain "The default "
        code { "broadcasts_to" }
        plain " path enqueues the render+broadcast as a background job on the default queue, "
        plain "so a broadcast can wait behind long-running jobs. To keep broadcasts off the "
        plain "critical path, set "
        code { "config.streams_broadcast_queue" }
        plain " and back it with a dedicated worker capsule, or pass "
        code { "durable: true" }
        plain " to broadcast synchronously in the request thread."
      end
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.streams_broadcast_queue = "realtime"
          c.workers = [
            { queues: ["realtime"], threads: 3 },  # broadcasts get their own pool
            { queues: ["*"],        threads: 10 }   # everything else
          ]
        end
      RUBY
    end
  end

  def what_it_fixes
    DocsUI::Section("What it fixes", description: "Three well-known Action Cable correctness bugs.") do
      md <<~'MD'
        Every broadcast gets a monotonic PGMQ `msg_id`. The helper captures the
        current max at render time and embeds it as a cursor; the SSE client sends
        it as `Last-Event-ID` on reconnect, and the streamer replays anything newer
        from the live queue and the archive. That cursor model is what fixes the
        classic Action Cable gaps:
      MD
      DocsUI::Table(
        [ "Bug", "What breaks", "How pgbus fixes it" ],
        [
          [ "Page born stale", "A broadcast between render and subscribe is lost.", "A render-time msg_id watermark replays the gap." ],
          [ "Missed on reconnect", "A dropped connection misses what aired.", [ :md, "`Last-Event-ID` replays from the PGMQ archive." ] ],
          [ "No disconnect signal", "The client can't tell it dropped.", [ :md, "`pgbus:open` / `pgbus:gap-detected` / `pgbus:close` DOM events." ] ]
        ]
      )
    end
  end

  def transactional
    DocsUI::Section("Transactional broadcasts", description: "Deferred until commit — no phantom updates.") do
      md <<~'MD'
        A broadcast issued inside an open Active Record transaction is deferred
        until the transaction commits. If it rolls back, the broadcast drops —
        clients never see a change the database never persisted. No other Rails
        real-time stack can do this, because Action Cable's path goes through a
        broker with no idea of your transaction boundary.
      MD
      DocsUI::Code(<<~RUBY)
        ActiveRecord::Base.transaction do
          @order.update!(status: "shipped")
          @order.broadcast_replace_to :account    # ← deferred until commit
          RelatedService.update_counters!(@order) # ← if this raises, both roll back
        end
        # Rolled back? No client ever saw "shipped".
      RUBY
    end
  end

  def replay
    DocsUI::Section("Replaying history on connect") do
      md <<~'MD'
        By default a stream shows only broadcasts published after render (the
        page-born-stale fix). For chat-style backlog, pass `replay:`:
      MD
      DocsUI::Code(<<~ERB, lexer: :erb)
        <%= pgbus_stream_from @room, replay: 50 %>       <%# last 50 on load %>
        <%= pgbus_stream_from @room, replay: :all %>     <%# everything in retention %>
        <%= pgbus_stream_from @room, replay: :watermark %> <%# default: post-render only %>
      ERB
      DocsUI::Callout(:note) do
        plain "How far back "
        code { "replay: :all" }
        plain " reaches depends on the stream's retention ("
        code { "streams_retention" }
        plain " / "
        code { "streams_default_retention" }
        plain ", default 5 minutes). Bump it for chat streams that need days of history."
      end
    end
  end

  def broadcast_options
    DocsUI::Section("Broadcast options", description: "Every keyword #broadcast and #broadcast_render accept.") do
      md <<~'MD'
        `Pgbus.stream(name).broadcast(html, **options)` (and `broadcast_render`,
        below) take a common set of keyword options that compose freely:
      MD
      DocsUI::Table(
        [ "Option", "Type", "Effect" ],
        [
          [ [ :code, "exclude:" ], "connection id", "Skip delivery to the named SSE connection — actor-echo suppression." ],
          [ [ :code, "event:" ], "String, Symbol", "Set the SSE `event:` field so clients can route without sniffing the HTML." ],
          [ [ :code, "coalesce:" ], "true, Numeric (ms)", [ :md, "Debounce rapid broadcasts to the same `(stream, target)`; requires `target:`." ] ],
          [ [ :code, "durable:" ], "true, false, nil", "Per-broadcast override of the stream's durable mode (PGMQ-backed vs. ephemeral NOTIFY-only)." ],
          [ [ :code, "visible_to:" ], "Symbol", "Restrict delivery to connections whose authorize-hook context passes the named filter." ]
        ]
      )
      md <<~'MD'
        `durable:` predates this release and is covered in
        [Transactional broadcasts](#transactional-broadcasts) above; `visible_to:`
        restricts delivery to connections whose authorize-hook context passes a
        filter registered via `Pgbus::Streams.filters.register` (see the
        [README's audience filtering section](https://github.com/zoolutions/pgbus#server-side-audience-filtering)
        for the full registry API). The rest — `exclude:`, `event:`,
        `coalesce:` — are documented below.
      MD

      DocsUI::Section("exclude: — actor-echo suppression", description: "Don't re-deliver a broadcast to the connection that triggered it.") do
        md <<~'MD'
          An actor who just triggered a change already applied it via the HTTP
          response of their own action. If the resulting broadcast reaches their
          own SSE connection too, it double-applies — re-running animations or
          clobbering an optimistic edit. Pass `exclude:` with a connection id to
          skip that one connection; everyone else still gets the broadcast:
        MD
        DocsUI::Code(<<~RUBY)
          Pgbus.stream(room).broadcast(html, exclude: connection_id)
        RUBY
        md <<~'MD'
          The connection id comes from the server. Right after the SSE handshake
          opens, pgbus sends a `pgbus:connected` frame carrying a server-minted
          connection id; `<pgbus-stream-source>` captures it onto its
          `connection-id` attribute and re-dispatches it as a `pgbus:connected`
          event (also present in `pgbus:open`'s detail). The page reads that id
          and sends it back as the `X-Pgbus-Connection` header on the action
          request that triggers the broadcast — the server then excludes it:
        MD
        DocsUI::Code(<<~JS, filename: "app/javascript/controllers/stream_controller.js", lexer: :javascript)
          document.addEventListener("pgbus:connected", (event) => {
            document.querySelector("meta[name='pgbus-connection-id']")
              ?.setAttribute("content", event.detail.connectionId)
          })

          // On the next fetch/form submit:
          fetch(url, {
            method: "POST",
            headers: { "X-Pgbus-Connection": connectionIdMetaTag() }
          })
        JS
        DocsUI::Code(<<~RUBY, filename: "app/controllers/messages_controller.rb")
          def create
            @message = @room.messages.create!(message_params)
            @room.broadcast_append_to(
              :messages,
              exclude: request.headers["X-Pgbus-Connection"]
            )
          end
        RUBY
        DocsUI::Callout(:note) do
          plain "A nil or blank "
          code { "exclude:" }
          plain " is a no-op — the common path for background jobs and other server-initiated broadcasts with no originating connection."
        end
      end
    end
  end

  def broadcast_render_section
    DocsUI::Section("broadcast_render — render and broadcast in one call") do
      md <<~'MD'
        `Stream#broadcast_render` renders a component and broadcasts it as a
        complete `<turbo-stream>` tag atomically, removing the off-request
        render-context boilerplate every call site would otherwise hand-roll:
      MD
      DocsUI::Code(<<~'RUBY')
        Pgbus.stream("chat", room).broadcast_render(
          renderable: Chat::Message.new(chat_message: msg),
          action: :append,
          target: "chat-messages-#{room}",
          exclude: connection_id   # composes with every option above
        )
      RUBY
      DocsUI::Table(
        [ "Renderable", "Resolution" ],
        [
          [ "String", "Used verbatim — already-rendered markup." ],
          [ [ :md, "responds to `#call`" ], "Phlex component — calls it and stringifies the result." ],
          [ [ :md, "responds to `#render_in`" ], [ :md, "ViewComponent / phlex-rails — calls `render_in(nil)` (no controller view context off-request)." ] ],
          [ "else", [ :md, "falls back to `#to_s`." ] ]
        ]
      )
      md <<~'MD'
        `action` defaults to `:replace`; `target:` is required. Content-less
        actions (`:remove`) emit no `<template>` wrapper and ignore `renderable:`.
        `exclude:`, `visible_to:`, `durable:`, `event:`, and `coalesce:` all
        forward to `#broadcast` unchanged.
      MD
      DocsUI::Callout(:tip) do
        plain "A component that needs URL helpers or a full view context (e.g. "
        code { "link_to" }
        plain ") should be rendered by the app — which has the request context — with the resulting string passed as "
        code { "renderable:" }
        plain "."
      end
    end
  end

  def typed_events
    DocsUI::Section("Typed SSE event names", description: "Route on a name instead of sniffing the HTML.") do
      md <<~'MD'
        A broadcast can set the SSE `event:` field while keeping the payload a
        Turbo Stream, so clients route on a typed name instead of parsing the
        markup:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.stream(name).broadcast(html, event: "presence")
        Pgbus.stream(name).broadcast_render(renderable: component, target: "cursor", event: "reactive")
      RUBY
      md <<~'MD'
        The default (`nil` or `"turbo-stream"`) is never written into the JSONB
        payload — it's implicit — but it's still set on the SSE frame's `event:`
        line (falling back to `turbo-stream`), so default consumers are
        unaffected either way.

        On the client, `<pgbus-stream-source>` dispatches a typed broadcast two
        ways:
      MD
      DocsUI::Table(
        [ "Event", "Detail", "Use for" ],
        [
          [ [ :code, "pgbus:event" ], [ :code, "{ event, data, msgId }" ], "One listener that handles every typed event." ],
          [ [ :code, "pgbus:<event>" ], [ :code, "{ data, msgId }" ], [ :md, "`addEventListener(\"pgbus:presence\", …)` ergonomics." ] ]
        ]
      )
      DocsUI::Code(<<~JS, lexer: :javascript)
        document.addEventListener("pgbus:presence", (event) => {
          const { data, msgId } = event.detail
          // ...
        })
      JS
      DocsUI::Callout(:warning) do
        plain "Native "
        code { "EventSource" }
        plain " (the reconnect path) only invokes listeners registered by name, so declare every typed event name you use on the element's "
        code { "listen-events" }
        plain " attribute (comma- or space-separated) — otherwise a typed broadcast is silently dropped after a reconnect, even though it worked on the first connection (which uses "
        code { "fetch()" }
        plain " and routes any event generically):"
      end
      DocsUI::Code(<<~ERB, lexer: :erb)
        <%= pgbus_stream_from @room, "listen-events": "presence reactive" %>
      ERB
    end
  end

  def coalescing
    DocsUI::Section("coalesce: — publish-side debounce", description: "Batch high-frequency broadcasts to the latest frame per target.") do
      md <<~'MD'
        A chatty component — a live cursor, a typing indicator, a progress bar —
        can fan out many small broadcasts per second. Pass `coalesce:` (a window
        in milliseconds, or `true` for the 50ms default) together with `target:`
        to batch broadcasts per `(stream, target)` and publish only the *latest*
        frame within the window:
      MD
      DocsUI::Code(<<~'RUBY')
        Pgbus.stream(name).broadcast_render(
          renderable: CursorPosition.new(x:, y:),
          target: "cursor-#{user_id}",
          coalesce: true          # or coalesce: 100 for a 100ms window
        )
      RUBY
      md <<~'MD'
        Superseded frames never hit the bus at all — no PGMQ insert, no NOTIFY,
        no fan-out. This is last-write-wins, so it's only safe for **idempotent
        replace/update of a stable target** (exactly the high-frequency case
        above) — never for actions where every intermediate frame matters (an
        `append` to a running log, for instance).

        Semantics: the first submit for a `(stream, target)` schedules the flush
        one window later; every subsequent submit within that window only
        overwrites the buffered payload. Latency is bounded to one window, and a
        continuous stream of updates can't starve the flush indefinitely
        (trailing-edge-with-max-wait, not a resettable debounce). The flush
        re-enters the normal broadcast path, so a coalesced frame still composes
        with `visible_to:`, `exclude:`, `event:`, and `durable:`.
      MD
      md <<~'MD'
        `coalesce:` works on the `Turbo::Broadcastable` path too — any targeted
        broadcast helper on a model, or a direct `Turbo::StreamsChannel` call.
        The frame's own `target:` is the coalescing key (resolved to the same
        dom id Turbo renders), so a record target keys on `order_7`, not on
        whichever object happened to carry it:
      MD
      DocsUI::Code(<<~'RUBY')
        @order.broadcast_replace_to :account, coalesce: 100

        Turbo::StreamsChannel.broadcast_replace_to(
          @account, target: "order-count", coalesce: true, partial: "orders/count"
        )
      RUBY
      DocsUI::Callout(:note) do
        plain "Coalescing is process-wide and in-memory. Behind multiple Puma workers or Falcon processes, each process debounces its own submissions independently."
      end
      DocsUI::Callout(:note) do
        plain "The coalescing key is the frame's target, so the helpers that carry no target — "
        code { "broadcast_refresh_to" }
        plain " and "
        code { "broadcast_render_to" }
        plain " — cannot coalesce; passing "
        code { "coalesce:" }
        plain " to one raises. The "
        code { "_later_to" }
        plain " variants run in a job, where the thread-local carrying the option cannot follow."
      end
    end
  end

  def presence
    DocsUI::Section("Presence", description: "\"X people are in this room.\"") do
      md <<~'MD'
        Track who is subscribed to a stream with a presence table. Two modes
        are available and can be mixed: the **manual API** below (explicit
        join/leave, full control over when someone counts as "present") and
        **connection-driven presence** (automatic, opt-in per stream pattern).
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:add_presence && rails db:migrate
      SHELL
      DocsUI::Code(<<~RUBY)
        Pgbus.stream(@room).presence.join(
          member_id: current_user.id.to_s,
          metadata: { name: current_user.name }
        ) { |member| render_to_string(partial: "presence/joined", locals: { member: }) }

        Pgbus.stream(@room).presence.members # => [{ "id" => "7", "metadata" => {...} }, …]
        Pgbus.stream(@room).presence.count   # => 5
      RUBY

      DocsUI::Section("Connection-driven presence (opt-in)", description: "Auto-join on connect, auto-leave on disconnect — no explicit wiring.") do
        md <<~'MD'
          Streams matching `config.streams_presence_patterns` (an exact string
          or a `Regexp`, mirroring `streams_durable_patterns`) automatically
          join a member when an SSE connection opens, leave when it closes, and
          refresh `last_seen_at` on every keepalive heartbeat tick — no explicit
          `join`/`leave`/sweeper calls required:
        MD
        DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
          Pgbus.configure do |c|
            c.streams_presence_patterns = [/^room:/, "lobby"]
          end
        RUBY
        md <<~'MD'
          Identity comes from the connection's authorize-hook context (the
          value your `StreamApp` `authorize:` callable returns). The built-in
          extractor handles the common shapes without any configuration:
        MD
        DocsUI::Table(
          [ "Context shape", "Extracted member" ],
          [
            [ [ :md, "`Hash` with `:member_id` or `:id`" ], [ :md, "`{ id:, metadata: }` — `:metadata` optional, defaults to `{}`" ] ],
            [ [ :md, "any object responding to `#id`" ], [ :md, "`{ id: object.id, metadata: {} }`" ] ]
          ]
        )
        md <<~'MD'
          For anything else, provide a custom extractor — a `->(context) { { id:, metadata: } }`
          callable returning `nil` for a context with no derivable identity
          (anonymous connections are simply skipped, not an error):
        MD
        DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
          Pgbus.configure do |c|
            c.streams_presence_patterns = [/^room:/]
            c.streams_presence_member = ->(user) {
              { id: user.id, metadata: { name: user.name, avatar: user.avatar_url } } if user
            }
          end
        RUBY
        DocsUI::Callout(:note) do
          plain "Membership work runs on the dispatcher thread and presence failures are logged and swallowed — a presence-table hiccup can never knock a live SSE connection out of the registry."
        end
      end
    end
  end

  def stream_keys
    DocsUI::Section("stream_key idempotency", description: "Hold one key value and reuse it safely.") do
      md <<~'MD'
        `Pgbus.stream_key` treats a single `String` argument as an already-built
        pgbus stream key and returns it unchanged (after the queue-name budget
        check), instead of tripping the colon-separator guard. This lets a
        consumer hold one `stream_key` value and pass it to both
        `turbo_stream_from`/`pgbus_stream_from` and the broadcaster:
      MD
      DocsUI::Code(<<~RUBY)
        key = Pgbus.stream_key(chat, :messages)  # => "ai_chat_a3f8c1e9d2b47610:messages"

        # Both calls accept the same pre-built key without raising:
        pgbus_stream_from(key)
        Pgbus.stream(key).broadcast(html)
      RUBY
      md <<~'MD'
        `Pgbus.stream_key!(key)` accepts a pre-built key explicitly — `String`
        required, budget still enforced — for call sites that want to be
        explicit that no re-keying should happen.

        The guard is still enforced for the cases that are genuinely ambiguous:
      MD
      DocsUI::Table(
        [ "Call", "Behavior" ],
        [
          [ [ :code, 'stream_key("chat:lobby")' ], "OK — single pre-built key, idempotent." ],
          [ [ :code, 'stream_key("a:b", :c)' ], [ :md, "Raises `ArgumentError` — an ambiguous multi-fragment join (`\"a:b\"` + `:c` could mean `stream_key(\"a\", \"b:c\")` too)." ] ],
          [ [ :code, "stream_key(:'a:b')" ], [ :md, "Raises `ArgumentError` — a colon in a `Symbol`/record fragment never came from `stream_key` and is treated as a mistake." ] ]
        ]
      )
    end
  end

  def reconciliation
    DocsUI::Section("msg_id reconciliation for optimistic UI", description: "Reconcile out-of-order or duplicate delivery on the client.") do
      md <<~'MD'
        Every delivered frame carries its monotonic PGMQ `msg_id` as the SSE
        `id:` line — this is the same watermark that powers reconnect replay.
        `<pgbus-stream-source>` surfaces it to the client two ways:
      MD
      DocsUI::Table(
        [ "Event", "Detail", "Use for" ],
        [
          [ [ :code, "message" ], [ :md, "standard `MessageEvent`, `lastEventId` set to the msg_id" ], [ :md, "Turbo ignores it; a reactive runtime listening for `message` reads the revision with no pgbus-specific API." ] ],
          [ [ :code, "pgbus:message" ], [ :code, "{ msgId, data }" ], "Optimistic-UI reconciliation — msgId is a Number when numeric." ]
        ]
      )
      md <<~'MD'
        A negative `msgId` marks an ephemeral frame (one that bypassed PGMQ —
        an ephemeral-mode broadcast) rather than a durable, archived one.

        **The reconciliation recipe:** track the highest applied `msgId` per
        render target; when a frame arrives, skip the morph if you've already
        applied a newer revision for that target. This stops a late echo — a
        broadcast that was in flight when a newer one landed — from clobbering
        a newer optimistic edit:
      MD
      DocsUI::Code(<<~JS, lexer: :javascript)
        const appliedRevision = new Map() // target -> highest applied msgId

        document.addEventListener("pgbus:message", (event) => {
          const { msgId, data } = event.detail
          const target = extractTargetFrom(data) // however your markup encodes it

          const highest = appliedRevision.get(target) ?? -Infinity
          if (msgId != null && msgId < highest) return // stale — skip the morph

          appliedRevision.set(target, msgId)
          applyMorph(target, data)
        })
      JS
      DocsUI::Callout(:note) do
        plain "This complements "
        code { "exclude:" }
        plain ": "
        code { "exclude:" }
        plain " handles the actor (never receives its own echo at all); msg_id reconciliation handles out-of-order delivery for everyone else."
      end
    end
  end

  def consumer
    DocsUI::Section("On the consuming side") do
      md <<~'MD'
        [phlex-reactive](https://phlex-reactive.zoolutions.llc) uses pgbus as its
        broadcast transport, so its
        [Transport: pgbus](https://phlex-reactive.zoolutions.llc/docs/transport-pgbus)
        page is a good companion read — it shows the same primitives from a
        component author's point of view, including the pgbus-only `exclude:` and
        `visible_to:` broadcast options.
      MD
    end
  end
end
