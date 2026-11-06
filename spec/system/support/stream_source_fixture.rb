# frozen_string_literal: true

# The fixture page for spec/system/stream_source_element_spec.rb (issue #644).
#
# The element runs as shipped (the committed .min.js, Turbo from the vendored
# module) on pages that Playwright serves by request interception, so the
# dummy app needs no route. The SSE server is faked in the page: `fetch` and
# `EventSource` for /fake-sse/* are replaced by stand-ins that, like the real
# streamer, replay every durable frame after ?since= and then deliver new
# frames live; an aborted request errors its body with an AbortError, as a
# network fetch does. Every request and every pgbus:message lands in a list,
# so Capybara can wait on them.
module StreamSourceFixture
  ROOT = Pathname.new(File.expand_path("../../..", __dir__))
  FIXTURE_ASSETS = {
    "stream_source_element.js" => ROOT.join("app/assets/javascripts/pgbus/stream_source_element.min.js"),
    "turbo.js" => ROOT.join("app/frontend/pgbus/vendor/turbo.min.js")
  }.freeze
  TURBO_RAILS_SHIM = 'import * as Turbo from "turbo"; export { Turbo }'

  FAKE_SSE = <<~JS
    (() => {
      const encoder = new TextEncoder()
      const logs = {}
      const open = new Set()
      let nextDurable = 100
      let nextEphemeral = 0
      const frameText = (f) => `id: ${f.id}\\nevent: turbo-stream\\ndata: ${f.data}\\n\\n`
      // The first page's lists, kept: during a Turbo Drive render the
      // element reconnects before Turbo has swapped the permanent lists back
      // in, when getElementById would find the new page's copies.
      const lists = {}
      const record = (listId, text) => {
        const li = document.createElement("li")
        li.textContent = text
        ;(lists[listId] ||= document.getElementById(listId)).append(li)
      }

      const realFetch = window.fetch.bind(window)
      window.fetch = (input, init = {}) => {
        const url = new URL(input, location.href)
        if (!url.pathname.startsWith("/fake-sse/")) return realFetch(input, init)

        const since = url.searchParams.get("since")
        record("requests", `fetch ${url.pathname} since=${since}`)
        const body = new ReadableStream({
          start(controller) {
            const sub = { stream: url.pathname, controller }
            if (since !== null) {
              for (const f of logs[url.pathname] || []) {
                if (f.id > Number(since)) controller.enqueue(encoder.encode(frameText(f)))
              }
            }
            open.add(sub)
            init.signal?.addEventListener("abort", () => {
              open.delete(sub)
              controller.error(new DOMException("The user aborted a request.", "AbortError"))
            })
          }
        })
        return Promise.resolve(new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } }))
      }

      window.EventSource = class {
        constructor(url) { record("requests", `eventsource ${url}`) }
        addEventListener() {}
        close() {}
      }

      document.addEventListener("pgbus:message", (event) => record("deliveries", String(event.detail.msgId)), true)

      window.fakeSse = {
        broadcast(stream, data, { ephemeral = false } = {}) {
          return this.broadcastChunk(stream, [data], { ephemeral })[0]
        },
        // Several durable frames in ONE network chunk, so the element parses
        // them in one pass of its read loop.
        broadcastChunk(stream, datas, { ephemeral = false } = {}) {
          const frames = datas.map((data) => ({ id: ephemeral ? --nextEphemeral : ++nextDurable, data }))
          if (!ephemeral) (logs[stream] ||= []).push(...frames)
          const chunk = encoder.encode(frames.map(frameText).join(""))
          for (const sub of open) {
            if (sub.stream === stream) sub.controller.enqueue(chunk)
          }
          return frames.map((f) => f.id)
        }
      }
    })()
  JS

  # Two pages with the same permanent element, for the Turbo Drive example:
  # Turbo carries the live element (and the request and delivery lists) into
  # the new body instead of the freshly rendered copy.
  def fixture_html(heading)
    imports = {
      "turbo" => "/__pgbus_fixture/turbo.js",
      "@hotwired/turbo-rails" => "/__pgbus_fixture/turbo-rails.js",
      "pgbus/stream_source_element" => "/__pgbus_fixture/stream_source_element.js"
    }
    <<~HTML
      <!DOCTYPE html>
      <html>
        <head>
          <title>stream source fixture: #{heading}</title>
          <script type="importmap">#{JSON.generate(imports: imports)}</script>
          <script>#{FAKE_SSE}</script>
          <script type="module">import "pgbus/stream_source_element"</script>
        </head>
        <body>
          <h1>#{heading}</h1>
          <a id="next" href="/__pgbus_fixture/second">next page</a>
          <div id="home">
            <pgbus-stream-source id="source" src="/fake-sse/demo" since-id="100" data-turbo-permanent></pgbus-stream-source>
          </div>
          <ul id="items"></ul>
          <div id="status">initial</div>
          <ol id="requests" data-turbo-permanent></ol>
          <ol id="deliveries" data-turbo-permanent></ol>
        </body>
      </html>
    HTML
  end

  def serve_fixture
    pages = { "first" => fixture_html("first"), "second" => fixture_html("second") }
    page.driver.with_playwright_page do |pw|
      pw.route("**/__pgbus_fixture/**", lambda { |route, request|
        name = URI(request.url).path.delete_prefix("/__pgbus_fixture/")
        if pages.key?(name)
          route.fulfill(status: 200, contentType: "text/html", body: pages[name])
        elsif name == "turbo-rails.js"
          route.fulfill(status: 200, contentType: "text/javascript", body: TURBO_RAILS_SHIM)
        else
          route.fulfill(status: 200, contentType: "text/javascript", path: FIXTURE_ASSETS.fetch(name).to_s)
        end
      })
    end
    visit "/__pgbus_fixture/first"
  end

  def broadcast_chunk(stream, datas)
    page.evaluate_script("window.fakeSse.broadcastChunk(#{stream.to_json}, #{datas.to_json})")
  end

  def broadcast(stream, data, ephemeral: false)
    page.evaluate_script("window.fakeSse.broadcast(#{stream.to_json}, #{data.to_json}, { ephemeral: #{ephemeral} })")
  end

  def append_item(id) = %(<turbo-stream action="append" target="items"><template><li id="i#{id}">item #{id}</li></template></turbo-stream>)

  def replace_status(text)
    %(<turbo-stream action="replace" target="status"><template><div id="status">#{text}</div></template></turbo-stream>)
  end

  def move_element
    page.execute_script('document.body.appendChild(document.getElementById("source"))')
  end

  def deliveries = all("#deliveries li", minimum: 0).map(&:text)

  def expect_request(count, text)
    expect(page).to have_css("#requests li", count: count)
    expect(page).to have_css("#requests li:nth-child(#{count})", exact_text: text)
  end
end
