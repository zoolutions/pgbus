# frozen_string_literal: true

require "net/http"
require "uri"
require "json"

module SseTestSupport
  # Minimal Server-Sent Events client for the streams integration tests.
  # Keep-it-simple: one thread, Net::HTTP streaming body, hand-rolled
  # SSE parser. Exists because `EventSource` is a browser API and no
  # battle-tested Ruby SSE client exposes Last-Event-ID on reconnect
  # the way we need for testing the rails/rails#52420 fix.
  #
  # Usage:
  #
  #   client = SseTestClient.connect(url: "http://localhost:3000/pgbus/streams/...")
  #   events = client.wait_for_events(count: 2, timeout: 5)
  #   events.first.id      # => "1248"
  #   events.first.event   # => "turbo-stream"
  #   events.first.data    # => "<turbo-stream ...>"
  #   client.close
  #
  # Reconnect is manual — call `client.close` and build a new one with
  # `headers: { "Last-Event-ID" => "..." }`. The client does not do
  # automatic reconnect because the tests need deterministic control
  # over when reconnects happen.
  class SseTestClient
    # received_at: CLOCK_REALTIME seconds when the event's block was parsed,
    # so a bench in another process on the same machine can time a broadcast
    # to its delivery (issue #620).
    Event = Struct.new(:id, :event, :data, :received_at)

    # status: the HTTP status code of the response (a String, as Net::HTTP
    # gives it), nil until the response headers arrive. error: "Class: message"
    # of an unexpected failure that ended the reader thread, else nil.
    attr_reader :events, :control_events, :status, :error

    def self.connect(url:, headers: {}, timeout: 5, read_timeout: 30)
      client = new(url: url, headers: headers, timeout: timeout, read_timeout: read_timeout)
      client.start
      client
    end

    # read_timeout: how long the reader waits for the next byte before giving
    # up; the default outlasts the streamer's heartbeats.
    def initialize(url:, headers: {}, timeout: 5, read_timeout: 30)
      @uri     = URI.parse(url)
      @headers = headers
      @timeout = timeout
      @read_timeout = read_timeout
      @events = []
      # pgbus control frames (event: pgbus:*) are kept separate from data
      # events so broadcast-delivery assertions (events.first / count:) are
      # not perturbed by the pgbus:connected handshake frame (issue #165).
      @control_events = []
      @mutex   = Mutex.new
      @thread  = nil
      @http    = nil
      @closed  = false
      @open_at = nil
      @status  = nil
      @error   = nil
    end

    def start
      start_nowait
      wait_until_open(@timeout)
      self
    end

    # Starts the reader thread without waiting for the response headers, so
    # a process holding hundreds of clients does not also run hundreds of
    # threads polling for them (the streams scale bench, issue #620). #status
    # stays nil until the headers arrive; a failure to connect ends the
    # thread and leaves it nil.
    def start_nowait
      @thread = Thread.new { run }
      self
    end

    def close
      @closed = true
      begin
        @http&.finish
      rescue StandardError
        # Best effort — the connection may already be closed by the peer.
      end
      @thread&.join(2)
      @thread = nil
    end

    # Blocks until `count` events have been received or `timeout` elapses.
    # Returns a snapshot slice — leaves accumulated events in @events so
    # callers can read more without losing earlier ones.
    def wait_for_events(count:, timeout: 5)
      deadline = monotonic + timeout
      loop do
        snapshot = @mutex.synchronize { @events.dup }
        return snapshot if snapshot.size >= count
        break if monotonic > deadline
        break if @closed

        sleep 0.01
      end
      @mutex.synchronize { @events.dup }
    end

    # Waits up to `timeout` seconds for no new events to arrive — useful
    # for asserting that a reconnect with Last-Event-ID delivers nothing
    # because the cursor is already caught up.
    def wait_for_quiet(seconds: 0.3)
      baseline = @mutex.synchronize { @events.size }
      sleep seconds
      current = @mutex.synchronize { @events.size }
      current == baseline
    end

    # Blocks until a pgbus:connected control frame arrives, then returns
    # the connection id it carried (or nil on timeout). Used by the
    # actor-echo (exclude) integration test.
    def wait_for_connection_id(timeout: 5)
      deadline = monotonic + timeout
      loop do
        frame = @mutex.synchronize { @control_events.find { |e| e.event == "pgbus:connected" } }
        if frame
          parsed = begin
            JSON.parse(frame.data)
          rescue JSON::ParserError
            nil
          end
          return parsed && parsed["connectionId"]
        end
        break if monotonic > deadline || @closed

        sleep 0.01
      end
      nil
    end

    private

    def run
      @http = Net::HTTP.new(@uri.host, @uri.port)
      @http.open_timeout = @timeout
      @http.read_timeout = @read_timeout

      request = Net::HTTP::Get.new(@uri.request_uri)
      request["Accept"] = "text/event-stream"
      request["Cache-Control"] = "no-cache"
      @headers.each { |k, v| request[k] = v }

      @http.start do |http|
        http.request(request) do |response|
          @status = response.code
          @open_at = monotonic
          parse_stream(response)
        end
      end
    rescue IOError, Errno::EPIPE, Errno::ECONNRESET, Net::ReadTimeout => e
      # Expected during clean shutdown or server-side close — swallow, but
      # keep it for a caller that counts failed connects.
      @error = "#{e.class}: #{e.message}"
      # EOFError is intentionally NOT listed because it's a subclass of
      # IOError and rubocop's ShadowedException cop flags the duplicate.
    rescue StandardError => e
      @error = "#{e.class}: #{e.message}"
      warn "[SseTestClient] #{@error}" unless @closed
    ensure
      @closed = true
    end

    def parse_stream(response)
      buffer = +""
      response.read_body do |chunk|
        break if @closed

        buffer << chunk
        # SSE events are terminated by a blank line. Split on \n\n and
        # keep the trailing partial block in the buffer.
        while (index = buffer.index("\n\n"))
          block = buffer.slice!(0, index + 2)
          process_block(block.chomp("\n\n"))
        end
      end
    end

    def process_block(block)
      return if block.empty?
      return if block.lines.all? { |line| line.start_with?(":") }

      event = Event.new(id: nil, event: "message", data: "",
                        received_at: ::Process.clock_gettime(::Process::CLOCK_REALTIME))
      recognized = false
      block.each_line do |raw|
        line = raw.chomp
        next if line.empty? || line.start_with?(":")

        next unless (match = line.match(/\A(\w+): ?(.*)\z/))

        field = match[1]
        value = match[2]
        case field
        when "id"
          event.id = value
          recognized = true
        when "event"
          event.event = value
          recognized = true
        when "data"
          event.data = event.data.empty? ? value : "#{event.data}\n#{value}"
          recognized = true
          # retry: directives and other SSE meta-fields are ignored; not an
          # event in the sense callers care about
        end
      end

      return unless recognized

      # Route pgbus control frames (event: pgbus:connected, pgbus:shutdown,
      # pgbus:gap-detected) into a separate list. They are connection
      # metadata, not broadcasts, so they must not shift the index/count of
      # data events that delivery assertions rely on.
      if event.event.to_s.start_with?("pgbus:")
        @mutex.synchronize { @control_events << event }
      else
        @mutex.synchronize { @events << event }
      end
    end

    def wait_until_open(timeout)
      deadline = monotonic + timeout
      sleep 0.01 until @open_at || @closed || monotonic > deadline
      raise "SseTestClient failed to connect within #{timeout}s" unless @open_at
    end

    def monotonic
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end
  end
end
