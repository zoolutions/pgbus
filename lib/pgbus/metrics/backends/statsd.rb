# frozen_string_literal: true

require "socket"

module Pgbus
  module Metrics
    module Backends
      # StatsD backend emitting UDP datagrams in DogStatsD dialect.
      #
      #   increment -> "name:value|c"
      #   gauge     -> "name:value|g"
      #   histogram -> "name:value|ms"
      #
      # StatsD reads |ms values as milliseconds, so a histogram named
      # *_seconds (the Prometheus convention, e.g.
      # pgbus_workflow_step_duration_seconds) is sent as *_ms, times 1000.
      #
      # Tags are appended as DogStatsD `|#k:v,k:v`. No gem dependency — a plain
      # UDPSocket. UDP send is fire-and-forget; any socket error is logged and
      # swallowed so a metrics hiccup never propagates into the producer thread.
      class Statsd < Backend
        def initialize(host: "127.0.0.1", port: 8125, socket: nil)
          super()
          @host = host
          @port = port
          @socket = socket
          @mutex = Mutex.new
        end

        def increment(name, value = 1, tags = {})
          emit(name, value, "c", tags)
        end

        def gauge(name, value, tags = {})
          emit(name, value, "g", tags)
        end

        def histogram(name, value, tags = {})
          return emit("#{name.delete_suffix("_seconds")}_ms", value * 1000.0, "ms", tags) if name.end_with?("_seconds")

          emit(name, value, "ms", tags)
        end

        private

        def emit(name, value, type, tags)
          datagram = "#{name}:#{value}|#{type}#{format_tags(tags)}"
          socket.send(datagram, 0, @host, @port)
          nil
        rescue StandardError => e
          Pgbus.logger.warn do
            "[Pgbus::Metrics::Statsd] send failed: #{e.class}: #{e.message}"
          end
          nil
        end

        def format_tags(tags)
          pairs = tags.compact.map { |k, v| "#{k}:#{v}" }
          pairs.empty? ? "" : "|##{pairs.join(",")}"
        end

        # UDPSocket is not guaranteed thread-safe for concurrent sends; guard
        # lazy creation so the first burst of instrumentation can't race.
        def socket
          @socket || @mutex.synchronize { @socket ||= UDPSocket.new }
        end
      end
    end
  end
end
