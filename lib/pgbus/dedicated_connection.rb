# frozen_string_literal: true

module Pgbus
  # Single choke point for opening a DEDICATED PG connection outside the
  # pgmq pools — the streamer's LISTEN connection and the worker
  # NotifyListener. Every such path MUST route through here (enforced by
  # spec/pgbus/pg_connect_guard_spec.rb): in :session GUC mode,
  # Configuration#forward_connection_variables leaves the database.yml
  # `variables:` hash on the connection options for the caller to apply
  # post-connect, and :variables is not a libpq keyword — a raw
  # PG.connect(**opts) fails with `invalid connection option "variables"`
  # (issue #352). This mirrors Client#wrap_session_gucs, which does the
  # same for pool connections: the GUCs are applied via post-connect SET
  # because a transaction-mode pooler rejects the libpq `options` startup
  # param (the reason :session mode exists).
  module DedicatedConnection
    module_function

    # Stable census tag for every dedicated LISTEN connection, so operators can
    # count them: SELECT count(*) FROM pg_stat_activity WHERE application_name
    # = 'pgbus-listen' (issue #381 connection budget). Applied as
    # fallback_application_name so an explicit application_name in the URL /
    # hash — or PGAPPNAME in the environment — always wins.
    APP_NAME = "pgbus-listen"

    # libpq TCP keepalives for the streamer's LISTEN connection (issue #664):
    # probing starts after 5 idle seconds, every 2 s, 3 times, so a dead
    # socket errors within ~11 s even between the listener's own keepalive
    # queries; tcp_user_timeout bounds data the peer never acknowledges by
    # the same 11 s. connect_timeout bounds the connect itself (issue #674:
    # a streamer starting for process signals, and a hub worker rebuilding
    # its own listener, otherwise wait on an unreachable host as long as the
    # OS lets the TCP handshake retry). Client-side keywords: never in the
    # startup packet, so a pooler cannot reject them.
    LISTEN_KEEPALIVES = {
      keepalives: 1,
      keepalives_idle: 5,
      keepalives_interval: 2,
      keepalives_count: 3,
      tcp_user_timeout: 11_000,
      connect_timeout: 10
    }.freeze

    def connect(opts)
      require "pg" unless defined?(::PG::Connection)
      case opts
      when String then ::PG.connect(with_app_name(opts))
      when Hash then connect_from_hash(opts)
      else
        raise Pgbus::ConfigurationError,
              "Cannot build a dedicated PG connection from #{opts.class}. " \
              "Set database_url or connection_params so pgbus can open its own connection."
      end
    end

    # Append the census tag to a conninfo String — URL query-param style for
    # postgres:// URLs, space-separated keyword style otherwise. A string that
    # already mentions application_name (either keyword) is left untouched.
    def with_app_name(conninfo)
      return conninfo if conninfo.include?("application_name")

      if url?(conninfo)
        "#{conninfo}#{query_separator(conninfo)}fallback_application_name=#{APP_NAME}"
      else
        "#{conninfo} fallback_application_name=#{APP_NAME}".strip
      end
    end

    # connect, plus LISTEN_KEEPALIVES: the streamer's LISTEN connection. A
    # keyword the options already set wins.
    def connect_with_keepalives(opts)
      require "pg" unless defined?(::PG::Connection)
      connect(with_keepalives(opts))
    end

    # Whether the linked libpq accepts the tcp_user_timeout / keepalives conninfo
    # keywords. Added in libpq 12; an older libpq raises "invalid connection
    # option" and fails the connection, so they must not be emitted there.
    def libpq_supports_socket_bounds?
      defined?(::PG) && ::PG.respond_to?(:library_version) && ::PG.library_version >= 120_000
    end

    # Fill in the LISTEN_KEEPALIVES keywords the options do not already set.
    # Unchanged on libpq < 12 (see libpq_supports_socket_bounds?), and for a
    # connection that names a service (service=, or PGSERVICE): libpq applies
    # the service file only to keywords the conninfo leaves unset, so an added
    # keyword would override the service's own.
    def with_keepalives(opts)
      return opts unless libpq_supports_socket_bounds?
      return opts if ENV.fetch("PGSERVICE", nil).to_s != ""

      case opts
      when Hash
        return opts if opts.key?(:service) || opts.key?("service")

        missing = LISTEN_KEEPALIVES.reject { |key, _| opts.key?(key) || opts.key?(key.to_s) }
        opts.merge(missing)
      when String then append_keepalives(opts)
      else opts
      end
    end

    # URL query-param style for postgres:// URLs, space-separated keyword
    # style otherwise (as with_app_name does). The keywords already set are
    # read with libpq's own parser, so a keyword quoted inside another value
    # does not count; a conninfo libpq cannot parse is left for PG.connect to
    # report.
    def append_keepalives(conninfo)
      set = conninfo_keywords(conninfo)
      return conninfo if set.nil? || set.include?("service")

      missing = LISTEN_KEEPALIVES.reject { |key, _| set.include?(key.to_s) }
      return conninfo if missing.empty?

      if url?(conninfo)
        "#{conninfo}#{query_separator(conninfo)}#{missing.map { |key, value| "#{key}=#{value}" }.join("&")}"
      else
        "#{conninfo} #{missing.map { |key, value| "#{key}=#{value}" }.join(" ")}".strip
      end
    end

    # A postgres:// or postgresql:// URL, as libpq tells them apart: by the
    # prefix, so a key=value conninfo with :// inside a value (a password)
    # stays key=value.
    def url?(conninfo)
      conninfo.start_with?("postgres://", "postgresql://")
    end

    # The keywords a conninfo String sets, or nil when libpq cannot parse it.
    def conninfo_keywords(conninfo)
      ::PG::Connection.conninfo_parse(conninfo).filter_map { |option| option[:keyword] if option[:val] }
    rescue ::PG::Error
      nil
    end

    # "?" to start a URL's query string, "&" to extend one, nothing after a
    # trailing "?" or "&" (an empty parameter would fail the connect).
    def query_separator(url)
      return "" if url.end_with?("?", "&")

      url.include?("?") ? "&" : "?"
    end

    def connect_from_hash(opts)
      variables = opts[:variables]
      conn = ::PG.connect(**with_app_name_hash(opts.except(:variables)))
      begin
        variables&.each { |name, value| conn.exec("SET #{name} = '#{value}'") }
      rescue StandardError
        # A failing SET (e.g. a bogus GUC name in database.yml variables:)
        # must not orphan the freshly opened socket — the reconnect loops
        # retry on a tight backoff and would leak one server connection per
        # attempt until PostgreSQL exhausts max_connections.
        close_quietly(conn)
        raise
      end
      conn
    end

    def with_app_name_hash(opts)
      return opts if opts.key?(:application_name) || opts.key?(:fallback_application_name)

      opts.merge(fallback_application_name: APP_NAME)
    end

    def close_quietly(conn)
      conn.close
    rescue StandardError
      nil
    end
  end
end
