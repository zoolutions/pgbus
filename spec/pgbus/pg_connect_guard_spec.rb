# frozen_string_literal: true

require "spec_helper"

# Architectural fitness function (issue #352). The :session GUC mode contract
# — Configuration#forward_connection_variables leaves a non-libpq :variables
# key on the connection options for the CALLER to strip and apply via
# post-connect SET — is invisible at the call site: a raw PG.connect(**opts)
# compiles, passes unit tests with clean options, and then fails in
# production with `invalid connection option "variables"`. That is exactly
# how the streamer and NotifyListener broke. This spec makes the contract
# structural: every raw PG connect outside the approved modules fails the
# build with directions to the choke point.
RSpec.describe "PG.connect guard (issue #352)" do
  it "confines raw PG connects to Client (pgmq pools) and DedicatedConnection" do
    lib = File.expand_path("../../lib", __dir__)
    # Client::SessionGucs is the pools' :session-mode connect, handed options
    # Client#wrap_session_gucs already stripped of :variables (issue #671).
    approved = [
      File.join(lib, "pgbus/client.rb"),
      File.join(lib, "pgbus/client/session_gucs.rb"),
      File.join(lib, "pgbus/dedicated_connection.rb")
    ]
    pattern = /PG\s*\.\s*connect|PG::Connection\.(new|open|connect)/

    offenders = Dir.glob(File.join(lib, "**/*.rb")).reject { |f| approved.include?(f) }.select do |f|
      File.readlines(f).any? { |line| !line.match?(/\A\s*#/) && line.match?(pattern) }
    end

    expect(offenders).to be_empty, lambda {
      list = offenders.map { |f| "  - lib#{f.delete_prefix(lib)}" }.join("\n")
      "Raw PG connect call site(s) outside the approved modules:\n#{list}\n" \
        "Route dedicated connections through Pgbus::DedicatedConnection.connect — " \
        "in :session GUC mode the options Hash carries a :variables key that libpq " \
        "rejects (issue #352), and DedicatedConnection strips it and applies the " \
        "GUCs via post-connect SET."
    }
  end

  it "keeps the approved modules honest — the detector still sees their call sites" do
    # If the detector regex rots (say PG.connect moves behind indirection it
    # can't see), the guard above would pass vacuously. Assert it still
    # detects the known-legitimate call sites.
    lib = File.expand_path("../../lib", __dir__)
    pattern = /PG\s*\.\s*connect|PG::Connection\.(new|open|connect)/

    %w[pgbus/client.rb pgbus/client/session_gucs.rb pgbus/dedicated_connection.rb].each do |path|
      lines = File.readlines(File.join(lib, path))
      expect(lines.any? { |line| !line.match?(/\A\s*#/) && line.match?(pattern) })
        .to be(true), "expected #{path} to contain a raw PG connect (detector may have rotted)"
    end
  end
end
