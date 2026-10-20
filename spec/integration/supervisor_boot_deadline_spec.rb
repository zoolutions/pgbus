# frozen_string_literal: true

require_relative "../integration_helper"
require "socket"

# Issue #587 over a real pgbus_processes table: which rows check a fork in.
# Only a row on this host, beaten since the fork, vouches for a child; a
# sibling container's row or a dead predecessor's row with the same pid does
# not, and the hung child is killed.
RSpec.describe "Supervisor boot deadline (integration)", :integration do
  let(:config) { Pgbus.configuration }
  let(:supervisor) { Pgbus::Process::Supervisor.new(config: config, last_watchdog_at: 0.0) }
  let(:pid) { rand(4_000_000..4_099_999) }
  let(:now) { Process.clock_gettime(Process::CLOCK_MONOTONIC) }

  around do |example|
    saved = config.boot_timeout
    config.boot_timeout = 120
    example.run
  ensure
    config.boot_timeout = saved
    Pgbus::ProcessEntry.where(pid: pid).delete_all
  end

  before do
    allow(Process).to receive(:kill)
    allow(supervisor).to receive(:dump_grace_sleep)
    supervisor.forks = {
      pid => { type: :dispatcher, spawned_at: now - 600, spawned_wall_at: Time.current - 600, checked_in: false }
    }
  end

  def row(hostname:, beaten_ago:)
    Pgbus::ProcessEntry.create!(kind: "dispatcher", hostname: hostname, pid: pid,
                                last_heartbeat_at: Time.current - beaten_ago)
  end

  it "checks in a fork whose row on this host was beaten since the fork" do
    row(hostname: Socket.gethostname, beaten_ago: 30)

    supervisor.send(:check_stalled_workers)

    expect(supervisor.forks[pid][:checked_in]).to be(true)
    expect(Process).not_to have_received(:kill)
  end

  it "kills the fork when the only row with its pid is another host's" do
    row(hostname: "another-container", beaten_ago: 30)

    supervisor.send(:check_stalled_workers)

    expect(supervisor.forks[pid][:checked_in]).to be(false)
    expect(Process).to have_received(:kill).with("KILL", pid)
  end

  it "kills the fork when the only row with its pid predates the fork" do
    row(hostname: Socket.gethostname, beaten_ago: 900)

    supervisor.send(:check_stalled_workers)

    expect(supervisor.forks[pid][:checked_in]).to be(false)
    expect(Process).to have_received(:kill).with("KILL", pid)
  end
end
