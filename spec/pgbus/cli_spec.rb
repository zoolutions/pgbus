# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::CLI do
  let(:mock_client) { build_mock_client }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
  end

  describe ".start" do
    it "routes 'version' to version output" do
      expect { described_class.start(["version"]) }.to output("pgbus #{Pgbus::VERSION}\n").to_stdout
    end

    it "routes 'help' to help text" do
      expect { described_class.start(["help"]) }.to output(/Usage: pgbus/).to_stdout
    end

    it "routes unknown command to help text and exits with 1" do
      expect { described_class.start(["bogus"]) }.to raise_error(SystemExit) do |error|
        expect(error.status).to eq(1)
      end
    end

    it "defaults to help when no args" do
      expect { described_class.start([]) }.to output(/Usage: pgbus/).to_stdout
    end

    it "lists the mcp command in help" do
      expect { described_class.start(["help"]) }.to output(/mcp\s+Start the read-only MCP/).to_stdout
    end

    it "routes 'mcp' to the MCP runner after loading the subsystem" do
      Pgbus::MCP.load!
      allow(Pgbus::MCP).to receive(:load!)
      allow(Pgbus::MCP::Runner).to receive(:run)

      described_class.start(["mcp"])

      expect(Pgbus::MCP).to have_received(:load!)
      expect(Pgbus::MCP::Runner).to have_received(:run)
    end

    it "lists the doctor command in help" do
      expect { described_class.start(["help"]) }.to output(/doctor\s+Run environment diagnostics/).to_stdout
    end

    it "lists the dlq command in help" do
      expect { described_class.start(["help"]) }.to output(/dlq\s+Inspect and drain dead-letter queues/).to_stdout
    end

    it "routes 'dlq' to the DLQ subcommand handler" do
      allow(Pgbus::CLI::DLQ).to receive(:start)

      described_class.start(%w[dlq list])

      expect(Pgbus::CLI::DLQ).to have_received(:start).with(%w[list])
    end
  end

  describe ".start with doctor command" do
    let(:doctor) { instance_double(Pgbus::Doctor, report: "Pgbus Doctor\n✓ ok") }

    before { allow(Pgbus::Doctor).to receive(:new).and_return(doctor) }

    it "prints the doctor report" do
      allow(doctor).to receive(:success?).and_return(true)

      expect { described_class.start(["doctor"]) }.to output(/Pgbus Doctor/).to_stdout
    end

    it "exits 0 when all checks pass" do
      allow(doctor).to receive(:success?).and_return(true)

      expect { described_class.start(["doctor"]) }.not_to raise_error
    end

    it "exits 1 when any check fails" do
      allow(doctor).to receive(:success?).and_return(false)

      expect do
        capture_stdout { described_class.start(["doctor"]) }
      end.to raise_error(SystemExit) { |e| expect(e.status).to eq(1) }
    end
  end

  describe ".start with start command" do
    let(:supervisor) { instance_double(Pgbus::Process::Supervisor, run: nil) }

    before do
      allow(Pgbus::Process::Supervisor).to receive(:new).and_return(supervisor)
      allow(Pgbus.logger).to receive(:info)
    end

    context "with --queues flag" do
      it "overrides Pgbus.configuration.workers from the CLI" do
        original_workers = Pgbus.configuration.workers
        described_class.start(["start", "--queues", "critical: 5; default: 10"])
        expect(Pgbus.configuration.workers.size).to eq(2)
        expect(Pgbus.configuration.workers.map { |c| c[:name] }).to eq(%w[critical default])
      ensure
        Pgbus.configuration.instance_variable_set(:@workers, original_workers)
      end

      it "supports --queues=STRING with equals sign" do
        original_workers = Pgbus.configuration.workers
        described_class.start(["start", "--queues=*: 7"])
        # Wildcard capsules are anonymous (no auto-assigned :name) — see
        # the long comment on Pgbus::Configuration#workers= for the why.
        expect(Pgbus.configuration.workers).to eq([{ queues: ["*"], threads: 7 }])
      ensure
        Pgbus.configuration.instance_variable_set(:@workers, original_workers)
      end

      it "raises a parse error for invalid queue strings" do
        expect do
          described_class.start(["start", "--queues", "default: 0"])
        end.to raise_error(Pgbus::Configuration::CapsuleDSL::ParseError)
      end
    end

    context "with --capsule flag" do
      it "filters workers to only the named capsule" do
        original_workers = Pgbus.configuration.workers
        Pgbus.configuration.instance_variable_set(:@workers, nil)
        Pgbus.configuration.capsule(:critical, queues: %w[critical], threads: 5)
        Pgbus.configuration.capsule(:default, queues: %w[default], threads: 10)

        described_class.start(["start", "--capsule", "critical"])

        expect(Pgbus.configuration.workers.size).to eq(1)
        expect(Pgbus.configuration.workers.first[:name]).to eq("critical")
      ensure
        Pgbus.configuration.instance_variable_set(:@workers, original_workers)
      end

      it "raises when the named capsule does not exist" do
        original_workers = Pgbus.configuration.workers
        Pgbus.configuration.instance_variable_set(:@workers, nil)
        Pgbus.configuration.capsule(:critical, queues: %w[critical], threads: 5)

        expect do
          described_class.start(["start", "--capsule", "missing"])
        end.to raise_error(/no capsule named.*missing/i)
      ensure
        Pgbus.configuration.instance_variable_set(:@workers, original_workers)
      end
    end

    context "when the flags change the configuration" do
      # As the app's boot leaves it: finalized, its collections frozen.
      around do |example|
        original = Pgbus.configuration
        published = Pgbus.configuration_snapshot
        Pgbus.instance_variable_set(:@configuration, Pgbus::Configuration.new.finalize!)
        example.run
      ensure
        Pgbus.instance_variable_set(:@configuration, original)
        Pgbus.instance_variable_set(:@configuration_snapshot, published)
      end

      # The flags go through setters after the app's boot finalized the
      # configuration, so the supervisor would fork with open values.
      it "finalizes the values they set before the supervisor boots" do
        shareable_at_boot = nil
        allow(supervisor).to receive(:run) do
          config = Pgbus.configuration
          shareable_at_boot = [config.workers, config.roles].map { |value| Ractor.shareable?(value) }
        end

        described_class.start(["start", "--queues", "critical: 5", "--workers-only"])

        expect(shareable_at_boot).to eq([true, true])
        expect(Pgbus.configuration.workers.map { |capsule| capsule[:name] }).to eq(%w[critical])
      end
    end

    context "without flags" do
      it "starts the supervisor with the existing workers config" do
        described_class.start(["start"])
        expect(supervisor).to have_received(:run)
      end

      it "leaves config.roles nil (boot every role)" do
        original_roles = Pgbus.configuration.roles
        described_class.start(["start"])
        expect(Pgbus.configuration.roles).to be_nil
      ensure
        Pgbus.configuration.roles = original_roles
      end
    end

    context "with --workers-only flag" do
      it "sets config.roles to [:workers]" do
        original_roles = Pgbus.configuration.roles
        described_class.start(["start", "--workers-only"])
        expect(Pgbus.configuration.roles).to eq([:workers])
      ensure
        Pgbus.configuration.roles = original_roles
      end
    end

    context "with --scheduler-only flag" do
      it "sets config.roles to [:scheduler]" do
        original_roles = Pgbus.configuration.roles
        described_class.start(["start", "--scheduler-only"])
        expect(Pgbus.configuration.roles).to eq([:scheduler])
      ensure
        Pgbus.configuration.roles = original_roles
      end
    end

    context "with --dispatcher-only flag" do
      it "sets config.roles to [:dispatcher]" do
        original_roles = Pgbus.configuration.roles
        described_class.start(["start", "--dispatcher-only"])
        expect(Pgbus.configuration.roles).to eq([:dispatcher])
      ensure
        Pgbus.configuration.roles = original_roles
      end
    end

    context "with --execution-mode flag" do
      it "sets config.execution_mode to :async" do
        original_mode = Pgbus.configuration.execution_mode
        described_class.start(["start", "--execution-mode", "async"])
        expect(Pgbus.configuration.execution_mode).to eq(:async)
      ensure
        Pgbus.configuration.execution_mode = original_mode
      end

      it "sets config.execution_mode to :threads" do
        original_mode = Pgbus.configuration.execution_mode
        described_class.start(["start", "--execution-mode", "threads"])
        expect(Pgbus.configuration.execution_mode).to eq(:threads)
      ensure
        Pgbus.configuration.execution_mode = original_mode
      end
    end

    context "with --doctor flag (issue #347)" do
      it "sets config.doctor_on_boot to :report" do
        original = Pgbus.configuration.doctor_on_boot
        described_class.start(["start", "--doctor"])
        expect(Pgbus.configuration.doctor_on_boot).to eq(:report)
      ensure
        Pgbus.configuration.doctor_on_boot = original
      end
    end

    context "with --doctor-strict flag (issue #347)" do
      it "sets config.doctor_on_boot to :strict" do
        original = Pgbus.configuration.doctor_on_boot
        described_class.start(["start", "--doctor-strict"])
        expect(Pgbus.configuration.doctor_on_boot).to eq(:strict)
      ensure
        Pgbus.configuration.doctor_on_boot = original
      end

      it "wins over --doctor when both are passed (strict is the stronger gate)" do
        original = Pgbus.configuration.doctor_on_boot
        described_class.start(["start", "--doctor", "--doctor-strict"])
        expect(Pgbus.configuration.doctor_on_boot).to eq(:strict)
      ensure
        Pgbus.configuration.doctor_on_boot = original
      end
    end

    context "without a doctor flag" do
      it "leaves config.doctor_on_boot at its configured value (default nil)" do
        original = Pgbus.configuration.doctor_on_boot
        Pgbus.configuration.doctor_on_boot = nil
        described_class.start(["start"])
        expect(Pgbus.configuration.doctor_on_boot).to be_nil
      ensure
        Pgbus.configuration.doctor_on_boot = original
      end
    end

    context "with multiple --*-only flags (mutually exclusive)" do
      it "raises ArgumentError when --workers-only and --scheduler-only are both passed" do
        expect do
          described_class.start(["start", "--workers-only", "--scheduler-only"])
        end.to raise_error(ArgumentError, /mutually exclusive/i)
      end

      it "raises ArgumentError when all three are passed" do
        expect do
          described_class.start(["start", "--workers-only", "--scheduler-only", "--dispatcher-only"])
        end.to raise_error(ArgumentError, /mutually exclusive/i)
      end
    end

    context "when composing role flags with --capsule" do
      it "allows --workers-only --capsule critical" do
        original_workers = Pgbus.configuration.workers
        original_roles = Pgbus.configuration.roles
        Pgbus.configuration.instance_variable_set(:@workers, nil)
        Pgbus.configuration.capsule(:critical, queues: %w[critical], threads: 5)

        described_class.start(["start", "--workers-only", "--capsule", "critical"])

        expect(Pgbus.configuration.roles).to eq([:workers])
        expect(Pgbus.configuration.workers.first[:name]).to eq("critical")
      ensure
        Pgbus.configuration.instance_variable_set(:@workers, original_workers)
        Pgbus.configuration.roles = original_roles
      end
    end
  end

  describe ".show_status" do
    it "prints processes table when processes exist" do
      process = double("ProcessEntry",
                       kind: "supervisor", hostname: "web-1", pid: "100",
                       last_heartbeat_at: "2026-01-01 00:00:00", metadata: "{}")
      scope = double("scope", none?: false, each: nil)
      allow(scope).to receive(:each).and_yield(process)
      allow(Pgbus::ProcessEntry).to receive_message_chain(:order, :select).and_return(scope) # rubocop:disable RSpec/MessageChain

      output = capture_stdout { described_class.show_status }

      expect(output).to include("KIND")
      expect(output).to include("supervisor")
      expect(output).to include("web-1")
    end

    it "prints 'no processes' when result is empty" do
      scope = double("scope", none?: true)
      allow(Pgbus::ProcessEntry).to receive_message_chain(:order, :select).and_return(scope) # rubocop:disable RSpec/MessageChain

      output = capture_stdout { described_class.show_status }

      expect(output).to include("No Pgbus processes running.")
    end
  end

  describe ".list_queues" do
    it "prints formatted table with queue metrics including the claimable age" do
      metric = double("metric",
                      queue_name: "pgbus_test_default",
                      queue_length: 10,
                      queue_visible_length: 8,
                      oldest_msg_age_sec: 42,
                      total_messages: 100)
      allow(mock_client).to receive_messages(metrics: [metric],
                                             oldest_claimable_ages: { "pgbus_test_default" => 12 })

      output = capture_stdout { described_class.list_queues }

      expect(output).to include("QUEUE")
      expect(output).to include("CLAIMABLE")
      expect(output).to include("pgbus_test_default")
      expect(output).to include("12")
    end

    it "prints a dash for a queue holding only vt-parked messages" do
      metric = double("metric",
                      queue_name: "pgbus_test_webhooks",
                      queue_length: 1,
                      queue_visible_length: 0,
                      oldest_msg_age_sec: 17_045,
                      total_messages: 500)
      allow(mock_client).to receive_messages(metrics: [metric],
                                             oldest_claimable_ages: { "pgbus_test_webhooks" => nil })

      output = capture_stdout { described_class.list_queues }

      expect(output).to match(/pgbus_test_webhooks.*17045.*-/)
    end
  end

  private

  def capture_stdout
    original = $stdout
    $stdout = StringIO.new
    yield
    $stdout.string
  ensure
    $stdout = original
  end
end
