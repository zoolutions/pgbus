# frozen_string_literal: true

require "spec_helper"

# Unit coverage for the version-listing logic used by the
# `pgbus:pgmq:versions` and `pgbus:pgmq:status` rake tasks
# (lib/tasks/pgbus_pgmq.rake). The tasks require a Rails :environment
# and a live database to run end to end, so we exercise the pure
# version-comparison logic they depend on here.
RSpec.describe "pgbus:pgmq rake task logic" do # rubocop:disable RSpec/DescribeClass
  describe "versions listing (mirrors pgbus:pgmq:versions)" do
    it "lists every vendored version with the newest marked latest" do
      latest = Pgbus::PgmqSchema.latest_version

      lines = Pgbus::PgmqSchema.available_versions.map do |v|
        marker = v == latest ? " (latest)" : ""
        "  #{v}#{marker}"
      end

      expect(lines).to eq(["  1.11.0", "  1.11.1", "  1.12.0", "  1.13.0 (latest)"])
    end
  end

  describe "status comparison (mirrors pgbus:pgmq:status)" do
    it "flags an update when an older version is installed" do
      installed = "1.11.0"
      latest = Pgbus::PgmqSchema.latest_version

      expect(Gem::Version.new(installed) < Gem::Version.new(latest)).to be(true)
    end

    it "reports up to date when the latest version is installed" do
      installed = Pgbus::PgmqSchema.latest_version
      latest = Pgbus::PgmqSchema.latest_version

      expect(Gem::Version.new(installed) < Gem::Version.new(latest)).to be(false)
    end
  end
end
