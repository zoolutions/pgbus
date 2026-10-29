# frozen_string_literal: true

require "spec_helper"
require "open3"
require "rbconfig"

# The bench script validates its knobs before it touches a database (issue
# #602): run it as a subprocess without PGBUS_DATABASE_URL and read how it stops.
RSpec.describe "benchmarks/worker_profile_bench.rb knobs" do # rubocop:disable RSpec/DescribeClass
  let(:script) { File.expand_path("../../benchmarks/worker_profile_bench.rb", __dir__) }

  def run_bench(env)
    clean = { "PGBUS_DATABASE_URL" => nil, "WP_BENCH_ROLE" => nil, "WP_BENCH_PROCESSES" => nil,
              "WPROF_CHILD" => nil, "WPROF_DRAIN_CHILD" => nil }
    out, status = Open3.capture2e(clean.merge(env), RbConfig.ruby, script)
    [out, status]
  end

  it "accepts WP_BENCH_ROLE=fleet and WP_BENCH_PROCESSES=1,2,4,8, then asks for a database" do
    out, status = run_bench("WP_BENCH_ROLE" => "fleet", "WP_BENCH_PROCESSES" => "1,2,4,8")

    expect(status).not_to be_success
    expect(out).to include("PGBUS_DATABASE_URL not set")
  end

  it "lists fleet and all among the roles when the role is unknown" do
    out, = run_bench("WP_BENCH_ROLE" => "nope")

    expect(out).to include("WP_BENCH_ROLE must be one of worker, consumer, both, fleet, all (got nope)")
  end

  it "rejects a zero or non-integer process count" do
    %w[0 two].each do |value|
      out, status = run_bench("WP_BENCH_PROCESSES" => value)

      expect(status).not_to be_success
      expect(out).to include("WP_BENCH_PROCESSES must list positive integers")
    end
  end
end
