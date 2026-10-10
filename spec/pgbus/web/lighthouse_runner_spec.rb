# frozen_string_literal: true

require "spec_helper"
require "open3"

# bin/lighthouse runs Lighthouse over lighthouserc.dashboard.json's pages
# against a running rake dummy:server. These examples need no Chrome: they
# cover the option parsing and the "nothing to audit" exit.
RSpec.describe "bin/lighthouse" do # rubocop:disable RSpec/DescribeClass
  let(:root) { Pathname.new(__dir__).join("../../..").expand_path }
  let(:script) { root.join("bin/lighthouse").to_s }

  def run(*args)
    Bundler.with_unbundled_env { Open3.capture3(script, *args, chdir: root.to_s) }
  end

  it "is executable" do
    expect(File.executable?(script)).to be(true)
  end

  it "prints its options with --help and exits 0" do
    stdout, _stderr, status = run("--help")

    expect(status.exitstatus).to eq(0)
    expect(stdout).to include("--base-url", "--pages", "--iterations", "--output")
  end

  it "exits 1 when --pages matches no audited URL" do
    _stdout, stderr, status = run("--pages", "/__none__")

    expect(status.exitstatus).to eq(1)
    expect(stderr).to include("No URLs matched")
  end
end
