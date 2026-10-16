# frozen_string_literal: true

require "spec_helper"
require "open3"

# A real boot, in a fresh process (script/boot_finalized.rb), finalizes the
# configuration with no app code calling Pgbus.finalize!: the engine's
# after_initialize in a Rails app, the end of Pgbus.configure without Rails.
# Each boot configures values no Ractor can share (a lambda closing over app
# state, a Logger, a filter list holding a proc); they are kept as they are.
RSpec.describe "Booting pgbus" do # rubocop:disable RSpec/DescribeClass
  def boot(mode)
    stdout, stderr, status = Open3.capture3(
      "bundle", "exec", "ruby", File.expand_path("../../script/boot_finalized.rb", __dir__), mode
    )
    expect(status).to be_success, "#{stdout}\n#{stderr}"
    stdout.lines.filter_map { |line| line.chomp.split("=", 2) if line.include?("=") }.to_h
  end

  # Frozen and shareable data; the app's callables, logger and proc-holding
  # filter list are the very objects it configured, and they still work.
  let(:finalized_boot) do
    { "FINALIZED" => "true", "SHAREABLE" => "true", "CALLABLES_KEPT" => "true", "AUTH_CALLED" => "true",
      "LOGGER_KEPT" => "true", "FILTERS_KEPT" => "true" }
  end

  it "finalizes a Rails app's configuration after its own after_initialize blocks" do
    expect(boot("rails")).to include(finalized_boot.merge("RAILS" => "true", "LATE_PATTERN" => "true"))
  end

  it "finalizes at the end of Pgbus.configure without Rails" do
    expect(boot("plain")).to include(finalized_boot.merge("RAILS" => "false"))
  end
end
