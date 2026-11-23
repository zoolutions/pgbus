# frozen_string_literal: true

require "spec_helper"

# Issue #689: `pgbus_coalesce false/true` declares whether a Pgbus.coalesce
# scope may hold a job class; without it the class follows
# config.coalesce_default.
RSpec.describe Pgbus::ActiveJob::Coalesce::JobMixin do
  let(:job_class) do
    Class.new do
      include Pgbus::ActiveJob::Coalesce::JobMixin
    end
  end

  around do |example|
    original = Pgbus.configuration.coalesce_default
    example.run
  ensure
    Pgbus.configuration.coalesce_default = original
  end

  it "follows config.coalesce_default when the class declares nothing" do
    expect(job_class.pgbus_coalesce_enabled).to be_nil
    expect(job_class.pgbus_coalesce?).to be(true)

    Pgbus.configuration.coalesce_default = false

    expect(job_class.pgbus_coalesce?).to be(false)
  end

  it "opts a class out" do
    job_class.pgbus_coalesce false

    expect(job_class.pgbus_coalesce_enabled).to be(false)
    expect(job_class.pgbus_coalesce?).to be(false)
  end

  it "opts a class in, over a false default" do
    Pgbus.configuration.coalesce_default = false
    job_class.pgbus_coalesce

    expect(job_class.pgbus_coalesce_enabled).to be(true)
    expect(job_class.pgbus_coalesce?).to be(true)
  end

  it "is inherited by subclasses and overridable per subclass" do
    job_class.pgbus_coalesce false
    subclass = Class.new(job_class)
    override = Class.new(job_class) { pgbus_coalesce true }

    expect(subclass.pgbus_coalesce?).to be(false)
    expect(override.pgbus_coalesce?).to be(true)
  end
end
