# frozen_string_literal: true

# SimpleCov must start before "pgbus" is required (below) so lib files are
# instrumented as they load. rails_helper.rb requires this file, so unit,
# generator, and request specs are all measured. NOTE: integration_helper.rb
# and system_helper.rb do NOT require spec_helper, so those runs are unmeasured.
require "simplecov"
SimpleCov.start do
  enable_coverage :branch

  add_filter "/spec/"

  add_group "Client", "lib/pgbus/client"
  add_group "ActiveJob", "lib/pgbus/active_job"
  add_group "Event Bus", "lib/pgbus/event_bus"
  add_group "Process", "lib/pgbus/process"
  add_group "Web", %w[lib/pgbus/web app/controllers app/views app/helpers]
  add_group "Generators", "lib/generators"

  # Baseline measured from `bundle exec rspec spec/pgbus/ spec/generators/`:
  # line 89.75% (7920/8825), branch 75.41% (2183/2895). Floors set below the
  # measured values so CI is green day one; ratchet toward the 80%/100% targets
  # in .claude/rules/testing.md in follow-ups.
  minimum_coverage line: 89, branch: 75
end

require "active_record"
require "pgbus"
require "conformance_kit/rspec"
require "globalid"

# GlobalID requires an app name to parse/create GIDs
GlobalID.app = "pgbus-test"

Dir[File.join(__dir__, "support/**/*.rb")].each { |f| require f }

RSpec.configure do |config|
  config.example_status_persistence_file_path = ".rspec_status"
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.order = :random
  Kernel.srand config.seed

  config.before(:suite) do
    Pgbus.configure do |c|
      c.logger = Logger.new(IO::NULL)
      c.queue_prefix = "pgbus_test"
      c.default_queue = "default"
    end
  end

  config.after(:suite) do
    Pgbus.reset!
  end
end
