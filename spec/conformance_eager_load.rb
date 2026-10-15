# frozen_string_literal: true

# conformance-kit's audit.require (.conformance.yml): boots the dummy app and
# loads every constant pgbus defines, so `conformance-audit --strict` with
# strict_autoloads sees all of it.
ENV["RAILS_ENV"] ||= "test"
require_relative "dummy/config/environment"

# app/: the engine's own autoload directories. Rails 8.1's Engine#eager_load!
# is a no-op (Zeitwerk::Loader.eager_load_all does that work at boot), and the
# dummy app boots with eager_load off.
engine_root = Pgbus::Engine.root.to_s
autoloader = Rails.autoloaders.main
autoloader.dirs.select { |dir| dir.start_with?(engine_root) }.each { |dir| autoloader.eager_load_dir(dir) }

# lib/pgbus: the gem's own Zeitwerk loader.
Pgbus.loader.eager_load

# What Pgbus.loader ignores on purpose, loaded the way an app loads it: the
# ActiveJob adapter (the dummy app has no Active Job railtie), the optional
# integrations and the test helpers. Not loaded: the Rails generators
# (lib/generators, run only by `rails generate`) and the Puma plugin
# (lib/puma, Puma's namespace, run in the Puma master).
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"
require "rspec/core"
require "rspec/expectations"
require "phlex-rails"
require "pgbus/streams/phlex_helpers"
require "pgbus/integrations/appsignal"
require "pgbus/testing/rspec"
require "pgbus/testing/minitest"
Pgbus::MCP.load!

# A model resolves its primary key on first use; until then Rails parks a
# BasicObject sentinel in @primary_key, which conformance-kit 0.3.0's audit
# cannot describe (it calls #class on the value and raises). Resolve each, as
# the first query would, so the audit sees the key itself.
[Pgbus::BusRecord, *Pgbus::BusRecord.descendants].each(&:primary_key)
