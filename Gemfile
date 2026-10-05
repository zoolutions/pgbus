# frozen_string_literal: true

source "https://rubygems.org"

gemspec

gem "irb"
gem "rake", "~> 13.0"

group :development do
  gem "bundler-audit", "~> 0.9", require: false
  gem "i18n-tasks", "~> 1.0"
  gem "rubocop", "~> 1.21"
  gem "rubocop-rspec", "~> 3.0"
  gem "ruby-openai"
end

group :test do
  gem "rspec", "~> 3.0"
  gem "rspec-rails", "~> 7.0"

  # System tests
  gem "capybara", require: false
  gem "capybara-playwright-driver", require: false
  # Keep the ruby client in lockstep with the node driver pinned in bun.lock
  # (package.json "playwright"). The gem is otherwise unpinned+transitive, so a
  # fresh CI bundle grabs the newest client, which speaks a newer protocol than
  # the pinned driver and every browser launch dies with
  # "timeout: expected float, got undefined". Bump BOTH together.
  gem "playwright-ruby-client", "~> 1.58.0", require: false
  gem "puma"
  gem "sqlite3"

  # Streams subsystem integration tests. Puma above is already listed
  # for system tests; the streams integration specs boot a real Puma
  # server via PumaTestHarness. Falcon is optional and exists here so
  # the Falcon streaming body code path (v1.1) can be exercised in
  # tests without being a runtime dependency.
  gem "falcon", require: false

  # Performance & memory profiling
  gem "benchmark-ips", "~> 2.13"
  gem "memory_profiler", "~> 1.1"
  # Multi-thread, GVL-aware sampling profiler for the worker profiling bench
  # (benchmarks/worker_profile_bench.rb, issue #484).
  gem "vernier", "~> 1.0", require: false
end

group :development, :test do
  # Rails component constraint. Defaults to the full supported range (>= 7.1,
  # < 9.0), which resolves to the latest 8.x. The 7.1 endpoint gemfile
  # (gemfiles/rails_7_1.gemfile) sets RAILS_VERSION to pin the lower bound so
  # CI proves the whole support matrix, not just the top. See issue #284.
  rails_version = ENV.fetch("RAILS_VERSION", nil)
  rails_requirement = rails_version ? [rails_version] : [">= 7.1", "< 9.0"]
  gem "actioncable", *rails_requirement
  gem "activejob", *rails_requirement
  gem "activerecord", *rails_requirement
  gem "globalid", ">= 1.0"
  # Optional runtime dep for Pgbus::MCP (not in the gemspec). >= 0.23 is the
  # floor Pgbus::MCP::RackApp needs: that release added the transport's
  # DNS-rebinding options (allowed_hosts / allowed_origins /
  # dns_rebinding_protection), which the rack app passes through.
  gem "mcp", ">= 0.23"
  gem "pg", "~> 1.5"
  # Optional integration — pgbus ships a Phlex-includable pgbus_stream_from
  # helper (Pgbus::Streams::PhlexHelpers) but does NOT depend on phlex-rails.
  # Present here only so the helper module can be exercised in the test suite.
  gem "phlex-rails", "~> 2.0", require: false

  # Coverage measurement — a dev tool, not a gemspec runtime dep.
  # Loaded at the top of spec/spec_helper.rb before "pgbus" so lib files
  # are instrumented before they load.
  gem "simplecov", require: false
end
