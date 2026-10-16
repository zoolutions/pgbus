# frozen_string_literal: true

require_relative "lib/pgbus/version"

Gem::Specification.new do |spec|
  spec.name = "pgbus"
  spec.version = Pgbus::VERSION
  spec.authors = ["Mikael Henriksson"]
  spec.email = ["mikael@mhenrixon.com"]

  spec.summary = "PostgreSQL-native job processing and event bus for Rails, built on PGMQ"
  spec.description = <<~DESC
    Pgbus is a PostgreSQL-native job processor and event bus for Ruby on Rails.
    Built on top of PGMQ, it provides ActiveJob integration, AMQP-style topic routing,
    idempotent event handling, worker memory management, dead letter queues,
    and a built-in dashboard with Hotwire support.
  DESC
  spec.homepage = "https://github.com/zoolutions/pgbus"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.3.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/zoolutions/pgbus/tree/main"
  spec.metadata["changelog_uri"] = "https://github.com/zoolutions/pgbus/blob/main/CHANGELOG.md"
  spec.metadata["documentation_uri"] = "https://pgbus.zoolutions.llc"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).select do |f|
      f.start_with?("app/", "config/", "exe/", "lib/", "sig/") ||
        f == "CHANGELOG.md" || f == "LICENSE.txt" || f == "README.md" || f == "Rakefile"
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "concurrent-ruby", "~> 1.2"
  spec.add_dependency "fugit", "~> 1.11", ">= 1.11.1"
  spec.add_dependency "globalid", "~> 1.0"
  spec.add_dependency "pgmq-ruby", "~> 0.7.1"
  spec.add_dependency "railties", ">= 7.1", "< 9.0"
  spec.add_dependency "zeitwerk", "~> 2.6"
end
