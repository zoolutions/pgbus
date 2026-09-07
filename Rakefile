# frozen_string_literal: true

require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec) do |t|
  t.pattern = "spec/pgbus/**/*_spec.rb"
end

require "rubocop/rake_task"

# Lint only the gem's own source — NOT the nested docs/ site. docs/ is a separate
# consuming app with its own bundle and .rubocop.yml (it inherit_gems
# rubocop-rails-omakase and requires docs_kit/rubocop, both absent from the gem's
# bundle). RuboCop loads a directory's .rubocop.yml while scanning it — before
# AllCops/Exclude applies — so a bare run discovers docs/.rubocop.yml and crashes
# on the unresolvable gem inheritance. Passing explicit paths stops the
# discovery. docs/ lints itself in the Docs site workflow. (Same fix as docs-kit.)
RuboCop::RakeTask.new do |task|
  task.patterns = %w[app benchmarks config lib spec Gemfile Rakefile pgbus.gemspec]
end

namespace :bench do
  bench_dir = "benchmarks"
  # Benches that need a real PostgreSQL/PGMQ (or boot Puma) — excluded from the
  # no-DB unit suite that bench:all runs in CI.
  db_benches = %w[connection_pool_bench integration_bench streams_bench streams_read_pool_bench
                  execution_modes_bench pool_swap_bench pool_autoscale_bench job_burst_bench
                  notify_wake_bench notify_chaos_bench streams_hub_bench fair_read_bench].freeze
  # The unit suite is every *_bench.rb that doesn't need a database, derived
  # from the directory so a new unit bench is picked up automatically (kept in
  # sync with bench:one, which globs the same files).
  unit_benches = Dir["#{bench_dir}/*_bench.rb"]
                 .map { |f| File.basename(f, ".rb") }
                 .reject { |name| db_benches.include?(name) }
                 .sort
                 .freeze

  desc "Run serialization benchmarks"
  task :serialization do
    ruby "benchmarks/serialization_bench.rb"
  end

  desc "Run client operation benchmarks"
  task :client do
    ruby "benchmarks/client_bench.rb"
  end

  desc "Run executor benchmarks"
  task :executor do
    ruby "benchmarks/executor_bench.rb"
  end

  desc "Run detailed memory profiling"
  task :memory do
    ruby "benchmarks/memory_profile.rb"
  end

  desc "Run integration benchmarks (requires PGBUS_DATABASE_URL)"
  task :integration do
    ruby "benchmarks/integration_bench.rb"
  end

  desc "Run fair share read benchmark (requires PGBUS_DATABASE_URL)"
  task :fair_read do
    ruby "benchmarks/fair_read_bench.rb"
  end

  desc "Run streams benchmarks (requires PGBUS_DATABASE_URL; boots real Puma + SSE)"
  task :streams do
    ruby "benchmarks/streams_bench.rb"
  end

  desc "Run execution-mode connection benchmark (threads vs async pool usage; requires PGBUS_DATABASE_URL)"
  task :execution_modes do
    ruby "benchmarks/execution_modes_bench.rb"
  end

  desc "Run streams-pool hot-swap benchmark (zero-loss/zero-leak/cost under load; requires PGBUS_DATABASE_URL)"
  task :pool_swap do
    ruby "benchmarks/pool_swap_bench.rb"
  end

  desc "Run streams-pool autoscaler benchmark (tick cost + grow-under-load; requires PGBUS_DATABASE_URL)"
  task :pool_autoscale do
    ruby "benchmarks/pool_autoscale_bench.rb"
  end

  desc "Run job-burst gate benchmark (#323 phase 3: is the job pool or DB pool the burst limiter?; requires PGBUS_DATABASE_URL)"
  task :job_burst do
    ruby "benchmarks/job_burst_bench.rb"
  end

  desc "Run NOTIFY wake-path benchmark (#381 wake latency + connection census; requires PGBUS_DATABASE_URL)"
  task :notify_wake do
    ruby "benchmarks/notify_wake_bench.rb"
  end

  desc "Run NotifyHub failure-mode measurements (#381 chaos scenarios; requires PGBUS_DATABASE_URL)"
  task :notify_chaos do
    ruby "benchmarks/notify_chaos_bench.rb"
  end

  desc "Run streams master-hub latency benchmark (#382 hop cost + census; requires PGBUS_DATABASE_URL)"
  task :streams_hub do
    ruby "benchmarks/streams_hub_bench.rb"
  end

  desc "Run a single benchmark: rake bench:one[client_bench]"
  task :one, [:name] do |_t, args|
    name = args[:name] or abort "Usage: rake bench:one[serialization_bench|client_bench|...]"
    available = Dir["#{bench_dir}/*_bench.rb"].map { |f| File.basename(f, ".rb") }
    abort "No such benchmark: #{name}. Available: #{available.sort.join(", ")}" unless available.include?(name)
    ruby "#{bench_dir}/#{name}.rb"
  end

  desc "Run all unit-level benchmarks, save report to tmp/benchmarks/"
  task :all do
    require "fileutils"
    require "open3"
    require "rbconfig"
    FileUtils.mkdir_p("tmp/benchmarks")

    # Run each bench under the SAME Ruby + gemset as this Rake process, not a
    # bare `ruby` from PATH — otherwise before/after numbers are unreliable and
    # gem loading can break under a different interpreter.
    ruby = RbConfig.ruby
    failed = []

    File.open("tmp/benchmarks/unit.txt", "w") do |report|
      unit_benches.each do |name|
        file = "#{bench_dir}/#{name}.rb"
        header = "\n### #{file} ###"
        puts "\e[1;35m#{header}\e[0m"
        report.puts header

        result, status = Open3.capture2e(ruby, file)
        puts result
        report.puts result.gsub(/\e\[[0-9;]*m/, "")

        unless status.success?
          failed << file
          report.puts "!!! FAILED (exit #{status.exitstatus})"
        end
      end
    end

    puts "\nSaved report to tmp/benchmarks/unit.txt"
    abort "\nBenchmark(s) failed: #{failed.join(", ")}" if failed.any?
  end
end

desc "Run all benchmarks (alias for bench:all)"
task bench: "bench:all"

desc "Build gem and verify contents"
task :build do
  sh("gem build pgbus.gemspec --strict")
  gem_file = Dir["pgbus-*.gem"].first
  abort "Gem file not found after build" unless gem_file

  sh("gem unpack #{gem_file} --target /tmp/gem-verify")
  puts "\n=== Gem contents ==="
  sh("find /tmp/gem-verify -type f | sort")
  sh("rm -rf /tmp/gem-verify #{gem_file}")
end

desc "Release a new version (rake release[1.2.3] or rake release[pre] or rake release[1.2.3,force])"
task :release, %i[version force] do |_t, args|
  require_relative "lib/pgbus/version"

  def info(msg)    = puts "\e[34m→\e[0m #{msg}"
  def success(msg) = puts "\e[32m✓\e[0m #{msg}"
  def skip(msg)    = puts "\e[33m⊘\e[0m #{msg} \e[33m(skipped)\e[0m"
  def warn(msg)    = puts "\e[33m⚠\e[0m #{msg}"
  def error(msg)   = puts "\e[31m✗\e[0m #{msg}"
  def header(msg)  = puts "\n\e[1;36m#{msg}\e[0m\n#{"─" * msg.length}"

  new_version = args[:version]
  abort "\e[31mUsage: rake release[X.Y.Z] or rake release[X.Y.Z,force]\e[0m" unless new_version

  force = args[:force]&.to_s&.downcase == "force"

  dirty = `git status --porcelain`.strip
  abort "\e[31mAborting: working directory is not clean.\e[0m\n#{dirty}" unless dirty.empty?

  current = Pgbus::VERSION
  prerelease = new_version.match?(/alpha|beta|rc|pre/) || new_version == "pre"

  if new_version == "pre"
    new_version = current
    prerelease = true
  end

  tag = "v#{new_version}"
  version_file = "lib/pgbus/version.rb"

  title = "Release #{tag}"
  title += " (force)" if force
  header title
  info "Current version: #{current}"
  info "New version:     #{new_version}"
  info "Pre-release:     #{prerelease}"

  # Step 0: Force cleanup — delete existing release and tag
  if force
    header "Force cleanup"
    if system("gh release view #{tag} >/dev/null 2>&1")
      sh("gh release delete #{tag} --yes --cleanup-tag")
      success "Deleted release and remote tag #{tag}"
    else
      skip "No release #{tag} to delete"
    end

    if system("git rev-parse #{tag} >/dev/null 2>&1")
      sh("git tag -d #{tag}")
      success "Deleted local tag #{tag}"
    else
      skip "No local tag #{tag} to delete"
    end
  end

  # Step 1: Update version file
  header "Version"
  if new_version == current
    skip "Version already #{new_version}"
  else
    content = File.read(version_file)
    content.sub!(/VERSION = ".*"/, "VERSION = \"#{new_version}\"")
    File.write(version_file, content)
    success "Updated #{version_file}"
  end

  # Step 1b: Regenerate the frozen lockfiles that pin the pgbus path gem, so the
  # bump ships with them in sync. These are installed with `--frozen`/deployment
  # in CI, so if they still name the OLD version they instant-fail (the root
  # Gemfile.lock on every main-Gemfile leg AND release.yml's own `bundle install`,
  # the Rails 7.1 leg with exit 16, and docs-CI on any docs change). Regenerating
  # here keeps the version-pin drift out of the release commit instead of
  # surfacing on the next PR — or, worse, in the Release workflow itself.
  header "Frozen lockfiles"
  # The ONLY thing a version bump changes in these frozen lockfiles is the pgbus
  # path-gem pin — so bump exactly that line, in place, with a string edit.
  #
  # We deliberately do NOT run `bundle lock` here: a full re-resolve trips over
  # constraints that have nothing to do with pgbus. Concretely, docs/Gemfile.lock
  # carries a broad PLATFORMS list (…-gnu / …-musl / arm-linux) for which a
  # platform gem like `thruster` ships no variant, so `bundle lock` fails with
  # "Could not find gems matching 'thruster' valid for all resolution platforms"
  # on any machine whose cache doesn't already hold those exact gems — aborting
  # the release. `bundle lock --local` was even worse (wrong-file write + no
  # fetch). A targeted pin edit sidesteps all of it, is deterministic on any
  # machine, and produces the minimal 2-line diff (the PATH spec + the
  # DEPENDENCIES pin). See #338/#341 and the surgical-bump fix.
  frozen_lockfiles = %w[Gemfile.lock gemfiles/rails_7_1.gemfile.lock docs/Gemfile.lock]
  regenerated_lockfiles = []
  frozen_lockfiles.each do |lockfile|
    unless File.exist?(lockfile)
      skip "#{lockfile} not present"
      next
    end

    content = File.read(lockfile)
    # Matches both the PATH-source spec ("    pgbus (X.Y.Z)") and the
    # DEPENDENCIES pin ("  pgbus (X.Y.Z)"), leaving everything else untouched.
    bumped = content.gsub(/^(\s+pgbus) \([^)]*\)$/, "\\1 (#{new_version})")

    if bumped == content
      skip "#{lockfile} — no pgbus pin to bump"
      next
    end

    File.write(lockfile, bumped)
    regenerated_lockfiles << lockfile
    success "Bumped pgbus pin in #{lockfile}"
  end

  # Step 2: Verify gem builds cleanly
  header "Build verification"
  sh("gem build pgbus.gemspec --strict")
  sh("rm -f pgbus-*.gem")
  success "Gem builds cleanly"

  # Step 3: Commit version bump (+ any re-synced lockfiles)
  header "Git commit"
  paths_to_stage = [version_file, *regenerated_lockfiles]
  staged_changes = paths_to_stage.any? do |path|
    !`git diff #{path}`.strip.empty? || !`git diff --cached #{path}`.strip.empty?
  end
  if staged_changes
    paths_to_stage.each { |path| sh("git add #{path}") }
    sh("git commit -m 'chore: bump version to #{new_version}'")
    success "Committed version bump"
  else
    skip "No version change to commit"
  end

  # Step 4: Push to origin
  header "Git push"
  local_sha = `git rev-parse HEAD`.strip
  remote_sha = `git rev-parse origin/main 2>/dev/null`.strip
  if local_sha == remote_sha
    skip "origin/main already at #{local_sha[0..6]}"
  else
    sh("git push origin main")
    success "Pushed to origin/main"
  end

  # Step 5: Create release
  header "Release"
  tag_exists = system("git rev-parse #{tag} >/dev/null 2>&1")
  release_exists = system("gh release view #{tag} >/dev/null 2>&1")

  if release_exists
    skip "Release #{tag} already exists (use force to re-create)"
  elsif tag_exists
    info "Tag #{tag} exists, creating release from it"
    pre_flag = prerelease ? "--prerelease" : ""
    sh("gh release create #{tag} --generate-notes #{pre_flag}".strip)
    success "Release #{tag} created from existing tag"
  else
    pre_flag = prerelease ? "--prerelease" : ""
    sh("gh release create #{tag} --generate-notes --target main #{pre_flag}".strip)
    success "Release #{tag} created"
  end

  puts ""
  success "\e[1mRelease #{tag} complete!\e[0m CI will handle the rest:"
  puts "    • Run tests"
  puts "    • Build + verify gem"
  puts "    • Sign with Sigstore"
  puts "    • Publish to RubyGems"
  puts "    • Upload assets to the release"
end

namespace :frontend do
  # app/frontend/pgbus/style.css is a committed build artifact — the engine ships
  # it so host apps need no Node toolchain. Nothing rebuilds it automatically, so
  # run this after adding a Tailwind class to a view; spec/pgbus/web/
  # compiled_css_coverage_spec.rb fails the build if you forget.
  desc "Recompile app/frontend/pgbus/style.css from the dashboard views"
  task :css do
    version = File.read("bun.lock")[/"tailwindcss@([\d.]+)"/, 1] or
      abort "Could not read the tailwindcss version from bun.lock"

    sh("bunx @tailwindcss/cli@#{version} " \
       "-i app/frontend/pgbus/tailwind.css " \
       "-o app/frontend/pgbus/style.css --minify")
  end
end

namespace :dummy do
  desc "Start dummy app with stub data for dashboard QA (PORT=3003, no database required)"
  task :server do
    port = ENV.fetch("PORT", "3003")
    ENV["PGBUS_STUB_DATA"] = "1"
    ENV["RAILS_ENV"] = "development"

    puts "\n\e[32m→ Starting dummy app with stub data at http://localhost:#{port}/pgbus\e[0m"
    puts "  Dashboard:  http://localhost:#{port}/pgbus"
    puts "  Using stub data source (no database needed)"
    puts "  Press Ctrl+C to stop\n\n"
    sh("bundle exec puma spec/dummy/config.ru -p #{port}")
  end
end

load File.expand_path("lib/tasks/pgbus_streams.rake", __dir__)

task default: %i[spec rubocop pgbus:streams:lint_no_live]
