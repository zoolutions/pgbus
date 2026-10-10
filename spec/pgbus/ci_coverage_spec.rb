# frozen_string_literal: true

require "yaml"

# Static guard (issue #499): every spec file under spec/ runs in at least one CI
# job. Parses the `rspec <paths>` arguments out of .github/workflows/main.yml and
# checks them against the spec tree, so adding a spec directory (or a top-level
# spec file) without wiring it into CI fails CI. No Rails, no database.
RSpec.describe "CI spec coverage" do # rubocop:disable RSpec/DescribeClass
  let(:root) { File.expand_path("../..", __dir__) }
  let(:excluded_dirs) { %w[dummy support] }

  let(:rspec_paths) do
    workflow = YAML.safe_load_file(File.join(root, ".github/workflows/main.yml"), aliases: true)
    steps = workflow.fetch("jobs").values.flat_map { |job| job.fetch("steps", []) }
    steps.filter_map { |step| step["run"] }.flat_map do |command|
      command.scan(/\brspec\b([^\n;&|]*)/).flatten.flat_map(&:split).select { |arg| arg.start_with?("spec") }
    end
  end

  let(:top_level_files) { Dir.glob("spec/*_spec.rb", base: root) }

  let(:top_level_dirs) do
    Dir.glob("spec/*/", base: root).map { |dir| dir.chomp("/") }
       .reject { |dir| excluded_dirs.include?(File.basename(dir)) }
       .select { |dir| Dir.glob("#{dir}/**/*_spec.rb", base: root).any? }
  end

  def covered?(path)
    rspec_paths.any? do |arg|
      normalized = arg.chomp("/")
      path == normalized || path.start_with?("#{normalized}/")
    end
  end

  it "finds rspec invocations in the workflow" do
    expect(rspec_paths).not_to be_empty
  end

  it "runs every top-level spec file in some CI job" do
    expect(top_level_files.reject { |path| covered?(path) }).to be_empty
  end

  it "runs every top-level spec directory in some CI job" do
    expect(top_level_dirs.reject { |path| covered?(path) }).to be_empty
  end

  it "names only spec paths that exist" do
    expect(rspec_paths.reject { |arg| File.exist?(File.join(root, arg)) }).to be_empty
  end
end
