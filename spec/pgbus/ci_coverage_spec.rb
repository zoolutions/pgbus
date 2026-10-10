# frozen_string_literal: true

require "yaml"

# Static guard (issue #499): every spec file under spec/ runs in at least one CI
# job. Parses the `rspec <paths>` arguments out of .github/workflows/main.yml and
# checks them against the spec tree, so adding a spec directory (or a top-level
# spec file) without wiring it into CI fails CI. No Rails, no database.
RSpec.describe "CI spec coverage" do # rubocop:disable RSpec/DescribeClass
  # rspec options whose value is the next token (`-I spec` must not count as a path).
  options_with_value = %w[
    -I -r --require -e --example -E --example-matches -t --tag -O --options
    -f --format -o --out --seed --order --pattern --exclude-pattern --default-path
  ].freeze

  define_method(:rspec_paths_in) do |command|
    command.scan(/\brspec\b([^\n;&|]*)/).flatten.flat_map do |args|
      tokens = args.split
      paths = []
      until tokens.empty?
        token = tokens.shift
        if options_with_value.include?(token)
          tokens.shift
        elsif !token.start_with?("-")
          paths << token
        end
      end
      paths.select { |path| path.start_with?("spec") }
    end
  end

  let(:root) { File.expand_path("../..", __dir__) }
  let(:excluded_dirs) { %w[dummy support] }

  let(:rspec_paths) do
    workflow = YAML.safe_load_file(File.join(root, ".github/workflows/main.yml"), aliases: true)
    steps = workflow.fetch("jobs").values.flat_map { |job| job.fetch("steps", []) }
    steps.filter_map { |step| step["run"] }.flat_map { |command| rspec_paths_in(command) }
  end

  let(:spec_files) do
    Dir.glob("spec/**/*_spec.rb", base: root)
       .reject { |path| excluded_dirs.include?(path.split("/")[1]) }
  end

  def covered?(path)
    rspec_paths.any? do |arg|
      normalized = arg.chomp("/")
      path == normalized || path.start_with?("#{normalized}/")
    end
  end

  describe "argument parsing" do
    it "skips option flags and the values they take" do
      expect(rspec_paths_in("bundle exec rspec -I spec --tag slow --format progress spec/requests/"))
        .to eq(["spec/requests/"])
    end

    it "skips --option=value tokens" do
      expect(rspec_paths_in("bundle exec rspec --require=spec_helper spec/pgbus_spec.rb")).to eq(["spec/pgbus_spec.rb"])
    end
  end

  it "finds rspec invocations in the workflow" do
    expect(rspec_paths).not_to be_empty
  end

  it "runs every spec file outside spec/dummy and spec/support in some CI job" do
    expect(spec_files.reject { |path| covered?(path) }).to be_empty
  end

  it "names only spec paths that exist" do
    expect(rspec_paths.reject { |arg| File.exist?(File.join(root, arg)) }).to be_empty
  end
end
