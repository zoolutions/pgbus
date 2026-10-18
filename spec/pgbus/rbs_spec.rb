# frozen_string_literal: true

require "rails_helper"
require "rbs"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# sig/ is the typed public surface (issue #527): `bundle exec rbs -I sig
# validate` checks the types, and this spec checks the files cover what the
# code defines, both ways, so a new public method needs a signature and a
# removed one takes its signature with it.
RSpec.describe "the RBS signatures in sig/" do # rubocop:disable RSpec/DescribeClass
  let(:environment) do
    loader = RBS::EnvironmentLoader.new(core_root: nil)
    loader.add(path: Pathname(File.expand_path("../../sig", __dir__)))
    RBS::Environment.from_loader(loader).resolve_type_names
  end

  # Method names declared for +constant+ (:instance or :singleton), from every
  # declaration of it across the files.
  def self.declared(env, constant, kind)
    entry = env.class_decls.fetch(RBS::TypeName.parse("::#{constant}"))
    entry.each_decl.flat_map(&:members)
         .grep(RBS::AST::Members::MethodDefinition)
         .select { |member| [kind, :singleton_instance].include?(member.kind) }
         .map(&:name).reject { |name| name == :initialize }.sort
  end

  # Defined in pgbus's own lib/: not inherited from Object, and not added by
  # ActiveSupport or by Rails (isolate_namespace defines Pgbus.table_name_prefix).
  # Walks past a wrapper prepended by a spec helper (integration_helper.rb's
  # reset_client!) to the definition it wraps, so the result does not depend
  # on which spec files share the process.
  def self.pgbus_defined?(method)
    lib = File.expand_path("../../lib", __dir__)
    while method
      return true if method.source_location&.first.to_s.start_with?(lib)

      method = method.super_method
    end
    false
  end

  # Public instance methods pgbus defines, wherever in the ancestry it does.
  def self.own_instance_methods(klass)
    klass.public_instance_methods.select { |name| pgbus_defined?(klass.instance_method(name)) }.sort
  end

  def self.own_singleton_methods(mod)
    mod.singleton_methods(false).select { |name| pgbus_defined?(mod.method(name)) }.sort
  end

  # Pgbus::Streams.install_* live in files the engine loads on demand.
  [Pgbus::Streams::TurboBroadcastable, Pgbus::Streams::TurboStreamOverride]

  {
    "Pgbus" => -> { [own_singleton_methods(Pgbus), :singleton] },
    "Pgbus::Configuration" => -> { [own_instance_methods(Pgbus::Configuration), :instance] },
    "Pgbus::Client" => -> { [own_instance_methods(Pgbus::Client), :instance] },
    "Pgbus::Client (class methods)" => -> { [own_singleton_methods(Pgbus::Client), :singleton] },
    "Pgbus::Streams" => -> { [own_singleton_methods(Pgbus::Streams), :singleton] },
    "Pgbus::Streams::Stream" => -> { [own_instance_methods(Pgbus::Streams::Stream), :instance] },
    "Pgbus::Streams::Stream (class methods)" => -> { [own_singleton_methods(Pgbus::Streams::Stream), :singleton] },
    "Pgbus::ActiveJob::Adapter" => -> { [own_instance_methods(Pgbus::ActiveJob::Adapter), :instance] },
    "ActiveJob::QueueAdapters::PgbusAdapter" => -> { [own_instance_methods(ActiveJob::QueueAdapters::PgbusAdapter), :instance] },
    "Pgbus::Concurrency::ClassMethods" => -> { [Pgbus::Concurrency::ClassMethods.public_instance_methods(false).sort, :instance] },
    "Pgbus::Uniqueness::ClassMethods" => -> { [Pgbus::Uniqueness::ClassMethods.public_instance_methods(false).sort, :instance] },
    "Pgbus::RetryBackoff::JobMixin::ClassMethods" =>
      -> { [Pgbus::RetryBackoff::JobMixin::ClassMethods.public_instance_methods(false).sort, :instance] },
    "Pgbus::VisibilityHeartbeat::JobMixin::ClassMethods" =>
      -> { [Pgbus::VisibilityHeartbeat::JobMixin::ClassMethods.public_instance_methods(false).sort, :instance] }
  }.each do |label, reflect|
    it "declares exactly the public methods of #{label}" do
      methods, kind = reflect.call
      constant = label.delete_suffix(" (class methods)")

      expect(self.class.declared(environment, constant, kind)).to eq(methods)
    end
  end

  # The typed surface is for the apps that install the gem.
  it "ships with the gem" do
    root = File.expand_path("../..", __dir__)
    signatures = Dir.glob("sig/**/*.rbs", base: root)
    packaged = Gem::Specification.load(File.join(root, "pgbus.gemspec")).files

    expect(signatures).not_to be_empty
    expect(packaged).to include(*signatures)
  end
end
