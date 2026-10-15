# frozen_string_literal: true

require "spec_helper"
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

  # Public instance methods defined by pgbus itself (not inherited from Object
  # or added by ActiveSupport), wherever in the ancestry pgbus defines them.
  def self.own_instance_methods(klass)
    klass.public_instance_methods.select do |name|
      klass.instance_method(name).owner.name.to_s.start_with?("Pgbus", "ActiveJob::QueueAdapters::Pgbus")
    end.sort
  end

  {
    "Pgbus" => -> { [Pgbus.singleton_methods(false).sort, :singleton] },
    "Pgbus::Configuration" => -> { [own_instance_methods(Pgbus::Configuration), :instance] },
    "Pgbus::Client" => -> { [own_instance_methods(Pgbus::Client), :instance] },
    "Pgbus::Client (class methods)" => -> { [Pgbus::Client.singleton_methods(false).sort, :singleton] },
    "Pgbus::Streams" => -> { [Pgbus::Streams.singleton_methods(false).sort, :singleton] },
    "Pgbus::Streams::Stream" => -> { [own_instance_methods(Pgbus::Streams::Stream), :instance] },
    "Pgbus::Streams::Stream (class methods)" => -> { [Pgbus::Streams::Stream.singleton_methods(false).sort, :singleton] },
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
end
