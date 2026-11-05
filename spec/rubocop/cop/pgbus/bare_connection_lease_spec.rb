# frozen_string_literal: true

require "spec_helper"
require "rubocop"
# See no_ruby_timeout_spec.rb for why these are required one by one.
require "rubocop/rspec/cop_helper"
require "rubocop/rspec/expect_offense"
require "rubocop/rspec/shared_contexts"
require_relative "../../../../rubocop/pgbus"

RSpec.describe RuboCop::Cop::Pgbus::BareConnectionLease do
  include CopHelper
  include RuboCop::RSpec::ExpectOffense

  include_context "config"

  let(:config) { RuboCop::Config.new }

  def lease_msg(method) = "Pgbus/BareConnectionLease: #{format(described_class::LEASE_MSG, method: method)}"
  def transaction_msg = "Pgbus/BareConnectionLease: #{described_class::TRANSACTION_MSG}"

  it "flags a bare connection, on no receiver, self or a constant" do
    expect_offense(<<~RUBY)
      connection.exec_query(sql, "Name", binds)
      ^^^^^^^^^^ #{lease_msg("connection")}
      self.connection.select_value(sql)
           ^^^^^^^^^^ #{lease_msg("connection")}
      Pgbus::Semaphore.connection.exec_query(sql)
                       ^^^^^^^^^^ #{lease_msg("connection")}
      ActiveRecord::Base.lease_connection.execute(sql)
                         ^^^^^^^^^^^^^^^^ #{lease_msg("lease_connection")}
    RUBY
  end

  it "flags a bare connection inside with_connection, which would make the lease sticky" do
    expect_offense(<<~RUBY)
      connection_pool.with_connection do |conn|
        connection.exec_query(sql)
        ^^^^^^^^^^ #{lease_msg("connection")}
      end
    RUBY
  end

  it "accepts the block parameter of connection_pool.with_connection" do
    expect_no_offenses(<<~RUBY)
      connection_pool.with_connection { |conn| conn.exec_query(sql, "Name", binds) }
      Pgbus::Semaphore.connection_pool.with_connection do |conn|
        conn.select_value(sql)
      end
    RUBY
  end

  it "accepts a connection read off another object" do
    expect_no_offenses(<<~RUBY)
      client.connection.exec(sql)
      @pool.connection
    RUBY
  end

  it "flags a transaction outside with_connection" do
    expect_offense(<<~RUBY)
      def signal(key)
        Pgbus::Semaphore.transaction do
                         ^^^^^^^^^^^ #{transaction_msg}
          release(key)
        end
      end

      def check
        transaction(requires_new: true) { lock.find_by(id: 1) }
        ^^^^^^^^^^^ #{transaction_msg}
      end
    RUBY
  end

  it "flags a transaction whose nearest block is not with_connection" do
    expect_offense(<<~RUBY)
      connection_pool.with_connection do
        keys.each do |key|
          Pgbus::Semaphore.transaction { release(key) }
                           ^^^^^^^^^^^ #{transaction_msg}
        end
      end
    RUBY
  end

  it "flags a transaction in a method defined inside a with_connection block" do
    expect_offense(<<~RUBY)
      connection_pool.with_connection do
        def later
          transaction { work }
          ^^^^^^^^^^^ #{transaction_msg}
        end
      end
    RUBY
  end

  it "accepts a transaction directly inside connection_pool.with_connection" do
    expect_no_offenses(<<~RUBY)
      def signal(key)
        Pgbus::Semaphore.connection_pool.with_connection do
          Pgbus::Semaphore.transaction do
            release(key)
          end
        end
      end

      def increment!
        connection_pool.with_connection { transaction { lock.find_by(id: 1) } }
      end
    RUBY
  end

  it "accepts a transaction on another object, such as the PGMQ client or the yielded connection" do
    expect_no_offenses(<<~RUBY)
      client.transaction do |txn|
        txn.archive(queue, id)
      end
      conn.transaction { work }
    RUBY
  end

  describe "the scope in .rubocop.yml" do
    let(:root) { File.expand_path("../../../..", __dir__) }
    # The cop's own block of the repository's config, read as YAML: loading
    # the whole file would require the custom cops a second time.
    let(:repo_cop) do
      path = File.join(root, ".rubocop.yml")
      cop_config = YAML.safe_load_file(path, aliases: true).fetch("Pgbus/BareConnectionLease")
      described_class.new(RuboCop::Config.new({ "Pgbus/BareConnectionLease" => cop_config }, path))
    end

    it "covers the job path" do
      %w[
        app/models/pgbus/uniqueness_key.rb
        app/models/pgbus/job_stat.rb
        lib/pgbus/batch.rb
        lib/pgbus/concurrency/semaphore.rb
        lib/pgbus/failed_event_recorder.rb
        lib/pgbus/active_job/executor.rb
      ].each do |path|
        expect(repo_cop.relevant_file?(File.join(root, path))).to be(true), path
      end
    end

    it "leaves out the dashboard and the workflow runner" do
      %w[lib/pgbus/web/data_source.rb lib/pgbus/workflow/runner.rb app/models/pgbus/workflow_entry.rb].each do |path|
        expect(repo_cop.relevant_file?(File.join(root, path))).to be(false), path
      end
    end
  end
end
