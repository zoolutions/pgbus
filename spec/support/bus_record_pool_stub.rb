# frozen_string_literal: true

# pgbus's own ActiveRecord work runs inside connection_pool.with_connection
# (issue #649), and unit specs have no database. A spec that stubs the model
# calls (transaction, where, exec_query) gives every pgbus model a pool whose
# with_connection yields +connection+ without checking one out; the models
# inherit BusRecord's stub. Anything else asked of the pool raises what a
# missing database raises, so schema probes (column_names, table_exists?)
# fail as they do with no pool at all.
module BusRecordPoolStub
  class NoDatabasePool
    attr_reader :leases

    def initialize(connection)
      @connection = connection
      @leases = 0
    end

    def with_connection(*)
      @leases += 1
      yield(@connection)
    end

    def method_missing(*)
      # ConnectionNotEstablished: Rails 7.1 has no ConnectionNotDefined, its
      # subclass on 7.2+, and every caller rescues the superclass alike.
      raise ActiveRecord::ConnectionNotEstablished, "No database connection defined."
    end

    def respond_to_missing?(*) = false
  end

  def stub_bus_record_pool(connection = double("ActiveRecord connection"))
    pool = NoDatabasePool.new(connection)
    allow(Pgbus::BusRecord).to receive(:connection_pool).and_return(pool)
    pool
  end
end

RSpec.configure { |config| config.include BusRecordPoolStub }
