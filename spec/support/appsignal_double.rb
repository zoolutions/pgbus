# frozen_string_literal: true

# A stateful stand-in for the appsignal gem (5.0.x), for the specs of
# Pgbus::Integrations::Appsignal. It keeps the state the real gem's guards
# read, so an example cannot pass through a call the real gem drops (issue #552):
#
#   * Transaction.create makes a transaction current, fiber-local like
#     Thread.current[:appsignal_transaction], and returns the current one when
#     there already is one; complete_current! completes and clears it.
#   * Appsignal.set_error does nothing without a current transaction
#     (appsignal 5.0.4 lib/appsignal/helpers/instrumentation.rb:380).
#   * Appsignal.report_error adds to the current transaction, or to a new
#     standalone one that it completes.
#   * An exception is recorded once: a transaction that does not have it yet
#     drops it when another one already reported it (the gem's
#     @__appsignal_error_reported marker), and a block given with it does not run.
#
# AppsignalDouble.build returns a fresh pair per example; stub both constants.
module AppsignalDouble
  CURRENT_KEY = :appsignal_double_current_transaction

  Pair = Data.define(:appsignal, :transaction_class)

  # The method names are the appsignal gem's Transaction API.
  # rubocop:disable-next Naming/AccessorMethodName
  class FakeTransaction
    attr_reader :namespace, :action, :tags, :queue_start, :params, :errors

    def initialize(namespace, reported)
      @namespace = namespace
      @reported = reported
      @action = nil
      @tags = {}
      @queue_start = nil
      @params = nil
      @errors = []
      @completed = false
    end

    def nil_transaction? = false

    def completed? = @completed

    def complete
      @completed = true
    end

    def set_namespace(namespace)
      @namespace = namespace
    end

    def set_action(action)
      @action = action
    end

    def set_action_if_nil(action)
      @action = action if @action.nil?
    end

    def add_tags(tags)
      @tags.merge!(tags)
    end

    def set_queue_start(millis)
      @queue_start = millis
    end

    def add_params_if_nil(params = nil)
      @params = block_given? ? yield : params if @params.nil?
    end

    def set_error(error)
      has_it = @errors.any? { |recorded| recorded.equal?(error) }
      return if @reported.key?(error) && !has_it

      @errors << error unless has_it
      @reported[error] = true
      yield self if block_given?
    end
  end

  # Like the gem's Transaction::NilTransaction: every call is a no-op.
  class NilTransaction
    def nil_transaction? = true

    def method_missing(*) = nil

    def respond_to_missing?(*) = true
  end

  module_function

  def build
    reported = {}.compare_by_identity
    transactions = []
    transaction_class = build_transaction_class(reported, transactions)
    Pair.new(appsignal: build_appsignal(transaction_class, reported, transactions), transaction_class: transaction_class)
  end

  def reset!
    Thread.current[CURRENT_KEY] = nil
  end

  def build_transaction_class(reported, transactions)
    Class.new do
      define_singleton_method(:all) { transactions }
      define_singleton_method(:current) { Thread.current[CURRENT_KEY] || NilTransaction.new }
      define_singleton_method(:current?) { !current.nil_transaction? }

      define_singleton_method(:create) do |namespace|
        next current if current?

        transaction = FakeTransaction.new(namespace, reported)
        transactions << transaction
        Thread.current[CURRENT_KEY] = transaction
      end

      define_singleton_method(:complete_current!) do
        current.complete
      ensure
        Thread.current[CURRENT_KEY] = nil
      end
    end
  end

  def build_appsignal(transaction_class, reported, transactions)
    counters = []
    distributions = []
    gauges = []
    Class.new do
      define_singleton_method(:active?) { true }
      define_singleton_method(:counters) { counters }
      define_singleton_method(:distributions) { distributions }
      define_singleton_method(:gauges) { gauges }
      define_singleton_method(:increment_counter) { |name, value, tags = {}| counters << [name, value, tags] }
      define_singleton_method(:add_distribution_value) { |name, value, tags = {}| distributions << [name, value, tags] }
      define_singleton_method(:set_gauge) { |name, value, tags = {}| gauges << [name, value, tags] }

      define_singleton_method(:set_error) do |error|
        next unless transaction_class.current?

        transaction_class.current.set_error(error)
      end

      define_singleton_method(:report_error) do |error, &block|
        next transaction_class.current.set_error(error, &block) if transaction_class.current?

        transaction = FakeTransaction.new("http_request", reported)
        transactions << transaction
        transaction.set_error(error, &block)
        transaction.complete
      end
    end
  end
end
