# frozen_string_literal: true

module Pgbus
  # Reports which Ruby JIT this process runs under (issue #484). Read-only:
  # Rails owns turning YJIT on (config.yjit), pgbus only makes the state
  # visible in the boot log and `pgbus doctor`.
  module RubyJit
    module_function

    def label
      return "yjit" if yjit_available? && RubyVM::YJIT.enabled?
      return "zjit" if defined?(RubyVM::ZJIT) && RubyVM::ZJIT.enabled?

      "none"
    end

    def yjit_available?
      defined?(RubyVM::YJIT) ? true : false
    end
  end
end
