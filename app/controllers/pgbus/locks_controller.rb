# frozen_string_literal: true

module Pgbus
  class LocksController < ApplicationController
    def index
      @concurrency = data_source.concurrency_stats
      return render_frame("pgbus/locks/concurrency") if params[:frame] == "concurrency"

      @locks = data_source.job_locks
    end

    def discard
      count = data_source.discard_lock(params[:id])
      if count.positive?
        redirect_to locks_path, notice: t("pgbus.locks.index.lock_discarded")
      else
        redirect_to locks_path, alert: t("pgbus.locks.index.lock_discard_failed")
      end
    end

    def discard_selected
      keys = Array(params[:lock_keys]).reject(&:blank?)
      if keys.empty?
        redirect_to locks_path, alert: t("pgbus.locks.index.none_selected")
        return
      end

      count = data_source.discard_locks(keys)
      redirect_to locks_path, notice: t("pgbus.locks.index.locks_discarded", count: count)
    end

    def discard_all
      count = data_source.discard_all_locks
      redirect_to locks_path, notice: t("pgbus.locks.index.all_locks_discarded", count: count)
    end

    # Drop a concurrency key's semaphore and promote whatever can now run.
    # The key is passed through verbatim — a `key:` proc may produce a string
    # with surrounding whitespace, and stripping it here would address a
    # different key (or none).
    # The promotion goes through the guarded upsert inside the data source, so
    # this is an escape hatch, never a way past the limit.
    def release_key
      key = params[:key].to_s
      return redirect_to(locks_path, alert: t("pgbus.locks.concurrency.no_key")) if key.strip.empty?

      count = data_source.release_concurrency_key(key)
      redirect_to locks_path, notice: t("pgbus.locks.concurrency.key_released", key: key, count: count)
    end

    # Drop every job parked behind a concurrency key. They never run, so the
    # data source resolves their batch and uniqueness bookkeeping.
    def discard_parked
      key = params[:key].to_s
      return redirect_to(locks_path, alert: t("pgbus.locks.concurrency.no_key")) if key.strip.empty?

      count = data_source.discard_parked_jobs(key)
      redirect_to locks_path, notice: t("pgbus.locks.concurrency.parked_discarded", count: count)
    end
  end
end
