# frozen_string_literal: true

require "json"
require "securerandom"

module Pgbus
  # The Workflows pages (issue #542): the list with its filters, one run's
  # detail with the operator actions, and the needs-attention groups with
  # their bulk actions. Every action goes through Web::DataSource, which
  # answers the real outcome, and is recorded as the dashboard actor
  # ("dashboard:<config.web_actor>", or "dashboard").
  class WorkflowsController < ApplicationController
    REASON_LIMIT = 1_000
    GROUP_FIELDS = %i[workflow_class step_name reason error_class].freeze

    rescue_from Web::WorkflowQuery::Invalid do |error|
      render plain: t("pgbus.workflows.bad_request", reason: error.message), status: :bad_request
    end

    def index
      @query = Web::WorkflowQuery.new(params)
      @page = page_param
      @per_page = per_page
      @workflows = data_source.workflows(status: @query.status, page: @page, per_page: @per_page, **@query.filters)
      @workflows_count = data_source.workflows_count(status: @query.status, **@query.filters)
      return render_frame("pgbus/workflows/workflows_table") if params[:frame] == "list"

      @attention_count = data_source.workflow_attention_count
    end

    # ?workflow_class= narrows the groups to one class (the insights' link).
    def attention
      @workflow_class = attention_class
      @groups = data_source.workflow_attention_groups(workflow_class: @workflow_class)
    end

    def show
      @workflow = data_source.workflow_detail(params[:workflow_id])
      render :not_found, status: :not_found unless @workflow
    end

    def resume = member_action(:resume) { |id, **who| data_source.resume_workflow(id, **who) }

    def cancel = member_action(:cancel) { |id, **who| data_source.cancel_workflow(id, **who) }

    def skip_step = member_action(:skip_step) { |id, **who| data_source.skip_workflow_step(id, **who) }

    def run_now = member_action(:run_now) { |id, **who| data_source.run_workflow_step_now(id, **who) }

    def resume_at
      step = params[:step].to_s.strip
      return back_to_workflow(alert: t("pgbus.workflows.flash.choose_step")) if step.empty?

      member_action(:resume_at, step: step) { |id, **who| data_source.resume_workflow_at(id, step: step, **who) }
    end

    def signal
      name = params[:name].to_s.strip
      return back_to_workflow(alert: t("pgbus.workflows.flash.signal_name_missing")) if name.empty?

      payload = signal_payload
      return back_to_workflow(alert: payload) if payload.is_a?(String)

      member_action(:signal, name: name) do |id, actor:, **|
        data_source.signal_workflow(id, name: name, payload: payload, idempotency_key: params[:idempotency_key].presence,
                                        actor: actor)
      end
    end

    def rerun
      result = data_source.rerun_workflow(params[:workflow_id], **who)
      return back_to_workflow(alert: failure_message(:rerun, result)) unless result[:ok]

      redirect_to pgbus.workflow_path(result[:workflow_id]), notice: t("pgbus.workflows.flash.rerun")
    end

    def bulk_resume = bulk_action(:bulk_resume) { |group| data_source.bulk_resume_workflows(group: group, **who) }

    def bulk_cancel = bulk_action(:bulk_cancel) { |group| data_source.bulk_cancel_workflows(group: group, **who) }

    private

    def member_action(action, **args)
      result = yield(params[:workflow_id], **who)
      if result[:ok]
        key = action == :signal && result[:duplicate] ? :signal_duplicate : action
        back_to_workflow(notice: t("pgbus.workflows.flash.#{key}", **args))
      else
        back_to_workflow(alert: failure_message(action, result))
      end
    end

    def bulk_action(action)
      group = attention_group
      return redirect_to(pgbus.attention_workflows_path, alert: t("pgbus.workflows.flash.bad_group")) unless valid_group?(group)

      result = yield(group)
      return redirect_to(pgbus.attention_workflows_path, alert: failure_message(action, result)) unless result[:ok]

      redirect_to pgbus.attention_workflows_path, notice: bulk_notice(action, result)
    end

    def attention_class
      name = params[:workflow_class]
      return nil if name.nil? || name == ""
      return name if name.is_a?(String) && Web::DataSource::WorkflowInsights::CLASS_NAME.match?(name)

      raise Web::WorkflowQuery::Invalid, "workflow_class must be a class name"
    end

    def valid_group?(group)
      return false unless group

      group.values_at(:workflow_class, :step_name).all? && Workflow::States::STUCK_REASONS.include?(group[:reason])
    end

    # The needs-attention group a bulk form posts (group[workflow_class] and
    # friends, each a String; a blank error_class is nil), or nil when the
    # param is not that shape.
    def attention_group
      raw = params[:group]
      return nil unless raw.is_a?(ActionController::Parameters)

      GROUP_FIELDS.to_h do |field|
        value = raw[field]
        return nil unless value.nil? || value.is_a?(String)

        [field, value.presence]
      end
    end

    # "Resumed 212; 2 had already moved on." and, at the cap, that more may wait.
    def bulk_notice(action, result)
      cap = Web::DataSource::WorkflowActions::BULK_CAP
      failed = result[:failed].to_i
      key = result[:skipped].positive? ? "#{action}_skipped" : action.to_s
      words = [t("pgbus.workflows.flash.#{key}", count: result[:done], skipped: result[:skipped])]
      words << t("pgbus.workflows.flash.bulk_failed", count: failed, reason: result[:failure]) if failed.positive?
      words << t("pgbus.workflows.flash.bulk_more", cap: cap) if result[:done] + result[:skipped] + failed >= cap
      words.join(" ")
    end

    def failure_message(action, result)
      case result[:code]
      when :not_allowed
        status = result[:status] ? t("pgbus.workflows.statuses.#{result[:status]}").downcase : t("pgbus.workflows.statuses.gone")
        t("pgbus.workflows.flash.failed.not_allowed", status: status)
      when :not_found then t("pgbus.workflows.flash.failed.not_found")
      else t("pgbus.workflows.flash.failed.other", action: t("pgbus.workflows.actions.#{action}"), reason: result[:reason])
      end
    end

    def back_to_workflow(**flash)
      redirect_to pgbus.workflow_path(params[:workflow_id]), **flash
    end

    # The parsed payload Hash, or the alert to show instead (nothing is sent).
    def signal_payload
      text = params[:payload].to_s.strip
      return {} if text.empty?

      limit = Pgbus.configuration.workflow_max_payload_size
      return t("pgbus.workflows.flash.payload_too_large", limit: limit) if text.bytesize > limit

      parsed = JSON.parse(text)
      parsed.is_a?(Hash) ? parsed : t("pgbus.workflows.flash.payload_not_object")
    rescue JSON::ParserError
      t("pgbus.workflows.flash.payload_not_object")
    end

    def who = { actor: dashboard_actor, reason: operator_reason }

    def operator_reason
      text = params[:reason].to_s.strip
      text.empty? ? nil : text[0, REASON_LIMIT]
    end

    # config.web_actor names the operator; a lambda that raises is logged
    # and the action is recorded as plain "dashboard".
    def dashboard_actor
      name = Pgbus.configuration.web_actor&.call(request)
      unless name.nil? || name.is_a?(String) || name.is_a?(Symbol)
        Pgbus.logger.warn { "[Pgbus::Web] web_actor returned a #{name.class}, not a name; recording \"dashboard\"" }
        return "dashboard"
      end

      name = name.to_s.strip
      name.empty? ? "dashboard" : "dashboard:#{name}"
    rescue StandardError => e
      Pgbus.logger.error { "[Pgbus::Web] web_actor raised #{e.class}: #{e.message}" }
      "dashboard"
    end
  end
end
