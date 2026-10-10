# frozen_string_literal: true

require "system_helper"

# The merge-time accessibility gate (#498): every dashboard page, light and
# dark, against the same sample data rake dummy:server and Lighthouse show.
# The page list is lighthouserc.dashboard.json; there is no accepted debt.
RSpec.describe "Dashboard accessibility", type: :system do
  describe "with sample data" do
    before { @stub_data_source.fill_sample_data! }

    Accessibility.audited_paths.each do |path|
      it_behaves_like "an accessible page", path, :light
      it_behaves_like "an accessible page", path, :dark
    end

    # The one state Lighthouse cannot reach: an expanded job or event row (and
    # the hover tint the pointer leaves on it).
    %i[light dark].each do |mode|
      it_behaves_like "an accessible page", "/pgbus/jobs", mode do
        let(:interaction) do
          lambda do
            first("details[data-job-toggle] summary").click
            page.assert_selector("tr[data-job-detail]", visible: :visible)
          end
        end
      end

      # The expanded event row (issue #494): error line, reroute, edit form.
      it_behaves_like "an accessible page", "/pgbus/events", mode do
        let(:interaction) do
          lambda do
            first("tr[data-state=retrying] details[data-job-toggle] summary").click
            page.assert_selector("tr[data-event-detail]", visible: :visible)
          end
        end
      end
    end
  end

  describe "empty states" do
    # The sparse stub still carries two queues and a worker (other specs rely
    # on them), so clear those for a truly empty dashboard.
    before do
      @stub_data_source.queues = []
      @stub_data_source.processes_list = []
      @stub_data_source.stats = @stub_data_source.stats.transform_values { |v| v.is_a?(Numeric) ? 0 : v }
    end

    %w[/pgbus /pgbus/queues /pgbus/jobs /pgbus/recurring_tasks /pgbus/processes /pgbus/events
       /pgbus/batches /pgbus/dlq /pgbus/outbox /pgbus/locks /pgbus/insights].each do |path|
      it_behaves_like "an accessible page", path, :light
      it_behaves_like "an accessible page", path, :dark
    end
  end

  # A new page cannot ship without an audit: every HTML GET route of the
  # engine must be reached by a URL in lighthouserc.dashboard.json.
  it "audits every HTML GET route of the engine" do
    required = Pgbus::Engine.routes.routes
                            .select { |r| r.verb == "GET" && r.requirements[:controller] }
                            .map { |r| "#{r.requirements[:controller]}##{r.requirements[:action]}" }
                            .reject { |ca| ca.start_with?("pgbus/api/", "pgbus/frontends#", "pgbus/locale#") }
                            .uniq
    covered = Accessibility.audited_paths.map do |path|
      route = Pgbus::Engine.routes.recognize_path(path.delete_prefix("/pgbus"), method: :get)
      "#{route[:controller]}##{route[:action]}"
    end.uniq

    missing = required - covered
    expect(missing).to be_empty, "add these pages to lighthouserc.dashboard.json: #{missing.join(", ")}"
  end
end
