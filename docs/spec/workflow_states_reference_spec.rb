# frozen_string_literal: true

require "rails_helper"
require "pgbus"

# The Workflows page lists the transition table from WorkflowStatesReference;
# this keeps it equal to the gem's Pgbus::Workflow::States::TRANSITIONS, verb
# for verb, so a new or changed transition fails the docs build until the page
# says so too.
RSpec.describe "Workflow transition table drift", type: :model do
  let(:transitions) { Pgbus::Workflow::States::TRANSITIONS }

  it "lists exactly the gem's verbs, outside and runner apart" do
    expect(WorkflowStatesReference::OUTSIDE.keys).to match_array(transitions.values.select(&:outside).map(&:verb))
    expect(WorkflowStatesReference::RUNNER.keys).to match_array(transitions.values.reject(&:outside).map(&:verb))
  end

  it "gives each verb the gem's from states, target and effect" do
    WorkflowStatesReference.all.each do |verb, (from, to, effect)|
      transition = transitions.fetch(verb)

      expect([ from, to, effect ]).to eq([ transition.from, transition.to, transition.effect ]), "for #{verb}"
    end
  end
end
