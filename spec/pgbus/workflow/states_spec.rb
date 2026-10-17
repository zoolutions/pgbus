# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Workflow::States do
  # The documented contract, written out by hand so the table cannot drift
  # silently: verb => [legal from-states, target state, error otherwise].
  let(:contract) do
    {
      pause: [%w[ready performing waiting], "paused", Pgbus::Workflow::InvalidTransition],
      resume: [%w[paused], "ready", Pgbus::Workflow::NotPaused],
      resume_at: [%w[paused], "ready", Pgbus::Workflow::NotPaused],
      cancel: [%w[ready performing waiting paused], "cancelled", Pgbus::Workflow::InvalidTransition],
      run_now: [%w[ready], "ready", Pgbus::Workflow::InvalidTransition],
      reschedule: [%w[ready], "ready", Pgbus::Workflow::InvalidTransition],
      rerun: [%w[finished cancelled], "ready", Pgbus::Workflow::InvalidTransition],
      skip: [%w[waiting], "ready", Pgbus::Workflow::InvalidTransition],
      signal: [%w[ready performing waiting paused], nil, Pgbus::Workflow::NotOngoing]
    }
  end

  describe "the state lists" do
    it "names the six workflow states, four of them ongoing" do
      expect(described_class::WORKFLOW).to eq(%w[ready performing waiting paused finished cancelled])
      expect(described_class::ONGOING).to eq(%w[ready performing waiting paused])
      expect(described_class::TERMINAL).to eq(%w[finished cancelled])
    end

    it "names the seven step states" do
      expect(described_class::STEP).to eq(%w[scheduled waiting in_progress completed failed cancelled skipped])
    end

    it "is Ractor-shareable" do
      expect(Ractor.shareable?(described_class::TRANSITIONS)).to be(true)
    end
  end

  describe "every (workflow state x outside verb) pair" do
    Pgbus::Workflow::States::WORKFLOW.each do |state|
      %i[pause resume resume_at cancel run_now reschedule rerun skip signal].each do |verb|
        it "#{verb} from #{state} is legal with its documented target, or raises the documented error" do
          from, to, error = contract.fetch(verb)

          if from.include?(state)
            expect(described_class.check!(verb, state)).to eq(to)
          else
            expect { described_class.check!(verb, state) }.to raise_error(error, /#{verb.to_s.tr("_", " ")}.*#{state}/)
          end
        end
      end
    end
  end

  describe ".available_actions" do
    it "lists exactly the outside verbs legal in each state" do
      expected = {
        "ready" => %i[pause cancel run_now reschedule signal],
        "performing" => %i[pause cancel signal],
        "waiting" => %i[pause cancel skip signal],
        "paused" => %i[resume resume_at cancel signal],
        "finished" => %i[rerun],
        "cancelled" => %i[rerun]
      }

      expected.each do |state, verbs|
        expect(described_class.available_actions(state)).to match_array(verbs), "for #{state}"
      end
    end

    it "accepts a Symbol state" do
      expect(described_class.available_actions(:paused)).to include(:resume)
    end
  end

  describe "runner transitions" do
    it "claims only a ready workflow and moves it to performing" do
      expect(described_class.from(:claim)).to eq(%w[ready])
      expect(described_class.to(:claim)).to eq("performing")
    end

    it "applies a step outcome to a performing workflow, and a finish or cancel to a paused one too" do
      expect(described_class.from(:advance)).to eq(%w[performing])
      expect(described_class.from(:reattempt)).to eq(%w[performing])
      expect(described_class.from(:step_pause)).to eq(%w[performing])
      expect(described_class.from(:finish)).to eq(%w[performing paused])
      expect(described_class.from(:step_cancel)).to eq(%w[performing paused])
      expect(described_class.from(:hold)).to eq(%w[paused])
    end

    it "lets resume! go back to performing when the step that was running at the pause still runs" do
      expect(described_class.from(:unpause)).to eq(%w[paused])
      expect(described_class.to(:unpause)).to eq("performing")
    end

    it "parks a performing run as waiting, and wakes, resolves or re-waits it" do
      expect([described_class.from(:park), described_class.to(:park)]).to eq([%w[performing], "waiting"])
      expect([described_class.from(:wake), described_class.to(:wake)]).to eq([%w[waiting], "ready"])
      expect([described_class.from(:resolve), described_class.to(:resolve)]).to eq([%w[waiting], "performing"])
      expect([described_class.from(:rewait), described_class.to(:rewait)]).to eq([%w[paused], "waiting"])
    end

    it "names waiting_overdue among the stuck reasons" do
      expect(described_class::STUCK_REASONS).to eq(%w[paused_error stalled paused_manual overdue waiting_overdue])
    end

    it "keeps runner transitions out of available_actions" do
      runner = described_class::TRANSITIONS.values.reject(&:outside).map(&:verb)

      expect(runner).to include(:claim, :advance, :finish, :reattempt, :step_pause, :step_cancel, :hold, :unpause,
                                :park, :wake, :resolve, :rewait)
      described_class::WORKFLOW.each do |state|
        expect(described_class.available_actions(state) & runner).to be_empty
      end
    end
  end

  it "rejects an unknown verb" do
    expect { described_class.fetch(:explode) }.to raise_error(ArgumentError, /unknown workflow verb :explode/)
  end
end
