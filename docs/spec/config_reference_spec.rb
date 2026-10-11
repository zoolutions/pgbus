# frozen_string_literal: true

require "rails_helper"
require "pgbus"

# The Configuration reference page is generated from ConfigReference::GROUPS.
# This spec keeps that data honest against the REAL Pgbus::Configuration, in both
# directions, so the reference can't silently drift from the gem:
#
#   1. Every documented option must be a real public writer accessor.
#   2. Every real writer accessor must be either documented or explicitly
#      allowlisted as internal — so a NEW config option fails the build until
#      someone documents it (or marks it internal on purpose).
RSpec.describe "Configuration reference drift", type: :model do
  # Public `foo=` writers on a fresh Configuration, minus private/internal ones.
  # Settings split into Pgbus::Configuration::* mixins (e.g. PresenceSettings)
  # count too: public_methods(false) does not see included modules.
  let(:config_mixins) do
    Pgbus::Configuration.included_modules.select { |m| m.name&.start_with?("Pgbus::Configuration::") }
  end

  let(:real_accessors) do
    (Pgbus::Configuration.new.public_methods(false) + config_mixins.flat_map { |m| m.public_instance_methods(false) })
      .grep(/=$/)
      .map { |m| m.to_s.chomp("=") }
      .reject { |m| m.start_with?("_") }
      .sort
      .uniq
  end

  let(:documented) { ConfigReference.documented_names.sort }
  let(:internal)   { ConfigReference::INTERNAL_ONLY.sort }

  it "documents only real configuration accessors" do
    unknown = documented - real_accessors
    expect(unknown).to be_empty,
                       "ConfigReference documents options that aren't Pgbus::Configuration accessors: #{unknown.inspect}"
  end

  it "documents (or allowlists) every real configuration accessor" do
    undocumented = real_accessors - documented - internal
    expect(undocumented).to be_empty, <<~MSG
      New Pgbus::Configuration accessors are neither documented nor allowlisted: #{undocumented.inspect}
      Add each to ConfigReference::GROUPS (to document it on the reference page)
      or to ConfigReference::INTERNAL_ONLY (to mark it deliberately internal).
    MSG
  end

  it "has no overlap between documented and internal lists" do
    overlap = documented & internal
    expect(overlap).to be_empty,
                       "Options appear in both GROUPS and INTERNAL_ONLY: #{overlap.inspect}"
  end
end
