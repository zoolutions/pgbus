# frozen_string_literal: true

# rails_helper, not a bare require of the controller: loaded before the dummy
# app initializes, the controller's skip of verify_same_origin_request would
# run before ActionController::Base gains protect_from_forgery, which then
# appends the check to every descendant and 422s the request specs that load
# a JS module (a run of spec/pgbus/web before spec/requests).
require "rails_helper"

RSpec.describe Pgbus::FrontendsController do
  describe "STATIC_ASSETS" do
    it "references only files that exist" do
      described_class::STATIC_ASSETS.each_value do |entries|
        entries.each do |name, path|
          expect(path).to exist, "Static asset #{name} not found at #{path}"
        end
      end
    end
  end

  describe "MODULE_OVERRIDES" do
    it "references only files that exist" do
      described_class::MODULE_OVERRIDES.each do |name, path|
        expect(path).to exist, "Module override #{name} not found at #{path}"
      end
    end
  end

  describe ".js_modules" do
    it "includes application, turbo, charts and apexcharts modules" do
      modules = described_class.js_modules
      expect(modules).to include(:application, :turbo, :charts, :apexcharts)
    end

    it "serves every module but the already-minified apexcharts as .min.js" do
      described_class.js_modules.except(:apexcharts).each do |name, path|
        expect(path.to_s).to end_with(".min.js"), "JS module #{name} is not minified: #{path}"
      end
    end

    it "keys modules by name, never a sourcemap or an unminified sibling" do
      expect(described_class.js_modules.keys.map(&:to_s)).to all(match(/\A[a-z_]+\z/))
    end

    it "references only files that exist" do
      described_class.js_modules.each do |name, path|
        expect(path).to exist, "JS module #{name} not found at #{path}"
      end
    end
  end

  describe "inherits from ActionController::Base" do
    it "does not inherit from Pgbus::ApplicationController" do
      expect(described_class.superclass).to eq(ActionController::Base)
    end
  end
end
