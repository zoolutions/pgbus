# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::WorkflowPayload do
  describe ".display" do
    it "drops ActiveJob's bookkeeping keys and keeps the values" do
      stored = { "plan" => "pro", "seats" => 3, "_aj_symbol_keys" => %w[plan seats] }

      expect(described_class.display(stored)).to eq("plan" => "pro", "seats" => 3)
    end

    it "shows a GlobalID as its URI instead of loading the record" do
      stored = { "user" => { "_aj_globalid" => "gid://app/User/42" }, "_aj_ruby2_keywords" => ["user"] }

      expect(described_class.display(stored)).to eq("user" => "gid://app/User/42")
    end

    it "shows a serialized value (a Time, a Symbol) as its stored value" do
      stored = { "at" => { "_aj_serialized" => "ActiveJob::Serializers::TimeSerializer", "value" => "2026-10-17T10:00:00Z" },
                 "kind" => { "_aj_serialized" => "ActiveJob::Serializers::SymbolSerializer", "value" => "express" } }

      expect(described_class.display(stored)).to eq("at" => "2026-10-17T10:00:00Z", "kind" => "express")
    end

    it "walks nested hashes and arrays" do
      stored = { "items" => [{ "sku" => "A", "_aj_symbol_keys" => ["sku"] }],
                 "_aj_hash_with_indifferent_access" => true }

      expect(described_class.display(stored)).to eq("items" => [{ "sku" => "A" }])
    end

    it "keeps nil elements and shows a multi-field serializer's fields" do
      stored = { "ids" => [1, nil], "window" => { "_aj_serialized" => "ActiveJob::Serializers::RangeSerializer",
                                                  "begin" => 1, "end" => 5, "exclude_end" => false } }

      expect(described_class.display(stored)).to eq("ids" => [1, nil], "window" => { "begin" => 1, "end" => 5, "exclude_end" => false })
    end

    it "returns {} for nil" do
      expect(described_class.display(nil)).to eq({})
    end
  end
end
