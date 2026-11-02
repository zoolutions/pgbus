# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::QueueNameValidator do
  describe ".validate!" do
    it "accepts valid alphanumeric queue names" do
      expect(described_class.validate!("pgbus_default")).to eq("pgbus_default")
    end

    it "accepts names with numbers" do
      expect(described_class.validate!("queue_42_p0")).to eq("queue_42_p0")
    end

    it "rejects blank names" do
      expect { described_class.validate!("") }.to raise_error(ArgumentError, /cannot be blank/)
    end

    it "rejects names with special characters" do
      expect { described_class.validate!("queue; DROP TABLE users--") }.to raise_error(ArgumentError, /Only alphanumeric/)
    end

    it "rejects names with SQL injection attempts" do
      expect { described_class.validate!("q' OR '1'='1") }.to raise_error(ArgumentError, /Only alphanumeric/)
    end

    it "rejects names with dots" do
      expect { described_class.validate!("pgmq.q_default") }.to raise_error(ArgumentError, /Only alphanumeric/)
    end

    it "rejects names with dashes" do
      expect { described_class.validate!("my-queue") }.to raise_error(ArgumentError, /Only alphanumeric/)
    end

    it "rejects names exceeding max length" do
      long_name = "a" * (described_class::MAX_QUEUE_NAME_LENGTH + 1)
      expect { described_class.validate!(long_name) }.to raise_error(ArgumentError, /too long/)
    end

    it "accepts names at max length" do
      name = "a" * described_class::MAX_QUEUE_NAME_LENGTH
      expect(described_class.validate!(name)).to eq(name)
    end

    it "matches the effective limit enforced by pgmq-ruby" do
      # pgmq-ruby's PGMQ::QueueName.validate! rejects length >= MAX_LENGTH, so
      # our constant must be one below it: higher lets names through pgbus
      # that then fail deep in the transport gem, lower rejects names PGMQ takes.
      expect(described_class::MAX_QUEUE_NAME_LENGTH).to eq(real_pgmq_queue_name::MAX_LENGTH - 1)
    end

    it "names the same ceiling as pgmq-ruby's own error (pgmq-ruby 0.8.2+)" do
      max = described_class::MAX_QUEUE_NAME_LENGTH
      queue_name = real_pgmq_queue_name

      expect(queue_name.validate!("a" * max)).to eq("a" * max)
      expect { queue_name.validate!("a" * (max + 1)) }
        .to raise_error(PGMQ::Errors::InvalidQueueNameError, /maximum length of #{max} characters/)
    end
  end

  describe ".normalize" do
    it "replaces hyphens with underscores" do
      expect(described_class.normalize("hotwire-livereload")).to eq("hotwire_livereload")
    end

    it "replaces dots with underscores" do
      expect(described_class.normalize("my.queue.name")).to eq("my_queue_name")
    end

    it "replaces colons with underscores" do
      # Colons are the turbo-rails stream-name separator
      # (e.g. "user_1:notifications"). Stripping them entirely would
      # collapse "user_1:notifications" and "user_1notifications"
      # onto the same queue, so they must map to a safe character.
      expect(described_class.normalize("user_1:notifications")).to eq("user_1_notifications")
    end

    it "does not collide colon-joined names with their stripped form" do
      with_colon = described_class.normalize("a:b:c")
      without_colon = described_class.normalize("abc")
      expect(with_colon).not_to eq(without_colon)
    end

    it "collapses consecutive underscores after replacement" do
      expect(described_class.normalize("my--queue")).to eq("my_queue")
    end

    it "strips leading/trailing underscores after replacement" do
      expect(described_class.normalize("-leading")).to eq("leading")
      expect(described_class.normalize("trailing-")).to eq("trailing")
    end

    it "strips characters that are not alphanumeric, hyphens, dots, colons, or underscores" do
      expect(described_class.normalize("queue!@#name")).to eq("queuename")
    end

    it "passes through already-valid names unchanged" do
      expect(described_class.normalize("pgbus_default")).to eq("pgbus_default")
    end

    it "raises when normalized result is blank" do
      expect { described_class.normalize("---!!!") }.to raise_error(ArgumentError, /cannot be blank/)
    end

    # Issue #633: PGMQ lower-cases its table names (pgmq.format_table_name),
    # so "Orders" and "orders" share pgmq.q_orders. The canonical name is the
    # table's spelling.
    it "lower-cases an already-valid name, the way PGMQ spells its table" do
      expect(described_class.normalize("pgbus_Orders")).to eq("pgbus_orders")
    end

    it "lower-cases a name it rewrites" do
      expect(described_class.normalize("gid://App/User-1")).to eq("gid_appuser_1")
    end

    it "gives names that differ only in case one spelling" do
      expect(described_class.normalize("pgbus_Z2lkOi8v")).to eq(described_class.normalize("pgbus_z2lkoi8v"))
    end
  end

  describe ".validate! and .sanitize! on mixed case" do
    # Both run on names read back from pgmq.meta (dashboard, dispatcher,
    # doctor), where an install from before #633 still holds mixed-case rows.
    it "keeps the case they were given" do
      expect(described_class.validate!("pgbus_Orders")).to eq("pgbus_Orders")
      expect(described_class.sanitize!("pgbus_Orders")).to eq("pgbus_Orders")
    end
  end

  describe ".sanitize!" do
    it "strips invalid characters and returns the result" do
      expect(described_class.sanitize!("my-queue.name!")).to eq("myqueuename")
    end

    it "raises when sanitized result is blank" do
      expect { described_class.sanitize!("---!!!") }.to raise_error(ArgumentError, /cannot be blank/)
    end

    it "strips SQL injection attempts" do
      result = described_class.sanitize!("queue'; DROP TABLE users--")
      expect(result).to eq("queueDROPTABLEusers")
    end
  end
end
