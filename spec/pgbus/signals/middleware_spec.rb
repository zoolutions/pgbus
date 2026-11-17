# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Signals::Middleware do
  subject(:middleware) { described_class.new(app) }

  let(:app) { ->(env) { [200, {}, [env["PATH_INFO"]]] } }

  it "starts listening for process signals, then serves the request" do
    order = []
    allow(Pgbus::Signals).to receive(:start_listening!) { order << :start }
    wrapped = described_class.new(lambda { |_env|
      order << :app
      [200, {}, ["ok"]]
    })

    expect(wrapped.call({})).to eq([200, {}, ["ok"]])
    expect(order).to eq(%i[start app])
  end

  it "passes the app's response through" do
    allow(Pgbus::Signals).to receive(:start_listening!)

    expect(middleware.call("PATH_INFO" => "/x")).to eq([200, {}, ["/x"]])
  end
end
