# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Vendored frontend assets" do # rubocop:disable RSpec/DescribeClass
  let(:frontend_dir) { Pgbus::Engine.root.join("app", "frontend", "pgbus") }

  describe "CSS" do
    it "has a compiled style.css" do
      css = frontend_dir.join("style.css")
      expect(css).to exist
      expect(css.size).to be > 1000 # compiled Tailwind is at least a few KB
    end
  end

  describe "JavaScript vendor files" do
    it "has vendored Turbo" do
      turbo = frontend_dir.join("vendor", "turbo.js")
      expect(turbo).to exist
      content = turbo.read
      expect(content).to include("Turbo")
    end

    it "has vendored ApexCharts" do
      apexcharts = frontend_dir.join("vendor", "apexcharts.js")
      expect(apexcharts).to exist
      content = File.binread(apexcharts.to_s, 200)
      expect(content).to include("ApexCharts")
    end
  end

  describe "JavaScript modules" do
    it "has application.js" do
      app_js = frontend_dir.join("application.js")
      expect(app_js).to exist
      content = app_js.read
      expect(content).to include('import * as Turbo from "turbo"')
    end

    it "has charts.js module" do
      charts = frontend_dir.join("modules", "charts.js")
      expect(charts).to exist
      content = charts.read
      expect(content).to include("export async function renderCharts")
    end
  end

  describe "minified builds (rake frontend:js)" do
    let(:assets_dir) { Pgbus::Engine.root.join("app", "assets", "javascripts", "pgbus") }

    # source, minified output, the sourceMappingURL the output must end with
    builds = {
      "stream element" => ["app/assets/javascripts/pgbus/stream_source_element.js",
                           "app/assets/javascripts/pgbus/stream_source_element.min.js",
                           "stream_source_element.min.js.map"],
      "application" => ["app/frontend/pgbus/application.js",
                        "app/frontend/pgbus/application.min.js",
                        "application.map"],
      "charts" => ["app/frontend/pgbus/modules/charts.js",
                   "app/frontend/pgbus/modules/charts.min.js",
                   "charts.map"],
      "turbo" => ["app/frontend/pgbus/vendor/turbo.js",
                  "app/frontend/pgbus/vendor/turbo.min.js",
                  "turbo.map"]
    }

    builds.each do |name, (source, minified, map_url)|
      context "with #{name}" do
        let(:source_path) { Pgbus::Engine.root.join(source) }
        let(:min_path) { Pgbus::Engine.root.join(minified) }
        let(:map_path) { Pgbus::Engine.root.join(minified.sub(/\.js\z/, ".js.map")) }

        it "exists, smaller than its source" do
          expect(min_path).to exist
          expect(min_path.size).to be < source_path.size
        end

        it "has a sibling sourcemap carrying the source text" do
          map = JSON.parse(map_path.read)
          expect(map["sources"]).to eq([File.basename(source)])
          expect(map["sourcesContent"]).to eq([source_path.read])
        end

        it "links its sourcemap by #{map_url}" do
          expect(min_path.read).to end_with("//# sourceMappingURL=#{map_url}\n")
        end
      end
    end

    it "minifies Turbo below half its size and keeps its license" do
      min = frontend_dir.join("vendor", "turbo.min.js")
      expect(min.size).to be < (frontend_dir.join("vendor", "turbo.js").size / 2)
      expect(min.read).to start_with("/*!\nTurbo 8.0.12")
      expect(min.read).to include("The MIT License (MIT)")
    end

    it "keeps bare specifiers external" do
      expect(frontend_dir.join("application.min.js").read).to match(/from\s*"turbo"/)
      expect(frontend_dir.join("modules", "charts.min.js").read).to include('import("apexcharts")')
      expect(assets_dir.join("stream_source_element.min.js").read).to match(%r{from\s*"@hotwired/turbo-rails"})
    end
  end

  describe "route integration" do
    it "includes frontend routes in the routes file" do
      routes_file = Pgbus::Engine.root.join("config", "routes.rb").read
      expect(routes_file).to include("frontend_module")
      expect(routes_file).to include("frontend_static")
    end
  end
end
