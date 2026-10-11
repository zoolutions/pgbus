// Minifies pgbus's JavaScript into committed <name>.min.js + <name>.min.js.map
// files next to each source, so host apps need neither bun nor Node (issue #517).
//
//   bundle exec rake frontend:js         # rebuild after editing any source below
//   bundle exec rake frontend:js_check   # CI: fails when a committed .min.js is stale
//
// Each entry builds on its own: every bare specifier ("turbo", "apexcharts",
// "@hotwired/turbo-rails") stays external for an importmap to resolve.
// The script lives outside app/ so it never ships in the gem.

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";

const root = join(import.meta.dir, "..");

// The output is only byte-stable on one bun, and frontend:js_check diffs it in
// CI: refuse any other version, whichever entry point (rake or bun run) ran us.
const pinned = (await Bun.file(join(root, ".bun-version")).text()).trim();
if (Bun.version !== pinned) {
  console.error(`build_frontend.js needs bun ${pinned} (.bun-version); this is bun ${Bun.version}`);
  process.exit(1);
}

// source → the sourceMappingURL the output links. The dashboard serves a
// module's map at /pgbus/frontend/modules/<version>/<id>.map (FrontendsController);
// the stream element's map sits beside it in the host's asset pipeline.
const ENTRIES = [
  { source: "app/assets/javascripts/pgbus/stream_source_element.js", mapUrl: "stream_source_element.min.js.map" },
  { source: "app/frontend/pgbus/application.js", mapUrl: "application.map" },
  { source: "app/frontend/pgbus/modules/charts.js", mapUrl: "charts.map" },
  { source: "app/frontend/pgbus/vendor/turbo.js", mapUrl: "turbo.map" },
];

// The source's leading comments (license banners). Bun keeps `/*!` comments
// but drops plain `/** … */` ones such as Turbo's MIT notice, so the build
// strips them all and prepends them verbatim.
function leadingComments(text) {
  return text.match(/^(?:\s*\/\*[\s\S]*?\*\/)*/)[0];
}

async function build({ source, mapUrl }) {
  const sourcePath = join(root, source);
  const sourceText = await Bun.file(sourcePath).text();
  const license = leadingComments(sourceText);

  // Blank the banners (newlines kept, so source line numbers hold) and build
  // that copy; the map's sources/sourcesContent are pointed back at the source.
  const tmp = await mkdtemp(join(tmpdir(), "pgbus-build-"));
  const entry = join(tmp, basename(source));
  await Bun.write(entry, license.replace(/[^\n]/g, "") + sourceText.slice(license.length));

  const result = await Bun.build({
    entrypoints: [entry],
    minify: true,
    sourcemap: "linked",
    format: "esm",
    target: "browser",
    external: ["*"],
  });
  await rm(tmp, { recursive: true });
  if (!result.success) throw new AggregateError(result.logs, `bun build failed for ${source}`);

  let code = await result.outputs.find((o) => o.kind === "entry-point").text();
  const map = JSON.parse(await result.outputs.find((o) => o.kind === "sourcemap").text());

  // Bun's own trailer (debugId + sourceMappingURL) is replaced with ours.
  code = code.replace(/\n\/\/# debugId=.*\n\/\/# sourceMappingURL=.*\n?$/, "").trimEnd();

  const banner = license.trim();
  if (banner) {
    code = `${banner}\n${code}`;
    // Every line the banner adds above the code shifts the mappings one line down.
    map.mappings = ";".repeat(banner.split("\n").length) + map.mappings;
  }

  // src/ keeps the original's URL apart from the module's own: the dashboard
  // serves application.js minified, and devtools must not conflate the two.
  map.sources = [`src/${basename(source)}`];
  map.sourcesContent = [sourceText];
  delete map.debugId;

  const minPath = sourcePath.replace(/\.js$/, ".min.js");
  await Bun.write(minPath, `${code}\n//# sourceMappingURL=${mapUrl}\n`);
  await Bun.write(`${minPath}.map`, `${JSON.stringify(map)}\n`);

  const kb = (n) => `${(n / 1024).toFixed(1)} KB`;
  console.log(`${source} ${kb(sourceText.length)} → ${join(dirname(source), basename(minPath))} ${kb(code.length)}`);
}

for (const entry of ENTRIES) await build(entry);
