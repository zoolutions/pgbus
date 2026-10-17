#!/usr/bin/env bash
# The report-only kino-smoke job (.github/workflows/main.yml, issue #527): serve
# the dummy app with kino, GET /up, open one SSE subscription, and write what
# happened to tmp/kino-smoke/. kino has no rack.hijack, so streams answer with a
# streaming body (spec/dummy/config/kino.ru).
#
# Two runs. Threaded mode is the control: the app and its stream work on kino,
# so whatever Ractor mode records is a Ractor blocker, not a broken setup.
# Ractor mode (ractor-rails-shim) cannot pass until Rails runs in a non-main
# Ractor; it records the first blocker and never fails the job. The script
# exits non-zero only when the control run fails.
#
#   BUNDLE_GEMFILE=gemfiles/kino.gemfile PGBUS_DATABASE_URL=postgres://localhost:5432/pgbus_test \
#     script/kino_smoke.sh
#
# PGBUS_DATABASE_URL names a database with the PGMQ schema (bin/conformance-db).
set -uo pipefail

out=tmp/kino-smoke
mkdir -p "$out"
export RAILS_ENV=test
export PGBUS_STREAMS_SECRET="${PGBUS_STREAMS_SECRET:-kino-smoke}"
: "${PGBUS_DATABASE_URL:?set PGBUS_DATABASE_URL to a database with the PGMQ schema}"

signed=$(bundle exec ruby -e 'require "./spec/dummy/config/environment"
  Pgbus.configure { |c| c.streams_signed_name_secret = ENV.fetch("PGBUS_STREAMS_SECRET") }
  print Pgbus::Streams::SignedName.sign("kino_smoke")' 2> "$out/sign.txt")

KINO_SMOKE_MODE=ractor bundle exec kino --check spec/dummy/config/kino.ru > "$out/check.txt" 2>&1
check=$?

# kino's own lines: a crashed worker, or a request answered with its error.
errors='^kino\[[0-9]+\] .*(crashed|[0-9]{3} (GET|POST) .* · )'

# smoke MODE PORT: serve, request, stop, and print the mode's report lines.
smoke() {
  local mode=$1 port=$2 log="$out/server-$1.log"
  KINO_SMOKE_MODE=$mode bundle exec kino -m "$mode" -w 2 -p "$port" spec/dummy/config/kino.ru > "$log" 2>&1 &
  local server=$!
  for _ in $(seq 1 90); do
    curl -s --max-time 2 -o /dev/null "http://127.0.0.1:$port/up" && break
    sleep 1
  done

  # Every request is bounded: a worker that accepts and never answers is
  # recorded (HTTP 000), not left to hang the job. A stream stays open, so it
  # is read for 5 seconds and then cut.
  up=$(curl -s --max-time 30 -o "$out/up-$mode.body" -w '%{http_code}' "http://127.0.0.1:$port/up")
  sse=$(curl -s -N --max-time 5 -o "$out/sse-$mode.body" -w '%{http_code}' \
    "http://127.0.0.1:$port/pgbus/streams/$signed")
  kill "$server" 2> /dev/null
  wait "$server" 2> /dev/null

  connected=no
  grep -q '^event: pgbus:connected' "$out/sse-$mode.body" 2> /dev/null && connected=yes
  blocker=$(grep -m1 -E "$errors" "$log" | cut -c1-400 || true)
  # A server that never answered: the error it died with, if any (a Ruby
  # error line ends in its class; the shim's notices do not).
  if [ -z "$blocker" ] && [ "$up" != 200 ]; then
    blocker=$(grep -m1 -E '\(([A-Z][A-Za-z0-9_]*::)*[A-Z][A-Za-z0-9_]*(Error|Exception)\)$' "$log" | cut -c1-400 || true)
  fi
  echo "== kino -m $mode"
  echo "GET /up: HTTP $up"
  echo "GET /pgbus/streams/<signed>: HTTP $sse, pgbus:connected event: $connected"
  echo "first blocker: ${blocker:-none}"
  echo "kino's error lines (first 5):"
  grep -E "$errors" "$log" | cut -c1-400 | head -5
  [ "$up" = 200 ] && [ "$sse" = 200 ] && [ "$connected" = yes ]
}

{
  echo "kino --check (ractor): exit $check ($(grep -m1 '^check:' "$out/check.txt" || echo 'no verdict'))"
  smoke threaded "${KINO_SMOKE_PORT:-39292}"
  control=$?
  smoke ractor "$((${KINO_SMOKE_PORT:-39292} + 1))"
  exit "$control"
} | tee "$out/report.txt"
