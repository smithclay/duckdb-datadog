#!/usr/bin/env bash
#
# End-to-end test for the duckdb-datadog extension.
#
# It sends a uniquely-tagged log line to Datadog, then reads it back through the extension's
# read_datadog_logs() table function and asserts the round-trip worked and the row is
# OTLP-shaped. Because Datadog log ingestion has indexing latency, it polls for up to a couple
# of minutes.
#
# Credentials come from the environment (the same variables the `pup` CLI uses):
#   DD_API_KEY   required  - Datadog API key (needs logs_read_data + log intake)
#   DD_APP_KEY   required  - Datadog Application key (needed by the Logs Search API)
#   DD_SITE      optional  - Datadog site, default datadoghq.com
#
# If you are logged in with `pup auth login`, make sure those keys are exported (pup reads the
# same DD_API_KEY / DD_APP_KEY / DD_SITE variables). This script uses `pup` for an auth-status
# check and an independent cross-check of the ingested log when it is available.
#
# Usage:
#   DD_API_KEY=... DD_APP_KEY=... test/e2e/run_e2e.sh
#
# Environment overrides:
#   DUCKDB_BIN       path to the duckdb shell (default: ./build/release/duckdb)
#   POLL_TIMEOUT     seconds to wait for the log to become searchable (default: 150)
#   POLL_INTERVAL    seconds between polls (default: 10)

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

DUCKDB_BIN="${DUCKDB_BIN:-./build/release/duckdb}"
DD_SITE="${DD_SITE:-datadoghq.com}"
POLL_TIMEOUT="${POLL_TIMEOUT:-150}"
POLL_INTERVAL="${POLL_INTERVAL:-10}"

log()  { printf '\033[1;34m[e2e]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[e2e] PASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[e2e] FAIL\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
command -v curl >/dev/null 2>&1 || fail "curl is required"
[ -x "${DUCKDB_BIN}" ] || fail "duckdb binary not found/executable at '${DUCKDB_BIN}' (run 'make release' first, or set DUCKDB_BIN)"
: "${DD_API_KEY:?DD_API_KEY must be set (export it, or run 'pup auth login' and export your keys)}"
: "${DD_APP_KEY:?DD_APP_KEY must be set (export it, or run 'pup auth login' and export your keys)}"

# Best-effort: show pup auth state so credential problems are obvious up front.
if command -v pup >/dev/null 2>&1; then
	log "pup detected; auth status:"
	pup auth status --site "${DD_SITE}" 2>/dev/null || pup auth status 2>/dev/null || log "  (pup auth status unavailable)"
else
	log "pup not on PATH; continuing with DD_API_KEY/DD_APP_KEY from the environment"
fi

# Secret-bearing temp files (curl header config, duckdb SQL) are removed on exit.
CURL_CFG=""
SQL_FILE=""
cleanup() {
	rm -f "${CURL_CFG}" "${SQL_FILE}" 2>/dev/null || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1. Send a uniquely-tagged log line via the Datadog log intake API
# ---------------------------------------------------------------------------
MARKER="duckdbe2e-$(date +%s)-${RANDOM}"
INTAKE_URL="https://http-intake.logs.${DD_SITE}/api/v2/logs"
HOST_NAME="$(hostname)"

# Keep DD-API-KEY off curl's argv (visible in ps) by passing it via a 600-mode config file.
CURL_CFG="$(mktemp -t duckdb_datadog_e2e.XXXXXX.curl)"
chmod 600 "${CURL_CFG}"
printf 'header = "DD-API-KEY: %s"\n' "${DD_API_KEY}" > "${CURL_CFG}"

log "Sending test log with marker '${MARKER}' to ${INTAKE_URL}"
intake_payload=$(cat <<JSON
[{
  "ddsource": "duckdb-datadog-e2e",
  "service": "duckdb-datadog-e2e",
  "hostname": "${HOST_NAME}",
  "ddtags": "test:duckdb_datadog_e2e,marker:${MARKER}",
  "status": "info",
  "message": "duckdb-datadog e2e test ${MARKER}",
  "marker": "${MARKER}"
}]
JSON
)

intake_status=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${INTAKE_URL}" \
	-K "${CURL_CFG}" \
	-H "Content-Type: application/json" \
	-d "${intake_payload}")
if [ "${intake_status}" != "202" ] && [ "${intake_status}" != "200" ]; then
	fail "log intake returned HTTP ${intake_status} (expected 202)"
fi
ok "log accepted by intake (HTTP ${intake_status})"

# ---------------------------------------------------------------------------
# 2. Read it back through the extension, polling until it is searchable
# ---------------------------------------------------------------------------
# The query is free-text on the marker plus the service, which is reliable regardless of how
# Datadog facets the custom attribute/tag.
DD_QUERY="service:duckdb-datadog-e2e ${MARKER}"

# A temp SQL file keeps the secret keys off the process command line (removed by cleanup trap).
SQL_FILE="$(mktemp -t duckdb_datadog_e2e.XXXXXX.sql)"
chmod 600 "${SQL_FILE}"

run_duckdb_scalar() { # $1 = SQL producing a single value
	cat > "${SQL_FILE}" <<SQL
CREATE OR REPLACE SECRET dd_e2e (TYPE datadog, API_KEY '${DD_API_KEY}', APP_KEY '${DD_APP_KEY}', SITE '${DD_SITE}');
$1
SQL
	"${DUCKDB_BIN}" -unsigned -noheader -list -init /dev/null < "${SQL_FILE}"
}

count_sql="SELECT count(*) FROM read_datadog_logs(query => '${DD_QUERY}', \"from\" => 'now-15m', \"to\" => 'now');"

log "Polling read_datadog_logs for the log (timeout ${POLL_TIMEOUT}s)..."
elapsed=0
found=0
while [ "${elapsed}" -lt "${POLL_TIMEOUT}" ]; do
	# `|| true` so a transient duckdb/API error during polling doesn't abort the run under set -e.
	count="$(run_duckdb_scalar "${count_sql}" 2>/dev/null | tr -d '[:space:]')" || true
	if [[ "${count}" =~ ^[0-9]+$ ]] && [ "${count}" -ge 1 ]; then
		found=1
		break
	fi
	log "  not indexed yet (matches=${count:-0}); retrying in ${POLL_INTERVAL}s (${elapsed}/${POLL_TIMEOUT}s)"
	sleep "${POLL_INTERVAL}"
	elapsed=$((elapsed + POLL_INTERVAL))
done
[ "${found}" -eq 1 ] || fail "log with marker '${MARKER}' never became searchable within ${POLL_TIMEOUT}s"
ok "read_datadog_logs returned the log (matches=${count})"

# ---------------------------------------------------------------------------
# 3. Assert the row is OTLP-shaped and mapped correctly
# ---------------------------------------------------------------------------
column_count="$(run_duckdb_scalar "SELECT count(*) FROM (DESCRIBE SELECT * FROM read_datadog_logs(query => '${DD_QUERY}', \"from\" => 'now-15m', \"to\" => 'now'));" | tr -d '[:space:]')"
[ "${column_count}" = "18" ] || fail "expected 18 OTLP columns, got '${column_count}'"
ok "output schema has the 18 OTLP columns"

checks_sql="SELECT
  (count(*) >= 1)
  AND bool_and(service_name = 'duckdb-datadog-e2e')
  AND bool_and(severity_text = 'info')
  AND bool_and(severity_number = 9)
  AND bool_and(body LIKE '%${MARKER}%')
  AND bool_and(time_unix_nano IS NOT NULL)
FROM read_datadog_logs(query => '${DD_QUERY}', \"from\" => 'now-15m', \"to\" => 'now');"
checks="$(run_duckdb_scalar "${checks_sql}" | tr -d '[:space:]')"
[ "${checks}" = "true" ] || fail "row content assertions failed (got '${checks}')"
ok "row content maps correctly (service_name, severity, body, timestamp)"

log "Sample row:"
run_duckdb_scalar "SELECT time_unix_nano, service_name, severity_text, severity_number, body
FROM read_datadog_logs(query => '${DD_QUERY}', \"from\" => 'now-15m', \"to\" => 'now') LIMIT 1;" || true

# ---------------------------------------------------------------------------
# 4. Independent cross-check via pup (optional)
# ---------------------------------------------------------------------------
if command -v pup >/dev/null 2>&1; then
	log "Cross-checking the same log via 'pup logs search'..."
	if pup logs search --query "${DD_QUERY}" --from 15m 2>/dev/null | grep -q "${MARKER}"; then
		ok "pup logs search also sees the log"
	else
		log "  (pup cross-check did not surface the marker; not failing the test on this)"
	fi
fi

ok "end-to-end test succeeded"
