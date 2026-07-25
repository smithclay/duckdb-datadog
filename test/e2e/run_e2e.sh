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

# The extension accepts a full-URL SITE (e.g. https://us5.datadoghq.com) and normalizes it, but the
# raw curl intake host below needs a bare host. Strip any scheme, trailing slash, and api./app.
# prefix so both a bare site and a URL-form site work.
DD_SITE_HOST="${DD_SITE#http://}"
DD_SITE_HOST="${DD_SITE_HOST#https://}"
DD_SITE_HOST="${DD_SITE_HOST%/}"
DD_SITE_HOST="${DD_SITE_HOST#api.}"
DD_SITE_HOST="${DD_SITE_HOST#app.}"

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
INTAKE_URL="https://http-intake.logs.${DD_SITE_HOST}/api/v2/logs"
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
	# `.output /dev/null` around CREATE SECRET suppresses its `Success = true` result row,
	# which would otherwise be prepended to (and corrupt) the scalar value we read back.
	cat > "${SQL_FILE}" <<SQL
.output /dev/null
CREATE OR REPLACE SECRET dd_e2e (TYPE datadog, API_KEY '${DD_API_KEY}', APP_KEY '${DD_APP_KEY}', SITE '${DD_SITE}');
.output
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
# Materialize the matching rows once into a temp table, then run every assertion against that local
# table. Fetching once (rather than a separate scan per assertion) keeps the test simple and light
# on the rate-limited search API; the extension itself retries transient HTTP 429s.
cat > "${SQL_FILE}" <<SQL
.output /dev/null
CREATE OR REPLACE SECRET dd_e2e (TYPE datadog, API_KEY '${DD_API_KEY}', APP_KEY '${DD_APP_KEY}', SITE '${DD_SITE}');
CREATE TEMP TABLE e2e_rows AS SELECT * FROM read_datadog_logs(query => '${DD_QUERY}', "from" => 'now-15m', "to" => 'now');
.output
SELECT (SELECT count(*) FROM (DESCRIBE e2e_rows))::VARCHAR || '|' ||
  (SELECT (count(*) >= 1
    AND bool_and(service_name = 'duckdb-datadog-e2e')
    AND bool_and(severity_text = 'info')
    AND bool_and(severity_number = 9)
    AND bool_and(body LIKE '%${MARKER}%')
    AND bool_and(time_unix_nano IS NOT NULL)) FROM e2e_rows)::VARCHAR;
.print ---SAMPLE---
SELECT time_unix_nano, service_name, severity_text, severity_number, body FROM e2e_rows LIMIT 1;
SQL
assert_out="$("${DUCKDB_BIN}" -unsigned -noheader -list -init /dev/null < "${SQL_FILE}")"

summary_line="$(printf '%s\n' "${assert_out}" | head -n1 | tr -d '[:space:]')"
column_count="${summary_line%%|*}"
checks="${summary_line##*|}"

[ "${column_count}" = "18" ] || fail "expected 18 OTLP columns, got '${column_count}'"
ok "output schema has the 18 OTLP columns"
[ "${checks}" = "true" ] || fail "row content assertions failed (got '${checks}')"
ok "row content maps correctly (service_name, severity, body, timestamp)"

log "Sample row:"
printf '%s\n' "${assert_out}" | sed -n '/---SAMPLE---/,$p' | tail -n +2

# ---------------------------------------------------------------------------
# 4. Send a log THROUGH the extension (send_datadog_logs) and read it back
# ---------------------------------------------------------------------------
# This exercises the write path: build one OTLP-shaped row in DuckDB, push it to Datadog with the
# send_datadog_logs() scalar function, then poll read_datadog_logs() until the intake is searchable.
SEND_MARKER="duckdbsend-$(date +%s)-${RANDOM}"
SEND_SERVICE="duckdb-datadog-send-e2e"
SEND_QUERY="service:${SEND_SERVICE} ${SEND_MARKER}"

log "Sending a log via send_datadog_logs() with marker '${SEND_MARKER}'"
# The OTLP-shaped row mirrors read_datadog_logs' output columns; extra columns would be ignored.
send_sql=$(cat <<SQL
CREATE TEMP TABLE to_send AS SELECT
    now()::TIMESTAMP_NS                                             AS time_unix_nano,
    '${SEND_SERVICE}'                                               AS service_name,
    'info'                                                          AS severity_text,
    9                                                               AS severity_number,
    'duckdb-datadog send e2e ${SEND_MARKER}'                       AS body,
    '{"marker":"${SEND_MARKER}","test":"duckdb_datadog_send_e2e"}' AS log_attributes,
    '{"host":"${HOST_NAME}","ddtags":["test:duckdb_datadog_send_e2e","marker:${SEND_MARKER}"]}' AS resource_attributes;
SELECT send_datadog_logs(t) FROM to_send t;
SQL
)
send_result="$(run_duckdb_scalar "${send_sql}" 2>/dev/null | tr -d '[:space:]')" || true
[ "${send_result}" = "ok" ] || fail "send_datadog_logs did not return 'ok' (got '${send_result:-<empty>}')"
ok "send_datadog_logs accepted the log (returned 'ok')"

log "Polling read_datadog_logs for the sent log (timeout ${POLL_TIMEOUT}s)..."
send_count_sql="SELECT count(*) FROM read_datadog_logs(query => '${SEND_QUERY}', \"from\" => 'now-15m', \"to\" => 'now');"
elapsed=0
send_found=0
while [ "${elapsed}" -lt "${POLL_TIMEOUT}" ]; do
	count="$(run_duckdb_scalar "${send_count_sql}" 2>/dev/null | tr -d '[:space:]')" || true
	if [[ "${count}" =~ ^[0-9]+$ ]] && [ "${count}" -ge 1 ]; then
		send_found=1
		break
	fi
	log "  not indexed yet (matches=${count:-0}); retrying in ${POLL_INTERVAL}s (${elapsed}/${POLL_TIMEOUT}s)"
	sleep "${POLL_INTERVAL}"
	elapsed=$((elapsed + POLL_INTERVAL))
done
[ "${send_found}" -eq 1 ] || fail "log sent via send_datadog_logs (marker '${SEND_MARKER}') never became searchable within ${POLL_TIMEOUT}s"
ok "round-trip through send_datadog_logs succeeded (matches=${count})"

# ---------------------------------------------------------------------------
# 5. Independent cross-check via pup (optional)
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
