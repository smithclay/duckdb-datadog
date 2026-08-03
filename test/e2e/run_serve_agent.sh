#!/usr/bin/env bash
#
# Local end-to-end test for datadog_serve using a real containerized Datadog Agent.
#
# The test starts DuckDB's intake server, configures the Agent to tail a temporary file and forward
# over HTTP, writes a unique marker, then asks the still-running DuckDB process whether it arrived.
# No Datadog account or real credentials are used.
#
# Usage:
#   test/e2e/run_serve_agent.sh
#
# Environment overrides:
#   DUCKDB_BIN             DuckDB shell (default: ./build/release/duckdb)
#   DD_AGENT_IMAGE         Agent image (default: registry.datadoghq.com/agent:7)
#   DATADOG_SERVE_E2E_PORT host intake port (default: 55520)
#   POLL_TIMEOUT           seconds to wait for the Agent (default: 60)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

DUCKDB_BIN="${DUCKDB_BIN:-./build/release/duckdb}"
DD_AGENT_IMAGE="${DD_AGENT_IMAGE:-registry.datadoghq.com/agent:7}"
SERVE_PORT="${DATADOG_SERVE_E2E_PORT:-55520}"
POLL_TIMEOUT="${POLL_TIMEOUT:-60}"
POLL_INTERVAL=2
API_KEY="0123456789abcdef0123456789abcdef"
CONTAINER_NAME="duckdb-datadog-agent-e2e-$$"

log()  { printf '\033[1;34m[serve-e2e]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[serve-e2e] PASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[serve-e2e] FAIL\033[0m %s\n' "$*" >&2; return 1; }

command -v curl >/dev/null 2>&1 || { fail "curl is required"; exit 1; }
command -v docker >/dev/null 2>&1 || { fail "Docker is required"; exit 1; }
[ -x "${DUCKDB_BIN}" ] || {
	fail "duckdb binary not found at '${DUCKDB_BIN}' (run 'make release' or set DUCKDB_BIN)"
	exit 1
}
[[ "${SERVE_PORT}" =~ ^[0-9]+$ ]] && [ "${SERVE_PORT}" -ge 1 ] && [ "${SERVE_PORT}" -le 65535 ] || {
	fail "DATADOG_SERVE_E2E_PORT must be an integer from 1 through 65535"
	exit 1
}
[[ "${POLL_TIMEOUT}" =~ ^[0-9]+$ ]] && [ "${POLL_TIMEOUT}" -ge 1 ] || {
	fail "POLL_TIMEOUT must be a positive integer"
	exit 1
}
docker info >/dev/null 2>&1 || { fail "Docker is installed but its daemon is unavailable"; exit 1; }

TMP_DIR="$(mktemp -d -t duckdb_datadog_serve_e2e.XXXXXX)"
FIFO="${TMP_DIR}/duckdb.stdin"
DUCKDB_LOG="${TMP_DIR}/duckdb.log"
AGENT_CONF="${TMP_DIR}/conf.yaml"
LOG_DIR="${TMP_DIR}/logs"
LOG_FILE="${LOG_DIR}/app.log"
DUCKDB_PID=""
FIFO_FD_OPEN=0
FAILED=1

cleanup() {
	set +e
	if [ "${FIFO_FD_OPEN}" -eq 1 ]; then
		printf '.quit\n' >&3 2>/dev/null
		exec 3>&-
	fi
	if [ -n "${DUCKDB_PID}" ]; then
		kill "${DUCKDB_PID}" >/dev/null 2>&1
		wait "${DUCKDB_PID}" >/dev/null 2>&1
	fi
	if [ "${FAILED}" -eq 1 ]; then
		printf '\n--- DuckDB output ---\n' >&2
		tail -n 80 "${DUCKDB_LOG}" >&2 2>/dev/null
		printf '%s\n' '--- Datadog Agent output ---' >&2
		docker logs --tail 120 "${CONTAINER_NAME}" >&2 2>/dev/null
	fi
	docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1
	rm -rf -- "${TMP_DIR}"
}
trap cleanup EXIT INT TERM

mkdir -p "${LOG_DIR}"
: > "${LOG_FILE}"
mkfifo "${FIFO}"

cat > "${AGENT_CONF}" <<YAML
logs:
  - type: file
    path: /var/log/duckdb-datadog-e2e/app.log
    service: duckdb-datadog-e2e
    source: duckdb-datadog-e2e
YAML

log "Starting datadog_serve on port ${SERVE_PORT}"
"${DUCKDB_BIN}" -unsigned -batch -init /dev/null < "${FIFO}" > "${DUCKDB_LOG}" 2>&1 &
DUCKDB_PID=$!
exec 3> "${FIFO}"
FIFO_FD_OPEN=1
printf "LOAD datadog;\nSELECT datadog_serve('datadog:0.0.0.0:%s', '%s');\n" "${SERVE_PORT}" "${API_KEY}" >&3

elapsed=0
until curl -fsS "http://127.0.0.1:${SERVE_PORT}/healthz" >/dev/null 2>&1; do
	if ! kill -0 "${DUCKDB_PID}" >/dev/null 2>&1; then
		fail "DuckDB exited before the intake server became ready"
		exit 1
	fi
	[ "${elapsed}" -lt 30 ] || { fail "intake server did not become ready within 30s"; exit 1; }
	sleep 1
	elapsed=$((elapsed + 1))
done
ok "DuckDB intake is healthy"

docker_args=(
	-d --rm --name "${CONTAINER_NAME}"
	--add-host host.docker.internal:host-gateway
	-e "DD_API_KEY=${API_KEY}"
	-e DD_HOSTNAME=duckdb-datadog-e2e
	-e DD_LOGS_ENABLED=true
	-e DD_LOGS_CONFIG_FORCE_USE_HTTP=true
	-e "DD_LOGS_CONFIG_LOGS_DD_URL=host.docker.internal:${SERVE_PORT}"
	-e DD_LOGS_CONFIG_LOGS_NO_SSL=true
	-e DD_APM_ENABLED=false
	-e DD_PROCESS_AGENT_ENABLED=false
	-e DD_PROCESS_CONFIG_CONTAINER_COLLECTION_ENABLED=false
	-e DD_REMOTE_CONFIGURATION_ENABLED=false
	-e DD_ENABLE_PAYLOADS_EVENTS=false
	-e DD_ENABLE_PAYLOADS_SERIES=false
	-e DD_ENABLE_PAYLOADS_SERVICE_CHECKS=false
	-e DD_ENABLE_PAYLOADS_SKETCHES=false
	-v "${AGENT_CONF}:/etc/datadog-agent/conf.d/duckdb_datadog_e2e.d/conf.yaml:ro"
	-v "${LOG_DIR}:/var/log/duckdb-datadog-e2e:ro"
)

log "Starting ${DD_AGENT_IMAGE}"
docker run "${docker_args[@]}" "${DD_AGENT_IMAGE}" >/dev/null

elapsed=0
until docker exec "${CONTAINER_NAME}" agent status >/dev/null 2>&1; do
	if ! docker inspect -f '{{.State.Running}}' "${CONTAINER_NAME}" 2>/dev/null | grep -q true; then
		fail "Datadog Agent container exited during startup"
		exit 1
	fi
	[ "${elapsed}" -lt 30 ] || { fail "Datadog Agent did not become ready within 30s"; exit 1; }
	sleep 1
	elapsed=$((elapsed + 1))
done
ok "Datadog Agent is running"

MARKER="duckdb-serve-agent-e2e-$(date +%s)-${RANDOM}"
printf 'datadog agent local e2e %s\n' "${MARKER}" >> "${LOG_FILE}"
log "Waiting for Agent-forwarded marker '${MARKER}' (timeout ${POLL_TIMEOUT}s)"

elapsed=0
found=0
attempt=0
while [ "${elapsed}" -lt "${POLL_TIMEOUT}" ]; do
	attempt=$((attempt + 1))
	RESULT_FILE="${TMP_DIR}/count-${attempt}.csv"
	printf "COPY (SELECT count(*) FROM datadog_logs WHERE body LIKE '%%%s%%') TO '%s' (HEADER false);\n" \
		"${MARKER}" "${RESULT_FILE}" >&3
	query_wait=0
	while [ ! -f "${RESULT_FILE}" ] && [ "${query_wait}" -lt 20 ]; do
		sleep 0.1
		query_wait=$((query_wait + 1))
	done
	if [ -f "${RESULT_FILE}" ]; then
		count="$(tr -d '[:space:]' < "${RESULT_FILE}")"
		if [[ "${count}" =~ ^[0-9]+$ ]] && [ "${count}" -ge 1 ]; then
			found=1
			break
		fi
	fi
	sleep "${POLL_INTERVAL}"
	elapsed=$((elapsed + POLL_INTERVAL))
done

[ "${found}" -eq 1 ] || { fail "Agent-forwarded log did not reach DuckDB"; exit 1; }
ok "Agent forwarded the file log into DuckDB (matches=${count})"

DETAIL_FILE="${TMP_DIR}/row.csv"
printf "COPY (SELECT service_name, body FROM datadog_logs WHERE body LIKE '%%%s%%' LIMIT 1) TO '%s' (HEADER true);\n" \
	"${MARKER}" "${DETAIL_FILE}" >&3
detail_wait=0
while [ ! -f "${DETAIL_FILE}" ] && [ "${detail_wait}" -lt 20 ]; do
	sleep 0.1
	detail_wait=$((detail_wait + 1))
done
[ -f "${DETAIL_FILE}" ] || { fail "could not read the ingested row back from DuckDB"; exit 1; }
grep -q 'duckdb-datadog-e2e' "${DETAIL_FILE}" || { fail "Agent service metadata was not mapped"; exit 1; }
ok "service metadata and message body mapped correctly"

printf "SELECT datadog_stop('datadog:0.0.0.0:%s');\n.quit\n" "${SERVE_PORT}" >&3
exec 3>&-
FIFO_FD_OPEN=0
wait "${DUCKDB_PID}"
DUCKDB_PID=""

FAILED=0
ok "local Agent end-to-end test succeeded"
