# duckdb-datadog

A DuckDB extension that reads logs and open monitor alerts from Datadog directly into DuckDB
tables. Log rows come from the [Datadog Logs Search API v2](https://docs.datadoghq.com/api/latest/logs/)
and conform to the
[duckdb-otlp](https://github.com/smithclay/otlp2records) `read_otlp_logs` schema, so Datadog logs
drop straight into an OTLP-shaped lakehouse alongside data from other sources.

Logs and open monitor alerts are supported today; traces/spans and metrics are planned.

## Quick start

```sql
LOAD datadog;

-- Store your Datadog credentials once (kept out of query text; redacted in duckdb_secrets()).
CREATE SECRET (
    TYPE datadog,
    API_KEY '<dd-api-key>',
    APP_KEY '<dd-application-key>',
    SITE 'datadoghq.com'          -- optional; e.g. datadoghq.eu, us5.datadoghq.com
);

-- Read a window of logs into a table.
CREATE TABLE logs AS
SELECT * FROM read_datadog_logs(
    query  => 'service:web-store status:error',
    "from" => 'now-1h',           -- relative (now-1h) or absolute (epoch ms / ISO-8601)
    "to"   => 'now'
);
```

`API_KEY` needs the `logs_read_data` permission. `from`/`to` accept anything the Datadog API
accepts: relative like `now-1h`, epoch milliseconds, or ISO-8601.

## Datadog catalog

Attach a Datadog account as a read-only DuckDB catalog to query each log index as a table in its
`logs` schema:

```sql
LOAD datadog;

CREATE SECRET dd_prod (
    TYPE datadog,
    API_KEY '<dd-api-key>',
    APP_KEY '<dd-application-key>',
    SITE 'datadoghq.com'
);

ATTACH 'datadog:' AS dd (
    TYPE datadog,
    SECRET 'dd_prod'
);

SELECT * FROM dd.logs.main LIMIT 10;

-- Index names are preserved exactly; quote names that are not plain SQL identifiers.
SELECT * FROM dd.logs."security-events" LIMIT 10;

-- One row per currently triggered monitor group.
SELECT * FROM dd.alerts.open ORDER BY last_triggered_at DESC;
```

Without `INDEXES`, `ATTACH` calls Datadog's log-index configuration endpoint once and caches the
returned index list for the lifetime of the attachment. Automatic discovery requires
`logs_read_config`; searching any catalog table requires `logs_read_data`.

Supply a `VARCHAR[]` when discovery is unavailable or when deterministic, network-free attachment
is preferable:

```sql
ATTACH 'datadog:' AS dd (
    TYPE datadog,
    SECRET 'dd_prod',
    INDEXES ['main', 'security-events']
);
```

When `INDEXES` is present, attachment makes no network request. Duplicate names are ignored while
input order and spelling are preserved. Omitting `SECRET` uses the same first in-scope `datadog`
secret selection as `read_datadog_logs`.

Every catalog log table has the same 18-column schema as `read_datadog_logs` and uses the reader's
current defaults: query `*`, from `now-15m` to `now`, ascending `timestamp` sort, page size 1000,
unlimited rows, four retries, and a 60-second request timeout. The catalog is read-only. Use
`read_datadog_logs` when you need a custom query or time window.

### Open alerts

`dd.alerts.open` reads Datadog's triggered monitor groups. A grouped monitor contributes one row
per reporting group, so (for example) 14 triggered hosts remain 14 independently actionable
alerts rather than collapsing into one monitor-level status. The table includes all states Datadog
defines as triggered: `Alert`, `Warn`, and `No Data`.

```sql
SELECT monitor_id, monitor_name, group_name, group_tags, status,
       last_triggered_at, last_nodata_at
FROM dd.alerts.open
ORDER BY last_triggered_at DESC NULLS LAST;
```

The alert table is fetched lazily when scanned and paginates through the Monitor Groups Search API.
It requires the `monitors_read` application-key permission. `RETRIES` and `TIMEOUT` from `ATTACH`
apply to alert requests as well as log requests. Datadog reports a zero timestamp when a group has
never entered a state; the table exposes those sentinel values as SQL `NULL`.

For a bounded latest-logs relation suitable for an interactive browser query, configure the
attachment explicitly:

```sql
ATTACH 'datadog:' AS dd (
    TYPE datadog,
    SECRET dd_web,
    INDEXES ['main'],
    SORT '-timestamp',
    PAGE_SIZE 100,
    MAX_ROWS 100,
    RETRIES 0
);

SELECT time_unix_nano, service_name, severity_text, body
FROM dd.logs.main
ORDER BY time_unix_nano DESC
LIMIT 100;
```

This configuration asks Datadog for the newest records first and makes at most one logs-search
request with a page limit of 100. `MAX_ROWS` changes every catalog table in that attachment into a
bounded source relation; `COUNT(*)`, exports, joins, and aggregations see only that bounded set, not
the full matching Datadog window. Omit `MAX_ROWS` (or set it to 0) to preserve an unlimited catalog
scan.

This is explicit catalog configuration, not automatic SQL `ORDER BY`/`LIMIT` or Top-N optimizer
pushdown. DuckDB still evaluates the SQL ordering and limit locally. `SORT`, `PAGE_SIZE`,
`MAX_ROWS`, `RETRIES`, and `TIMEOUT` are attachment-wide and apply to every index table in that
catalog; `TIMEOUT` is measured in seconds.

A small, conservative subset of SQL predicates is pushed into Datadog while the original `WHERE`
clause is still evaluated by DuckDB for exact SQL semantics:

```sql
SELECT time_unix_nano, service_name, severity_text, body
FROM dd.logs.main
WHERE service_name = 'edge'
  AND severity_text = 'error'
  AND time_unix_nano >= TIMESTAMP '2026-07-16 03:00:00';
```

Literal equality on `service_name` and `severity_text` becomes Datadog `service:` and `status:`
search terms. `AND` combinations are supported. Timestamp predicates remain local for catalog
tables because their `now-15m` to `now` window is evaluated by Datadog when the request arrives;
rewriting those relative endpoints with the client clock could shift the window. For
`read_datadog_logs`, literal `>`, `>=`, `<`, and `<=` bounds on `time_unix_nano` additionally narrow
the request when both `from` and `to` are explicit epoch-millisecond values. Other
predicates—including `OR`, `NOT`, `IN`, `LIKE`, regular expressions, JSON expressions, and
non-literal comparisons—remain local DuckDB filters.

### `read_datadog_logs` parameters

| Parameter   | Type    | Default    | Description |
|-------------|---------|------------|-------------|
| `query`     | VARCHAR | `*`        | Datadog log search query. |
| `from`      | VARCHAR | `now-15m`  | Start of the time window. |
| `to`        | VARCHAR | `now`      | End of the time window. |
| `sort`      | VARCHAR | `timestamp` | Datadog sort: `timestamp` (oldest first) or `-timestamp` (newest first). |
| `page_size` | BIGINT  | `1000`     | Rows fetched per API request (1–1000, the Datadog max). |
| `max_rows`  | BIGINT  | unlimited  | Safety cap on total rows returned. |
| `retries`   | BIGINT  | `4`        | Retry budget for transient failures (HTTP 429/5xx, network errors); 0 disables retrying. |
| `timeout`   | BIGINT  | `60`       | Per-request connection/read timeout, in seconds. |
| `secret`    | VARCHAR | first `datadog` secret | Name of a specific secret to use. |

The function pages through the whole window for you by following Datadog's cursor
(`meta.page.after`) in the configured timestamp order. Results stream page-by-page and are never
fully buffered in memory. When `max_rows` is positive, each request is limited to the smaller of
`page_size` and the remaining row budget, and pagination stops at the cap. For a window so large it
exceeds Datadog's cursor depth, page through it by calling the function once per narrower sub-range
(e.g. an hour at a time).

Transient failures are retried automatically: HTTP 429 waits out the server-advised rate-limit
reset (`X-RateLimit-Reset` / `Retry-After`), and HTTP 5xx or dropped connections retry with
exponential backoff — so a long paginated scan rides out blips instead of losing its cursor.
Retry waits honor query cancellation, so interrupting a query takes effect within ~100ms. Only
the columns a query actually selects are decoded from the API response (projection pushdown):
network cost is unchanged, but queries that skip `log_attributes` — counts, `GROUP BY
service_name`, severity triage — avoid most per-row CPU and buffering.

Throughput is bounded by Datadog's search API rate limit, which on some sites is as low as
2 requests / 10s (≈ 12k rows/minute at the default page size). That is fine for investigation
windows and error triage; for bulk export of entire indexes, use Datadog log archives instead.

### Output schema

Matches duckdb-otlp `read_otlp_logs`:

`time_unix_nano`, `observed_time_unix_nano` (TIMESTAMP_NS); `trace_id`, `span_id` (VARCHAR hex);
`service_name`, `service_namespace`, `service_instance_id` (VARCHAR); `severity_number` (INTEGER),
`severity_text` (VARCHAR); `event_name`, `body` (VARCHAR); `resource_attributes` (VARCHAR JSON —
host/tags); `scope_name`, `scope_version`, `scope_attributes` (VARCHAR); `log_attributes` (VARCHAR
JSON — Datadog custom attributes); `dropped_attributes_count`, `flags` (INTEGER).

Because attribute columns are JSON strings, query them with DuckDB's JSON functions, e.g.
`SELECT log_attributes->>'$.http.status_code' FROM logs`.

## Building

Native dependencies are minimal: only OpenSSL (via vcpkg). HTTP (cpp-httplib) and JSON (yyjson)
reuse the copies DuckDB already bundles, so nothing extra is pulled in.

```shell
# vcpkg provides OpenSSL
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake

make            # builds ./build/release/duckdb and the loadable extension
make test       # runs the offline SQL tests in test/sql/
```

Built artifacts:
- `./build/release/duckdb` — DuckDB shell with the extension preloaded.
- `./build/release/extension/datadog/datadog.duckdb_extension` — the loadable binary.

For DuckDB-WASM, source an Emscripten SDK compatible with the target DuckDB-WASM runtime and build
the same checkout directly:

```shell
make wasm_eh
```

Emscripten builds use DuckDB's browser-backed HTTP transport and do not link OpenSSL or native
sockets. In a browser build, `SITE` may also be a full `http://` or `https://` URL pointing at a
same-origin CORS proxy; ordinary Datadog site names keep the `https://api.<site>` behavior used by
native builds.

## Testing

`make test` runs `test/sql/datadog.test`, which covers extension loading, the `datadog` secret
type (including credential redaction), function registration, and the output schema. These tests
are fully offline (no network).

### End-to-end test

`test/e2e/run_e2e.sh` exercises the full round-trip against a real Datadog account: it sends a
uniquely-tagged log via the log intake API, then reads it back through `read_datadog_logs` and
asserts the mapping. It shares credentials with the [`pup` CLI](https://docs.datadoghq.com/cli/)
(`DD_API_KEY` / `DD_APP_KEY` / `DD_SITE`) and, when `pup` is installed, uses it for an auth-status
check and an independent cross-check of the ingested log.

```shell
make release                                   # build the duckdb binary + extension first
DD_API_KEY=... DD_APP_KEY=... test/e2e/run_e2e.sh
```

Because Datadog ingestion has indexing latency, the script polls for up to ~150s.

The automated end-to-end test intentionally exercises `read_datadog_logs` and therefore does not
require `logs_read_config`. To verify live catalog discovery manually with credentials that have
both `logs_read_data` and `logs_read_config`, create a secret as above, omit `INDEXES` from the
`ATTACH`, then run `SELECT * FROM dd.logs.main LIMIT 10` (substituting an index present in the
account).
