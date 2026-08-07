# duckdb-datadog

A DuckDB extension that reads logs, open monitor alerts, and APM service dependencies from Datadog
directly into DuckDB, runs server-side log aggregations (counts, cardinalities, percentiles),
receives logs through a native intake server, and sends OTLP-shaped log tables back to Datadog.
Log rows conform to the [duckdb-otlp](https://github.com/smithclay/otlp2records)
`read_otlp_logs` schema, so Datadog logs drop straight into an OTLP-shaped lakehouse alongside
data from other sources.

Reading, receiving, and sending logs, reading and writing traces/spans, reading open monitor
alerts, and querying service maps are supported today.
See [Receiving logs](#receiving-logs) for the native `datadog_serve` intake server,
[Sending logs](#sending-logs) for the `send_datadog_logs` scalar function, and
[Traces](#traces) for `read_datadog_traces` / `write_datadog_traces`.

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
    SECRET 'dd_prod',
    SERVICE_MAP_ENV 'prod'
);

SELECT * FROM dd.logs.main LIMIT 10;

-- Index names are preserved exactly; quote names that are not plain SQL identifiers.
SELECT * FROM dd.logs."security-events" LIMIT 10;

-- One row per currently triggered monitor group.
SELECT * FROM dd.alerts.open ORDER BY last_triggered_at DESC;

-- One row per directed caller -> callee edge from the latest hour.
SELECT * FROM dd.service_map.dependencies;

-- Indexed spans (last 15 minutes) in the duckdb-otlp trace schema.
SELECT * FROM dd.traces.spans WHERE service_name = 'checkout';

-- Timeseries points for the attachment's METRICS_QUERY.
SELECT * FROM dd.metrics.series;
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

### Service maps

`dd.service_map.dependencies` exposes Datadog's APM dependency graph as directed edges. Configure
its scope on the attachment; the default window is `-1h` through `now`:

```sql
ATTACH 'datadog:' AS dd (
    TYPE datadog,
    SECRET 'dd_prod',
    INDEXES ['main'],
    SERVICE_MAP_ENV 'prod',
    SERVICE_MAP_START_TIME '-1h',
    SERVICE_MAP_END_TIME 'now',
    SERVICE_MAP_PRIMARY_TAG 'region:us-west-2'
);

SELECT source_service, target_service
FROM dd.service_map.dependencies;
```

The table is lazy: `ATTACH` and schema inspection do not call the service-dependencies API. Relative
windows are evaluated when each scan starts, so a long-lived attachment continues to represent the
latest hour. Existing attachments that omit `SERVICE_MAP_ENV` still work for logs and alerts; the
service-map table remains discoverable and gives a focused configuration error only if scanned.

For an ad hoc environment or window, use the same scanner through the table function:

```sql
SELECT *
FROM read_datadog_service_dependencies(
    env => 'staging',
    "from" => 'now-15m',
    "to" => 'now',
    primary_tag => 'region:us-east-1'
);
```

`env` is required. `from` and `to` accept epoch seconds, `now`, or past relative values such as
`-15m` and `now-1h` (units: `s`, `m`, `h`, `d`, or `w`). Optional `secret`, `retries`, and `timeout`
parameters follow `read_datadog_logs`. The API response supplies topology but not per-edge types or
statistics, so those canonical columns are `NULL` for Datadog.

### Traces and metrics tables

`dd.traces.spans` exposes indexed spans through the same scanner as
[`read_datadog_traces`](#reading-spans): the 24-column duckdb-otlp schema over the reader's
defaults (query `*`, `now-15m` to `now`, and the attachment's `SORT`/`PAGE_SIZE`/`MAX_ROWS`/
`RETRIES`/`TIMEOUT`). Supported `WHERE` predicates are translated conservatively — a
`service_name = '...'` equality becomes a `service:` search term and `start_time_unix_nano`
bounds tighten an absolute window — with the original predicates always retained as DuckDB
residual filters. Use `read_datadog_traces` for a custom span query or relative window.

`dd.metrics.series` runs the attachment's metrics query through the same scanner as
`read_datadog_metrics`:

```sql
ATTACH 'datadog:' AS dd (
    TYPE datadog,
    SECRET 'dd_prod',
    INDEXES ['main'],
    METRICS_QUERY 'avg:system.cpu.user{*} by {host}',
    METRICS_START_TIME '-1h',   -- default now-15m
    METRICS_END_TIME 'now'
);

SELECT time_unix_nano, name, double_value, service_name
FROM dd.metrics.series;
```

Like the service map, the metrics table is lazy: it binds and `DESCRIBE`s without configuration
and raises a focused `METRICS_QUERY` error only when scanned, and relative windows are resolved
when each scan starts, so a long-lived attachment keeps returning the latest window.

Both interfaces return the same schema, making provider maps easy to combine with
`UNION ALL BY NAME`: `provider`, `source_service`, `target_service`, `source_type`, `target_type`, `edge_type`,
`environment`, `window_start`, `window_end`, `request_count`, `error_count`, `fault_count`,
`throttle_count`, `total_response_time_seconds`, `source_attributes`, `target_attributes`, and
`edge_attributes`. The `window_*` columns are `TIMESTAMP_NS`; attributes are JSON strings when a
provider supplies them. Datadog's service-dependencies endpoint is public beta and requires APM
read access.

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

## Aggregations

`read_datadog_log_stats` asks the [Logs Aggregation API](https://docs.datadoghq.com/api/latest/logs/#aggregate-events)
one question and returns its buckets, so a count over millions of logs costs one request instead
of a paginated row scan. Reach for it for count-style questions; use `read_datadog_logs` when you
need the rows themselves.

```sql
-- Error logs per service over the last hour.
SELECT by->>'$.service' AS service, value::BIGINT AS errors
FROM read_datadog_log_stats(
    query    => 'status:error',
    "from"   => 'now-1h',
    "to"     => 'now',
    compute  => 'count',
    group_by => ['service'],
    group_limit => 100
);

-- Distinct trace ids matching a filter (one cardinality request, no row transfer).
SELECT value::BIGINT AS traces
FROM read_datadog_log_stats(
    query   => 'service:payment @duration_ns:>=4500000000',
    "from"  => 'now-1h',
    "to"    => 'now',
    compute => 'cardinality',
    metric  => '@trace_id'
);
```

Each row is one bucket: `by` (VARCHAR) holds the group-by key values as a JSON object (`{}` for an
ungrouped total) and `value` (DOUBLE) holds the computed number. `compute` accepts Datadog's
aggregations (`count`, `cardinality`, `avg`, `sum`, `min`, `max`, `pc75`/`pc90`/`pc95`/`pc98`/`pc99`);
everything except `count` needs `metric`, the `@attribute` facet to aggregate over. A null compute
(e.g. a percentile over zero matching logs) is a SQL `NULL`. `value` is exact for counts up to
2^53, far past any log volume the API will return.

| Parameter     | Type      | Default    | Description |
|---------------|-----------|------------|-------------|
| `query`       | VARCHAR   | `*`        | Datadog log search query. |
| `from`        | VARCHAR   | `now-15m`  | Start of the time window. |
| `to`          | VARCHAR   | `now`      | End of the time window. |
| `compute`     | VARCHAR   | `count`    | Datadog aggregation to compute. |
| `metric`      | VARCHAR   | —          | Facet to aggregate (required unless `compute` is `count`). |
| `group_by`    | VARCHAR[] | none       | Facets to group by. |
| `group_limit` | BIGINT    | `10`       | Max buckets per facet (1–10000, the Datadog cap). |
| `retries`, `timeout`, `secret` | | | Same behavior as `read_datadog_logs`. |

Grouping and non-count aggregations follow Datadog's facet rules: an attribute generally needs a
facet declared on it before it can appear in `metric` or `group_by`. Requires the
`logs_read_data` permission.

## Receiving logs

`datadog_serve` starts a native HTTP server inside DuckDB and returns its Datadog v2 intake URL.
It accepts JSON arrays, single-object JSON, and newline-delimited JSON at `/api/v2/logs` (with
the legacy `/v1/input` and `/api/v1/logs` paths as aliases). Gzip- and zstd-compressed request
bodies are supported. Accepted rows are committed synchronously to `main.datadog_logs`, using the same
18-column OTLP-shaped schema as `read_datadog_logs`:

```sql
LOAD datadog;

SELECT datadog_serve();
-- http://localhost:10518/api/v2/logs

SELECT time_unix_nano, service_name, severity_text, body
FROM datadog_logs;

SELECT datadog_stop();
```

The optional first argument is a `datadog:` listen URI. For an authenticated listener, the common
case is just the URI and API key:

```sql
SELECT datadog_serve('datadog:0.0.0.0:10518', 'local-agent-key');
```

The API-key shorthand permits a non-loopback bind because the resulting listener is authenticated.
For advanced configuration, pass an options struct instead:

```sql
SELECT datadog_serve(
    'datadog:127.0.0.1:10518',
    {
        schema_name: 'main',
        table_name: 'incoming_logs',
        api_key: 'local-agent-key',
        max_body_bytes: 5242880,
        http_threads: 8
    }
);
```

| Option | Default | Description |
| --- | --- | --- |
| `schema_name` | `main` | Target schema (`schema` is retained as an alias). |
| `table_name` | `datadog_logs` | Target table (`table` is retained as an alias). |
| `create_table` | `true` | Create the target table if needed; otherwise validate an existing table. |
| `api_key` | empty | When set, require the same value in `DD-API-KEY`. Empty accepts any key. |
| `secret` | unset | Read the API key from a named `datadog` secret; mutually exclusive with `api_key`. |
| `allow_other_hostname` | `false` | Permit non-loopback binds such as `0.0.0.0`. |
| `max_body_bytes` | 5 MiB | Maximum request-body size. |
| `http_threads` | auto | HTTP worker count. |

The default and advanced struct form are deliberately loopback-only. Set
`allow_other_hostname: true` in the struct to receive from another host or container, and use
`api_key` or `secret` whenever the listener is reachable by another machine. The listener remains
alive until `datadog_stop(uri)` is called or the owning DuckDB database closes. `datadog_stop`
returns `stopped` or `not found`.

To point a local Datadog Agent at the default listener, force its HTTP transport and disable TLS
only for this loopback hop:

```yaml
logs_enabled: true
logs_config:
  force_use_http: true
  logs_dd_url: "127.0.0.1:10518"
  logs_no_ssl: true
```

The Agent sends its configured Datadog API key in `DD-API-KEY`. Pass the same value through
`api_key`, or use `secret` to read it from an existing Datadog secret. Keep `logs_no_ssl` limited
to a local or otherwise trusted connection.

## Sending logs

`send_datadog_logs` pushes an OTLP-shaped log table into a Datadog account through the
[log intake API](https://docs.datadoghq.com/api/latest/logs/#send-logs). It takes a single `STRUCT`
argument — pass a whole row by naming the table — and returns `'ok'` for each accepted row:

```sql
LOAD datadog;

-- Uses the first in-scope `datadog` secret (only API_KEY is required to send; SITE is honored).
SELECT send_datadog_logs(l) FROM my_logs l;

-- Or name a specific secret:
SELECT send_datadog_logs(l, 'dd_prod') FROM my_logs l;
```

Any struct works, so you can copy logs straight from one Datadog window to another, from a Parquet
file, or from duckdb-otlp's `read_otlp_logs`:

```sql
SELECT send_datadog_logs(l)
FROM read_datadog_logs(query => 'service:web-store', "from" => 'now-15m', "to" => 'now') l;
```

Recognized columns are mapped loosely by name; **unknown columns are ignored** and missing columns
are simply omitted:

| Struct column (first match wins)                | Datadog intake attribute                          |
| ----------------------------------------------- | ------------------------------------------------- |
| `body` / `message`                              | `message`                                         |
| `service_name` / `service`                      | `service`                                         |
| `severity_text` / `severity` / `status`         | `status` (falls back to `severity_number`)        |
| `hostname` / `host`                             | `hostname` (falls back to `resource_attributes.host`) |
| `ddsource`                                       | `ddsource` (defaults to `duckdb`)                 |
| `time_unix_nano` / `timestamp`                  | `timestamp` (epoch ms; `observed_time_unix_nano` fallback) |
| `severity_number`                               | `status` (only when no severity/status column is present) |
| `ddtags`                                        | `ddtags` (falls back to `resource_attributes.ddtags`) |
| `trace_id`, `span_id`                           | custom attributes                                 |
| `resource_attributes` (JSON)                    | nested `resource_attributes`, mined for host/tags |
| `log_attributes` (JSON object)                  | merged as top-level custom attributes             |

Notes:
- Rows are batched into intake requests bounded by Datadog's limits (≤1000 logs and ≤5 MB per
  request); each batch is one HTTP POST. Cancellation (Ctrl+C) is honored between batches.
- The intake endpoint is **not idempotent**, so sends are retried only on failures that provably
  occurred before any data reached Datadog (connection setup) and on rate limits (`429`); a response
  lost after the batch may have been accepted is surfaced as an error rather than silently re-sent
  and double-indexed.
- Because a large input is sent as several independent requests, a failure partway through leaves
  earlier batches already delivered. The statement raises an error and returns no rows, but the
  already-sent logs remain in Datadog — re-running may duplicate them. For all-or-nothing semantics,
  send in a single batch (≤1000 rows, ≤5 MB).
- The timestamp column may be a `TIMESTAMP`/`TIMESTAMP_NS`/`DATE` (converted precisely) or a bare
  integer epoch — nanoseconds for `time_unix_nano`, milliseconds for a `timestamp` column.
- `log_attributes` keys never overwrite a reserved attribute already set from another column.
- Datadog drops logs whose `timestamp` is more than ~18h in the past; when replaying old data,
  either omit the timestamp column or expect those logs not to be indexed.
- A `NULL` log struct maps to a `NULL` result and is not sent.
- Request bodies are gzip-compressed, and each projection thread sends on its own pooled
  keep-alive connection, so large sends upload batches in parallel rather than serializing on
  one socket. Note that parallel batches make the partial-failure window above wider, not
  narrower — order of delivery across batches is not guaranteed.

### Sending somewhere other than Datadog

The optional `INTAKE_URL` secret field points `send_datadog_logs` at any v2-intake-compatible
listener instead of `https://http-intake.logs.<site>`: a local `datadog_serve`, an intake proxy,
or a mock in CI. It accepts a bare origin or a full intake URL (a trailing `/api/v2/logs` is
stripped), so the value `datadog_serve()` returns can be pasted verbatim:

```sql
SELECT datadog_serve('datadog:127.0.0.1:10518', {table_name: 'received'});
-- http://127.0.0.1:10518/api/v2/logs

CREATE SECRET dd_local (
    TYPE datadog,
    API_KEY 'local-key',
    APP_KEY 'unused-for-intake',
    INTAKE_URL 'http://127.0.0.1:10518/api/v2/logs'
);

SELECT send_datadog_logs(l, 'dd_local') FROM my_logs l;
SELECT count(*) FROM received;  -- the logs, round-tripped without leaving the process
```

This makes the whole send path testable offline; `test/sql/datadog_send_local.test` is exactly
this loop. Reads (`read_datadog_logs` and friends) are unaffected by `INTAKE_URL`.

## Traces

### Reading spans

`read_datadog_traces` reads indexed spans through the
[Spans API](https://docs.datadoghq.com/api/latest/spans/#search-spans) into the
[duckdb-otlp `read_otlp_traces`](https://smithclay.github.io/duckdb-otlp/reference/schemas/#traces-read_otlp_traces)
24-column schema, so Datadog spans line up with trace data from any other OTLP source:

```sql
-- Slow checkout spans in the last hour.
SELECT trace_id, name, service_name, duration_time_unix_nano / 1e6 AS ms
FROM read_datadog_traces(
    query  => 'service:checkout @duration:>1s',
    "from" => 'now-1h',
    "to"   => 'now'
)
ORDER BY duration_time_unix_nano DESC;
```

Parameters (`query`, `from`, `to`, `sort`, `page_size`, `max_rows`, `retries`, `timeout`,
`secret`) behave exactly like `read_datadog_logs`, including cursor pagination, retry/rate-limit
handling, and projection pushdown. `query` takes Datadog's span search syntax. Requires
`APP_KEY` with the `apm_read` permission.

Column notes for the Datadog mapping:
- `start_time_unix_nano` / `duration_time_unix_nano` come from the span's start/end timestamps
  (falling back to the custom `duration` measure, which Datadog records in nanoseconds).
- `trace_id`, `span_id`, `parent_span_id` are passed through as the API's string form.
- `name` is the Datadog operation name when present, else `resource_name`.
- `kind` and error status (`status_code` = 2, `status_status_message`) are derived from the
  `span.kind` and `error.*` tags when the span carries them.
- `span_attributes` is the span's custom attributes plus Datadog's reserved per-span fields
  (`resource_name`, `type`, `single_span`, `ingestion_reason`, `retained_by`);
  `resource_attributes` carries `host`, `env`, and `ddtags`. Columns Datadog has no equivalent
  for (`trace_state`, `scope_*`, `events_json`, `links_json`, `dropped_*`, `flags`) are `NULL`.

### Writing spans

`write_datadog_traces` sends OTLP-shaped span rows to Datadog. Like `send_datadog_logs` it takes
a `STRUCT` per row and returns `'ok'` per sent span, and it has two destinations:

- **`agent` (default)** — JSON to a Datadog Agent's
  [local trace API](https://docs.datadoghq.com/api/latest/tracing/) (`http://localhost:8126` by
  default, override with the `TRACE_AGENT_URL` secret field). A local Agent needs no credentials,
  so this works with zero secrets configured.
- **`direct`** — set `TRACE_INTAKE 'direct'` on the secret to skip the Agent entirely: spans are
  encoded as the protobuf `AgentPayload` and sent straight to Datadog's backend trace intake at
  `https://trace.agent.<site>/api/v0.2/traces`, authenticated by `API_KEY` alone. This is the
  same wire protocol the OpenTelemetry collector's Datadog exporter uses for agentless ingestion;
  it is stable in practice but not a documented public API, and Agent-side niceties (APM stats,
  obfuscation, local sampling) do not apply.

```sql
-- No secret needed for a local Agent:
SELECT write_datadog_traces(t) FROM my_spans t;

-- Agentless: straight to the Datadog backend.
CREATE SECRET dd_direct (
    TYPE datadog, API_KEY '<dd-api-key>', APP_KEY '<dd-app-key>',
    SITE 'datadoghq.com', TRACE_INTAKE 'direct'
);
SELECT write_datadog_traces(t, 'dd_direct') FROM my_spans t;

-- Round-trip spans from one account/window into another:
SELECT write_datadog_traces(t)
FROM read_datadog_traces(query => 'service:checkout', "from" => 'now-15m') t;
```

Recognized columns (first match wins): `trace_id` and `span_id` (required; OTLP hex strings or
unsigned integers), `parent_span_id`/`parent_id`, `name`/`operation_name`,
`resource`/`resource_name` (defaults to the name), `service_name`/`service`,
`start_time_unix_nano` (TIMESTAMP or integer epoch nanoseconds; required),
`duration_time_unix_nano`/`duration_ns`/`duration`, `kind` (OTLP integer → `span.kind` tag;
servers default to type `web`), `type`/`span_type`, `status_code` (2 → `error: 1`),
`status_status_message`/`status_message` (→ `error.message`), `trace_state`, `events_json`,
`links_json`, and JSON `span_attributes`/`resource_attributes` (strings → `meta`, numbers →
`metrics`).

Notes:
- A 128-bit hex `trace_id` is split Datadog-style: lower 64 bits become the wire trace id and the
  upper 64 bits ride in the `_dd.p.tid` tag, so 128-bit OTLP traces keep their identity.
- Rows that cannot form a valid span (missing/invalid `trace_id`, `span_id`, or start time)
  return `'skipped: <reason>'` instead of failing the whole send; a `NULL` struct returns `NULL`.
- Spans are grouped into traces per chunk and sent as one JSON `PUT /v0.3/traces` request, with
  the same conservative non-idempotent retry rules as `send_datadog_logs`.
- The optional `TRACE_AGENT_URL` secret field points the writer at a remote Agent or a mock in
  CI (a trailing `/v0.3/traces` is stripped). `DD-API-KEY` is attached only when the secret has
  an `API_KEY`, so a plain local Agent needs no secret at all.
- The Agent applies its own normalization and sampling; spans older than the Agent's acceptance
  window may be dropped.

## Building

Native dependencies are minimal: OpenSSL and zlib (via vcpkg). HTTP (cpp-httplib) and JSON (yyjson)
reuse the copies DuckDB already bundles.

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

For a completely local receiving-path test, `run_serve_agent.sh` starts `datadog_serve`, launches a
real Datadog Agent container with a temporary file-log source, and verifies that the Agent-forwarded
row lands in DuckDB. It needs Docker but no Datadog account or credentials:

```shell
make release
test/e2e/run_serve_agent.sh
```

Set `DD_AGENT_IMAGE`, `DUCKDB_BIN`, `DATADOG_SERVE_E2E_PORT`, or `POLL_TIMEOUT` to override the
script defaults. The temporary container, database, configuration, and logs are removed on exit.

`test/e2e/run_e2e.sh` exercises the full round-trip against a real Datadog account. It sends a
uniquely-tagged log via the log intake API and reads it back through `read_datadog_logs` (asserting
the mapping), then sends a second log **through the extension** with `send_datadog_logs` and polls
until it is searchable — validating the write path end to end. It shares credentials with the
[`pup` CLI](https://docs.datadoghq.com/cli/) (`DD_API_KEY` / `DD_APP_KEY` / `DD_SITE`) and, when
`pup` is installed, uses it for an auth-status check and an independent cross-check of the ingested log.

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
