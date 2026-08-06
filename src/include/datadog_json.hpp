#pragma once

#include "duckdb.hpp"

#include <utility>

namespace duckdb {

//! Conservative predicates translated from DuckDB WHERE clauses. Query terms are
//! Datadog log-search expressions; timestamp bounds are outward-rounded epoch
//! milliseconds so DuckDB can safely reapply the original predicates.
struct DatadogFilterPushdown {
	vector<string> query_terms;
	bool has_lower_bound_ms = false;
	int64_t lower_bound_ms = 0;
	bool has_upper_bound_ms = false;
	int64_t upper_bound_ms = 0;
};

struct DatadogResolvedSearch {
	string query;
	string from;
	string to;
	bool empty = false;
};

//! One currently triggered Datadog monitor group. Presence flags distinguish a missing/null API
//! field from a legitimate empty value so catalog scans can preserve SQL NULL semantics.
struct DatadogOpenAlertGroup {
	bool has_monitor_id = false;
	int64_t monitor_id = 0;
	bool has_monitor_name = false;
	string monitor_name;
	bool has_group = false;
	string group;
	bool has_group_tags = false;
	vector<string> group_tags;
	bool has_status = false;
	string status;
	bool has_last_triggered_ts = false;
	int64_t last_triggered_ts = 0;
	bool has_last_nodata_ts = false;
	int64_t last_nodata_ts = 0;
};

struct DatadogOpenAlertsPage {
	vector<DatadogOpenAlertGroup> groups;
	bool has_total_count = false;
	int64_t total_count = 0;
};

//! One directed caller -> callee edge returned by Datadog's APM service-dependencies endpoint.
struct DatadogServiceDependency {
	string source_service;
	string target_service;
};

struct DatadogServiceMapWindow {
	int64_t start_epoch_seconds = 0;
	int64_t end_epoch_seconds = 0;
};

//! Parse GET /api/v1/logs/config/indexes, validating every returned index name and
//! deduplicating exact names while preserving response order.
vector<string> ParseDatadogLogIndexes(const string &response_json);

//! Build a paginated monitor-group search path for Datadog's definition of triggered/open:
//! group status Alert, Warn, or No Data.
string BuildDatadogOpenAlertsPath(int64_t page, int64_t per_page);

//! Parse one response from /api/v1/monitor/groups/search.
DatadogOpenAlertsPage ParseDatadogOpenAlertsPage(const string &response_json);

//! Build the percent-encoded GET target for /api/v1/service_dependencies. Start and end are
//! always explicit so the rows' window columns exactly describe the requested graph.
string BuildDatadogServiceDependenciesPath(const string &environment, const string &primary_tag,
                                           int64_t start_epoch_seconds, int64_t end_epoch_seconds);

//! Parse the public-beta service-dependencies object into directed caller -> callee edges.
//! Unknown fields inside a service object are ignored for forward compatibility.
vector<DatadogServiceDependency> ParseDatadogServiceDependencies(const string &response_json);

//! Resolve epoch-second or relative (`now`, `-15m`, `now-1h`) endpoints against the supplied
//! current epoch second. Supplying `now` makes this deterministic and independently testable;
//! scans call it only when execution starts, never when a catalog is attached.
DatadogServiceMapWindow ResolveDatadogServiceMapWindow(const string &from, const string &to, int64_t now_epoch_seconds);

//! Combine the user's query with conservative query terms translated from SQL.
//! The original query is unchanged when there are no pushed terms.
string BuildDatadogSearchQuery(const string &query, const vector<string> &query_terms);

//! Resolve pushed timestamp bounds against an explicit epoch-millisecond request
//! window. Relative and ISO-8601 windows remain unchanged so their server-side
//! interpretation is preserved.
DatadogResolvedSearch ResolveDatadogSearch(const string &query, const string &from, const string &to,
                                           const DatadogFilterPushdown &pushdown);

//! Build the POST body used by the Datadog Logs Search API. The indexes field is
//! omitted when `indexes` is empty so read_datadog_logs retains its existing request shape.
string BuildDatadogLogsSearchBody(const string &query, const string &from, const string &to, const string &sort,
                                  int64_t limit, const string &cursor, const vector<string> &indexes = {});

//! One bucket returned by the Logs Aggregation API: the group-by key values as a JSON object
//! (`{}` for an ungrouped total) and the single requested compute value. `has_value` is false
//! when Datadog returns a null compute (e.g. a percentile over zero matching logs).
struct DatadogLogStatsBucket {
	string by_json = "{}";
	bool has_value = false;
	double value = 0;
};

//! Build the POST body for /api/v2/logs/analytics/aggregate: one compute (aggregation +
//! optional metric) and zero or more group-by facets, each capped at `group_limit` buckets.
//! The metric field is omitted when `metric` is empty (a plain count needs none).
string BuildDatadogLogsAggregateBody(const string &query, const string &from, const string &to,
                                     const string &aggregation, const string &metric, const vector<string> &group_by,
                                     int64_t group_limit);

//! Parse one aggregation response into buckets. A missing or empty bucket list is a valid
//! zero-match result, not an error. Throws IOException on malformed JSON.
vector<DatadogLogStatsBucket> ParseDatadogLogsAggregateResponse(const string &response_json);

//! Build the POST body for /api/v2/spans/events/search. The spans search API wraps its request
//! in a {"data": {"type": "search_request", "attributes": {...}}} envelope, unlike the flat body
//! the logs search API accepts; filter/page/sort semantics are otherwise identical.
string BuildDatadogSpansSearchBody(const string &query, const string &from, const string &to, const string &sort,
                                   int64_t limit, const string &cursor);

//! Parse an OTLP hex identifier (1-32 hex characters, e.g. a trace_id or span_id column) into the
//! Datadog trace agent's unsigned 64-bit representation. A 128-bit id is split: `low` receives the
//! lower 64 bits and `high_hex` the upper 64 bits as zero-stripped lowercase hex (the value Datadog
//! carries in the `_dd.p.tid` tag). Returns false for empty, non-hex, or over-long input.
bool ParseDatadogHexId(const string &text, uint64_t &low, string &high_hex);

//! One span for the Datadog trace agent JSON API (PUT <agent>/v0.3/traces). Ids are the agent's
//! unsigned 64-bit form; empty string fields are omitted from the payload.
struct DatadogAgentSpan {
	uint64_t trace_id = 0; //! lower 64 bits of the trace id
	uint64_t span_id = 0;
	bool has_parent_id = false;
	uint64_t parent_id = 0;
	string trace_id_high_hex; //! non-empty for a 128-bit trace id -> meta `_dd.p.tid`
	string service;
	string name;             //! Datadog operation name
	string resource;         //! Datadog resource; callers usually default it to `name`
	string type;             //! Datadog span type (web/db/cache/custom); "" omits the field
	int64_t start_ns = 0;    //! span start, nanoseconds since epoch
	int64_t duration_ns = 0; //! span duration in nanoseconds
	bool error = false;      //! true -> error: 1
	//! Reserved meta entries (span.kind, error.message, ...) set by the caller. These are written
	//! first, so attribute JSON below can never overwrite them.
	vector<std::pair<string, string>> meta;
	string span_attributes_json;     //! JSON object; strings -> meta, numbers -> metrics
	string resource_attributes_json; //! JSON object; merged after span attributes, never overwrites
};

//! Resolve a span's final meta (string) and metrics (numeric) entries, in order: `_dd.p.tid` for a
//! 128-bit trace id, the caller's reserved meta entries, then span_attributes_json, then
//! resource_attributes_json. The first writer of a key wins across both lists, so reserved entries
//! can never be overwritten by attribute JSON. Booleans become "true"/"false" meta strings, JSON
//! objects/arrays are serialized to compact JSON meta strings, and nulls are dropped. Both the
//! JSON trace-agent body and the protobuf direct-intake payload are built from this one resolver,
//! so the two write paths cannot drift apart.
void ResolveDatadogAgentSpanAttributes(const DatadogAgentSpan &span, vector<std::pair<string, string>> &meta,
                                       vector<std::pair<string, double>> &metrics);

//! Build the trace agent body from `count` spans starting at `spans`: a JSON array of traces
//! (spans grouped by trace id in first-seen order), each an array of span objects. `trace_count`
//! receives the number of traces for the X-Datadog-Trace-Count request header.
string BuildDatadogAgentTracesBody(const DatadogAgentSpan *spans, idx_t count, idx_t &trace_count);
inline string BuildDatadogAgentTracesBody(const vector<DatadogAgentSpan> &spans, idx_t &trace_count) {
	return BuildDatadogAgentTracesBody(spans.data(), spans.size(), trace_count);
}

//! One OTLP-shaped log to send through the Datadog log intake API. Every string field is optional;
//! an empty string means "absent" and the corresponding Datadog attribute is omitted. `host` and
//! `ddtags` fall back to values discovered inside `resource_attributes_json` when left empty.
struct DatadogIntakeLog {
	string message;                  //! OTLP body -> reserved `message`
	string service;                  //! OTLP service_name -> reserved `service`
	string status;                   //! OTLP severity_text -> reserved `status`
	string hostname;                 //! -> reserved `hostname` (falls back to resource host)
	string ddsource;                 //! -> reserved `ddsource`
	string ddtags;                   //! comma-separated -> reserved `ddtags` (falls back to resource ddtags)
	string trace_id;                 //! -> custom attribute `trace_id`
	string span_id;                  //! -> custom attribute `span_id`
	bool has_timestamp_ms = false;   //! whether `timestamp_ms` is set
	int64_t timestamp_ms = 0;        //! epoch milliseconds -> reserved `timestamp`
	string log_attributes_json;      //! JSON object; keys merged as top-level custom attributes
	string resource_attributes_json; //! JSON object; nested under `resource_attributes`, mined for host/ddtags
};

//! Build the JSON array body for `POST https://http-intake.logs.<site>/api/v2/logs` from `count`
//! OTLP-shaped logs starting at `logs`. Reserved attributes are set from the matching fields;
//! `hostname`/`ddtags` fall back to `resource_attributes`; `log_attributes` keys become top-level
//! custom attributes without ever overwriting an already-set reserved key. Malformed attribute JSON
//! is skipped rather than fatal. The pointer form lets callers serialize a sub-range (one intake
//! batch) without copying the logs out of their backing vector.
string BuildDatadogIntakeBody(const DatadogIntakeLog *logs, idx_t count);
inline string BuildDatadogIntakeBody(const vector<DatadogIntakeLog> &logs) {
	return BuildDatadogIntakeBody(logs.data(), logs.size());
}

//! Approximate serialized byte size of one intake log, used to keep an intake batch under the
//! Datadog 5 MB request limit. Deliberately an over-estimate (counts raw field bytes plus a fixed
//! per-log envelope) so batching stays safely below the hard limit.
idx_t EstimateDatadogIntakeLogBytes(const DatadogIntakeLog &log);

//! Return the next outgoing page limit. A positive max_rows reduces the request to the
//! unreserved portion of the bounded relation's row budget.
int64_t GetDatadogLogsPageLimit(int64_t page_size, int64_t max_rows, idx_t row_budget_used);

//! True once a positive max_rows cap has been emitted. Scans use this to stop before
//! requesting another cursor page.
bool DatadogLogsMaxRowsReached(int64_t max_rows, idx_t total_emitted);

} // namespace duckdb
