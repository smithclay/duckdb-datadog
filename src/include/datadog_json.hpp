#pragma once

#include "duckdb.hpp"

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

//! Parse GET /api/v1/logs/config/indexes, validating every returned index name and
//! deduplicating exact names while preserving response order.
vector<string> ParseDatadogLogIndexes(const string &response_json);

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

//! Build the JSON array body for `POST https://http-intake.logs.<site>/api/v2/logs` from OTLP-shaped
//! logs. Reserved attributes are set from the matching fields; `hostname`/`ddtags` fall back to
//! `resource_attributes`; `log_attributes` keys become top-level custom attributes without ever
//! overwriting an already-set reserved key. Malformed attribute JSON is skipped rather than fatal.
string BuildDatadogIntakeBody(const vector<DatadogIntakeLog> &logs);

//! Return the next outgoing page limit. A positive max_rows reduces the request to the
//! unreserved portion of the bounded relation's row budget.
int64_t GetDatadogLogsPageLimit(int64_t page_size, int64_t max_rows, idx_t row_budget_used);

//! True once a positive max_rows cap has been emitted. Scans use this to stop before
//! requesting another cursor page.
bool DatadogLogsMaxRowsReached(int64_t max_rows, idx_t total_emitted);

} // namespace duckdb
