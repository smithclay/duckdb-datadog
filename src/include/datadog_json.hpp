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

//! Parse GET /api/v1/logs/config/indexes, validating every returned index name and
//! deduplicating exact names while preserving response order.
vector<string> ParseDatadogLogIndexes(const string &response_json);

//! Build a paginated monitor-group search path for Datadog's definition of triggered/open:
//! group status Alert, Warn, or No Data.
string BuildDatadogOpenAlertsPath(int64_t page, int64_t per_page);

//! Parse one response from /api/v1/monitor/groups/search.
DatadogOpenAlertsPage ParseDatadogOpenAlertsPage(const string &response_json);

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

//! Return the next outgoing page limit. A positive max_rows reduces the request to the
//! unreserved portion of the bounded relation's row budget.
int64_t GetDatadogLogsPageLimit(int64_t page_size, int64_t max_rows, idx_t row_budget_used);

//! True once a positive max_rows cap has been emitted. Scans use this to stop before
//! requesting another cursor page.
bool DatadogLogsMaxRowsReached(int64_t max_rows, idx_t total_emitted);

} // namespace duckdb
