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
string BuildDatadogLogsSearchBody(const string &query, const string &from, const string &to, int64_t limit,
                                  const string &cursor, const vector<string> &indexes = {});

} // namespace duckdb
