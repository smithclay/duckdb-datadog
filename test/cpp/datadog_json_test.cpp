#include "datadog_json.hpp"

#include <iostream>
#include <stdexcept>

using namespace duckdb;

static void Require(bool condition, const char *message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

int main() {
	try {
		auto indexes =
		    ParseDatadogLogIndexes(R"({"indexes":[{"name":"main"},{"name":"security-events"},{"name":"main"}]})");
		Require(indexes.size() == 2, "index discovery should deduplicate names");
		Require(indexes[0] == "main" && indexes[1] == "security-events",
		        "index discovery should preserve response order and exact names");
		Require(ParseDatadogLogIndexes(R"({"indexes":[]})").empty(), "an empty discovered index list should be valid");

		bool malformed_rejected = false;
		try {
			ParseDatadogLogIndexes(R"({"indexes":[{"name":null}]})");
		} catch (const IOException &) {
			malformed_rejected = true;
		}
		Require(malformed_rejected, "malformed discovered index names should be rejected");

		auto bound = BuildDatadogLogsSearchBody("*", "now-15m", "now", 1000, "", {"main"});
		Require(bound.find("\"indexes\":[\"main\"]") != string::npos,
		        "catalog search body should contain exactly its bound index");
		Require(bound.find("security-events") == string::npos, "catalog search body should not contain another index");

		auto all = BuildDatadogLogsSearchBody("*", "now-15m", "now", 1000, "");
		Require(all.find("\"indexes\"") == string::npos,
		        "table-function search body should omit indexes when none are bound");

		DatadogFilterPushdown pushed;
		pushed.query_terms = {"service:edge", "status:error"};
		Require(BuildDatadogSearchQuery("*", pushed.query_terms) == "service:edge AND status:error",
		        "default query should be replaced by pushed Datadog terms");
		Require(BuildDatadogSearchQuery("env:prod", pushed.query_terms) ==
		            "(env:prod) AND service:edge AND status:error",
		        "custom query should be preserved when pushed terms are appended");

		pushed.has_lower_bound_ms = true;
		pushed.lower_bound_ms = 1500000;
		pushed.has_upper_bound_ms = true;
		pushed.upper_bound_ms = 1750000;
		auto resolved = ResolveDatadogSearch("*", "now-15m", "now", pushed);
		Require(resolved.query == "service:edge AND status:error", "resolved search should contain pushed terms");
		Require(resolved.from == "now-15m" && resolved.to == "now",
		        "timestamp predicates must preserve server-relative request bounds");
		Require(!resolved.empty, "relative request bounds must not be declared empty using the client clock");
		auto pushed_body = BuildDatadogLogsSearchBody(resolved.query, resolved.from, resolved.to, 1000, "", {"main"});
		Require(pushed_body.find("\"query\":\"service:edge AND status:error\"") != string::npos,
		        "search request should contain translated service and status predicates");
		Require(pushed_body.find("\"from\":\"now-15m\"") != string::npos &&
		            pushed_body.find("\"to\":\"now\"") != string::npos,
		        "search request should retain relative timestamp bounds");

		resolved = ResolveDatadogSearch("*", "1100000", "2000000", pushed);
		Require(resolved.from == "1500000" && resolved.to == "1750000",
		        "timestamp predicates should tighten an absolute epoch-millisecond window");
		Require(!resolved.empty, "overlapping absolute timestamp predicates should not make the search empty");

		pushed.lower_bound_ms = 0;
		pushed.upper_bound_ms = 3000000;
		resolved = ResolveDatadogSearch("*", "1100000", "2000000", pushed);
		Require(resolved.from == "1100000" && resolved.to == "2000000",
		        "pushed timestamps must not widen an absolute request window");

		resolved = ResolveDatadogSearch("*", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", pushed);
		Require(resolved.from == "2026-01-01T00:00:00Z" && resolved.to == "2026-01-01T01:00:00Z",
		        "ISO-8601 time windows should retain server-side interpretation");

		pushed.lower_bound_ms = 1800000;
		pushed.upper_bound_ms = 1700000;
		Require(ResolveDatadogSearch("*", "1100000", "2000000", pushed).empty,
		        "disjoint pushed timestamps in an absolute window should avoid a network request");
	} catch (const std::exception &error) {
		std::cerr << "datadog_json_test failed: " << error.what() << std::endl;
		return 1;
	}
	return 0;
}
