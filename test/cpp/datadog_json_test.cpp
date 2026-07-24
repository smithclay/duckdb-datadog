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

		auto bound = BuildDatadogLogsSearchBody("*", "now-15m", "now", "timestamp", 1000, "", {"main"});
		Require(bound.find("\"indexes\":[\"main\"]") != string::npos,
		        "catalog search body should contain exactly its bound index");
		Require(bound.find("security-events") == string::npos, "catalog search body should not contain another index");
		Require(bound.find("\"sort\":\"timestamp\"") != string::npos,
		        "default search body should sort by ascending timestamp");

		auto all = BuildDatadogLogsSearchBody("*", "now-15m", "now", "timestamp", 1000, "");
		Require(all.find("\"indexes\"") == string::npos,
		        "table-function search body should omit indexes when none are bound");

		auto latest = BuildDatadogLogsSearchBody("*", "now-15m", "now", "-timestamp", 100, "", {"main"});
		Require(
		    latest ==
		        R"({"filter":{"query":"*","from":"now-15m","to":"now","indexes":["main"]},"sort":"-timestamp","page":{"limit":100}})",
		    "bounded latest-log request should use descending sort, limit 100, and no cursor");
		Require(GetDatadogLogsPageLimit(1000, 100, 0) == 100,
		        "max_rows smaller than page_size should reduce the first request limit");
		Require(GetDatadogLogsPageLimit(100, 150, 100) == 50,
		        "later requests should be limited to the remaining max_rows budget");
		Require(GetDatadogLogsPageLimit(1000, 0, 5000) == 1000,
		        "unlimited scans should retain the configured page size");
		Require(DatadogLogsMaxRowsReached(100, 100), "a capped scan should stop before requesting another cursor page");
		Require(GetDatadogLogsPageLimit(100, 100, 100) == 0,
		        "the first request should reserve the full cap and prevent another cursor request");

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
		auto pushed_body =
		    BuildDatadogLogsSearchBody(resolved.query, resolved.from, resolved.to, "timestamp", 1000, "", {"main"});
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

		// --- Log intake body (send_datadog_logs) ---------------------------------------------
		Require(BuildDatadogIntakeBody({}) == "[]", "an empty send should produce an empty JSON array");

		DatadogIntakeLog basic;
		basic.message = "hello world";
		basic.service = "web-store";
		basic.status = "error";
		basic.ddsource = "duckdb";
		basic.has_timestamp_ms = true;
		basic.timestamp_ms = 1750000000000;
		auto basic_body = BuildDatadogIntakeBody({basic});
		Require(basic_body.front() == '[' && basic_body.back() == ']', "intake body must be a JSON array");
		Require(basic_body.find("\"message\":\"hello world\"") != string::npos, "message maps from OTLP body");
		Require(basic_body.find("\"service\":\"web-store\"") != string::npos, "service maps from service_name");
		Require(basic_body.find("\"status\":\"error\"") != string::npos, "status maps from severity_text");
		Require(basic_body.find("\"timestamp\":1750000000000") != string::npos, "timestamp is emitted as epoch ms");

		// Absent fields are omitted rather than emitted as null/empty.
		DatadogIntakeLog sparse;
		sparse.message = "only a message";
		auto sparse_body = BuildDatadogIntakeBody({sparse});
		Require(sparse_body.find("\"service\"") == string::npos, "absent service must be omitted");
		Require(sparse_body.find("\"timestamp\"") == string::npos, "absent timestamp must be omitted");

		// resource_attributes supplies host and ddtags when not set directly.
		DatadogIntakeLog with_resource;
		with_resource.message = "m";
		with_resource.resource_attributes_json = R"({"host":"host-1","ddtags":["env:prod","team:core"]})";
		auto resource_body = BuildDatadogIntakeBody({with_resource});
		Require(resource_body.find("\"hostname\":\"host-1\"") != string::npos,
		        "hostname should fall back to resource_attributes.host");
		Require(resource_body.find("\"ddtags\":\"env:prod,team:core\"") != string::npos,
		        "ddtags should fall back to comma-joined resource_attributes.ddtags");
		Require(resource_body.find("\"resource_attributes\":{") != string::npos,
		        "the full resource block should be preserved as a nested object");

		// An explicit hostname wins over the resource host.
		DatadogIntakeLog host_override;
		host_override.hostname = "explicit-host";
		host_override.resource_attributes_json = R"({"host":"resource-host"})";
		auto host_body = BuildDatadogIntakeBody({host_override});
		Require(host_body.find("\"hostname\":\"explicit-host\"") != string::npos,
		        "an explicit hostname must not be overwritten by the resource host");
		Require(host_body.find("resource-host") != string::npos,
		        "the resource host is still preserved in the nested block");

		// log_attributes keys become top-level custom attributes but never clobber reserved keys.
		DatadogIntakeLog with_attrs;
		with_attrs.message = "reserved-message";
		with_attrs.log_attributes_json = R"({"trace_id":"abc","user_id":"42","message":"should-not-win"})";
		auto attrs_body = BuildDatadogIntakeBody({with_attrs});
		Require(attrs_body.find("\"user_id\":\"42\"") != string::npos, "log_attributes keys are merged top-level");
		Require(attrs_body.find("\"message\":\"reserved-message\"") != string::npos,
		        "reserved keys must win over colliding log_attributes keys");
		Require(attrs_body.find("should-not-win") == string::npos,
		        "a colliding log_attributes value must not overwrite a reserved attribute");

		// Malformed attribute JSON is skipped, not fatal.
		DatadogIntakeLog malformed;
		malformed.message = "m";
		malformed.log_attributes_json = "{not valid json";
		malformed.resource_attributes_json = "]]";
		auto malformed_body = BuildDatadogIntakeBody({malformed});
		Require(malformed_body.find("\"message\":\"m\"") != string::npos,
		        "malformed attribute JSON should be ignored without failing the build");

		// Multiple logs produce a two-element array.
		auto multi_body = BuildDatadogIntakeBody({basic, sparse});
		size_t first = multi_body.find("\"message\"");
		Require(first != string::npos && multi_body.find("\"message\"", first + 1) != string::npos,
		        "each input log should appear as its own array element");
	} catch (const std::exception &error) {
		std::cerr << "datadog_json_test failed: " << error.what() << std::endl;
		return 1;
	}
	return 0;
}
