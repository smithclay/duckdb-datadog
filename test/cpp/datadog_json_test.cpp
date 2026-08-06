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

		Require(BuildDatadogOpenAlertsPath(3, 100) ==
		            "/api/v1/monitor/groups/search?query="
		            "group_status%3A%28Alert%20OR%20Warn%20OR%20%22No%20Data%22%29&page=3&per_page=100",
		        "open-alert search should use the triggered monitor-group states and requested page");
		auto alerts = ParseDatadogOpenAlertsPage(R"({
			"groups":[
				{"group":"host:web01,env:prod","group_tags":["host:web01","env:prod"],
				 "last_nodata_ts":0,"last_triggered_ts":1525702966,"monitor_id":2738266,
				 "monitor_name":"Disk usage is high","status":"Alert"},
				{"group":"*","group_tags":[],"last_triggered_ts":null,"monitor_id":1576648,
				 "monitor_name":"Service missing","status":"No Data"}
			],
			"metadata":{"page":0,"page_count":2,"per_page":100,"total_count":2}
		})");
		Require(alerts.groups.size() == 2, "open-alert response should return every monitor group");
		Require(alerts.has_total_count && alerts.total_count == 2,
		        "open-alert response should retain pagination total_count");
		Require(alerts.groups[0].has_monitor_id && alerts.groups[0].monitor_id == 2738266,
		        "open-alert monitor id should be parsed");
		Require(alerts.groups[0].has_group_tags && alerts.groups[0].group_tags.size() == 2,
		        "open-alert group tags should be parsed as a list");
		Require(alerts.groups[0].has_last_nodata_ts && alerts.groups[0].last_nodata_ts == 0,
		        "zero-valued Datadog timestamp sentinels should remain distinguishable from missing fields");
		Require(!alerts.groups[1].has_last_triggered_ts,
		        "null open-alert timestamps should preserve SQL NULL semantics");

		malformed_rejected = false;
		try {
			ParseDatadogOpenAlertsPage(R"({"groups":[{"group_tags":[null]}]})");
		} catch (const IOException &) {
			malformed_rejected = true;
		}
		Require(malformed_rejected, "malformed open-alert group tags should be rejected");

		Require(BuildDatadogServiceDependenciesPath("prod/us east", "region:us-west-2", 1700000000, 1700003600) ==
		            "/api/v1/service_dependencies?env=prod%2Fus%20east&primary_tag=region%3Aus-west-2&"
		            "start=1700000000&end=1700003600",
		        "service-dependency query values should be RFC 3986 percent-encoded");
		Require(BuildDatadogServiceDependenciesPath("prod", "", 1, 2) ==
		            "/api/v1/service_dependencies?env=prod&start=1&end=2",
		        "an absent primary tag should be omitted from the service-dependency request");

		auto dependencies = ParseDatadogServiceDependencies(R"({
			"web":{"calls":["checkout","checkout","database"],"public_beta_future_field":true},
			"checkout":{"calls":["database"]},
			"database":{"calls":[]}
		})");
		Require(dependencies.size() == 3, "duplicate source-target dependency edges should be removed");
		Require(dependencies[0].source_service == "web" && dependencies[0].target_service == "checkout",
		        "dependency parsing should preserve source and target names");
		Require(dependencies[1].source_service == "web" && dependencies[1].target_service == "database" &&
		            dependencies[2].source_service == "checkout" && dependencies[2].target_service == "database",
		        "dependency parsing should preserve first-seen edge order");

		malformed_rejected = false;
		try {
			ParseDatadogServiceDependencies(R"({"web":{"calls":[null]}})");
		} catch (const IOException &) {
			malformed_rejected = true;
		}
		Require(malformed_rejected, "malformed service-dependency calls should be rejected");

		auto service_window = ResolveDatadogServiceMapWindow("-1h", "now", 1700003600);
		Require(service_window.start_epoch_seconds == 1700000000 && service_window.end_epoch_seconds == 1700003600,
		        "relative service-map windows should resolve against the supplied scan clock");
		service_window = ResolveDatadogServiceMapWindow(" NOW-15M ", " NOW ", 1700003600);
		Require(service_window.start_epoch_seconds == 1700002700 && service_window.end_epoch_seconds == 1700003600,
		        "relative service-map windows should tolerate whitespace and case");
		service_window = ResolveDatadogServiceMapWindow("1700000000", "1700003600", 123);
		Require(service_window.start_epoch_seconds == 1700000000 && service_window.end_epoch_seconds == 1700003600,
		        "absolute service-map epoch seconds should not depend on the scan clock");
		malformed_rejected = false;
		try {
			ResolveDatadogServiceMapWindow("now", "-1h", 1700003600);
		} catch (const InvalidInputException &) {
			malformed_rejected = true;
		}
		Require(malformed_rejected, "a reversed service-map window should be rejected");
		malformed_rejected = false;
		try {
			ResolveDatadogServiceMapWindow("0", "9223372037", 1700003600);
		} catch (const InvalidInputException &) {
			malformed_rejected = true;
		}
		Require(malformed_rejected, "service-map windows outside TIMESTAMP_NS range should be rejected");

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

		// The pointer/count overload serializes a sub-range without copying it out of the vector.
		vector<DatadogIntakeLog> many = {basic, sparse, with_resource};
		auto sub_body = BuildDatadogIntakeBody(many.data() + 1, 2); // sparse + with_resource only
		Require(sub_body.find("web-store") == string::npos, "sub-range must exclude logs before the offset");
		Require(sub_body.find("host-1") != string::npos, "sub-range must include logs within the range");
		Require(BuildDatadogIntakeBody(many.data(), 0) == "[]", "a zero-length range is an empty array");

		// The byte estimate grows with payload size and carries a non-zero fixed envelope.
		DatadogIntakeLog small_est;
		small_est.message = "x";
		DatadogIntakeLog big_est;
		big_est.message = string(10000, 'x');
		Require(EstimateDatadogIntakeLogBytes(big_est) > EstimateDatadogIntakeLogBytes(small_est) + 9000,
		        "byte estimate should scale with field sizes");
		Require(EstimateDatadogIntakeLogBytes(DatadogIntakeLog()) > 0,
		        "byte estimate should include a fixed per-log envelope");

		// Aggregation request bodies carry the filter, exactly one compute, and per-facet group-bys.
		auto agg_body = BuildDatadogLogsAggregateBody("service:web @duration_ns:>=100", "now-1h", "now", "cardinality",
		                                              "@trace_id", {"service", "status"}, 25);
		Require(agg_body ==
		            "{\"filter\":{\"query\":\"service:web @duration_ns:>=100\",\"from\":\"now-1h\",\"to\":\"now\"},"
		            "\"compute\":[{\"aggregation\":\"cardinality\",\"metric\":\"@trace_id\"}],"
		            "\"group_by\":[{\"facet\":\"service\",\"limit\":25},{\"facet\":\"status\",\"limit\":25}]}",
		        "aggregate body should carry filter, one compute, and per-facet group-bys");
		auto count_body = BuildDatadogLogsAggregateBody("*", "0", "10", "count", "", {}, 10);
		Require(count_body.find("metric") == string::npos, "a plain count carries no metric field");
		Require(count_body.find("group_by") == string::npos, "no group_by field without facets");

		auto agg_buckets = ParseDatadogLogsAggregateResponse(
		    R"({"data":{"buckets":[{"by":{"service":"payment"},"computes":{"c0":42}},)"
		    R"({"by":{"service":"web"},"computes":{"c0":7.5}}]}})");
		Require(agg_buckets.size() == 2, "every aggregation bucket should be returned");
		Require(agg_buckets[0].by_json == "{\"service\":\"payment\"}", "group keys should round-trip as JSON");
		Require(agg_buckets[0].has_value && agg_buckets[0].value == 42, "integer computes should be parsed");
		Require(agg_buckets[1].has_value && agg_buckets[1].value == 7.5, "fractional computes should be parsed");

		auto agg_total = ParseDatadogLogsAggregateResponse(R"({"data":{"buckets":[{"computes":{"c0":0}}]}})");
		Require(agg_total.size() == 1 && agg_total[0].by_json == "{}" && agg_total[0].has_value &&
		            agg_total[0].value == 0,
		        "an ungrouped total should carry an empty group object");

		auto agg_null =
		    ParseDatadogLogsAggregateResponse(R"({"data":{"buckets":[{"by":{},"computes":{"c0":null}}]}})");
		Require(agg_null.size() == 1 && !agg_null[0].has_value,
		        "a null compute (e.g. a percentile over zero logs) should have no value");

		Require(ParseDatadogLogsAggregateResponse(R"({"data":{}})").empty(),
		        "a zero-match response without buckets is a valid empty result");
		bool malformed_agg_rejected = false;
		try {
			ParseDatadogLogsAggregateResponse("not json");
		} catch (const IOException &) {
			malformed_agg_rejected = true;
		}
		Require(malformed_agg_rejected, "malformed aggregation responses should be rejected");
	} catch (const std::exception &error) {
		std::cerr << "datadog_json_test failed: " << error.what() << std::endl;
		return 1;
	}
	return 0;
}
