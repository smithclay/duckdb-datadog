#include "datadog_json.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "yyjson.hpp"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <limits>
#include <memory>
#include <set>
#include <unordered_set>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace {

struct YyjsonDocDeleter {
	void operator()(yyjson_doc *doc) const {
		yyjson_doc_free(doc);
	}
};

struct YyjsonMutDocDeleter {
	void operator()(yyjson_mut_doc *doc) const {
		yyjson_mut_doc_free(doc);
	}
};

struct YyjsonFreeDeleter {
	void operator()(char *ptr) const {
		free(ptr);
	}
};

using YyjsonDocPtr = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>;
using YyjsonMutDocPtr = std::unique_ptr<yyjson_mut_doc, YyjsonMutDocDeleter>;
using YyjsonStrPtr = std::unique_ptr<char, YyjsonFreeDeleter>;

} // namespace

string BuildDatadogOpenAlertsPath(int64_t page, int64_t per_page) {
	// Datadog's Triggered Monitors page defines an open alert as a monitor group in Alert, Warn,
	// or No Data. Keep the fixed query percent-encoded because cpp-httplib expects an HTTP target,
	// while DuckDB-WASM's HTTPUtil expects the same value as part of a full URL.
	return "/api/v1/monitor/groups/search?query="
	       "group_status%3A%28Alert%20OR%20Warn%20OR%20%22No%20Data%22%29&page=" +
	       std::to_string(page) + "&per_page=" + std::to_string(per_page);
}

static string PercentEncodeQueryValue(const string &value) {
	static constexpr char HEX[] = "0123456789ABCDEF";
	string result;
	result.reserve(value.size());
	for (auto character : value) {
		auto byte = static_cast<unsigned char>(character);
		if ((byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') || (byte >= '0' && byte <= '9') ||
		    byte == '-' || byte == '_' || byte == '.' || byte == '~') {
			result.push_back(static_cast<char>(byte));
		} else {
			result.push_back('%');
			result.push_back(HEX[byte >> 4]);
			result.push_back(HEX[byte & 0x0F]);
		}
	}
	return result;
}

string BuildDatadogServiceDependenciesPath(const string &environment, const string &primary_tag,
                                           int64_t start_epoch_seconds, int64_t end_epoch_seconds) {
	string result = "/api/v1/service_dependencies?env=" + PercentEncodeQueryValue(environment);
	if (!primary_tag.empty()) {
		result += "&primary_tag=" + PercentEncodeQueryValue(primary_tag);
	}
	result += "&start=" + std::to_string(start_epoch_seconds);
	result += "&end=" + std::to_string(end_epoch_seconds);
	return result;
}

vector<DatadogServiceDependency> ParseDatadogServiceDependencies(const string &response_json) {
	YyjsonDocPtr doc(yyjson_read(response_json.c_str(), response_json.size(), 0));
	if (!doc) {
		throw IOException("Datadog service dependencies returned a response that is not valid JSON");
	}
	auto root = yyjson_doc_get_root(doc.get());
	if (!root || !yyjson_is_obj(root)) {
		throw IOException("Datadog service dependencies returned malformed data: expected a top-level object");
	}

	vector<DatadogServiceDependency> result;
	std::set<std::pair<string, string>> seen_edges;
	size_t service_index, service_max;
	yyjson_val *service_key;
	yyjson_val *service_value;
	yyjson_obj_foreach(root, service_index, service_max, service_key, service_value) {
		const char *source = yyjson_get_str(service_key);
		if (!source || !service_value || !yyjson_is_obj(service_value)) {
			throw IOException(
			    "Datadog service dependencies returned malformed data: every service value must be an object");
		}
		auto calls = yyjson_obj_get(service_value, "calls");
		if (!calls || !yyjson_is_arr(calls)) {
			throw IOException(
			    "Datadog service dependencies returned malformed data: service '%s'.calls must be an array", source);
		}
		size_t call_index, call_max;
		yyjson_val *call;
		yyjson_arr_foreach(calls, call_index, call_max, call) {
			if (!yyjson_is_str(call)) {
				throw IOException(
				    "Datadog service dependencies returned malformed data: service '%s'.calls[%d] must be a string",
				    source, call_index);
			}
			DatadogServiceDependency dependency {source, yyjson_get_str(call)};
			if (seen_edges.emplace(dependency.source_service, dependency.target_service).second) {
				result.push_back(std::move(dependency));
			}
		}
	}
	return result;
}

static int64_t ParseRelativeDurationSeconds(const string &value, const string &parameter_name) {
	if (value.size() < 3 || value[0] != '-') {
		throw InvalidInputException(
		    "Datadog service map %s must be epoch seconds, 'now', or a past relative value such as '-1h' or 'now-1h'",
		    parameter_name);
	}
	auto unit = value.back();
	int64_t multiplier;
	switch (unit) {
	case 's':
		multiplier = 1;
		break;
	case 'm':
		multiplier = 60;
		break;
	case 'h':
		multiplier = 60 * 60;
		break;
	case 'd':
		multiplier = 24 * 60 * 60;
		break;
	case 'w':
		multiplier = 7 * 24 * 60 * 60;
		break;
	default:
		throw InvalidInputException("Datadog service map %s has an unsupported relative unit; use s, m, h, d, or w",
		                            parameter_name);
	}
	int64_t magnitude = 0;
	for (idx_t index = 1; index + 1 < value.size(); index++) {
		auto character = value[index];
		if (!std::isdigit(static_cast<unsigned char>(character))) {
			throw InvalidInputException("Datadog service map %s has an invalid relative value '%s'", parameter_name,
			                            value);
		}
		auto digit = static_cast<int64_t>(character - '0');
		if (magnitude > (std::numeric_limits<int64_t>::max() - digit) / 10) {
			throw InvalidInputException("Datadog service map %s relative value is too large", parameter_name);
		}
		magnitude = magnitude * 10 + digit;
	}
	if (magnitude == 0 || magnitude > std::numeric_limits<int64_t>::max() / multiplier) {
		throw InvalidInputException("Datadog service map %s relative value must be positive and fit in seconds",
		                            parameter_name);
	}
	return magnitude * multiplier;
}

static int64_t ResolveDatadogServiceMapEndpoint(const string &input, const string &parameter_name,
                                                int64_t now_epoch_seconds) {
	string normalized = input;
	StringUtil::Trim(normalized);
	normalized = StringUtil::Lower(normalized);
	if (normalized == "now") {
		return now_epoch_seconds;
	}
	string relative = normalized;
	if (relative.rfind("now-", 0) == 0) {
		relative = relative.substr(3);
	}
	if (!relative.empty() && relative[0] == '-') {
		auto offset = ParseRelativeDurationSeconds(relative, parameter_name);
		if (now_epoch_seconds < std::numeric_limits<int64_t>::min() + offset) {
			throw InvalidInputException("Datadog service map %s relative value is out of range", parameter_name);
		}
		return now_epoch_seconds - offset;
	}
	if (normalized.empty()) {
		throw InvalidInputException("Datadog service map %s must not be empty", parameter_name);
	}
	idx_t offset = normalized[0] == '-' ? 1 : 0;
	if (offset == normalized.size()) {
		throw InvalidInputException("Datadog service map %s has invalid epoch seconds '%s'", parameter_name,
		                            normalized);
	}
	for (idx_t index = offset; index < normalized.size(); index++) {
		if (!std::isdigit(static_cast<unsigned char>(normalized[index]))) {
			throw InvalidInputException("Datadog service map %s must be epoch seconds, 'now', or a past relative value "
			                            "such as '-1h' or 'now-1h'",
			                            parameter_name);
		}
	}
	try {
		return std::stoll(normalized);
	} catch (const std::exception &) {
		throw InvalidInputException("Datadog service map %s epoch seconds are out of range", parameter_name);
	}
}

DatadogServiceMapWindow ResolveDatadogServiceMapWindow(const string &from, const string &to,
                                                       int64_t now_epoch_seconds) {
	DatadogServiceMapWindow result;
	result.start_epoch_seconds = ResolveDatadogServiceMapEndpoint(from, "from", now_epoch_seconds);
	result.end_epoch_seconds = ResolveDatadogServiceMapEndpoint(to, "to", now_epoch_seconds);
	if (result.start_epoch_seconds > result.end_epoch_seconds) {
		throw InvalidInputException("Datadog service map from must not be later than to");
	}
	constexpr int64_t NANOS_PER_SECOND = 1000LL * 1000LL * 1000LL;
	constexpr int64_t MIN_TIMESTAMP_SECONDS = std::numeric_limits<int64_t>::min() / NANOS_PER_SECOND;
	constexpr int64_t MAX_TIMESTAMP_SECONDS = std::numeric_limits<int64_t>::max() / NANOS_PER_SECOND;
	if (result.start_epoch_seconds < MIN_TIMESTAMP_SECONDS || result.start_epoch_seconds > MAX_TIMESTAMP_SECONDS ||
	    result.end_epoch_seconds < MIN_TIMESTAMP_SECONDS || result.end_epoch_seconds > MAX_TIMESTAMP_SECONDS) {
		throw InvalidInputException("Datadog service map window is outside the TIMESTAMP_NS range");
	}
	return result;
}

static bool ReadOptionalString(yyjson_val *object, const char *key, string &result, const string &location) {
	auto value = yyjson_obj_get(object, key);
	if (!value || yyjson_is_null(value)) {
		return false;
	}
	if (!yyjson_is_str(value)) {
		throw IOException("Datadog open alerts returned malformed data: %s.%s must be a string or null", location, key);
	}
	result = yyjson_get_str(value);
	return true;
}

static bool ReadOptionalInt64(yyjson_val *object, const char *key, int64_t &result, const string &location) {
	auto value = yyjson_obj_get(object, key);
	if (!value || yyjson_is_null(value)) {
		return false;
	}
	if (!yyjson_is_int(value)) {
		throw IOException("Datadog open alerts returned malformed data: %s.%s must be an integer or null", location,
		                  key);
	}
	result = yyjson_get_sint(value);
	return true;
}

DatadogOpenAlertsPage ParseDatadogOpenAlertsPage(const string &response_json) {
	YyjsonDocPtr doc(yyjson_read(response_json.c_str(), response_json.size(), 0));
	if (!doc) {
		throw IOException("Datadog open alerts returned a response that is not valid JSON");
	}
	auto root = yyjson_doc_get_root(doc.get());
	if (!root || !yyjson_is_obj(root)) {
		throw IOException("Datadog open alerts returned malformed data: expected a top-level object");
	}
	auto groups = yyjson_obj_get(root, "groups");
	if (!groups || !yyjson_is_arr(groups)) {
		throw IOException("Datadog open alerts returned malformed data: expected a 'groups' array");
	}

	DatadogOpenAlertsPage result;
	size_t index, max;
	yyjson_val *item;
	yyjson_arr_foreach(groups, index, max, item) {
		if (!item || !yyjson_is_obj(item)) {
			throw IOException("Datadog open alerts returned malformed data: groups[%d] is not an object", index);
		}
		DatadogOpenAlertGroup group;
		auto location = "groups[" + std::to_string(index) + "]";
		group.has_monitor_id = ReadOptionalInt64(item, "monitor_id", group.monitor_id, location);
		group.has_monitor_name = ReadOptionalString(item, "monitor_name", group.monitor_name, location);
		group.has_group = ReadOptionalString(item, "group", group.group, location);
		group.has_status = ReadOptionalString(item, "status", group.status, location);
		group.has_last_triggered_ts = ReadOptionalInt64(item, "last_triggered_ts", group.last_triggered_ts, location);
		group.has_last_nodata_ts = ReadOptionalInt64(item, "last_nodata_ts", group.last_nodata_ts, location);

		auto tags = yyjson_obj_get(item, "group_tags");
		if (tags && !yyjson_is_null(tags)) {
			if (!yyjson_is_arr(tags)) {
				throw IOException("Datadog open alerts returned malformed data: %s.group_tags must be an array or null",
				                  location);
			}
			group.has_group_tags = true;
			size_t tag_index, tag_max;
			yyjson_val *tag;
			yyjson_arr_foreach(tags, tag_index, tag_max, tag) {
				if (!yyjson_is_str(tag)) {
					throw IOException("Datadog open alerts returned malformed data: %s.group_tags[%d] must be a string",
					                  location, tag_index);
				}
				group.group_tags.emplace_back(yyjson_get_str(tag));
			}
		}
		result.groups.push_back(std::move(group));
	}

	auto metadata = yyjson_obj_get(root, "metadata");
	if (metadata && !yyjson_is_null(metadata)) {
		if (!yyjson_is_obj(metadata)) {
			throw IOException("Datadog open alerts returned malformed data: metadata must be an object or null");
		}
		result.has_total_count = ReadOptionalInt64(metadata, "total_count", result.total_count, "metadata");
		if (result.has_total_count && result.total_count < 0) {
			throw IOException("Datadog open alerts returned malformed data: metadata.total_count must be non-negative");
		}
	}
	return result;
}

vector<string> ParseDatadogLogIndexes(const string &response_json) {
	YyjsonDocPtr doc(yyjson_read(response_json.c_str(), response_json.size(), 0));
	if (!doc) {
		throw IOException("Datadog index discovery returned a response that is not valid JSON");
	}
	auto root = yyjson_doc_get_root(doc.get());
	if (!root || !yyjson_is_obj(root)) {
		throw IOException("Datadog index discovery returned malformed data: expected a top-level object");
	}
	auto indexes = yyjson_obj_get(root, "indexes");
	if (!indexes || !yyjson_is_arr(indexes)) {
		throw IOException("Datadog index discovery returned malformed data: expected an 'indexes' array");
	}

	vector<string> result;
	std::unordered_set<string> seen;
	size_t index, max;
	yyjson_val *item;
	yyjson_arr_foreach(indexes, index, max, item) {
		if (!item || !yyjson_is_obj(item)) {
			throw IOException("Datadog index discovery returned malformed data: indexes[%d] is not an object", index);
		}
		auto name_value = yyjson_obj_get(item, "name");
		if (!name_value || !yyjson_is_str(name_value)) {
			throw IOException(
			    "Datadog index discovery returned malformed data: indexes[%d].name must be a non-empty string", index);
		}
		string name = yyjson_get_str(name_value);
		if (name.empty()) {
			throw IOException(
			    "Datadog index discovery returned malformed data: indexes[%d].name must be a non-empty string", index);
		}
		if (seen.insert(name).second) {
			result.push_back(std::move(name));
		}
	}
	return result;
}

string BuildDatadogSearchQuery(const string &query, const vector<string> &query_terms) {
	if (query_terms.empty()) {
		return query;
	}

	string pushed_query;
	for (idx_t i = 0; i < query_terms.size(); i++) {
		if (i > 0) {
			pushed_query += " AND ";
		}
		pushed_query += query_terms[i];
	}
	if (query == "*") {
		return pushed_query;
	}
	return "(" + query + ") AND " + pushed_query;
}

static bool TryParseEpochMilliseconds(const string &value, int64_t &result) {
	if (value.empty()) {
		return false;
	}
	idx_t offset = value[0] == '-' ? 1 : 0;
	if (offset == value.size()) {
		return false;
	}
	for (idx_t i = offset; i < value.size(); i++) {
		if (value[i] < '0' || value[i] > '9') {
			return false;
		}
	}
	try {
		result = std::stoll(value);
		return true;
	} catch (const std::exception &) {
		return false;
	}
}

DatadogResolvedSearch ResolveDatadogSearch(const string &query, const string &from, const string &to,
                                           const DatadogFilterPushdown &pushdown) {
	DatadogResolvedSearch result;
	result.query = BuildDatadogSearchQuery(query, pushdown.query_terms);
	result.from = from;
	result.to = to;
	if (!pushdown.has_lower_bound_ms && !pushdown.has_upper_bound_ms) {
		return result;
	}

	// Datadog evaluates relative values such as `now-15m` and `now` on the server when it receives
	// the request. Replacing either with a client-side timestamp shifts the source relation because
	// of request latency and clock skew. Only an already-absolute window can be intersected exactly.
	int64_t lower_bound_ms;
	int64_t upper_bound_ms;
	if (!TryParseEpochMilliseconds(from, lower_bound_ms) || !TryParseEpochMilliseconds(to, upper_bound_ms)) {
		return result;
	}
	if (pushdown.has_lower_bound_ms) {
		lower_bound_ms = std::max(lower_bound_ms, pushdown.lower_bound_ms);
	}
	if (pushdown.has_upper_bound_ms) {
		upper_bound_ms = std::min(upper_bound_ms, pushdown.upper_bound_ms);
	}
	result.from = std::to_string(lower_bound_ms);
	result.to = std::to_string(upper_bound_ms);
	result.empty = lower_bound_ms > upper_bound_ms;
	return result;
}

string BuildDatadogLogsSearchBody(const string &query, const string &from, const string &to, const string &sort,
                                  int64_t limit, const string &cursor, const vector<string> &indexes) {
	YyjsonMutDocPtr doc(yyjson_mut_doc_new(nullptr));
	auto root = yyjson_mut_obj(doc.get());
	yyjson_mut_doc_set_root(doc.get(), root);

	auto filter = yyjson_mut_obj(doc.get());
	yyjson_mut_obj_add_strcpy(doc.get(), filter, "query", query.c_str());
	yyjson_mut_obj_add_strcpy(doc.get(), filter, "from", from.c_str());
	yyjson_mut_obj_add_strcpy(doc.get(), filter, "to", to.c_str());
	if (!indexes.empty()) {
		auto index_array = yyjson_mut_arr(doc.get());
		for (const auto &index : indexes) {
			yyjson_mut_arr_add_strcpy(doc.get(), index_array, index.c_str());
		}
		yyjson_mut_obj_add_val(doc.get(), filter, "indexes", index_array);
	}
	yyjson_mut_obj_add_val(doc.get(), root, "filter", filter);

	yyjson_mut_obj_add_strcpy(doc.get(), root, "sort", sort.c_str());

	auto page = yyjson_mut_obj(doc.get());
	yyjson_mut_obj_add_int(doc.get(), page, "limit", limit);
	if (!cursor.empty()) {
		yyjson_mut_obj_add_strcpy(doc.get(), page, "cursor", cursor.c_str());
	}
	yyjson_mut_obj_add_val(doc.get(), root, "page", page);

	YyjsonStrPtr json(yyjson_mut_write(doc.get(), 0, nullptr));
	return json ? string(json.get()) : string();
}

//! Merge every key of the JSON object `source_json` into `dest` as a top-level custom attribute,
//! skipping any key already present (reserved attributes set earlier always win). Non-object or
//! unparseable input is silently ignored so a malformed attribute column never fails the send.
static void MergeJsonObjectAttributes(yyjson_mut_doc *doc, yyjson_mut_val *dest, const string &source_json) {
	if (source_json.empty()) {
		return;
	}
	YyjsonDocPtr parsed(yyjson_read(source_json.c_str(), source_json.size(), 0));
	if (!parsed) {
		return;
	}
	yyjson_val *root = yyjson_doc_get_root(parsed.get());
	if (!root || !yyjson_is_obj(root)) {
		return;
	}
	yyjson_obj_iter iter;
	yyjson_obj_iter_init(root, &iter);
	yyjson_val *key;
	while ((key = yyjson_obj_iter_next(&iter))) {
		const char *key_str = yyjson_get_str(key);
		if (!key_str || yyjson_mut_obj_get(dest, key_str)) {
			continue; // reserved / already-set keys are never overwritten
		}
		yyjson_val *val = yyjson_obj_iter_get_val(key);
		yyjson_mut_val *mut_key = yyjson_mut_strcpy(doc, key_str);
		yyjson_mut_val *mut_val = yyjson_val_mut_copy(doc, val);
		if (mut_key && mut_val) {
			yyjson_mut_obj_add(dest, mut_key, mut_val);
		}
	}
}

//! If `resource_json` is a JSON object, nest it under `resource_attributes` and, when the caller has
//! not already set them, derive `hostname` from its `host` string and `ddtags` from its `ddtags`
//! string array (comma-joined, matching Datadog's tag encoding).
static void ApplyResourceAttributes(yyjson_mut_doc *doc, yyjson_mut_val *obj, const string &resource_json,
                                    bool have_hostname, bool have_ddtags) {
	if (resource_json.empty()) {
		return;
	}
	YyjsonDocPtr parsed(yyjson_read(resource_json.c_str(), resource_json.size(), 0));
	if (!parsed) {
		return;
	}
	yyjson_val *root = yyjson_doc_get_root(parsed.get());
	if (!root || !yyjson_is_obj(root)) {
		return;
	}

	if (!have_hostname) {
		yyjson_val *host = yyjson_obj_get(root, "host");
		if (host && yyjson_is_str(host)) {
			yyjson_mut_obj_add_strcpy(doc, obj, "hostname", yyjson_get_str(host));
		}
	}
	if (!have_ddtags) {
		yyjson_val *tags = yyjson_obj_get(root, "ddtags");
		if (tags && yyjson_is_arr(tags) && yyjson_arr_size(tags) > 0) {
			string joined;
			size_t idx, max;
			yyjson_val *tag;
			yyjson_arr_foreach(tags, idx, max, tag) {
				if (!yyjson_is_str(tag)) {
					continue;
				}
				if (!joined.empty()) {
					joined += ",";
				}
				joined += yyjson_get_str(tag);
			}
			if (!joined.empty()) {
				yyjson_mut_obj_add_strcpy(doc, obj, "ddtags", joined.c_str());
			}
		}
	}

	// Preserve the full resource block so nothing is lost even when host/ddtags were already set.
	yyjson_mut_val *copy = yyjson_val_mut_copy(doc, root);
	if (copy) {
		yyjson_mut_obj_add(obj, yyjson_mut_strcpy(doc, "resource_attributes"), copy);
	}
}

idx_t EstimateDatadogIntakeLogBytes(const DatadogIntakeLog &log) {
	// Fixed envelope covers the JSON braces, reserved keys, quotes, and separators; the exact number
	// does not matter as long as it is a comfortable over-estimate of the non-value overhead.
	constexpr idx_t PER_LOG_ENVELOPE = 256;
	return PER_LOG_ENVELOPE + log.message.size() + log.service.size() + log.status.size() + log.hostname.size() +
	       log.ddsource.size() + log.ddtags.size() + log.trace_id.size() + log.span_id.size() +
	       log.log_attributes_json.size() + log.resource_attributes_json.size();
}

string BuildDatadogIntakeBody(const DatadogIntakeLog *logs, idx_t count) {
	YyjsonMutDocPtr doc(yyjson_mut_doc_new(nullptr));
	auto arr = yyjson_mut_arr(doc.get());
	yyjson_mut_doc_set_root(doc.get(), arr);

	for (idx_t i = 0; i < count; i++) {
		const auto &log = logs[i];
		auto obj = yyjson_mut_obj(doc.get());

		// Reserved Datadog attributes first, so log_attributes can never clobber them.
		if (!log.message.empty()) {
			yyjson_mut_obj_add_strcpy(doc.get(), obj, "message", log.message.c_str());
		}
		if (!log.service.empty()) {
			yyjson_mut_obj_add_strcpy(doc.get(), obj, "service", log.service.c_str());
		}
		if (!log.status.empty()) {
			yyjson_mut_obj_add_strcpy(doc.get(), obj, "status", log.status.c_str());
		}
		if (!log.ddsource.empty()) {
			yyjson_mut_obj_add_strcpy(doc.get(), obj, "ddsource", log.ddsource.c_str());
		}
		if (!log.hostname.empty()) {
			yyjson_mut_obj_add_strcpy(doc.get(), obj, "hostname", log.hostname.c_str());
		}
		if (!log.ddtags.empty()) {
			yyjson_mut_obj_add_strcpy(doc.get(), obj, "ddtags", log.ddtags.c_str());
		}
		if (log.has_timestamp_ms) {
			yyjson_mut_obj_add_int(doc.get(), obj, "timestamp", log.timestamp_ms);
		}
		if (!log.trace_id.empty()) {
			yyjson_mut_obj_add_strcpy(doc.get(), obj, "trace_id", log.trace_id.c_str());
		}
		if (!log.span_id.empty()) {
			yyjson_mut_obj_add_strcpy(doc.get(), obj, "span_id", log.span_id.c_str());
		}

		ApplyResourceAttributes(doc.get(), obj, log.resource_attributes_json, !log.hostname.empty(),
		                        !log.ddtags.empty());
		MergeJsonObjectAttributes(doc.get(), obj, log.log_attributes_json);

		yyjson_mut_arr_append(arr, obj);
	}

	YyjsonStrPtr json(yyjson_mut_write(doc.get(), 0, nullptr));
	return json ? string(json.get()) : string("[]");
}

bool DatadogLogsMaxRowsReached(int64_t max_rows, idx_t total_emitted) {
	return max_rows > 0 && total_emitted >= static_cast<idx_t>(max_rows);
}

int64_t GetDatadogLogsPageLimit(int64_t page_size, int64_t max_rows, idx_t row_budget_used) {
	if (max_rows <= 0) {
		return page_size;
	}
	if (row_budget_used >= static_cast<idx_t>(max_rows)) {
		return 0;
	}
	auto remaining = static_cast<int64_t>(static_cast<idx_t>(max_rows) - row_budget_used);
	return MinValue<int64_t>(page_size, remaining);
}

} // namespace duckdb
