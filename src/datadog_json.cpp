#include "datadog_json.hpp"

#include "duckdb/common/exception.hpp"

#include "yyjson.hpp"

#include <algorithm>
#include <cstdlib>
#include <memory>
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
