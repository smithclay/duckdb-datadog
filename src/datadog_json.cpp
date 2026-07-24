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

string BuildDatadogIntakeBody(const vector<DatadogIntakeLog> &logs) {
	YyjsonMutDocPtr doc(yyjson_mut_doc_new(nullptr));
	auto arr = yyjson_mut_arr(doc.get());
	yyjson_mut_doc_set_root(doc.get(), arr);

	for (const auto &log : logs) {
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
