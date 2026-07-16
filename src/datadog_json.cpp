#include "datadog_json.hpp"

#include "duckdb/common/exception.hpp"

#include "yyjson.hpp"

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

string BuildDatadogLogsSearchBody(const string &query, const string &from, const string &to, int64_t limit,
                                  const string &cursor, const vector<string> &indexes) {
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

	// Ascending, stable sort so cursor pagination returns a deterministic ordering.
	yyjson_mut_obj_add_strcpy(doc.get(), root, "sort", "timestamp");

	auto page = yyjson_mut_obj(doc.get());
	yyjson_mut_obj_add_int(doc.get(), page, "limit", limit);
	if (!cursor.empty()) {
		yyjson_mut_obj_add_strcpy(doc.get(), page, "cursor", cursor.c_str());
	}
	yyjson_mut_obj_add_val(doc.get(), root, "page", page);

	YyjsonStrPtr json(yyjson_mut_write(doc.get(), 0, nullptr));
	return json ? string(json.get()) : string();
}

} // namespace duckdb
