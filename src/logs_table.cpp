#include "logs_table.hpp"

#include "datadog_client.hpp"
#include "datadog_secret.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "yyjson.hpp"

#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

//===--------------------------------------------------------------------===//
// yyjson RAII helpers — free docs/buffers on every path (incl. exceptions)
//===--------------------------------------------------------------------===//
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

//===--------------------------------------------------------------------===//
// Output schema — matches duckdb-otlp `read_otlp_logs`
//===--------------------------------------------------------------------===//
static constexpr idx_t COL_TIME = 0;
static constexpr idx_t COL_OBSERVED_TIME = 1;
static constexpr idx_t COL_TRACE_ID = 2;
static constexpr idx_t COL_SPAN_ID = 3;
static constexpr idx_t COL_SERVICE_NAME = 4;
static constexpr idx_t COL_SEVERITY_NUMBER = 7;
static constexpr idx_t COL_SEVERITY_TEXT = 8;
static constexpr idx_t COL_BODY = 10;
static constexpr idx_t COL_RESOURCE_ATTRS = 11;
static constexpr idx_t COL_LOG_ATTRS = 15;
static constexpr idx_t COLUMN_COUNT = 18;

static void GetLogsSchema(vector<LogicalType> &types, vector<string> &names) {
	names = {"time_unix_nano",
	         "observed_time_unix_nano",
	         "trace_id",
	         "span_id",
	         "service_name",
	         "service_namespace",
	         "service_instance_id",
	         "severity_number",
	         "severity_text",
	         "event_name",
	         "body",
	         "resource_attributes",
	         "scope_name",
	         "scope_version",
	         "scope_attributes",
	         "log_attributes",
	         "dropped_attributes_count",
	         "flags"};
	types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::INTEGER,
	         LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::INTEGER,      LogicalType::INTEGER};
	D_ASSERT(names.size() == COLUMN_COUNT && types.size() == COLUMN_COUNT);
}

//===--------------------------------------------------------------------===//
// Datadog -> OTLP mapping helpers
//===--------------------------------------------------------------------===//

//! Map a Datadog log status to an OTLP SeverityNumber (1-24). 0 = unspecified/unknown.
static int32_t StatusToSeverityNumber(const string &status) {
	auto s = StringUtil::Lower(status);
	if (s == "trace") {
		return 1;
	}
	if (s == "debug") {
		return 5;
	}
	if (s == "info" || s == "notice" || s == "ok") {
		return 9;
	}
	if (s == "warn" || s == "warning") {
		return 13;
	}
	if (s == "error" || s == "err") {
		return 17;
	}
	if (s == "critical" || s == "crit" || s == "alert" || s == "emergency" || s == "fatal") {
		return 21;
	}
	return 0;
}

//! Parse an ISO-8601 timestamp (e.g. "2026-07-07T10:30:45.123Z") into nanoseconds since epoch.
static bool ParseIso8601ToNanos(const char *str, int64_t &out_nanos) {
	if (!str) {
		return false;
	}
	idx_t len = strlen(str);

	timestamp_t ts;
	bool has_offset = false;
	string_t tz;
	int32_t sub_micro_nanos = 0;
	auto result = Timestamp::TryConvertTimestampTZ(str, len, ts, /*use_offset=*/true, has_offset, tz, &sub_micro_nanos);
	if (result != TimestampCastResult::SUCCESS) {
		// Fall back to the strict (offset-less) nanosecond parser.
		timestamp_ns_t ts_ns;
		if (Timestamp::TryConvertTimestamp(str, len, ts_ns) != TimestampCastResult::SUCCESS) {
			return false;
		}
		out_nanos = ts_ns.value;
		return true;
	}

	int64_t epoch_nanos;
	if (!Timestamp::TryGetEpochNanoSeconds(ts, epoch_nanos)) {
		return false;
	}
	out_nanos = epoch_nanos + sub_micro_nanos;
	return true;
}

//! Serialize an immutable yyjson value subtree back to a compact JSON string.
static string SerializeVal(yyjson_val *val) {
	if (!val) {
		return string();
	}
	size_t len = 0;
	YyjsonStrPtr json(yyjson_val_write(val, 0, &len));
	return json ? string(json.get(), len) : string();
}

static const char *GetStr(yyjson_val *obj, const char *key) {
	if (!obj) {
		return nullptr;
	}
	yyjson_val *v = yyjson_obj_get(obj, key);
	return (v && yyjson_is_str(v)) ? yyjson_get_str(v) : nullptr;
}

//! Build the OTLP `resource_attributes` JSON from Datadog resource-ish fields (host, tags).
static string BuildResourceAttributes(yyjson_val *attributes) {
	if (!attributes) {
		return string();
	}
	yyjson_val *host = yyjson_obj_get(attributes, "host");
	yyjson_val *tags = yyjson_obj_get(attributes, "tags");
	bool has_host = host && yyjson_is_str(host);
	bool has_tags = tags && yyjson_is_arr(tags) && yyjson_arr_size(tags) > 0;
	if (!has_host && !has_tags) {
		return string();
	}

	YyjsonMutDocPtr doc(yyjson_mut_doc_new(nullptr));
	yyjson_mut_val *root = yyjson_mut_obj(doc.get());
	yyjson_mut_doc_set_root(doc.get(), root);
	if (has_host) {
		yyjson_mut_obj_add_strcpy(doc.get(), root, "host", yyjson_get_str(host));
	}
	if (has_tags) {
		yyjson_mut_val *tags_copy = yyjson_val_mut_copy(doc.get(), tags);
		yyjson_mut_obj_add_val(doc.get(), root, "ddtags", tags_copy);
	}
	YyjsonStrPtr json(yyjson_mut_write(doc.get(), 0, nullptr));
	return json ? string(json.get()) : string();
}

//! Build the POST body for /api/v2/logs/events/search.
static string BuildSearchBody(const string &query, const string &from, const string &to, int64_t limit,
                              const string &cursor) {
	YyjsonMutDocPtr doc(yyjson_mut_doc_new(nullptr));
	yyjson_mut_val *root = yyjson_mut_obj(doc.get());
	yyjson_mut_doc_set_root(doc.get(), root);

	yyjson_mut_val *filter = yyjson_mut_obj(doc.get());
	yyjson_mut_obj_add_strcpy(doc.get(), filter, "query", query.c_str());
	yyjson_mut_obj_add_strcpy(doc.get(), filter, "from", from.c_str());
	yyjson_mut_obj_add_strcpy(doc.get(), filter, "to", to.c_str());
	yyjson_mut_obj_add_val(doc.get(), root, "filter", filter);

	// Ascending, stable sort so cursor pagination returns a deterministic ordering.
	yyjson_mut_obj_add_strcpy(doc.get(), root, "sort", "timestamp");

	yyjson_mut_val *page = yyjson_mut_obj(doc.get());
	yyjson_mut_obj_add_int(doc.get(), page, "limit", limit);
	if (!cursor.empty()) {
		yyjson_mut_obj_add_strcpy(doc.get(), page, "cursor", cursor.c_str());
	}
	yyjson_mut_obj_add_val(doc.get(), root, "page", page);

	YyjsonStrPtr json(yyjson_mut_write(doc.get(), 0, nullptr));
	return json ? string(json.get()) : string();
}

//! Map one Datadog log event (`data[]` element) to a row of Values in output-schema order.
static void MapEvent(yyjson_val *item, vector<Value> &row) {
	row.assign(COLUMN_COUNT, Value()); // all columns NULL by default

	yyjson_val *attributes = yyjson_obj_get(item, "attributes");
	yyjson_val *custom = attributes ? yyjson_obj_get(attributes, "attributes") : nullptr;

	const char *timestamp = GetStr(attributes, "timestamp");
	if (timestamp) {
		int64_t nanos;
		if (ParseIso8601ToNanos(timestamp, nanos)) {
			row[COL_TIME] = Value::TIMESTAMPNS(timestamp_ns_t(nanos));
			row[COL_OBSERVED_TIME] = Value::TIMESTAMPNS(timestamp_ns_t(nanos));
		}
	}

	// Trace correlation is only present when the log carries it in custom attributes.
	const char *trace_id = GetStr(custom, "trace_id");
	const char *span_id = GetStr(custom, "span_id");
	if (trace_id) {
		row[COL_TRACE_ID] = Value(string(trace_id));
	}
	if (span_id) {
		row[COL_SPAN_ID] = Value(string(span_id));
	}

	const char *service = GetStr(attributes, "service");
	if (service) {
		row[COL_SERVICE_NAME] = Value(string(service));
	}

	const char *status = GetStr(attributes, "status");
	if (status) {
		row[COL_SEVERITY_TEXT] = Value(string(status));
		row[COL_SEVERITY_NUMBER] = Value::INTEGER(StatusToSeverityNumber(status));
	}

	const char *message = GetStr(attributes, "message");
	if (message) {
		row[COL_BODY] = Value(string(message));
	}

	string resource_attributes = BuildResourceAttributes(attributes);
	if (!resource_attributes.empty()) {
		row[COL_RESOURCE_ATTRS] = Value(resource_attributes);
	}

	if (custom && yyjson_is_obj(custom)) {
		string log_attributes = SerializeVal(custom);
		if (!log_attributes.empty()) {
			row[COL_LOG_ATTRS] = Value(log_attributes);
		}
	}
}

//===--------------------------------------------------------------------===//
// Table function state
//===--------------------------------------------------------------------===//

struct DatadogLogsBindData : public TableFunctionData {
	string query = "*";
	string from = "now-15m";
	string to = "now";
	int64_t page_limit = 1000; // Datadog max
	int64_t max_rows = 0;      // 0 = unlimited
	DatadogClient client;
};

struct DatadogLogsGlobalState : public GlobalTableFunctionState {
	//! Rows parsed and waiting to be emitted.
	std::deque<vector<Value>> buffer;
	//! Next-page cursor from meta.page.after ("" = request the first page).
	string cursor;
	idx_t total_emitted = 0;
	bool finished = false;

	idx_t MaxThreads() const override {
		return 1; // Serialize: cursor pagination is inherently sequential.
	}
};

//! Fetch the next page and update pagination state. Pure cursor pagination: follow
//! meta.page.after until it is empty, a page returns no events, or the cursor stops advancing
//! (the latter two guard against an endless stream of empty / non-advancing pages).
//!
//! Datadog cursor pagination retrieves the full [from, to] window. Windows so large that they
//! exceed the API's cursor depth should be paged through by narrowing `from`/`to` at the call
//! site (e.g. one hour at a time).
static void FetchNextPage(const DatadogLogsBindData &bind, DatadogLogsGlobalState &state) {
	string body = BuildSearchBody(bind.query, bind.from, bind.to, bind.page_limit, state.cursor);
	string response = bind.client.SearchLogs(body);

	YyjsonDocPtr doc(yyjson_read(response.c_str(), response.size(), 0));
	if (!doc) {
		throw IOException("Datadog API returned a response that is not valid JSON");
	}
	yyjson_val *root = yyjson_doc_get_root(doc.get());

	idx_t page_rows = 0;
	yyjson_val *data = yyjson_obj_get(root, "data");
	if (data && yyjson_is_arr(data)) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(data, idx, max, item) {
			vector<Value> row;
			MapEvent(item, row);
			state.buffer.push_back(std::move(row));
			page_rows++;
		}
	}

	yyjson_val *meta = yyjson_obj_get(root, "meta");
	yyjson_val *page = meta ? yyjson_obj_get(meta, "page") : nullptr;
	const char *after = GetStr(page, "after");

	if (!after || after[0] == '\0' || page_rows == 0 || state.cursor == after) {
		state.finished = true;
	} else {
		state.cursor = after; // copies the C-string before `doc` is freed at scope end
	}
}

static unique_ptr<FunctionData> DatadogLogsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DatadogLogsBindData>();
	string secret_name;

	for (auto &param : input.named_parameters) {
		auto key = StringUtil::Lower(param.first);
		if (param.second.IsNull()) {
			continue;
		}
		if (key == "query") {
			result->query = param.second.ToString();
		} else if (key == "from") {
			result->from = param.second.ToString();
		} else if (key == "to") {
			result->to = param.second.ToString();
		} else if (key == "limit") {
			result->page_limit = param.second.GetValue<int64_t>();
		} else if (key == "max_rows") {
			result->max_rows = param.second.GetValue<int64_t>();
		} else if (key == "secret") {
			secret_name = param.second.ToString();
		}
	}

	if (result->page_limit < 1) {
		result->page_limit = 1;
	}
	if (result->page_limit > 1000) {
		result->page_limit = 1000; // Datadog hard limit
	}
	if (result->max_rows < 0) {
		throw InvalidInputException("read_datadog_logs: max_rows must be >= 0 (0 means unlimited)");
	}

	auto credentials = GetDatadogCredentials(context, secret_name);
	result->client.site = credentials.site;
	result->client.api_key = credentials.api_key;
	result->client.app_key = credentials.app_key;

	GetLogsSchema(return_types, names);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DatadogLogsInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	return make_uniq<DatadogLogsGlobalState>();
}

static void DatadogLogsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<DatadogLogsBindData>();
	auto &state = data_p.global_state->Cast<DatadogLogsGlobalState>();

	if (bind.max_rows > 0 && state.total_emitted >= static_cast<idx_t>(bind.max_rows)) {
		// Row cap already reached — stop before issuing another request.
		state.finished = true;
		state.buffer.clear();
	}

	// Refill the buffer, fetching as many pages as needed to get at least one row (or finish).
	while (state.buffer.empty() && !state.finished) {
		FetchNextPage(bind, state);
	}

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && !state.buffer.empty()) {
		if (bind.max_rows > 0 && state.total_emitted >= static_cast<idx_t>(bind.max_rows)) {
			state.finished = true;
			state.buffer.clear();
			break;
		}
		auto &row = state.buffer.front();
		for (idx_t col = 0; col < COLUMN_COUNT; col++) {
			output.SetValue(col, count, row[col]);
		}
		state.buffer.pop_front();
		count++;
		state.total_emitted++;
	}

	output.SetCardinality(count);
}

void RegisterDatadogLogsFunction(ExtensionLoader &loader) {
	TableFunction function("read_datadog_logs", {}, DatadogLogsScan, DatadogLogsBind, DatadogLogsInitGlobal);
	function.named_parameters["query"] = LogicalType::VARCHAR;
	function.named_parameters["from"] = LogicalType::VARCHAR;
	function.named_parameters["to"] = LogicalType::VARCHAR;
	function.named_parameters["limit"] = LogicalType::BIGINT;
	function.named_parameters["max_rows"] = LogicalType::BIGINT;
	function.named_parameters["secret"] = LogicalType::VARCHAR;
	loader.RegisterFunction(function);
}

} // namespace duckdb
