#include "traces_table.hpp"

#include "datadog_client.hpp"
#include "datadog_json.hpp"
#include "datadog_secret.hpp"
#include "logs_table.hpp"

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
// Output schema — matches duckdb-otlp `read_otlp_traces`
//===--------------------------------------------------------------------===//
static constexpr idx_t COL_START_TIME = 0;
static constexpr idx_t COL_DURATION = 1;
static constexpr idx_t COL_TRACE_ID = 2;
static constexpr idx_t COL_SPAN_ID = 3;
static constexpr idx_t COL_PARENT_SPAN_ID = 4;
static constexpr idx_t COL_SERVICE_NAME = 6;
static constexpr idx_t COL_NAME = 9;
static constexpr idx_t COL_KIND = 10;
static constexpr idx_t COL_STATUS_CODE = 11;
static constexpr idx_t COL_STATUS_MESSAGE = 12;
static constexpr idx_t COL_RESOURCE_ATTRS = 13;
static constexpr idx_t COL_SPAN_ATTRS = 17;
static constexpr idx_t COLUMN_COUNT = 24;

void GetDatadogTracesSchema(vector<LogicalType> &types, vector<string> &names) {
	names = {"start_time_unix_nano",
	         "duration_time_unix_nano",
	         "trace_id",
	         "span_id",
	         "parent_span_id",
	         "trace_state",
	         "service_name",
	         "service_namespace",
	         "service_instance_id",
	         "name",
	         "kind",
	         "status_code",
	         "status_status_message",
	         "resource_attributes",
	         "scope_name",
	         "scope_version",
	         "scope_attributes",
	         "span_attributes",
	         "events_json",
	         "links_json",
	         "dropped_attributes_count",
	         "dropped_events_count",
	         "dropped_links_count",
	         "flags"};
	types = {LogicalType::TIMESTAMP_NS, LogicalType::BIGINT,  LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER,
	         LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::INTEGER,      LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER};
	D_ASSERT(names.size() == COLUMN_COUNT && types.size() == COLUMN_COUNT);
}

//===--------------------------------------------------------------------===//
// Datadog spans -> OTLP mapping helpers
//===--------------------------------------------------------------------===//

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

static const char *GetStr(yyjson_val *obj, const char *key) {
	if (!obj) {
		return nullptr;
	}
	yyjson_val *v = yyjson_obj_get(obj, key);
	return (v && yyjson_is_str(v)) ? yyjson_get_str(v) : nullptr;
}

//! Map an OTLP span.kind tag value ("server", "SPAN_KIND_SERVER", ...) to the OTLP kind integer.
static int32_t SpanKindToInteger(const string &kind) {
	auto value = StringUtil::Lower(kind);
	auto prefix = string("span_kind_");
	if (StringUtil::StartsWith(value, prefix)) {
		value = value.substr(prefix.size());
	}
	if (value == "internal") {
		return 1;
	}
	if (value == "server") {
		return 2;
	}
	if (value == "client") {
		return 3;
	}
	if (value == "producer") {
		return 4;
	}
	if (value == "consumer") {
		return 5;
	}
	return 0;
}

//! Find the span.kind of a span event. Datadog nests dotted attributes into objects, so the
//! `span.kind` tag arrives as custom.span.kind; a `span.kind:<value>` entry in the reserved tags
//! array is the fallback.
static int32_t ExtractSpanKind(yyjson_val *custom, yyjson_val *tags) {
	yyjson_val *span_obj = custom ? yyjson_obj_get(custom, "span") : nullptr;
	const char *kind = (span_obj && yyjson_is_obj(span_obj)) ? GetStr(span_obj, "kind") : nullptr;
	if (kind) {
		return SpanKindToInteger(kind);
	}
	if (tags && yyjson_is_arr(tags)) {
		size_t idx, max;
		yyjson_val *tag;
		yyjson_arr_foreach(tags, idx, max, tag) {
			if (!yyjson_is_str(tag)) {
				continue;
			}
			const char *text = yyjson_get_str(tag);
			constexpr const char *PREFIX = "span.kind:";
			if (strncmp(text, PREFIX, strlen(PREFIX)) == 0) {
				return SpanKindToInteger(text + strlen(PREFIX));
			}
		}
	}
	return 0;
}

//! Whether a span event is an error. The reserved `status` field ("ok"/"error") is authoritative;
//! spans without it fall back to the error objects: Datadog nests dotted error tags, so they
//! arrive as {attributes,custom}.error.{message,type,stack}.
static bool SpanHasError(yyjson_val *attributes, yyjson_val *custom) {
	const char *status = GetStr(attributes, "status");
	if (status) {
		return StringUtil::CIEquals(status, "error");
	}
	for (yyjson_val *parent : {attributes, custom}) {
		yyjson_val *error = parent ? yyjson_obj_get(parent, "error") : nullptr;
		if (!error) {
			continue;
		}
		if (yyjson_is_num(error)) {
			return yyjson_get_num(error) != 0;
		}
		if (yyjson_is_bool(error)) {
			return yyjson_get_bool(error);
		}
		if (yyjson_is_obj(error) && yyjson_obj_size(error) > 0) {
			return true;
		}
	}
	return false;
}

//! The error description: {attributes,custom}.error.message, whichever carries it.
static const char *ExtractErrorMessage(yyjson_val *attributes, yyjson_val *custom) {
	for (yyjson_val *parent : {attributes, custom}) {
		yyjson_val *error = parent ? yyjson_obj_get(parent, "error") : nullptr;
		if (error && yyjson_is_obj(error)) {
			const char *message = GetStr(error, "message");
			if (message) {
				return message;
			}
		}
	}
	return nullptr;
}

//! Build the OTLP `resource_attributes` JSON from the span's resource-ish reserved fields.
static string BuildResourceAttributes(yyjson_val *attributes) {
	if (!attributes) {
		return string();
	}
	yyjson_val *host = yyjson_obj_get(attributes, "host");
	yyjson_val *env = yyjson_obj_get(attributes, "env");
	yyjson_val *tags = yyjson_obj_get(attributes, "tags");
	bool has_host = host && yyjson_is_str(host);
	bool has_env = env && yyjson_is_str(env);
	bool has_tags = tags && yyjson_is_arr(tags) && yyjson_arr_size(tags) > 0;
	if (!has_host && !has_env && !has_tags) {
		return string();
	}

	YyjsonMutDocPtr doc(yyjson_mut_doc_new(nullptr));
	yyjson_mut_val *root = yyjson_mut_obj(doc.get());
	yyjson_mut_doc_set_root(doc.get(), root);
	if (has_host) {
		yyjson_mut_obj_add_strcpy(doc.get(), root, "host", yyjson_get_str(host));
	}
	if (has_env) {
		yyjson_mut_obj_add_strcpy(doc.get(), root, "env", yyjson_get_str(env));
	}
	if (has_tags) {
		yyjson_mut_val *tags_copy = yyjson_val_mut_copy(doc.get(), tags);
		yyjson_mut_obj_add_val(doc.get(), root, "ddtags", tags_copy);
	}
	YyjsonStrPtr json(yyjson_mut_write(doc.get(), 0, nullptr));
	return json ? string(json.get()) : string();
}

//! Build the OTLP `span_attributes` JSON: the span's custom attributes plus the Datadog-reserved
//! per-span fields (resource_name, type, single_span, ingestion_reason, retained_by) that have no
//! OTLP column of their own, so nothing the API returned is lost.
static string BuildSpanAttributes(yyjson_val *attributes, yyjson_val *custom) {
	YyjsonMutDocPtr doc(yyjson_mut_doc_new(nullptr));
	yyjson_mut_val *root = nullptr;
	if (custom && yyjson_is_obj(custom)) {
		root = yyjson_val_mut_copy(doc.get(), custom);
	}
	if (!root || !yyjson_mut_is_obj(root)) {
		root = yyjson_mut_obj(doc.get());
	}
	yyjson_mut_doc_set_root(doc.get(), root);

	for (const char *key : {"resource_name", "type", "single_span", "ingestion_reason", "retained_by"}) {
		if (yyjson_mut_obj_get(root, key)) {
			continue; // a custom attribute of the same name wins
		}
		yyjson_val *value = attributes ? yyjson_obj_get(attributes, key) : nullptr;
		if (!value || yyjson_is_null(value)) {
			continue;
		}
		yyjson_mut_val *copy = yyjson_val_mut_copy(doc.get(), value);
		if (copy) {
			yyjson_mut_obj_add(root, yyjson_mut_strcpy(doc.get(), key), copy);
		}
	}
	if (yyjson_mut_obj_size(root) == 0) {
		return string();
	}
	YyjsonStrPtr json(yyjson_mut_write(doc.get(), 0, nullptr));
	return json ? string(json.get()) : string();
}

//! Map one Datadog span event (`data[]` element) to the projected columns of a row. Only projected
//! columns are computed (projection pushdown), so deselected fields — above all the span_attributes
//! JSON serialization — are skipped for aggregate/triage queries.
static void MapSpanEvent(yyjson_val *item, const vector<column_t> &column_ids, vector<Value> &row) {
	row.assign(column_ids.size(), Value()); // all projected columns NULL by default

	yyjson_val *attributes = yyjson_obj_get(item, "attributes");
	yyjson_val *custom = attributes ? yyjson_obj_get(attributes, "custom") : nullptr;

	for (idx_t c = 0; c < column_ids.size(); c++) {
		switch (column_ids[c]) {
		case COL_START_TIME: {
			const char *start = GetStr(attributes, "start_timestamp");
			int64_t nanos;
			if (start && ParseIso8601ToNanos(start, nanos)) {
				row[c] = Value::TIMESTAMPNS(timestamp_ns_t(nanos));
			}
			break;
		}
		case COL_DURATION: {
			// Prefer the reserved timestamps; fall back to the custom `duration` measure Datadog
			// records in nanoseconds.
			const char *start = GetStr(attributes, "start_timestamp");
			const char *end = GetStr(attributes, "end_timestamp");
			int64_t start_nanos, end_nanos;
			if (start && end && ParseIso8601ToNanos(start, start_nanos) && ParseIso8601ToNanos(end, end_nanos)) {
				row[c] = Value::BIGINT(end_nanos - start_nanos);
				break;
			}
			yyjson_val *duration = custom ? yyjson_obj_get(custom, "duration") : nullptr;
			if (duration && yyjson_is_num(duration)) {
				row[c] = Value::BIGINT(static_cast<int64_t>(yyjson_get_num(duration)));
			}
			break;
		}
		case COL_TRACE_ID: {
			const char *trace_id = GetStr(attributes, "trace_id");
			if (trace_id) {
				row[c] = Value(string(trace_id));
			}
			break;
		}
		case COL_SPAN_ID: {
			const char *span_id = GetStr(attributes, "span_id");
			if (span_id) {
				row[c] = Value(string(span_id));
			}
			break;
		}
		case COL_PARENT_SPAN_ID: {
			const char *parent_id = GetStr(attributes, "parent_id");
			if (parent_id) {
				row[c] = Value(string(parent_id));
			}
			break;
		}
		case COL_SERVICE_NAME: {
			const char *service = GetStr(attributes, "service");
			if (service) {
				row[c] = Value(string(service));
			}
			break;
		}
		case COL_NAME: {
			// Datadog's operation name lives in the custom attributes; fall back to resource_name
			// (which for OTel-instrumented services carries the original OTLP span name).
			const char *name = GetStr(custom, "operation_name");
			if (!name) {
				name = GetStr(attributes, "operation_name");
			}
			if (!name) {
				name = GetStr(attributes, "resource_name");
			}
			if (name) {
				row[c] = Value(string(name));
			}
			break;
		}
		case COL_KIND: {
			auto kind = ExtractSpanKind(custom, attributes ? yyjson_obj_get(attributes, "tags") : nullptr);
			if (kind != 0) {
				row[c] = Value::INTEGER(kind);
			}
			break;
		}
		case COL_STATUS_CODE: {
			if (attributes) {
				row[c] = Value::INTEGER(SpanHasError(attributes, custom) ? 2 : 0);
			}
			break;
		}
		case COL_STATUS_MESSAGE: {
			const char *message = ExtractErrorMessage(attributes, custom);
			if (message) {
				row[c] = Value(string(message));
			}
			break;
		}
		case COL_RESOURCE_ATTRS: {
			string resource_attributes = BuildResourceAttributes(attributes);
			if (!resource_attributes.empty()) {
				row[c] = Value(resource_attributes);
			}
			break;
		}
		case COL_SPAN_ATTRS: {
			string span_attributes = BuildSpanAttributes(attributes, custom);
			if (!span_attributes.empty()) {
				row[c] = Value(span_attributes);
			}
			break;
		}
		default:
			// Columns Datadog has no data for (trace_state, scope_*, events, links, dropped_*,
			// flags, ...) and virtual columns (e.g. the rowid sentinel a bare count(*) projects)
			// stay NULL.
			break;
		}
	}
}

//===--------------------------------------------------------------------===//
// Table function state
//===--------------------------------------------------------------------===//

struct DatadogTracesBindData : public TableFunctionData {
	string query = "*";
	string from = "now-15m";
	string to = "now";
	string sort = "timestamp";
	int64_t page_size = 1000; // rows per API request (Datadog max)
	int64_t max_rows = 0;     // 0 = unlimited
	DatadogClient client;
};

struct DatadogTracesGlobalState : public GlobalTableFunctionState {
	//! Source column for each output slot (projection pushdown); may contain virtual-column
	//! sentinels, which MapSpanEvent leaves NULL.
	vector<column_t> column_ids;
	//! Rows (projected columns only) parsed and waiting to be emitted.
	std::deque<vector<Value>> buffer;
	//! Next-page cursor from meta.page.after ("" = request the first page).
	string cursor;
	//! Sum of requested page limits; reserving the row budget when a request is sent
	//! guarantees PAGE_SIZE >= MAX_ROWS cannot trigger another cursor request.
	idx_t row_budget_used = 0;
	idx_t total_emitted = 0;
	bool finished = false;

	idx_t MaxThreads() const override {
		return 1; // Serialize: cursor pagination is inherently sequential.
	}
};

//! Fetch the next page and update pagination state; the cursor scheme matches the logs search API.
static void FetchNextPage(ClientContext &context, const DatadogTracesBindData &bind, DatadogTracesGlobalState &state) {
	auto page_limit = GetDatadogLogsPageLimit(bind.page_size, bind.max_rows, state.row_budget_used);
	D_ASSERT(page_limit > 0);
	string body = BuildDatadogSpansSearchBody(bind.query, bind.from, bind.to, bind.sort, page_limit, state.cursor);
	string response = bind.client.SearchSpans(context, body);
	state.row_budget_used += static_cast<idx_t>(page_limit);

	YyjsonDocPtr doc(yyjson_read(response.c_str(), response.size(), 0));
	if (!doc) {
		throw IOException("Datadog spans API returned a response that is not valid JSON");
	}
	yyjson_val *root = yyjson_doc_get_root(doc.get());

	idx_t page_rows = 0;
	yyjson_val *data = yyjson_obj_get(root, "data");
	if (data && yyjson_is_arr(data)) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(data, idx, max, item) {
			vector<Value> row;
			MapSpanEvent(item, state.column_ids, row);
			state.buffer.push_back(std::move(row));
			page_rows++;
		}
	}

	yyjson_val *meta = yyjson_obj_get(root, "meta");
	yyjson_val *page = meta ? yyjson_obj_get(meta, "page") : nullptr;
	const char *after = GetStr(page, "after");

	if (GetDatadogLogsPageLimit(bind.page_size, bind.max_rows, state.row_budget_used) == 0 || !after ||
	    after[0] == '\0' || page_rows == 0 || state.cursor == after) {
		state.finished = true;
	} else {
		state.cursor = after; // copies the C-string before `doc` is freed at scope end
	}
}

static unique_ptr<FunctionData> DatadogTracesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DatadogTracesBindData>();
	DatadogLogsSettings settings;
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
		} else if (key == "sort") {
			settings.sort = param.second.ToString();
		} else if (key == "page_size") {
			settings.page_size = param.second.GetValue<int64_t>();
		} else if (key == "max_rows") {
			settings.max_rows = param.second.GetValue<int64_t>();
		} else if (key == "retries") {
			settings.retries = param.second.GetValue<int64_t>();
		} else if (key == "timeout") {
			settings.timeout_seconds = param.second.GetValue<int64_t>();
		} else if (key == "secret") {
			secret_name = param.second.ToString();
		}
	}

	ValidateDatadogLogsSettings(settings, "read_datadog_traces");
	result->sort = settings.sort;
	result->page_size = settings.page_size;
	result->max_rows = settings.max_rows;
	result->client.retries = static_cast<uint64_t>(settings.retries);
	result->client.timeout_seconds = static_cast<uint64_t>(settings.timeout_seconds);

	auto credentials = GetDatadogCredentials(context, secret_name);
	result->client.site = credentials.site;
	result->client.api_key = credentials.api_key;
	result->client.app_key = credentials.app_key;

	GetDatadogTracesSchema(return_types, names);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DatadogTracesInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto state = make_uniq<DatadogTracesGlobalState>();
	state->column_ids = input.column_ids;
	return std::move(state);
}

static void DatadogTracesScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<DatadogTracesBindData>();
	auto &state = data_p.global_state->Cast<DatadogTracesGlobalState>();

	if (DatadogLogsMaxRowsReached(bind.max_rows, state.total_emitted)) {
		// Row cap already reached — stop before issuing another request.
		state.finished = true;
		state.buffer.clear();
	}

	// Refill the buffer, fetching as many pages as needed to get at least one row (or finish).
	while (state.buffer.empty() && !state.finished) {
		FetchNextPage(context, bind, state);
	}

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && !state.buffer.empty()) {
		if (DatadogLogsMaxRowsReached(bind.max_rows, state.total_emitted)) {
			state.finished = true;
			state.buffer.clear();
			break;
		}
		auto &row = state.buffer.front();
		for (idx_t col = 0; col < row.size(); col++) {
			output.SetValue(col, count, row[col]);
		}
		state.buffer.pop_front();
		count++;
		state.total_emitted++;
		if (DatadogLogsMaxRowsReached(bind.max_rows, state.total_emitted)) {
			state.finished = true;
			state.buffer.clear();
			break;
		}
	}

	output.SetCardinality(count);
}

static InsertionOrderPreservingMap<string> DatadogTracesToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind = input.bind_data->Cast<DatadogTracesBindData>();
	result["Function"] = input.table_function.name;
	result["Datadog Query"] = bind.query;
	result["Datadog From"] = bind.from;
	result["Datadog To"] = bind.to;
	result["Datadog Sort"] = bind.sort;
	result["Datadog Page Size"] = std::to_string(bind.page_size);
	result["Datadog Max Rows"] = std::to_string(bind.max_rows);
	result["Datadog Retries"] = std::to_string(bind.client.retries);
	result["Datadog Timeout"] = std::to_string(bind.client.timeout_seconds);
	return result;
}

void RegisterDatadogTracesFunction(ExtensionLoader &loader) {
	TableFunction function("read_datadog_traces", {}, DatadogTracesScan, DatadogTracesBind, DatadogTracesInitGlobal);
	function.named_parameters["query"] = LogicalType::VARCHAR;
	function.named_parameters["from"] = LogicalType::VARCHAR;
	function.named_parameters["to"] = LogicalType::VARCHAR;
	function.named_parameters["sort"] = LogicalType::VARCHAR;
	function.named_parameters["page_size"] = LogicalType::BIGINT;
	function.named_parameters["max_rows"] = LogicalType::BIGINT;
	function.named_parameters["retries"] = LogicalType::BIGINT;
	function.named_parameters["timeout"] = LogicalType::BIGINT;
	function.named_parameters["secret"] = LogicalType::VARCHAR;
	// Only projected columns are mapped from the API response; the network cost is unchanged, but
	// e.g. a count(*) or a GROUP BY service_name never pays the per-row span_attributes JSON
	// serialization.
	function.projection_pushdown = true;
	function.to_string = DatadogTracesToString;
	loader.RegisterFunction(function);
}

} // namespace duckdb
