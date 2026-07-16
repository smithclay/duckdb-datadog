#include "logs_table.hpp"

#include "datadog_client.hpp"
#include "datadog_json.hpp"
#include "datadog_secret.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

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

void GetDatadogLogsSchema(vector<LogicalType> &types, vector<string> &names) {
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

//! Map one Datadog log event (`data[]` element) to the projected columns of a row.
//! `column_ids[c]` is the source column for output slot c (projection pushdown); only projected
//! columns are computed, so deselected fields — above all the log_attributes JSON serialization,
//! the dominant per-row cost — are skipped entirely for aggregate/triage queries.
static void MapEvent(yyjson_val *item, const vector<column_t> &column_ids, vector<Value> &row) {
	row.assign(column_ids.size(), Value()); // all projected columns NULL by default

	yyjson_val *attributes = yyjson_obj_get(item, "attributes");
	yyjson_val *custom = attributes ? yyjson_obj_get(attributes, "attributes") : nullptr;

	for (idx_t c = 0; c < column_ids.size(); c++) {
		switch (column_ids[c]) {
		case COL_TIME:
		case COL_OBSERVED_TIME: {
			const char *timestamp = GetStr(attributes, "timestamp");
			int64_t nanos;
			if (timestamp && ParseIso8601ToNanos(timestamp, nanos)) {
				row[c] = Value::TIMESTAMPNS(timestamp_ns_t(nanos));
			}
			break;
		}
		// Trace correlation is only present when the log carries it in custom attributes.
		case COL_TRACE_ID: {
			const char *trace_id = GetStr(custom, "trace_id");
			if (trace_id) {
				row[c] = Value(string(trace_id));
			}
			break;
		}
		case COL_SPAN_ID: {
			const char *span_id = GetStr(custom, "span_id");
			if (span_id) {
				row[c] = Value(string(span_id));
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
		case COL_SEVERITY_NUMBER: {
			const char *status = GetStr(attributes, "status");
			if (status) {
				row[c] = Value::INTEGER(StatusToSeverityNumber(status));
			}
			break;
		}
		case COL_SEVERITY_TEXT: {
			const char *status = GetStr(attributes, "status");
			if (status) {
				row[c] = Value(string(status));
			}
			break;
		}
		case COL_BODY: {
			const char *message = GetStr(attributes, "message");
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
		case COL_LOG_ATTRS: {
			if (custom && yyjson_is_obj(custom)) {
				string log_attributes = SerializeVal(custom);
				if (!log_attributes.empty()) {
					row[c] = Value(log_attributes);
				}
			}
			break;
		}
		default:
			// Columns Datadog has no data for (scope_*, event_name, ...) and virtual columns
			// (e.g. the rowid sentinel a bare count(*) projects) stay NULL.
			break;
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
	int64_t page_size = 1000; // rows per API request (Datadog max)
	int64_t max_rows = 0;     // 0 = unlimited
	vector<string> indexes;   // empty = search all indexes (existing table-function behavior)
	DatadogFilterPushdown pushdown;
	TableCatalogEntry *table = nullptr;
	DatadogClient client;
};

struct DatadogLogsGlobalState : public GlobalTableFunctionState {
	//! Source column for each output slot (projection pushdown); may contain virtual-column
	//! sentinels (e.g. rowid for a bare count(*)), which MapEvent leaves NULL.
	vector<column_t> column_ids;
	//! Rows (projected columns only) parsed and waiting to be emitted.
	std::deque<vector<Value>> buffer;
	//! Next-page cursor from meta.page.after ("" = request the first page).
	string cursor;
	//! Effective request values after conservative WHERE pushdown is resolved.
	string request_query;
	string request_from;
	string request_to;
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
static void FetchNextPage(ClientContext &context, const DatadogLogsBindData &bind, DatadogLogsGlobalState &state) {
	string body = BuildDatadogLogsSearchBody(state.request_query, state.request_from, state.request_to, bind.page_size,
	                                         state.cursor, bind.indexes);
	string response = bind.client.SearchLogs(context, body);

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
			MapEvent(item, state.column_ids, row);
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
		} else if (key == "page_size") {
			result->page_size = param.second.GetValue<int64_t>();
		} else if (key == "max_rows") {
			result->max_rows = param.second.GetValue<int64_t>();
		} else if (key == "retries") {
			auto retries = param.second.GetValue<int64_t>();
			if (retries < 0) {
				throw InvalidInputException("read_datadog_logs: retries must be >= 0 (0 disables retrying)");
			}
			result->client.retries = static_cast<uint64_t>(retries);
		} else if (key == "timeout") {
			auto timeout = param.second.GetValue<int64_t>();
			if (timeout < 1) {
				throw InvalidInputException("read_datadog_logs: timeout must be >= 1 (seconds)");
			}
			result->client.timeout_seconds = static_cast<uint64_t>(timeout);
		} else if (key == "secret") {
			secret_name = param.second.ToString();
		}
	}

	if (result->page_size < 1 || result->page_size > 1000) {
		throw InvalidInputException("read_datadog_logs: page_size must be between 1 and 1000 (the Datadog API limit)");
	}
	if (result->max_rows < 0) {
		throw InvalidInputException("read_datadog_logs: max_rows must be >= 0 (0 means unlimited)");
	}

	auto credentials = GetDatadogCredentials(context, secret_name);
	result->client.site = credentials.site;
	result->client.api_key = credentials.api_key;
	result->client.app_key = credentials.app_key;

	GetDatadogLogsSchema(return_types, names);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DatadogLogsInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto state = make_uniq<DatadogLogsGlobalState>();
	auto &bind = input.bind_data->Cast<DatadogLogsBindData>();
	state->column_ids = input.column_ids;
	auto resolved = ResolveDatadogSearch(bind.query, bind.from, bind.to, bind.pushdown);
	state->request_query = std::move(resolved.query);
	state->request_from = std::move(resolved.from);
	state->request_to = std::move(resolved.to);
	state->finished = resolved.empty;
	return std::move(state);
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
		FetchNextPage(context, bind, state);
	}

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && !state.buffer.empty()) {
		if (bind.max_rows > 0 && state.total_emitted >= static_cast<idx_t>(bind.max_rows)) {
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
	}

	output.SetCardinality(count);
}

static bool IsSafeDatadogFacetValue(const string &value) {
	if (value.empty()) {
		return false;
	}
	for (idx_t i = 0; i < value.size(); i++) {
		auto ch = value[i];
		auto c = static_cast<unsigned char>(ch);
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.' ||
		    (c == '-' && i > 0)) {
			continue;
		}
		return false;
	}
	return true;
}

static ExpressionType ReverseComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_LESSTHAN:
		return ExpressionType::COMPARE_GREATERTHAN;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	case ExpressionType::COMPARE_GREATERTHAN:
		return ExpressionType::COMPARE_LESSTHAN;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	default:
		return type;
	}
}

static int64_t FloorNanosecondsToMilliseconds(int64_t nanos) {
	constexpr int64_t NANOS_PER_MILLISECOND = 1000 * 1000;
	auto millis = nanos / NANOS_PER_MILLISECOND;
	if (nanos % NANOS_PER_MILLISECOND < 0) {
		millis--;
	}
	return millis;
}

static int64_t CeilNanosecondsToMilliseconds(int64_t nanos) {
	constexpr int64_t NANOS_PER_MILLISECOND = 1000 * 1000;
	auto millis = nanos / NANOS_PER_MILLISECOND;
	if (nanos % NANOS_PER_MILLISECOND > 0) {
		millis++;
	}
	return millis;
}

static void AddDatadogQueryTerm(DatadogFilterPushdown &pushdown, string term) {
	for (const auto &existing : pushdown.query_terms) {
		if (existing == term) {
			return;
		}
	}
	pushdown.query_terms.push_back(std::move(term));
}

static void TryPushDatadogComparison(const LogicalGet &get, const BoundComparisonExpression &comparison,
                                     DatadogFilterPushdown &pushdown) {
	const BoundColumnRefExpression *column = nullptr;
	const BoundConstantExpression *constant = nullptr;
	auto comparison_type = comparison.type;

	if (comparison.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
	    comparison.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column = &comparison.left->Cast<BoundColumnRefExpression>();
		constant = &comparison.right->Cast<BoundConstantExpression>();
	} else if (comparison.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
	           comparison.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column = &comparison.right->Cast<BoundColumnRefExpression>();
		constant = &comparison.left->Cast<BoundConstantExpression>();
		comparison_type = ReverseComparison(comparison_type);
	} else {
		return;
	}

	const auto &column_ids = get.GetColumnIds();
	if (column->depth != 0 || column->binding.table_index != get.table_index ||
	    column->binding.column_index >= column_ids.size() || constant->value.IsNull()) {
		return;
	}

	auto source_column = column_ids[column->binding.column_index].GetPrimaryIndex();
	if (source_column >= get.names.size()) {
		return;
	}
	const auto &column_name = get.names[source_column];
	if ((column_name == "service_name" || column_name == "severity_text") &&
	    comparison_type == ExpressionType::COMPARE_EQUAL && constant->value.type().id() == LogicalTypeId::VARCHAR) {
		auto value = constant->value.GetValue<string>();
		if (!IsSafeDatadogFacetValue(value)) {
			return;
		}
		AddDatadogQueryTerm(pushdown, (column_name == "service_name" ? "service:" : "status:") + value);
		return;
	}

	if (column_name != "time_unix_nano" || constant->value.type().id() != LogicalTypeId::TIMESTAMP_NS) {
		return;
	}
	auto nanos = constant->value.GetValue<timestamp_ns_t>().value;
	switch (comparison_type) {
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		auto millis = FloorNanosecondsToMilliseconds(nanos);
		if (!pushdown.has_lower_bound_ms || millis > pushdown.lower_bound_ms) {
			pushdown.has_lower_bound_ms = true;
			pushdown.lower_bound_ms = millis;
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		auto millis = CeilNanosecondsToMilliseconds(nanos);
		if (!pushdown.has_upper_bound_ms || millis < pushdown.upper_bound_ms) {
			pushdown.has_upper_bound_ms = true;
			pushdown.upper_bound_ms = millis;
		}
		break;
	}
	default:
		break;
	}
}

static void TryPushDatadogExpression(const LogicalGet &get, const Expression &expression,
                                     DatadogFilterPushdown &pushdown) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		TryPushDatadogComparison(get, expression.Cast<BoundComparisonExpression>(), pushdown);
		return;
	}
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_CONJUNCTION ||
	    expression.type != ExpressionType::CONJUNCTION_AND) {
		return;
	}
	for (const auto &child : expression.Cast<BoundConjunctionExpression>().children) {
		TryPushDatadogExpression(get, *child, pushdown);
	}
}

static void DatadogLogsPushdownComplexFilter(ClientContext &, LogicalGet &get, FunctionData *bind_data,
                                             vector<unique_ptr<Expression>> &filters) {
	auto &bind = bind_data->Cast<DatadogLogsBindData>();
	bind.pushdown = DatadogFilterPushdown();
	// max_rows caps the unfiltered table-function input. Moving a WHERE predicate below that cap
	// changes which rows participate, so no remote filters are valid for a capped scan.
	if (bind.max_rows > 0) {
		return;
	}
	for (const auto &filter : filters) {
		TryPushDatadogExpression(get, *filter, bind.pushdown);
	}
	// Deliberately leave every expression in `filters`. DuckDB retains a residual
	// filter above the scan, guaranteeing exact SQL semantics even if Datadog's
	// search behavior or timestamp precision is broader than DuckDB's.
}

static InsertionOrderPreservingMap<string> DatadogLogsToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind = input.bind_data->Cast<DatadogLogsBindData>();
	auto resolved = ResolveDatadogSearch(bind.query, bind.from, bind.to, bind.pushdown);
	result["Function"] = input.table_function.name;
	result["Datadog Query"] = resolved.query;
	if (resolved.from != bind.from) {
		result["Datadog From"] = resolved.from;
	}
	if (resolved.to != bind.to) {
		result["Datadog To"] = resolved.to;
	}
	return result;
}

void RegisterDatadogLogsFunction(ExtensionLoader &loader) {
	TableFunction function("read_datadog_logs", {}, DatadogLogsScan, DatadogLogsBind, DatadogLogsInitGlobal);
	function.named_parameters["query"] = LogicalType::VARCHAR;
	function.named_parameters["from"] = LogicalType::VARCHAR;
	function.named_parameters["to"] = LogicalType::VARCHAR;
	function.named_parameters["page_size"] = LogicalType::BIGINT;
	function.named_parameters["max_rows"] = LogicalType::BIGINT;
	function.named_parameters["retries"] = LogicalType::BIGINT;
	function.named_parameters["timeout"] = LogicalType::BIGINT;
	function.named_parameters["secret"] = LogicalType::VARCHAR;
	// Only projected columns are mapped from the API response; the network cost is unchanged
	// (the search API has no response field selection), but e.g. a count(*) or a GROUP BY
	// service_name never pays the per-row log_attributes JSON serialization.
	function.projection_pushdown = true;
	function.pushdown_complex_filter = DatadogLogsPushdownComplexFilter;
	function.filter_pushdown = false; // translated predicates always remain as DuckDB residual filters
	function.to_string = DatadogLogsToString;
	loader.RegisterFunction(function);
}

static BindInfo DatadogLogsGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &data = bind_data->Cast<DatadogLogsBindData>();
	D_ASSERT(data.table);
	return BindInfo(*data.table);
}

TableFunction GetDatadogLogsTableScan(ClientContext &context, TableCatalogEntry &table, const string &secret_name,
                                      const string &index_name, unique_ptr<FunctionData> &bind_data) {
	auto result = make_uniq<DatadogLogsBindData>();
	result->indexes.push_back(index_name);
	result->table = &table;
	auto credentials = GetDatadogCredentials(context, secret_name);
	result->client.site = credentials.site;
	result->client.api_key = credentials.api_key;
	result->client.app_key = credentials.app_key;
	bind_data = std::move(result);

	TableFunction function("datadog_logs_scan", {}, DatadogLogsScan, nullptr, DatadogLogsInitGlobal);
	function.projection_pushdown = true;
	function.pushdown_complex_filter = DatadogLogsPushdownComplexFilter;
	function.filter_pushdown = false; // translated predicates always remain as DuckDB residual filters
	function.to_string = DatadogLogsToString;
	function.get_bind_info = DatadogLogsGetBindInfo;
	return function;
}

} // namespace duckdb
