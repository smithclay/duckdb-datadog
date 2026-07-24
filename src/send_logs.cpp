#include "send_logs.hpp"

#include "datadog_client.hpp"
#include "datadog_json.hpp"
#include "datadog_secret.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <unordered_map>

namespace duckdb {

//! The log intake API accepts at most 1000 logs per request; larger chunks are split into batches.
static constexpr idx_t DATADOG_INTAKE_MAX_BATCH = 1000;

//! Index of each recognized OTLP struct field within the argument struct, or -1 when the input
//! struct does not carry that field. Resolved once at bind time from the struct's child names.
struct SendLogsFieldIndices {
	int32_t message = -1;             // body | message -> Datadog `message`
	int32_t service = -1;             // service_name | service -> `service`
	int32_t status = -1;              // severity_text | severity | status -> `status`
	int32_t severity_number = -1;     // severity_number -> `status` fallback
	int32_t hostname = -1;            // hostname | host -> `hostname`
	int32_t ddsource = -1;            // ddsource -> `ddsource`
	int32_t timestamp = -1;           // time_unix_nano | timestamp -> `timestamp`
	int32_t observed_timestamp = -1;  // observed_time_unix_nano -> `timestamp` fallback
	int32_t trace_id = -1;            // trace_id -> custom attribute
	int32_t span_id = -1;             // span_id -> custom attribute
	int32_t ddtags = -1;              // ddtags -> `ddtags`
	int32_t resource_attributes = -1; // resource_attributes (JSON) -> host/ddtags + nested block
	int32_t log_attributes = -1;      // log_attributes (JSON) -> merged top-level attributes
};

struct DatadogSendLogsBindData : public FunctionData {
	SendLogsFieldIndices fields;
	DatadogClient client;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<DatadogSendLogsBindData>();
		result->fields = fields;
		result->client.site = client.site;
		result->client.api_key = client.api_key;
		result->client.app_key = client.app_key;
		result->client.retries = client.retries;
		result->client.timeout_seconds = client.timeout_seconds;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DatadogSendLogsBindData>();
		return client.site == other.client.site && client.api_key == other.client.api_key &&
		       client.app_key == other.client.app_key;
	}
};

//! Return the child index for the first of `names` present in the struct, or -1 if none match.
static int32_t PickField(const std::unordered_map<string, idx_t> &by_name, const vector<const char *> &names) {
	for (const auto *name : names) {
		auto it = by_name.find(name);
		if (it != by_name.end()) {
			return static_cast<int32_t>(it->second);
		}
	}
	return -1;
}

//! Map OTLP severity numbers (1-24) to a Datadog status string; used only when severity_text is
//! absent. Ranges follow the OTLP SeverityNumber spec. Returns "" for unspecified/out-of-range.
static string SeverityNumberToStatus(int32_t severity_number) {
	if (severity_number >= 1 && severity_number <= 4) {
		return "trace";
	}
	if (severity_number >= 5 && severity_number <= 8) {
		return "debug";
	}
	if (severity_number >= 9 && severity_number <= 12) {
		return "info";
	}
	if (severity_number >= 13 && severity_number <= 16) {
		return "warn";
	}
	if (severity_number >= 17 && severity_number <= 20) {
		return "error";
	}
	if (severity_number >= 21 && severity_number <= 24) {
		return "critical";
	}
	return "";
}

static unique_ptr<FunctionData> DatadogSendLogsBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty() || arguments[0]->return_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("send_datadog_logs: the first argument must be a STRUCT of OTLP-shaped log columns "
		                      "(e.g. send_datadog_logs(logs) where 'logs' is the source table)");
	}

	auto result = make_uniq<DatadogSendLogsBindData>();

	const auto &struct_type = arguments[0]->return_type;
	auto child_count = StructType::GetChildCount(struct_type);
	std::unordered_map<string, idx_t> by_name;
	for (idx_t i = 0; i < child_count; i++) {
		by_name.emplace(StringUtil::Lower(StructType::GetChildName(struct_type, i)), i);
	}

	auto &fields = result->fields;
	fields.message = PickField(by_name, {"body", "message"});
	fields.service = PickField(by_name, {"service_name", "service"});
	fields.status = PickField(by_name, {"severity_text", "severity", "status"});
	fields.severity_number = PickField(by_name, {"severity_number"});
	fields.hostname = PickField(by_name, {"hostname", "host"});
	fields.ddsource = PickField(by_name, {"ddsource"});
	fields.timestamp = PickField(by_name, {"time_unix_nano", "timestamp"});
	fields.observed_timestamp = PickField(by_name, {"observed_time_unix_nano"});
	fields.trace_id = PickField(by_name, {"trace_id"});
	fields.span_id = PickField(by_name, {"span_id"});
	fields.ddtags = PickField(by_name, {"ddtags"});
	fields.resource_attributes = PickField(by_name, {"resource_attributes"});
	fields.log_attributes = PickField(by_name, {"log_attributes"});

	// Optional second argument: an explicit secret name (must be a constant).
	string secret_name;
	if (arguments.size() == 2) {
		if (!arguments[1]->IsFoldable()) {
			throw BinderException("send_datadog_logs: the secret name must be a constant string");
		}
		auto secret_value = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (!secret_value.IsNull()) {
			secret_name = secret_value.ToString();
		}
	}

	auto credentials = GetDatadogCredentials(context, secret_name);
	result->client.site = credentials.site;
	result->client.api_key = credentials.api_key;
	result->client.app_key = credentials.app_key;

	bound_function.return_type = LogicalType::VARCHAR;
	return std::move(result);
}

//! Read a struct child as a string; returns "" when the field is absent or NULL.
static string ReadStringField(vector<unique_ptr<Vector>> &children, int32_t index, idx_t row) {
	if (index < 0) {
		return string();
	}
	auto value = children[index]->GetValue(row);
	if (value.IsNull()) {
		return string();
	}
	return value.ToString();
}

//! Read a struct child as epoch milliseconds. Accepts any DuckDB timestamp/date type by casting to
//! nanosecond precision. Returns false when the field is absent, NULL, or not time-convertible.
static bool ReadTimestampMs(vector<unique_ptr<Vector>> &children, int32_t index, idx_t row, int64_t &out_ms) {
	if (index < 0) {
		return false;
	}
	auto value = children[index]->GetValue(row);
	if (value.IsNull()) {
		return false;
	}
	Value nanos;
	if (!value.DefaultTryCastAs(LogicalType::TIMESTAMP_NS, nanos, nullptr) || nanos.IsNull()) {
		return false;
	}
	out_ms = nanos.GetValue<timestamp_ns_t>().value / 1000000; // ns -> ms
	return true;
}

static DatadogIntakeLog BuildIntakeLogFromRow(const SendLogsFieldIndices &fields, vector<unique_ptr<Vector>> &children,
                                              idx_t row) {
	DatadogIntakeLog log;
	log.message = ReadStringField(children, fields.message, row);
	log.service = ReadStringField(children, fields.service, row);
	log.status = ReadStringField(children, fields.status, row);
	log.hostname = ReadStringField(children, fields.hostname, row);
	log.ddsource = ReadStringField(children, fields.ddsource, row);
	log.ddtags = ReadStringField(children, fields.ddtags, row);
	log.trace_id = ReadStringField(children, fields.trace_id, row);
	log.span_id = ReadStringField(children, fields.span_id, row);
	log.log_attributes_json = ReadStringField(children, fields.log_attributes, row);
	log.resource_attributes_json = ReadStringField(children, fields.resource_attributes, row);

	if (log.status.empty() && fields.severity_number >= 0) {
		auto severity = children[fields.severity_number]->GetValue(row);
		if (!severity.IsNull()) {
			Value as_int;
			if (severity.DefaultTryCastAs(LogicalType::INTEGER, as_int, nullptr) && !as_int.IsNull()) {
				log.status = SeverityNumberToStatus(as_int.GetValue<int32_t>());
			}
		}
	}
	// Default ddsource so logs sent by this extension are easy to identify in Datadog.
	if (log.ddsource.empty()) {
		log.ddsource = "duckdb";
	}

	int64_t timestamp_ms;
	if (ReadTimestampMs(children, fields.timestamp, row, timestamp_ms) ||
	    ReadTimestampMs(children, fields.observed_timestamp, row, timestamp_ms)) {
		log.has_timestamp_ms = true;
		log.timestamp_ms = timestamp_ms;
	}
	return log;
}

static void DatadogSendLogsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind = func_expr.bind_info->Cast<DatadogSendLogsBindData>();
	auto &context = state.GetContext();
	idx_t count = args.size();

	// Flatten so struct children are directly row-addressable regardless of the input vector type.
	auto &input = args.data[0];
	input.Flatten(count);
	auto &children = StructVector::GetEntries(input);
	auto &input_validity = FlatVector::Validity(input);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);

	// Collect the non-NULL rows into intake logs, remembering which output row each came from.
	vector<DatadogIntakeLog> logs;
	vector<idx_t> source_rows;
	logs.reserve(count);
	source_rows.reserve(count);
	for (idx_t row = 0; row < count; row++) {
		if (!input_validity.RowIsValid(row)) {
			result_validity.SetInvalid(row); // NULL log struct -> NULL result, nothing sent
			continue;
		}
		logs.push_back(BuildIntakeLogFromRow(bind.fields, children, row));
		source_rows.push_back(row);
	}

	// Send in batches of at most DATADOG_INTAKE_MAX_BATCH logs each.
	for (idx_t offset = 0; offset < logs.size(); offset += DATADOG_INTAKE_MAX_BATCH) {
		idx_t batch_end = MinValue<idx_t>(offset + DATADOG_INTAKE_MAX_BATCH, logs.size());
		vector<DatadogIntakeLog> batch(logs.begin() + offset, logs.begin() + batch_end);
		string body = BuildDatadogIntakeBody(batch);
		bind.client.SendLogs(context, body); // throws on non-2xx / exhausted retries
		for (idx_t i = offset; i < batch_end; i++) {
			result.SetValue(source_rows[i], Value("ok"));
		}
	}
}

void RegisterDatadogSendLogsFunction(ExtensionLoader &loader) {
	ScalarFunctionSet set("send_datadog_logs");
	for (auto &arguments : vector<vector<LogicalType>> {{LogicalType::ANY}, {LogicalType::ANY, LogicalType::VARCHAR}}) {
		ScalarFunction function(arguments, LogicalType::VARCHAR, DatadogSendLogsFunction, DatadogSendLogsBind);
		// Sending a log is a side effect: never constant-fold, cache, or eliminate a call.
		function.SetStability(FunctionStability::VOLATILE);
		// Handle NULLs ourselves so a NULL struct maps to a NULL result without an intake request.
		function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		set.AddFunction(function);
	}
	loader.RegisterFunction(set);
}

} // namespace duckdb
