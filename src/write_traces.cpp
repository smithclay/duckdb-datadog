#include "write_traces.hpp"

#include "datadog_client.hpp"
#include "datadog_json.hpp"
#include "datadog_secret.hpp"
#include "datadog_trace_proto.hpp"

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

//! Index of each recognized OTLP struct field within the argument struct, or -1 when the input
//! struct does not carry that field. Resolved once at bind time from the struct's child names.
//! Aliases accept both the duckdb-otlp trace column names and their common Datadog equivalents.
struct WriteTracesFieldIndices {
	int32_t trace_id = -1;            // trace_id (hex string or unsigned integer)
	int32_t span_id = -1;             // span_id
	int32_t parent_span_id = -1;      // parent_span_id | parent_id
	int32_t name = -1;                // name | operation_name -> Datadog `name`
	int32_t resource = -1;            // resource | resource_name -> `resource` (falls back to name)
	int32_t service = -1;             // service_name | service -> `service`
	int32_t type = -1;                // type | span_type -> `type`
	int32_t start = -1;               // start_time_unix_nano | start_time | start -> `start`
	int32_t duration = -1;            // duration_time_unix_nano | duration_ns | duration -> `duration`
	int32_t kind = -1;                // kind -> meta `span.kind` (+ default type for servers)
	int32_t status_code = -1;         // status_code (2 = error) -> `error`
	int32_t status_message = -1;      // status_status_message | status_message -> meta `error.message`
	int32_t trace_state = -1;         // trace_state -> meta `w3c.tracestate`
	int32_t events = -1;              // events_json | events -> meta `events`
	int32_t links = -1;               // links_json | links -> meta `_dd.span_links`
	int32_t span_attributes = -1;     // span_attributes (JSON) -> meta/metrics
	int32_t resource_attributes = -1; // resource_attributes (JSON) -> meta/metrics (never overwrites)

	bool operator==(const WriteTracesFieldIndices &o) const {
		return trace_id == o.trace_id && span_id == o.span_id && parent_span_id == o.parent_span_id && name == o.name &&
		       resource == o.resource && service == o.service && type == o.type && start == o.start &&
		       duration == o.duration && kind == o.kind && status_code == o.status_code &&
		       status_message == o.status_message && trace_state == o.trace_state && events == o.events &&
		       links == o.links && span_attributes == o.span_attributes && resource_attributes == o.resource_attributes;
	}
};

struct DatadogWriteTracesBindData : public FunctionData {
	WriteTracesFieldIndices fields;
	DatadogClient client;
	//! true = protobuf straight to the backend trace intake (TRACE_INTAKE 'direct');
	//! false = JSON to a Datadog Agent (the default).
	bool direct_intake = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<DatadogWriteTracesBindData>();
		result->fields = fields;
		result->direct_intake = direct_intake;
		client.CopyConfigTo(result->client);
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DatadogWriteTracesBindData>();
		return fields == other.fields && direct_intake == other.direct_intake && client.site == other.client.site &&
		       client.api_key == other.client.api_key && client.app_key == other.client.app_key &&
		       client.trace_agent_url == other.client.trace_agent_url;
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

static unique_ptr<FunctionData> DatadogWriteTracesBind(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty() || arguments[0]->return_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("write_datadog_traces: the first argument must be a STRUCT of OTLP-shaped span columns "
		                      "(e.g. write_datadog_traces(t) where 't' is the source table)");
	}

	auto result = make_uniq<DatadogWriteTracesBindData>();

	const auto &struct_type = arguments[0]->return_type;
	auto child_count = StructType::GetChildCount(struct_type);
	std::unordered_map<string, idx_t> by_name;
	for (idx_t i = 0; i < child_count; i++) {
		by_name.emplace(StringUtil::Lower(StructType::GetChildName(struct_type, i)), i);
	}

	auto &fields = result->fields;
	fields.trace_id = PickField(by_name, {"trace_id"});
	fields.span_id = PickField(by_name, {"span_id"});
	fields.parent_span_id = PickField(by_name, {"parent_span_id", "parent_id"});
	fields.name = PickField(by_name, {"name", "operation_name"});
	fields.resource = PickField(by_name, {"resource", "resource_name"});
	fields.service = PickField(by_name, {"service_name", "service"});
	fields.type = PickField(by_name, {"type", "span_type"});
	fields.start = PickField(by_name, {"start_time_unix_nano", "start_time", "start"});
	fields.duration = PickField(by_name, {"duration_time_unix_nano", "duration_ns", "duration"});
	fields.kind = PickField(by_name, {"kind"});
	fields.status_code = PickField(by_name, {"status_code"});
	fields.status_message = PickField(by_name, {"status_status_message", "status_message"});
	fields.trace_state = PickField(by_name, {"trace_state"});
	fields.events = PickField(by_name, {"events_json", "events"});
	fields.links = PickField(by_name, {"links_json", "links"});
	fields.span_attributes = PickField(by_name, {"span_attributes"});
	fields.resource_attributes = PickField(by_name, {"resource_attributes"});

	if (fields.trace_id < 0 || fields.span_id < 0) {
		throw BinderException("write_datadog_traces: the input struct must contain trace_id and span_id columns");
	}

	// Optional second argument: an explicit secret name (must be a constant).
	string secret_name;
	if (arguments.size() == 2) {
		if (!arguments[1]->IsFoldable()) {
			throw BinderException("write_datadog_traces: the secret name must be a constant string");
		}
		auto secret_value = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (!secret_value.IsNull()) {
			secret_name = secret_value.ToString();
		}
	}

	// By default traces go to a (local) Datadog Agent, which needs no credentials, so a completely
	// unconfigured session still works: an in-scope datadog secret merely contributes its
	// trace_agent_url/api_key configuration when present. TRACE_INTAKE 'direct' switches to the
	// backend trace intake, which authenticates with the API key alone.
	DatadogCredentials credentials;
	if (TryGetDatadogCredentials(context, secret_name, credentials)) {
		result->client.site = credentials.site;
		result->client.api_key = credentials.api_key;
		result->client.app_key = credentials.app_key;
		result->client.trace_agent_url = credentials.trace_agent_url;
		auto mode = StringUtil::Lower(credentials.trace_intake);
		if (mode == "direct") {
			if (credentials.api_key.empty()) {
				throw BinderException("write_datadog_traces: TRACE_INTAKE 'direct' sends straight to the Datadog "
				                      "backend and requires API_KEY in the secret");
			}
			result->direct_intake = true;
		} else if (!mode.empty() && mode != "agent") {
			throw BinderException("write_datadog_traces: TRACE_INTAKE must be 'agent' or 'direct' (got '%s')",
			                      credentials.trace_intake);
		}
		if (result->direct_intake && !credentials.trace_agent_url.empty()) {
			throw BinderException(
			    "write_datadog_traces: TRACE_AGENT_URL and TRACE_INTAKE 'direct' are mutually exclusive");
		}
	}

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

//! Read a trace/span identifier: an unsigned integer column is used directly (upper bits empty);
//! any other type is read as text and parsed as OTLP hex. Returns false when absent or invalid.
static bool ReadIdField(vector<unique_ptr<Vector>> &children, int32_t index, idx_t row, uint64_t &low,
                        string &high_hex) {
	if (index < 0) {
		return false;
	}
	auto value = children[index]->GetValue(row);
	if (value.IsNull()) {
		return false;
	}
	if (value.type().IsIntegral()) {
		Value as_uint;
		if (!value.DefaultTryCastAs(LogicalType::UBIGINT, as_uint, nullptr) || as_uint.IsNull()) {
			return false;
		}
		low = as_uint.GetValue<uint64_t>();
		high_hex = string();
		return true;
	}
	return ParseDatadogHexId(value.ToString(), low, high_hex);
}

//! Read a struct child as epoch nanoseconds. Temporal (TIMESTAMP*/DATE/VARCHAR) values are cast to
//! TIMESTAMP_NS; integer values are taken as raw OTLP epoch nanoseconds.
static bool ReadTimestampNs(vector<unique_ptr<Vector>> &children, int32_t index, idx_t row, int64_t &out_ns) {
	if (index < 0) {
		return false;
	}
	auto value = children[index]->GetValue(row);
	if (value.IsNull()) {
		return false;
	}
	if (value.type().IsIntegral()) {
		Value as_big;
		if (!value.DefaultTryCastAs(LogicalType::BIGINT, as_big, nullptr) || as_big.IsNull()) {
			return false;
		}
		out_ns = as_big.GetValue<int64_t>();
		return true;
	}
	Value nanos;
	if (!value.DefaultTryCastAs(LogicalType::TIMESTAMP_NS, nanos, nullptr) || nanos.IsNull()) {
		return false;
	}
	out_ns = nanos.GetValue<timestamp_ns_t>().value;
	return true;
}

//! Read a struct child as a 64-bit integer; returns false when absent, NULL, or not convertible.
static bool ReadIntegerField(vector<unique_ptr<Vector>> &children, int32_t index, idx_t row, int64_t &out) {
	if (index < 0) {
		return false;
	}
	auto value = children[index]->GetValue(row);
	if (value.IsNull()) {
		return false;
	}
	Value as_big;
	if (!value.DefaultTryCastAs(LogicalType::BIGINT, as_big, nullptr) || as_big.IsNull()) {
		return false;
	}
	out = as_big.GetValue<int64_t>();
	return true;
}

//! OTLP span kind (1-5) -> the span.kind tag value Datadog uses.
static const char *SpanKindToString(int64_t kind) {
	switch (kind) {
	case 1:
		return "internal";
	case 2:
		return "server";
	case 3:
		return "client";
	case 4:
		return "producer";
	case 5:
		return "consumer";
	default:
		return nullptr;
	}
}

//! Map one input row to an agent span. Returns false with `reason` set when the row cannot form a
//! valid span (missing/invalid ids or start time) — the row is reported, not silently dropped.
static bool BuildAgentSpanFromRow(const WriteTracesFieldIndices &fields, vector<unique_ptr<Vector>> &children,
                                  idx_t row, DatadogAgentSpan &span, string &reason) {
	if (!ReadIdField(children, fields.trace_id, row, span.trace_id, span.trace_id_high_hex)) {
		reason = "missing or invalid trace_id";
		return false;
	}
	string span_high_hex; // a span id is 64-bit; upper bits of an over-long value are dropped
	if (!ReadIdField(children, fields.span_id, row, span.span_id, span_high_hex)) {
		reason = "missing or invalid span_id";
		return false;
	}
	string parent_high_hex;
	uint64_t parent_id;
	if (ReadIdField(children, fields.parent_span_id, row, parent_id, parent_high_hex) && parent_id != 0) {
		span.has_parent_id = true;
		span.parent_id = parent_id;
	}
	if (!ReadTimestampNs(children, fields.start, row, span.start_ns)) {
		reason = "missing or invalid start_time_unix_nano";
		return false;
	}
	int64_t duration;
	if (ReadIntegerField(children, fields.duration, row, duration) && duration >= 0) {
		span.duration_ns = duration;
	}

	span.service = ReadStringField(children, fields.service, row);
	if (span.service.empty()) {
		span.service = "unknown-service";
	}
	span.name = ReadStringField(children, fields.name, row);
	if (span.name.empty()) {
		span.name = "unnamed_operation";
	}
	span.resource = ReadStringField(children, fields.resource, row);
	if (span.resource.empty()) {
		span.resource = span.name;
	}
	span.type = ReadStringField(children, fields.type, row);

	int64_t kind;
	if (ReadIntegerField(children, fields.kind, row, kind)) {
		const char *kind_text = SpanKindToString(kind);
		if (kind_text) {
			span.meta.emplace_back("span.kind", kind_text);
			// Datadog's span type enum is web/db/cache/custom; a server span renders as "web".
			if (span.type.empty() && kind == 2) {
				span.type = "web";
			}
		}
	}

	int64_t status_code;
	if (ReadIntegerField(children, fields.status_code, row, status_code) && status_code == 2) {
		span.error = true;
	}
	auto status_message = ReadStringField(children, fields.status_message, row);
	if (!status_message.empty()) {
		span.meta.emplace_back("error.message", status_message);
	}
	auto trace_state = ReadStringField(children, fields.trace_state, row);
	if (!trace_state.empty()) {
		span.meta.emplace_back("w3c.tracestate", trace_state);
	}
	auto events = ReadStringField(children, fields.events, row);
	if (!events.empty()) {
		span.meta.emplace_back("events", events);
	}
	auto links = ReadStringField(children, fields.links, row);
	if (!links.empty()) {
		span.meta.emplace_back("_dd.span_links", links);
	}

	span.span_attributes_json = ReadStringField(children, fields.span_attributes, row);
	span.resource_attributes_json = ReadStringField(children, fields.resource_attributes, row);
	return true;
}

static void DatadogWriteTracesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind = func_expr.bind_info->Cast<DatadogWriteTracesBindData>();
	auto &context = state.GetContext();
	idx_t count = args.size();

	// Flatten so struct children are directly row-addressable regardless of the input vector type.
	auto &input = args.data[0];
	input.Flatten(count);
	auto &children = StructVector::GetEntries(input);
	auto &input_validity = FlatVector::Validity(input);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);

	// Collect the valid rows into agent spans, remembering which output row each came from. Rows
	// that cannot form a span report the reason in their result value instead of failing the send.
	vector<DatadogAgentSpan> spans;
	vector<idx_t> source_rows;
	spans.reserve(count);
	source_rows.reserve(count);
	for (idx_t row = 0; row < count; row++) {
		if (!input_validity.RowIsValid(row)) {
			result_validity.SetInvalid(row); // NULL span struct -> NULL result, nothing sent
			continue;
		}
		DatadogAgentSpan span;
		string reason;
		if (!BuildAgentSpanFromRow(bind.fields, children, row, span, reason)) {
			result.SetValue(row, Value("skipped: " + reason));
			continue;
		}
		spans.push_back(std::move(span));
		source_rows.push_back(row);
	}

	if (spans.empty()) {
		return;
	}

	// One request per chunk: spans grouped into traces. A chunk holds at most STANDARD_VECTOR_SIZE
	// spans, which stays far below either endpoint's payload limits. Both bodies are built from the
	// same resolved spans, so agent and direct mode send identical data.
	idx_t trace_count = 0;
	if (bind.direct_intake) {
		string payload = EncodeDatadogAgentPayload(spans, /*hostname=*/"", /*env=*/"", trace_count);
		bind.client.SendTracesDirect(context, payload, trace_count); // throws on non-2xx / exhausted retries
	} else {
		string body = BuildDatadogAgentTracesBody(spans, trace_count);
		bind.client.SendTraces(context, body, trace_count); // throws on non-2xx / exhausted retries
	}
	for (auto row : source_rows) {
		result.SetValue(row, Value("ok"));
	}
}

void RegisterDatadogWriteTracesFunction(ExtensionLoader &loader) {
	ScalarFunctionSet set("write_datadog_traces");
	for (auto &arguments : vector<vector<LogicalType>> {{LogicalType::ANY}, {LogicalType::ANY, LogicalType::VARCHAR}}) {
		ScalarFunction function(arguments, LogicalType::VARCHAR, DatadogWriteTracesFunction, DatadogWriteTracesBind);
		// Sending a span is a side effect: never constant-fold, cache, or eliminate a call.
		function.SetStability(FunctionStability::VOLATILE);
		// Handle NULLs ourselves so a NULL struct maps to a NULL result without an agent request.
		function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		set.AddFunction(function);
	}
	loader.RegisterFunction(set);
}

} // namespace duckdb
