#include "send_metrics.hpp"

#include "datadog_client.hpp"
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

#include "yyjson.hpp"

#include <chrono>
#include <cstdlib>
#include <unordered_map>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {

//! The series intake accepts at most 5 MB uncompressed. Cap batches well under it, and cap the
//! series count so one oversized chunk cannot build an unbounded payload in memory.
constexpr idx_t DATADOG_SERIES_MAX_BATCH = 1000;
constexpr idx_t DATADOG_SERIES_MAX_BATCH_BYTES = 4 * 1024 * 1024;

//! Datadog metric types, from the v2 intake schema.
constexpr int64_t DATADOG_TYPE_GAUGE = 3;

struct SendMetricsFieldIndices {
	int32_t name = -1;       // name | metric | metric_name
	int32_t value = -1;      // double_value | value
	int32_t timestamp = -1;  // time_unix_nano | timestamp
	int32_t unit = -1;       // unit
	int32_t service = -1;    // service_name
	int32_t attributes = -1; // metric_attributes | attributes
	int32_t ddtags = -1;     // ddtags
	bool timestamp_is_nanos = false;

	bool operator==(const SendMetricsFieldIndices &o) const {
		return name == o.name && value == o.value && timestamp == o.timestamp && unit == o.unit &&
		       service == o.service && attributes == o.attributes && ddtags == o.ddtags &&
		       timestamp_is_nanos == o.timestamp_is_nanos;
	}
};

struct DatadogSendMetricsBindData : public FunctionData {
	SendMetricsFieldIndices fields;
	DatadogClient client;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<DatadogSendMetricsBindData>();
		result->fields = fields;
		client.CopyConfigTo(result->client);
		return std::move(result);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DatadogSendMetricsBindData>();
		return fields == other.fields && client.site == other.client.site && client.api_key == other.client.api_key;
	}
};

struct MetricPoint {
	idx_t source_row = 0;
	string name;
	string unit;
	double value = 0;
	int64_t timestamp_seconds = 0;
	vector<string> tags;
};

int32_t PickField(const std::unordered_map<string, idx_t> &by_name, const vector<const char *> &names) {
	for (const auto *name : names) {
		auto entry = by_name.find(name);
		if (entry != by_name.end()) {
			return static_cast<int32_t>(entry->second);
		}
	}
	return -1;
}

unique_ptr<FunctionData> DatadogSendMetricsBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty() || arguments[0]->return_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("send_datadog_metrics: the first argument must be a STRUCT of OTLP-shaped gauge columns "
		                      "(e.g. send_datadog_metrics(metrics) where 'metrics' is the source table)");
	}

	auto result = make_uniq<DatadogSendMetricsBindData>();
	const auto &struct_type = arguments[0]->return_type;
	std::unordered_map<string, idx_t> by_name;
	for (idx_t index = 0; index < StructType::GetChildCount(struct_type); index++) {
		by_name.emplace(StringUtil::Lower(StructType::GetChildName(struct_type, index)), index);
	}
	result->fields.name = PickField(by_name, {"name", "metric", "metric_name"});
	result->fields.value = PickField(by_name, {"double_value", "value"});
	result->fields.timestamp = PickField(by_name, {"time_unix_nano", "timestamp"});
	result->fields.timestamp_is_nanos = by_name.count("time_unix_nano") > 0;
	result->fields.unit = PickField(by_name, {"unit"});
	result->fields.service = PickField(by_name, {"service_name"});
	result->fields.attributes = PickField(by_name, {"metric_attributes", "attributes"});
	result->fields.ddtags = PickField(by_name, {"ddtags"});
	if (result->fields.name < 0) {
		throw BinderException("send_datadog_metrics: the input struct needs a 'name' or 'metric' column");
	}
	if (result->fields.value < 0) {
		throw BinderException("send_datadog_metrics: the input struct needs a 'double_value' or 'value' column");
	}

	string secret_name;
	if (arguments.size() >= 2) {
		if (!arguments[1]->IsFoldable()) {
			throw BinderException("send_datadog_metrics: secret name must be a constant string");
		}
		auto value = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (!value.IsNull()) {
			secret_name = value.ToString();
		}
	}
	auto credentials = GetDatadogCredentials(context, secret_name);
	result->client.site = credentials.site;
	result->client.api_key = credentials.api_key;
	result->client.app_key = credentials.app_key;
	result->client.intake_url = credentials.intake_url;
	bound_function.return_type = LogicalType::VARCHAR;
	return std::move(result);
}

string ReadStringField(vector<unique_ptr<Vector>> &children, int32_t index, idx_t row) {
	if (index < 0) {
		return string();
	}
	auto value = children[index]->GetValue(row);
	return value.IsNull() ? string() : value.ToString();
}

bool ReadDoubleField(vector<unique_ptr<Vector>> &children, int32_t index, idx_t row, double &out) {
	if (index < 0) {
		return false;
	}
	auto value = children[index]->GetValue(row);
	if (value.IsNull()) {
		return false;
	}
	Value number;
	if (!value.DefaultTryCastAs(LogicalType::DOUBLE, number, nullptr) || number.IsNull()) {
		return false;
	}
	out = number.GetValue<double>();
	return true;
}

bool ReadTimestampSeconds(vector<unique_ptr<Vector>> &children, int32_t index, bool is_nanos, idx_t row,
                          int64_t &timestamp_seconds) {
	if (index < 0) {
		return false;
	}
	auto value = children[index]->GetValue(row);
	if (value.IsNull()) {
		return false;
	}
	if (value.type().IsIntegral()) {
		Value integer;
		if (!value.DefaultTryCastAs(LogicalType::BIGINT, integer, nullptr) || integer.IsNull()) {
			return false;
		}
		auto raw = integer.GetValue<int64_t>();
		timestamp_seconds = is_nanos ? raw / 1000000000 : raw / 1000;
		return true;
	}
	Value nanos;
	if (!value.DefaultTryCastAs(LogicalType::TIMESTAMP_NS, nanos, nullptr) || nanos.IsNull()) {
		return false;
	}
	timestamp_seconds = nanos.GetValue<timestamp_ns_t>().value / 1000000000;
	return true;
}

//! service_name becomes a `service:` tag, and every metric_attributes entry a `key:value` tag --
//! Datadog's only per-point dimension mechanism.
void CollectTags(const DatadogSendMetricsBindData &bind, vector<unique_ptr<Vector>> &children, idx_t row,
                 MetricPoint &point) {
	auto service = ReadStringField(children, bind.fields.service, row);
	if (!service.empty()) {
		point.tags.push_back("service:" + service);
	}
	auto ddtags = ReadStringField(children, bind.fields.ddtags, row);
	if (!ddtags.empty()) {
		for (auto &tag : StringUtil::Split(ddtags, ',')) {
			StringUtil::Trim(tag);
			if (!tag.empty()) {
				point.tags.push_back(tag);
			}
		}
	}
	auto attributes = ReadStringField(children, bind.fields.attributes, row);
	if (attributes.empty()) {
		return;
	}
	auto document = yyjson_read(attributes.c_str(), attributes.size(), 0);
	if (!document) {
		throw IOException("send_datadog_metrics: metric_attributes is not valid JSON");
	}
	auto root = yyjson_doc_get_root(document);
	if (root && yyjson_is_obj(root)) {
		size_t index, max;
		yyjson_val *key, *child;
		yyjson_obj_foreach(root, index, max, key, child) {
			if (!yyjson_is_str(child)) {
				continue; // Datadog tags are string-valued only.
			}
			point.tags.push_back(string(yyjson_get_str(key)) + ":" + string(yyjson_get_str(child)));
		}
	}
	yyjson_doc_free(document);
}

string BuildSeriesRequest(const MetricPoint *points, idx_t count) {
	auto document = yyjson_mut_doc_new(nullptr);
	auto root = yyjson_mut_obj(document);
	yyjson_mut_doc_set_root(document, root);
	auto series = yyjson_mut_arr(document);
	for (idx_t index = 0; index < count; index++) {
		const auto &point = points[index];
		auto entry = yyjson_mut_obj(document);
		yyjson_mut_obj_add_strncpy(document, entry, "metric", point.name.c_str(), point.name.size());
		yyjson_mut_obj_add_sint(document, entry, "type", DATADOG_TYPE_GAUGE);
		if (!point.unit.empty()) {
			yyjson_mut_obj_add_strncpy(document, entry, "unit", point.unit.c_str(), point.unit.size());
		}
		auto samples = yyjson_mut_arr(document);
		auto sample = yyjson_mut_obj(document);
		yyjson_mut_obj_add_sint(document, sample, "timestamp", point.timestamp_seconds);
		yyjson_mut_obj_add_real(document, sample, "value", point.value);
		yyjson_mut_arr_add_val(samples, sample);
		yyjson_mut_obj_add_val(document, entry, "points", samples);
		if (!point.tags.empty()) {
			auto tags = yyjson_mut_arr(document);
			for (const auto &tag : point.tags) {
				yyjson_mut_arr_add_strncpy(document, tags, tag.c_str(), tag.size());
			}
			yyjson_mut_obj_add_val(document, entry, "tags", tags);
		}
		yyjson_mut_arr_add_val(series, entry);
	}
	yyjson_mut_obj_add_val(document, root, "series", series);
	size_t length = 0;
	auto text = yyjson_mut_write(document, 0, &length);
	yyjson_mut_doc_free(document);
	if (!text) {
		throw IOException("send_datadog_metrics: request serialization failed");
	}
	string result(text, length);
	free(text);
	return result;
}

idx_t EstimatePointBytes(const MetricPoint &point) {
	idx_t bytes = 128 + point.name.size() + point.unit.size();
	for (const auto &tag : point.tags) {
		bytes += 8 + tag.size();
	}
	return bytes;
}

void DatadogSendMetricsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &function = state.expr.Cast<BoundFunctionExpression>();
	auto &bind = function.bind_info->Cast<DatadogSendMetricsBindData>();
	auto &context = state.GetContext();
	const auto count = args.size();

	auto &input = args.data[0];
	input.Flatten(count);
	auto &children = StructVector::GetEntries(input);
	auto &input_validity = FlatVector::Validity(input);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);

	const auto now_seconds =
	    std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

	vector<MetricPoint> points;
	points.reserve(count);
	for (idx_t row = 0; row < count; row++) {
		if (!input_validity.RowIsValid(row)) {
			result_validity.SetInvalid(row);
			continue;
		}
		MetricPoint point;
		point.source_row = row;
		point.name = ReadStringField(children, bind.fields.name, row);
		if (point.name.empty()) {
			throw IOException("send_datadog_metrics: metric name must not be empty");
		}
		if (!ReadDoubleField(children, bind.fields.value, row, point.value)) {
			// A gauge with no reading is not a zero; skip it rather than invent a datapoint.
			result_validity.SetInvalid(row);
			continue;
		}
		if (!ReadTimestampSeconds(children, bind.fields.timestamp, bind.fields.timestamp_is_nanos, row,
		                          point.timestamp_seconds)) {
			point.timestamp_seconds = now_seconds;
		}
		point.unit = ReadStringField(children, bind.fields.unit, row);
		CollectTags(bind, children, row, point);
		points.push_back(std::move(point));
	}

	idx_t offset = 0;
	while (offset < points.size()) {
		if (context.interrupted) {
			throw InterruptException();
		}
		idx_t batch = 0;
		idx_t bytes = 0;
		while (offset + batch < points.size() && batch < DATADOG_SERIES_MAX_BATCH) {
			auto next = EstimatePointBytes(points[offset + batch]);
			if (batch > 0 && bytes + next > DATADOG_SERIES_MAX_BATCH_BYTES) {
				break;
			}
			bytes += next;
			batch++;
		}
		auto body = BuildSeriesRequest(points.data() + offset, batch);
		bind.client.SendMetrics(context, body);
		for (idx_t index = offset; index < offset + batch; index++) {
			result.SetValue(points[index].source_row, Value("ok"));
		}
		offset += batch;
	}
}

} // namespace

void RegisterDatadogSendMetricsFunction(ExtensionLoader &loader) {
	ScalarFunctionSet set("send_datadog_metrics");
	for (auto &arguments : vector<vector<LogicalType>> {{LogicalType::ANY}, {LogicalType::ANY, LogicalType::VARCHAR}}) {
		ScalarFunction function(arguments, LogicalType::VARCHAR, DatadogSendMetricsFunction, DatadogSendMetricsBind);
		function.SetStability(FunctionStability::VOLATILE);
		function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		set.AddFunction(function);
	}
	loader.RegisterFunction(set);
}

} // namespace duckdb
