#include "log_stats_table.hpp"

#include "datadog_client.hpp"
#include "datadog_json.hpp"
#include "datadog_secret.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

//! `read_datadog_log_stats` asks the Logs Aggregation API one question and returns its buckets,
//! so a count over millions of logs costs one request instead of a paginated row scan. This is
//! the right tool for count-style questions ("how many error logs per service?", "how many
//! distinct trace ids match this filter?"); use `read_datadog_logs` when you need the rows.

struct DatadogLogStatsBindData : public TableFunctionData {
	string query = "*";
	string from = "now-15m";
	string to = "now";
	string aggregation = "count";
	string metric;
	vector<string> group_by;
	int64_t group_limit = 10;
	DatadogClient client;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<DatadogLogStatsBindData>();
		result->query = query;
		result->from = from;
		result->to = to;
		result->aggregation = aggregation;
		result->metric = metric;
		result->group_by = group_by;
		result->group_limit = group_limit;
		client.CopyConfigTo(result->client);
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DatadogLogStatsBindData>();
		return query == other.query && from == other.from && to == other.to &&
		       aggregation == other.aggregation && metric == other.metric && group_by == other.group_by &&
		       group_limit == other.group_limit && client.site == other.client.site &&
		       client.api_key == other.client.api_key && client.app_key == other.client.app_key;
	}
};

struct DatadogLogStatsGlobalState : public GlobalTableFunctionState {
	bool fetched = false;
	vector<DatadogLogStatsBucket> buckets;
	idx_t emitted = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> DatadogLogStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DatadogLogStatsBindData>();
	string secret_name;
	int64_t retries = 4;
	int64_t timeout_seconds = 60;

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
		} else if (key == "compute") {
			result->aggregation = StringUtil::Lower(param.second.ToString());
		} else if (key == "metric") {
			result->metric = param.second.ToString();
		} else if (key == "group_by") {
			for (const auto &facet : ListValue::GetChildren(param.second)) {
				if (facet.IsNull() || facet.ToString().empty()) {
					throw InvalidInputException("read_datadog_log_stats: group_by facets must be non-empty strings");
				}
				result->group_by.push_back(facet.ToString());
			}
		} else if (key == "group_limit") {
			result->group_limit = param.second.GetValue<int64_t>();
		} else if (key == "retries") {
			retries = param.second.GetValue<int64_t>();
		} else if (key == "timeout") {
			timeout_seconds = param.second.GetValue<int64_t>();
		} else if (key == "secret") {
			secret_name = param.second.ToString();
		}
	}

	if (result->aggregation.empty()) {
		throw InvalidInputException("read_datadog_log_stats: compute must be a Datadog aggregation "
		                            "(count, cardinality, avg, sum, min, max, pc90, ...)");
	}
	// Every aggregation except a plain count aggregates over a facet, which the API takes as
	// `metric`. Failing here names the fix; the API alone reports an opaque 400.
	if (result->aggregation != "count" && result->metric.empty()) {
		throw InvalidInputException("read_datadog_log_stats: compute '%s' requires metric (a facet such as "
		                            "'@duration_ns' or '@trace_id')",
		                            result->aggregation);
	}
	// Datadog caps group-by buckets at 10000 per facet.
	if (result->group_limit < 1 || result->group_limit > 10000) {
		throw InvalidInputException("read_datadog_log_stats: group_limit must be between 1 and 10000");
	}
	if (retries < 0) {
		throw InvalidInputException("read_datadog_log_stats: retries must be >= 0 (0 disables retrying)");
	}
	if (timeout_seconds < 1) {
		throw InvalidInputException("read_datadog_log_stats: timeout must be >= 1 (seconds)");
	}
	result->client.retries = static_cast<uint64_t>(retries);
	result->client.timeout_seconds = static_cast<uint64_t>(timeout_seconds);

	auto credentials = GetDatadogCredentials(context, secret_name);
	result->client.site = credentials.site;
	result->client.api_key = credentials.api_key;
	result->client.app_key = credentials.app_key;

	names = {"by", "value"};
	return_types = {LogicalType::VARCHAR, LogicalType::DOUBLE};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DatadogLogStatsInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<DatadogLogStatsGlobalState>();
}

static void DatadogLogStatsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<DatadogLogStatsBindData>();
	auto &state = data_p.global_state->Cast<DatadogLogStatsGlobalState>();

	if (!state.fetched) {
		state.fetched = true;
		auto body = BuildDatadogLogsAggregateBody(bind.query, bind.from, bind.to, bind.aggregation, bind.metric,
		                                          bind.group_by, bind.group_limit);
		auto response = bind.client.AggregateLogs(context, body);
		state.buckets = ParseDatadogLogsAggregateResponse(response);
	}

	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.buckets.size() - state.emitted);
	for (idx_t i = 0; i < count; i++) {
		auto &bucket = state.buckets[state.emitted + i];
		output.SetValue(0, i, Value(bucket.by_json));
		output.SetValue(1, i, bucket.has_value ? Value::DOUBLE(bucket.value) : Value(LogicalType::DOUBLE));
	}
	state.emitted += count;
	output.SetCardinality(count);
}

void RegisterDatadogLogStatsFunction(ExtensionLoader &loader) {
	TableFunction function("read_datadog_log_stats", {}, DatadogLogStatsScan, DatadogLogStatsBind,
	                       DatadogLogStatsInitGlobal);
	function.named_parameters["query"] = LogicalType::VARCHAR;
	function.named_parameters["from"] = LogicalType::VARCHAR;
	function.named_parameters["to"] = LogicalType::VARCHAR;
	function.named_parameters["compute"] = LogicalType::VARCHAR;
	function.named_parameters["metric"] = LogicalType::VARCHAR;
	function.named_parameters["group_by"] = LogicalType::LIST(LogicalType::VARCHAR);
	function.named_parameters["group_limit"] = LogicalType::BIGINT;
	function.named_parameters["retries"] = LogicalType::BIGINT;
	function.named_parameters["timeout"] = LogicalType::BIGINT;
	function.named_parameters["secret"] = LogicalType::VARCHAR;
	loader.RegisterFunction(function);
}

} // namespace duckdb
