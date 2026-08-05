#include "service_dependencies_table.hpp"

#include "datadog_client.hpp"
#include "datadog_json.hpp"
#include "datadog_secret.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <chrono>
#include <deque>

namespace duckdb {
namespace {

static constexpr idx_t COL_PROVIDER = 0;
static constexpr idx_t COL_SOURCE_SERVICE = 1;
static constexpr idx_t COL_TARGET_SERVICE = 2;
static constexpr idx_t COL_ENVIRONMENT = 6;
static constexpr idx_t COL_WINDOW_START = 7;
static constexpr idx_t COL_WINDOW_END = 8;
static constexpr idx_t COLUMN_COUNT = 17;
static constexpr int64_t NANOS_PER_SECOND = 1000LL * 1000LL * 1000LL;

struct DatadogServiceDependenciesBindData : public TableFunctionData {
	DatadogServiceMapSettings settings;
	TableCatalogEntry *table = nullptr;
	DatadogClient client;
};

struct DatadogServiceDependenciesGlobalState : public GlobalTableFunctionState {
	vector<column_t> column_ids;
	std::deque<vector<Value>> buffer;
	DatadogServiceMapWindow window;
	bool fetched = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static void ConfigureClient(ClientContext &context, DatadogServiceDependenciesBindData &result,
                            const string &secret_name) {
	result.client.retries = static_cast<uint64_t>(result.settings.retries);
	result.client.timeout_seconds = static_cast<uint64_t>(result.settings.timeout_seconds);
	auto credentials = GetDatadogCredentials(context, secret_name);
	result.client.site = credentials.site;
	result.client.api_key = credentials.api_key;
	result.client.app_key = credentials.app_key;
}

static unique_ptr<FunctionData> DatadogServiceDependenciesBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	auto result = make_uniq<DatadogServiceDependenciesBindData>();
	string secret_name;
	for (const auto &parameter : input.named_parameters) {
		auto key = StringUtil::Lower(parameter.first);
		if (parameter.second.IsNull()) {
			continue;
		}
		if (key == "env") {
			result->settings.environment = parameter.second.GetValue<string>();
		} else if (key == "from") {
			result->settings.from = parameter.second.GetValue<string>();
		} else if (key == "to") {
			result->settings.to = parameter.second.GetValue<string>();
		} else if (key == "primary_tag") {
			result->settings.primary_tag = parameter.second.GetValue<string>();
		} else if (key == "retries") {
			result->settings.retries = parameter.second.GetValue<int64_t>();
		} else if (key == "timeout") {
			result->settings.timeout_seconds = parameter.second.GetValue<int64_t>();
		} else if (key == "secret") {
			secret_name = parameter.second.GetValue<string>();
		}
	}

	ValidateDatadogServiceMapSettings(result->settings, "read_datadog_service_dependencies", true);
	ConfigureClient(context, *result, secret_name);
	GetDatadogServiceDependenciesSchema(return_types, names);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DatadogServiceDependenciesInitGlobal(ClientContext &,
                                                                                 TableFunctionInitInput &input) {
	auto state = make_uniq<DatadogServiceDependenciesGlobalState>();
	state->column_ids = input.column_ids;
	auto &bind = input.bind_data->Cast<DatadogServiceDependenciesBindData>();
	if (bind.settings.environment.empty()) {
		throw InvalidInputException("Datadog service map is not configured: ATTACH with SERVICE_MAP_ENV 'prod', or use "
		                            "read_datadog_service_dependencies(env => 'prod', ...)");
	}
	auto now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch());
	state->window = ResolveDatadogServiceMapWindow(bind.settings.from, bind.settings.to, now.count());
	return std::move(state);
}

static void MapServiceDependency(const DatadogServiceDependency &dependency,
                                 const DatadogServiceDependenciesBindData &bind,
                                 const DatadogServiceDependenciesGlobalState &state, vector<Value> &row) {
	row.assign(state.column_ids.size(), Value());
	for (idx_t output_column = 0; output_column < state.column_ids.size(); output_column++) {
		switch (state.column_ids[output_column]) {
		case COL_PROVIDER:
			row[output_column] = Value("datadog");
			break;
		case COL_SOURCE_SERVICE:
			row[output_column] = Value(dependency.source_service);
			break;
		case COL_TARGET_SERVICE:
			row[output_column] = Value(dependency.target_service);
			break;
		case COL_ENVIRONMENT:
			row[output_column] = Value(bind.settings.environment);
			break;
		case COL_WINDOW_START:
			row[output_column] =
			    Value::TIMESTAMPNS(timestamp_ns_t(state.window.start_epoch_seconds * NANOS_PER_SECOND));
			break;
		case COL_WINDOW_END:
			row[output_column] = Value::TIMESTAMPNS(timestamp_ns_t(state.window.end_epoch_seconds * NANOS_PER_SECOND));
			break;
		default:
			// Datadog's dependency endpoint provides topology only. Types, statistics, and
			// provider-specific attribute payloads remain SQL NULL in the canonical schema.
			break;
		}
	}
}

static void FetchServiceDependencies(ClientContext &context, const DatadogServiceDependenciesBindData &bind,
                                     DatadogServiceDependenciesGlobalState &state) {
	auto response =
	    bind.client.GetServiceDependencies(context, bind.settings.environment, bind.settings.primary_tag,
	                                       state.window.start_epoch_seconds, state.window.end_epoch_seconds);
	for (const auto &dependency : ParseDatadogServiceDependencies(response)) {
		vector<Value> row;
		MapServiceDependency(dependency, bind, state, row);
		state.buffer.push_back(std::move(row));
	}
	state.fetched = true;
}

static void DatadogServiceDependenciesScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->Cast<DatadogServiceDependenciesBindData>();
	auto &state = input.global_state->Cast<DatadogServiceDependenciesGlobalState>();
	if (!state.fetched) {
		FetchServiceDependencies(context, bind, state);
	}

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && !state.buffer.empty()) {
		auto &row = state.buffer.front();
		for (idx_t column = 0; column < row.size(); column++) {
			output.SetValue(column, count, row[column]);
		}
		state.buffer.pop_front();
		count++;
	}
	output.SetCardinality(count);
}

static InsertionOrderPreservingMap<string> DatadogServiceDependenciesToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind = input.bind_data->Cast<DatadogServiceDependenciesBindData>();
	result["Function"] = input.table_function.name;
	result["Datadog Environment"] = bind.settings.environment;
	result["Datadog From"] = bind.settings.from;
	result["Datadog To"] = bind.settings.to;
	if (!bind.settings.primary_tag.empty()) {
		result["Datadog Primary Tag"] = bind.settings.primary_tag;
	}
	result["Datadog Retries"] = std::to_string(bind.client.retries);
	result["Datadog Timeout"] = std::to_string(bind.client.timeout_seconds);
	return result;
}

static BindInfo DatadogServiceDependenciesGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &data = bind_data->Cast<DatadogServiceDependenciesBindData>();
	D_ASSERT(data.table);
	return BindInfo(*data.table);
}

} // namespace

void ValidateDatadogServiceMapSettings(const DatadogServiceMapSettings &settings, const string &error_prefix,
                                       bool require_environment) {
	if (require_environment && settings.environment.empty()) {
		throw InvalidInputException("%s: environment must not be empty", error_prefix);
	}
	if (settings.from.empty()) {
		throw InvalidInputException("%s: from must not be empty", error_prefix);
	}
	if (settings.to.empty()) {
		throw InvalidInputException("%s: to must not be empty", error_prefix);
	}
	if (settings.retries < 0) {
		throw InvalidInputException("%s: retries must be >= 0 (0 disables retrying)", error_prefix);
	}
	if (settings.timeout_seconds < 1) {
		throw InvalidInputException("%s: timeout must be >= 1 (seconds)", error_prefix);
	}
}

void GetDatadogServiceDependenciesSchema(vector<LogicalType> &types, vector<string> &names) {
	names = {"provider",          "source_service",
	         "target_service",    "source_type",
	         "target_type",       "edge_type",
	         "environment",       "window_start",
	         "window_end",        "request_count",
	         "error_count",       "fault_count",
	         "throttle_count",    "total_response_time_seconds",
	         "source_attributes", "target_attributes",
	         "edge_attributes"};
	types = {LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TIMESTAMP_NS,
	         LogicalType::TIMESTAMP_NS, LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT,
	         LogicalType::BIGINT,       LogicalType::DOUBLE,  LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::VARCHAR};
	D_ASSERT(names.size() == COLUMN_COUNT && types.size() == COLUMN_COUNT);
}

void RegisterDatadogServiceDependenciesFunction(ExtensionLoader &loader) {
	TableFunction function("read_datadog_service_dependencies", {}, DatadogServiceDependenciesScan,
	                       DatadogServiceDependenciesBind, DatadogServiceDependenciesInitGlobal);
	function.named_parameters["env"] = LogicalType::VARCHAR;
	function.named_parameters["from"] = LogicalType::VARCHAR;
	function.named_parameters["to"] = LogicalType::VARCHAR;
	function.named_parameters["primary_tag"] = LogicalType::VARCHAR;
	function.named_parameters["retries"] = LogicalType::BIGINT;
	function.named_parameters["timeout"] = LogicalType::BIGINT;
	function.named_parameters["secret"] = LogicalType::VARCHAR;
	function.projection_pushdown = true;
	function.to_string = DatadogServiceDependenciesToString;
	loader.RegisterFunction(function);
}

TableFunction GetDatadogServiceDependenciesTableScan(ClientContext &context, TableCatalogEntry &table,
                                                     const string &secret_name,
                                                     const DatadogServiceMapSettings &settings,
                                                     unique_ptr<FunctionData> &bind_data) {
	auto result = make_uniq<DatadogServiceDependenciesBindData>();
	result->settings = settings;
	result->table = &table;
	ValidateDatadogServiceMapSettings(result->settings, "Datadog service map catalog", false);
	ConfigureClient(context, *result, secret_name);
	bind_data = std::move(result);

	TableFunction function("datadog_service_dependencies_scan", {}, DatadogServiceDependenciesScan, nullptr,
	                       DatadogServiceDependenciesInitGlobal);
	function.projection_pushdown = true;
	function.to_string = DatadogServiceDependenciesToString;
	function.get_bind_info = DatadogServiceDependenciesGetBindInfo;
	return function;
}

} // namespace duckdb
