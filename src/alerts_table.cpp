#include "alerts_table.hpp"

#include "datadog_client.hpp"
#include "datadog_json.hpp"
#include "datadog_secret.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"

#include <deque>

namespace duckdb {
namespace {

static constexpr idx_t COL_MONITOR_ID = 0;
static constexpr idx_t COL_MONITOR_NAME = 1;
static constexpr idx_t COL_GROUP_NAME = 2;
static constexpr idx_t COL_GROUP_TAGS = 3;
static constexpr idx_t COL_STATUS = 4;
static constexpr idx_t COL_LAST_TRIGGERED_AT = 5;
static constexpr idx_t COL_LAST_NODATA_AT = 6;
static constexpr idx_t COLUMN_COUNT = 7;
static constexpr int64_t PAGE_SIZE = 100;

struct DatadogOpenAlertsBindData : public TableFunctionData {
	TableCatalogEntry *table = nullptr;
	DatadogClient client;
};

struct DatadogOpenAlertsGlobalState : public GlobalTableFunctionState {
	vector<column_t> column_ids;
	std::deque<vector<Value>> buffer;
	int64_t page = 0;
	idx_t api_rows_received = 0;
	bool finished = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static bool IsOpenStatus(const DatadogOpenAlertGroup &group) {
	return group.has_status && (group.status == "Alert" || group.status == "Warn" || group.status == "No Data");
}

static Value ToVarcharList(const vector<string> &strings) {
	vector<Value> values;
	values.reserve(strings.size());
	for (const auto &value : strings) {
		values.emplace_back(value);
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(values));
}

static void MapOpenAlert(const DatadogOpenAlertGroup &group, const vector<column_t> &column_ids, vector<Value> &row) {
	row.assign(column_ids.size(), Value());
	for (idx_t output_column = 0; output_column < column_ids.size(); output_column++) {
		switch (column_ids[output_column]) {
		case COL_MONITOR_ID:
			if (group.has_monitor_id) {
				row[output_column] = Value::BIGINT(group.monitor_id);
			}
			break;
		case COL_MONITOR_NAME:
			if (group.has_monitor_name) {
				row[output_column] = Value(group.monitor_name);
			}
			break;
		case COL_GROUP_NAME:
			if (group.has_group) {
				row[output_column] = Value(group.group);
			}
			break;
		case COL_GROUP_TAGS:
			if (group.has_group_tags) {
				row[output_column] = ToVarcharList(group.group_tags);
			}
			break;
		case COL_STATUS:
			if (group.has_status) {
				row[output_column] = Value(group.status);
			}
			break;
		case COL_LAST_TRIGGERED_AT:
			if (group.has_last_triggered_ts && group.last_triggered_ts > 0) {
				row[output_column] = Value::TIMESTAMP(Timestamp::FromEpochSeconds(group.last_triggered_ts));
			}
			break;
		case COL_LAST_NODATA_AT:
			if (group.has_last_nodata_ts && group.last_nodata_ts > 0) {
				row[output_column] = Value::TIMESTAMP(Timestamp::FromEpochSeconds(group.last_nodata_ts));
			}
			break;
		default:
			// Virtual columns such as the count(*) row-id sentinel stay NULL.
			break;
		}
	}
}

static void FetchNextPage(ClientContext &context, const DatadogOpenAlertsBindData &bind,
                          DatadogOpenAlertsGlobalState &state) {
	auto response = bind.client.SearchOpenAlerts(context, state.page, PAGE_SIZE);
	auto page = ParseDatadogOpenAlertsPage(response);
	state.page++;
	state.api_rows_received += page.groups.size();

	for (const auto &group : page.groups) {
		// The remote query supplies this filter, and the local check preserves the table contract if
		// a monitor resolves concurrently with pagination.
		if (!IsOpenStatus(group)) {
			continue;
		}
		vector<Value> row;
		MapOpenAlert(group, state.column_ids, row);
		state.buffer.push_back(std::move(row));
	}

	if (page.groups.empty() ||
	    (page.has_total_count && state.api_rows_received >= static_cast<idx_t>(page.total_count)) ||
	    (!page.has_total_count && page.groups.size() < static_cast<idx_t>(PAGE_SIZE))) {
		state.finished = true;
	}
}

static unique_ptr<GlobalTableFunctionState> DatadogOpenAlertsInitGlobal(ClientContext &,
                                                                        TableFunctionInitInput &input) {
	auto state = make_uniq<DatadogOpenAlertsGlobalState>();
	state->column_ids = input.column_ids;
	return std::move(state);
}

static void DatadogOpenAlertsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<DatadogOpenAlertsBindData>();
	auto &state = data.global_state->Cast<DatadogOpenAlertsGlobalState>();

	while (state.buffer.empty() && !state.finished) {
		FetchNextPage(context, bind, state);
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

static BindInfo DatadogOpenAlertsGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &data = bind_data->Cast<DatadogOpenAlertsBindData>();
	D_ASSERT(data.table);
	return BindInfo(*data.table);
}

static InsertionOrderPreservingMap<string> DatadogOpenAlertsToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind = input.bind_data->Cast<DatadogOpenAlertsBindData>();
	result["Function"] = input.table_function.name;
	result["Datadog Alert States"] = "Alert, Warn, No Data";
	result["Datadog Page Size"] = std::to_string(PAGE_SIZE);
	result["Datadog Retries"] = std::to_string(bind.client.retries);
	result["Datadog Timeout"] = std::to_string(bind.client.timeout_seconds);
	return result;
}

} // namespace

void GetDatadogOpenAlertsSchema(vector<LogicalType> &types, vector<string> &names) {
	names = {"monitor_id", "monitor_name", "group_name", "group_tags", "status", "last_triggered_at", "last_nodata_at"};
	types = {
	    LogicalType::BIGINT,  LogicalType::VARCHAR,   LogicalType::VARCHAR,  LogicalType::LIST(LogicalType::VARCHAR),
	    LogicalType::VARCHAR, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP};
	D_ASSERT(names.size() == COLUMN_COUNT && types.size() == COLUMN_COUNT);
}

TableFunction GetDatadogOpenAlertsTableScan(ClientContext &context, TableCatalogEntry &table, const string &secret_name,
                                            int64_t retries, int64_t timeout_seconds,
                                            unique_ptr<FunctionData> &bind_data) {
	auto result = make_uniq<DatadogOpenAlertsBindData>();
	result->table = &table;
	result->client.retries = static_cast<uint64_t>(retries);
	result->client.timeout_seconds = static_cast<uint64_t>(timeout_seconds);
	auto credentials = GetDatadogCredentials(context, secret_name);
	result->client.site = credentials.site;
	result->client.api_key = credentials.api_key;
	result->client.app_key = credentials.app_key;
	bind_data = std::move(result);

	TableFunction function("datadog_open_alerts_scan", {}, DatadogOpenAlertsScan, nullptr, DatadogOpenAlertsInitGlobal);
	function.projection_pushdown = true;
	function.to_string = DatadogOpenAlertsToString;
	function.get_bind_info = DatadogOpenAlertsGetBindInfo;
	return function;
}

} // namespace duckdb
