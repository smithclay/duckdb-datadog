#pragma once

#include "duckdb.hpp"

#include <utility>

namespace duckdb {

class ExtensionLoader;
class ClientContext;
class Expression;
class FunctionData;
class LogicalGet;
class TableFunction;
class TableCatalogEntry;
struct DatadogFilterPushdown;

//! Request and scan limits shared by read_datadog_logs and catalog-backed log tables.
//! max_rows = 0 leaves the relation unlimited.
struct DatadogLogsSettings {
	string sort = "timestamp";
	int64_t page_size = 1000;
	int64_t max_rows = 0;
	int64_t retries = 4;
	int64_t timeout_seconds = 60;
};

//! Register the `read_datadog_logs(...)` table function.
void RegisterDatadogLogsFunction(ExtensionLoader &loader);

//! Return the stable 18-column OTLP-shaped output schema used by both public interfaces.
void GetDatadogLogsSchema(vector<LogicalType> &types, vector<string> &names);

//! Create the scan function and already-bound data for one catalog table. Credentials are
//! resolved through GetDatadogCredentials at this table-bind boundary.
TableFunction GetDatadogLogsTableScan(ClientContext &context, TableCatalogEntry &table, const string &secret_name,
                                      const string &index_name, const DatadogLogsSettings &settings,
                                      unique_ptr<FunctionData> &bind_data);

//! Validate the shared settings using an interface-specific error prefix.
void ValidateDatadogLogsSettings(const DatadogLogsSettings &settings, const string &error_prefix);

//! Translate conservative WHERE predicates into a Datadog search pushdown, shared by every
//! search-backed reader. `time_column` names the TIMESTAMP_NS column whose bounds tighten an
//! absolute request window; `facet_columns` maps output columns to Datadog facet keys pushed as
//! equality terms (e.g. {"service_name", "service"}). Callers must leave every expression in
//! `filters` so DuckDB retains the residual filter above the scan.
void CollectDatadogSearchPushdown(const LogicalGet &get, const vector<unique_ptr<Expression>> &filters,
                                  const string &time_column, const vector<std::pair<string, string>> &facet_columns,
                                  DatadogFilterPushdown &pushdown);

} // namespace duckdb
