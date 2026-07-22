#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;
class ClientContext;
class FunctionData;
class TableFunction;
class TableCatalogEntry;

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

} // namespace duckdb
