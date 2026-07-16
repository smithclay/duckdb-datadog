#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;
class ClientContext;
class FunctionData;
class TableFunction;
class TableCatalogEntry;

//! Register the `read_datadog_logs(...)` table function.
void RegisterDatadogLogsFunction(ExtensionLoader &loader);

//! Return the stable 18-column OTLP-shaped output schema used by both public interfaces.
void GetDatadogLogsSchema(vector<LogicalType> &types, vector<string> &names);

//! Create the scan function and already-bound data for one catalog table. Credentials are
//! resolved through GetDatadogCredentials at this table-bind boundary.
TableFunction GetDatadogLogsTableScan(ClientContext &context, TableCatalogEntry &table, const string &secret_name,
                                      const string &index_name, unique_ptr<FunctionData> &bind_data);

} // namespace duckdb
