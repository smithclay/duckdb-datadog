#pragma once

#include "duckdb.hpp"
#include "logs_table.hpp"

namespace duckdb {

class ExtensionLoader;

//! Register the `read_datadog_traces(...)` table function.
void RegisterDatadogTracesFunction(ExtensionLoader &loader);

//! Return the stable 24-column OTLP-shaped trace schema (matches duckdb-otlp `read_otlp_traces`).
void GetDatadogTracesSchema(vector<LogicalType> &types, vector<string> &names);

//! Create the scan function and already-bound data for the catalog-backed `traces.spans` table.
//! Reuses the read_datadog_traces scan implementation and the shared logs request settings.
TableFunction GetDatadogTracesTableScan(ClientContext &context, TableCatalogEntry &table, const string &secret_name,
                                        const DatadogLogsSettings &settings, unique_ptr<FunctionData> &bind_data);

} // namespace duckdb
