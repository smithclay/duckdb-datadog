#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;
class FunctionData;
class TableCatalogEntry;
class TableFunction;

//! Settings shared by read_datadog_service_dependencies and the catalog-backed dependency table.
//! Relative windows are stored verbatim and resolved only when a scan starts.
struct DatadogServiceMapSettings {
	string environment;
	string from = "-1h";
	string to = "now";
	string primary_tag;
	int64_t retries = 4;
	int64_t timeout_seconds = 60;
};

//! Register read_datadog_service_dependencies(env => ..., ...).
void RegisterDatadogServiceDependenciesFunction(ExtensionLoader &loader);

//! Return the canonical cross-provider directed-edge schema.
void GetDatadogServiceDependenciesSchema(vector<LogicalType> &types, vector<string> &names);

//! Create the scan function and bound data for dd.service_map.dependencies. This is the same scan
//! implementation used by the public table function.
TableFunction GetDatadogServiceDependenciesTableScan(ClientContext &context, TableCatalogEntry &table,
                                                     const string &secret_name,
                                                     const DatadogServiceMapSettings &settings,
                                                     unique_ptr<FunctionData> &bind_data);

//! Validate non-window settings. Window syntax is deliberately resolved at execution time so
//! relative endpoints use the scan clock rather than the attach/bind clock.
void ValidateDatadogServiceMapSettings(const DatadogServiceMapSettings &settings, const string &error_prefix,
                                       bool require_environment);

} // namespace duckdb
