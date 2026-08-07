#pragma once
#include "duckdb.hpp"
namespace duckdb {
class ClientContext;
class ExtensionLoader;
class FunctionData;
class TableCatalogEntry;
class TableFunction;

//! Settings shared by read_datadog_metrics and the catalog-backed `metrics.series` table.
//! Relative windows are stored verbatim and resolved only when a scan starts.
struct DatadogMetricsSettings {
	string query;
	string from = "now-15m";
	string to = "now";
	int64_t max_rows = 0;
	int64_t retries = 4;
	int64_t timeout_seconds = 60;
};

void RegisterDatadogMetricsFunction(ExtensionLoader &loader);

//! Return the stable 17-column OTLP-shaped metrics schema used by both public interfaces.
void GetDatadogMetricsSchema(vector<LogicalType> &types, vector<string> &names);

//! Create the scan function and already-bound data for the catalog-backed `metrics.series` table.
//! The METRICS_QUERY requirement and the window are validated/resolved when a scan starts, so an
//! attachment without metrics configuration still binds and DESCRIBEs offline.
TableFunction GetDatadogMetricsTableScan(ClientContext &context, TableCatalogEntry &table, const string &secret_name,
                                         const DatadogMetricsSettings &settings, unique_ptr<FunctionData> &bind_data);
} // namespace duckdb
