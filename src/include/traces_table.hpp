#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

//! Register the `read_datadog_traces(...)` table function.
void RegisterDatadogTracesFunction(ExtensionLoader &loader);

//! Return the stable 24-column OTLP-shaped trace schema (matches duckdb-otlp `read_otlp_traces`).
void GetDatadogTracesSchema(vector<LogicalType> &types, vector<string> &names);

} // namespace duckdb
