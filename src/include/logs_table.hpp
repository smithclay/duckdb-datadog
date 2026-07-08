#pragma once

namespace duckdb {

class ExtensionLoader;

//! Register the `read_datadog_logs(...)` table function.
void RegisterDatadogLogsFunction(ExtensionLoader &loader);

} // namespace duckdb
