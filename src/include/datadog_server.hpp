#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

//! Register the native live-ingest scalar functions:
//!   datadog_serve([uri [, options_struct]]) -> intake URL
//!   datadog_stop([uri])                     -> status
void RegisterDatadogServerFunctions(ExtensionLoader &loader);

//! Register per-DatabaseInstance server state. The state owns every listener and
//! stops them before the database is destroyed.
void RegisterDatadogServerState(ExtensionLoader &loader);

} // namespace duckdb
