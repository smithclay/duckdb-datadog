#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

//! Register send_datadog_metrics(metric [, secret]).
void RegisterDatadogSendMetricsFunction(ExtensionLoader &loader);

} // namespace duckdb
