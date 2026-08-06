#pragma once
#include "duckdb.hpp"
namespace duckdb {
class ExtensionLoader;
void RegisterDatadogMetricsFunction(ExtensionLoader &loader);
} // namespace duckdb
