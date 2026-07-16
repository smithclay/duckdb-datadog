#pragma once

namespace duckdb {

class ExtensionLoader;

//! Register the `datadog` storage extension used by ATTACH ... (TYPE datadog).
void RegisterDatadogCatalog(ExtensionLoader &loader);

} // namespace duckdb
