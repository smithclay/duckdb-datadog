#pragma once

namespace duckdb {

class ExtensionLoader;

//! Register the `write_datadog_traces(struct [, secret])` scalar function, which sends
//! OTLP-shaped span rows to a Datadog Agent's trace API.
void RegisterDatadogWriteTracesFunction(ExtensionLoader &loader);

} // namespace duckdb
