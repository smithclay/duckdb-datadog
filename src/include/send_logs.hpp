#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

//! Register the `send_datadog_logs` scalar function so users can push an OTLP-shaped log table into
//! a Datadog account through the log intake API:
//!   SELECT send_datadog_logs(l) FROM logs l;                  -- default `datadog` secret
//!   SELECT send_datadog_logs(l, 'dd_prod') FROM logs l;       -- explicit secret
//! The single argument is any STRUCT (typically a whole row); recognized OTLP fields are mapped
//! loosely by name and extra columns are ignored. Returns 'ok' per row that was accepted.
void RegisterDatadogSendLogsFunction(ExtensionLoader &loader);

} // namespace duckdb
