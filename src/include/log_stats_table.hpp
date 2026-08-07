#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

//! Register `read_datadog_log_stats`, the Logs Aggregation API reader:
//!   SELECT * FROM read_datadog_log_stats(query => 'service:web', compute => 'count',
//!                                        "from" => 'now-1h', "to" => 'now',
//!                                        group_by => ['service']);
void RegisterDatadogLogStatsFunction(ExtensionLoader &loader);

} // namespace duckdb
