#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ClientContext;
class FunctionData;
class TableCatalogEntry;
class TableFunction;

//! Return the schema for the catalog-backed `alerts.open` table.
void GetDatadogOpenAlertsSchema(vector<LogicalType> &types, vector<string> &names);

//! Create the already-bound scan for `alerts.open`. Credentials are resolved at table bind time,
//! matching the catalog log tables and preserving secret replacement semantics.
TableFunction GetDatadogOpenAlertsTableScan(ClientContext &context, TableCatalogEntry &table, const string &secret_name,
                                            int64_t retries, int64_t timeout_seconds,
                                            unique_ptr<FunctionData> &bind_data);

} // namespace duckdb
