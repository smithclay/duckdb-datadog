#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

//! Credentials needed to talk to the Datadog Logs API.
struct DatadogCredentials {
	string api_key;
	string app_key;
	string site = "datadoghq.com";
};

//! Register the `datadog` secret type and its `config` provider so users can run:
//!   CREATE SECRET (TYPE datadog, API_KEY '...', APP_KEY '...', SITE 'datadoghq.com');
void RegisterDatadogSecretType(ExtensionLoader &loader);

//! Resolve Datadog credentials from the secret manager. If `secret_name` is empty the first
//! secret of type `datadog` in scope is used. Throws a helpful error if none is found or if a
//! required field is missing.
DatadogCredentials GetDatadogCredentials(ClientContext &context, const string &secret_name);

} // namespace duckdb
