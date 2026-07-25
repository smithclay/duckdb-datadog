#define DUCKDB_EXTENSION_MAIN

#include "datadog_extension.hpp"

#include "datadog_catalog.hpp"
#include "datadog_secret.hpp"
#include "logs_table.hpp"
#include "send_logs.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Credentials: CREATE SECRET (TYPE datadog, API_KEY '...', APP_KEY '...', SITE '...').
	RegisterDatadogSecretType(loader);
	// Catalog: ATTACH 'datadog:' AS dd (TYPE datadog, SECRET '...', INDEXES [...]).
	RegisterDatadogCatalog(loader);
	// Reader: SELECT * FROM read_datadog_logs(query => '...', "from" => 'now-1h', "to" => 'now').
	RegisterDatadogLogsFunction(loader);
	// Sender: SELECT send_datadog_logs(l) FROM logs l.
	RegisterDatadogSendLogsFunction(loader);
}

void DatadogExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DatadogExtension::Name() {
	return "datadog";
}

std::string DatadogExtension::Version() const {
#ifdef EXT_VERSION_DATADOG
	return EXT_VERSION_DATADOG;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(datadog, loader) {
	duckdb::LoadInternal(loader);
}
}
