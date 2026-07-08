#include "datadog_secret.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

//! Build a KeyValueSecret from `CREATE SECRET (TYPE datadog, ...)` options.
static unique_ptr<BaseSecret> CreateDatadogSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, "datadog", "config", input.name);

	// Only copy the keys we understand; ignore anything else.
	for (const auto &option : input.options) {
		auto lower_name = StringUtil::Lower(option.first);
		if (lower_name == "api_key" || lower_name == "app_key" || lower_name == "site") {
			secret->secret_map[lower_name] = option.second;
		}
	}

	// Never print the keys in duckdb_secrets() / SHOW SECRETS.
	secret->redact_keys = {"api_key", "app_key"};
	return std::move(secret);
}

void RegisterDatadogSecretType(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = "datadog";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	loader.RegisterSecretType(secret_type);

	CreateSecretFunction datadog_secret_function = {"datadog", "config", CreateDatadogSecretFromConfig};
	datadog_secret_function.named_parameters["api_key"] = LogicalType::VARCHAR;
	datadog_secret_function.named_parameters["app_key"] = LogicalType::VARCHAR;
	datadog_secret_function.named_parameters["site"] = LogicalType::VARCHAR;
	loader.RegisterFunction(datadog_secret_function);
}

DatadogCredentials GetDatadogCredentials(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	unique_ptr<SecretEntry> entry;
	if (!secret_name.empty()) {
		entry = secret_manager.GetSecretByName(transaction, secret_name);
		if (!entry) {
			throw InvalidInputException("No secret with name '%s' found", secret_name);
		}
	} else {
		// No explicit name: use the first secret of type `datadog`.
		for (auto &candidate : secret_manager.AllSecrets(transaction)) {
			if (candidate.secret && candidate.secret->GetType() == "datadog") {
				entry = make_uniq<SecretEntry>(candidate);
				break;
			}
		}
		if (!entry) {
			throw InvalidInputException("No 'datadog' secret found. Create one first, e.g.:\n"
			                            "  CREATE SECRET (TYPE datadog, API_KEY '<dd-api-key>', APP_KEY "
			                            "'<dd-app-key>', SITE 'datadoghq.com');");
		}
	}

	const auto &base_secret = *entry->secret;
	if (base_secret.GetType() != "datadog") {
		throw InvalidInputException("Secret '%s' is not a 'datadog' secret (found type '%s')", secret_name,
		                            base_secret.GetType());
	}
	const auto *kv_secret = dynamic_cast<const KeyValueSecret *>(&base_secret);
	if (!kv_secret) {
		throw InvalidInputException("Secret '%s' is not a key-value 'datadog' secret", base_secret.GetName());
	}

	DatadogCredentials creds;
	Value value;
	if (kv_secret->TryGetValue("api_key", value) && !value.IsNull()) {
		creds.api_key = value.ToString();
	}
	if (kv_secret->TryGetValue("app_key", value) && !value.IsNull()) {
		creds.app_key = value.ToString();
	}
	if (kv_secret->TryGetValue("site", value) && !value.IsNull() && !value.ToString().empty()) {
		creds.site = value.ToString();
	}

	if (creds.api_key.empty()) {
		throw InvalidInputException("datadog secret is missing required field API_KEY");
	}
	if (creds.app_key.empty()) {
		throw InvalidInputException("datadog secret is missing required field APP_KEY");
	}
	return creds;
}

} // namespace duckdb
