#include "datadog_catalog.hpp"

#include "alerts_table.hpp"
#include "datadog_client.hpp"
#include "datadog_json.hpp"
#include "datadog_secret.hpp"
#include "logs_table.hpp"
#include "metrics_table.hpp"
#include "service_dependencies_table.hpp"
#include "traces_table.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include <functional>
#include <unordered_set>

namespace duckdb {
namespace {

[[noreturn]] static void ThrowReadOnly() {
	throw BinderException("Datadog catalogs are read-only");
}

//! One catalog table backed by a table-function scan. The factory captures whatever
//! per-table configuration (index name, settings, secret) its reader needs, so every
//! Datadog surface shares this single entry class.
class DatadogFunctionTableEntry : public TableCatalogEntry {
public:
	using ScanFactory = std::function<TableFunction(ClientContext &, TableCatalogEntry &, unique_ptr<FunctionData> &)>;

	DatadogFunctionTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo info, ScanFactory factory)
	    : TableCatalogEntry(catalog, schema, info), factory(std::move(factory)) {
	}

	static CreateTableInfo MakeInfo(SchemaCatalogEntry &schema, const string &table_name,
	                                const std::function<void(vector<LogicalType> &, vector<string> &)> &get_schema) {
		CreateTableInfo info(schema, table_name);
		vector<LogicalType> types;
		vector<string> names;
		get_schema(types, names);
		for (idx_t i = 0; i < names.size(); i++) {
			info.columns.AddColumn(ColumnDefinition(names[i], types[i]));
		}
		return info;
	}

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &, column_t) override {
		return nullptr;
	}

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override {
		return factory(context, *this, bind_data);
	}

	TableStorageInfo GetStorageInfo(ClientContext &) override {
		return TableStorageInfo();
	}

private:
	ScanFactory factory;
};

//! A read-only schema holding a fixed set of function-backed tables.
class DatadogSchemaEntry : public SchemaCatalogEntry {
public:
	DatadogSchemaEntry(Catalog &catalog, const string &schema_name)
	    : DatadogSchemaEntry(catalog, MakeInfo(schema_name)) {
	}

	void AddTable(unique_ptr<DatadogFunctionTableEntry> table) {
		tables.push_back(std::move(table));
	}

	void Scan(ClientContext &, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
		Scan(type, callback);
	}

	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
		if (type != CatalogType::TABLE_ENTRY) {
			return;
		}
		for (auto &table : tables) {
			callback(*table);
		}
	}

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction, const EntryLookupInfo &lookup_info) override {
		if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
			return nullptr;
		}
		const auto &name = lookup_info.GetEntryName();
		// Prefer an exact match so case-distinct Datadog names remain deterministic, then
		// honor DuckDB's normal case-insensitive identifier lookup behavior.
		for (auto &table : tables) {
			if (table->name == name) {
				return table.get();
			}
		}
		for (auto &table : tables) {
			if (StringUtil::CIEquals(table->name, name)) {
				return table.get();
			}
		}
		return nullptr;
	}

	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction, CreateIndexInfo &, TableCatalogEntry &) override {
		ThrowReadOnly();
	}
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction, CreateFunctionInfo &) override {
		ThrowReadOnly();
	}
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction, BoundCreateTableInfo &) override {
		ThrowReadOnly();
	}
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction, CreateViewInfo &) override {
		ThrowReadOnly();
	}
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction, CreateSequenceInfo &) override {
		ThrowReadOnly();
	}
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) override {
		ThrowReadOnly();
	}
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) override {
		ThrowReadOnly();
	}
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) override {
		ThrowReadOnly();
	}
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction, CreateCollationInfo &) override {
		ThrowReadOnly();
	}
	optional_ptr<CatalogEntry> CreateCoordinateSystem(CatalogTransaction, CreateCoordinateSystemInfo &) override {
		ThrowReadOnly();
	}
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction, CreateTypeInfo &) override {
		ThrowReadOnly();
	}
	void DropEntry(ClientContext &, DropInfo &) override {
		ThrowReadOnly();
	}
	void Alter(CatalogTransaction, AlterInfo &) override {
		ThrowReadOnly();
	}

private:
	DatadogSchemaEntry(Catalog &catalog, CreateSchemaInfo info) : SchemaCatalogEntry(catalog, info) {
	}

	static CreateSchemaInfo MakeInfo(const string &schema_name) {
		CreateSchemaInfo info;
		info.schema = schema_name;
		return info;
	}

	vector<unique_ptr<DatadogFunctionTableEntry>> tables;
};

class DatadogCatalog : public Catalog {
public:
	DatadogCatalog(AttachedDatabase &db, const vector<string> &indexes, const string &secret_name,
	               const DatadogLogsSettings &settings, const DatadogServiceMapSettings &service_map_settings,
	               const DatadogMetricsSettings &metrics_settings)
	    : Catalog(db) {
		auto &logs = AddSchema("logs");
		for (const auto &index : indexes) {
			AddTable(logs, index, GetDatadogLogsSchema,
			         [secret_name, index, settings](ClientContext &context, TableCatalogEntry &table,
			                                        unique_ptr<FunctionData> &bind_data) {
				         return GetDatadogLogsTableScan(context, table, secret_name, index, settings, bind_data);
			         });
		}

		auto &alerts = AddSchema("alerts");
		AddTable(alerts, "open", GetDatadogOpenAlertsSchema,
		         [secret_name, settings](ClientContext &context, TableCatalogEntry &table,
		                                 unique_ptr<FunctionData> &bind_data) {
			         return GetDatadogOpenAlertsTableScan(context, table, secret_name, settings.retries,
			                                              settings.timeout_seconds, bind_data);
		         });

		auto &service_map = AddSchema("service_map");
		AddTable(service_map, "dependencies", GetDatadogServiceDependenciesSchema,
		         [secret_name, service_map_settings](ClientContext &context, TableCatalogEntry &table,
		                                             unique_ptr<FunctionData> &bind_data) {
			         return GetDatadogServiceDependenciesTableScan(context, table, secret_name, service_map_settings,
			                                                       bind_data);
		         });

		auto &traces = AddSchema("traces");
		AddTable(traces, "spans", GetDatadogTracesSchema,
		         [secret_name, settings](ClientContext &context, TableCatalogEntry &table,
		                                 unique_ptr<FunctionData> &bind_data) {
			         return GetDatadogTracesTableScan(context, table, secret_name, settings, bind_data);
		         });

		auto &metrics = AddSchema("metrics");
		AddTable(metrics, "series", GetDatadogMetricsSchema,
		         [secret_name, metrics_settings](ClientContext &context, TableCatalogEntry &table,
		                                         unique_ptr<FunctionData> &bind_data) {
			         return GetDatadogMetricsTableScan(context, table, secret_name, metrics_settings, bind_data);
		         });
	}

	void Initialize(bool) override {
	}

	string GetCatalogType() override {
		return "datadog";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction, CreateSchemaInfo &) override {
		ThrowReadOnly();
	}

	void ScanSchemas(ClientContext &, std::function<void(SchemaCatalogEntry &)> callback) override {
		for (auto &schema : schemas) {
			callback(*schema);
		}
	}

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override {
		for (auto &schema : schemas) {
			if (StringUtil::CIEquals(schema_lookup.GetEntryName(), schema->name)) {
				return schema.get();
			}
		}
		if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw CatalogException(schema_lookup.GetErrorContext(), "Schema with name %s does not exist!",
			                       schema_lookup.GetEntryName());
		}
		return nullptr;
	}

	PhysicalOperator &PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &, LogicalCreateTable &,
	                                    PhysicalOperator &) override {
		ThrowReadOnly();
	}
	PhysicalOperator &PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
	                             optional_ptr<PhysicalOperator>) override {
		ThrowReadOnly();
	}
	PhysicalOperator &PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &,
	                             PhysicalOperator &) override {
		ThrowReadOnly();
	}
	PhysicalOperator &PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &,
	                             PhysicalOperator &) override {
		ThrowReadOnly();
	}

	DatabaseSize GetDatabaseSize(ClientContext &) override {
		return DatabaseSize();
	}
	bool InMemory() override {
		return false;
	}
	string GetDBPath() override {
		return "datadog:";
	}

private:
	void DropSchema(ClientContext &, DropInfo &) override {
		ThrowReadOnly();
	}

	DatadogSchemaEntry &AddSchema(const string &name) {
		schemas.push_back(make_uniq<DatadogSchemaEntry>(*this, name));
		return *schemas.back();
	}

	void AddTable(DatadogSchemaEntry &schema, const string &table_name,
	              const std::function<void(vector<LogicalType> &, vector<string> &)> &get_schema,
	              DatadogFunctionTableEntry::ScanFactory factory) {
		auto info = DatadogFunctionTableEntry::MakeInfo(schema, table_name, get_schema);
		schema.AddTable(make_uniq<DatadogFunctionTableEntry>(*this, schema, std::move(info), std::move(factory)));
	}

	vector<unique_ptr<DatadogSchemaEntry>> schemas;
};

class DatadogTransaction : public Transaction {
public:
	DatadogTransaction(TransactionManager &manager, ClientContext &context) : Transaction(manager, context) {
	}

	void SetReadWrite() override {
		ThrowReadOnly();
	}

	void SetModifications(DatabaseModificationType) override {
		ThrowReadOnly();
	}
};

class DatadogTransactionManager : public TransactionManager {
public:
	explicit DatadogTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
	}

	Transaction &StartTransaction(ClientContext &context) override {
		auto transaction = make_uniq<DatadogTransaction>(*this, context);
		auto result = transaction.get();
		lock_guard<mutex> guard(transaction_lock);
		transactions.emplace(result, std::move(transaction));
		return *result;
	}

	ErrorData CommitTransaction(ClientContext &, Transaction &transaction) override {
		lock_guard<mutex> guard(transaction_lock);
		transactions.erase(&transaction);
		return ErrorData();
	}

	void RollbackTransaction(Transaction &transaction) override {
		lock_guard<mutex> guard(transaction_lock);
		transactions.erase(&transaction);
	}

	void Checkpoint(ClientContext &, bool) override {
	}

private:
	mutex transaction_lock;
	unordered_map<Transaction *, unique_ptr<Transaction>> transactions;
};

static vector<string> ParseExplicitIndexes(const Value &value) {
	if (value.IsNull() || value.type().id() != LogicalTypeId::LIST ||
	    ListType::GetChildType(value.type()).id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("Datadog ATTACH option INDEXES must be a VARCHAR[]");
	}
	vector<string> result;
	std::unordered_set<string> seen;
	for (const auto &child : ListValue::GetChildren(value)) {
		if (child.IsNull() || child.type().id() != LogicalTypeId::VARCHAR) {
			throw InvalidInputException("Datadog ATTACH option INDEXES must contain only non-null VARCHAR names");
		}
		auto name = child.GetValue<string>();
		if (name.empty()) {
			throw InvalidInputException("Datadog ATTACH option INDEXES must not contain empty index names");
		}
		if (seen.insert(name).second) {
			result.push_back(std::move(name));
		}
	}
	return result;
}

static int64_t ParseAttachInteger(const string &option_name, const Value &value) {
	if (value.IsNull() || !value.type().IsIntegral()) {
		throw InvalidInputException("Datadog ATTACH option %s must be a non-null integer", option_name);
	}
	return value.GetValue<int64_t>();
}

static string ParseAttachVarchar(const string &option_name, const Value &value, bool allow_empty) {
	if (value.IsNull() || value.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("Datadog ATTACH option %s must be a non-null VARCHAR", option_name);
	}
	auto result = value.GetValue<string>();
	if (!allow_empty && result.empty()) {
		throw InvalidInputException("Datadog ATTACH option %s must not be empty", option_name);
	}
	return result;
}

static unique_ptr<Catalog> AttachDatadog(optional_ptr<StorageExtensionInfo>, ClientContext &context,
                                         AttachedDatabase &db, const string &, AttachInfo &info,
                                         AttachOptions &options) {
	if (info.path != "datadog:") {
		throw InvalidInputException("Datadog catalogs must be attached from the path 'datadog:'");
	}

	string secret_name;
	vector<string> indexes;
	DatadogLogsSettings settings;
	DatadogServiceMapSettings service_map_settings;
	DatadogMetricsSettings metrics_settings;
	bool indexes_supplied = false;
	for (const auto &option : options.options) {
		auto key = StringUtil::Lower(option.first);
		if (key == "secret") {
			secret_name = ParseAttachVarchar("SECRET", option.second, false);
		} else if (key == "indexes") {
			indexes = ParseExplicitIndexes(option.second);
			indexes_supplied = true;
		} else if (key == "sort") {
			settings.sort = ParseAttachVarchar("SORT", option.second, true);
		} else if (key == "page_size") {
			settings.page_size = ParseAttachInteger("PAGE_SIZE", option.second);
		} else if (key == "max_rows") {
			settings.max_rows = ParseAttachInteger("MAX_ROWS", option.second);
		} else if (key == "retries") {
			settings.retries = ParseAttachInteger("RETRIES", option.second);
		} else if (key == "timeout") {
			settings.timeout_seconds = ParseAttachInteger("TIMEOUT", option.second);
		} else if (key == "service_map_env") {
			service_map_settings.environment = ParseAttachVarchar("SERVICE_MAP_ENV", option.second, false);
		} else if (key == "service_map_start_time") {
			service_map_settings.from = ParseAttachVarchar("SERVICE_MAP_START_TIME", option.second, true);
		} else if (key == "service_map_end_time") {
			service_map_settings.to = ParseAttachVarchar("SERVICE_MAP_END_TIME", option.second, true);
		} else if (key == "service_map_primary_tag") {
			service_map_settings.primary_tag = ParseAttachVarchar("SERVICE_MAP_PRIMARY_TAG", option.second, false);
		} else if (key == "metrics_query") {
			metrics_settings.query = ParseAttachVarchar("METRICS_QUERY", option.second, false);
		} else if (key == "metrics_start_time") {
			metrics_settings.from = ParseAttachVarchar("METRICS_START_TIME", option.second, true);
		} else if (key == "metrics_end_time") {
			metrics_settings.to = ParseAttachVarchar("METRICS_END_TIME", option.second, true);
		} else {
			throw InvalidInputException(
			    "Unsupported Datadog ATTACH option '%s'; supported options are SECRET, INDEXES, "
			    "SORT, PAGE_SIZE, MAX_ROWS, RETRIES, TIMEOUT, SERVICE_MAP_ENV, "
			    "SERVICE_MAP_START_TIME, SERVICE_MAP_END_TIME, SERVICE_MAP_PRIMARY_TAG, "
			    "METRICS_QUERY, METRICS_START_TIME, and METRICS_END_TIME",
			    option.first);
		}
	}
	ValidateDatadogLogsSettings(settings, "Datadog ATTACH");
	service_map_settings.retries = settings.retries;
	service_map_settings.timeout_seconds = settings.timeout_seconds;
	ValidateDatadogServiceMapSettings(service_map_settings, "Datadog ATTACH service map", false);
	metrics_settings.max_rows = settings.max_rows;
	metrics_settings.retries = settings.retries;
	metrics_settings.timeout_seconds = settings.timeout_seconds;

	// Always validate/select the secret at attach time, but retain only its name. Explicit
	// INDEXES never performs a network request.
	auto credentials = GetDatadogCredentials(context, secret_name);
	// Pin implicit selection to the same secret used at attach time. Leaving the name empty would
	// re-run first-secret selection at every table bind and could silently switch accounts.
	if (secret_name.empty()) {
		secret_name = credentials.name;
	}
	if (!indexes_supplied) {
		DatadogClient client;
		client.site = credentials.site;
		client.api_key = credentials.api_key;
		client.app_key = credentials.app_key;
		client.retries = static_cast<uint64_t>(settings.retries);
		client.timeout_seconds = static_cast<uint64_t>(settings.timeout_seconds);
		indexes = ParseDatadogLogIndexes(client.ListLogIndexes(context));
	}

	db.SetReadOnlyDatabase();
	return make_uniq<DatadogCatalog>(db, indexes, secret_name, settings, service_map_settings, metrics_settings);
}

static unique_ptr<TransactionManager> CreateDatadogTransactionManager(optional_ptr<StorageExtensionInfo>,
                                                                      AttachedDatabase &db, Catalog &) {
	return make_uniq<DatadogTransactionManager>(db);
}

} // namespace

void RegisterDatadogCatalog(ExtensionLoader &loader) {
	auto storage = make_shared_ptr<StorageExtension>();
	storage->attach = AttachDatadog;
	storage->create_transaction_manager = CreateDatadogTransactionManager;
	StorageExtension::Register(DBConfig::GetConfig(loader.GetDatabaseInstance()), "datadog", std::move(storage));
}

} // namespace duckdb
