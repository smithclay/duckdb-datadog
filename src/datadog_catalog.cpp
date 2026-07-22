#include "datadog_catalog.hpp"

#include "datadog_client.hpp"
#include "datadog_json.hpp"
#include "datadog_secret.hpp"
#include "logs_table.hpp"

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

#include <unordered_set>

namespace duckdb {
namespace {

[[noreturn]] static void ThrowReadOnly() {
	throw BinderException("Datadog catalogs are read-only");
}

class DatadogTableEntry : public TableCatalogEntry {
public:
	DatadogTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, const string &index_name, const string &secret_name,
	                  const DatadogLogsSettings &settings)
	    : DatadogTableEntry(catalog, schema, index_name, secret_name, settings, CreateInfo(schema, index_name)) {
	}

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &, column_t) override {
		return nullptr;
	}

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override {
		return GetDatadogLogsTableScan(context, *this, secret_name, index_name, settings, bind_data);
	}

	TableStorageInfo GetStorageInfo(ClientContext &) override {
		return TableStorageInfo();
	}

private:
	DatadogTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, const string &index_name, const string &secret_name,
	                  const DatadogLogsSettings &settings, CreateTableInfo info)
	    : TableCatalogEntry(catalog, schema, info), index_name(index_name), secret_name(secret_name),
	      settings(settings) {
	}

	static CreateTableInfo CreateInfo(SchemaCatalogEntry &schema, const string &index_name) {
		CreateTableInfo info(schema, index_name);
		vector<LogicalType> types;
		vector<string> names;
		GetDatadogLogsSchema(types, names);
		for (idx_t i = 0; i < names.size(); i++) {
			info.columns.AddColumn(ColumnDefinition(names[i], types[i]));
		}
		return info;
	}

	string index_name;
	string secret_name;
	DatadogLogsSettings settings;
};

class DatadogSchemaEntry : public SchemaCatalogEntry {
public:
	DatadogSchemaEntry(Catalog &catalog, const vector<string> &indexes, const string &secret_name,
	                   const DatadogLogsSettings &settings)
	    : DatadogSchemaEntry(catalog, indexes, secret_name, settings, CreateInfo()) {
	}

private:
	DatadogSchemaEntry(Catalog &catalog, const vector<string> &indexes, const string &secret_name,
	                   const DatadogLogsSettings &settings, CreateSchemaInfo info)
	    : SchemaCatalogEntry(catalog, info) {
		for (const auto &index : indexes) {
			tables.push_back(make_uniq<DatadogTableEntry>(catalog, *this, index, secret_name, settings));
		}
	}

public:
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
	static CreateSchemaInfo CreateInfo() {
		CreateSchemaInfo info;
		info.schema = "logs";
		return info;
	}

	vector<unique_ptr<DatadogTableEntry>> tables;
};

class DatadogCatalog : public Catalog {
public:
	DatadogCatalog(AttachedDatabase &db, vector<string> indexes, string secret_name,
	               const DatadogLogsSettings &settings)
	    : Catalog(db), logs_schema(make_uniq<DatadogSchemaEntry>(*this, indexes, secret_name, settings)) {
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
		callback(*logs_schema);
	}

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override {
		if (StringUtil::CIEquals(schema_lookup.GetEntryName(), "logs")) {
			return logs_schema.get();
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

	unique_ptr<DatadogSchemaEntry> logs_schema;
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

static unique_ptr<Catalog> AttachDatadog(optional_ptr<StorageExtensionInfo>, ClientContext &context,
                                         AttachedDatabase &db, const string &, AttachInfo &info,
                                         AttachOptions &options) {
	if (info.path != "datadog:") {
		throw InvalidInputException("Datadog catalogs must be attached from the path 'datadog:'");
	}

	string secret_name;
	vector<string> indexes;
	DatadogLogsSettings settings;
	bool indexes_supplied = false;
	for (const auto &option : options.options) {
		auto key = StringUtil::Lower(option.first);
		if (key == "secret") {
			if (option.second.IsNull() || option.second.type().id() != LogicalTypeId::VARCHAR) {
				throw InvalidInputException("Datadog ATTACH option SECRET must be a non-null VARCHAR name");
			}
			secret_name = option.second.GetValue<string>();
			if (secret_name.empty()) {
				throw InvalidInputException("Datadog ATTACH option SECRET must not be empty");
			}
		} else if (key == "indexes") {
			indexes = ParseExplicitIndexes(option.second);
			indexes_supplied = true;
		} else if (key == "sort") {
			if (option.second.IsNull() || option.second.type().id() != LogicalTypeId::VARCHAR) {
				throw InvalidInputException("Datadog ATTACH option SORT must be a non-null VARCHAR");
			}
			settings.sort = option.second.GetValue<string>();
		} else if (key == "page_size") {
			settings.page_size = ParseAttachInteger("PAGE_SIZE", option.second);
		} else if (key == "max_rows") {
			settings.max_rows = ParseAttachInteger("MAX_ROWS", option.second);
		} else if (key == "retries") {
			settings.retries = ParseAttachInteger("RETRIES", option.second);
		} else if (key == "timeout") {
			settings.timeout_seconds = ParseAttachInteger("TIMEOUT", option.second);
		} else {
			throw InvalidInputException(
			    "Unsupported Datadog ATTACH option '%s'; supported options are SECRET, INDEXES, "
			    "SORT, PAGE_SIZE, MAX_ROWS, RETRIES, and TIMEOUT",
			    option.first);
		}
	}
	ValidateDatadogLogsSettings(settings, "Datadog ATTACH");

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
	return make_uniq<DatadogCatalog>(db, std::move(indexes), std::move(secret_name), settings);
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
