#include "datadog_server.hpp"

#include "datadog_secret.hpp"
#include "logs_table.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/storage/storage_extension.hpp"

#ifndef __EMSCRIPTEN__
#define CPPHTTPLIB_ZSTD_SUPPORT
#include "zstd.h"
// DuckDB namespaces its bundled zstd symbols. Make them visible to cpp-httplib's optional zstd
// decoder so the intake can accept the compression used by current Datadog Agents.
namespace duckdb_httplib_openssl {
using namespace duckdb_zstd; // NOLINT
}
#include "httplib.hpp"
#endif
#include "yyjson.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace {

static constexpr idx_t DEFAULT_MAX_BODY_BYTES = 5ULL * 1024ULL * 1024ULL;
static constexpr uint16_t DEFAULT_DATADOG_PORT = 10518;

struct YyjsonDocDeleter {
	void operator()(yyjson_doc *doc) const {
		yyjson_doc_free(doc);
	}
};
struct YyjsonMutDocDeleter {
	void operator()(yyjson_mut_doc *doc) const {
		yyjson_mut_doc_free(doc);
	}
};
struct YyjsonFreeDeleter {
	void operator()(char *ptr) const {
		free(ptr);
	}
};
using YyjsonDocPtr = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>;
using YyjsonMutDocPtr = std::unique_ptr<yyjson_mut_doc, YyjsonMutDocDeleter>;
using YyjsonStrPtr = std::unique_ptr<char, YyjsonFreeDeleter>;

class DatadogListenUri {
public:
	explicit DatadogListenUri(string input = "datadog:localhost:10518") {
		StringUtil::Trim(input);
		string remainder;
		if (StringUtil::StartsWith(input, "datadog://")) {
			remainder = input.substr(strlen("datadog://"));
		} else if (StringUtil::StartsWith(input, "datadog:")) {
			remainder = input.substr(strlen("datadog:"));
		} else {
			throw InvalidInputException("Invalid Datadog listen URI: expected 'datadog:host[:port]'");
		}
		if (remainder.empty()) {
			remainder = "localhost";
		}
		port = DEFAULT_DATADOG_PORT;
		if (StringUtil::StartsWith(remainder, "[")) {
			auto closing = remainder.find(']');
			if (closing == string::npos || closing == 1) {
				throw InvalidInputException("Invalid IPv6 Datadog listen URI");
			}
			ipv6 = true;
			host = remainder.substr(1, closing - 1);
			auto suffix = remainder.substr(closing + 1);
			if (!suffix.empty()) {
				if (suffix[0] != ':') {
					throw InvalidInputException("Invalid IPv6 Datadog listen URI");
				}
				port = ParsePort(suffix.substr(1));
			}
			for (auto c : host) {
				if (!isxdigit(static_cast<unsigned char>(c)) && c != ':' && c != '.' && c != '%') {
					throw InvalidInputException("Invalid character in IPv6 Datadog listen address");
				}
			}
		} else {
			auto colon = remainder.find(':');
			if (colon != string::npos) {
				if (remainder.find(':', colon + 1) != string::npos) {
					throw InvalidInputException("IPv6 Datadog listen addresses must be enclosed in brackets");
				}
				port = ParsePort(remainder.substr(colon + 1));
				remainder.resize(colon);
			}
			host = remainder;
			if (host.empty()) {
				throw InvalidInputException("Missing Datadog listen hostname");
			}
			for (auto c : host) {
				if (!isalnum(static_cast<unsigned char>(c)) && c != '-' && c != '.') {
					throw InvalidInputException("Invalid character in Datadog listen hostname");
				}
			}
		}
		canonical = "datadog:" + (ipv6 ? "[" + host + "]" : host) + ":" + std::to_string(port);
		base_url = "http://" + (ipv6 ? "[" + host + "]" : host) + ":" + std::to_string(port);
	}

	string Host() const {
		return host;
	}
	uint16_t Port() const {
		return port;
	}
	string Canonical() const {
		return canonical;
	}
	string BaseUrl() const {
		return base_url;
	}
	string IntakeUrl() const {
		return base_url + "/api/v2/logs";
	}
	bool IsLocal() const {
		return StringUtil::CIEquals(host, "localhost") || host == "127.0.0.1" || host == "::1";
	}

private:
	static uint16_t ParsePort(const string &text) {
		size_t parsed = 0;
		int value = 0;
		try {
			value = std::stoi(text, &parsed);
		} catch (...) {
			throw InvalidInputException("Invalid Datadog listen port");
		}
		if (text.empty() || parsed != text.size() || value < 1 || value > 65535) {
			throw InvalidInputException("Invalid Datadog listen port");
		}
		return static_cast<uint16_t>(value);
	}

	bool ipv6 = false;
	string host;
	uint16_t port;
	string canonical;
	string base_url;
};

struct DatadogServerConfig {
	string schema_name = "main";
	string table_name = "datadog_logs";
	string api_key;
	bool allow_other_hostname = false;
	bool create_table = true;
	idx_t max_body_bytes = DEFAULT_MAX_BODY_BYTES;
	idx_t http_threads = 0;
};

static int32_t StatusToSeverityNumber(const string &status) {
	auto value = StringUtil::Lower(status);
	if (value == "trace") {
		return 1;
	}
	if (value == "debug") {
		return 5;
	}
	if (value == "info" || value == "notice" || value == "ok") {
		return 9;
	}
	if (value == "warn" || value == "warning") {
		return 13;
	}
	if (value == "error" || value == "err") {
		return 17;
	}
	if (value == "critical" || value == "crit" || value == "alert" || value == "emergency" || value == "fatal") {
		return 21;
	}
	return 0;
}

static bool TimingSafeEqual(const string &left, const string &right) {
	if (left.size() != right.size()) {
		return false;
	}
	volatile unsigned char difference = 0;
	for (idx_t index = 0; index < left.size(); index++) {
		difference |= static_cast<unsigned char>(left[index]) ^ static_cast<unsigned char>(right[index]);
	}
	return difference == 0;
}

static int64_t NowNanos() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

static bool ParseIso8601ToNanos(const char *text, int64_t &result) {
	if (!text) {
		return false;
	}
	auto length = strlen(text);
	timestamp_t timestamp;
	bool has_offset = false;
	string_t tz;
	int32_t sub_micro_nanos = 0;
	auto converted = Timestamp::TryConvertTimestampTZ(text, length, timestamp, true, has_offset, tz, &sub_micro_nanos);
	if (converted == TimestampCastResult::SUCCESS) {
		int64_t epoch_nanos;
		if (Timestamp::TryGetEpochNanoSeconds(timestamp, epoch_nanos)) {
			result = epoch_nanos + sub_micro_nanos;
			return true;
		}
	}
	timestamp_ns_t timestamp_ns;
	if (Timestamp::TryConvertTimestamp(text, length, timestamp_ns) == TimestampCastResult::SUCCESS) {
		result = timestamp_ns.value;
		return true;
	}
	return false;
}

static bool ParseDatadogTimestamp(yyjson_val *value, int64_t &nanos) {
	if (!value) {
		return false;
	}
	if (yyjson_is_num(value)) {
		long double milliseconds =
		    yyjson_is_real(value) ? yyjson_get_real(value) : static_cast<long double>(yyjson_get_sint(value));
		nanos = static_cast<int64_t>(milliseconds * 1000000.0L);
		return true;
	}
	if (!yyjson_is_str(value)) {
		return false;
	}
	auto text = string(yyjson_get_str(value));
	try {
		size_t parsed = 0;
		auto milliseconds = std::stoll(text, &parsed);
		if (parsed == text.size()) {
			nanos = milliseconds * 1000000;
			return true;
		}
	} catch (...) {
	}
	return ParseIso8601ToNanos(text.c_str(), nanos);
}

static const char *GetString(yyjson_val *object, const char *key) {
	auto value = object ? yyjson_obj_get(object, key) : nullptr;
	return value && yyjson_is_str(value) ? yyjson_get_str(value) : nullptr;
}

static string SerializeValue(yyjson_val *value) {
	if (!value) {
		return string();
	}
	size_t length = 0;
	YyjsonStrPtr json(yyjson_val_write(value, 0, &length));
	return json ? string(json.get(), length) : string();
}

static string MessageValue(yyjson_val *object) {
	auto message = yyjson_obj_get(object, "message");
	if (!message) {
		return string();
	}
	return yyjson_is_str(message) ? string(yyjson_get_str(message)) : SerializeValue(message);
}

static string BuildResourceAttributes(yyjson_val *object) {
	const char *hostname = GetString(object, "hostname");
	if (!hostname) {
		hostname = GetString(object, "host");
	}
	const char *tags = GetString(object, "ddtags");
	const char *source = GetString(object, "ddsource");
	if (!hostname && !tags && !source) {
		return string();
	}
	YyjsonMutDocPtr doc(yyjson_mut_doc_new(nullptr));
	auto root = yyjson_mut_obj(doc.get());
	yyjson_mut_doc_set_root(doc.get(), root);
	if (hostname) {
		yyjson_mut_obj_add_strcpy(doc.get(), root, "host", hostname);
	}
	if (source) {
		yyjson_mut_obj_add_strcpy(doc.get(), root, "ddsource", source);
	}
	if (tags) {
		auto array = yyjson_mut_arr(doc.get());
		for (auto &tag : StringUtil::Split(tags, ',')) {
			StringUtil::Trim(tag);
			if (!tag.empty()) {
				yyjson_mut_arr_add_strcpy(doc.get(), array, tag.c_str());
			}
		}
		yyjson_mut_obj_add_val(doc.get(), root, "ddtags", array);
	}
	YyjsonStrPtr json(yyjson_mut_write(doc.get(), 0, nullptr));
	return json ? string(json.get()) : string();
}

static string BuildLogAttributes(yyjson_val *object) {
	static const std::unordered_set<string> reserved = {"message",  "service", "status", "timestamp",
	                                                    "hostname", "host",    "ddtags", "ddsource"};
	YyjsonMutDocPtr doc(yyjson_mut_doc_new(nullptr));
	auto root = yyjson_mut_obj(doc.get());
	yyjson_mut_doc_set_root(doc.get(), root);
	yyjson_obj_iter iterator;
	yyjson_obj_iter_init(object, &iterator);
	yyjson_val *key;
	idx_t copied = 0;
	while ((key = yyjson_obj_iter_next(&iterator))) {
		auto name = yyjson_get_str(key);
		if (!name || reserved.find(name) != reserved.end()) {
			continue;
		}
		auto value = yyjson_obj_iter_get_val(key);
		auto mutable_key = yyjson_mut_strcpy(doc.get(), name);
		auto mutable_value = yyjson_val_mut_copy(doc.get(), value);
		if (mutable_key && mutable_value) {
			yyjson_mut_obj_add(root, mutable_key, mutable_value);
			copied++;
		}
	}
	if (copied == 0) {
		return string();
	}
	YyjsonStrPtr json(yyjson_mut_write(doc.get(), 0, nullptr));
	return json ? string(json.get()) : string();
}

static vector<Value> MapIntakeLog(yyjson_val *object) {
	if (!object || !yyjson_is_obj(object)) {
		throw InvalidInputException("Each Datadog log must be a JSON object");
	}
	auto observed_nanos = NowNanos();
	auto event_nanos = observed_nanos;
	ParseDatadogTimestamp(yyjson_obj_get(object, "timestamp"), event_nanos);

	auto status = GetString(object, "status");
	auto service = GetString(object, "service");
	auto trace_id = GetString(object, "dd.trace_id");
	if (!trace_id) {
		trace_id = GetString(object, "trace_id");
	}
	auto span_id = GetString(object, "dd.span_id");
	if (!span_id) {
		span_id = GetString(object, "span_id");
	}
	auto resource_attributes = BuildResourceAttributes(object);
	auto log_attributes = BuildLogAttributes(object);

	vector<Value> row(18);
	row[0] = Value::TIMESTAMPNS(timestamp_ns_t(event_nanos));
	row[1] = Value::TIMESTAMPNS(timestamp_ns_t(observed_nanos));
	if (trace_id) {
		row[2] = Value(trace_id);
	}
	if (span_id) {
		row[3] = Value(span_id);
	}
	if (service) {
		row[4] = Value(service);
	}
	if (status) {
		row[7] = Value::INTEGER(StatusToSeverityNumber(status));
		row[8] = Value(status);
	}
	auto message = MessageValue(object);
	if (!message.empty()) {
		row[10] = Value(message);
	}
	if (!resource_attributes.empty()) {
		row[11] = Value(resource_attributes);
	}
	if (!log_attributes.empty()) {
		row[15] = Value(log_attributes);
	}
	return row;
}

static void ParseJsonDocument(const string &body, vector<vector<Value>> &rows) {
	YyjsonDocPtr doc(yyjson_read(body.c_str(), body.size(), 0));
	if (!doc) {
		throw InvalidInputException("Datadog intake body is not valid JSON");
	}
	auto root = yyjson_doc_get_root(doc.get());
	if (yyjson_is_arr(root)) {
		if (yyjson_arr_size(root) > 1000) {
			throw InvalidInputException("Datadog intake arrays may contain at most 1000 logs");
		}
		size_t index, count;
		yyjson_val *item;
		yyjson_arr_foreach(root, index, count, item) {
			rows.push_back(MapIntakeLog(item));
		}
	} else if (yyjson_is_obj(root)) {
		rows.push_back(MapIntakeLog(root));
	} else {
		throw InvalidInputException("Datadog intake body must be a JSON object or array");
	}
}

static vector<vector<Value>> ParseIntakeBody(const string &content_type, const string &body) {
	vector<vector<Value>> rows;
	auto media_type = StringUtil::Lower(content_type.substr(0, content_type.find(';')));
	if (media_type == "application/x-ndjson" || media_type == "application/ndjson" || media_type == "text/plain") {
		idx_t begin = 0;
		while (begin < body.size()) {
			auto end = body.find('\n', begin);
			if (end == string::npos) {
				end = body.size();
			}
			auto line = body.substr(begin, end - begin);
			StringUtil::Trim(line);
			if (!line.empty()) {
				ParseJsonDocument(line, rows);
			}
			begin = end + 1;
		}
	} else {
		ParseJsonDocument(body, rows);
	}
	return rows;
}

static idx_t DefaultHttpThreads() {
	auto cores = std::thread::hardware_concurrency();
	if (cores == 0) {
		return 8;
	}
	return std::min<idx_t>(32, std::max<idx_t>(4, static_cast<idx_t>(cores) * 2));
}

#ifndef __EMSCRIPTEN__
static string JsonEscape(const string &input) {
	string output;
	for (auto c : input) {
		switch (c) {
		case '\\':
			output += "\\\\";
			break;
		case '"':
			output += "\\\"";
			break;
		case '\n':
			output += "\\n";
			break;
		case '\r':
			output += "\\r";
			break;
		default:
			output += c;
		}
	}
	return output;
}

static void SetJson(duckdb_httplib_openssl::Response &response, int status, const string &body) {
	response.status = status;
	response.set_content(body, "application/json");
}

class DatadogServer {
public:
	DatadogServer(ClientContext &context, DatadogListenUri uri_p, DatadogServerConfig config_p)
	    : db_ptr(context.db), uri(std::move(uri_p)), config(std::move(config_p)) {
		if (!config.allow_other_hostname && !uri.IsLocal()) {
			throw InvalidInputException("Only localhost is allowed as a Datadog hostname by default; set "
			                            "allow_other_hostname=true to override");
		}
		if (config.schema_name.empty() || config.table_name.empty()) {
			throw InvalidInputException("Datadog target schema and table names must not be empty");
		}
		if (config.max_body_bytes == 0 || config.http_threads == 0) {
			throw InvalidInputException("max_body_bytes and http_threads must be greater than zero");
		}
		auto db = db_ptr.lock();
		if (!db) {
			throw InternalException("Database was closed");
		}
		writer = make_uniq<Connection>(*db);
		writer->context->config.enable_progress_bar = false;
		EnsureTargetTable();

		server = make_uniq<duckdb_httplib_openssl::Server>();
		auto threads = config.http_threads;
		server->new_task_queue = [threads] {
			return new duckdb_httplib_openssl::ThreadPool(static_cast<size_t>(threads));
		};
		server->set_keep_alive_max_count(128);
		server->set_keep_alive_timeout(10);
		server->set_tcp_nodelay(true);
		server->set_payload_max_length(static_cast<size_t>(config.max_body_bytes));
		server->Get("/healthz",
		            [](const duckdb_httplib_openssl::Request &, duckdb_httplib_openssl::Response &response) {
			            SetJson(response, 200, "{\"status\":\"ok\"}");
		            });
		auto handler = [this](const duckdb_httplib_openssl::Request &request,
		                      duckdb_httplib_openssl::Response &response) {
			try {
				if (request.body.size() > config.max_body_bytes) {
					SetJson(response, 413, "{\"errors\":[\"Payload Too Large\"]}");
					return;
				}
				auto supplied_key = request.get_header_value("DD-API-KEY");
				if (!config.api_key.empty() && !TimingSafeEqual(supplied_key, config.api_key)) {
					SetJson(response, 403, "{\"errors\":[\"Forbidden\"]}");
					return;
				}
				auto rows = ParseIntakeBody(request.get_header_value("Content-Type"), request.body);
				Append(rows);
				total_requests++;
				total_rows += rows.size();
				SetJson(response, 202,
				        StringUtil::Format("{\"status\":\"ok\",\"rows\":%llu}", static_cast<uint64_t>(rows.size())));
			} catch (InvalidInputException &ex) {
				SetJson(response, 400, "{\"errors\":[\"" + JsonEscape(ex.what()) + "\"]}");
			} catch (std::exception &ex) {
				SetJson(response, 500, "{\"errors\":[\"" + JsonEscape(ex.what()) + "\"]}");
			}
		};
		server->Post("/api/v2/logs", handler);
		server->Post("/api/v1/logs", handler);
		server->Post("/v1/input", handler);

		if (!server->is_valid() || !server->bind_to_port(uri.Host(), uri.Port())) {
			throw IOException("Failed to bind Datadog HTTP server to %s", uri.BaseUrl());
		}
		listening.store(true);
		listen_thread = std::thread([this] {
			server->listen_after_bind();
			listening.store(false);
		});
		server->wait_until_ready();
	}

	~DatadogServer() {
		Close();
	}

	void Close() {
		std::lock_guard<std::mutex> close_lock(close_mutex);
		if (!server) {
			return;
		}
		server->stop();
		if (listen_thread.joinable()) {
			listen_thread.join();
		}
		server.reset();
		writer.reset();
		listening.store(false);
	}

	string IntakeUrl() const {
		return uri.IntakeUrl();
	}
	const DatadogServerConfig &Config() const {
		return config;
	}

private:
	void EnsureTargetTable() {
		auto qualified = KeywordHelper::WriteOptionallyQuoted(config.schema_name) + "." +
		                 KeywordHelper::WriteOptionallyQuoted(config.table_name);
		if (config.create_table) {
			auto result = writer->Query(
			    "CREATE TABLE IF NOT EXISTS " + qualified +
			    " (time_unix_nano TIMESTAMP_NS, observed_time_unix_nano TIMESTAMP_NS, trace_id VARCHAR, "
			    "span_id VARCHAR, service_name VARCHAR, service_namespace VARCHAR, service_instance_id VARCHAR, "
			    "severity_number INTEGER, severity_text VARCHAR, event_name VARCHAR, body VARCHAR, "
			    "resource_attributes VARCHAR, scope_name VARCHAR, scope_version VARCHAR, scope_attributes VARCHAR, "
			    "log_attributes VARCHAR, dropped_attributes_count INTEGER, flags INTEGER)");
			if (!result || result->HasError()) {
				throw IOException("Could not create Datadog target table %s: %s", qualified,
				                  result ? result->GetError() : "query failed");
			}
		}
		auto result = writer->Query("SELECT * FROM " + qualified + " LIMIT 0");
		vector<LogicalType> expected_types;
		vector<string> expected_names;
		GetDatadogLogsSchema(expected_types, expected_names);
		bool valid = result && !result->HasError() && result->types.size() == expected_types.size();
		if (valid) {
			for (idx_t index = 0; index < expected_types.size(); index++) {
				if (result->names[index] != expected_names[index] || result->types[index] != expected_types[index]) {
					valid = false;
					break;
				}
			}
		}
		if (!valid) {
			throw InvalidInputException("Datadog target table %s must have the 18-column read_datadog_logs schema",
			                            qualified);
		}
	}

	void Append(const vector<vector<Value>> &rows) {
		if (rows.empty()) {
			return;
		}
		std::lock_guard<std::mutex> lock(writer_mutex);
		writer->BeginTransaction();
		try {
			Appender appender(*writer, config.schema_name, config.table_name);
			for (auto &row : rows) {
				appender.BeginRow();
				for (auto &value : row) {
					appender.Append(value);
				}
				appender.EndRow();
			}
			appender.Close();
			writer->Commit();
		} catch (...) {
			writer->Rollback();
			throw;
		}
	}

	weak_ptr<DatabaseInstance> db_ptr;
	DatadogListenUri uri;
	DatadogServerConfig config;
	unique_ptr<Connection> writer;
	unique_ptr<duckdb_httplib_openssl::Server> server;
	std::thread listen_thread;
	std::mutex writer_mutex;
	std::mutex close_mutex;
	std::atomic<bool> listening {false};
	std::atomic<idx_t> total_requests {0};
	std::atomic<idx_t> total_rows {0};
};
#endif

class DatadogServerExtensionInfo : public StorageExtensionInfo {
public:
	static constexpr const char *KEY = "datadog_server";

	~DatadogServerExtensionInfo() override {
		StopAll();
	}

	static DatadogServerExtensionInfo &Get(DatabaseInstance &database) {
		auto extension = StorageExtension::Find(database.config, KEY);
		if (!extension || !extension->storage_info) {
			throw InternalException("Datadog server extension state is not registered");
		}
		return *static_cast<DatadogServerExtensionInfo *>(extension->storage_info.get());
	}

	string Start(ClientContext &context, const DatadogListenUri &uri, const DatadogServerConfig &config) {
#ifdef __EMSCRIPTEN__
		throw NotImplementedException("datadog_serve is not implemented for the wasm platform");
#else
		std::lock_guard<std::mutex> lock(mutex);
		auto key = uri.Canonical();
		auto existing = servers.find(key);
		if (existing != servers.end()) {
			auto &current = existing->second->Config();
			if (current.schema_name != config.schema_name || current.table_name != config.table_name ||
			    current.api_key != config.api_key || current.allow_other_hostname != config.allow_other_hostname ||
			    current.create_table != config.create_table || current.max_body_bytes != config.max_body_bytes ||
			    current.http_threads != config.http_threads) {
				throw InvalidInputException("A Datadog server already exists for %s with different options", key);
			}
			return existing->second->IntakeUrl();
		}
		shared_ptr<DatadogServer> server(new DatadogServer(context, uri, config));
		auto url = server->IntakeUrl();
		servers.emplace(key, std::move(server));
		return url;
#endif
	}

	bool Stop(const DatadogListenUri &uri) {
#ifdef __EMSCRIPTEN__
		return false;
#else
		shared_ptr<DatadogServer> server;
		{
			std::lock_guard<std::mutex> lock(mutex);
			auto entry = servers.find(uri.Canonical());
			if (entry == servers.end()) {
				return false;
			}
			server = std::move(entry->second);
			servers.erase(entry);
		}
		server->Close();
		return true;
#endif
	}

private:
	void StopAll() {
#ifndef __EMSCRIPTEN__
		vector<shared_ptr<DatadogServer>> active;
		{
			std::lock_guard<std::mutex> lock(mutex);
			for (auto &entry : servers) {
				active.push_back(std::move(entry.second));
			}
			servers.clear();
		}
		for (auto &server : active) {
			server->Close();
		}
#endif
	}

	std::mutex mutex;
#ifndef __EMSCRIPTEN__
	unordered_map<string, shared_ptr<DatadogServer>> servers;
#endif
};

static bool TryGetOption(const Value &options, const string &name, Value &result) {
	auto &type = options.type();
	if (type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("datadog_serve options must be a STRUCT");
	}
	auto &children = StructValue::GetChildren(options);
	for (idx_t index = 0; index < children.size(); index++) {
		if (StringUtil::CIEquals(StructType::GetChildName(type, index), name)) {
			result = children[index];
			return true;
		}
	}
	return false;
}

template <class T>
static bool ReadOption(const Value &options, const string &name, T &target) {
	Value value;
	if (!TryGetOption(options, name, value)) {
		return false;
	}
	if (value.IsNull()) {
		throw InvalidInputException("datadog_serve option '%s' must not be NULL", name);
	}
	target = value.GetValue<T>();
	return true;
}

static DatadogServerConfig ParseOptions(ClientContext &context, const Value &options) {
	DatadogServerConfig config;
	config.http_threads = DefaultHttpThreads();
	bool has_schema_name = ReadOption(options, "schema_name", config.schema_name);
	string legacy_schema_name;
	if (ReadOption(options, "schema", legacy_schema_name)) {
		if (has_schema_name) {
			throw InvalidInputException("datadog_serve options 'schema_name' and 'schema' are aliases; use only one");
		}
		config.schema_name = legacy_schema_name;
	}
	bool has_table_name = ReadOption(options, "table_name", config.table_name);
	string legacy_table_name;
	if (ReadOption(options, "table", legacy_table_name)) {
		if (has_table_name) {
			throw InvalidInputException("datadog_serve options 'table_name' and 'table' are aliases; use only one");
		}
		config.table_name = legacy_table_name;
	}
	ReadOption(options, "api_key", config.api_key);
	ReadOption(options, "allow_other_hostname", config.allow_other_hostname);
	ReadOption(options, "create_table", config.create_table);
	ReadOption(options, "max_body_bytes", config.max_body_bytes);
	ReadOption(options, "http_threads", config.http_threads);
	string secret_name;
	if (ReadOption(options, "secret", secret_name)) {
		if (!config.api_key.empty()) {
			throw InvalidInputException("datadog_serve options 'api_key' and 'secret' are mutually exclusive");
		}
		config.api_key = GetDatadogCredentials(context, secret_name).api_key;
	}
	static const std::unordered_set<string> valid = {
	    "schema_name",          "schema",       "table_name",     "table",       "api_key", "secret",
	    "allow_other_hostname", "create_table", "max_body_bytes", "http_threads"};
	for (idx_t index = 0; index < StructType::GetChildCount(options.type()); index++) {
		auto name = StructType::GetChildName(options.type(), index);
		if (valid.find(StringUtil::Lower(name)) == valid.end()) {
			throw InvalidInputException("Unsupported datadog_serve option '%s'", name);
		}
	}
	return config;
}

static void DatadogServe(DataChunk &arguments, ExpressionState &state, Vector &result) {
	if (!arguments.AllConstant()) {
		throw InvalidInputException("datadog_serve arguments must be constant");
	}
	string uri_text = "datadog:localhost:10518";
	if (arguments.ColumnCount() >= 1) {
		auto value = arguments.GetValue(0, 0);
		if (value.IsNull() || value.GetValue<string>().empty()) {
			throw InvalidInputException("Datadog listen URI must not be NULL or empty");
		}
		uri_text = value.GetValue<string>();
	}
	DatadogServerConfig config;
	config.http_threads = DefaultHttpThreads();
	if (arguments.ColumnCount() == 2) {
		auto value = arguments.GetValue(1, 0);
		if (value.IsNull()) {
			throw InvalidInputException("datadog_serve API key or options must not be NULL");
		}
		if (value.type().id() == LogicalTypeId::VARCHAR) {
			config.api_key = value.GetValue<string>();
			if (config.api_key.empty()) {
				throw InvalidInputException("datadog_serve API key must not be empty");
			}
			// The string shorthand is intended for Agents outside the DuckDB process (most commonly a
			// local container). Requiring an API key keeps the convenient non-loopback bind authenticated.
			config.allow_other_hostname = true;
		} else {
			config = ParseOptions(state.GetContext(), value);
		}
	}
	DatadogListenUri uri(uri_text);
	auto db = state.GetContext().db;
	if (!db) {
		throw InternalException("Database was closed");
	}
	auto url = DatadogServerExtensionInfo::Get(*db).Start(state.GetContext(), uri, config);
	result.SetValue(0, Value(url));
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

static void DatadogStop(DataChunk &arguments, ExpressionState &state, Vector &result) {
	if (!arguments.AllConstant()) {
		throw InvalidInputException("datadog_stop arguments must be constant");
	}
	string uri_text = "datadog:localhost:10518";
	if (arguments.ColumnCount() == 1) {
		auto value = arguments.GetValue(0, 0);
		if (value.IsNull() || value.GetValue<string>().empty()) {
			throw InvalidInputException("Datadog listen URI must not be NULL or empty");
		}
		uri_text = value.GetValue<string>();
	}
	DatadogListenUri uri(uri_text);
	auto db = state.GetContext().db;
	if (!db) {
		throw InternalException("Database was closed");
	}
	auto stopped = DatadogServerExtensionInfo::Get(*db).Stop(uri);
	result.SetValue(0, Value(stopped ? "stopped" : "not found"));
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

} // namespace

void RegisterDatadogServerState(ExtensionLoader &loader) {
	auto extension = make_shared_ptr<StorageExtension>();
	extension->storage_info = make_shared_ptr<DatadogServerExtensionInfo>();
	StorageExtension::Register(loader.GetDatabaseInstance().config, DatadogServerExtensionInfo::KEY, extension);
}

void RegisterDatadogServerFunctions(ExtensionLoader &loader) {
	ScalarFunctionSet serve("datadog_serve");
	for (auto &arguments :
	     vector<vector<LogicalType>> {{}, {LogicalType::VARCHAR}, {LogicalType::VARCHAR, LogicalType::ANY}}) {
		ScalarFunction function(arguments, LogicalType::VARCHAR, DatadogServe);
		function.SetStability(FunctionStability::VOLATILE);
		function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		serve.AddFunction(function);
	}
	loader.RegisterFunction(serve);

	ScalarFunctionSet stop("datadog_stop");
	for (auto &arguments : vector<vector<LogicalType>> {{}, {LogicalType::VARCHAR}}) {
		ScalarFunction function(arguments, LogicalType::VARCHAR, DatadogStop);
		function.SetStability(FunctionStability::VOLATILE);
		function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		stop.AddFunction(function);
	}
	loader.RegisterFunction(stop);
}

} // namespace duckdb
