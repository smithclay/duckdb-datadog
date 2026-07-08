#include "datadog_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

// Use DuckDB's bundled cpp-httplib. Defining CPPHTTPLIB_OPENSSL_SUPPORT (see CMakeLists) both
// enables TLS and selects the `duckdb_httplib_openssl` namespace, so these symbols never collide
// with core DuckDB's non-SSL `duckdb_httplib` build.
#include "httplib.hpp"

namespace duckdb {

//! Reduce a user-supplied SITE to a bare host so `https://api.<host>` is always well-formed.
//! Tolerates a leading scheme, an `api.`/`app.` prefix, surrounding whitespace, and trailing '/'.
static string NormalizeSite(const string &raw) {
	string s = raw;
	StringUtil::Trim(s);
	if (StringUtil::StartsWith(s, "https://")) {
		s = s.substr(8);
	} else if (StringUtil::StartsWith(s, "http://")) {
		s = s.substr(7);
	}
	while (!s.empty() && s.back() == '/') {
		s.pop_back();
	}
	if (StringUtil::StartsWith(s, "api.") || StringUtil::StartsWith(s, "app.")) {
		s = s.substr(4);
	}
	return s.empty() ? "datadoghq.com" : s;
}

static string BuildBaseUrl(const string &site) {
	return "https://api." + NormalizeSite(site);
}

// Defined here, where `Client` is complete, so the header's unique_ptr<Client> member can point at
// a forward-declared type. Every special member the compiler might instantiate at a use site (where
// Client is still incomplete) must be out-of-line: the default constructor and the destructor. The
// destructor uses an empty body rather than `= default` to keep clang-tidy's
// performance-trivially-destructible check quiet.
DatadogClient::DatadogClient() = default;
DatadogClient::~DatadogClient() {
}

duckdb_httplib_openssl::Client &DatadogClient::GetConnection() const {
	if (!connection) {
		connection = make_uniq<duckdb_httplib_openssl::Client>(BuildBaseUrl(site));
		connection->set_connection_timeout(static_cast<time_t>(timeout_seconds), 0);
		connection->set_read_timeout(static_cast<time_t>(timeout_seconds), 0);
		// Keep the socket open between requests so cursor pagination reuses one TCP+TLS connection
		// instead of handshaking per page. cpp-httplib defaults keep-alive off.
		connection->set_keep_alive(true);
		// No set_follow_location: the endpoint is a fixed POST; following a 3xx would forward the
		// DD-API-KEY/DD-APPLICATION-KEY headers to the redirect target and mask real non-2xx errors.
	}
	return *connection;
}

string DatadogClient::SearchLogs(const string &request_body_json) const {
	auto &client = GetConnection();

	duckdb_httplib_openssl::Headers headers = {
	    {"DD-API-KEY", api_key},
	    {"DD-APPLICATION-KEY", app_key},
	    {"Accept", "application/json"},
	};

	auto response = client.Post("/api/v2/logs/events/search", headers, request_body_json, "application/json");
	if (!response) {
		throw IOException("Datadog API request to %s failed: %s", BuildBaseUrl(site),
		                  duckdb_httplib_openssl::to_string(response.error()));
	}
	if (response->status < 200 || response->status >= 300) {
		throw IOException("Datadog API returned HTTP %d: %s", response->status, response->body);
	}
	return response->body;
}

} // namespace duckdb
