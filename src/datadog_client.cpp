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

string DatadogClient::SearchLogs(const string &request_body_json) const {
	const string base_url = "https://api." + NormalizeSite(site);
	duckdb_httplib_openssl::Client client(base_url);
	client.set_connection_timeout(static_cast<time_t>(timeout_seconds), 0);
	client.set_read_timeout(static_cast<time_t>(timeout_seconds), 0);
	// No set_follow_location: the endpoint is a fixed POST; following a 3xx would forward the
	// DD-API-KEY/DD-APPLICATION-KEY headers to the redirect target and mask real non-2xx errors.

	duckdb_httplib_openssl::Headers headers = {
	    {"DD-API-KEY", api_key},
	    {"DD-APPLICATION-KEY", app_key},
	    {"Accept", "application/json"},
	};

	auto response =
	    client.Post("/api/v2/logs/events/search", headers, request_body_json, "application/json");
	if (!response) {
		throw IOException("Datadog API request to %s failed: %s", base_url,
		                  duckdb_httplib_openssl::to_string(response.error()));
	}
	if (response->status < 200 || response->status >= 300) {
		throw IOException("Datadog API returned HTTP %d: %s", response->status, response->body);
	}
	return response->body;
}

} // namespace duckdb
