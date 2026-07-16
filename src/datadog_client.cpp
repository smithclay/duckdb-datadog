#include "datadog_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <chrono>
#include <thread>

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

//! Sleep for `seconds`, polling the query's interrupt flag so a cancelled query (Ctrl+C) aborts
//! the wait within ~100ms instead of blocking a scan thread for the full retry delay.
static void SleepCheckingInterrupt(ClientContext &context, uint64_t seconds) {
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
	while (std::chrono::steady_clock::now() < deadline) {
		if (context.interrupted) {
			throw InterruptException();
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
}

//! TLS certificate/hostname failures are configuration or security problems — retrying cannot
//! succeed and would only delay (or worse, mask) the real error.
static bool IsRetryableTransportError(duckdb_httplib_openssl::Error error) {
	switch (error) {
	case duckdb_httplib_openssl::Error::SSLLoadingCerts:
	case duckdb_httplib_openssl::Error::SSLServerVerification:
	case duckdb_httplib_openssl::Error::SSLServerHostnameVerification:
		return false;
	default:
		return true;
	}
}

//! Seconds to wait before retrying a 429, based on the server's advice. Datadog sends
//! X-RateLimit-Reset (seconds until the limit resets); Retry-After is the conventional fallback.
//! If neither is present or parseable, fall back to exponential backoff. The result is clamped to
//! [1, 60] so a stray/huge header value can't stall the query, and includes a small margin so we
//! retry just after the window rolls over rather than exactly on the boundary.
static uint64_t RateLimitRetryDelaySeconds(const duckdb_httplib_openssl::Response &response, uint64_t attempt) {
	for (const char *header : {"X-RateLimit-Reset", "Retry-After"}) {
		if (!response.has_header(header)) {
			continue;
		}
		try {
			long long secs = std::stoll(response.get_header_value(header));
			if (secs < 0) {
				secs = 0;
			}
			if (secs > 59) {
				return 60;
			}
			return static_cast<uint64_t>(secs) + 1; // +1s margin to clear the reset boundary
		} catch (const std::exception &) {
			// Unparseable header (e.g. an HTTP-date Retry-After); fall back to backoff below.
		}
	}
	return MinValue<uint64_t>(uint64_t(1) << attempt, 60); // 1, 2, 4, 8, ... seconds
}

string DatadogClient::AuthenticatedRequest(ClientContext &context, const string &path, const string *body,
                                           bool index_discovery) const {
	duckdb_httplib_openssl::Headers headers = {
	    {"DD-API-KEY", api_key},
	    {"DD-APPLICATION-KEY", app_key},
	    {"Accept", "application/json"},
	};

	for (uint64_t attempt = 0;; attempt++) {
		if (context.interrupted) {
			throw InterruptException();
		}
		auto response =
		    body ? GetConnection().Post(path, headers, *body, "application/json") : GetConnection().Get(path, headers);

		if (!response) {
			auto error = response.error();
			// Drop the pooled connection: after a transport error the socket may be half-dead, and
			// reconnecting from scratch is the reliable way to retry.
			connection.reset();
			if (attempt >= retries || !IsRetryableTransportError(error)) {
				throw IOException("Datadog API request to %s failed: %s", BuildBaseUrl(site),
				                  duckdb_httplib_openssl::to_string(error));
			}
			SleepCheckingInterrupt(context, MinValue<uint64_t>(uint64_t(1) << attempt, 60));
			continue;
		}

		// Rate limited: wait out the server-advised reset (Datadog limits the search API, e.g.
		// 2 requests / 10s on some sites) instead of failing the whole query on a transient 429.
		if (response->status == 429 && attempt < retries) {
			SleepCheckingInterrupt(context, RateLimitRetryDelaySeconds(*response, attempt));
			continue;
		}
		// Server-side errors are usually transient; a long paginated scan should ride them out
		// rather than lose the cursor (there is no way to resume a scan mid-pagination).
		if (response->status >= 500 && attempt < retries) {
			SleepCheckingInterrupt(context, MinValue<uint64_t>(uint64_t(1) << attempt, 60));
			continue;
		}
		if (response->status < 200 || response->status >= 300) {
			if (index_discovery && (response->status == 401 || response->status == 403)) {
				throw IOException("Datadog index discovery returned HTTP %d. Automatic discovery requires the "
				                  "logs_read_config permission; attach with INDEXES ['main', ...] to bypass discovery",
				                  response->status);
			}
			throw IOException("Datadog API returned HTTP %d: %s", response->status, response->body);
		}
		return response->body;
	}
}

string DatadogClient::SearchLogs(ClientContext &context, const string &request_body_json) const {
	return AuthenticatedRequest(context, "/api/v2/logs/events/search", &request_body_json, false);
}

string DatadogClient::ListLogIndexes(ClientContext &context) const {
	return AuthenticatedRequest(context, "/api/v1/logs/config/indexes", nullptr, true);
}

} // namespace duckdb
