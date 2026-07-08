#pragma once

#include "duckdb.hpp"

//! Forward-declared so the (large) httplib header stays out of this public header. The namespace
//! name matches cpp-httplib's OpenSSL build, which CMake selects globally via CPPHTTPLIB_OPENSSL_SUPPORT.
namespace duckdb_httplib_openssl {
class Client;
}

namespace duckdb {

//! Minimal client for the Datadog Logs Search API v2. It knows how to authenticate and POST search
//! requests; pagination and JSON mapping live in the table function. A single keep-alive connection
//! is reused across calls, so paginating a query pays the TCP+TLS handshake cost only once.
struct DatadogClient {
	//! Datadog site, e.g. "datadoghq.com", "datadoghq.eu", "us5.datadoghq.com". Requests go to
	//! https://api.<site>.
	string site = "datadoghq.com";
	string api_key;
	string app_key;
	//! Per-request connection/read timeout.
	uint64_t timeout_seconds = 60;
	//! On HTTP 429 (rate limited), retry the request up to this many times, waiting the server-advised
	//! delay (Datadog's X-RateLimit-Reset, or Retry-After) between attempts. 0 disables retrying.
	uint64_t rate_limit_retries = 4;

	// Owns a live keep-alive connection (the unique_ptr below), so the type is non-copyable. It is
	// only ever default-constructed in place inside the table function's bind data — never copied
	// or moved — so the implicitly-deleted copy/move are fine. The constructor and destructor are
	// declared here and defined out-of-line so the unique_ptr may hold a forward-declared
	// (incomplete) Client; both must live where Client is complete.
	DatadogClient();
	~DatadogClient();

	//! POST `request_body_json` to /api/v2/logs/events/search and return the raw response body.
	//! Transparently retries HTTP 429 responses (honoring the server's reset delay) up to
	//! `rate_limit_retries` times. Throws IOException on transport failure or a non-2xx status that
	//! is not a retryable 429. Successive calls reuse the same HTTP connection.
	string SearchLogs(const string &request_body_json) const;

private:
	//! Lazily created on first use and reused (HTTP keep-alive) for every later request. Mutable
	//! because SearchLogs is const — it runs against the const bind data shared by all scans — yet
	//! must cache the socket.
	mutable unique_ptr<duckdb_httplib_openssl::Client> connection;

	//! Return the shared connection, creating and configuring it on the first call.
	duckdb_httplib_openssl::Client &GetConnection() const;
};

} // namespace duckdb
