#pragma once

#include "duckdb.hpp"

//! Forward-declared so the (large) httplib header stays out of this public header. The namespace
//! name matches cpp-httplib's OpenSSL build, which CMake selects globally via CPPHTTPLIB_OPENSSL_SUPPORT.
namespace duckdb_httplib_openssl {
class Client;
}

namespace duckdb {
class ClientContext;

//! Minimal client for Datadog's log search and index configuration APIs. It owns shared
//! authentication, transport, timeout, retry, and cancellation behavior; pagination and JSON
//! mapping live outside the client. A single keep-alive connection is reused across calls.
struct DatadogClient {
	//! Datadog site, e.g. "datadoghq.com", "datadoghq.eu", "us5.datadoghq.com". Requests go to
	//! https://api.<site>.
	string site = "datadoghq.com";
	string api_key;
	string app_key;
	//! Per-request connection/read timeout.
	uint64_t timeout_seconds = 60;
	//! Retry budget for transient failures: HTTP 429 (waits the server-advised delay from
	//! X-RateLimit-Reset / Retry-After), HTTP 5xx, and transport errors (connection reset, timeout)
	//! all share this budget, using exponential backoff where the server gives no delay.
	//! 0 disables retrying. Non-transient failures (4xx other than 429, TLS certificate
	//! verification) are never retried.
	uint64_t retries = 4;

	// Owns a live keep-alive connection (the unique_ptr below), so the type is non-copyable. It is
	// only ever default-constructed in place inside the table function's bind data — never copied
	// or moved — so the implicitly-deleted copy/move are fine. The constructor and destructor are
	// declared here and defined out-of-line so the unique_ptr may hold a forward-declared
	// (incomplete) Client; both must live where Client is complete.
	DatadogClient();
	~DatadogClient();

	//! POST `request_body_json` to /api/v2/logs/events/search and return the raw response body.
	//! Transparently retries transient failures (429 / 5xx / transport errors) up to `retries`
	//! times, sleeping in small slices so query interrupts (Ctrl+C) cancel the wait promptly.
	//! Throws IOException when retries are exhausted or the failure is not transient, and
	//! InterruptException if the query was cancelled. Successive calls reuse the same HTTP
	//! connection.
	string SearchLogs(ClientContext &context, const string &request_body_json) const;

	//! GET /api/v1/logs/config/indexes and return the raw response body. Authentication,
	//! connection pooling, timeouts, retries, cancellation, and TLS behavior are shared with
	//! SearchLogs. Permission failures include guidance about logs_read_config and INDEXES.
	string ListLogIndexes(ClientContext &context) const;

private:
	//! Lazily created on first use and reused (HTTP keep-alive) for every later request. Mutable
	//! because request methods are const — scans share const bind data — yet
	//! must cache the socket. Reset (and re-established on the next request) after a transport
	//! error, since the failure may have left the pooled socket in a broken state.
	mutable unique_ptr<duckdb_httplib_openssl::Client> connection;

	//! Return the shared connection, creating and configuring it on the first call.
	duckdb_httplib_openssl::Client &GetConnection() const;

	//! Perform an authenticated GET or POST. A null body selects GET; otherwise POST JSON.
	string AuthenticatedRequest(ClientContext &context, const string &path, const string *body,
	                            bool index_discovery) const;
};

} // namespace duckdb
