#pragma once

#include "duckdb.hpp"

#ifndef __EMSCRIPTEN__
#include <mutex>
#include <vector>

//! Forward-declared so the native-only httplib header stays out of this public header. The
//! namespace name matches cpp-httplib's OpenSSL build selected by CMake.
namespace duckdb_httplib_openssl {
class Client;
}
#endif

namespace duckdb {
class ClientContext;

//! Minimal client for the Datadog APIs used by this extension: log search/index discovery,
//! triggered monitor-group search, and APM service dependencies. It owns shared
//! authentication, transport, timeout, retry, and cancellation behavior; pagination and JSON
//! mapping live outside the client. A single keep-alive connection is reused across calls.
struct DatadogClient {
	//! Datadog site, e.g. "datadoghq.com", "datadoghq.eu", "us5.datadoghq.com". Requests go to
	//! https://api.<site>. Browser builds additionally accept a full URL for a same-origin proxy.
	string site = "datadoghq.com";
	string api_key;
	string app_key;
	//! Optional intake base URL override (default: https://http-intake.logs.<site>). Accepts a
	//! bare origin or a full intake URL (a trailing /api/v2/logs is stripped), so the value
	//! `datadog_serve()` returns can be pasted verbatim.
	string intake_url;
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

	//! Copy the configuration (not the live connection) into `target`. Centralizes the field list so
	//! adding a config field does not silently miss a hand-rolled copy at a bind site. DatadogClient
	//! is intentionally non-copyable because it owns a live socket, so callers that need a configured
	//! clone (e.g. FunctionData::Copy) default-construct one and call this.
	void CopyConfigTo(DatadogClient &target) const;

	//! POST `request_body_json` to /api/v2/logs/events/search and return the raw response body.
	//! Transparently retries transient failures (429 / 5xx / transport errors) up to `retries`
	//! times, sleeping in small slices so query interrupts (Ctrl+C) cancel the wait promptly.
	//! Throws IOException when retries are exhausted or the failure is not transient, and
	//! InterruptException if the query was cancelled. Successive calls reuse the same HTTP
	//! connection.
	string SearchLogs(ClientContext &context, const string &request_body_json) const;
	//! POST `request_body_json` to /api/v2/logs/analytics/aggregate and return the raw response
	//! body. Authentication, retries, backoff, cancellation, and TLS behavior are shared with
	//! SearchLogs; one aggregation request replaces a paginated scan for count-style questions.
	string AggregateLogs(ClientContext &context, const string &request_body_json) const;
	//! GET one v1 metrics query window; the query text is percent-encoded by the client.
	string QueryMetrics(ClientContext &context, const string &query, int64_t from, int64_t to) const;

	//! GET one page of currently triggered monitor groups (Alert, Warn, or No Data) from
	//! /api/v1/monitor/groups/search. The returned JSON is parsed by the catalog table scan.
	string SearchOpenAlerts(ClientContext &context, int64_t page, int64_t per_page) const;

	//! GET the APM service-dependency graph for one environment, optional primary tag, and
	//! epoch-second window from /api/v1/service_dependencies.
	string GetServiceDependencies(ClientContext &context, const string &environment, const string &primary_tag,
	                              int64_t start_epoch_seconds, int64_t end_epoch_seconds) const;

	//! GET /api/v1/logs/config/indexes and return the raw response body. Authentication,
	//! connection pooling, timeouts, retries, cancellation, and TLS behavior are shared with
	//! SearchLogs. Permission failures include guidance about logs_read_config and INDEXES.
	string ListLogIndexes(ClientContext &context) const;

	//! POST `intake_body_json` (a JSON array of logs) to the log intake API at
	//! https://http-intake.logs.<site>/api/v2/logs (or the intake_url override) and return the raw
	//! response body. Unlike the search/config endpoints this host uses only DD-API-KEY (no
	//! application key). Shares the same retry, backoff, cancellation, and TLS behavior; a
	//! successful send returns HTTP 202. Native builds gzip the request body and may run several
	//! sends concurrently, each on its own pooled connection.
	string SendLogs(ClientContext &context, const string &intake_body_json) const;

private:
#ifndef __EMSCRIPTEN__
	//! Lazily created on first use and reused (HTTP keep-alive) for every later request. Mutable
	//! because request methods are const — scans share const bind data — yet
	//! must cache the socket. Reset (and re-established on the next request) after a transport
	//! error, since the failure may have left the pooled socket in a broken state.
	mutable unique_ptr<duckdb_httplib_openssl::Client> connection;

	//! Pool of idle keep-alive connections to the log intake host (http-intake.logs.<site> or the
	//! intake_url override), which is a different origin from the api.<site> host the search/config
	//! endpoints use. The send_datadog_logs scalar can run on several projection threads; each send
	//! checks a connection out of the pool (creating one when the pool is empty), so batches upload
	//! in parallel instead of serializing on one socket. The mutex guards only the pool itself.
	//! Connections that hit a transport error are dropped rather than returned.
	mutable std::mutex intake_pool_mutex;
	mutable std::vector<unique_ptr<duckdb_httplib_openssl::Client>> intake_pool;

	//! Return the shared connection, creating and configuring it on the first call.
	duckdb_httplib_openssl::Client &GetConnection() const;
	//! Create a fresh, configured connection to the intake host.
	unique_ptr<duckdb_httplib_openssl::Client> NewIntakeConnection() const;
	//! Check an intake connection out of the pool, creating one when the pool is empty.
	unique_ptr<duckdb_httplib_openssl::Client> AcquireIntakeConnection() const;
	//! Return a healthy intake connection to the pool for reuse.
	void ReleaseIntakeConnection(unique_ptr<duckdb_httplib_openssl::Client> intake_connection) const;
#endif

	//! Perform an authenticated GET or POST. A null body selects GET; otherwise POST JSON.
	string AuthenticatedRequest(ClientContext &context, const string &path, const string *body,
	                            bool index_discovery) const;
};

} // namespace duckdb
