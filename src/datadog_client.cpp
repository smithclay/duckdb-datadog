#include "datadog_client.hpp"

#include "datadog_json.hpp"

#include "duckdb/common/exception.hpp"
#ifdef __EMSCRIPTEN__
#include "duckdb/common/http_util.hpp"
#endif
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <cstring>

#ifndef __EMSCRIPTEN__
#include <chrono>
#include <thread>

// Use DuckDB's bundled cpp-httplib. Defining CPPHTTPLIB_OPENSSL_SUPPORT (see CMakeLists) both
// enables TLS and selects the `duckdb_httplib_openssl` namespace, so these symbols never collide
// with core DuckDB's non-SSL `duckdb_httplib` build.
#include "httplib.hpp"
#endif

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

//! Base URL for the log intake API. This is a distinct host from the search/config API
//! (`api.<site>`); by default logs are accepted at `https://http-intake.logs.<site>`. A non-empty
//! `intake_url` overrides the host entirely (e.g. a local `datadog_serve` listener or an intake
//! proxy). The override accepts a bare origin or a full intake URL: a trailing `/api/v2/logs` is
//! stripped so the value `datadog_serve()` returns can be pasted verbatim.
static string BuildIntakeBaseUrl(const string &site, const string &intake_url) {
	if (!intake_url.empty()) {
		string url = intake_url;
		StringUtil::Trim(url);
		while (!url.empty() && url.back() == '/') {
			url.pop_back();
		}
		constexpr const char *INTAKE_PATH_SUFFIX = "/api/v2/logs";
		if (StringUtil::EndsWith(url, INTAKE_PATH_SUFFIX)) {
			url.resize(url.size() - strlen(INTAKE_PATH_SUFFIX));
		}
		return url;
	}
	return "https://http-intake.logs." + NormalizeSite(site);
}

static string BuildBaseUrl(const string &site) {
#ifdef __EMSCRIPTEN__
	string browser_site = site;
	StringUtil::Trim(browser_site);
	while (!browser_site.empty() && browser_site.back() == '/') {
		browser_site.pop_back();
	}
	if (StringUtil::StartsWith(browser_site, "https://") || StringUtil::StartsWith(browser_site, "http://")) {
		return browser_site;
	}
#endif
	return "https://api." + NormalizeSite(site);
}

#ifdef __EMSCRIPTEN__
static string BrowserHTTPErrorDetail(const HTTPResponse &response) {
	string detail = response.body.empty() ? response.GetError() : response.body;
	StringUtil::Trim(detail);
	for (auto &character : detail) {
		if (character == '\r' || character == '\n' || character == '\t') {
			character = ' ';
		}
	}
	constexpr idx_t MAX_ERROR_DETAIL_LENGTH = 500;
	if (detail.size() > MAX_ERROR_DETAIL_LENGTH) {
		detail.resize(MAX_ERROR_DETAIL_LENGTH);
		detail += "...";
	}
	return detail;
}
#endif

// Native builds keep these special members out-of-line so the header can hold a unique_ptr to the
// forward-declared httplib Client. The definitions are harmless in browser builds, where no native
// connection member exists. The destructor uses an empty body rather than `= default` to keep
// clang-tidy's performance-trivially-destructible check quiet.
DatadogClient::DatadogClient() = default;
DatadogClient::~DatadogClient() {
}

void DatadogClient::CopyConfigTo(DatadogClient &target) const {
	target.site = site;
	target.api_key = api_key;
	target.app_key = app_key;
	target.intake_url = intake_url;
	target.timeout_seconds = timeout_seconds;
	target.retries = retries;
}

#ifndef __EMSCRIPTEN__
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

unique_ptr<duckdb_httplib_openssl::Client> DatadogClient::NewIntakeConnection() const {
	auto intake_connection = make_uniq<duckdb_httplib_openssl::Client>(BuildIntakeBaseUrl(site, intake_url));
	intake_connection->set_connection_timeout(static_cast<time_t>(timeout_seconds), 0);
	intake_connection->set_read_timeout(static_cast<time_t>(timeout_seconds), 0);
	intake_connection->set_keep_alive(true);
	// Datadog's intake accepts Content-Encoding: gzip; batches compress well (JSON with repeated
	// keys), so this cuts upload bytes several-fold on large sends.
	intake_connection->set_compress(true);
	return intake_connection;
}

unique_ptr<duckdb_httplib_openssl::Client> DatadogClient::AcquireIntakeConnection() const {
	{
		std::lock_guard<std::mutex> pool_guard(intake_pool_mutex);
		if (!intake_pool.empty()) {
			auto intake_connection = std::move(intake_pool.back());
			intake_pool.pop_back();
			return intake_connection;
		}
	}
	return NewIntakeConnection();
}

void DatadogClient::ReleaseIntakeConnection(unique_ptr<duckdb_httplib_openssl::Client> intake_connection) const {
	if (!intake_connection) {
		return;
	}
	std::lock_guard<std::mutex> pool_guard(intake_pool_mutex);
	intake_pool.push_back(std::move(intake_connection));
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

//! Transport errors that are guaranteed to have happened *before* any request bytes reached the
//! server, so retrying them cannot duplicate a write. The log intake endpoint is not idempotent, so
//! SendLogs only retries these (a Read/Write/Unknown error may mean the batch was already accepted
//! and its 202 response was merely lost — retrying that would double-index the logs).
static bool IsPreSendTransportError(duckdb_httplib_openssl::Error error) {
	switch (error) {
	case duckdb_httplib_openssl::Error::Connection:
	case duckdb_httplib_openssl::Error::ConnectionTimeout:
	case duckdb_httplib_openssl::Error::BindIPAddress:
	case duckdb_httplib_openssl::Error::ProxyConnection:
		return true;
	default:
		return false;
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
#endif

string DatadogClient::AuthenticatedRequest(ClientContext &context, const string &path, const string *body,
                                           bool index_discovery) const {
#ifdef __EMSCRIPTEN__
	if (context.interrupted) {
		throw InterruptException();
	}

	const auto base_url = BuildBaseUrl(site);
	const auto url = base_url + path;
	auto &http_util = HTTPUtil::Get(*context.db);
	auto params = http_util.InitializeParameters(context, url);
	params->timeout = timeout_seconds;
	params->retries = retries;
	params->keep_alive = true;
	params->follow_location = false;

	HTTPHeaders headers;
	headers.Insert("DD-API-KEY", api_key);
	headers.Insert("DD-APPLICATION-KEY", app_key);
	headers.Insert("Accept", "application/json");

	unique_ptr<HTTPResponse> response;
	if (body) {
		headers.Insert("Content-Type", "application/json");
		PostRequestInfo request(url, headers, *params, reinterpret_cast<const_data_ptr_t>(body->data()), body->size());
		request.try_request = true;
		response = http_util.Request(request);
	} else {
		GetRequestInfo request(url, headers, *params, nullptr, nullptr);
		request.try_request = true;
		response = http_util.Request(request);
	}

	if (!response) {
		throw IOException("Datadog browser request failed through %s: no response (check the proxy URL and CORS "
		                  "allowlist)",
		                  base_url);
	}
	if (!response->Success()) {
		auto status = static_cast<uint16_t>(response->status);
		if (index_discovery && (status == 401 || status == 403)) {
			throw IOException("Datadog index discovery returned HTTP %d through %s. Automatic discovery requires "
			                  "logs_read_config; attach with INDEXES ['main', ...] to bypass discovery",
			                  status, base_url);
		}
		if (response->status != HTTPStatusCode::INVALID) {
			auto detail = BrowserHTTPErrorDetail(*response);
			if (detail.empty()) {
				detail = "request rejected without a response body";
			}
			throw IOException("Datadog API returned HTTP %d through %s: %s", status, base_url, detail);
		}
		throw IOException("Datadog browser request failed through %s: %s (check the proxy URL, CORS allowlist, "
		                  "and network connection)",
		                  base_url, response->GetError());
	}
	return response->body;
#else
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
#endif
}

string DatadogClient::SearchLogs(ClientContext &context, const string &request_body_json) const {
	return AuthenticatedRequest(context, "/api/v2/logs/events/search", &request_body_json, false);
}

string DatadogClient::AggregateLogs(ClientContext &context, const string &request_body_json) const {
	return AuthenticatedRequest(context, "/api/v2/logs/analytics/aggregate", &request_body_json, false);
}

string DatadogClient::QueryMetrics(ClientContext &context, const string &query, int64_t from, int64_t to) const {
	static constexpr char HEX[] = "0123456789ABCDEF";
	string encoded;
	for (auto ch : query) { auto c = static_cast<unsigned char>(ch); if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~') encoded += char(c); else { encoded += '%'; encoded += HEX[c >> 4]; encoded += HEX[c & 15]; } }
	return AuthenticatedRequest(context, "/api/v1/query?from=" + std::to_string(from) + "&to=" + std::to_string(to) + "&query=" + encoded, nullptr, false);
}

string DatadogClient::SearchOpenAlerts(ClientContext &context, int64_t page, int64_t per_page) const {
	auto path = BuildDatadogOpenAlertsPath(page, per_page);
	return AuthenticatedRequest(context, path, nullptr, false);
}

string DatadogClient::GetServiceDependencies(ClientContext &context, const string &environment,
                                             const string &primary_tag, int64_t start_epoch_seconds,
                                             int64_t end_epoch_seconds) const {
	auto path = BuildDatadogServiceDependenciesPath(environment, primary_tag, start_epoch_seconds, end_epoch_seconds);
	return AuthenticatedRequest(context, path, nullptr, false);
}

string DatadogClient::ListLogIndexes(ClientContext &context) const {
	return AuthenticatedRequest(context, "/api/v1/logs/config/indexes", nullptr, true);
}

string DatadogClient::SendLogs(ClientContext &context, const string &intake_body_json) const {
	constexpr const char *INTAKE_PATH = "/api/v2/logs";
#ifdef __EMSCRIPTEN__
	if (context.interrupted) {
		throw InterruptException();
	}

	const auto base_url = BuildIntakeBaseUrl(site, intake_url);
	const auto url = base_url + INTAKE_PATH;
	auto &http_util = HTTPUtil::Get(*context.db);
	auto params = http_util.InitializeParameters(context, url);
	params->timeout = timeout_seconds;
	// The log intake endpoint is not idempotent, so do not let the browser transport blindly retry a
	// write whose response may have been lost — that would double-index the batch.
	params->retries = 0;
	params->keep_alive = true;
	params->follow_location = false;

	HTTPHeaders headers;
	headers.Insert("DD-API-KEY", api_key);
	headers.Insert("Content-Type", "application/json");
	headers.Insert("Accept", "application/json");

	PostRequestInfo request(url, headers, *params, reinterpret_cast<const_data_ptr_t>(intake_body_json.data()),
	                        intake_body_json.size());
	request.try_request = true;
	auto response = http_util.Request(request);

	if (!response) {
		throw IOException("Datadog log intake request failed through %s: no response (check the proxy URL and CORS "
		                  "allowlist)",
		                  base_url);
	}
	if (!response->Success()) {
		auto status = static_cast<uint16_t>(response->status);
		if (response->status != HTTPStatusCode::INVALID) {
			auto detail = BrowserHTTPErrorDetail(*response);
			if (detail.empty()) {
				detail = "request rejected without a response body";
			}
			throw IOException("Datadog log intake returned HTTP %d through %s: %s", status, base_url, detail);
		}
		throw IOException("Datadog log intake request failed through %s: %s (check the proxy URL, CORS allowlist, "
		                  "and network connection)",
		                  base_url, response->GetError());
	}
	return response->body;
#else
	// Intake authenticates with the API key only; the application key is neither required nor used.
	// Content-Type is supplied by Post()'s content_type argument below — setting it here too would
	// send a duplicate header that the intake API rejects with HTTP 415.
	duckdb_httplib_openssl::Headers headers = {
	    {"DD-API-KEY", api_key},
	    {"Accept", "application/json"},
	};

	// Each send checks its own connection out of the pool, so concurrent send_datadog_logs
	// projection threads upload batches in parallel. Only a send that completes returns its
	// connection; any exception path lets the local unique_ptr close the socket instead, so a
	// broken or interrupted connection can never re-enter the pool.
	auto intake_connection = AcquireIntakeConnection();

	for (uint64_t attempt = 0;; attempt++) {
		if (context.interrupted) {
			throw InterruptException();
		}
		auto response = intake_connection->Post(INTAKE_PATH, headers, intake_body_json, "application/json");

		if (!response) {
			auto error = response.error();
			// The socket may be half-dead after a transport error; retry on a fresh connection.
			intake_connection = NewIntakeConnection();
			// Only pre-send failures are safe to retry on this non-idempotent write endpoint.
			if (attempt >= retries || !IsPreSendTransportError(error)) {
				throw IOException("Datadog log intake request to %s failed: %s", BuildIntakeBaseUrl(site, intake_url),
				                  duckdb_httplib_openssl::to_string(error));
			}
			SleepCheckingInterrupt(context, MinValue<uint64_t>(uint64_t(1) << attempt, 60));
			continue;
		}

		if (response->status == 429 && attempt < retries) {
			SleepCheckingInterrupt(context, RateLimitRetryDelaySeconds(*response, attempt));
			continue;
		}
		if (response->status >= 500 && attempt < retries) {
			SleepCheckingInterrupt(context, MinValue<uint64_t>(uint64_t(1) << attempt, 60));
			continue;
		}
		if (response->status < 200 || response->status >= 300) {
			throw IOException("Datadog log intake returned HTTP %d: %s", response->status, response->body);
		}
		auto body = response->body;
		ReleaseIntakeConnection(std::move(intake_connection));
		return body;
	}
#endif
}

} // namespace duckdb
