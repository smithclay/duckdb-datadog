#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Minimal client for the Datadog Logs Search API v2. It knows how to authenticate and POST a
//! single search request; pagination and JSON mapping live in the table function.
struct DatadogClient {
	//! Datadog site, e.g. "datadoghq.com", "datadoghq.eu", "us5.datadoghq.com". Requests go to
	//! https://api.<site>.
	string site = "datadoghq.com";
	string api_key;
	string app_key;
	//! Per-request connection/read timeout.
	uint64_t timeout_seconds = 60;

	//! POST `request_body_json` to /api/v2/logs/events/search and return the raw response body.
	//! Throws IOException on transport failure or a non-2xx status.
	string SearchLogs(const string &request_body_json) const;
};

} // namespace duckdb
