#pragma once

#include "datadog_json.hpp"

#include "duckdb.hpp"

namespace duckdb {

//! Encode spans into the protobuf `AgentPayload` accepted by Datadog's trace intake at
//! https://trace.agent.<site>/api/v0.2/traces — the wire format the OpenTelemetry collector's
//! Datadog exporter uses for agentless, API-key-authenticated trace ingestion. Spans are grouped
//! into one TraceChunk per trace (first-seen order, 128-bit ids kept distinct) with sampling
//! priority 1 (keep). Field numbers follow pkg/proto/datadog/trace in DataDog/datadog-agent.
//! `hostname` and `env` stamp the TracerPayload when non-empty. `trace_count` receives the number
//! of traces for the X-Datadog-Trace-Count request header.
string EncodeDatadogAgentPayload(const DatadogAgentSpan *spans, idx_t count, const string &hostname, const string &env,
                                 idx_t &trace_count);
inline string EncodeDatadogAgentPayload(const vector<DatadogAgentSpan> &spans, const string &hostname,
                                        const string &env, idx_t &trace_count) {
	return EncodeDatadogAgentPayload(spans.data(), spans.size(), hostname, env, trace_count);
}

} // namespace duckdb
