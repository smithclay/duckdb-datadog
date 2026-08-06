#include "datadog_trace_proto.hpp"

#include <cstring>
#include <unordered_map>
#include <utility>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Minimal protobuf wire-format writer
//
// The payload is four small nested messages with only scalar, string, and
// map<string, string|double> fields, so hand-encoding the proto3 wire format
// is a few dozen lines — far cheaper than taking a protobuf dependency for
// one endpoint. Wire types: 0 = varint, 1 = 64-bit, 2 = length-delimited.
//===--------------------------------------------------------------------===//
namespace {

static void AppendVarint(string &out, uint64_t value) {
	while (value >= 0x80) {
		out.push_back(static_cast<char>((value & 0x7F) | 0x80));
		value >>= 7;
	}
	out.push_back(static_cast<char>(value));
}

static void AppendTag(string &out, uint32_t field, uint32_t wire_type) {
	AppendVarint(out, (static_cast<uint64_t>(field) << 3) | wire_type);
}

//! Varint-typed field; omitted at 0, matching proto3 default-value semantics.
static void AppendVarintField(string &out, uint32_t field, uint64_t value) {
	if (value == 0) {
		return;
	}
	AppendTag(out, field, 0);
	AppendVarint(out, value);
}

//! int64 fields (start, duration) use the same non-zigzag varint encoding as uint64.
static void AppendInt64Field(string &out, uint32_t field, int64_t value) {
	AppendVarintField(out, field, static_cast<uint64_t>(value));
}

static void AppendLenDelimited(string &out, uint32_t field, const string &payload) {
	AppendTag(out, field, 2);
	AppendVarint(out, payload.size());
	out.append(payload);
}

//! String field; omitted when empty, matching proto3 default-value semantics.
static void AppendStringField(string &out, uint32_t field, const string &value) {
	if (value.empty()) {
		return;
	}
	AppendLenDelimited(out, field, value);
}

static void AppendDoubleField(string &out, uint32_t field, double value) {
	AppendTag(out, field, 1);
	uint64_t bits;
	static_assert(sizeof(bits) == sizeof(value), "double must be 64-bit");
	memcpy(&bits, &value, sizeof(bits));
	for (int i = 0; i < 8; i++) {
		out.push_back(static_cast<char>((bits >> (8 * i)) & 0xFF)); // little-endian
	}
}

//! A protobuf map<K, V> is a repeated embedded message with key = field 1, value = field 2.
static void AppendStringMapEntry(string &out, uint32_t field, const string &key, const string &value) {
	string entry;
	AppendStringField(entry, 1, key);
	AppendStringField(entry, 2, value);
	AppendLenDelimited(out, field, entry);
}

static void AppendDoubleMapEntry(string &out, uint32_t field, const string &key, double value) {
	string entry;
	AppendStringField(entry, 1, key);
	AppendDoubleField(entry, 2, value);
	AppendLenDelimited(out, field, entry);
}

//! pb.Span (pkg/proto/datadog/trace/span.proto).
static string EncodeSpan(const DatadogAgentSpan &span) {
	string out;
	AppendStringField(out, 1, span.service);
	AppendStringField(out, 2, span.name);
	AppendStringField(out, 3, span.resource);
	AppendVarintField(out, 4, span.trace_id);
	AppendVarintField(out, 5, span.span_id);
	if (span.has_parent_id) {
		AppendVarintField(out, 6, span.parent_id);
	}
	AppendInt64Field(out, 7, span.start_ns);
	AppendInt64Field(out, 8, span.duration_ns);
	if (span.error) {
		AppendVarintField(out, 9, 1);
	}
	vector<std::pair<string, string>> meta;
	vector<std::pair<string, double>> metrics;
	ResolveDatadogAgentSpanAttributes(span, meta, metrics);
	for (const auto &pair : meta) {
		AppendStringMapEntry(out, 10, pair.first, pair.second);
	}
	for (const auto &pair : metrics) {
		AppendDoubleMapEntry(out, 11, pair.first, pair.second);
	}
	AppendStringField(out, 12, span.type);
	return out;
}

} // namespace

string EncodeDatadogAgentPayload(const DatadogAgentSpan *spans, idx_t count, const string &hostname, const string &env,
                                 idx_t &trace_count) {
	// pb.TraceChunk (field 3 = spans), one per trace in first-seen order. Priority 1 (sampler
	// keep) tells the backend these spans were deliberately sent, not sampled out.
	vector<string> chunks;
	std::unordered_map<string, idx_t> chunk_by_trace;
	trace_count = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto &span = spans[i];
		auto trace_key = span.trace_id_high_hex + ":" + std::to_string(span.trace_id);
		auto entry = chunk_by_trace.find(trace_key);
		idx_t chunk_index;
		if (entry == chunk_by_trace.end()) {
			chunk_index = chunks.size();
			chunks.emplace_back();
			AppendVarintField(chunks[chunk_index], 1, 1); // priority = 1 (keep)
			chunk_by_trace.emplace(std::move(trace_key), chunk_index);
			trace_count++;
		} else {
			chunk_index = entry->second;
		}
		AppendLenDelimited(chunks[chunk_index], 3, EncodeSpan(span));
	}

	// pb.TracerPayload: language (2), chunks (6), env (8), hostname (9).
	string tracer_payload;
	AppendStringField(tracer_payload, 2, "duckdb");
	for (const auto &chunk : chunks) {
		AppendLenDelimited(tracer_payload, 6, chunk);
	}
	AppendStringField(tracer_payload, 8, env);
	AppendStringField(tracer_payload, 9, hostname);

	// pb.AgentPayload: hostName (1), env (2), tracerPayloads (5), agentVersion (7).
	string payload;
	AppendStringField(payload, 1, hostname);
	AppendStringField(payload, 2, env);
	AppendLenDelimited(payload, 5, tracer_payload);
	AppendStringField(payload, 7, "duckdb-datadog");
	return payload;
}

} // namespace duckdb
