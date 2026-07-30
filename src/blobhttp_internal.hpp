/*
 * blobhttp_internal.hpp — what the core's translation units share.
 *
 * Not installed and not part of the ABI: include/blobhttp.h is the only thing
 * adapters see. This exists because the HTTP machinery and the LLM completion
 * loop are separate .cpp files but the loop needs the same rate limiters,
 * error slot and config resolution as an ordinary request — it is a sequence
 * of ordinary requests.
 */
#pragma once

#include "http_config.hpp"
#include "rate_limiter.hpp"

#include <string>
#include <utility>
#include <vector>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

namespace blobhttp {

using Pairs = std::vector<std::pair<std::string, std::string>>;

// ── Error slot (thread-local; see blobhttp_core.cpp) ─────────────────
void SetError(const std::string &msg);
void ClearError();
char *DupString(const std::string &s);

// ── Process-wide limiter state ───────────────────────────────────────
RateLimiterRegistry &Registry();
GCRARateLimiter *GlobalLimiter(const std::string &spec, double burst);
GCRARateLimiter *GlobalLimiterSnapshot();
void AcquireRateLimit(GCRARateLimiter *limiter);
void RecordResponseStats(const cpr::Response &response, const std::string &host);

// ── Helpers ──────────────────────────────────────────────────────────
std::string ExtractHost(const std::string &url);
Pairs ParseJsonObject(const char *json);

// ── LLM completion ───────────────────────────────────────────────────
//
// Stats accumulated across every HTTP round-trip in one logical LLM call: a
// single llm_complete() may be several requests once continuations and schema
// retries are counted, and the caller should be able to see that.

struct LlmStats {
    int http_requests = 0;
    int continuations = 0;
    int retries = 0;
    int prompt_tokens = 0;
    int completion_tokens = 0;
    int total_tokens = 0;
    double elapsed_seconds = 0.0;
    std::string model;
    std::string finish_reason;

    void AccumulateUsage(const nlohmann::json &response, double elapsed);
    nlohmann::json ToJson() const;
};

struct LlmResult {
    std::string content;
    LlmStats stats;
};

/// Post a chat completion, continuing while the model stops on length and
/// retrying while the output fails `output_schema_str`, feeding the validation
/// errors back as correction. Throws on unrecoverable failure.
LlmResult LlmCompleteLoop(const std::string &url,
                          nlohmann::json body,
                          const HttpConfig &config,
                          const Pairs &extra_headers,
                          const std::string &output_schema_str,
                          int max_continuations,
                          int max_retries);

} // namespace blobhttp
