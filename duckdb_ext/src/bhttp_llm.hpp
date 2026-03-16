#pragma once

#include "duckdb_extension.h"

#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include "http_config.hpp"

namespace blobhttp {

// ---------------------------------------------------------------------------
// Stats accumulated across all HTTP round-trips in one logical LLM call.
// Pure C++ — no DuckDB dependency. Usable from SQLite extension too.
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Core completion loop — shared by DuckDB and SQLite extensions.
// ---------------------------------------------------------------------------

LlmResult LlmCompleteLoop(
    const std::string &url,
    nlohmann::json body,
    const HttpConfig &config,
    const std::vector<std::pair<std::string, std::string>> &extra_headers,
    const std::string &output_schema_str,
    int max_continuations,
    int max_retries);

// ---------------------------------------------------------------------------
// DuckDB function registration
// ---------------------------------------------------------------------------

void RegisterLlmFunctions(duckdb_connection connection);

} // namespace blobhttp
