#pragma once

#include "duckdb_extension.h"

namespace blobhttp {

// LlmStats, LlmResult and the completion loop moved to the core — see
// include/blobhttp.h (bh_llm_complete / bh_llm_adapt) and
// src/blobhttp_llm.cpp. What is left in duckdb_ext is registration and the
// marshalling of DuckDB vectors to and from the JSON the ABI takes.

void RegisterLlmFunctions(duckdb_connection connection);
void RegisterLlmAdaptFunction(duckdb_connection connection);

} // namespace blobhttp
