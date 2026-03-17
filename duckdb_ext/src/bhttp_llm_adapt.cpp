#include "duckdb_extension.h"
#include "bhttp_llm.hpp"
#include "bhttp_llm_adapt.hpp"
#include "http_config.hpp"
#include "rate_limiter.hpp"

#include <sstream>
#include <string>
#include <vector>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jmespath/jmespath.hpp>

DUCKDB_EXTENSION_EXTERN

namespace blobhttp {

// LlmResult and LlmCompleteLoop are declared in bhttp_llm.hpp

// ---------------------------------------------------------------------------
// _llm_adapt_raw(config_json) -> VARCHAR (kept unprefixed — llm_* names are independent)
//
// Single-argument volatile scalar. Receives a fully-resolved JSON blob:
//
//   Well-known keys:
//     prompt_text        (required) Rendered prompt — already filled by
//                        bt_template_render() in the SQL macro layer.
//     output_schema      (optional) JSON Schema for tool-call validation.
//     response_jmespath  (optional) JMESPath to reshape the LLM output.
//     endpoint           Chat completions URL.
//     model              Model identifier (e.g. "anthropic/claude-haiku-4-5-20251001").
//     max_tokens         Max tokens per LLM call.
//     max_continuations  Max continuation round-trips on finish_reason=="length".
//     max_retries        Max retries on schema validation failure.
//     http_config        Object of scope->config entries for auth/rate-limiting.
//
// Everything else in the blob is ignored by this function but was available
// to the bt_template_render() call that produced prompt_text.
//
// This function never touches the database.
// ---------------------------------------------------------------------------

static void LlmAdaptRawFunc(duckdb_function_info info,
                             duckdb_data_chunk input,
                             duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	if (input_size == 0) return;

	duckdb_vector config_vec = duckdb_data_chunk_get_vector(input, 0);
	auto *validity = duckdb_vector_get_validity(config_vec);

	for (idx_t row = 0; row < input_size; row++) {
		if (validity && !(validity[row / 64] & (1ULL << (row % 64)))) {
			duckdb_scalar_function_set_error(info, "config must not be NULL");
			return;
		}

		auto *data = (duckdb_string_t *)duckdb_vector_get_data(config_vec);
		auto str = duckdb_string_t_data(&data[row]);
		auto len = duckdb_string_t_length(data[row]);
		std::string config_str(str, len);

		try {
			auto cfg = nlohmann::json::parse(config_str);

			// Read prompt_text (required — rendered upstream by bt_template_render)
			std::string prompt_text = cfg.value("prompt_text", "");
			if (prompt_text.empty()) {
				throw std::runtime_error("prompt_text is required in config");
			}

			// Infrastructure fields
			std::string endpoint = cfg.value("endpoint",
			    "http://localhost:8080/v1/chat/completions");
			std::string model = cfg.value("model",
			    "anthropic/claude-haiku-4-5-20251001");
			std::string output_schema = cfg.value("output_schema", "");
			std::string response_jmespath = cfg.value("response_jmespath", "");

			// Integers may arrive as strings from SQL json_object
			auto parse_int = [&cfg](const char *key, int def) -> int {
				if (!cfg.contains(key)) return def;
				auto &v = cfg[key];
				if (v.is_number()) return v.get<int>();
				if (v.is_string()) {
					try { return std::stoi(v.get<std::string>()); }
					catch (...) {}
				}
				return def;
			};
			int max_tokens = parse_int("max_tokens", 4096);
			int max_continuations = parse_int("max_continuations", 10);
			int max_retries = parse_int("max_retries", 3);

			// Resolve http_config for auth / rate limiting
			std::vector<std::pair<std::string, std::string>> http_config_entries;
			if (cfg.contains("http_config") && cfg["http_config"].is_object()) {
				for (auto &[k, v] : cfg["http_config"].items()) {
					http_config_entries.emplace_back(
					    k, v.is_string() ? v.get<std::string>() : v.dump());
				}
			}
			auto config = ResolveConfig(endpoint, http_config_entries);
			std::vector<std::pair<std::string, std::string>> params;
			ResolveVaultSecrets(config, params);
			std::vector<std::pair<std::string, std::string>> extra_headers;

			// Build LLM request
			nlohmann::json body = {
			    {"model", model},
			    {"max_tokens", max_tokens},
			    {"messages", nlohmann::json::array({
			        {{"role", "user"}, {"content", prompt_text}}
			    })}
			};

			auto llm_result = LlmCompleteLoop(
			    endpoint, std::move(body), config, extra_headers,
			    output_schema, max_continuations, max_retries);

			// Apply response_jmespath (jsoncons, not blobtemplates)
			std::string content_result = llm_result.content;
			if (!response_jmespath.empty()) {
				auto doc = jsoncons::json::parse(content_result);
				auto jmes_result = jsoncons::jmespath::search(
				    doc, response_jmespath);
				std::ostringstream oss;
				oss << jmes_result;
				content_result = oss.str();
			}

			// Build result: {data: <content>, _meta: <stats>}
			nlohmann::json result_obj;
			try {
				result_obj["data"] = nlohmann::json::parse(content_result);
			} catch (...) {
				result_obj["data"] = content_result;
			}
			result_obj["_meta"] = llm_result.stats.ToJson();

			auto final_str = result_obj.dump();
			duckdb_vector_assign_string_element_len(
			    output, row, final_str.c_str(), final_str.length());

		} catch (const std::exception &e) {
			duckdb_scalar_function_set_error(info, e.what());
			return;
		}
	}
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

void RegisterLlmAdaptFunction(duckdb_connection connection) {
	duckdb_scalar_function func = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(func, "_llm_adapt_raw");

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_add_parameter(func, varchar_type);
	duckdb_scalar_function_set_return_type(func, varchar_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_scalar_function_set_function(func, LlmAdaptRawFunc);
	duckdb_scalar_function_set_volatile(func);

	duckdb_register_scalar_function(connection, func);
	duckdb_destroy_scalar_function(&func);
}

} // namespace blobhttp
