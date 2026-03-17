#include "duckdb_extension.h"
#include "bhttp_llm.hpp"
#include "http_config.hpp"
#include "rate_limiter.hpp"

#include <chrono>
#include <sstream>
#include <string>
#include <vector>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

DUCKDB_EXTENSION_EXTERN

namespace blobhttp {

// ---------------------------------------------------------------------------
// Rate limiter access
// ---------------------------------------------------------------------------

extern RateLimiterRegistry &GetRateLimiterRegistry();
extern GCRARateLimiter *GetGlobalLimiter(const std::string &spec, double burst);
extern void AcquireRateLimit(GCRARateLimiter *limiter);
extern void RecordResponseStats(const cpr::Response &response, const std::string &host);

// ---------------------------------------------------------------------------
// LlmStats methods (declared in bhttp_llm.hpp)
// ---------------------------------------------------------------------------

void LlmStats::AccumulateUsage(const nlohmann::json &response, double elapsed) {
	http_requests++;
	elapsed_seconds += elapsed;

	if (response.contains("usage")) {
		auto &u = response["usage"];
		prompt_tokens += u.value("prompt_tokens", 0);
		completion_tokens += u.value("completion_tokens", 0);
		total_tokens += u.value("total_tokens", 0);
	}
	if (response.contains("model")) {
		model = response["model"].get<std::string>();
	}
}

nlohmann::json LlmStats::ToJson() const {
	return {
	    {"http_requests", http_requests},
	    {"continuations", continuations},
	    {"retries", retries},
	    {"prompt_tokens", prompt_tokens},
	    {"completion_tokens", completion_tokens},
	    {"total_tokens", total_tokens},
	    {"elapsed_seconds", elapsed_seconds},
	    {"model", model},
	    {"finish_reason", finish_reason}
	};
}

// ---------------------------------------------------------------------------
// Core: LLM completion with continuation and optional schema validation
// ---------------------------------------------------------------------------

//! Execute a single POST to the chat completions endpoint.
//! Returns the parsed JSON response and the wall-clock elapsed time.
static std::pair<nlohmann::json, double> PostChatCompletion(
    const std::string &url,
    const nlohmann::json &body,
    const HttpConfig &config,
    const std::vector<std::pair<std::string, std::string>> &extra_headers) {

	auto session = std::make_shared<cpr::Session>();
	session->SetUrl(cpr::Url{url});
	session->SetTimeout(cpr::Timeout{config.timeout * 1000});

	cpr::Header cpr_headers;
	cpr_headers["Content-Type"] = "application/json";

	if (config.auth_type == "bearer" && !config.bearer_token.empty()) {
		cpr_headers["Authorization"] = "Bearer " + config.bearer_token;
	}
	for (auto &[k, v] : extra_headers) {
		cpr_headers[k] = v;
	}

	session->SetHeader(cpr_headers);

	if (!config.verify_ssl) {
		session->SetVerifySsl(cpr::VerifySsl{false});
	}
	if (!config.ca_bundle.empty() || !config.client_cert.empty() || !config.client_key.empty()) {
		cpr::SslOptions ssl_opts;
		if (!config.ca_bundle.empty()) ssl_opts.SetOption(cpr::ssl::CaInfo{config.ca_bundle});
		if (!config.client_cert.empty()) ssl_opts.SetOption(cpr::ssl::CertFile{config.client_cert});
		if (!config.client_key.empty()) ssl_opts.SetOption(cpr::ssl::KeyFile{config.client_key});
		session->SetSslOptions(ssl_opts);
	}
	if (!config.proxy.empty()) {
		session->SetProxies(cpr::Proxies{{"http", config.proxy}, {"https", config.proxy}});
	}

	auto body_str = body.dump();
	session->SetBody(cpr::Body{body_str});

	auto host = ExtractHostFromUrl(url);
	AcquireRateLimit(GetGlobalLimiter(config.global_rate_limit_spec, config.global_burst));
	AcquireRateLimit(GetRateLimiterRegistry().GetOrCreate(
	    host, config.rate_limit_spec, config.burst));

	auto response = session->Post();
	RecordResponseStats(response, host);

	if (response.status_code != 200) {
		throw std::runtime_error(
		    "LLM request failed (HTTP " + std::to_string(response.status_code) +
		    "): " + response.text.substr(0, 500));
	}

	return {nlohmann::json::parse(response.text), response.elapsed};
}

static std::string ExtractContent(const nlohmann::json &response) {
	auto &choice = response["choices"][0];
	auto &message = choice["message"];
	if (message.contains("content") && !message["content"].is_null()) {
		return message["content"].get<std::string>();
	}
	if (message.contains("tool_calls") && !message["tool_calls"].empty()) {
		return message["tool_calls"][0]["function"]["arguments"].get<std::string>();
	}
	return "";
}

static std::string ExtractFinishReason(const nlohmann::json &response) {
	auto &choice = response["choices"][0];
	if (choice.contains("finish_reason") && !choice["finish_reason"].is_null()) {
		return choice["finish_reason"].get<std::string>();
	}
	return "";
}

//! The main completion loop. Returns content + accumulated stats.
LlmResult LlmCompleteLoop(
    const std::string &url,
    nlohmann::json body,
    const HttpConfig &config,
    const std::vector<std::pair<std::string, std::string>> &extra_headers,
    const std::string &output_schema_str,
    int max_continuations,
    int max_retries) {

	LlmStats stats;

	// --- Phase 1: Schema setup ---
	bool use_schema = !output_schema_str.empty();
	std::unique_ptr<jsoncons::jsonschema::json_schema<jsoncons::json>> compiled_schema;
	nlohmann::json schema_json;

	if (use_schema) {
		schema_json = nlohmann::json::parse(output_schema_str);

		nlohmann::json tool = {
		    {"type", "function"},
		    {"function", {
		        {"name", "extract"},
		        {"parameters", schema_json}
		    }}
		};
		body["tools"] = nlohmann::json::array({tool});
		body["tool_choice"] = {
		    {"type", "function"},
		    {"function", {{"name", "extract"}}}
		};

		auto jc_schema = jsoncons::json::parse(output_schema_str);
		compiled_schema = std::make_unique<jsoncons::jsonschema::json_schema<jsoncons::json>>(
		    jsoncons::jsonschema::make_json_schema(std::move(jc_schema)));
	}

	// --- Phase 2: Completion with continuation ---
	auto do_complete = [&](nlohmann::json &req_body) -> std::string {
		std::string accumulated;

		for (int cont = 0; cont < max_continuations; cont++) {
			auto [response, elapsed] = PostChatCompletion(url, req_body, config, extra_headers);
			stats.AccumulateUsage(response, elapsed);

			auto fragment = ExtractContent(response);
			auto finish_reason = ExtractFinishReason(response);

			accumulated += fragment;

			if (finish_reason != "length") {
				stats.finish_reason = finish_reason;
				return accumulated;
			}

			stats.continuations++;
			req_body["messages"].push_back({
			    {"role", "assistant"},
			    {"content", accumulated}
			});
			req_body["messages"].push_back({
			    {"role", "user"},
			    {"content", "Continue exactly where you left off."}
			});
		}

		throw std::runtime_error(
		    "LLM continuation limit (" + std::to_string(max_continuations) +
		    ") reached — response still incomplete");
	};

	// --- Phase 3: Validate + retry loop ---
	if (!use_schema) {
		LlmResult r;
		r.content = do_complete(body);
		r.stats = stats;
		return r;
	}

	for (int attempt = 0; attempt < max_retries; attempt++) {
		nlohmann::json attempt_body = body;
		auto result = do_complete(attempt_body);

		try {
			auto parsed = jsoncons::json::parse(result);

			jsoncons::json_decoder<jsoncons::json> decoder;
			compiled_schema->validate(parsed, decoder);
			auto output = decoder.get_result();

			if (output.is_object() && output.contains("valid") &&
			    !output["valid"].as<bool>()) {
				std::ostringstream oss;
				oss << jsoncons::pretty_print(output);
				std::string error_text = oss.str();
				if (error_text.size() > 2000) {
					error_text = error_text.substr(0, 2000) + "...";
				}

				stats.retries++;
				body["messages"].push_back({
				    {"role", "assistant"},
				    {"content", result}
				});
				body["messages"].push_back({
				    {"role", "user"},
				    {"content", "Validation errors:\n" + error_text +
				                "\nFix the errors and try again."}
				});
				continue;
			}

			LlmResult r;
			r.content = result;
			r.stats = stats;
			return r;

		} catch (const jsoncons::ser_error &e) {
			stats.retries++;
			body["messages"].push_back({
			    {"role", "assistant"},
			    {"content", result}
			});
			body["messages"].push_back({
			    {"role", "user"},
			    {"content", std::string("Invalid JSON: ") + e.what() +
			                "\nReturn valid JSON matching the schema."}
			});
			continue;
		}
	}

	throw std::runtime_error(
	    "Schema validation failed after " + std::to_string(max_retries) + " attempts");
}

// ---------------------------------------------------------------------------
// DuckDB scalar function: _llm_complete_raw (kept unprefixed — llm_* names are independent)
// ---------------------------------------------------------------------------

static void LlmCompleteScalarFunc(duckdb_function_info info,
                                   duckdb_data_chunk input,
                                   duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	if (input_size == 0) return;

	auto read_varchar = [](duckdb_vector vec, idx_t row) -> std::string {
		auto *validity = duckdb_vector_get_validity(vec);
		if (validity && !(validity[row / 64] & (1ULL << (row % 64)))) {
			return "";
		}
		auto *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
		auto str = duckdb_string_t_data(&data[row]);
		auto len = duckdb_string_t_length(data[row]);
		return std::string(str, len);
	};

	auto read_int = [](duckdb_vector vec, idx_t row, int default_val) -> int {
		auto *validity = duckdb_vector_get_validity(vec);
		if (validity && !(validity[row / 64] & (1ULL << (row % 64)))) {
			return default_val;
		}
		auto *data = (int32_t *)duckdb_vector_get_data(vec);
		return data[row];
	};

	duckdb_vector url_vec = duckdb_data_chunk_get_vector(input, 0);
	duckdb_vector body_vec = duckdb_data_chunk_get_vector(input, 1);
	duckdb_vector headers_vec = duckdb_data_chunk_get_vector(input, 2);
	duckdb_vector config_vec = duckdb_data_chunk_get_vector(input, 3);
	duckdb_vector schema_vec = duckdb_data_chunk_get_vector(input, 4);
	duckdb_vector max_cont_vec = duckdb_data_chunk_get_vector(input, 5);
	duckdb_vector max_retry_vec = duckdb_data_chunk_get_vector(input, 6);

	for (idx_t row = 0; row < input_size; row++) {
		auto url = read_varchar(url_vec, row);
		auto body_str = read_varchar(body_vec, row);
		auto headers_json = read_varchar(headers_vec, row);
		auto config_json = read_varchar(config_vec, row);
		auto output_schema = read_varchar(schema_vec, row);
		auto max_continuations = read_int(max_cont_vec, row, 10);
		auto max_retries = read_int(max_retry_vec, row, 3);

		if (url.empty() || body_str.empty()) {
			duckdb_scalar_function_set_error(info, "url and body are required");
			return;
		}

		try {
			std::vector<std::pair<std::string, std::string>> config_entries;
			if (!config_json.empty()) {
				auto cj = nlohmann::json::parse(config_json);
				if (cj.is_object()) {
					for (auto &[k, v] : cj.items()) {
						config_entries.emplace_back(k, v.is_string() ? v.get<std::string>() : v.dump());
					}
				}
			}
			auto config = ResolveConfig(url, config_entries);

			std::vector<std::pair<std::string, std::string>> params;
			ResolveVaultSecrets(config, params);

			std::vector<std::pair<std::string, std::string>> extra_headers;
			if (!headers_json.empty()) {
				auto hj = nlohmann::json::parse(headers_json);
				if (hj.is_object()) {
					for (auto &[k, v] : hj.items()) {
						extra_headers.emplace_back(k, v.is_string() ? v.get<std::string>() : v.dump());
					}
				}
			}

			auto body = nlohmann::json::parse(body_str);

			auto llm_result = LlmCompleteLoop(
			    url, std::move(body), config, extra_headers,
			    output_schema, max_continuations, max_retries);

			// Return just the content for the low-level scalar
			auto &result = llm_result.content;
			duckdb_vector_assign_string_element_len(
			    output, row, result.c_str(), result.length());

		} catch (const std::exception &e) {
			duckdb_scalar_function_set_error(info, e.what());
			return;
		}
	}
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

void RegisterLlmFunctions(duckdb_connection connection) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "_llm_complete_raw");

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

	duckdb_scalar_function_add_parameter(function, varchar_type);  // 0: url
	duckdb_scalar_function_add_parameter(function, varchar_type);  // 1: body (JSON)
	duckdb_scalar_function_add_parameter(function, varchar_type);  // 2: headers (JSON)
	duckdb_scalar_function_add_parameter(function, varchar_type);  // 3: config (JSON)
	duckdb_scalar_function_add_parameter(function, varchar_type);  // 4: output_schema (JSON)
	duckdb_scalar_function_add_parameter(function, int_type);      // 5: max_continuations
	duckdb_scalar_function_add_parameter(function, int_type);      // 6: max_retries

	duckdb_scalar_function_set_return_type(function, varchar_type);
	duckdb_scalar_function_set_function(function, LlmCompleteScalarFunc);
	duckdb_scalar_function_set_volatile(function);

	duckdb_register_scalar_function(connection, function);

	duckdb_destroy_logical_type(&varchar_type);
	duckdb_destroy_logical_type(&int_type);
	duckdb_destroy_scalar_function(&function);
}

} // namespace blobhttp
