#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "blobhttp.h"
#include "blobhttp_internal.hpp"
#include "http_config.hpp"
#include "lru_pool.hpp"
#include "negotiate_auth.hpp"
#include "rate_limiter.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jmespath/jmespath.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

using namespace blobhttp;

//! Parse a JSON object string into config entries.
static std::vector<std::pair<std::string, std::string>>
ParseConfigJson(const char *str, int len) {
	std::vector<std::pair<std::string, std::string>> result;
	if (!str || len <= 0) return result;
	try {
		auto j = nlohmann::json::parse(std::string(str, len));
		if (j.is_object()) {
			for (auto &[key, val] : j.items()) {
				result.emplace_back(key, val.is_string() ? val.get<std::string>() : val.dump());
			}
		}
	} catch (...) {}
	return result;
}

//! Parse a JSON object string into header key-value pairs.
static std::vector<std::pair<std::string, std::string>>
ParseHeadersJson(const char *str, int len) {
	std::vector<std::pair<std::string, std::string>> result;
	if (!str || len <= 0) return result;
	try {
		auto j = nlohmann::json::parse(std::string(str, len));
		if (j.is_object()) {
			for (auto &[key, val] : j.items()) {
				result.emplace_back(key, val.is_string() ? val.get<std::string>() : val.dump());
			}
		}
	} catch (...) {}
	return result;
}

/* ══════════════════════════════════════════════════════════════════════
 * One request, through the core
 *
 * Was ~100 lines building a cpr::Session, applying auth, rate-limiting and
 * shaping a JSON response — a near-copy of what duckdb_ext did. All of that
 * is include/blobhttp.h now. The batch is returned rather than just its JSON
 * so callers can also reach the raw bytes, which is what bh_http_body needs.
 * ══════════════════════════════════════════════════════════════════════ */

static bh_batch *PerformOne(const std::string &method, const std::string &url,
                            const std::string &headers_json,
                            const std::string &params_json,
                            const std::string &body,
                            const std::string &content_type,
                            const std::string &config_json) {
	bh_batch *batch = bh_batch_new(config_json.empty() ? "{}" : config_json.c_str());
	if (!batch) throw std::runtime_error(bh_errmsg());

	auto fail = [&]() {
		std::string msg = bh_errmsg();
		bh_batch_free(batch);
		throw std::runtime_error(msg);
	};

	if (bh_batch_add(batch, method.c_str(), url.c_str(),
	                 headers_json.empty() ? nullptr : headers_json.c_str(),
	                 params_json.empty() ? nullptr : params_json.c_str(),
	                 body.empty() ? nullptr : body.data(), body.size(),
	                 content_type.empty() ? nullptr : content_type.c_str(),
	                 -1, -1) != 0) {
		fail();
	}
	if (bh_batch_perform(batch) != 0) fail();
	return batch;
}

/// The JSON envelope for one performed batch, then free it.
static std::string ExecuteRequest(const std::string &method, const std::string &url,
                                  const std::string &headers_json,
                                  const std::string &params_json,
                                  const std::string &body,
                                  const std::string &content_type,
                                  const std::string &config_json) {
	bh_batch *batch = PerformOne(method, url, headers_json, params_json, body,
	                             content_type, config_json);
	char *json = bh_result_json(batch, 0);
	if (!json) {
		std::string msg = bh_errmsg();
		bh_batch_free(batch);
		throw std::runtime_error(msg);
	}
	std::string out(json);
	bh_free(json);
	bh_batch_free(batch);
	return out;
}

/// Read one text argument, or "" when absent or NULL.
static std::string ArgText(int argc, sqlite3_value **argv, int i) {
	if (argc <= i || sqlite3_value_type(argv[i]) == SQLITE_NULL) return "";
	auto *p = reinterpret_cast<const char *>(sqlite3_value_text(argv[i]));
	if (!p) return "";
	return std::string(p, sqlite3_value_bytes(argv[i]));
}

/* ══════════════════════════════════════════════════════════════════════
 * Scalar: bh_http_request(method, url [, headers_json [, params_json
 *                          [, body [, content_type [, config_json]]]]])
 * Returns JSON string with full request/response envelope.
 * All optional args are JSON strings — uniform with DuckDB interface.
 * ══════════════════════════════════════════════════════════════════════ */

static void bhttp_request_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	if (argc < 2) {
		sqlite3_result_error(ctx, "bh_http_request requires at least 2 arguments: method, url", -1);
		return;
	}
	auto method = ArgText(argc, argv, 0);
	auto url = ArgText(argc, argv, 1);
	if (method.empty() || url.empty()) {
		sqlite3_result_error(ctx, "method and url must not be NULL", -1);
		return;
	}
	for (auto &c : method) c = toupper(c);

	try {
		auto result = ExecuteRequest(method, url,
		                             ArgText(argc, argv, 2),   // headers JSON
		                             ArgText(argc, argv, 3),   // params JSON
		                             ArgText(argc, argv, 4),   // body
		                             ArgText(argc, argv, 5),   // content type
		                             ArgText(argc, argv, 6));  // config JSON
		sqlite3_result_text(ctx, result.c_str(), result.length(), SQLITE_TRANSIENT);
	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}

/* ══════════════════════════════════════════════════════════════════════
 * bh_http_get(url, [headers, params, config]) -> JSON
 * ══════════════════════════════════════════════════════════════════════ */

static void bhttp_get_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	if (argc < 1) {
		sqlite3_result_error(ctx, "bh_http_get requires at least 1 argument: url", -1);
		return;
	}
	auto url = ArgText(argc, argv, 0);
	if (url.empty()) { sqlite3_result_null(ctx); return; }

	try {
		auto result = ExecuteRequest("GET", url, ArgText(argc, argv, 1),
		                             ArgText(argc, argv, 2), "", "",
		                             ArgText(argc, argv, 3));
		sqlite3_result_text(ctx, result.c_str(), result.length(), SQLITE_TRANSIENT);
	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}

/* ══════════════════════════════════════════════════════════════════════
 * bh_http_post(url, [body, content_type, headers, config]) -> JSON
 * ══════════════════════════════════════════════════════════════════════ */

static void bhttp_post_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	if (argc < 1) {
		sqlite3_result_error(ctx, "bh_http_post requires at least 1 argument: url", -1);
		return;
	}
	auto url = ArgText(argc, argv, 0);
	if (url.empty()) { sqlite3_result_null(ctx); return; }

	try {
		auto result = ExecuteRequest("POST", url, ArgText(argc, argv, 3), "",
		                             ArgText(argc, argv, 1),   // body
		                             ArgText(argc, argv, 2),   // content type
		                             ArgText(argc, argv, 4));
		sqlite3_result_text(ctx, result.c_str(), result.length(), SQLITE_TRANSIENT);
	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}

/* ══════════════════════════════════════════════════════════════════════
 * bh_http_body(method, url, [headers, params, body, content_type, config])
 *     -> BLOB
 *
 * The raw response bytes, with no JSON in the way. SQLite had no path to
 * these at all: the JSON envelope carries response_body as a string, and
 * nlohmann rejects a non-UTF-8 value outright, so any binary response failed
 * with "invalid UTF-8 byte" rather than returning anything. DuckDB has had
 * response_blob all along; this is the SQLite equivalent.
 * ══════════════════════════════════════════════════════════════════════ */

static void bhttp_body_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	if (argc < 2) {
		sqlite3_result_error(ctx, "bh_http_body requires at least 2 arguments: method, url", -1);
		return;
	}
	auto method = ArgText(argc, argv, 0);
	auto url = ArgText(argc, argv, 1);
	if (method.empty() || url.empty()) {
		sqlite3_result_error(ctx, "method and url must not be NULL", -1);
		return;
	}
	for (auto &c : method) c = toupper(c);

	try {
		bh_batch *batch = PerformOne(method, url, ArgText(argc, argv, 2),
		                             ArgText(argc, argv, 3), ArgText(argc, argv, 4),
		                             ArgText(argc, argv, 5), ArgText(argc, argv, 6));
		size_t len = 0;
		const void *body = bh_result_body(batch, 0, &len);
		// SQLITE_TRANSIENT: SQLite copies before the batch is freed, which it
		// must, since the pointer is borrowed and dies with the batch.
		sqlite3_result_blob(ctx, body ? body : "", static_cast<int>(len), SQLITE_TRANSIENT);
		bh_batch_free(batch);
	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}


static void negotiate_auth_header_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	const char *url = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
	if (!url) { sqlite3_result_null(ctx); return; }

	try {
		auto result = GenerateNegotiateToken(url);
		std::string header = "Negotiate " + result.token;
		sqlite3_result_text(ctx, header.c_str(), header.length(), SQLITE_TRANSIENT);
	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}

static void negotiate_auth_header_json_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	const char *url = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
	if (!url) { sqlite3_result_null(ctx); return; }

	try {
		auto result = GenerateNegotiateToken(url);
		nlohmann::json j;
		j["token"] = result.token;
		j["header"] = "Negotiate " + result.token;
		j["url"] = result.url;
		j["hostname"] = result.hostname;
		j["spn"] = result.spn;
		j["provider"] = result.provider;
		j["library"] = result.library;
		auto json_str = j.dump();
		sqlite3_result_text(ctx, json_str.c_str(), json_str.length(), SQLITE_TRANSIENT);
	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}

/* ══════════════════════════════════════════════════════════════════════
 * bh_http_rate_limit_stats() -> JSON array
 * ══════════════════════════════════════════════════════════════════════ */

static void bhttp_rate_limit_stats_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	nlohmann::json arr = nlohmann::json::array();

	auto snapshot = [](const std::string &host, GCRARateLimiter &limiter) -> nlohmann::json {
		return {
		    {"host", host},
		    {"rate_limit", limiter.RateSpec()},
		    {"rate_rps", limiter.Rate()},
		    {"burst", limiter.Burst()},
		    {"requests", limiter.Requests()},
		    {"paced", limiter.Paced()},
		    {"total_wait_seconds", limiter.TotalWaitSeconds()},
		    {"throttled_429", limiter.Throttled429()},
		    {"backlog_seconds", limiter.BacklogSeconds()},
		    {"total_responses", limiter.TotalResponses()},
		    {"total_response_bytes", limiter.TotalResponseBytes()},
		    {"total_elapsed", limiter.TotalElapsed()},
		    {"min_elapsed", limiter.MinElapsed()},
		    {"max_elapsed", limiter.MaxElapsed()},
		    {"errors", limiter.Errors()},
		};
	};

	auto *global = GlobalLimiterSnapshot();
	if (global) arr.push_back(snapshot("(global)", *global));

	Registry().ForEach([&](const std::string &host, GCRARateLimiter &limiter) {
		arr.push_back(snapshot(host, limiter));
	});

	auto result = arr.dump();
	sqlite3_result_text(ctx, result.c_str(), result.length(), SQLITE_TRANSIENT);
}

/* ══════════════════════════════════════════════════════════════════════
 * bh_http_adapt(adapter_name, params_json) -> JSON TEXT
 *
 * Looks up adapter from llm_adapter table, renders prompt via
 * bt_template_render() (blobtemplates must be loaded), calls the LLM
 * with schema validation and continuation, applies response JMESPath.
 * Returns {"data": ..., "_meta": {...}}.
 * ══════════════════════════════════════════════════════════════════════ */

//! Run a SQL query against the db and return the first column of the first row.
static std::string SqliteQueryScalar(sqlite3 *db, const std::string &sql) {
	sqlite3_stmt *stmt = nullptr;
	if (sqlite3_prepare_v2(db, sql.c_str(), sql.size(), &stmt, nullptr) != SQLITE_OK) {
		return "";
	}
	std::string result;
	if (sqlite3_step(stmt) == SQLITE_ROW) {
		auto text = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
		if (text) result = text;
	}
	sqlite3_finalize(stmt);
	return result;
}

//! Escape a string for single-quoted SQL literal.
static std::string SqliteEscapeSql(const std::string &s) {
	std::string out;
	out.reserve(s.size() + s.size() / 10);
	for (char c : s) {
		if (c == '\'') out += "''";
		else out += c;
	}
	return out;
}

//! POST to a chat completions endpoint. Returns parsed response + elapsed.
static std::pair<nlohmann::json, double> SqlitePostChatCompletion(
    const std::string &url,
    const nlohmann::json &body,
    const HttpConfig &config) {

	auto session = std::make_shared<cpr::Session>();
	session->SetUrl(cpr::Url{url});
	session->SetTimeout(cpr::Timeout{config.timeout * 1000});

	cpr::Header hdrs;
	hdrs["Content-Type"] = "application/json";
	if (config.auth_type == "bearer" && !config.bearer_token.empty()) {
		hdrs["Authorization"] = "Bearer " + config.bearer_token;
	}
	session->SetHeader(hdrs);

	if (!config.verify_ssl) session->SetVerifySsl(cpr::VerifySsl{false});
	if (!config.proxy.empty()) {
		session->SetProxies(cpr::Proxies{{"http", config.proxy}, {"https", config.proxy}});
	}

	session->SetBody(cpr::Body{body.dump()});

	auto host = ExtractHost(url);
	AcquireRateLimit(GlobalLimiter(config.global_rate_limit_spec, config.global_burst));
	AcquireRateLimit(Registry().GetOrCreate(host, config.rate_limit_spec, config.burst));

	auto response = session->Post();
	RecordResponseStats(response, host);

	if (response.status_code != 200) {
		throw std::runtime_error(
		    "LLM request failed (HTTP " + std::to_string(response.status_code) +
		    "): " + response.text.substr(0, 500));
	}

	return {nlohmann::json::parse(response.text), response.elapsed};
}

static void bhttp_adapt_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	if (argc < 2) {
		sqlite3_result_error(ctx, "bhttp_adapt requires 2 arguments: adapter_name, params_json", -1);
		return;
	}

	const char *adapter_name_str = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
	const char *params_str = reinterpret_cast<const char *>(sqlite3_value_text(argv[1]));
	if (!params_str) params_str = "{}";

	sqlite3 *db = sqlite3_context_db_handle(ctx);

	try {
		nlohmann::json params = nlohmann::json::parse(params_str);

		// Look up adapter row as JSON
		nlohmann::json adapter_row = nlohmann::json::object();
		if (adapter_name_str && adapter_name_str[0]) {
			std::string sql =
			    "SELECT json_object("
			    "'prompt_template', prompt_template, "
			    "'output_schema', output_schema, "
			    "'response_jmespath', response_jmespath, "
			    "'max_tokens', max_tokens"
			    ") FROM llm_adapter WHERE name = '" +
			    SqliteEscapeSql(adapter_name_str) + "'";

			auto json_str = SqliteQueryScalar(db, sql);
			if (json_str.empty()) {
				std::string err = std::string("Adapter '") + adapter_name_str +
				                  "' not found in llm_adapter table";
				sqlite3_result_error(ctx, err.c_str(), err.size());
				return;
			}
			adapter_row = nlohmann::json::parse(json_str);
			for (auto it = adapter_row.begin(); it != adapter_row.end(); ) {
				if (it.value().is_null()) it = adapter_row.erase(it);
				else ++it;
			}
		}

		// Merge: adapter is base, params overwrite
		nlohmann::json cfg = adapter_row;
		cfg.merge_patch(params);

		// Resolve infrastructure
		std::string endpoint = cfg.value("endpoint", "http://localhost:8080/v1/chat/completions");
		std::string model = cfg.value("model", "anthropic/claude-haiku-4-5-20251001");
		std::string output_schema = cfg.value("output_schema", "");
		std::string response_jmespath = cfg.value("response_jmespath", "");

		auto parse_int = [&cfg](const char *key, int def) -> int {
			if (!cfg.contains(key)) return def;
			auto &v = cfg[key];
			if (v.is_number()) return v.get<int>();
			if (v.is_string()) {
				try { return std::stoi(v.get<std::string>()); } catch (...) {}
			}
			return def;
		};
		int max_tokens = parse_int("max_tokens", 4096);
		int max_continuations = parse_int("max_continuations", 10);
		int max_retries = parse_int("max_retries", 3);

		// Render prompt via bt_template_render() (blobtemplates SQLite function)
		std::string prompt_template = cfg.value("prompt_template", "");
		std::string prompt_text;
		if (!prompt_template.empty()) {
			std::string sql = "SELECT bt_template_render('" +
			    SqliteEscapeSql(prompt_template) + "', '" +
			    SqliteEscapeSql(std::string(params_str)) + "')";
			prompt_text = SqliteQueryScalar(db, sql);
			if (prompt_text.empty()) {
				sqlite3_result_error(ctx, "bt_template_render failed — is blobtemplates loaded?", -1);
				return;
			}
		} else {
			sqlite3_result_error(ctx, "No prompt_template in adapter config", -1);
			return;
		}

		// Resolve HTTP config
		std::vector<std::pair<std::string, std::string>> config_entries;
		HttpConfig config = ResolveConfig(endpoint, config_entries);
		std::vector<std::pair<std::string, std::string>> http_params;
		ResolveVaultSecrets(config, http_params);

		// Build request body
		nlohmann::json body = {
		    {"model", model},
		    {"max_tokens", max_tokens},
		    {"messages", nlohmann::json::array({
		        {{"role", "user"}, {"content", prompt_text}}
		    })}
		};

		// Schema setup
		bool use_schema = !output_schema.empty();
		std::unique_ptr<jsoncons::jsonschema::json_schema<jsoncons::json>> compiled_schema;
		if (use_schema) {
			auto schema_json = nlohmann::json::parse(output_schema);
			body["tools"] = nlohmann::json::array({{
			    {"type", "function"},
			    {"function", {{"name", "extract"}, {"parameters", schema_json}}}
			}});
			body["tool_choice"] = {
			    {"type", "function"},
			    {"function", {{"name", "extract"}}}
			};
			auto jc_schema = jsoncons::json::parse(output_schema);
			compiled_schema = std::make_unique<jsoncons::jsonschema::json_schema<jsoncons::json>>(
			    jsoncons::jsonschema::make_json_schema(std::move(jc_schema)));
		}

		// Stats tracking
		int stat_requests = 0, stat_continuations = 0, stat_retries = 0;
		int stat_prompt_tokens = 0, stat_completion_tokens = 0, stat_total_tokens = 0;
		double stat_elapsed = 0.0;
		std::string stat_model, stat_finish_reason;

		auto accumulate = [&](const nlohmann::json &resp, double elapsed) {
			stat_requests++;
			stat_elapsed += elapsed;
			if (resp.contains("usage")) {
				stat_prompt_tokens += resp["usage"].value("prompt_tokens", 0);
				stat_completion_tokens += resp["usage"].value("completion_tokens", 0);
				stat_total_tokens += resp["usage"].value("total_tokens", 0);
			}
			if (resp.contains("model")) stat_model = resp["model"].get<std::string>();
		};

		// Completion with continuation
		auto do_complete = [&](nlohmann::json &req_body) -> std::string {
			std::string accumulated;
			for (int cont = 0; cont < max_continuations; cont++) {
				auto [resp, elapsed] = SqlitePostChatCompletion(endpoint, req_body, config);
				accumulate(resp, elapsed);

				auto &choice = resp["choices"][0];
				auto &msg = choice["message"];
				std::string fragment;
				if (msg.contains("tool_calls") && !msg["tool_calls"].empty()) {
					fragment = msg["tool_calls"][0]["function"]["arguments"].get<std::string>();
				} else if (msg.contains("content") && !msg["content"].is_null()) {
					fragment = msg["content"].get<std::string>();
				}

				accumulated += fragment;
				std::string fr = choice.value("finish_reason", "");
				if (fr != "length") {
					stat_finish_reason = fr;
					return accumulated;
				}
				stat_continuations++;
				req_body["messages"].push_back({{"role", "assistant"}, {"content", accumulated}});
				req_body["messages"].push_back({{"role", "user"}, {"content", "Continue exactly where you left off."}});
			}
			throw std::runtime_error("Continuation limit reached");
		};

		// Execute with optional validation retry
		std::string content;
		if (!use_schema) {
			content = do_complete(body);
		} else {
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
						if (error_text.size() > 2000) error_text = error_text.substr(0, 2000) + "...";
						stat_retries++;
						body["messages"].push_back({{"role", "assistant"}, {"content", result}});
						body["messages"].push_back({{"role", "user"},
						    {"content", "Validation errors:\n" + error_text + "\nFix the errors and try again."}});
						continue;
					}
					content = result;
					break;
				} catch (const jsoncons::ser_error &e) {
					stat_retries++;
					body["messages"].push_back({{"role", "assistant"}, {"content", result}});
					body["messages"].push_back({{"role", "user"},
					    {"content", std::string("Invalid JSON: ") + e.what() + "\nReturn valid JSON."}});
					continue;
				}
			}
			if (content.empty()) {
				throw std::runtime_error("Schema validation failed after retries");
			}
		}

		// Apply response JMESPath
		if (!response_jmespath.empty()) {
			auto doc = jsoncons::json::parse(content);
			auto jmes_result = jsoncons::jmespath::search(doc, response_jmespath);
			std::ostringstream oss;
			oss << jmes_result;
			content = oss.str();
		}

		// Build result with _meta
		nlohmann::json result_obj;
		try {
			result_obj["data"] = nlohmann::json::parse(content);
		} catch (...) {
			result_obj["data"] = content;
		}
		result_obj["_meta"] = {
		    {"http_requests", stat_requests},
		    {"continuations", stat_continuations},
		    {"retries", stat_retries},
		    {"prompt_tokens", stat_prompt_tokens},
		    {"completion_tokens", stat_completion_tokens},
		    {"total_tokens", stat_total_tokens},
		    {"elapsed_seconds", stat_elapsed},
		    {"model", stat_model},
		    {"finish_reason", stat_finish_reason}
		};

		auto final_str = result_obj.dump();
		sqlite3_result_text(ctx, final_str.c_str(), final_str.length(), SQLITE_TRANSIENT);

	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}

/* ══════════════════════════════════════════════════════════════════════
 * Extension entry point
 * ══════════════════════════════════════════════════════════════════════ */

extern "C" {
#ifdef _WIN32
__declspec(dllexport)
#else
__attribute__((visibility("default")))
#endif
int sqlite3_bhttp_init(sqlite3 *db, char **pzErrMsg,
                       const sqlite3_api_routines *pApi) {
	SQLITE_EXTENSION_INIT2(pApi);
	int rc;

	rc = sqlite3_create_function(db, "bh_http_request", -1, SQLITE_UTF8, nullptr,
	                              bhttp_request_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bh_http_get", -1, SQLITE_UTF8, nullptr,
	                              bhttp_get_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bh_http_post", -1, SQLITE_UTF8, nullptr,
	                              bhttp_post_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bh_http_body", -1, SQLITE_UTF8, nullptr,
	                             bhttp_body_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bh_negotiate_auth_header", 1, SQLITE_UTF8, nullptr,
	                              negotiate_auth_header_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bh_negotiate_auth_header_json", 1, SQLITE_UTF8, nullptr,
	                              negotiate_auth_header_json_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bh_http_rate_limit_stats", 0, SQLITE_UTF8, nullptr,
	                              bhttp_rate_limit_stats_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bh_http_adapt", 2, SQLITE_UTF8, nullptr,
	                              bhttp_adapt_func, nullptr, nullptr);
	return rc;
}
}
