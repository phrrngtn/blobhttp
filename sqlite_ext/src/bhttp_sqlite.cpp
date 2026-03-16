#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

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

/* ══════════════════════════════════════════════════════════════════════
 * Global state: session pool and rate limiter registry (per-process)
 * ══════════════════════════════════════════════════════════════════════ */

using namespace blobhttp;

static LRUPool<std::string, cpr::Session> &GetSessionPool() {
	static LRUPool<std::string, cpr::Session> pool(50);
	return pool;
}

static RateLimiterRegistry &GetRateLimiterRegistry() {
	static RateLimiterRegistry registry(200);
	return registry;
}

static std::mutex g_global_limiter_mutex;
static std::unique_ptr<GCRARateLimiter> g_global_limiter;
static std::string g_global_limiter_spec;

static GCRARateLimiter *GetGlobalLimiter(const std::string &spec, double burst) {
	if (spec.empty()) return nullptr;
	std::lock_guard<std::mutex> lock(g_global_limiter_mutex);
	if (!g_global_limiter || g_global_limiter_spec != spec) {
		double rate = ParseRateLimit(spec);
		g_global_limiter = std::make_unique<GCRARateLimiter>(rate, burst, spec);
		g_global_limiter_spec = spec;
	}
	return g_global_limiter.get();
}

static GCRARateLimiter *GetGlobalLimiterSnapshot() {
	std::lock_guard<std::mutex> lock(g_global_limiter_mutex);
	return g_global_limiter.get();
}

/* ══════════════════════════════════════════════════════════════════════
 * Helpers
 * ══════════════════════════════════════════════════════════════════════ */

static std::string ExtractHost(const std::string &url) {
	auto pos = url.find("://");
	if (pos == std::string::npos) return url;
	auto host_start = pos + 3;
	auto host_end = url.find_first_of(":/?#", host_start);
	if (host_end == std::string::npos) host_end = url.length();
	return url.substr(host_start, host_end - host_start);
}

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

static void AcquireRateLimit(GCRARateLimiter *limiter) {
	if (!limiter) return;
	int max_retries = 50;
	bool was_paced = false;
	double total_pacing = 0.0;
	while (!limiter->TryAcquire() && max_retries-- > 0) {
		double wait = limiter->WaitTime();
		if (wait > 0.0) {
			was_paced = true;
			total_pacing += wait;
			std::this_thread::sleep_for(std::chrono::duration<double>(wait));
		}
	}
	limiter->RecordRequest();
	if (was_paced) limiter->RecordPacing(total_pacing);
}

static void RecordResponseStats(const cpr::Response &response, const std::string &host) {
	auto *limiter = GetRateLimiterRegistry().GetOrCreate(host);
	if (!limiter) return;
	limiter->RecordResponse(response.elapsed, response.text.size(),
	                        static_cast<int>(response.status_code));
	if (response.status_code == 429) {
		double retry_after = 1.0;
		auto it = response.header.find("Retry-After");
		if (it != response.header.end()) {
			try { retry_after = std::stod(it->second); } catch (...) {}
		}
		limiter->RecordThrottle(retry_after);
	}
	auto *global = GetGlobalLimiterSnapshot();
	if (global) {
		global->RecordResponse(response.elapsed, response.text.size(),
		                       static_cast<int>(response.status_code));
	}
}

//! Build a cpr::Session and execute a single HTTP request. Returns JSON string.
static std::string ExecuteRequest(const std::string &method, const std::string &url,
                                  const std::vector<std::pair<std::string, std::string>> &headers,
                                  const std::vector<std::pair<std::string, std::string>> &params,
                                  const std::string &body, const std::string &content_type,
                                  const HttpConfig &config) {
	auto session = std::make_shared<cpr::Session>();
	session->SetUrl(cpr::Url{url});
	session->SetTimeout(cpr::Timeout{config.timeout * 1000});

	cpr::Header cpr_headers;
	for (auto &[k, v] : headers) {
		cpr_headers[k] = v;
	}

	// Apply auth
	if (config.auth_type == "negotiate" && cpr_headers.find("Authorization") == cpr_headers.end()) {
		auto neg_result = GenerateNegotiateToken(url);
		cpr_headers["Authorization"] = "Negotiate " + neg_result.token;
	} else if (config.auth_type == "bearer" && !config.bearer_token.empty() &&
	           cpr_headers.find("Authorization") == cpr_headers.end()) {
		if (config.bearer_token_expires_at > 0) {
			auto now_epoch = std::chrono::duration_cast<std::chrono::seconds>(
			    std::chrono::system_clock::now().time_since_epoch()).count();
			if (now_epoch >= config.bearer_token_expires_at) {
				throw std::runtime_error("Bearer token expired");
			}
		}
		cpr_headers["Authorization"] = "Bearer " + config.bearer_token;
	}

	auto effective_ct = content_type;
	if (!body.empty() && effective_ct.empty()) effective_ct = "application/json";
	if (!effective_ct.empty()) cpr_headers["Content-Type"] = effective_ct;

	session->SetHeader(cpr_headers);

	if (!config.verify_ssl) session->SetVerifySsl(cpr::VerifySsl{false});
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
	if (!params.empty()) {
		cpr::Parameters cpr_params;
		for (auto &[k, v] : params) {
			cpr_params.Add(cpr::Parameter{k, v});
		}
		session->SetParameters(cpr_params);
	}
	if (!body.empty()) session->SetBody(cpr::Body{body});

	// Rate limiting
	auto host = ExtractHost(url);
	AcquireRateLimit(GetGlobalLimiter(config.global_rate_limit_spec, config.global_burst));
	AcquireRateLimit(GetRateLimiterRegistry().GetOrCreate(host, config.rate_limit_spec, config.burst));

	// Execute
	cpr::Response response;
	if (method == "GET") response = session->Get();
	else if (method == "POST") response = session->Post();
	else if (method == "PUT") response = session->Put();
	else if (method == "DELETE") response = session->Delete();
	else if (method == "PATCH") response = session->Patch();
	else if (method == "HEAD") response = session->Head();
	else if (method == "OPTIONS") response = session->Options();
	else throw std::runtime_error("Unsupported HTTP method: " + method);

	RecordResponseStats(response, host);

	// Build JSON response
	nlohmann::json result;
	result["request_url"] = url;
	result["request_method"] = method;

	nlohmann::json req_hdrs = nlohmann::json::object();
	for (auto &[k, v] : cpr_headers) req_hdrs[k] = v;
	result["request_headers"] = req_hdrs;
	result["request_body"] = body;

	result["response_status_code"] = static_cast<int>(response.status_code);
	result["response_status"] = response.status_line;

	nlohmann::json resp_hdrs = nlohmann::json::object();
	for (auto &[k, v] : response.header) {
		std::string lower_k = k;
		std::transform(lower_k.begin(), lower_k.end(), lower_k.begin(), ::tolower);
		resp_hdrs[lower_k] = v;
	}
	result["response_headers"] = resp_hdrs;
	result["response_body"] = response.text;
	result["response_url"] = response.url.str();
	result["elapsed"] = response.elapsed;
	result["redirect_count"] = static_cast<int>(response.redirect_count);

	return result.dump();
}

/* ══════════════════════════════════════════════════════════════════════
 * Scalar: bhttp_request(method, url [, headers_json [, params_json
 *                        [, body [, content_type [, config_json]]]]])
 * Returns JSON string with full request/response envelope.
 * All optional args are JSON strings — uniform with DuckDB interface.
 * ══════════════════════════════════════════════════════════════════════ */

static void bhttp_request_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	if (argc < 2) {
		sqlite3_result_error(ctx, "bhttp_request requires at least 2 arguments: method, url", -1);
		return;
	}

	const char *method_str = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
	const char *url_str = reinterpret_cast<const char *>(sqlite3_value_text(argv[1]));
	if (!method_str || !url_str) {
		sqlite3_result_error(ctx, "method and url must not be NULL", -1);
		return;
	}

	std::string method(method_str);
	for (auto &c : method) c = toupper(c);
	std::string url(url_str);

	std::vector<std::pair<std::string, std::string>> headers;
	std::vector<std::pair<std::string, std::string>> params;
	std::string body;
	std::string content_type;
	std::vector<std::pair<std::string, std::string>> config_entries;

	if (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL) {
		headers = ParseHeadersJson(
		    reinterpret_cast<const char *>(sqlite3_value_text(argv[2])),
		    sqlite3_value_bytes(argv[2]));
	}
	if (argc >= 4 && sqlite3_value_type(argv[3]) != SQLITE_NULL) {
		params = ParseHeadersJson(
		    reinterpret_cast<const char *>(sqlite3_value_text(argv[3])),
		    sqlite3_value_bytes(argv[3]));
	}
	if (argc >= 5 && sqlite3_value_type(argv[4]) != SQLITE_NULL) {
		body = std::string(reinterpret_cast<const char *>(sqlite3_value_text(argv[4])),
		                   sqlite3_value_bytes(argv[4]));
	}
	if (argc >= 6 && sqlite3_value_type(argv[5]) != SQLITE_NULL) {
		content_type = std::string(reinterpret_cast<const char *>(sqlite3_value_text(argv[5])),
		                           sqlite3_value_bytes(argv[5]));
	}
	if (argc >= 7 && sqlite3_value_type(argv[6]) != SQLITE_NULL) {
		config_entries = ParseConfigJson(
		    reinterpret_cast<const char *>(sqlite3_value_text(argv[6])),
		    sqlite3_value_bytes(argv[6]));
	}

	HttpConfig config = ResolveConfig(url, config_entries);
	ResolveVaultSecrets(config, params);

	try {
		auto result = ExecuteRequest(method, url, headers, params, body, content_type, config);
		sqlite3_result_text(ctx, result.c_str(), result.length(), SQLITE_TRANSIENT);
	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}

/* ══════════════════════════════════════════════════════════════════════
 * Convenience: bhttp_get(url [, headers_json [, params_json [, config_json]]])
 * ══════════════════════════════════════════════════════════════════════ */

static void bhttp_get_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	if (argc < 1) {
		sqlite3_result_error(ctx, "bhttp_get requires at least 1 argument: url", -1);
		return;
	}

	const char *url_str = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
	if (!url_str) { sqlite3_result_null(ctx); return; }

	std::vector<std::pair<std::string, std::string>> headers;
	std::vector<std::pair<std::string, std::string>> params;
	std::vector<std::pair<std::string, std::string>> config_entries;

	if (argc >= 2 && sqlite3_value_type(argv[1]) != SQLITE_NULL) {
		headers = ParseHeadersJson(
		    reinterpret_cast<const char *>(sqlite3_value_text(argv[1])),
		    sqlite3_value_bytes(argv[1]));
	}
	if (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL) {
		params = ParseHeadersJson(
		    reinterpret_cast<const char *>(sqlite3_value_text(argv[2])),
		    sqlite3_value_bytes(argv[2]));
	}
	if (argc >= 4 && sqlite3_value_type(argv[3]) != SQLITE_NULL) {
		config_entries = ParseConfigJson(
		    reinterpret_cast<const char *>(sqlite3_value_text(argv[3])),
		    sqlite3_value_bytes(argv[3]));
	}

	HttpConfig config = ResolveConfig(url_str, config_entries);
	ResolveVaultSecrets(config, params);

	try {
		auto result = ExecuteRequest("GET", url_str, headers, params, "", "", config);
		sqlite3_result_text(ctx, result.c_str(), result.length(), SQLITE_TRANSIENT);
	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}

/* ══════════════════════════════════════════════════════════════════════
 * Convenience: bhttp_post(url [, body [, headers_json [, params_json [, config_json]]]])
 * ══════════════════════════════════════════════════════════════════════ */

static void bhttp_post_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	if (argc < 1) {
		sqlite3_result_error(ctx, "bhttp_post requires at least 1 argument: url", -1);
		return;
	}

	const char *url_str = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
	if (!url_str) { sqlite3_result_null(ctx); return; }

	std::string body;
	std::vector<std::pair<std::string, std::string>> headers;
	std::vector<std::pair<std::string, std::string>> params;
	std::vector<std::pair<std::string, std::string>> config_entries;

	if (argc >= 2 && sqlite3_value_type(argv[1]) != SQLITE_NULL) {
		body = std::string(reinterpret_cast<const char *>(sqlite3_value_text(argv[1])),
		                   sqlite3_value_bytes(argv[1]));
	}
	if (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL) {
		headers = ParseHeadersJson(
		    reinterpret_cast<const char *>(sqlite3_value_text(argv[2])),
		    sqlite3_value_bytes(argv[2]));
	}
	if (argc >= 4 && sqlite3_value_type(argv[3]) != SQLITE_NULL) {
		params = ParseHeadersJson(
		    reinterpret_cast<const char *>(sqlite3_value_text(argv[3])),
		    sqlite3_value_bytes(argv[3]));
	}
	if (argc >= 5 && sqlite3_value_type(argv[4]) != SQLITE_NULL) {
		config_entries = ParseConfigJson(
		    reinterpret_cast<const char *>(sqlite3_value_text(argv[4])),
		    sqlite3_value_bytes(argv[4]));
	}

	HttpConfig config = ResolveConfig(url_str, config_entries);
	ResolveVaultSecrets(config, params);

	try {
		auto result = ExecuteRequest("POST", url_str, headers, params, body, "", config);
		sqlite3_result_text(ctx, result.c_str(), result.length(), SQLITE_TRANSIENT);
	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}

/* ══════════════════════════════════════════════════════════════════════
 * negotiate_auth_header(url) -> TEXT
 * negotiate_auth_header_json(url) -> JSON TEXT
 * ══════════════════════════════════════════════════════════════════════ */

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
 * bhttp_rate_limit_stats() -> JSON array
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

	auto *global = GetGlobalLimiterSnapshot();
	if (global) arr.push_back(snapshot("(global)", *global));

	GetRateLimiterRegistry().ForEach([&](const std::string &host, GCRARateLimiter &limiter) {
		arr.push_back(snapshot(host, limiter));
	});

	auto result = arr.dump();
	sqlite3_result_text(ctx, result.c_str(), result.length(), SQLITE_TRANSIENT);
}

/* ══════════════════════════════════════════════════════════════════════
 * bhttp_adapt(adapter_name, params_json) -> JSON TEXT
 *
 * Looks up adapter from llm_adapter table, renders prompt via
 * template_render() (blobtemplates must be loaded), calls the LLM
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
	AcquireRateLimit(GetGlobalLimiter(config.global_rate_limit_spec, config.global_burst));
	AcquireRateLimit(GetRateLimiterRegistry().GetOrCreate(host, config.rate_limit_spec, config.burst));

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

		// Render prompt via template_render() (blobtemplates SQLite function)
		std::string prompt_template = cfg.value("prompt_template", "");
		std::string prompt_text;
		if (!prompt_template.empty()) {
			std::string sql = "SELECT template_render('" +
			    SqliteEscapeSql(prompt_template) + "', '" +
			    SqliteEscapeSql(std::string(params_str)) + "')";
			prompt_text = SqliteQueryScalar(db, sql);
			if (prompt_text.empty()) {
				sqlite3_result_error(ctx, "template_render failed — is blobtemplates loaded?", -1);
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

	rc = sqlite3_create_function(db, "bhttp_request", -1, SQLITE_UTF8, nullptr,
	                              bhttp_request_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bhttp_get", -1, SQLITE_UTF8, nullptr,
	                              bhttp_get_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bhttp_post", -1, SQLITE_UTF8, nullptr,
	                              bhttp_post_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "negotiate_auth_header", 1, SQLITE_UTF8, nullptr,
	                              negotiate_auth_header_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "negotiate_auth_header_json", 1, SQLITE_UTF8, nullptr,
	                              negotiate_auth_header_json_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bhttp_rate_limit_stats", 0, SQLITE_UTF8, nullptr,
	                              bhttp_rate_limit_stats_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bhttp_adapt", 2, SQLITE_UTF8, nullptr,
	                              bhttp_adapt_func, nullptr, nullptr);
	return rc;
}
}
