#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

// The C ABI is the whole dependency. This file no longer sees cpr, the rate
// limiter, HttpConfig, the session pool or GSS-API — everything that is not
// about SQLite's own dialect and value marshalling now lives in the core.
#include "blobhttp.h"

#include <cstring>
#include <memory>
#include <string>

#include <nlohmann/json.hpp>


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
	auto url = ArgText(argc, argv, 0);
	std::unique_ptr<char, void (*)(void *)> result(
	    bh_negotiate_auth_header(url.c_str()), bh_free);
	if (!result) {
		sqlite3_result_error(ctx, bh_errmsg(), -1);
		return;
	}
	sqlite3_result_text(ctx, result.get(), -1, SQLITE_TRANSIENT);
}

static void negotiate_auth_header_json_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	auto url = ArgText(argc, argv, 0);
	std::unique_ptr<char, void (*)(void *)> result(
	    bh_negotiate_auth_header_json(url.c_str()), bh_free);
	if (!result) {
		sqlite3_result_error(ctx, bh_errmsg(), -1);
		return;
	}
	sqlite3_result_text(ctx, result.get(), -1, SQLITE_TRANSIENT);
}

/* ══════════════════════════════════════════════════════════════════════
 * bh_sso_jwt(config_json)      -> the access token
 * bh_sso_jwt_json(config_json) -> the provider's whole token response
 *
 * Kerberos ticket in, JWT out. blobsso offers the same exchange to DuckDB via
 * httpfs; SQLite cannot reach a DuckDB extension, which is why this exists.
 * ══════════════════════════════════════════════════════════════════════ */

static void sso_jwt_impl(sqlite3_context *ctx, int argc, sqlite3_value **argv, bool whole) {
	auto cfg = ArgText(argc, argv, 0);
	if (cfg.empty()) {
		sqlite3_result_error(ctx, "bh_sso_jwt requires a config JSON argument", -1);
		return;
	}
	std::unique_ptr<char, void (*)(void *)> res(bh_sso_jwt(cfg.c_str()), bh_free);
	if (!res) {
		sqlite3_result_error(ctx, bh_errmsg(), -1);
		return;
	}
	if (whole) {
		sqlite3_result_text(ctx, res.get(), -1, SQLITE_TRANSIENT);
		return;
	}
	try {
		auto tok = nlohmann::json::parse(res.get()).value("access_token", std::string{});
		sqlite3_result_text(ctx, tok.c_str(), tok.length(), SQLITE_TRANSIENT);
	} catch (const std::exception &e) {
		sqlite3_result_error(ctx, e.what(), -1);
	}
}

static void bhttp_sso_jwt_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	sso_jwt_impl(ctx, argc, argv, false);
}

static void bhttp_sso_jwt_json_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	sso_jwt_impl(ctx, argc, argv, true);
}

/* ══════════════════════════════════════════════════════════════════════
 * bh_http_rate_limit_stats() -> JSON array
 * ══════════════════════════════════════════════════════════════════════ */

static void bhttp_rate_limit_stats_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	(void)argc;
	(void)argv;
	std::unique_ptr<char, void (*)(void *)> stats(bh_rate_limit_stats_json(), bh_free);
	if (!stats) {
		sqlite3_result_error(ctx, bh_errmsg(), -1);
		return;
	}
	sqlite3_result_text(ctx, stats.get(), -1, SQLITE_TRANSIENT);
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
static void bhttp_adapt_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	if (argc < 1) {
		sqlite3_result_error(ctx, "bh_http_adapt requires at least 1 argument: adapter_name", -1);
		return;
	}

	const char *adapter_name_str = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
	const char *params_str = argc >= 2
	    ? reinterpret_cast<const char *>(sqlite3_value_text(argv[1])) : nullptr;
	if (!params_str) params_str = "{}";

	sqlite3 *db = sqlite3_context_db_handle(ctx);

	try {
		// ── Host-specific half: the adapter row and the rendered prompt ──
		//
		// This is why bh_http_adapt exists in the adapter rather than the
		// core: it reads a table in the caller's own database, and renders
		// through blobtemplates' bt_template_render() if that extension is
		// loaded. DuckDB does the equivalent in the llm_adapt() macro.
		if (!adapter_name_str || !adapter_name_str[0]) {
			sqlite3_result_error(ctx, "adapter_name must not be NULL", -1);
			return;
		}

		std::string sql =
		    "SELECT json_object("
		    "'prompt_template', prompt_template, "
		    "'output_schema', output_schema, "
		    "'response_jmespath', response_jmespath, "
		    "'max_tokens', max_tokens"
		    ") FROM llm_adapter WHERE name = '" +
		    SqliteEscapeSql(adapter_name_str) + "'";

		auto adapter_json = SqliteQueryScalar(db, sql);
		if (adapter_json.empty()) {
			std::string err = std::string("Adapter '") + adapter_name_str + "' not found";
			sqlite3_result_error(ctx, err.c_str(), -1);
			return;
		}
		auto cfg = nlohmann::json::parse(adapter_json);

		std::string prompt_template = cfg.value("prompt_template", "");
		if (prompt_template.empty()) {
			sqlite3_result_error(ctx, "No prompt_template in adapter config", -1);
			return;
		}
		std::string render_sql = "SELECT bt_template_render('" +
		    SqliteEscapeSql(prompt_template) + "', '" +
		    SqliteEscapeSql(std::string(params_str)) + "')";
		std::string prompt_text = SqliteQueryScalar(db, render_sql);
		if (prompt_text.empty()) {
			sqlite3_result_error(ctx,
			    "bt_template_render failed — is blobtemplates loaded?", -1);
			return;
		}

		// ── Portable half: hand it to the core ──────────────────────────
		//
		// Was ~200 lines here: its own chat-completion POST, its own
		// continuation loop and its own schema compile-and-retry — a third
		// copy of what LlmCompleteLoop does, free to drift from the other
		// two. Same function name, same SQL, same behaviour now.
		nlohmann::json request;
		request["prompt_text"] = prompt_text;
		request["output_schema"] = cfg.value("output_schema", "");
		request["response_jmespath"] = cfg.value("response_jmespath", "");
		if (cfg.contains("max_tokens") && !cfg["max_tokens"].is_null()) {
			request["max_tokens"] = cfg["max_tokens"];
		}
		for (const char *key : {"endpoint", "model", "max_continuations", "max_retries"}) {
			if (cfg.contains(key) && !cfg[key].is_null()) request[key] = cfg[key];
		}

		auto request_str = request.dump();
		std::unique_ptr<char, void (*)(void *)> result(
		    bh_llm_adapt(request_str.c_str()), bh_free);
		if (!result) {
			sqlite3_result_error(ctx, bh_errmsg(), -1);
			return;
		}
		sqlite3_result_text(ctx, result.get(), -1, SQLITE_TRANSIENT);

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

	rc = sqlite3_create_function(db, "bh_sso_jwt", 1, SQLITE_UTF8, nullptr,
	                             bhttp_sso_jwt_func, nullptr, nullptr);
	if (rc != SQLITE_OK) return rc;

	rc = sqlite3_create_function(db, "bh_sso_jwt_json", 1, SQLITE_UTF8, nullptr,
	                             bhttp_sso_jwt_json_func, nullptr, nullptr);
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
