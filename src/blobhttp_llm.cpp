/*
 * blobhttp_llm.cpp — chat completion with continuation and schema retry.
 *
 * Moved out of duckdb_ext/src/bhttp_llm.cpp, which included
 * duckdb_extension.h purely for the scalar-function glue at the bottom. The
 * loop itself never touched DuckDB — its own header already said "shared by
 * DuckDB and SQLite extensions" — it just had no way to be shared.
 *
 * What makes this worth having in a core at all is that it is a loop:
 *
 *   - continue while the model stops on finish_reason == "length", feeding the
 *     accumulated text back as an assistant turn;
 *   - if an output schema is given, validate the result and retry with the
 *     validation errors as correction.
 *
 * Neither is expressible in SQL, which is why it was written in C++ in the
 * first place, and there is no reason SQLite and Python should go without it.
 */

#include "blobhttp.h"
#include "blobhttp_internal.hpp"

#include "http_config.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jmespath/jmespath.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

namespace blobhttp {

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
        {"finish_reason", finish_reason},
    };
}

namespace {

/// One POST to the chat-completions endpoint. Rate-limited like any other
/// request, and recorded on the same limiters, so an LLM call and a plain
/// bh_http_get against the same host share a budget rather than each having
/// their own.
std::pair<nlohmann::json, double> PostChatCompletion(const std::string &url,
                                                     const nlohmann::json &body,
                                                     const HttpConfig &config,
                                                     const Pairs &extra_headers) {
    auto session = std::make_shared<cpr::Session>();
    ShareConnections(*session);
    session->SetUrl(cpr::Url{url});
    session->SetTimeout(cpr::Timeout{config.timeout * 1000});

    cpr::Header headers;
    headers["Content-Type"] = "application/json";
    if (config.auth_type == "bearer" && !config.bearer_token.empty()) {
        headers["Authorization"] = "Bearer " + config.bearer_token;
    }
    for (auto &[k, v] : extra_headers) headers[k] = v;
    session->SetHeader(headers);

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

    auto body_str = body.dump();
    session->SetBody(cpr::Body{body_str});

    auto host = ExtractHost(url);
    AcquireRateLimit(GlobalLimiter(config.global_rate_limit_spec, config.global_burst));
    AcquireRateLimit(Registry().GetOrCreate(host, config.rate_limit_spec, config.burst));

    auto response = session->Post();
    RecordResponseStats(response, host);

    if (response.status_code != 200) {
        throw std::runtime_error("LLM request failed (HTTP " +
                                 std::to_string(response.status_code) +
                                 "): " + response.text.substr(0, 500));
    }
    return {nlohmann::json::parse(response.text), response.elapsed};
}

/// The assistant's text, or the arguments of its first tool call — which is
/// where the answer lives when an output schema forced a tool call.
std::string ExtractContent(const nlohmann::json &response) {
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

std::string ExtractFinishReason(const nlohmann::json &response) {
    auto &choice = response["choices"][0];
    if (choice.contains("finish_reason") && !choice["finish_reason"].is_null()) {
        return choice["finish_reason"].get<std::string>();
    }
    return "";
}

/// Integers arriving from SQL's json_object() may be strings.
int ParseInt(const nlohmann::json &cfg, const char *key, int def) {
    if (!cfg.contains(key)) return def;
    auto &v = cfg[key];
    if (v.is_number()) return v.get<int>();
    if (v.is_string()) {
        try {
            return std::stoi(v.get<std::string>());
        } catch (...) {
        }
    }
    return def;
}

Pairs JsonObjectToPairs(const nlohmann::json &cfg, const char *key) {
    Pairs out;
    if (cfg.contains(key) && cfg[key].is_object()) {
        for (auto &[k, v] : cfg[key].items()) {
            out.emplace_back(k, v.is_string() ? v.get<std::string>() : v.dump());
        }
    }
    return out;
}

} // namespace

LlmResult LlmCompleteLoop(const std::string &url,
                          nlohmann::json body,
                          const HttpConfig &config,
                          const Pairs &extra_headers,
                          const std::string &output_schema_str,
                          int max_continuations,
                          int max_retries) {
    LlmStats stats;

    // ── Schema setup ─────────────────────────────────────────────────
    //
    // A schema is imposed as a forced tool call rather than as a prompt
    // instruction: the model then has to emit arguments matching the schema,
    // which is a far stronger constraint than asking politely for JSON.
    const bool use_schema = !output_schema_str.empty();
    std::unique_ptr<jsoncons::jsonschema::json_schema<jsoncons::json>> compiled_schema;

    if (use_schema) {
        nlohmann::json schema_json = nlohmann::json::parse(output_schema_str);
        body["tools"] = nlohmann::json::array({nlohmann::json{
            {"type", "function"},
            {"function", {{"name", "extract"}, {"parameters", schema_json}}},
        }});
        body["tool_choice"] = {{"type", "function"}, {"function", {{"name", "extract"}}}};

        auto jc_schema = jsoncons::json::parse(output_schema_str);
        compiled_schema = std::make_unique<jsoncons::jsonschema::json_schema<jsoncons::json>>(
            jsoncons::jsonschema::make_json_schema(std::move(jc_schema)));
    }

    // ── Completion, continuing while truncated ───────────────────────
    auto do_complete = [&](nlohmann::json &req_body) -> std::string {
        std::string accumulated;
        for (int cont = 0; cont < max_continuations; cont++) {
            auto [response, elapsed] = PostChatCompletion(url, req_body, config, extra_headers);
            stats.AccumulateUsage(response, elapsed);

            accumulated += ExtractContent(response);
            auto finish_reason = ExtractFinishReason(response);
            if (finish_reason != "length") {
                stats.finish_reason = finish_reason;
                return accumulated;
            }

            stats.continuations++;
            req_body["messages"].push_back({{"role", "assistant"}, {"content", accumulated}});
            req_body["messages"].push_back(
                {{"role", "user"}, {"content", "Continue exactly where you left off."}});
        }
        throw std::runtime_error("LLM continuation limit (" +
                                 std::to_string(max_continuations) +
                                 ") reached — response still incomplete");
    };

    if (!use_schema) {
        LlmResult r;
        r.content = do_complete(body);
        r.stats = stats;
        return r;
    }

    // ── Validate, and retry with the errors as correction ────────────
    for (int attempt = 0; attempt < max_retries; attempt++) {
        nlohmann::json attempt_body = body;
        auto result = do_complete(attempt_body);

        try {
            auto parsed = jsoncons::json::parse(result);
            jsoncons::json_decoder<jsoncons::json> decoder;
            compiled_schema->validate(parsed, decoder);
            auto output = decoder.get_result();

            if (output.is_object() && output.contains("valid") && !output["valid"].as<bool>()) {
                std::ostringstream oss;
                oss << jsoncons::pretty_print(output);
                std::string error_text = oss.str();
                if (error_text.size() > 2000) error_text = error_text.substr(0, 2000) + "...";

                stats.retries++;
                body["messages"].push_back({{"role", "assistant"}, {"content", result}});
                body["messages"].push_back(
                    {{"role", "user"},
                     {"content", "Validation errors:\n" + error_text +
                                     "\nFix the errors and try again."}});
                continue;
            }

            LlmResult r;
            r.content = result;
            r.stats = stats;
            return r;

        } catch (const jsoncons::ser_error &e) {
            stats.retries++;
            body["messages"].push_back({{"role", "assistant"}, {"content", result}});
            body["messages"].push_back(
                {{"role", "user"},
                 {"content", std::string("Invalid JSON: ") + e.what() +
                                 "\nReturn valid JSON matching the schema."}});
            continue;
        }
    }

    throw std::runtime_error("Schema validation failed after " +
                             std::to_string(max_retries) + " attempts");
}

} // namespace blobhttp

using namespace blobhttp;

extern "C" {

char *bh_llm_complete(const char *request_json) {
    ClearError();
    if (!request_json) {
        SetError("request_json is required");
        return nullptr;
    }
    try {
        auto cfg = nlohmann::json::parse(request_json);

        std::string url = cfg.value("url", "");
        if (url.empty()) throw std::runtime_error("url is required");

        nlohmann::json body = cfg.contains("body") ? cfg["body"] : nlohmann::json::object();
        if (body.is_string()) body = nlohmann::json::parse(body.get<std::string>());

        auto config = ResolveConfig(url, JsonObjectToPairs(cfg, "http_config"));
        Pairs params;
        ResolveVaultSecrets(config, params);

        auto result = LlmCompleteLoop(url, std::move(body), config,
                                      JsonObjectToPairs(cfg, "headers"),
                                      cfg.value("output_schema", ""),
                                      ParseInt(cfg, "max_continuations", 10),
                                      ParseInt(cfg, "max_retries", 3));

        nlohmann::json out;
        out["content"] = result.content;
        out["stats"] = result.stats.ToJson();
        return DupString(out.dump());
    } catch (const std::exception &e) {
        SetError(e.what());
        return nullptr;
    }
}

char *bh_llm_adapt(const char *request_json) {
    ClearError();
    if (!request_json) {
        SetError("request_json is required");
        return nullptr;
    }
    try {
        auto cfg = nlohmann::json::parse(request_json);

        // Rendered upstream — in DuckDB by the llm_adapt() macro, which looks
        // the adapter up in the llm_adapter table and renders it through
        // blobtemplates. That half stays in the macro layer; this half is the
        // portable one.
        std::string prompt_text = cfg.value("prompt_text", "");
        if (prompt_text.empty()) throw std::runtime_error("prompt_text is required in config");

        std::string endpoint = cfg.value("endpoint", "http://localhost:8080/v1/chat/completions");
        std::string model = cfg.value("model", "anthropic/claude-haiku-4-5-20251001");
        std::string output_schema = cfg.value("output_schema", "");
        std::string response_jmespath = cfg.value("response_jmespath", "");

        auto config = ResolveConfig(endpoint, JsonObjectToPairs(cfg, "http_config"));
        Pairs params;
        ResolveVaultSecrets(config, params);

        nlohmann::json body = {
            {"model", model},
            {"max_tokens", ParseInt(cfg, "max_tokens", 4096)},
            {"messages",
             nlohmann::json::array({{{"role", "user"}, {"content", prompt_text}}})},
        };

        auto result = LlmCompleteLoop(endpoint, std::move(body), config, Pairs{},
                                      output_schema,
                                      ParseInt(cfg, "max_continuations", 10),
                                      ParseInt(cfg, "max_retries", 3));

        std::string content = result.content;
        if (!response_jmespath.empty()) {
            auto doc = jsoncons::json::parse(content);
            std::ostringstream oss;
            oss << jsoncons::jmespath::search(doc, response_jmespath);
            content = oss.str();
        }

        // {data, _meta} rather than {content, stats}: this is what
        // _llm_adapt_raw has always emitted and what blobapi's SQL reads.
        nlohmann::json out;
        try {
            out["data"] = nlohmann::json::parse(content);
        } catch (...) {
            out["data"] = content;
        }
        out["_meta"] = result.stats.ToJson();
        return DupString(out.dump());
    } catch (const std::exception &e) {
        SetError(e.what());
        return nullptr;
    }
}

} // extern "C"
