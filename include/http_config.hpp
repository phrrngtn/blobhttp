#pragma once

#include <algorithm>
#include <chrono>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>

#include <cpr/cpr.h>

namespace blobhttp {

//! Extract hostname from a URL, lowercased. Returns empty string on failure.
inline std::string ExtractHostFromUrl(const std::string &url) {
	auto scheme_end = url.find("://");
	if (scheme_end == std::string::npos) return "";
	auto host_start = scheme_end + 3;
	// Skip userinfo@ if present
	auto at_pos = url.find('@', host_start);
	auto slash_pos = url.find('/', host_start);
	if (at_pos != std::string::npos && (slash_pos == std::string::npos || at_pos < slash_pos)) {
		host_start = at_pos + 1;
	}
	// Host ends at '/', ':', or '?' — whichever comes first
	auto host_end = url.find_first_of("/:?", host_start);
	if (host_end == std::string::npos) host_end = url.length();
	std::string host = url.substr(host_start, host_end - host_start);
	std::transform(host.begin(), host.end(), host.begin(), ::tolower);
	return host;
}

//! Resolved configuration for a single HTTP request.
//! Built by merging: per-call overrides > scope match > default > hard-coded fallbacks.
struct HttpConfig {
	double rate_limit_rps = 20.0;  // requests per second (parsed from rate_limit string)
	std::string rate_limit_spec = "20/s";
	double burst = 5.0;
	int timeout = 30;              // seconds
	bool verify_ssl = true;
	std::string proxy;             // empty = no proxy
	std::string ca_bundle;         // empty = system default
	std::string client_cert;       // empty = no client certificate
	std::string client_key;        // empty = no client key
	std::string auth_type;         // "negotiate", "bearer", or empty
	std::string bearer_token;      // for auth_type=bearer
	int64_t bearer_token_expires_at = 0; // Unix epoch seconds; 0 = no expiry check
	int max_concurrent = 10;       // max parallel requests in a scalar function chunk
	std::string global_rate_limit_spec; // empty = no global limit; only meaningful from "default" scope
	double global_burst = 10.0;

	// Vault/OpenBao integration — fetch secrets automatically.
	// If vault_path is set, blobhttp fetches the secret before making
	// the actual request and injects it per auth_type.
	std::string vault_path;            // e.g. "secret/blobapi/geocodio"
	std::string vault_addr = "http://127.0.0.1:8200"; // vault/bao address
	std::string vault_token;           // vault auth token
	std::string vault_field = "api_key"; // field to extract from the secret
	std::string vault_param_name;      // for auth_type=query_param: query param name for the key
	int vault_kv_version = 2;          // KV secrets engine version (1 or 2)

	// How to authenticate TO vault/OpenBao.
	//
	// "token"  (default) — vault_token above is a long-lived token, in plain
	//                      text in bh_http_config. Fine for a loopback dev
	//                      server; a liability for anything shared.
	// "jwt"              — no stored secret at all. The ambient Kerberos
	//                      ticket is exchanged for a JWT at oidc_issuer, and
	//                      that JWT is exchanged at vault's auth/jwt/login for
	//                      a short-lived token, cached for its own lease.
	std::string vault_auth_method = "token";
	std::string vault_jwt_role;        // the OpenBao JWT role to log in as
	std::string oidc_issuer;           // e.g. https://keycloak.example/realms/lake
	std::string oidc_client_id;
	std::string oidc_client_secret;

	//! Apply values from a JSON config object, overwriting only fields that are present.
	void MergeFrom(const nlohmann::json &j) {
		if (j.contains("rate_limit") && j["rate_limit"].is_string()) {
			rate_limit_spec = j["rate_limit"].get<std::string>();
		}
		if (j.contains("burst") && j["burst"].is_number()) {
			burst = j["burst"].get<double>();
		}
		if (j.contains("timeout") && j["timeout"].is_number()) {
			timeout = j["timeout"].get<int>();
		}
		if (j.contains("verify_ssl") && j["verify_ssl"].is_boolean()) {
			verify_ssl = j["verify_ssl"].get<bool>();
		}
		if (j.contains("proxy") && j["proxy"].is_string()) {
			proxy = j["proxy"].get<std::string>();
		}
		if (j.contains("ca_bundle") && j["ca_bundle"].is_string()) {
			ca_bundle = j["ca_bundle"].get<std::string>();
		}
		if (j.contains("client_cert") && j["client_cert"].is_string()) {
			client_cert = j["client_cert"].get<std::string>();
		}
		if (j.contains("client_key") && j["client_key"].is_string()) {
			client_key = j["client_key"].get<std::string>();
		}
		if (j.contains("auth_type") && j["auth_type"].is_string()) {
			auth_type = j["auth_type"].get<std::string>();
		}
		if (j.contains("bearer_token") && j["bearer_token"].is_string()) {
			bearer_token = j["bearer_token"].get<std::string>();
		}
		if (j.contains("bearer_token_expires_at") && j["bearer_token_expires_at"].is_number()) {
			bearer_token_expires_at = j["bearer_token_expires_at"].get<int64_t>();
		}
		if (j.contains("max_concurrent") && j["max_concurrent"].is_number()) {
			max_concurrent = j["max_concurrent"].get<int>();
			if (max_concurrent < 1) max_concurrent = 1;
		}
		if (j.contains("global_rate_limit") && j["global_rate_limit"].is_string()) {
			global_rate_limit_spec = j["global_rate_limit"].get<std::string>();
		}
		if (j.contains("global_burst") && j["global_burst"].is_number()) {
			global_burst = j["global_burst"].get<double>();
		}
		if (j.contains("vault_path") && j["vault_path"].is_string()) {
			vault_path = j["vault_path"].get<std::string>();
		}
		if (j.contains("vault_addr") && j["vault_addr"].is_string()) {
			vault_addr = j["vault_addr"].get<std::string>();
		}
		if (j.contains("vault_token") && j["vault_token"].is_string()) {
			vault_token = j["vault_token"].get<std::string>();
		}
		if (j.contains("vault_field") && j["vault_field"].is_string()) {
			vault_field = j["vault_field"].get<std::string>();
		}
		if (j.contains("vault_param_name") && j["vault_param_name"].is_string()) {
			vault_param_name = j["vault_param_name"].get<std::string>();
		}
		if (j.contains("vault_kv_version") && j["vault_kv_version"].is_number()) {
			vault_kv_version = j["vault_kv_version"].get<int>();
		}
		if (j.contains("vault_auth_method") && j["vault_auth_method"].is_string()) {
			vault_auth_method = j["vault_auth_method"].get<std::string>();
		}
		if (j.contains("vault_jwt_role") && j["vault_jwt_role"].is_string()) {
			vault_jwt_role = j["vault_jwt_role"].get<std::string>();
		}
		if (j.contains("oidc_issuer") && j["oidc_issuer"].is_string()) {
			oidc_issuer = j["oidc_issuer"].get<std::string>();
		}
		if (j.contains("oidc_client_id") && j["oidc_client_id"].is_string()) {
			oidc_client_id = j["oidc_client_id"].get<std::string>();
		}
		if (j.contains("oidc_client_secret") && j["oidc_client_secret"].is_string()) {
			oidc_client_secret = j["oidc_client_secret"].get<std::string>();
		}
	}
};

//! Resolve the HttpConfig for a given URL from a config map.
//! The config_entries is a map of scope -> json-config-string.
//!
//! Resolution order:
//! 1. Hard-coded defaults (in HttpConfig struct)
//! 2. "default" key from the config map
//! 3. Longest matching scope prefix
//! 4. Per-call overrides (applied by the caller after this function returns)
inline HttpConfig ResolveConfig(const std::string &url,
                                const std::vector<std::pair<std::string, std::string>> &config_entries) {
	HttpConfig config;

	// Apply "default" entry first
	for (auto &[scope, json_str] : config_entries) {
		if (scope == "default") {
			try {
				config.MergeFrom(nlohmann::json::parse(json_str));
			} catch (...) {
				// Malformed JSON in default config — skip it
			}
			break;
		}
	}

	// Find longest matching scope: prefer prefix match, fall back to domain-suffix match
	std::string best_scope;
	std::string best_json;
	bool best_is_prefix = false;
	for (auto &[scope, json_str] : config_entries) {
		if (scope == "default") {
			continue;
		}
		// Prefix match: url starts with scope (e.g. https://api.example.com/v1 matches https://api.example.com/v1/users)
		if (url.rfind(scope, 0) == 0) {
			if (!best_is_prefix || scope.length() > best_scope.length()) {
				best_scope = scope;
				best_json = json_str;
				best_is_prefix = true;
			}
			continue;
		}
		// Domain-suffix match: scope's host is a suffix of url's host
		// (e.g. scope https://acmecorp.test matches url https://sub.acmecorp.test/path)
		if (!best_is_prefix) {
			auto scope_host = ExtractHostFromUrl(scope);
			auto url_host = ExtractHostFromUrl(url);
			if (!scope_host.empty() && !url_host.empty()) {
				bool matches = (url_host == scope_host) ||
				    (url_host.length() > scope_host.length() &&
				     url_host.compare(url_host.length() - scope_host.length(), scope_host.length(), scope_host) == 0 &&
				     url_host[url_host.length() - scope_host.length() - 1] == '.');
				if (matches && scope_host.length() > ExtractHostFromUrl(best_scope).length()) {
					best_scope = scope;
					best_json = json_str;
				}
			}
		}
	}

	// Apply the best scope match on top of defaults
	if (!best_scope.empty()) {
		try {
			config.MergeFrom(nlohmann::json::parse(best_json));
		} catch (...) {
			// Malformed JSON in scope config — skip it
		}
	}

	return config;
}

// ---------------------------------------------------------------------------
// Vault/OpenBao secret fetching with process-global cache.
//
// Uses cpr directly — does NOT go through blobhttp config resolution,
// so there's no recursion risk and no proxy/rate-limit/auth overhead.
// Vault is assumed to be a local or trusted-network service.
// ---------------------------------------------------------------------------

//! Cache entry for a fetched vault secret.
struct VaultCacheEntry {
	std::string value;                                  // the extracted field value
	std::chrono::steady_clock::time_point fetched_at;   // for TTL
};

//! Process-global vault secret cache. Thread-safe.
inline std::string FetchVaultSecret(const std::string &vault_addr,
                                     const std::string &vault_token,
                                     const std::string &vault_path,
                                     const std::string &vault_field,
                                     int kv_version) {
	// Cache key: addr + path + field
	std::string cache_key = vault_addr + "|" + vault_path + "|" + vault_field;

	static std::mutex cache_mutex;
	static std::unordered_map<std::string, VaultCacheEntry> cache;
	static constexpr auto CACHE_TTL = std::chrono::minutes(5);

	// Check cache
	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		auto it = cache.find(cache_key);
		if (it != cache.end()) {
			auto age = std::chrono::steady_clock::now() - it->second.fetched_at;
			if (age < CACHE_TTL) {
				return it->second.value;
			}
		}
	}

	// Fetch from vault — bare cpr, no blobhttp config
	// KV v2: GET /v1/secret/data/{path} → $.data.data.{field}
	// KV v1: GET /v1/{path}             → $.data.{field}
	std::string url;
	std::string json_path;
	if (kv_version == 2) {
		// vault_path might be "secret/blobapi/geocodio"
		// KV v2 API: /v1/secret/data/blobapi/geocodio
		auto slash = vault_path.find('/');
		if (slash != std::string::npos) {
			std::string mount = vault_path.substr(0, slash);
			std::string subpath = vault_path.substr(slash + 1);
			url = vault_addr + "/v1/" + mount + "/data/" + subpath;
		} else {
			url = vault_addr + "/v1/" + vault_path;
		}
		json_path = "data";  // nested under $.data.data for KV v2
	} else {
		url = vault_addr + "/v1/" + vault_path;
		json_path = "";  // directly under $.data for KV v1
	}

	cpr::Response response = cpr::Get(
		cpr::Url{url},
		cpr::Header{{"X-Vault-Token", vault_token}},
		cpr::Timeout{5000}
	);

	if (response.status_code != 200) {
		throw std::runtime_error(
			"Vault fetch failed for " + vault_path +
			" (status " + std::to_string(response.status_code) + "): " +
			response.text.substr(0, 200));
	}

	// Parse response and extract the field
	std::string value;
	try {
		auto j = nlohmann::json::parse(response.text);
		if (kv_version == 2) {
			value = j["data"]["data"][vault_field].get<std::string>();
		} else {
			value = j["data"][vault_field].get<std::string>();
		}
	} catch (const std::exception &e) {
		throw std::runtime_error(
			"Vault secret parse error for " + vault_path + "." + vault_field +
			": " + e.what());
	}

	// Cache it
	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		cache[cache_key] = {value, std::chrono::steady_clock::now()};
	}

	return value;
}

//! After ResolveConfig, call this to fetch vault secrets and inject auth.
//! Modifies config in place — sets bearer_token or populates params.
inline void ResolveVaultSecrets(HttpConfig &config,
                                 std::vector<std::pair<std::string, std::string>> &params) {
	// vault_token may have been filled in by ResolveVaultAuth from an SSO
	// exchange rather than read from config — see blobhttp_oidc.cpp. Either
	// way, by here it is just a token.
	if (config.vault_path.empty() || config.vault_token.empty()) {
		return;
	}

	std::string secret = FetchVaultSecret(
		config.vault_addr, config.vault_token, config.vault_path,
		config.vault_field, config.vault_kv_version);

	if (config.auth_type == "bearer") {
		config.bearer_token = secret;
	} else if (config.auth_type == "query_param" && !config.vault_param_name.empty()) {
		params.emplace_back(config.vault_param_name, secret);
	}
	// For other auth_types, the secret is fetched but the caller
	// decides how to use it.
}

} // namespace blobhttp
