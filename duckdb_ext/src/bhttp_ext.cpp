#include "duckdb_extension.h"
#include "bhttp_ext.hpp"
#include "bhttp_llm.hpp"
#include "bhttp_llm_adapt.hpp"
#include "blobhttp.h"

#include <cstring>
#include <memory>

#include <cstring>
#include <string>

#include <nlohmann/json.hpp>

DUCKDB_EXTENSION_EXTERN

// ---------------------------------------------------------------------------
// negotiate_auth_token(url) -> VARCHAR
// Returns the base64-encoded SPNEGO token for the given HTTPS URL.
// ---------------------------------------------------------------------------

static void NegotiateAuthTokenFunc(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector url_vec = duckdb_data_chunk_get_vector(input, 0);

	auto url_data = (duckdb_string_t *)duckdb_vector_get_data(url_vec);

	for (idx_t row = 0; row < input_size; row++) {
		std::string url(duckdb_string_t_data(&url_data[row]),
		                duckdb_string_t_length(url_data[row]));
		std::unique_ptr<char, void (*)(void *)> header(
		    bh_negotiate_auth_header(url.c_str()), bh_free);
		if (!header) {
			duckdb_scalar_function_set_error(info, bh_errmsg());
			return;
		}
		duckdb_vector_assign_string_element_len(output, row, header.get(),
		                                        std::strlen(header.get()));
	}
}

// ---------------------------------------------------------------------------
// negotiate_auth_token_json(url) -> VARCHAR (JSON)
// Returns a JSON object with the token and debugging metadata.
// ---------------------------------------------------------------------------

static void NegotiateAuthTokenJsonFunc(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector url_vec = duckdb_data_chunk_get_vector(input, 0);

	auto url_data = (duckdb_string_t *)duckdb_vector_get_data(url_vec);

	for (idx_t row = 0; row < input_size; row++) {
		auto url_str = duckdb_string_t_data(&url_data[row]);
		auto url_len = duckdb_string_t_length(url_data[row]);

		std::string url(url_str, url_len);
		std::unique_ptr<char, void (*)(void *)> json(
		    bh_negotiate_auth_header_json(url.c_str()), bh_free);
		if (!json) {
			duckdb_scalar_function_set_error(info, bh_errmsg());
			return;
		}
		duckdb_vector_assign_string_element_len(output, row, json.get(),
		                                        std::strlen(json.get()));
	}
}

// ---------------------------------------------------------------------------
// Function registration helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// bh_sso_jwt(config_json) -> VARCHAR  — the access token
// bh_sso_jwt_json(config_json) -> VARCHAR — the provider's whole response
//
// Kerberos ticket in, JWT out. blobsso does this same exchange for httpfs and
// turns the result into S3 credentials; here the token is the product, so it
// can be handed to anything — a vault, an API expecting a bearer, another
// query.
// ---------------------------------------------------------------------------

static void SsoJwtFunc(duckdb_function_info info, duckdb_data_chunk input,
                       duckdb_vector output, bool whole_response) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector cfg_vec = duckdb_data_chunk_get_vector(input, 0);
	auto *validity = duckdb_vector_get_validity(cfg_vec);
	auto *data = (duckdb_string_t *)duckdb_vector_get_data(cfg_vec);

	for (idx_t row = 0; row < input_size; row++) {
		if (validity && !(validity[row / 64] & (1ULL << (row % 64)))) {
			duckdb_scalar_function_set_error(info, "bh_sso_jwt: config must not be NULL");
			return;
		}
		std::string cfg(duckdb_string_t_data(&data[row]), duckdb_string_t_length(data[row]));

		std::unique_ptr<char, void (*)(void *)> res(bh_sso_jwt(cfg.c_str()), bh_free);
		if (!res) {
			duckdb_scalar_function_set_error(info, bh_errmsg());
			return;
		}
		if (whole_response) {
			duckdb_vector_assign_string_element_len(output, row, res.get(),
			                                        std::strlen(res.get()));
		} else {
			// Pull out access_token so the common case is not a json_extract.
			try {
				auto tok = nlohmann::json::parse(res.get()).value("access_token", "");
				duckdb_vector_assign_string_element_len(output, row, tok.c_str(), tok.length());
			} catch (const std::exception &e) {
				duckdb_scalar_function_set_error(info, e.what());
				return;
			}
		}
	}
}

static void SsoJwtTokenFunc(duckdb_function_info i, duckdb_data_chunk in, duckdb_vector out) {
	SsoJwtFunc(i, in, out, false);
}

static void SsoJwtJsonFunc(duckdb_function_info i, duckdb_data_chunk in, duckdb_vector out) {
	SsoJwtFunc(i, in, out, true);
}

// ---------------------------------------------------------------------------
// bh_negotiate_available() -> BOOLEAN
// Is GSS-API present and usable in this process?
//
// The C ABI has always exported this; it simply was never surfaced. Without it
// the only way to ask from SQL was to call bh_negotiate_auth_header and read
// the failure, which conflates "no Kerberos on this machine" with "this
// particular URL was refused".
// ---------------------------------------------------------------------------

static void NegotiateAvailableFunc(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	(void)info;
	const idx_t input_size = duckdb_data_chunk_get_size(input);
	auto *out = (bool *)duckdb_vector_get_data(output);
	// Constant for the life of the process, so hoist it out of the row loop.
	const bool available = bh_negotiate_available() != 0;
	for (idx_t row = 0; row < input_size; row++) {
		out[row] = available;
	}
}

static void RegisterNoArgBoolFunction(duckdb_connection connection, const char *name,
                                      duckdb_scalar_function_t func) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, name);

	duckdb_logical_type bool_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
	duckdb_scalar_function_set_return_type(function, bool_type);
	duckdb_destroy_logical_type(&bool_type);

	duckdb_scalar_function_set_function(function, func);

	duckdb_register_scalar_function(connection, function);
	duckdb_destroy_scalar_function(&function);
}

static void RegisterScalarVarcharFunction(duckdb_connection connection, const char *name,
                                          duckdb_scalar_function_t func) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, name);

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_add_parameter(function, varchar_type);
	duckdb_scalar_function_set_return_type(function, varchar_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_scalar_function_set_function(function, func);

	duckdb_register_scalar_function(connection, function);
	duckdb_destroy_scalar_function(&function);
}

// ---------------------------------------------------------------------------
// Extension entry point
// ---------------------------------------------------------------------------

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info,
                            struct duckdb_extension_access *access) {
	RegisterScalarVarcharFunction(connection, "bh_negotiate_auth_header", NegotiateAuthTokenFunc);
	RegisterScalarVarcharFunction(connection, "bh_negotiate_auth_header_json", NegotiateAuthTokenJsonFunc);
	RegisterScalarVarcharFunction(connection, "bh_sso_jwt", SsoJwtTokenFunc);
	RegisterScalarVarcharFunction(connection, "bh_sso_jwt_json", SsoJwtJsonFunc);
	RegisterNoArgBoolFunction(connection, "bh_negotiate_available", NegotiateAvailableFunc);

	// Order is load-bearing: every raw _-prefixed scalar must be registered
	// before RegisterHttpMacros runs, because DuckDB validates a macro body at
	// CREATE time and a macro naming an unknown function is rejected.
	//
	// This was wrong until 2026-07-31. RegisterHttpFunctions used to register
	// the macros itself, so llm_complete and llm_adapt were created before
	// _llm_complete_raw and _llm_adapt_raw existed. They failed, TryRegisterMacro
	// swallowed the error, and the whole LLM surface was silently missing from
	// DuckDB while the raw functions sat there registered and unreachable.
	blobhttp::RegisterHttpFunctions(connection);
	blobhttp::RegisterLlmFunctions(connection);
	blobhttp::RegisterLlmAdaptFunction(connection);
	blobhttp::RegisterHttpMacros(connection);
	return true;
}
