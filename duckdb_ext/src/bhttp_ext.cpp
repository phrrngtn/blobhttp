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
	blobhttp::RegisterHttpFunctions(connection);
	blobhttp::RegisterLlmFunctions(connection);
	blobhttp::RegisterLlmAdaptFunction(connection);
	return true;
}
