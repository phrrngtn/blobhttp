/*
 * _llm_complete_raw — DuckDB marshalling over bh_llm_complete.
 *
 * The completion loop, the schema retry and the stats all live in the core
 * now. This packs seven scalar arguments into the JSON the ABI takes, and
 * unpacks the content back out.
 *
 * The VARCHAR return is frozen: blobapi sources llm_complete.sql from the
 * installed package and reads the text directly, so this returns .content and
 * discards .stats. Nothing is lost at the ABI — a future STRUCT-returning
 * variant can surface the stats without any core change.
 */

#include "duckdb_extension.h"
#include "bhttp_llm.hpp"
#include "blobhttp.h"

#include <memory>
#include <string>

#include <nlohmann/json.hpp>

DUCKDB_EXTENSION_EXTERN

namespace blobhttp {

namespace {

std::string ReadVarchar(duckdb_vector vec, idx_t row) {
	auto *validity = duckdb_vector_get_validity(vec);
	if (validity && !(validity[row / 64] & (1ULL << (row % 64)))) return "";
	auto *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
	return std::string(duckdb_string_t_data(&data[row]), duckdb_string_t_length(data[row]));
}

int ReadInt(duckdb_vector vec, idx_t row) {
	return ((int32_t *)duckdb_vector_get_data(vec))[row];
}

/// Parse a JSON string argument, or return an empty object if it is blank.
nlohmann::json ParseOrEmpty(const std::string &s) {
	if (s.empty()) return nlohmann::json::object();
	return nlohmann::json::parse(s);
}

} // namespace

static void LlmCompleteScalarFunc(duckdb_function_info info,
                                  duckdb_data_chunk input,
                                  duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	if (input_size == 0) return;

	duckdb_vector url_vec = duckdb_data_chunk_get_vector(input, 0);
	duckdb_vector body_vec = duckdb_data_chunk_get_vector(input, 1);
	duckdb_vector headers_vec = duckdb_data_chunk_get_vector(input, 2);
	duckdb_vector config_vec = duckdb_data_chunk_get_vector(input, 3);
	duckdb_vector schema_vec = duckdb_data_chunk_get_vector(input, 4);
	duckdb_vector cont_vec = duckdb_data_chunk_get_vector(input, 5);
	duckdb_vector retries_vec = duckdb_data_chunk_get_vector(input, 6);

	for (idx_t row = 0; row < input_size; row++) {
		try {
			nlohmann::json request;
			request["url"] = ReadVarchar(url_vec, row);
			request["body"] = ParseOrEmpty(ReadVarchar(body_vec, row));
			request["headers"] = ParseOrEmpty(ReadVarchar(headers_vec, row));
			request["http_config"] = ParseOrEmpty(ReadVarchar(config_vec, row));
			request["output_schema"] = ReadVarchar(schema_vec, row);
			request["max_continuations"] = ReadInt(cont_vec, row);
			request["max_retries"] = ReadInt(retries_vec, row);

			auto request_str = request.dump();
			std::unique_ptr<char, void (*)(void *)> result(
			    bh_llm_complete(request_str.c_str()), bh_free);
			if (!result) {
				duckdb_scalar_function_set_error(info, bh_errmsg());
				return;
			}

			auto parsed = nlohmann::json::parse(result.get());
			auto content = parsed.value("content", "");
			duckdb_vector_assign_string_element_len(output, row, content.c_str(),
			                                        content.length());
		} catch (const std::exception &e) {
			duckdb_scalar_function_set_error(info, e.what());
			return;
		}
	}
}

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
	duckdb_destroy_scalar_function(&function);
	duckdb_destroy_logical_type(&varchar_type);
	duckdb_destroy_logical_type(&int_type);
}

} // namespace blobhttp
