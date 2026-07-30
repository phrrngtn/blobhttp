/*
 * _llm_adapt_raw — DuckDB marshalling over bh_llm_adapt.
 *
 * Takes one fully-resolved JSON blob and passes it straight through. The
 * adapter lookup and prompt rendering happen above this, in the llm_adapt()
 * macro: it reads the row from the llm_adapter table and renders the template
 * with blobtemplates' bt_template_render(). Those are genuinely DuckDB-shaped
 * — a table and a cross-extension call — which is why only the half below
 * moved to the core.
 *
 * The {data, _meta} envelope is frozen: blobapi's SQL reads it.
 */

#include "duckdb_extension.h"
#include "bhttp_llm.hpp"
#include "blobhttp.h"

#include <memory>
#include <cstring>
#include <string>

DUCKDB_EXTENSION_EXTERN

namespace blobhttp {

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
		std::string config(duckdb_string_t_data(&data[row]),
		                   duckdb_string_t_length(data[row]));

		std::unique_ptr<char, void (*)(void *)> result(bh_llm_adapt(config.c_str()), bh_free);
		if (!result) {
			duckdb_scalar_function_set_error(info, bh_errmsg());
			return;
		}
		duckdb_vector_assign_string_element_len(output, row, result.get(),
		                                        std::strlen(result.get()));
	}
}

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
