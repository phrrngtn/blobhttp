#include "duckdb_extension.h"
#include "bhttp_ext.hpp"
#include "blobhttp.h"
#include "blobhttp_internal.hpp"
#include "http_config.hpp"
#include "rate_limiter.hpp"

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "sql_resources.hpp"

DUCKDB_EXTENSION_EXTERN

namespace blobhttp {


//! Read a MAP(VARCHAR, VARCHAR) from a data chunk vector at the given row.
//! MAPs are stored as LIST(STRUCT(key, value)). Returns empty if NULL.
static std::vector<std::pair<std::string, std::string>>
ReadMapVector(duckdb_vector map_vec, uint64_t *validity, idx_t row) {
	std::vector<std::pair<std::string, std::string>> result;
	if (validity && !(validity[row / 64] & (1ULL << (row % 64)))) {
		return result;
	}

	auto *entries = (duckdb_list_entry *)duckdb_vector_get_data(map_vec);
	auto offset = entries[row].offset;
	auto length = entries[row].length;

	duckdb_vector child = duckdb_list_vector_get_child(map_vec);
	duckdb_vector key_vec = duckdb_struct_vector_get_child(child, 0);
	duckdb_vector val_vec = duckdb_struct_vector_get_child(child, 1);

	auto *key_data = (duckdb_string_t *)duckdb_vector_get_data(key_vec);
	auto *val_data = (duckdb_string_t *)duckdb_vector_get_data(val_vec);

	for (idx_t i = 0; i < length; i++) {
		idx_t idx = offset + i;
		auto k = std::string(duckdb_string_t_data(&key_data[idx]),
		                     duckdb_string_t_length(key_data[idx]));
		auto v = std::string(duckdb_string_t_data(&val_data[idx]),
		                     duckdb_string_t_length(val_data[idx]));
		result.emplace_back(std::move(k), std::move(v));
	}
	return result;
}

//! Write a MAP(VARCHAR, VARCHAR) entry into a MAP vector at the given row.
//! The caller must reserve space and set the final list size after all rows.
//! @param map_vec   the MAP vector (top-level column or struct child)
//! @param row       row index for the list entry
//! @param kvs       key-value pairs to write
//! @param offset    running offset into the list child; updated after writing
static void WriteMapEntry(duckdb_vector map_vec, idx_t row,
                          const std::vector<std::pair<std::string, std::string>> &kvs,
                          idx_t &offset) {
	auto *entries = (duckdb_list_entry *)duckdb_vector_get_data(map_vec);
	entries[row].offset = offset;
	entries[row].length = kvs.size();

	duckdb_vector child = duckdb_list_vector_get_child(map_vec);
	duckdb_vector key_vec = duckdb_struct_vector_get_child(child, 0);
	duckdb_vector val_vec = duckdb_struct_vector_get_child(child, 1);

	for (auto &[k, v] : kvs) {
		duckdb_vector_assign_string_element_len(key_vec, offset, k.c_str(), k.length());
		duckdb_vector_assign_string_element_len(val_vec, offset, v.c_str(), v.length());
		offset++;
	}
}

//! Helper to read a VARCHAR vector element, returning empty string if null.
static std::string ReadVarchar(duckdb_vector vec, uint64_t *validity, idx_t row) {
	if (validity && !(validity[row / 64] & (1ULL << (row % 64)))) {
		return "";
	}
	auto *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
	auto str = duckdb_string_t_data(&data[row]);
	auto len = duckdb_string_t_length(data[row]);
	return std::string(str, len);
}

//! Write the non-MAP struct fields for one result.
//! MAP fields (2, 6) share a list child across rows and are written in bulk
//! afterwards with coordinated offsets.
static void WriteResultScalarFields(duckdb_vector output, idx_t row,
                                    const bh_batch *batch, size_t i) {
	auto set_varchar = [&](idx_t col, const char *s, size_t len) {
		duckdb_vector vec = duckdb_struct_vector_get_child(output, col);
		duckdb_vector_assign_string_element_len(vec, row, s ? s : "", len);
	};
	auto set_int = [&](idx_t col, int val) {
		duckdb_vector vec = duckdb_struct_vector_get_child(output, col);
		((int32_t *)duckdb_vector_get_data(vec))[row] = val;
	};
	auto set_double = [&](idx_t col, double val) {
		duckdb_vector vec = duckdb_struct_vector_get_child(output, col);
		((double *)duckdb_vector_get_data(vec))[row] = val;
	};

	size_t len = 0;
	const char *s;

	s = bh_result_request_url(batch, i, &len);       set_varchar(0, s, len);
	s = bh_result_request_method(batch, i, &len);    set_varchar(1, s, len);
	s = (const char *)bh_result_request_body(batch, i, &len); set_varchar(3, s, len);
	set_int(4, bh_result_status(batch, i));
	s = bh_result_status_line(batch, i, &len);       set_varchar(5, s, len);

	// response_body (VARCHAR, field 7) and response_blob (BLOB, field 11) are
	// the same bytes. VARCHAR silently degrades for a non-UTF-8 body — every
	// string operation on it yields NULL — which is exactly why the BLOB field
	// exists and why bh_result_body hands back raw bytes with a length.
	//
	// body_len is its own variable rather than reusing `len`: the BLOB is
	// written last, and the intervening response_url accessor would otherwise
	// overwrite the length out-param — which truncated the blob to the length
	// of the URL, a bug that reads like a server problem rather than ours.
	size_t body_len = 0;
	const char *body = (const char *)bh_result_body(batch, i, &body_len);
	set_varchar(7, body, body_len);

	s = bh_result_response_url(batch, i, &len);      set_varchar(8, s, len);
	set_double(9, bh_result_elapsed(batch, i));
	set_int(10, bh_result_redirect_count(batch, i));

	duckdb_vector blob_vec = duckdb_struct_vector_get_child(output, 11);
	duckdb_vector_assign_string_element_len(blob_vec, row, body ? body : "", body_len);
}

//! Copy one result's headers out of the batch into key/value pairs.
static std::vector<std::pair<std::string, std::string>>
ResultHeaders(const bh_batch *batch, size_t i, int which) {
	std::vector<std::pair<std::string, std::string>> out;
	size_t n = bh_result_header_count(batch, i, which);
	out.reserve(n);
	for (size_t k = 0; k < n; k++) {
		size_t nlen = 0, vlen = 0;
		const char *name = bh_result_header_name(batch, i, which, k, &nlen);
		const char *value = bh_result_header_value(batch, i, which, k, &vlen);
		out.emplace_back(std::string(name ? name : "", nlen),
		                 std::string(value ? value : "", vlen));
	}
	return out;
}

// ---------------------------------------------------------------------------
// Scalar function: _bh_http_raw_request(method, url, headers, params, body,
//                                       content_type, config)
//
// Marshalling only. Config resolution, Vault lookup, rate limiting, session
// building, execution and response shaping all live in the core now — this
// reads DuckDB vectors in, hands the whole chunk to one batch, and writes the
// results back out.
// ---------------------------------------------------------------------------

static void HttpRawRequestScalarFunc(duckdb_function_info info, duckdb_data_chunk input,
                                     duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	if (input_size == 0) return;

	duckdb_vector method_vec = duckdb_data_chunk_get_vector(input, 0);
	duckdb_vector url_vec = duckdb_data_chunk_get_vector(input, 1);
	duckdb_vector headers_vec = duckdb_data_chunk_get_vector(input, 2);
	duckdb_vector params_vec = duckdb_data_chunk_get_vector(input, 3);
	duckdb_vector body_vec = duckdb_data_chunk_get_vector(input, 4);
	duckdb_vector ct_vec = duckdb_data_chunk_get_vector(input, 5);
	duckdb_vector config_vec = duckdb_data_chunk_get_vector(input, 6);

	auto *method_validity = duckdb_vector_get_validity(method_vec);
	auto *url_validity = duckdb_vector_get_validity(url_vec);
	auto *headers_validity = duckdb_vector_get_validity(headers_vec);
	auto *params_validity = duckdb_vector_get_validity(params_vec);
	auto *body_validity = duckdb_vector_get_validity(body_vec);
	auto *ct_validity = duckdb_vector_get_validity(ct_vec);
	auto *config_validity = duckdb_vector_get_validity(config_vec);

	std::vector<std::vector<std::pair<std::string, std::string>>> all_req_headers(input_size);
	std::vector<std::vector<std::pair<std::string, std::string>>> all_resp_headers(input_size);

	// The config argument is one expression evaluated per row, and in practice
	// it is getvariable('bh_http_config') — identical down the chunk. Rows are
	// grouped into batches by config string so the common case is a single
	// batch that fans out, while a genuinely per-row config still resolves
	// correctly rather than silently taking row 0's.
	idx_t row = 0;
	while (row < input_size) {
		std::string config_json = ReadVarchar(config_vec, config_validity, row);
		idx_t group_end = row + 1;
		while (group_end < input_size &&
		       ReadVarchar(config_vec, config_validity, group_end) == config_json) {
			group_end++;
		}

		bh_batch *batch = bh_batch_new(config_json.empty() ? "{}" : config_json.c_str());
		if (!batch) {
			duckdb_scalar_function_set_error(info, bh_errmsg());
			return;
		}

		for (idx_t r = row; r < group_end; r++) {
			auto method = ReadVarchar(method_vec, method_validity, r);
			auto url = ReadVarchar(url_vec, url_validity, r);
			if (method.empty() || url.empty()) {
				bh_batch_free(batch);
				duckdb_scalar_function_set_error(info, "method and url are required");
				return;
			}
			auto headers = ReadVarchar(headers_vec, headers_validity, r);
			auto params = ReadVarchar(params_vec, params_validity, r);
			auto body = ReadVarchar(body_vec, body_validity, r);
			auto content_type = ReadVarchar(ct_vec, ct_validity, r);

			if (bh_batch_add(batch, method.c_str(), url.c_str(),
			                 headers.empty() ? nullptr : headers.c_str(),
			                 params.empty() ? nullptr : params.c_str(),
			                 body.empty() ? nullptr : body.data(), body.size(),
			                 content_type.empty() ? nullptr : content_type.c_str(),
			                 -1, -1) != 0) {
				bh_batch_free(batch);
				duckdb_scalar_function_set_error(info, bh_errmsg());
				return;
			}
		}

		if (bh_batch_perform(batch) != 0) {
			bh_batch_free(batch);
			duckdb_scalar_function_set_error(info, bh_errmsg());
			return;
		}

		for (idx_t r = row; r < group_end; r++) {
			size_t i = r - row;
			WriteResultScalarFields(output, r, batch, i);
			all_req_headers[r] = ResultHeaders(batch, i, BH_REQUEST_HEADERS);
			all_resp_headers[r] = ResultHeaders(batch, i, BH_RESPONSE_HEADERS);
		}

		bh_batch_free(batch);
		row = group_end;
	}

	// Write MAP fields in bulk. MAP vectors share a single list child across
	// all rows, so the total must be reserved and written with coordinated
	// offsets.
	idx_t total_req = 0, total_resp = 0;
	for (idx_t r = 0; r < input_size; r++) {
		total_req += all_req_headers[r].size();
		total_resp += all_resp_headers[r].size();
	}

	duckdb_vector req_headers_map = duckdb_struct_vector_get_child(output, 2);
	duckdb_vector resp_headers_map = duckdb_struct_vector_get_child(output, 6);
	duckdb_list_vector_reserve(req_headers_map, total_req);
	duckdb_list_vector_reserve(resp_headers_map, total_resp);

	idx_t req_offset = 0, resp_offset = 0;
	for (idx_t r = 0; r < input_size; r++) {
		WriteMapEntry(req_headers_map, r, all_req_headers[r], req_offset);
		WriteMapEntry(resp_headers_map, r, all_resp_headers[r], resp_offset);
	}
	duckdb_list_vector_set_size(req_headers_map, total_req);
	duckdb_list_vector_set_size(resp_headers_map, total_resp);
}


// Build the STRUCT return type matching the table function's output schema.
static duckdb_logical_type CreateHttpResultStructType() {
	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_logical_type double_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
	duckdb_logical_type blob_type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
	duckdb_logical_type map_type = duckdb_create_map_type(varchar_type, varchar_type);

	duckdb_logical_type member_types[] = {
	    varchar_type, varchar_type, map_type,     varchar_type,  // request_url, method, headers, body
	    int_type,                                                 // response_status_code
	    varchar_type, map_type,     varchar_type, varchar_type,  // response_status, headers, body, url
	    double_type,                                              // elapsed
	    int_type,                                                 // redirect_count
	    blob_type                                                 // response_blob
	};
	const char *member_names[] = {
	    "request_url", "request_method", "request_headers", "request_body",
	    "response_status_code",
	    "response_status", "response_headers", "response_body", "response_url",
	    "elapsed",
	    "redirect_count",
	    "response_blob"
	};

	duckdb_logical_type struct_type = duckdb_create_struct_type(member_types, member_names, 12);

	duckdb_destroy_logical_type(&varchar_type);
	duckdb_destroy_logical_type(&int_type);
	duckdb_destroy_logical_type(&double_type);
	duckdb_destroy_logical_type(&blob_type);
	duckdb_destroy_logical_type(&map_type);

	return struct_type;
}

//! Register a scalar HTTP function with the given name and volatility.
//! Both the idempotent and volatile variants share the same implementation;
//! they differ only in how the optimizer treats them.
static void RegisterHttpScalarVariant(duckdb_connection connection, const char *name, bool is_volatile) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, name);

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_logical_type map_type = duckdb_create_map_type(varchar_type, varchar_type);

	// All VARCHAR (JSON strings) — uniform, composable, works across SQLite/DuckDB.
	// The SQL macros use CAST(headers AS JSON) to accept MAP literals transparently.
	// (method, url, headers_json, params_json, body, content_type, config_json)
	duckdb_scalar_function_add_parameter(function, varchar_type); // 0: method
	duckdb_scalar_function_add_parameter(function, varchar_type); // 1: url
	duckdb_scalar_function_add_parameter(function, varchar_type); // 2: headers (JSON object)
	duckdb_scalar_function_add_parameter(function, varchar_type); // 3: params (JSON object → query string)
	duckdb_scalar_function_add_parameter(function, varchar_type); // 4: body
	duckdb_scalar_function_add_parameter(function, varchar_type); // 5: content_type
	duckdb_scalar_function_add_parameter(function, varchar_type); // 6: config (JSON string)

	duckdb_logical_type struct_type = CreateHttpResultStructType();
	duckdb_scalar_function_set_return_type(function, struct_type);
	duckdb_destroy_logical_type(&struct_type);
	duckdb_destroy_logical_type(&map_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_scalar_function_set_function(function, HttpRawRequestScalarFunc);
	duckdb_scalar_function_set_special_handling(function);
	if (is_volatile) {
		duckdb_scalar_function_set_volatile(function);
	}

	duckdb_register_scalar_function(connection, function);
	duckdb_destroy_scalar_function(&function);
}

static void RegisterHttpRawRequestScalar(duckdb_connection connection) {
	// Idempotent variant: safe to deduplicate identical calls (GET, HEAD, etc.)
	RegisterHttpScalarVariant(connection, "_bh_http_raw_request", false);
	// Volatile variant: every call fires regardless of argument identity (POST, PATCH, etc.)
	RegisterHttpScalarVariant(connection, "_bh_http_raw_request_volatile", true);
}

// ---------------------------------------------------------------------------
// Table function: bh_http_rate_limit_stats()
// Returns one row per host with rate limiter diagnostics.
// ---------------------------------------------------------------------------

struct RateLimitStatsData {
	struct HostStats {
		std::string host;
		std::string rate_spec;
		double rate_rps;
		double burst;
		uint64_t requests;
		uint64_t paced;
		double total_wait_seconds;
		uint64_t throttled_429;
		double backlog_seconds;
		// Response facts
		uint64_t total_responses;
		uint64_t total_response_bytes;
		double total_elapsed;
		double min_elapsed;
		double max_elapsed;
		uint64_t errors;
	};
	std::vector<HostStats> rows;
	idx_t current_row = 0;
};

static void DestroyRateLimitStatsData(void *data) {
	delete static_cast<RateLimitStatsData *>(data);
}

static void RateLimitStatsBind(duckdb_bind_info info) {
	// Snapshot the stats at bind time
	auto *data = new RateLimitStatsData();
	auto snapshot = [](const std::string &host, GCRARateLimiter &limiter) -> RateLimitStatsData::HostStats {
		return {host, limiter.RateSpec(), limiter.Rate(), limiter.Burst(), limiter.Requests(),
		    limiter.Paced(), limiter.TotalWaitSeconds(), limiter.Throttled429(), limiter.BacklogSeconds(),
		    limiter.TotalResponses(), limiter.TotalResponseBytes(), limiter.TotalElapsed(),
		    limiter.MinElapsed(), limiter.MaxElapsed(), limiter.Errors()};
	};

	// Include the global limiter as a special "(global)" row if configured
	auto *global = GlobalLimiterSnapshot();
	if (global) {
		data->rows.push_back(snapshot("(global)", *global));
	}

	Registry().ForEach([&](const std::string &host, GCRARateLimiter &limiter) {
		data->rows.push_back(snapshot(host, limiter));
	});

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_logical_type bigint_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_logical_type double_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);

	duckdb_bind_add_result_column(info, "host", varchar_type);
	duckdb_bind_add_result_column(info, "rate_limit", varchar_type);
	duckdb_bind_add_result_column(info, "rate_rps", double_type);
	duckdb_bind_add_result_column(info, "burst", double_type);
	duckdb_bind_add_result_column(info, "requests", bigint_type);
	duckdb_bind_add_result_column(info, "paced", bigint_type);
	duckdb_bind_add_result_column(info, "total_wait_seconds", double_type);
	duckdb_bind_add_result_column(info, "throttled_429", bigint_type);
	duckdb_bind_add_result_column(info, "backlog_seconds", double_type);
	duckdb_bind_add_result_column(info, "total_responses", bigint_type);
	duckdb_bind_add_result_column(info, "total_response_bytes", bigint_type);
	duckdb_bind_add_result_column(info, "total_elapsed", double_type);
	duckdb_bind_add_result_column(info, "min_elapsed", double_type);
	duckdb_bind_add_result_column(info, "max_elapsed", double_type);
	duckdb_bind_add_result_column(info, "errors", bigint_type);

	duckdb_destroy_logical_type(&varchar_type);
	duckdb_destroy_logical_type(&bigint_type);
	duckdb_destroy_logical_type(&double_type);

	duckdb_bind_set_cardinality(info, data->rows.size(), true);
	duckdb_bind_set_bind_data(info, data, DestroyRateLimitStatsData);
}

static void RateLimitStatsInit(duckdb_init_info info) {
	// No per-thread state needed; we use bind_data.current_row
}

static void RateLimitStatsExecute(duckdb_function_info info, duckdb_data_chunk output) {
	auto *data = static_cast<RateLimitStatsData *>(duckdb_function_get_bind_data(info));

	idx_t remaining = data->rows.size() - data->current_row;
	idx_t count = std::min(remaining, duckdb_vector_size());
	if (count == 0) {
		duckdb_data_chunk_set_size(output, 0);
		return;
	}

	duckdb_vector host_vec = duckdb_data_chunk_get_vector(output, 0);
	duckdb_vector rate_limit_vec = duckdb_data_chunk_get_vector(output, 1);
	duckdb_vector rate_rps_vec = duckdb_data_chunk_get_vector(output, 2);
	duckdb_vector burst_vec = duckdb_data_chunk_get_vector(output, 3);
	duckdb_vector requests_vec = duckdb_data_chunk_get_vector(output, 4);
	duckdb_vector paced_vec = duckdb_data_chunk_get_vector(output, 5);
	duckdb_vector wait_vec = duckdb_data_chunk_get_vector(output, 6);
	duckdb_vector throttled_vec = duckdb_data_chunk_get_vector(output, 7);
	duckdb_vector backlog_vec = duckdb_data_chunk_get_vector(output, 8);
	duckdb_vector total_resp_vec = duckdb_data_chunk_get_vector(output, 9);
	duckdb_vector total_bytes_vec = duckdb_data_chunk_get_vector(output, 10);
	duckdb_vector total_elapsed_vec = duckdb_data_chunk_get_vector(output, 11);
	duckdb_vector min_elapsed_vec = duckdb_data_chunk_get_vector(output, 12);
	duckdb_vector max_elapsed_vec = duckdb_data_chunk_get_vector(output, 13);
	duckdb_vector errors_vec = duckdb_data_chunk_get_vector(output, 14);

	auto *rate_rps_data = (double *)duckdb_vector_get_data(rate_rps_vec);
	auto *burst_data = (double *)duckdb_vector_get_data(burst_vec);
	auto *requests_data = (int64_t *)duckdb_vector_get_data(requests_vec);
	auto *paced_data = (int64_t *)duckdb_vector_get_data(paced_vec);
	auto *wait_data = (double *)duckdb_vector_get_data(wait_vec);
	auto *throttled_data = (int64_t *)duckdb_vector_get_data(throttled_vec);
	auto *backlog_data = (double *)duckdb_vector_get_data(backlog_vec);
	auto *total_resp_data = (int64_t *)duckdb_vector_get_data(total_resp_vec);
	auto *total_bytes_data = (int64_t *)duckdb_vector_get_data(total_bytes_vec);
	auto *total_elapsed_data = (double *)duckdb_vector_get_data(total_elapsed_vec);
	auto *min_elapsed_data = (double *)duckdb_vector_get_data(min_elapsed_vec);
	auto *max_elapsed_data = (double *)duckdb_vector_get_data(max_elapsed_vec);
	auto *errors_data = (int64_t *)duckdb_vector_get_data(errors_vec);

	for (idx_t i = 0; i < count; i++) {
		auto &row = data->rows[data->current_row + i];
		duckdb_vector_assign_string_element_len(host_vec, i, row.host.c_str(), row.host.length());
		duckdb_vector_assign_string_element_len(rate_limit_vec, i, row.rate_spec.c_str(), row.rate_spec.length());
		rate_rps_data[i] = row.rate_rps;
		burst_data[i] = row.burst;
		requests_data[i] = static_cast<int64_t>(row.requests);
		paced_data[i] = static_cast<int64_t>(row.paced);
		wait_data[i] = row.total_wait_seconds;
		throttled_data[i] = static_cast<int64_t>(row.throttled_429);
		backlog_data[i] = row.backlog_seconds;
		total_resp_data[i] = static_cast<int64_t>(row.total_responses);
		total_bytes_data[i] = static_cast<int64_t>(row.total_response_bytes);
		total_elapsed_data[i] = row.total_elapsed;
		min_elapsed_data[i] = row.min_elapsed;
		max_elapsed_data[i] = row.max_elapsed;
		errors_data[i] = static_cast<int64_t>(row.errors);
	}

	data->current_row += count;
	duckdb_data_chunk_set_size(output, count);
}

static void RegisterRateLimitStatsFunction(duckdb_connection connection) {
	duckdb_table_function function = duckdb_create_table_function();
	duckdb_table_function_set_name(function, "bh_http_rate_limit_stats");

	duckdb_table_function_set_bind(function, RateLimitStatsBind);
	duckdb_table_function_set_init(function, RateLimitStatsInit);
	duckdb_table_function_set_function(function, RateLimitStatsExecute);

	duckdb_register_table_function(connection, function);
	duckdb_destroy_table_function(&function);
}

// ---------------------------------------------------------------------------
// SQL macro registration: user-facing wrappers that inject config
// ---------------------------------------------------------------------------

//! Helper: try to register a SQL macro via duckdb_query. Ignores errors on
//! individual macros so the extension still loads if a macro fails.
static void TryRegisterMacro(duckdb_connection connection, const char *sql) {
	duckdb_result result;
	duckdb_query(connection, sql, &result);
	duckdb_destroy_result(&result);
}

void RegisterHttpMacros(duckdb_connection connection) {
	// SQL macros are defined in sql/*.sql files, embedded at build time
	// by cmake/embed_sql.py into sql_resources.hpp.  Each file becomes
	// a vector of SQL statements (split on semicolons, comments stripped).
	// Registration order matters: http_config first (provides _bh_http_config),
	// then verbs (depend on _bh_http_config), then helpers (depend on both).
	const std::vector<std::vector<std::string> *> macro_groups = {
		const_cast<std::vector<std::string> *>(&sql::http_config),
		const_cast<std::vector<std::string> *>(&sql::http_verbs),
		const_cast<std::vector<std::string> *>(&sql::http_config_helpers),
		const_cast<std::vector<std::string> *>(&sql::llm_complete),
		const_cast<std::vector<std::string> *>(&sql::llm_adapt),
	};
	for (auto *group : macro_groups) {
		for (auto &stmt : *group) {
			TryRegisterMacro(connection, stmt.c_str());
		}
	}
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

void RegisterHttpFunctions(duckdb_connection connection) {
	// Raw C scalar functions (prefixed with _ — not intended for direct use)
	RegisterHttpRawRequestScalar(connection);

	// Diagnostics
	RegisterRateLimitStatsFunction(connection);

	// SQL macros: user-facing wrappers that inject config
	RegisterHttpMacros(connection);
}

} // namespace blobhttp
