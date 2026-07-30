/*
 * blobhttp.h — C ABI for the blobhttp core.
 *
 * The DuckDB extension, the SQLite extension and the Python package all bind
 * to this header. Before it existed each host carried its own copy of the same
 * logic — config resolution, Vault lookup, rate limiting, session building,
 * response shaping — which is why ResolveConfig, ResolveVaultSecrets,
 * AcquireRateLimit and RecordResponseStats each appeared twice.
 *
 * Everything above this line is C++ (cpr over libcurl, nlohmann/json, and
 * jsoncons for JSON Schema and JMESPath). That island is permanent: those are
 * class and template libraries, and a C header is the only thing all three
 * hosts can consume.
 *
 * MEMORY. Functions returning `char *` return a malloc'd, NUL-terminated
 * string the caller frees with bh_free(). Functions returning `const char *`
 * or `const void *` return a BORROWED pointer into the batch, valid until
 * bh_batch_free() — nothing to free, and nothing to copy either, which is the
 * point when DuckDB hands over 2048 rows at a time.
 *
 * ERRORS. A NULL return (or a negative int) means failure; bh_errmsg() gives
 * the reason. It is thread-local and only valid until the next blobhttp call
 * on that thread, so read it immediately.
 *
 * THREADING. Separate bh_batch handles are independent and may be used
 * concurrently — DuckDB calls scalar functions from several threads. The
 * process-wide state they share (the rate-limiter registry, the Vault secret
 * cache, the session pool) is internally synchronised. A single bh_batch must
 * not be touched from two threads at once.
 */
#ifndef BLOBHTTP_H
#define BLOBHTTP_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Free a buffer returned by any bh_* function that returns `char *`. */
void bh_free(void *p);

/* Last error on this thread; "" if none. Borrowed, do not free. */
const char *bh_errmsg(void);

/* ================================================================== *
 *  Batched HTTP                                                      *
 * ================================================================== *
 *
 * Batch-oriented rather than one-request-per-call because DuckDB is
 * vectorised: a whole chunk goes out through libcurl's multi interface, in
 * sub-batches of the resolved max_concurrent, so a scan of 2048 URLs fans out
 * instead of serialising. SQLite and Python use the same API with one request
 * in the batch, which costs them nothing.
 *
 * Typical use:
 *
 *     bh_batch *b = bh_batch_new(config_json);
 *     for (each row) bh_batch_add(b, method, url, ...);
 *     if (bh_batch_perform(b) != 0) { ... bh_errmsg() ... }
 *     for (i < bh_batch_count(b)) { bh_result_status(b, i); ... }
 *     bh_batch_free(b);
 */

typedef struct bh_batch bh_batch;

/*
 * Create a batch. `config_json` is the scoped configuration object —
 * {scope_url: config_object} — as read from the bh_http_config variable.
 * Pass NULL or "{}" for none. Config is resolved per request URL by longest
 * prefix, then by domain suffix.
 */
bh_batch *bh_batch_new(const char *config_json);

/*
 * Queue one request. Returns 0 on success, -1 on failure.
 *
 * `headers_json` and `params_json` are JSON objects, or NULL. `body` may be
 * NULL with body_len 0, and may contain NUL bytes. `content_type` may be NULL.
 * `timeout_override` and `verify_ssl_override` take -1 to mean "use the
 * resolved config"; verify_ssl_override otherwise takes 0 or 1.
 *
 * Everything is copied — the caller's buffers need not outlive the call.
 */
int bh_batch_add(bh_batch *b,
                 const char *method, const char *url,
                 const char *headers_json, const char *params_json,
                 const void *body, size_t body_len,
                 const char *content_type,
                 int timeout_override, int verify_ssl_override);

/*
 * Execute every queued request. Blocks. Applies the global and per-host rate
 * limits before each sub-batch, records response stats, and feeds 429
 * Retry-After back into the limiter. Returns 0 on success, -1 on failure.
 */
int bh_batch_perform(bh_batch *b);

/* Number of results available (equals the number of successful adds). */
size_t bh_batch_count(const bh_batch *b);

void bh_batch_free(bh_batch *b);

/* ── Result accessors ─────────────────────────────────────────────── *
 *
 * All are valid only after bh_batch_perform() and only until
 * bh_batch_free(). An out-of-range index yields 0 / NULL rather than
 * undefined behaviour.
 */

#define BH_REQUEST_HEADERS  0
#define BH_RESPONSE_HEADERS 1

int    bh_result_status(const bh_batch *b, size_t i);
double bh_result_elapsed(const bh_batch *b, size_t i);
int    bh_result_redirect_count(const bh_batch *b, size_t i);

/* Borrowed, with explicit lengths: bodies are arbitrary bytes and may contain
 * NUL, so length is authoritative and the NUL terminator is a convenience. */
const char *bh_result_request_url(const bh_batch *b, size_t i, size_t *len);
const char *bh_result_request_method(const bh_batch *b, size_t i, size_t *len);
const void *bh_result_request_body(const bh_batch *b, size_t i, size_t *len);
const char *bh_result_status_line(const bh_batch *b, size_t i, size_t *len);
const void *bh_result_body(const bh_batch *b, size_t i, size_t *len);
const char *bh_result_response_url(const bh_batch *b, size_t i, size_t *len);

/* Headers by index. `which` is BH_REQUEST_HEADERS or BH_RESPONSE_HEADERS.
 * Response header names are lowercased by libcurl. Iterating these writes
 * straight into a DuckDB MAP vector with no intermediate allocation. */
size_t      bh_result_header_count(const bh_batch *b, size_t i, int which);
const char *bh_result_header_name (const bh_batch *b, size_t i, int which,
                                   size_t k, size_t *len);
const char *bh_result_header_value(const bh_batch *b, size_t i, int which,
                                   size_t k, size_t *len);

/*
 * The whole result as a JSON object — request_url, request_method,
 * request_headers, request_body, response_status_code, response_status,
 * response_headers, response_body, response_url, elapsed, redirect_count.
 *
 * For SQLite and Python, which want JSON anyway. The DuckDB adapter uses the
 * accessors above instead, to avoid encoding a response body into JSON and
 * immediately decoding it again. Malloc'd; free with bh_free.
 */
char *bh_result_json(const bh_batch *b, size_t i);

/* ================================================================== *
 *  LLM completion                                                    *
 * ================================================================== *
 *
 * In the core rather than the DuckDB adapter because the value is the loop —
 * continue while the model stops on length, retry while the output fails its
 * schema, feeding the validation errors back as correction. That is not
 * something to express in SQL, and there is no reason SQLite and Python
 * should go without it.
 *
 * Both take one fully-resolved JSON object and return malloc'd JSON; free with
 * bh_free, NULL on failure. The stats carried are http_requests,
 * continuations, retries, prompt_tokens, completion_tokens, total_tokens,
 * elapsed_seconds, model and finish_reason — a single logical call is often
 * several HTTP round-trips once continuations and retries are counted, and the
 * caller should be able to see that.
 *
 * The two differ in their envelope, because bh_llm_adapt's is already
 * user-visible through llm_adapt() and blobapi reads it. See each below.
 *
 * Callers that only want the text (the DuckDB _llm_complete_raw scalar, whose
 * VARCHAR return is fixed) read the content field and drop the rest; nothing
 * is lost at the ABI.
 */

/*
 * Keys: url, body (the chat-completion request object), headers,
 * http_config, output_schema, max_continuations, max_retries.
 *
 * Returns {"content": "...", "stats": {...}}.
 */
char *bh_llm_complete(const char *request_json);

/*
 * As bh_llm_complete, but taking prompt_text / model / endpoint / max_tokens
 * in place of a pre-built body, and accepting `response_jmespath` to reshape
 * the result.
 *
 * The prompt is expected already rendered — in DuckDB the llm_adapt() macro
 * looks the adapter up in the llm_adapter table and renders it with
 * blobtemplates' bt_template_render() before calling this. That table lookup
 * and templating stay in the macro layer, which is why only this half is here.
 *
 * Returns {"data": <parsed content, or the raw string>, "_meta": {...stats}} —
 * NOT the {content, stats} shape above. That is what _llm_adapt_raw has always
 * emitted and what blobapi's SQL reads, so it is frozen.
 */
char *bh_llm_adapt(const char *request_json);

/* ================================================================== *
 *  Diagnostics and auth                                              *
 * ================================================================== */

/* Per-host and global rate-limiter counters, as a JSON array. */
char *bh_rate_limit_stats_json(void);

/*
 * SPNEGO/Negotiate header for a URL, as "Negotiate <base64>".
 *
 * GSS-API is loaded with dlopen and its types declared locally, so this adds
 * no link-time dependency and no reliance on the host's libcurl having been
 * built --with-gssapi. Absent GSS-API this fails cleanly and everything else
 * keeps working. HTTPS only.
 */
char *bh_negotiate_auth_header(const char *url);

/* As above, but {"header": "...", "spn": "...", ...} for diagnostics. */
char *bh_negotiate_auth_header_json(const char *url);

#ifdef __cplusplus
}
#endif

#endif /* BLOBHTTP_H */
