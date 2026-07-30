/*
 * blobhttp_core.cpp — the shared implementation behind include/blobhttp.h.
 *
 * Everything here previously existed twice: once in
 * duckdb_ext/src/bhttp_functions.cpp and once in
 * sqlite_ext/src/bhttp_sqlite.cpp. ResolveConfig, ResolveVaultSecrets,
 * AcquireRateLimit and RecordResponseStats each had two call sites in two
 * files, and the two copies had already drifted (only DuckDB batched through
 * the multi interface; only DuckDB surfaced the raw body as a BLOB).
 *
 * The process-wide state — rate-limiter registry, global limiter, Vault secret
 * cache — lives here now, so there is one of each rather than one per host.
 */

#include "blobhttp.h"

#include "blobhttp_internal.hpp"

#include "http_config.hpp"
#include <spnego_token.hpp>
#include "rate_limiter.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

namespace blobhttp {

// ─────────────────────────────────────────────────────────────────────
// Errors
//
// Thread-local because DuckDB calls scalar functions from several threads and
// a shared buffer would let one thread's failure be reported as another's.
// ─────────────────────────────────────────────────────────────────────

thread_local std::string g_errmsg;

void SetError(const std::string &msg) { g_errmsg = msg; }
void ClearError() { g_errmsg.clear(); }

/// Duplicate a std::string into a malloc'd buffer for the caller to bh_free.
/// Returns NULL (and sets the error) if the allocation fails, so a caller that
/// only checks for NULL still gets a usable message.
char *DupString(const std::string &s) {
    char *out = static_cast<char *>(std::malloc(s.size() + 1));
    if (!out) {
        SetError("out of memory");
        return nullptr;
    }
    std::memcpy(out, s.data(), s.size());
    out[s.size()] = '\0';
    return out;
}

// ─────────────────────────────────────────────────────────────────────
// Process-wide limiter state
// ─────────────────────────────────────────────────────────────────────

RateLimiterRegistry &Registry() {
    static RateLimiterRegistry registry(200);
    return registry;
}

std::mutex g_global_limiter_mutex;
std::unique_ptr<GCRARateLimiter> g_global_limiter;
std::string g_global_limiter_spec;

/// Get or re-create the global limiter. NULL when no global limit is set.
GCRARateLimiter *GlobalLimiter(const std::string &spec, double burst) {
    if (spec.empty()) return nullptr;
    std::lock_guard<std::mutex> lock(g_global_limiter_mutex);
    if (!g_global_limiter || g_global_limiter_spec != spec) {
        g_global_limiter = std::make_unique<GCRARateLimiter>(ParseRateLimit(spec), burst, spec);
        g_global_limiter_spec = spec;
    }
    return g_global_limiter.get();
}

GCRARateLimiter *GlobalLimiterSnapshot() {
    std::lock_guard<std::mutex> lock(g_global_limiter_mutex);
    return g_global_limiter.get();
}

// ─────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────

/// Hostname from a URL, for limiter keying and session pooling.

// ─────────────────────────────────────────────────────────────────────
// Shared connection cache
//
// libcurl keeps its connection cache inside the easy handle, and a fresh
// cpr::Session is a fresh easy handle — so a session per request means a new
// TCP connection, and over HTTPS a new TLS handshake, every single time.
// Measured before this existed: ten requests to one host opened ten
// connections.
//
// Both adapters used to declare an LRUPool<std::string, cpr::Session> for
// this, and neither ever called it; the pools were dead code, so there was
// never any reuse to lose.
//
// A CURLSH share handle is the right mechanism rather than pooling the
// sessions themselves: sessions stay single-use (so no per-request state can
// leak from one request into the next) while the connections, DNS results and
// TLS sessions behind them are shared. It is also safe with the multi
// interface, where pooled sessions would not be — the same easy handle cannot
// be attached to two concurrent transfers.
//
// The lock callbacks are required because DuckDB calls scalar functions from
// several threads; without them curl documents the share as unsafe.
// ─────────────────────────────────────────────────────────────────────

std::mutex &ShareMutex(curl_lock_data data) {
    static std::mutex locks[CURL_LOCK_DATA_LAST];
    return locks[data < CURL_LOCK_DATA_LAST ? data : 0];
}

void ShareLock(CURL *, curl_lock_data data, curl_lock_access, void *) {
    ShareMutex(data).lock();
}

void ShareUnlock(CURL *, curl_lock_data data, void *) {
    ShareMutex(data).unlock();
}

CURLSH *ConnectionShare() {
    static CURLSH *share = [] {
        CURLSH *sh = curl_share_init();
        if (!sh) return static_cast<CURLSH *>(nullptr);
        curl_share_setopt(sh, CURLSHOPT_LOCKFUNC, ShareLock);
        curl_share_setopt(sh, CURLSHOPT_UNLOCKFUNC, ShareUnlock);
        curl_share_setopt(sh, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT);
        curl_share_setopt(sh, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
        curl_share_setopt(sh, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
        return sh;
    }();
    return share;
}

/// Attach the process-wide connection cache to one session.
void ShareConnections(cpr::Session &session) {
    if (CURLSH *sh = ConnectionShare()) {
        if (auto holder = session.GetCurlHolder()) {
            curl_easy_setopt(holder->handle, CURLOPT_SHARE, sh);
        }
    }
}

std::string ExtractHost(const std::string &url) {
    auto pos = url.find("://");
    if (pos == std::string::npos) return url;
    auto start = pos + 3;
    auto end = url.find_first_of(":/?#", start);
    if (end == std::string::npos) end = url.length();
    return url.substr(start, end - start);
}

/// Flatten a JSON object into key/value pairs. Non-string values are dumped,
/// so {"n": 1} yields ("n", "1") — which is what a header or query parameter
/// needs. Malformed input yields no pairs rather than an error, matching what
/// both adapters did.
Pairs ParseJsonObject(const char *json) {
    Pairs out;
    if (!json || !*json) return out;
    try {
        auto j = nlohmann::json::parse(json);
        if (j.is_object()) {
            for (auto &[k, v] : j.items()) {
                out.emplace_back(k, v.is_string() ? v.get<std::string>() : v.dump());
            }
        }
    } catch (...) {
        // Malformed — treated as absent, as before.
    }
    return out;
}

std::string FormatEpoch(int64_t epoch) {
    time_t t = static_cast<time_t>(epoch);
    struct tm tm_buf;
#ifdef _WIN32
    gmtime_s(&tm_buf, &t);
#else
    gmtime_r(&t, &tm_buf);
#endif
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm_buf);
    return std::string(buf) + " (" + std::to_string(epoch) + ")";
}

// ─────────────────────────────────────────────────────────────────────
// One queued request, and one result
// ─────────────────────────────────────────────────────────────────────

struct Pending {
    std::string method;
    std::string url;
    Pairs headers;
    Pairs params;
    std::string body;
    std::string content_type;
    int timeout_override = -1;
    int verify_ssl_override = -1;

    HttpConfig config;
    std::string host;
    std::shared_ptr<cpr::Session> session;
    cpr::Header req_headers;
    cpr::MultiPerform::HttpMethod cpr_method = cpr::MultiPerform::HttpMethod::GET_REQUEST;
};

struct Result {
    std::string request_url;
    std::string request_method;
    Pairs request_headers;
    std::string request_body;
    int status_code = 0;
    std::string status_line;
    Pairs response_headers;
    std::string body;
    std::string response_url;
    double elapsed = 0.0;
    int redirect_count = 0;
};

cpr::MultiPerform::HttpMethod ToCprMethod(const std::string &method) {
    if (method == "GET") return cpr::MultiPerform::HttpMethod::GET_REQUEST;
    if (method == "POST") return cpr::MultiPerform::HttpMethod::POST_REQUEST;
    if (method == "PUT") return cpr::MultiPerform::HttpMethod::PUT_REQUEST;
    if (method == "DELETE") return cpr::MultiPerform::HttpMethod::DELETE_REQUEST;
    if (method == "PATCH") return cpr::MultiPerform::HttpMethod::PATCH_REQUEST;
    if (method == "HEAD") return cpr::MultiPerform::HttpMethod::HEAD_REQUEST;
    if (method == "OPTIONS") return cpr::MultiPerform::HttpMethod::OPTIONS_REQUEST;
    throw std::runtime_error("Unsupported HTTP method: " + method);
}

/// Build a configured session. Throws on an expired bearer token or a failed
/// Negotiate token — both are propagated rather than swallowed, because a
/// silent failure here surfaces as a baffling 401 with no hint that auth was
/// even attempted.
std::pair<std::shared_ptr<cpr::Session>, cpr::Header>
BuildSession(const Pending &req, const HttpConfig &config) {
    int timeout = (req.timeout_override >= 0) ? req.timeout_override : config.timeout;

    auto session = std::make_shared<cpr::Session>();
    ShareConnections(*session);
    session->SetUrl(cpr::Url{req.url});
    session->SetTimeout(cpr::Timeout{timeout * 1000});

    cpr::Header headers;
    for (auto &[k, v] : req.headers) headers[k] = v;

    const bool has_auth = headers.find("Authorization") != headers.end();
    if (config.auth_type == "negotiate" && !has_auth) {
        headers["Authorization"] = "Negotiate " + spnego::GenerateTokenForUrl(req.url).token;
    } else if (config.auth_type == "bearer" && !config.bearer_token.empty() && !has_auth) {
        if (config.bearer_token_expires_at > 0) {
            auto now = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
            if (now >= config.bearer_token_expires_at) {
                throw std::runtime_error(
                    "Bearer token for " + ExtractHost(req.url) + " expired at " +
                    FormatEpoch(config.bearer_token_expires_at) +
                    " (current time: " + FormatEpoch(now) +
                    "). Refresh the token via your application and update bh_http_config.");
            }
        }
        headers["Authorization"] = "Bearer " + config.bearer_token;
    }

    auto content_type = req.content_type;
    if (!req.body.empty() && content_type.empty()) content_type = "application/json";
    if (!content_type.empty()) headers["Content-Type"] = content_type;

    session->SetHeader(headers);

    bool verify_ssl = (req.verify_ssl_override >= 0) ? (req.verify_ssl_override == 1)
                                                     : config.verify_ssl;
    if (!verify_ssl) session->SetVerifySsl(cpr::VerifySsl{false});

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
    if (!req.params.empty()) {
        cpr::Parameters params;
        for (auto &[k, v] : req.params) params.Add(cpr::Parameter{k, v});
        session->SetParameters(params);
    }
    if (!req.body.empty()) session->SetBody(cpr::Body{req.body});

    return {session, headers};
}

Result ResponseToResult(const cpr::Response &response, const Pending &req,
                        const cpr::Header &req_headers) {
    Result r;
    r.request_url = req.url;
    r.request_method = req.method;
    for (auto &[k, v] : req_headers) r.request_headers.emplace_back(k, v);
    r.request_body = req.body;

    r.status_code = static_cast<int>(response.status_code);
    r.status_line = response.status_line;
    for (auto &[k, v] : response.header) {
        std::string lower = k;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        r.response_headers.emplace_back(lower, v);
    }
    r.body = response.text;
    r.response_url = response.url.str();
    r.elapsed = response.elapsed;
    r.redirect_count = static_cast<int>(response.redirect_count);
    return r;
}

/// Take a token, sleeping as required. Bounded at 50 attempts so a
/// misconfigured limiter stalls a request rather than hanging it forever.
void AcquireRateLimit(GCRARateLimiter *limiter) {
    if (!limiter) return;
    int max_retries = 50;
    bool paced = false;
    double total = 0.0;
    while (!limiter->TryAcquire() && max_retries-- > 0) {
        double wait = limiter->WaitTime();
        if (wait > 0.0) {
            paced = true;
            total += wait;
            std::this_thread::sleep_for(std::chrono::duration<double>(wait));
        }
    }
    limiter->RecordRequest();
    if (paced) limiter->RecordPacing(total);
}

/// Record response facts, and feed 429 Retry-After back into the limiter so
/// the server's own pacing signal shapes subsequent requests.
void RecordResponseStats(const cpr::Response &response, const std::string &host) {
    auto *limiter = Registry().GetOrCreate(host);
    if (!limiter) return;

    limiter->RecordResponse(response.elapsed, response.text.size(),
                            static_cast<int>(response.status_code));
    if (response.status_code == 429) {
        double retry_after = 1.0;
        auto it = response.header.find("Retry-After");
        if (it != response.header.end()) {
            try {
                retry_after = std::stod(it->second);
            } catch (...) {
            }
        }
        limiter->RecordThrottle(retry_after);
    }
    if (auto *global = GlobalLimiterSnapshot()) {
        global->RecordResponse(response.elapsed, response.text.size(),
                               static_cast<int>(response.status_code));
    }
}


/// Whether a byte range is well-formed UTF-8.
///
/// nlohmann refuses to serialise a string value that is not, and a JSON string
/// genuinely cannot hold arbitrary bytes, so this decides between putting the
/// body in as text and base64-encoding it.
bool IsValidUtf8(const std::string &s) {
    size_t i = 0;
    while (i < s.size()) {
        unsigned char c = static_cast<unsigned char>(s[i]);
        size_t extra;
        unsigned int cp;
        if (c < 0x80) { i++; continue; }
        else if ((c & 0xE0) == 0xC0) { extra = 1; cp = c & 0x1F; }
        else if ((c & 0xF0) == 0xE0) { extra = 2; cp = c & 0x0F; }
        else if ((c & 0xF8) == 0xF0) { extra = 3; cp = c & 0x07; }
        else return false;

        if (i + extra >= s.size()) return false;
        for (size_t k = 1; k <= extra; k++) {
            unsigned char cc = static_cast<unsigned char>(s[i + k]);
            if ((cc & 0xC0) != 0x80) return false;
            cp = (cp << 6) | (cc & 0x3F);
        }
        // Reject overlong encodings, surrogates and out-of-range code points —
        // all of which some decoders accept and others reject, which is
        // exactly the ambiguity worth not shipping.
        if (extra == 1 && cp < 0x80) return false;
        if (extra == 2 && cp < 0x800) return false;
        if (extra == 3 && cp < 0x10000) return false;
        if (cp > 0x10FFFF) return false;
        if (cp >= 0xD800 && cp <= 0xDFFF) return false;
        i += extra + 1;
    }
    return true;
}

std::string Base64Encode(const std::string &in) {
    static const char *T = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((in.size() + 2) / 3) * 4);
    size_t i = 0;
    while (i + 2 < in.size()) {
        unsigned v = (static_cast<unsigned char>(in[i]) << 16) |
                     (static_cast<unsigned char>(in[i + 1]) << 8) |
                     static_cast<unsigned char>(in[i + 2]);
        out += T[(v >> 18) & 63];
        out += T[(v >> 12) & 63];
        out += T[(v >> 6) & 63];
        out += T[v & 63];
        i += 3;
    }
    if (i < in.size()) {
        unsigned v = static_cast<unsigned char>(in[i]) << 16;
        bool two = (i + 1 < in.size());
        if (two) v |= static_cast<unsigned char>(in[i + 1]) << 8;
        out += T[(v >> 18) & 63];
        out += T[(v >> 12) & 63];
        out += two ? T[(v >> 6) & 63] : '=';
        out += '=';
    }
    return out;
}

nlohmann::json PairsToJson(const Pairs &pairs) {
    nlohmann::json obj = nlohmann::json::object();
    for (auto &[k, v] : pairs) obj[k] = v;
    return obj;
}

} // namespace blobhttp

// ─────────────────────────────────────────────────────────────────────
// The batch handle
// ─────────────────────────────────────────────────────────────────────

struct bh_batch {
    blobhttp::Pairs config_entries;
    std::vector<blobhttp::Pending> pending;
    std::vector<blobhttp::Result> results;
};

using namespace blobhttp;

extern "C" {

void bh_free(void *p) { std::free(p); }

const char *bh_errmsg(void) { return g_errmsg.c_str(); }

bh_batch *bh_batch_new(const char *config_json) {
    ClearError();
    try {
        auto *b = new bh_batch();
        b->config_entries = ParseJsonObject(config_json);
        return b;
    } catch (const std::exception &e) {
        SetError(e.what());
        return nullptr;
    }
}

void bh_batch_free(bh_batch *b) { delete b; }

size_t bh_batch_count(const bh_batch *b) { return b ? b->results.size() : 0; }

int bh_batch_add(bh_batch *b, const char *method, const char *url,
                 const char *headers_json, const char *params_json,
                 const void *body, size_t body_len, const char *content_type,
                 int timeout_override, int verify_ssl_override) {
    ClearError();
    if (!b) {
        SetError("null batch");
        return -1;
    }
    if (!method || !*method || !url || !*url) {
        SetError("method and url are required");
        return -1;
    }
    try {
        Pending req;
        req.method = method;
        req.url = url;
        req.headers = ParseJsonObject(headers_json);
        req.params = ParseJsonObject(params_json);
        if (body && body_len) req.body.assign(static_cast<const char *>(body), body_len);
        if (content_type) req.content_type = content_type;
        req.timeout_override = timeout_override;
        req.verify_ssl_override = verify_ssl_override;
        b->pending.push_back(std::move(req));
        return 0;
    } catch (const std::exception &e) {
        SetError(e.what());
        return -1;
    }
}

int bh_batch_perform(bh_batch *b) {
    ClearError();
    if (!b) {
        SetError("null batch");
        return -1;
    }
    try {
        // Resolve configuration and build every session before any request
        // goes out, so a config error fails the whole batch rather than
        // leaving some requests sent and others not.
        int max_concurrent = 10;
        for (size_t i = 0; i < b->pending.size(); i++) {
            auto &req = b->pending[i];
            req.config = ResolveConfig(req.url, b->config_entries);
            ResolveVaultSecrets(req.config, req.params);
            req.host = ExtractHost(req.url);
            req.cpr_method = ToCprMethod(req.method);
            std::tie(req.session, req.req_headers) = BuildSession(req, req.config);
            if (i == 0) max_concurrent = req.config.max_concurrent;
        }
        if (max_concurrent < 1) max_concurrent = 1;

        b->results.assign(b->pending.size(), Result{});

        // Sub-batches of max_concurrent go out together through libcurl's
        // multi interface. Rate limits are taken for every request in the
        // sub-batch first, so pacing applies per request and not per batch.
        for (size_t start = 0; start < b->pending.size(); start += max_concurrent) {
            size_t end = std::min(start + static_cast<size_t>(max_concurrent), b->pending.size());

            for (size_t i = start; i < end; i++) {
                AcquireRateLimit(GlobalLimiter(b->pending[i].config.global_rate_limit_spec,
                                               b->pending[i].config.global_burst));
                AcquireRateLimit(Registry().GetOrCreate(b->pending[i].host,
                                                        b->pending[i].config.rate_limit_spec,
                                                        b->pending[i].config.burst));
            }

            cpr::MultiPerform multi;
            for (size_t i = start; i < end; i++) {
                multi.AddSession(b->pending[i].session, b->pending[i].cpr_method);
            }
            auto responses = multi.Perform();

            for (size_t i = start; i < end; i++) {
                auto &response = responses[i - start];
                RecordResponseStats(response, b->pending[i].host);
                b->results[i] = ResponseToResult(response, b->pending[i], b->pending[i].req_headers);
            }
        }
        return 0;
    } catch (const std::exception &e) {
        SetError(e.what());
        return -1;
    }
}

// ── Result accessors ──────────────────────────────────────────────────
//
// Out of range yields 0/NULL rather than undefined behaviour: an adapter bug
// should produce a NULL column, not a crash inside the host process.

namespace {
const Result *At(const bh_batch *b, size_t i) {
    if (!b || i >= b->results.size()) return nullptr;
    return &b->results[i];
}

const Pairs *HeadersOf(const bh_batch *b, size_t i, int which) {
    const Result *r = At(b, i);
    if (!r) return nullptr;
    return which == BH_REQUEST_HEADERS ? &r->request_headers : &r->response_headers;
}

const char *Borrow(const std::string &s, size_t *len) {
    if (len) *len = s.size();
    return s.data();
}
} // namespace

int bh_result_status(const bh_batch *b, size_t i) {
    const Result *r = At(b, i);
    return r ? r->status_code : 0;
}

double bh_result_elapsed(const bh_batch *b, size_t i) {
    const Result *r = At(b, i);
    return r ? r->elapsed : 0.0;
}

int bh_result_redirect_count(const bh_batch *b, size_t i) {
    const Result *r = At(b, i);
    return r ? r->redirect_count : 0;
}

#define BH_BORROW_FIELD(fn, field)                                   \
    const char *fn(const bh_batch *b, size_t i, size_t *len) {       \
        const Result *r = At(b, i);                                  \
        if (!r) {                                                    \
            if (len) *len = 0;                                       \
            return nullptr;                                          \
        }                                                            \
        return Borrow(r->field, len);                                \
    }

BH_BORROW_FIELD(bh_result_request_url, request_url)
BH_BORROW_FIELD(bh_result_request_method, request_method)
BH_BORROW_FIELD(bh_result_status_line, status_line)
BH_BORROW_FIELD(bh_result_response_url, response_url)
#undef BH_BORROW_FIELD

const void *bh_result_request_body(const bh_batch *b, size_t i, size_t *len) {
    const Result *r = At(b, i);
    if (!r) {
        if (len) *len = 0;
        return nullptr;
    }
    return Borrow(r->request_body, len);
}

const void *bh_result_body(const bh_batch *b, size_t i, size_t *len) {
    const Result *r = At(b, i);
    if (!r) {
        if (len) *len = 0;
        return nullptr;
    }
    return Borrow(r->body, len);
}

size_t bh_result_header_count(const bh_batch *b, size_t i, int which) {
    const Pairs *h = HeadersOf(b, i, which);
    return h ? h->size() : 0;
}

const char *bh_result_header_name(const bh_batch *b, size_t i, int which, size_t k, size_t *len) {
    const Pairs *h = HeadersOf(b, i, which);
    if (!h || k >= h->size()) {
        if (len) *len = 0;
        return nullptr;
    }
    return Borrow((*h)[k].first, len);
}

const char *bh_result_header_value(const bh_batch *b, size_t i, int which, size_t k, size_t *len) {
    const Pairs *h = HeadersOf(b, i, which);
    if (!h || k >= h->size()) {
        if (len) *len = 0;
        return nullptr;
    }
    return Borrow((*h)[k].second, len);
}

char *bh_result_json(const bh_batch *b, size_t i) {
    ClearError();
    const Result *r = At(b, i);
    if (!r) {
        SetError("result index out of range");
        return nullptr;
    }
    try {
        nlohmann::json j;
        j["request_url"] = r->request_url;
        j["request_method"] = r->request_method;
        j["request_headers"] = PairsToJson(r->request_headers);
        j["request_body"] = r->request_body;
        j["response_status_code"] = r->status_code;
        j["response_status"] = r->status_line;
        j["response_headers"] = PairsToJson(r->response_headers);

        // A JSON string cannot hold arbitrary bytes. A UTF-8 body goes in as
        // text, byte-identical to what this always produced; anything else is
        // base64 with an explicit marker, rather than the hard failure both
        // hosts used to give ("invalid UTF-8 byte at index N") for any binary
        // response. Callers wanting the bytes unconditionally should use
        // bh_result_body, which has no such constraint.
        if (IsValidUtf8(r->body)) {
            j["response_body"] = r->body;
        } else {
            j["response_body"] = Base64Encode(r->body);
            j["response_body_encoding"] = "base64";
        }
        j["response_url"] = r->response_url;
        j["elapsed"] = r->elapsed;
        j["redirect_count"] = r->redirect_count;
        return DupString(j.dump());
    } catch (const std::exception &e) {
        SetError(e.what());
        return nullptr;
    }
}

// ── Diagnostics and auth ──────────────────────────────────────────────

char *bh_rate_limit_stats_json(void) {
    ClearError();
    try {
        auto snapshot = [](const std::string &host, GCRARateLimiter &l) -> nlohmann::json {
            return {
                {"host", host},
                {"rate_limit", l.RateSpec()},
                {"rate_rps", l.Rate()},
                {"burst", l.Burst()},
                {"requests", l.Requests()},
                {"paced", l.Paced()},
                {"total_wait_seconds", l.TotalWaitSeconds()},
                {"throttled_429", l.Throttled429()},
                {"backlog_seconds", l.BacklogSeconds()},
                {"total_responses", l.TotalResponses()},
                {"total_response_bytes", l.TotalResponseBytes()},
                {"total_elapsed", l.TotalElapsed()},
                {"min_elapsed", l.MinElapsed()},
                {"max_elapsed", l.MaxElapsed()},
                {"errors", l.Errors()},
            };
        };
        nlohmann::json arr = nlohmann::json::array();
        if (auto *global = GlobalLimiterSnapshot()) arr.push_back(snapshot("(global)", *global));
        Registry().ForEach([&](const std::string &host, GCRARateLimiter &l) {
            arr.push_back(snapshot(host, l));
        });
        return DupString(arr.dump());
    } catch (const std::exception &e) {
        SetError(e.what());
        return nullptr;
    }
}

char *bh_negotiate_auth_header(const char *url) {
    ClearError();
    if (!url) {
        SetError("url is required");
        return nullptr;
    }
    try {
        return DupString("Negotiate " + spnego::GenerateTokenForUrl(url).token);
    } catch (const std::exception &e) {
        SetError(e.what());
        return nullptr;
    }
}

int bh_negotiate_available(void) {
    ClearError();
    try {
        return spnego::IsAvailable() ? 1 : 0;
    } catch (const std::exception &e) {
        SetError(e.what());
        return 0;
    }
}

char *bh_negotiate_auth_header_json(const char *url) {
    ClearError();
    if (!url) {
        SetError("url is required");
        return nullptr;
    }
    try {
        auto result = spnego::GenerateTokenForUrl(url);
        // Field order and names match what the DuckDB scalar emitted before
        // the extraction — this JSON is user-visible via
        // bh_negotiate_auth_header_json().
        nlohmann::json j;
        j["token"] = result.token;
        j["header"] = "Negotiate " + result.token;
        j["url"] = result.url;
        j["hostname"] = result.hostname;
        j["spn"] = result.spn;
        j["provider"] = result.provider;
        j["library"] = result.library;
        return DupString(j.dump());
    } catch (const std::exception &e) {
        SetError(e.what());
        return nullptr;
    }
}

} // extern "C"
