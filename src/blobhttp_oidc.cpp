/*
 * blobhttp_oidc.cpp — Kerberos ticket to JWT, via an OIDC provider.
 *
 * Three round-trips, none of them novel:
 *
 *   1. GET  <issuer>/.well-known/openid-configuration   -> the endpoints
 *   2. GET  <authorization_endpoint>?response_type=code&…
 *           with `Authorization: Negotiate <spnego>`, redirects NOT followed
 *           -> a 302 whose Location carries ?code=…
 *   3. POST <token_endpoint>  grant_type=authorization_code&code=…
 *           -> {"access_token": "<jwt>", …}
 *
 * The SPNEGO token in step 2 comes from the spnego-token atom, which mints it
 * from the ambient Kerberos credential and touches no socket. So the only
 * secret in play is the one already in the user's ticket cache — nothing is
 * stored in config, and nothing is prompted for.
 *
 * This exists in blobhttp because blobsso — which does the same exchange for
 * httpfs — is a DuckDB extension, and SQLite and Python cannot reach it. The
 * two implementations differ in their transport (blobsso borrows DuckDB's
 * HTTPUtil; this uses blobhttp's own core) and agree on the protocol. If that
 * protocol logic is ever worth sharing, the shape to extract will be obvious
 * from having both in front of us rather than guessed from one.
 */

#include "blobhttp.h"
#include "blobhttp_internal.hpp"

#include "http_config.hpp"

#include <string>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include <spnego_token.hpp>

namespace blobhttp {
namespace {

/// Percent-encode for application/x-www-form-urlencoded.
std::string UrlEncode(const std::string &s) {
    static const char *hex = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            out += static_cast<char>(c);
        } else {
            out += '%';
            out += hex[c >> 4];
            out += hex[c & 15];
        }
    }
    return out;
}

/// Apply the TLS settings from the scoped config to a session.
///
/// An OIDC issuer on a private CA is the normal case, not the exception —
/// keycloak.phrrngtn.arpa is signed by a demo CA — so `ca_bundle` has to reach
/// these requests, not just the ones the user makes explicitly.
void ApplyTls(cpr::Session &session, const HttpConfig &config) {
    if (!config.verify_ssl) session.SetVerifySsl(cpr::VerifySsl{false});
    if (!config.ca_bundle.empty() || !config.client_cert.empty() || !config.client_key.empty()) {
        cpr::SslOptions ssl;
        if (!config.ca_bundle.empty()) ssl.SetOption(cpr::ssl::CaInfo{config.ca_bundle});
        if (!config.client_cert.empty()) ssl.SetOption(cpr::ssl::CertFile{config.client_cert});
        if (!config.client_key.empty()) ssl.SetOption(cpr::ssl::KeyFile{config.client_key});
        session.SetSslOptions(ssl);
    }
    if (!config.proxy.empty()) {
        session.SetProxies(cpr::Proxies{{"http", config.proxy}, {"https", config.proxy}});
    }
}

/// Case-insensitive header lookup. cpr's Header is already case-insensitive,
/// but being explicit costs nothing and documents that Location's spelling is
/// not guaranteed.
std::string HeaderValue(const cpr::Header &h, const std::string &name) {
    auto it = h.find(name);
    return it == h.end() ? std::string{} : it->second;
}

struct Endpoints {
    std::string authorization;
    std::string token;
};

Endpoints Discover(const std::string &issuer, const HttpConfig &config) {
    std::string url = issuer;
    if (!url.empty() && url.back() == '/') url.pop_back();
    url += "/.well-known/openid-configuration";

    cpr::Session s;
    ShareConnections(s);
    s.SetUrl(cpr::Url{url});
    s.SetTimeout(cpr::Timeout{config.timeout * 1000});
    ApplyTls(s, config);

    auto r = s.Get();
    if (r.status_code != 200) {
        throw std::runtime_error("OIDC discovery failed (HTTP " +
                                 std::to_string(r.status_code) + ") at " + url +
                                 ": " + r.text.substr(0, 200));
    }
    auto j = nlohmann::json::parse(r.text);
    Endpoints ep;
    ep.authorization = j.value("authorization_endpoint", "");
    ep.token = j.value("token_endpoint", "");
    if (ep.authorization.empty() || ep.token.empty()) {
        throw std::runtime_error("OIDC discovery at " + url +
                                 " returned no authorization_endpoint/token_endpoint");
    }
    return ep;
}

/// Present a SPNEGO token to the authorization endpoint and pull the
/// authorization code out of the redirect.
///
/// Redirects are deliberately NOT followed: the code is in the Location
/// header, and following it would send it to redirect_uri — typically a
/// localhost address with nothing listening.
std::string AuthorizeWithSpnego(const Endpoints &ep, const std::string &client_id,
                                const std::string &redirect_uri, const std::string &scope,
                                const HttpConfig &config) {
    std::string url = ep.authorization + "?client_id=" + UrlEncode(client_id) +
                      "&response_type=code&scope=" + UrlEncode(scope) +
                      "&redirect_uri=" + UrlEncode(redirect_uri) + "&state=bhttp";

    // allow_insecure: an issuer reached over an already-encrypted transport
    // (WireGuard, Tailscale) is legitimate, and the atom refuses plain http://
    // by default. Follow the scope's verify_ssl as the signal for that.
    auto spnego_token = spnego::GenerateTokenForUrl(ep.authorization, !config.verify_ssl).token;

    cpr::Session s;
    ShareConnections(s);
    s.SetUrl(cpr::Url{url});
    s.SetTimeout(cpr::Timeout{config.timeout * 1000});
    s.SetHeader(cpr::Header{{"Authorization", "Negotiate " + spnego_token}});
    s.SetRedirect(cpr::Redirect{0L, false, false, cpr::PostRedirectFlags::POST_ALL});
    ApplyTls(s, config);

    auto r = s.Get();
    auto location = HeaderValue(r.header, "Location");
    if (location.empty()) {
        throw std::runtime_error(
            "OIDC authorize did not redirect (HTTP " + std::to_string(r.status_code) +
            "). The Kerberos principal is probably not mapped in this realm, or the "
            "ticket has expired — check `klist`. Body: " + r.text.substr(0, 200));
    }
    auto pos = location.find("code=");
    if (pos == std::string::npos) {
        throw std::runtime_error("OIDC authorize redirect carried no code: " +
                                 location.substr(0, 200));
    }
    auto code = location.substr(pos + 5);
    auto amp = code.find('&');
    if (amp != std::string::npos) code = code.substr(0, amp);
    return code;
}

nlohmann::json ExchangeCode(const Endpoints &ep, const std::string &code,
                            const std::string &client_id, const std::string &client_secret,
                            const std::string &redirect_uri, const HttpConfig &config) {
    std::string body = "grant_type=authorization_code&code=" + UrlEncode(code) +
                       "&client_id=" + UrlEncode(client_id) +
                       "&redirect_uri=" + UrlEncode(redirect_uri);
    if (!client_secret.empty()) body += "&client_secret=" + UrlEncode(client_secret);

    cpr::Session s;
    ShareConnections(s);
    s.SetUrl(cpr::Url{ep.token});
    s.SetTimeout(cpr::Timeout{config.timeout * 1000});
    s.SetHeader(cpr::Header{{"Content-Type", "application/x-www-form-urlencoded"}});
    s.SetBody(cpr::Body{body});
    ApplyTls(s, config);

    auto r = s.Post();
    if (r.status_code != 200) {
        throw std::runtime_error("OIDC token exchange failed (HTTP " +
                                 std::to_string(r.status_code) + "): " + r.text.substr(0, 300));
    }
    auto j = nlohmann::json::parse(r.text);
    if (!j.contains("access_token")) {
        throw std::runtime_error("OIDC token response carried no access_token: " +
                                 r.text.substr(0, 200));
    }
    return j;
}

} // namespace

nlohmann::json SsoJwt(const nlohmann::json &req) {
    std::string issuer = req.value("issuer", "");
    if (issuer.empty()) throw std::runtime_error("issuer is required");
    std::string client_id = req.value("client_id", "");
    if (client_id.empty()) throw std::runtime_error("client_id is required");

    // The scope's own config supplies CA bundle, proxy and timeouts, so an
    // issuer behind a private CA or a proxy needs no separate plumbing.
    Pairs entries;
    if (req.contains("http_config") && req["http_config"].is_object()) {
        for (auto &[k, v] : req["http_config"].items()) {
            entries.emplace_back(k, v.is_string() ? v.get<std::string>() : v.dump());
        }
    }
    auto config = ResolveConfig(issuer, entries);

    auto ep = Discover(issuer, config);
    auto redirect_uri = req.value("redirect_uri", std::string{"http://localhost/cb"});
    auto scope = req.value("scope", std::string{"openid"});
    auto code = AuthorizeWithSpnego(ep, client_id, redirect_uri, scope, config);
    return ExchangeCode(ep, code, client_id, req.value("client_secret", ""), redirect_uri, config);
}

} // namespace blobhttp

using namespace blobhttp;

extern "C" {

char *bh_sso_jwt(const char *request_json) {
    ClearError();
    if (!request_json) {
        SetError("request_json is required");
        return nullptr;
    }
    try {
        return DupString(SsoJwt(nlohmann::json::parse(request_json)).dump());
    } catch (const std::exception &e) {
        SetError(e.what());
        return nullptr;
    }
}

} // extern "C"
