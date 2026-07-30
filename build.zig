//! blobhttp — HTTP verbs and LLM completion as SQL functions, for DuckDB /
//! SQLite / Python.
//!
//! Fifth consumer of blobzig, and the messiest starting point in the family:
//! this repo had five build systems (Makefile, configure/, cmake/,
//! cmake_build/, build_sqlite/) plus a vendored copy of DuckDB's
//! extension-ci-tools and of the DuckDB C API headers. All of it is gone.
//!
//! Two things here are unlike the other repos:
//!
//! 1. **There is no core library and no C ABI.** The logic lives in
//!    duckdb_ext/src (1,562 lines of C++) and sqlite_ext/src (835), each with
//!    its own copy. Only `src/negotiate_auth.cpp` is shared. Extracting a core
//!    is a real refactor, deliberately not bundled with the build migration.
//! 2. **The transport is a system library.** cpr wraps libcurl, and libcurl is
//!    linked from the host rather than built here, so `allow_undefined` lists
//!    `curl_` — the artifact is not self-contained, and saying so is the point.
//!    That costs cross-compilation, which a system library cannot give without
//!    a sysroot.
//!
//! cpr itself is compiled from source (28 .cpp files) against that system
//! libcurl. Its CMake would otherwise build libcurl *and* zlib from source,
//! which is the large subtree this migration exists to remove.

const std = @import("std");
const blobzig = @import("blobzig");

const cxx_flags: []const []const u8 = &.{ "-std=c++17", "-Wno-deprecated-literal-operator" };

/// cpr's own sources. Enumerated rather than globbed so an upstream bump has
/// to be looked at rather than silently absorbed.
const cpr_sources: []const []const u8 = &.{
    "cpr/accept_encoding.cpp",
    "cpr/async.cpp",
    "cpr/auth.cpp",
    "cpr/bearer.cpp",
    "cpr/callback.cpp",
    "cpr/cert_info.cpp",
    "cpr/cookies.cpp",
    "cpr/cprtypes.cpp",
    "cpr/curl_container.cpp",
    "cpr/curlholder.cpp",
    "cpr/curlmultiholder.cpp",
    "cpr/error.cpp",
    "cpr/file.cpp",
    "cpr/interceptor.cpp",
    "cpr/multipart.cpp",
    "cpr/multiperform.cpp",
    "cpr/parameters.cpp",
    "cpr/payload.cpp",
    "cpr/proxies.cpp",
    "cpr/proxyauth.cpp",
    "cpr/redirect.cpp",
    "cpr/response.cpp",
    "cpr/session.cpp",
    "cpr/ssl_ctx.cpp",
    "cpr/threadpool.cpp",
    "cpr/timeout.cpp",
    "cpr/unix_socket.cpp",
    "cpr/util.cpp",
};

/// curl's whole lib tree. Every file is compiled and the CURL_DISABLE_*
/// macros in curl_config.h hollow out the ones we do not want — that is how
/// curl's own build works, so the feature set lives in the config header
/// rather than in this list.
const curl_sources: []const []const u8 = &.{
    "lib/altsvc.c",
    "lib/amigaos.c",
    "lib/asyn-ares.c",
    "lib/asyn-thread.c",
    "lib/base64.c",
    "lib/bufq.c",
    "lib/bufref.c",
    "lib/c-hyper.c",
    "lib/cf-h1-proxy.c",
    "lib/cf-h2-proxy.c",
    "lib/cf-haproxy.c",
    "lib/cf-https-connect.c",
    "lib/cf-socket.c",
    "lib/cfilters.c",
    "lib/conncache.c",
    "lib/connect.c",
    "lib/content_encoding.c",
    "lib/cookie.c",
    "lib/curl_addrinfo.c",
    "lib/curl_des.c",
    "lib/curl_endian.c",
    "lib/curl_fnmatch.c",
    "lib/curl_get_line.c",
    "lib/curl_gethostname.c",
    "lib/curl_gssapi.c",
    "lib/curl_memrchr.c",
    "lib/curl_multibyte.c",
    "lib/curl_ntlm_core.c",
    "lib/curl_range.c",
    "lib/curl_rtmp.c",
    "lib/curl_sasl.c",
    "lib/curl_sha512_256.c",
    "lib/curl_sspi.c",
    "lib/curl_threads.c",
    "lib/curl_trc.c",
    "lib/cw-out.c",
    "lib/dict.c",
    "lib/dllmain.c",
    "lib/doh.c",
    "lib/dynbuf.c",
    "lib/dynhds.c",
    "lib/easy.c",
    "lib/easygetopt.c",
    "lib/easyoptions.c",
    "lib/escape.c",
    "lib/file.c",
    "lib/fileinfo.c",
    "lib/fopen.c",
    "lib/formdata.c",
    "lib/ftp.c",
    "lib/ftplistparser.c",
    "lib/getenv.c",
    "lib/getinfo.c",
    "lib/gopher.c",
    "lib/hash.c",
    "lib/headers.c",
    "lib/hmac.c",
    "lib/hostasyn.c",
    "lib/hostip.c",
    "lib/hostip4.c",
    "lib/hostip6.c",
    "lib/hostsyn.c",
    "lib/hsts.c",
    "lib/http.c",
    "lib/http1.c",
    "lib/http2.c",
    "lib/http_aws_sigv4.c",
    "lib/http_chunks.c",
    "lib/http_digest.c",
    "lib/http_negotiate.c",
    "lib/http_ntlm.c",
    "lib/http_proxy.c",
    "lib/idn.c",
    "lib/if2ip.c",
    "lib/imap.c",
    "lib/inet_ntop.c",
    "lib/inet_pton.c",
    "lib/krb5.c",
    "lib/ldap.c",
    "lib/llist.c",
    "lib/macos.c",
    "lib/md4.c",
    "lib/md5.c",
    "lib/memdebug.c",
    "lib/mime.c",
    "lib/mprintf.c",
    "lib/mqtt.c",
    "lib/multi.c",
    "lib/netrc.c",
    "lib/nonblock.c",
    "lib/noproxy.c",
    "lib/openldap.c",
    "lib/parsedate.c",
    "lib/pingpong.c",
    "lib/pop3.c",
    "lib/progress.c",
    "lib/psl.c",
    "lib/rand.c",
    "lib/rename.c",
    "lib/request.c",
    "lib/rtsp.c",
    "lib/select.c",
    "lib/sendf.c",
    "lib/setopt.c",
    "lib/sha256.c",
    "lib/share.c",
    "lib/slist.c",
    "lib/smb.c",
    "lib/smtp.c",
    "lib/socketpair.c",
    "lib/socks.c",
    "lib/socks_gssapi.c",
    "lib/socks_sspi.c",
    "lib/speedcheck.c",
    "lib/splay.c",
    "lib/strcase.c",
    "lib/strdup.c",
    "lib/strerror.c",
    "lib/strtok.c",
    "lib/strtoofft.c",
    "lib/system_win32.c",
    "lib/telnet.c",
    "lib/tftp.c",
    "lib/timediff.c",
    "lib/timeval.c",
    "lib/transfer.c",
    "lib/url.c",
    "lib/urlapi.c",
    "lib/version.c",
    "lib/version_win32.c",
    "lib/warnless.c",
    "lib/ws.c",
    "lib/vtls/bearssl.c",
    "lib/vtls/cipher_suite.c",
    "lib/vtls/gtls.c",
    "lib/vtls/hostcheck.c",
    "lib/vtls/keylog.c",
    "lib/vtls/mbedtls.c",
    "lib/vtls/mbedtls_threadlock.c",
    "lib/vtls/openssl.c",
    "lib/vtls/rustls.c",
    "lib/vtls/schannel.c",
    "lib/vtls/schannel_verify.c",
    "lib/vtls/sectransp.c",
    "lib/vtls/vtls.c",
    "lib/vtls/wolfssl.c",
    "lib/vtls/x509asn1.c",
    "lib/vauth/cleartext.c",
    "lib/vauth/cram.c",
    "lib/vauth/digest.c",
    "lib/vauth/digest_sspi.c",
    "lib/vauth/gsasl.c",
    "lib/vauth/krb5_gssapi.c",
    "lib/vauth/krb5_sspi.c",
    "lib/vauth/ntlm.c",
    "lib/vauth/ntlm_sspi.c",
    "lib/vauth/oauth2.c",
    "lib/vauth/spnego_gssapi.c",
    "lib/vauth/spnego_sspi.c",
    "lib/vauth/vauth.c",
    "lib/vquic/curl_msh3.c",
    "lib/vquic/curl_ngtcp2.c",
    "lib/vquic/curl_osslq.c",
    "lib/vquic/curl_quiche.c",
    "lib/vquic/vquic-tls.c",
    "lib/vquic/vquic.c",
    "lib/vssh/curl_path.c",
    "lib/vssh/libssh.c",
    "lib/vssh/libssh2.c",
    "lib/vssh/wolfssh.c",
};

/// nghttp2, for HTTP/2 multiplexing.
const nghttp2_sources: []const []const u8 = &.{
    "lib/nghttp2_alpn.c",
    "lib/nghttp2_buf.c",
    "lib/nghttp2_callbacks.c",
    "lib/nghttp2_debug.c",
    "lib/nghttp2_extpri.c",
    "lib/nghttp2_frame.c",
    "lib/nghttp2_hd.c",
    "lib/nghttp2_hd_huffman.c",
    "lib/nghttp2_hd_huffman_data.c",
    "lib/nghttp2_helper.c",
    "lib/nghttp2_http.c",
    "lib/nghttp2_map.c",
    "lib/nghttp2_mem.c",
    "lib/nghttp2_option.c",
    "lib/nghttp2_outbound_item.c",
    "lib/nghttp2_pq.c",
    "lib/nghttp2_priority_spec.c",
    "lib/nghttp2_queue.c",
    "lib/nghttp2_ratelim.c",
    "lib/nghttp2_rcbuf.c",
    "lib/nghttp2_session.c",
    "lib/nghttp2_stream.c",
    "lib/nghttp2_submit.c",
    "lib/nghttp2_time.c",
    "lib/nghttp2_version.c",
    "lib/sfparse.c",
};

/// zlib, for gzip/deflate Content-Encoding.
///
/// The gz*.c files are omitted: they are zlib's stdio-based gzFile API, which
/// curl does not use — it drives deflate/inflate directly. They also want
/// unistd.h, which zconf.h only exposes when its configure detected it, so
/// including them would mean carrying a define to enable code nothing calls.
const zlib_sources: []const []const u8 = &.{
    "adler32.c",
    "compress.c",
    "crc32.c",
    "deflate.c",
    "infback.c",
    "inffast.c",
    "inflate.c",
    "inftrees.c",
    "trees.c",
    "uncompr.c",
    "zutil.c",
};

/// mbedtls, which ships a usable default config rather than generating one.
const mbedtls_sources: []const []const u8 = &.{
    "library/aes.c",
    "library/aesce.c",
    "library/aesni.c",
    "library/aria.c",
    "library/asn1parse.c",
    "library/asn1write.c",
    "library/base64.c",
    "library/bignum.c",
    "library/bignum_core.c",
    "library/bignum_mod.c",
    "library/bignum_mod_raw.c",
    "library/block_cipher.c",
    "library/camellia.c",
    "library/ccm.c",
    "library/chacha20.c",
    "library/chachapoly.c",
    "library/cipher.c",
    "library/cipher_wrap.c",
    "library/cmac.c",
    "library/constant_time.c",
    "library/ctr_drbg.c",
    "library/debug.c",
    "library/des.c",
    "library/dhm.c",
    "library/ecdh.c",
    "library/ecdsa.c",
    "library/ecjpake.c",
    "library/ecp.c",
    "library/ecp_curves.c",
    "library/ecp_curves_new.c",
    "library/entropy.c",
    "library/entropy_poll.c",
    "library/error.c",
    "library/gcm.c",
    "library/hkdf.c",
    "library/hmac_drbg.c",
    "library/lmots.c",
    "library/lms.c",
    "library/md.c",
    "library/md5.c",
    "library/memory_buffer_alloc.c",
    "library/mps_reader.c",
    "library/mps_trace.c",
    "library/net_sockets.c",
    "library/nist_kw.c",
    "library/oid.c",
    "library/padlock.c",
    "library/pem.c",
    "library/pk.c",
    "library/pk_ecc.c",
    "library/pk_wrap.c",
    "library/pkcs12.c",
    "library/pkcs5.c",
    "library/pkcs7.c",
    "library/pkparse.c",
    "library/pkwrite.c",
    "library/platform.c",
    "library/platform_util.c",
    "library/poly1305.c",
    "library/psa_crypto.c",
    "library/psa_crypto_aead.c",
    "library/psa_crypto_cipher.c",
    "library/psa_crypto_client.c",
    "library/psa_crypto_driver_wrappers_no_static.c",
    "library/psa_crypto_ecp.c",
    "library/psa_crypto_ffdh.c",
    "library/psa_crypto_hash.c",
    "library/psa_crypto_mac.c",
    "library/psa_crypto_pake.c",
    "library/psa_crypto_rsa.c",
    "library/psa_crypto_se.c",
    "library/psa_crypto_slot_management.c",
    "library/psa_crypto_storage.c",
    "library/psa_its_file.c",
    "library/psa_util.c",
    "library/ripemd160.c",
    "library/rsa.c",
    "library/rsa_alt_helpers.c",
    "library/sha1.c",
    "library/sha256.c",
    "library/sha3.c",
    "library/sha512.c",
    "library/ssl_cache.c",
    "library/ssl_ciphersuites.c",
    "library/ssl_client.c",
    "library/ssl_cookie.c",
    "library/ssl_debug_helpers_generated.c",
    "library/ssl_msg.c",
    "library/ssl_ticket.c",
    "library/ssl_tls.c",
    "library/ssl_tls12_client.c",
    "library/ssl_tls12_server.c",
    "library/ssl_tls13_client.c",
    "library/ssl_tls13_generic.c",
    "library/ssl_tls13_keys.c",
    "library/ssl_tls13_server.c",
    "library/threading.c",
    "library/timing.c",
    "library/version.c",
    "library/version_features.c",
    "library/x509.c",
    "library/x509_create.c",
    "library/x509_crl.c",
    "library/x509_crt.c",
    "library/x509_csr.c",
    "library/x509write.c",
    "library/x509write_crt.c",
    "library/x509write_csr.c",
};

/// mbedtls 3.6.2 does not compile clean under current clang — it trips
/// -Wunterminated-string-initialization in ssl_tls13_keys.c, which its own
/// build promotes to an error. Upstream's, not ours, and the same shape as
/// jsoncons' literal-operator spelling: a library pinned in the CMake era
/// meeting a newer compiler.
const mbedtls_flags: []const []const u8 = &.{ "-std=c11", "-Wno-unterminated-string-initialization" };

const curl_flags: []const []const u8 = &.{"-std=c11"};

const StaticCurl = struct {
    curl: *std.Build.Dependency,
    mbedtls: *std.Build.Dependency,
    nghttp2: *std.Build.Dependency,
    zlib: *std.Build.Dependency,
};

const Deps = struct {
    jsoncons: *std.Build.Dependency,
    cpr: *std.Build.Dependency,
    /// Present only when -Dstatic-curl=true. Lazy, so an ordinary build does
    /// not fetch curl, mbedtls, nghttp2 and zlib to leave them unused.
    static_curl: ?StaticCurl,
    /// Directory holding the generated sql_resources.hpp.
    sql_resources_dir: std.Build.LazyPath,
};

/// Everything every artifact needs: the shared source, both fat libraries, and
/// the system transport.
fn addCore(b: *std.Build, mod: *std.Build.Module, d: Deps) void {
    mod.addIncludePath(b.path("include"));
    mod.addIncludePath(b.path("src")); // blobhttp_internal.hpp
    mod.addIncludePath(b.path("third_party")); // nlohmann/json.hpp, cpr/cprver.h
    mod.addIncludePath(d.jsoncons.path("include"));
    mod.addIncludePath(d.cpr.path("include"));
    mod.addIncludePath(d.sql_resources_dir);

    mod.addCSourceFiles(.{
        .root = d.cpr.path("."),
        .files = cpr_sources,
        .flags = cxx_flags,
    });
    mod.addCSourceFile(.{ .file = b.path("src/negotiate_auth.cpp"), .flags = cxx_flags });

    addCurl(b, mod, d);
    mod.link_libcpp = true;
}

/// Provide libcurl, one of two ways.
///
/// **System (default).** One line, no CMake ever, and the library is whatever
/// the machine's own package manager installed — patched by the vendor,
/// approved by whoever approves such things. The right answer when you control
/// the machines that will load this.
///
/// **Static (`-Dstatic-curl=true`).** curl, mbedtls, nghttp2 and zlib compiled
/// in, with a deliberately narrow feature set. The right answer when you do
/// not control those machines: a published extension cannot depend on a
/// libcurl whose TLS backend, protocol support and compression vary per host,
/// and Windows and WASM have no system libcurl at all. The cost is owning
/// curl's CVE stream and regenerating config headers per platform.
///
/// `curl_config.h` is NOT hand-derived. curl's ~200 feature defines are
/// interdependent and its HAVE_* entries are real compile-and-link probes, so
/// writing them by hand produces a curl that compiles and then misbehaves. It
/// was generated once by curl's own CMake with the flags in
/// third_party/curl_config/README.md, and committed — machine-produced config,
/// but no CMake at build time and `-Dtarget=` still works.
/// The directory name holding this target's generated configs.
///
/// curl's HAVE_* entries are real compile-and-link probes, so they genuinely
/// differ: Linux has HAVE_EVENTFD and HAVE_GETHOSTBYNAME_R_6, macOS has
/// HAVE_MACH_ABSOLUTE_TIME, and they disagree even on HAVE_FSETXATTR_5 versus
/// _6 — a difference that would be a silent ABI mismatch if hand-derived.
///
/// An unconfigured target fails here rather than at some confusing include,
/// with the fix in the message. tools/gen_curl_config.sh generates a new one;
/// run it on the target platform.
fn configDir(target: std.Target) []const u8 {
    return switch (target.os.tag) {
        .macos => switch (target.cpu.arch) {
            .aarch64 => "macos_arm64",
            else => @panic("no committed curl config for this macOS arch — see tools/gen_curl_config.sh"),
        },
        .linux => switch (target.cpu.arch) {
            .x86_64 => "linux_x86_64",
            else => @panic("no committed curl config for this Linux arch — see tools/gen_curl_config.sh"),
        },
        else => @panic("no committed curl config for this OS — see tools/gen_curl_config.sh"),
    };
}

fn addCurl(b: *std.Build, mod: *std.Build.Module, d: Deps) void {
    const cfg = configDir(mod.resolved_target.?.result);
    const sc = d.static_curl orelse {
        // OPENSSL_BACKEND_USED is deliberately not defined: it only enables
        // cpr's SSL_CTX callback, and nothing here sets one.
        mod.linkSystemLibrary("curl", .{});
        return;
    };
    mod.addIncludePath(sc.curl.path("include"));
    mod.addIncludePath(sc.curl.path("lib"));
    mod.addIncludePath(b.path(b.fmt("third_party/curl_config/{s}", .{cfg})));
    mod.addIncludePath(sc.mbedtls.path("include"));
    mod.addIncludePath(sc.nghttp2.path("lib/includes"));
    mod.addIncludePath(b.path(b.fmt("third_party/nghttp2_config/{s}", .{cfg})));
    // nghttp2ver.h is version numbers only and is the same everywhere, so it
    // sits above the per-platform directories rather than being duplicated.
    mod.addIncludePath(b.path("third_party/nghttp2_config"));
    mod.addIncludePath(sc.zlib.path("."));
    mod.addIncludePath(b.path("third_party/zlib_config"));

    mod.addCMacro("BUILDING_LIBCURL", "1");
    mod.addCMacro("NGHTTP2_STATICLIB", "1");

    // Not everything curl's build needs lives in curl_config.h. Its CMakeLists
    // sets -D_GNU_SOURCE as a compiler flag on Linux ("Required for
    // sendmmsg()"), and without it glibc hides struct addrinfo and EAI_* behind
    // their feature-test guards — 80 errors, none of which mention the cause.
    // Capturing the generated header is necessary but not sufficient; the flags
    // have to come across too.
    if (mod.resolved_target.?.result.os.tag == .linux) {
        mod.addCMacro("_GNU_SOURCE", "1");
    }
    mod.addCMacro("CURL_STATICLIB", "1");
    mod.addCMacro("HAVE_CONFIG_H", "1");

    mod.addCSourceFiles(.{ .root = sc.curl.path("."), .files = curl_sources, .flags = curl_flags });
    mod.addCSourceFiles(.{
        .root = sc.mbedtls.path("."),
        .files = mbedtls_sources,
        .flags = mbedtls_flags,
    });
    mod.addCSourceFiles(.{
        .root = sc.nghttp2.path("."),
        .files = nghttp2_sources,
        .flags = &.{ "-std=c11", "-DHAVE_CONFIG_H" },
    });
    mod.addCSourceFiles(.{ .root = sc.zlib.path("."), .files = zlib_sources, .flags = curl_flags });
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const bz = b.dependency("blobzig", .{ .target = target, .optimize = optimize });

    // Default false: linking the machine's own libcurl is the right choice
    // when you control the machines, and it needs no CMake and no per-platform
    // config. Turn it on to publish. See addCurl.
    const static_curl = b.option(bool, "static-curl",
        "Compile curl, mbedtls, nghttp2 and zlib in, instead of linking the system libcurl") orelse false;

    const base = struct {
        fn mod(bld: *std.Build, t: std.Build.ResolvedTarget, o: std.builtin.OptimizeMode) *std.Build.Module {
            return bld.createModule(.{ .target = t, .optimize = o, .link_libc = true });
        }
    }.mod;

    // ── Embed sql/*.sql as C++ string constants ───────────────────────
    //
    // Was a Python script run by CMake. Now a Zig tool, and the inputs are
    // named rather than globbed, so the build knows to re-run it when a .sql
    // file changes.
    const embed = b.addExecutable(.{
        .name = "embed_sql",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/embed_sql.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
        }),
    });
    const run_embed = b.addRunArtifact(embed);
    const sql_resources = run_embed.addOutputFileArg("sql_resources.hpp");
    for ([_][]const u8{
        "sql/http_config.sql",
        "sql/http_config_helpers.sql",
        "sql/http_verbs.sql",
        "sql/llm_adapt.sql",
        "sql/llm_complete.sql",
    }) |f| run_embed.addFileArg(b.path(f));

    const deps: Deps = .{
        .jsoncons = b.dependency("jsoncons", .{}),
        .cpr = b.dependency("cpr", .{}),
        .static_curl = if (!static_curl) null else .{
            // lazyDependency returns null on the first run, when the package
            // has yet to be fetched; the build re-runs itself afterwards.
            .curl = b.lazyDependency("curl", .{}) orelse return,
            .mbedtls = b.lazyDependency("mbedtls", .{}) orelse return,
            .nghttp2 = b.lazyDependency("nghttp2", .{}) orelse return,
            .zlib = b.lazyDependency("zlib", .{}) orelse return,
        },
        .sql_resources_dir = sql_resources.dirname(),
    };

    // ── The core, as the cdylib's root ────────────────────────────────
    //
    // Its own module rather than being compiled into each extension, because
    // the adapters still carry their own copies of AcquireRateLimit and
    // RecordResponseStats — the very duplication the core exists to remove.
    // Linking both into one artifact is a duplicate-symbol error, which is a
    // usefully loud reminder that rewiring the adapters is the next step.
    const core = base(b, target, optimize);
    addCore(b, core, deps);
    core.addCSourceFiles(.{
        .files = &.{ "src/blobhttp_core.cpp", "src/blobhttp_llm.cpp" },
        .flags = cxx_flags,
    });

    // ── DuckDB extension (C++ against the C API) ──────────────────────
    const duckdb_mod = base(b, target, optimize);
    addCore(b, duckdb_mod, deps);
    duckdb_mod.addIncludePath(bz.namedLazyPath("duckdb_capi_include"));
    duckdb_mod.addIncludePath(b.path("duckdb_ext/src"));
    duckdb_mod.addCMacro("DUCKDB_EXTENSION_NAME", "bhttp");
    duckdb_mod.addCSourceFiles(.{
        .files = &.{
            "src/blobhttp_core.cpp",
            "src/blobhttp_llm.cpp",
            "duckdb_ext/src/bhttp_ext.cpp",
            "duckdb_ext/src/bhttp_functions.cpp",
            "duckdb_ext/src/bhttp_llm.cpp",
            "duckdb_ext/src/bhttp_llm_adapt.cpp",
        },
        .flags = cxx_flags,
    });

    // ── SQLite extension ──────────────────────────────────────────────
    //
    // bhttp_sqlite.cpp includes only blobhttp.h and nlohmann — no cpr, no rate
    // limiter, no HttpConfig. The core sources still compile into this
    // artifact (it needs an implementation to link against), but the adapter
    // itself is now pure marshalling.
    const sqlite_mod = base(b, target, optimize);
    addCore(b, sqlite_mod, deps);
    sqlite_mod.addIncludePath(bz.namedLazyPath("sqlite_include"));
    sqlite_mod.addCSourceFiles(.{
        .files = &.{
            "src/blobhttp_core.cpp",
            "src/blobhttp_llm.cpp",
            "sqlite_ext/src/bhttp_sqlite.cpp",
        },
        .flags = cxx_flags,
    });

    const artifacts = blobzig.addHostExtensions(b, bz, .{
        .name = "bhttp",
        .target = target,
        .optimize = optimize,
        .core = core,
        .duckdb_module = duckdb_mod,
        .sqlite_module = sqlite_mod,
        // Static: the only symbols outside the artifact are two macOS
        // frameworks, named in full so the caveat stays as small as it is.
        // System: libcurl is resolved from the host at load, as blobodbc
        // resolves SQL* from the ODBC driver manager.
        .allow_undefined = if (static_curl)
            &.{ "CFRelease", "SCDynamicStoreCopyProxies" }
        else
            &.{"curl_"},
    });
    // curl reads macOS proxy settings via SCDynamicStoreCopyProxies, which
    // curl_setup.h enables automatically on Apple targets with IPv6. Both
    // frameworks ship with the OS exactly as libSystem does, so this costs
    // nothing in self-containment — and the alternative, disabling the lookup,
    // would silently ignore a proxy the user set in System Settings.
    //
    // Linked on the artifacts rather than the modules: on the module it did not
    // reach the final link step.
    if (static_curl and target.result.os.tag == .macos) {
        for ([_]?*std.Build.Step.Compile{ artifacts.lib, artifacts.duckdb, artifacts.sqlite }) |maybe| {
            if (maybe) |art| {
                art.root_module.linkFramework("SystemConfiguration", .{});
                art.root_module.linkFramework("CoreFoundation", .{});
            }
        }
    }

    artifacts.lib.?.installHeader(b.path("include/blobhttp.h"), "blobhttp.h");

    // ── C test for the core ABI, with no host involved ────────────────
    const t = b.addExecutable(.{
        .name = "test_core_abi",
        .root_module = base(b, target, optimize),
    });
    addCore(b, t.root_module, deps);
    t.root_module.addCSourceFiles(.{
        .files = &.{ "src/blobhttp_core.cpp", "src/blobhttp_llm.cpp" },
        .flags = cxx_flags,
    });
    t.root_module.addCSourceFile(.{
        .file = b.path("test/test_core_abi.c"),
        .flags = &.{"-std=c11"},
    });
    if (static_curl and target.result.os.tag == .macos) {
        t.root_module.linkFramework("SystemConfiguration", .{});
        t.root_module.linkFramework("CoreFoundation", .{});
    }
    b.installArtifact(t);
    b.step("test-core", "Exercise the C ABI directly (needs the network)")
        .dependOn(&b.addRunArtifact(t).step);
}
