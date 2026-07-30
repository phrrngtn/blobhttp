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

const Deps = struct {
    jsoncons: *std.Build.Dependency,
    cpr: *std.Build.Dependency,
    /// Directory holding the generated sql_resources.hpp.
    sql_resources_dir: std.Build.LazyPath,
};

/// Everything every artifact needs: the shared source, both fat libraries, and
/// the system transport.
fn addCore(b: *std.Build, mod: *std.Build.Module, d: Deps) void {
    mod.addIncludePath(b.path("include"));
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

    // libcurl comes from the host. OPENSSL_BACKEND_USED is deliberately NOT
    // defined: it only enables cpr's SSL_CTX callback, and nothing here sets
    // one — the SSL surface used is CaInfo/CertFile/KeyFile/VerifySsl, which
    // are plain curl options on any backend.
    mod.linkSystemLibrary("curl", .{});
    mod.link_libcpp = true;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const bz = b.dependency("blobzig", .{ .target = target, .optimize = optimize });

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
        .sql_resources_dir = sql_resources.dirname(),
    };

    // ── DuckDB extension (C++ against the C API) ──────────────────────
    const duckdb_mod = base(b, target, optimize);
    addCore(b, duckdb_mod, deps);
    duckdb_mod.addIncludePath(bz.namedLazyPath("duckdb_capi_include"));
    duckdb_mod.addIncludePath(b.path("duckdb_ext/src"));
    duckdb_mod.addCMacro("DUCKDB_EXTENSION_NAME", "bhttp");
    duckdb_mod.addCSourceFiles(.{
        .files = &.{
            "duckdb_ext/src/bhttp_ext.cpp",
            "duckdb_ext/src/bhttp_functions.cpp",
            "duckdb_ext/src/bhttp_llm.cpp",
            "duckdb_ext/src/bhttp_llm_adapt.cpp",
        },
        .flags = cxx_flags,
    });

    // ── SQLite extension ──────────────────────────────────────────────
    const sqlite_mod = base(b, target, optimize);
    addCore(b, sqlite_mod, deps);
    sqlite_mod.addIncludePath(bz.namedLazyPath("sqlite_include"));
    sqlite_mod.addCSourceFile(.{
        .file = b.path("sqlite_ext/src/bhttp_sqlite.cpp"),
        .flags = cxx_flags,
    });

    _ = blobzig.addHostExtensions(b, bz, .{
        .name = "bhttp",
        .target = target,
        .optimize = optimize,
        // No `core`: there is no C ABI to publish yet, so there is nothing for
        // a cdylib to export. See the note at the top of this file.
        .duckdb_module = duckdb_mod,
        .sqlite_module = sqlite_mod,
        // libcurl is resolved from the host process, exactly as blobodbc
        // resolves SQL* from the ODBC driver manager. Listing it here is the
        // documentation that this artifact is not self-contained.
        .allow_undefined = &.{"curl_"},
    });
}
