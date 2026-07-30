//! Generate a C++ header of SQL statement constants from .sql files.
//!
//! A port of cmake/embed_sql.py, so the build needs no Python — the same move
//! blobzig made with append_metadata.py, and for the same reason: a build step
//! that shells out to an interpreter is a dependency the build cannot check.
//!
//! Per input file, emit
//!
//!     inline const std::vector<std::string> <stem> = { "stmt", "stmt", ... };
//!
//! in namespace blobhttp::sql. Statements are split on semicolons after line
//! comments are stripped, with runs of horizontal whitespace collapsed — these
//! end up in the binary, so the formatting of the source file should not.
//!
//! The inputs are named on the command line rather than globbed from a
//! directory, which is the one deliberate difference from the Python: build.zig
//! lists them, so the build system knows the dependency set and a changed .sql
//! file actually triggers a rebuild.
//!
//! usage: embed_sql <output_file> <input.sql>...

const std = @import("std");

/// Strip `--` line comments.
///
/// Deliberately not string-literal-aware, matching the Python original: a `--`
/// inside a SQL string literal would be eaten. Nothing in sql/ has one, and
/// every statement here is executed by the extension's own tests, so a
/// regression fails loudly rather than silently.
fn stripComments(gpa: std.mem.Allocator, sql: []const u8, out: *std.ArrayList(u8)) !void {
    var i: usize = 0;
    while (i < sql.len) {
        if (i + 1 < sql.len and sql[i] == '-' and sql[i + 1] == '-') {
            while (i < sql.len and sql[i] != '\n') i += 1;
        } else {
            try out.append(gpa, sql[i]);
            i += 1;
        }
    }
}

/// Collapse horizontal whitespace runs, drop indentation, collapse blank
/// lines, then trim. Mirrors the Python's four regexes.
fn normalize(gpa: std.mem.Allocator, raw: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);
    var i: usize = 0;
    while (i < raw.len) {
        switch (raw[i]) {
            ' ', '\t' => {
                while (i < raw.len and (raw[i] == ' ' or raw[i] == '\t')) i += 1;
                const prev = if (out.items.len > 0) out.items[out.items.len - 1] else '\n';
                if (prev != '\n') try out.append(gpa, ' ');
            },
            '\n' => {
                while (i < raw.len and (raw[i] == '\n' or raw[i] == ' ' or raw[i] == '\t')) i += 1;
                if (out.items.len > 0) try out.append(gpa, '\n');
            },
            else => {
                try out.append(gpa, raw[i]);
                i += 1;
            },
        }
    }
    return gpa.dupe(u8, std.mem.trim(u8, out.items, " \t\r\n"));
}

fn appendEscaped(gpa: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    for (s) |c| switch (c) {
        '\\' => try out.appendSlice(gpa, "\\\\"),
        '"' => try out.appendSlice(gpa, "\\\""),
        '\n' => try out.appendSlice(gpa, "\\n"),
        else => try out.append(gpa, c),
    };
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    if (args.len < 3) {
        std.debug.print("usage: {s} <output_file> <input.sql>...\n", .{args[0]});
        std.process.exit(1);
    }
    const out_path = args[1];
    const inputs = args[2..];

    const cwd = std.Io.Dir.cwd();

    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);

    try out.appendSlice(gpa,
        \\#pragma once
        \\// Auto-generated from sql/*.sql — do not edit.
        \\// Regenerated at build time by tools/embed_sql.zig.
        \\
        \\#include <string>
        \\#include <vector>
        \\
        \\namespace blobhttp {
        \\namespace sql {
        \\
        \\
    );

    for (inputs) |path| {
        const base = std.fs.path.basename(path);
        const stem = base[0 .. base.len - std.fs.path.extension(base).len];

        const raw = try cwd.readFileAlloc(io, path, gpa, .unlimited);
        defer gpa.free(raw);

        var uncommented: std.ArrayList(u8) = .empty;
        defer uncommented.deinit(gpa);
        try stripComments(gpa, raw, &uncommented);

        try out.appendSlice(gpa, "inline const std::vector<std::string> ");
        try out.appendSlice(gpa, stem);
        try out.appendSlice(gpa, " = {\n");

        var first = true;
        var parts = std.mem.splitScalar(u8, uncommented.items, ';');
        while (parts.next()) |part| {
            const stmt = try normalize(gpa, part);
            defer gpa.free(stmt);
            if (stmt.len == 0) continue;
            if (!first) try out.appendSlice(gpa, ",\n");
            first = false;
            try out.appendSlice(gpa, "    \"");
            try appendEscaped(gpa, &out, stmt);
            try out.append(gpa, '"');
        }
        try out.appendSlice(gpa, "\n};\n\n");
    }

    try out.appendSlice(gpa,
        \\} // namespace sql
        \\} // namespace blobhttp
        \\
    );

    try cwd.writeFile(io, .{ .sub_path = out_path, .data = out.items });
}
