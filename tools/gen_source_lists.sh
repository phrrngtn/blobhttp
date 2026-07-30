#!/bin/sh
# Regenerate the enumerated source lists in build.zig's curl/mbedtls sections.
# Enumerated rather than globbed so an upstream bump shows up as a diff to read
# rather than as files silently joining the build.
set -eu
pkg="$1"   # path to the fetched package
sub="$2"   # subdirectory to list, relative to pkg
( cd "$pkg" && ls "$sub"/*.c | sed 's/^/    "/; s/$/",/' )
