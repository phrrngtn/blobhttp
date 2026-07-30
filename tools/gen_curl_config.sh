#!/bin/sh
# Generate curl_config.h for this platform, with blobhttp's feature set.
#
# Run on the target platform. Produces ./curl_config.h, which belongs in
# blobhttp/third_party/curl_config/<platform>/. See that directory's README for
# why the config is generated once and committed rather than hand-derived or
# regenerated at build time.
set -eu

WORK="${WORK:-$PWD/curlcfg}"
CURL_TAG="${CURL_TAG:-curl-8_11_1}"
MBEDTLS_TAG="${MBEDTLS_TAG:-v3.6.2}"
MBEDTLS_FRAMEWORK="${MBEDTLS_FRAMEWORK:-94599c0e3b5036e086446a51a3f79640f70f22f6}"
NGHTTP2_TAG="${NGHTTP2_TAG:-v1.64.0}"
ZLIB_TAG="${ZLIB_TAG:-v1.3.1}"

mkdir -p "$WORK"
cd "$WORK"

clone() { # repo tag dir
    [ -d "$3" ] || git clone -q --depth 1 --branch "$2" "$1" "$3"
}

clone https://github.com/curl/curl.git            "$CURL_TAG"    curl
clone https://github.com/Mbed-TLS/mbedtls.git     "$MBEDTLS_TAG" mbedtls
clone https://github.com/nghttp2/nghttp2.git      "$NGHTTP2_TAG" nghttp2
clone https://github.com/madler/zlib.git          "$ZLIB_TAG"    zlib

# mbedtls needs its `framework` submodule even with testing off. Cloned
# explicitly because `zig fetch` does not fetch submodules — the same trap that
# bit rapidyaml/c4core and c4core/debugbreak in this migration.
if [ ! -f mbedtls/framework/CMakeLists.txt ]; then
    rm -rf mbedtls/framework
    git clone -q https://github.com/Mbed-TLS/mbedtls-framework mbedtls/framework
    (cd mbedtls/framework && git checkout -q "$MBEDTLS_FRAMEWORK")
fi

# -DMBEDTLS_FATAL_WARNINGS=OFF: mbedtls 3.6.2 trips
# -Wunterminated-string-initialization on current compilers and makes it fatal.
echo "==> mbedtls"
cmake -S mbedtls -B b-mbedtls -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_TESTING=OFF -DENABLE_PROGRAMS=OFF \
    -DUSE_SHARED_MBEDTLS_LIBRARY=OFF -DMBEDTLS_FATAL_WARNINGS=OFF \
    -DCMAKE_INSTALL_PREFIX="$WORK/i-mbedtls" >/dev/null
cmake --build b-mbedtls -j"$(nproc 2>/dev/null || echo 4)" >/dev/null
cmake --install b-mbedtls >/dev/null

# -DBUILD_TESTING=OFF or nghttp2 fails generating a test target with no sources.
echo "==> nghttp2"
cmake -S nghttp2 -B b-nghttp2 -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_LIB_ONLY=ON -DBUILD_SHARED_LIBS=OFF -DBUILD_STATIC_LIBS=ON \
    -DBUILD_TESTING=OFF -DENABLE_DOC=OFF \
    -DCMAKE_INSTALL_PREFIX="$WORK/i-nghttp2" >/dev/null
cmake --build b-nghttp2 -j"$(nproc 2>/dev/null || echo 4)" >/dev/null
cmake --install b-nghttp2 >/dev/null

echo "==> zlib"
cmake -S zlib -B b-zlib -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$WORK/i-zlib" >/dev/null
cmake --build b-zlib -j"$(nproc 2>/dev/null || echo 4)" >/dev/null
cmake --install b-zlib >/dev/null

# curl: configure only. The build artifacts are discarded; the point is the
# generated header and the feature summary it implies.
echo "==> curl (configure only)"
cmake -S curl -B b-curl \
    -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DBUILD_STATIC_LIBS=ON \
    -DBUILD_CURL_EXE=OFF -DBUILD_TESTING=OFF -DBUILD_EXAMPLES=OFF \
    -DCURL_USE_MBEDTLS=ON -DCURL_USE_OPENSSL=OFF \
    -DCURL_USE_LIBSSH2=OFF -DCURL_USE_LIBSSH=OFF -DCURL_USE_LIBPSL=OFF \
    -DUSE_LIBIDN2=OFF -DCURL_USE_GSSAPI=OFF \
    -DCURL_BROTLI=OFF -DCURL_ZSTD=OFF -DCURL_ZLIB=ON -DUSE_NGHTTP2=ON \
    -DCURL_DISABLE_LDAP=ON -DCURL_DISABLE_LDAPS=ON -DCURL_DISABLE_FTP=ON \
    -DCURL_DISABLE_TELNET=ON -DCURL_DISABLE_DICT=ON -DCURL_DISABLE_TFTP=ON \
    -DCURL_DISABLE_GOPHER=ON -DCURL_DISABLE_IMAP=ON -DCURL_DISABLE_POP3=ON \
    -DCURL_DISABLE_SMTP=ON -DCURL_DISABLE_SMB=ON -DCURL_DISABLE_RTSP=ON \
    -DCURL_DISABLE_MQTT=ON -DCURL_DISABLE_FILE=ON -DCURL_DISABLE_NTLM=ON \
    -DMBEDTLS_INCLUDE_DIRS="$WORK/i-mbedtls/include" \
    -DMBEDTLS_LIBRARY="$WORK/i-mbedtls/lib/libmbedtls.a" \
    -DMBEDX509_LIBRARY="$WORK/i-mbedtls/lib/libmbedx509.a" \
    -DMBEDCRYPTO_LIBRARY="$WORK/i-mbedtls/lib/libmbedcrypto.a" \
    -DNGHTTP2_INCLUDE_DIR="$WORK/i-nghttp2/include" \
    -DNGHTTP2_LIBRARY="$WORK/i-nghttp2/lib/libnghttp2.a" \
    -DZLIB_INCLUDE_DIR="$WORK/i-zlib/include" \
    -DZLIB_LIBRARY="$WORK/i-zlib/lib/libz.a" \
    | grep -E "^-- (Protocols|Features|Enabled SSL)"

cp b-curl/lib/curl_config.h "$WORK/curl_config.h"
cp b-nghttp2/config.h "$WORK/nghttp2_config.h"
cp b-nghttp2/lib/includes/nghttp2/nghttp2ver.h "$WORK/nghttp2ver.h"
cp b-zlib/zconf.h "$WORK/zconf.h"

echo
echo "wrote:"
echo "  $WORK/curl_config.h"
echo "  $WORK/nghttp2_config.h   -> third_party/nghttp2_config/config.h"
echo "  $WORK/nghttp2ver.h       -> third_party/nghttp2_config/nghttp2/nghttp2ver.h"
echo "  $WORK/zconf.h            -> third_party/zlib_config/zconf.h"
