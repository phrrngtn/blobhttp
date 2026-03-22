cmake_minimum_required(VERSION 3.20)
cmake_policy(SET CMP0148 OLD)
project(blobhttp_sqlite LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(ROOT "${CMAKE_CURRENT_SOURCE_DIR}")

include(FetchContent)

# ── nlohmann/json ────────────────────────────────────────────────────
FetchContent_Declare(nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
    GIT_SHALLOW    TRUE)
set(JSON_BuildTests OFF CACHE INTERNAL "")
FetchContent_MakeAvailable(nlohmann_json)

# ── jsoncons (header-only: JMESPath, JSON Schema validation) ─────
FetchContent_Declare(jsoncons
    GIT_REPOSITORY https://github.com/danielaparker/jsoncons.git
    GIT_TAG        v1.1.0
    GIT_SHALLOW    TRUE)
set(JSONCONS_BUILD_TESTS OFF CACHE INTERNAL "")
FetchContent_MakeAvailable(jsoncons)

# ── cpr — C++ Requests: Curl for People ─────────────────────────────
FetchContent_Declare(cpr
    GIT_REPOSITORY https://github.com/libcpr/cpr.git
    GIT_TAG        1.11.2
    GIT_SHALLOW    TRUE)
set(CPR_BUILD_TESTS OFF CACHE INTERNAL "")
set(BUILD_SHARED_LIBS OFF CACHE INTERNAL "")
set(CURL_USE_LIBSSH2 OFF CACHE INTERNAL "")
set(USE_LIBIDN2 OFF CACHE INTERNAL "")
FetchContent_MakeAvailable(cpr)

# ── SQLite amalgamation ──────────────────────────────────────────────
FetchContent_Declare(sqlite3
    URL "https://www.sqlite.org/2024/sqlite-amalgamation-3460100.zip"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_MakeAvailable(sqlite3)

# ── Sources shared by all targets ────────────────────────────────────
set(BHTTP_SOURCES ${ROOT}/src/negotiate_auth.cpp)
set(BHTTP_INCLUDES ${ROOT}/include)
set(BHTTP_LIBS nlohmann_json::nlohmann_json cpr::cpr jsoncons)

# Hide symbols by default
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(VISIBILITY_INLINES_HIDDEN ON)
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -s")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -s")

# On macOS with Homebrew, ensure linker can find Homebrew libraries (e.g. libidn2)
if(APPLE)
    execute_process(
        COMMAND brew --prefix
        OUTPUT_VARIABLE HOMEBREW_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
    if(HOMEBREW_PREFIX)
        link_directories(${HOMEBREW_PREFIX}/lib)
    endif()
endif()

# Helper: link SSPI on Windows for SPNEGO
function(bhttp_platform_setup target)
    if(WIN32)
        target_link_libraries(${target} PRIVATE secur32)
    endif()
endfunction()

# ── SQLite extension ────────────────────────────────────────────────
add_library(bhttp_sqlite MODULE
    ${ROOT}/sqlite_ext/src/bhttp_sqlite.cpp
    ${BHTTP_SOURCES})
target_include_directories(bhttp_sqlite PRIVATE
    ${BHTTP_INCLUDES} ${sqlite3_SOURCE_DIR})
target_link_libraries(bhttp_sqlite PRIVATE ${BHTTP_LIBS})
bhttp_platform_setup(bhttp_sqlite)
set_target_properties(bhttp_sqlite PROPERTIES
    OUTPUT_NAME "bhttp"
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/sqlite")
if(APPLE)
    target_link_options(bhttp_sqlite PRIVATE -undefined dynamic_lookup)
    add_custom_command(TARGET bhttp_sqlite POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            bhttp.so "$<TARGET_FILE_DIR:bhttp_sqlite>/bhttp.dylib")
endif()

install(TARGETS bhttp_sqlite LIBRARY DESTINATION blobhttp_sqlite/)
if(APPLE)
    install(CODE "
        execute_process(COMMAND \${CMAKE_COMMAND} -E create_symlink
            bhttp.so bhttp.dylib
            WORKING_DIRECTORY \${CMAKE_INSTALL_PREFIX}/blobhttp_sqlite)
    ")
endif()
