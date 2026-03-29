cmake_minimum_required(VERSION 3.20)
cmake_policy(SET CMP0148 OLD)
project(blobhttp_duckdb LANGUAGES C CXX)

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

# ── DuckDB C API headers (checked in, symlinked at duckdb_capi/) ─────
set(DUCKDB_CAPI_INCLUDE "${ROOT}/duckdb_capi")

# ── DuckDB extension ────────────────────────────────────────────────
if(NOT DEFINED EXTENSION_NAME)
    set(EXTENSION_NAME "bhttp")
endif()
if(DEFINED TARGET_DUCKDB_VERSION_MAJOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MAJOR=${TARGET_DUCKDB_VERSION_MAJOR})
endif()
if(DEFINED TARGET_DUCKDB_VERSION_MINOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MINOR=${TARGET_DUCKDB_VERSION_MINOR})
endif()
if(DEFINED TARGET_DUCKDB_VERSION_PATCH)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_PATCH=${TARGET_DUCKDB_VERSION_PATCH})
endif()
add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})
if(DEFINED DUCKDB_EXTENSION_API_VERSION_UNSTABLE)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=${DUCKDB_EXTENSION_API_VERSION_UNSTABLE})
endif()

# Embed SQL macros as C++ string constants
file(GLOB SQL_FILES ${ROOT}/sql/*.sql)
set(SQL_RESOURCES_HEADER ${CMAKE_BINARY_DIR}/sql_resources.hpp)
find_package(Python3 REQUIRED COMPONENTS Interpreter)
add_custom_command(
    OUTPUT ${SQL_RESOURCES_HEADER}
    COMMAND ${Python3_EXECUTABLE}
        ${ROOT}/cmake/embed_sql.py
        ${ROOT}/sql
        ${SQL_RESOURCES_HEADER}
    DEPENDS ${SQL_FILES} ${ROOT}/cmake/embed_sql.py
    COMMENT "Embedding SQL macros into sql_resources.hpp")

set(DUCKDB_EXTENSION_SOURCES
    ${ROOT}/duckdb_ext/src/bhttp_ext.cpp
    ${ROOT}/duckdb_ext/src/bhttp_functions.cpp
    ${ROOT}/duckdb_ext/src/bhttp_llm.cpp
    ${ROOT}/duckdb_ext/src/bhttp_llm_adapt.cpp
    ${BHTTP_SOURCES}
    ${SQL_RESOURCES_HEADER})

option(DUCKDB_WASM_EXTENSION "Whether compiling for Wasm target" OFF)
if(DUCKDB_WASM_EXTENSION)
    add_library(${EXTENSION_NAME} STATIC ${DUCKDB_EXTENSION_SOURCES})
else()
    add_library(${EXTENSION_NAME} SHARED ${DUCKDB_EXTENSION_SOURCES})
    if(WIN32 AND MINGW)
        set_target_properties(${EXTENSION_NAME} PROPERTIES PREFIX "")
        target_link_options(${EXTENSION_NAME} PRIVATE -static-libgcc -static-libstdc++ -Wl,-Bstatic -lpthread -Wl,-Bdynamic)
    endif()
endif()

target_include_directories(${EXTENSION_NAME} PRIVATE
    ${BHTTP_INCLUDES} ${DUCKDB_CAPI_INCLUDE} ${ROOT}/duckdb_ext/src ${CMAKE_BINARY_DIR})
target_link_libraries(${EXTENSION_NAME} PRIVATE ${BHTTP_LIBS})
bhttp_platform_setup(${EXTENSION_NAME})

# Append DuckDB extension metadata so DuckDB 1.5+ accepts the file
add_custom_command(TARGET ${EXTENSION_NAME} POST_BUILD
    COMMAND ${Python3_EXECUTABLE}
        "${ROOT}/duckdb_ext/append_metadata.py"
        "$<TARGET_FILE:${EXTENSION_NAME}>"
        "$<TARGET_FILE_DIR:${EXTENSION_NAME}>/${EXTENSION_NAME}.duckdb_extension"
    COMMENT "Appending DuckDB extension metadata")

install(FILES "$<TARGET_FILE_DIR:${EXTENSION_NAME}>/${EXTENSION_NAME}.duckdb_extension"
    DESTINATION blobhttp_duckdb/)

# Bundle SQL macros that depend on other extensions (llm_adapt, llm_complete).
# These can't be embedded in the C extension because they reference functions
# from blobtemplates which may not be loaded at extension init time.
install(DIRECTORY ${ROOT}/sql/ DESTINATION blobhttp_duckdb/sql
        FILES_MATCHING PATTERN "llm_*.sql")
