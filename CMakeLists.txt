cmake_minimum_required(VERSION 3.20)
project(blobhttp LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# All code (including static deps like cpr/libcurl) must be compiled with -fPIC
# since extensions are shared libraries.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

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

# ── SQLite (for SQLite extension) ────────────────────────────────────
option(BUILD_SQLITE_EXTENSION "Build SQLite loadable extension" OFF)
if(BUILD_SQLITE_EXTENSION)
  FetchContent_Declare(sqlite3
    URL "https://www.sqlite.org/2024/sqlite-amalgamation-3460100.zip"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
  FetchContent_MakeAvailable(sqlite3)

  if(NOT TARGET sqlite3_lib)
    add_library(sqlite3_lib STATIC ${sqlite3_SOURCE_DIR}/sqlite3.c)
    target_include_directories(sqlite3_lib PUBLIC ${sqlite3_SOURCE_DIR})
    target_compile_definitions(sqlite3_lib PRIVATE SQLITE_CORE)
  endif()
endif()

# ── Sources shared by all targets ────────────────────────────────────
set(BHTTP_SOURCES src/negotiate_auth.cpp)
set(BHTTP_INCLUDES include)
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

# ── DuckDB C API headers (checked in at duckdb_capi/) ────────────────
set(DUCKDB_CAPI_INCLUDE "${CMAKE_SOURCE_DIR}/duckdb_capi")

# ── DuckDB extension ────────────────────────────────────────────────
option(BUILD_DUCKDB_EXTENSION "Build DuckDB loadable extension" ON)

if(BUILD_DUCKDB_EXTENSION)
  # DuckDB extension configuration
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
  file(GLOB SQL_FILES ${CMAKE_SOURCE_DIR}/sql/*.sql)
  set(SQL_RESOURCES_HEADER ${CMAKE_BINARY_DIR}/sql_resources.hpp)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_custom_command(
    OUTPUT ${SQL_RESOURCES_HEADER}
    COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_SOURCE_DIR}/cmake/embed_sql.py
      ${CMAKE_SOURCE_DIR}/sql
      ${SQL_RESOURCES_HEADER}
    DEPENDS ${SQL_FILES} ${CMAKE_SOURCE_DIR}/cmake/embed_sql.py
    COMMENT "Embedding SQL macros into sql_resources.hpp")

  set(DUCKDB_EXTENSION_SOURCES
    duckdb_ext/src/bhttp_ext.cpp
    duckdb_ext/src/bhttp_functions.cpp
    duckdb_ext/src/bhttp_llm.cpp
    duckdb_ext/src/bhttp_llm_adapt.cpp
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
    ${BHTTP_INCLUDES} ${DUCKDB_CAPI_INCLUDE} duckdb_ext/src ${CMAKE_BINARY_DIR})
  target_link_libraries(${EXTENSION_NAME} PRIVATE ${BHTTP_LIBS})
  bhttp_platform_setup(${EXTENSION_NAME})
endif()

# ── SQLite extension ────────────────────────────────────────────────
if(BUILD_SQLITE_EXTENSION)
  add_library(bhttp_sqlite MODULE
    sqlite_ext/src/bhttp_sqlite.cpp
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
endif()

# ── Python bindings (nanobind) ───────────────────────────────────────
option(BUILD_PYTHON_BINDINGS "Build nanobind Python module" OFF)
if(BUILD_PYTHON_BINDINGS)
  find_package(Python 3.8 REQUIRED COMPONENTS Interpreter Development.Module)
  execute_process(COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
    OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE NB_DIR)
  list(APPEND CMAKE_PREFIX_PATH "${NB_DIR}")
  find_package(nanobind CONFIG REQUIRED)

  nanobind_add_module(blobhttp_ext NB_STATIC
    python/bindings.cpp
    ${BHTTP_SOURCES})
  target_include_directories(blobhttp_ext PRIVATE ${BHTTP_INCLUDES})
  target_link_libraries(blobhttp_ext PRIVATE ${BHTTP_LIBS})
  bhttp_platform_setup(blobhttp_ext)
  set_target_properties(blobhttp_ext PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/python/blobhttp")
endif()
