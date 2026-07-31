#pragma once

#include "duckdb_extension.h"

namespace blobhttp {
void RegisterHttpFunctions(duckdb_connection connection);

//! Register the SQL macros embedded from sql/*.sql.
//!
//! Must be called AFTER every raw _-prefixed scalar is registered: DuckDB
//! validates a macro body at CREATE time, so a macro naming a function that
//! does not yet exist is rejected.
void RegisterHttpMacros(duckdb_connection connection);
} // namespace blobhttp
