#pragma once

#include "duckdb_extension.h"

namespace blobhttp {
void RegisterLlmFunctions(duckdb_connection connection);
} // namespace blobhttp
