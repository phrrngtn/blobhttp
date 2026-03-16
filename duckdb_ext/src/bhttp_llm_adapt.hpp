#pragma once

#include "duckdb_extension.h"

namespace blobhttp {
void RegisterLlmAdaptFunction(duckdb_connection connection);
} // namespace blobhttp
