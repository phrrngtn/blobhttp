"""Packaging wrapper for the bhttp DuckDB extension."""

import pathlib

_HERE = pathlib.Path(__file__).parent


def extension_path() -> str:
    """Return the absolute path to the bhttp DuckDB extension.

    Usage:
        LOAD '<path>';  -- in DuckDB with allow_unsigned_extensions
    """
    # blobhttp produces a shared lib (not .duckdb_extension) since it
    # doesn't use append_metadata.py
    for name in ("libbhttp.dylib", "libbhttp.so", "bhttp.dll", "bhttp.duckdb_extension"):
        ext = _HERE / name
        if ext.exists():
            return str(ext)
    raise FileNotFoundError(f"Extension not found in {_HERE}")
