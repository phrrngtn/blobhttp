"""Packaging wrapper for the bhttp DuckDB extension.

Provides extension_path() for manual loading and setup() for
one-call initialization including cross-extension SQL macros.
"""

import pathlib

_HERE = pathlib.Path(__file__).parent
_loaded = False


def extension_path() -> str:
    """Return the absolute path to the bhttp DuckDB extension.

    Usage:
        LOAD '<path>';  -- in DuckDB with allow_unsigned_extensions
    """
    for name in ("libbhttp.dylib", "libbhttp.so", "bhttp.dll", "bhttp.duckdb_extension"):
        ext = _HERE / name
        if ext.exists():
            return str(ext)
    raise FileNotFoundError(f"Extension not found in {_HERE}")


def setup(con):
    """Load the bhttp extension and register all SQL macros.

    Idempotent — safe to call multiple times on the same connection.
    All macros use CREATE OR REPLACE.

    If blobtemplates is available, also registers llm_adapt() and
    llm_complete() macros which depend on bt_template_render().

    Args:
        con: a duckdb.DuckDBPyConnection
    """
    global _loaded
    if not _loaded:
        con.execute(f"LOAD '{extension_path()}'")
        _loaded = True

    # The C extension auto-registers http_verbs and config macros.
    # Load cross-extension macros from bundled SQL if deps are available.
    sql_dir = _HERE / "sql"
    if sql_dir.exists():
        for sql_file in sorted(sql_dir.glob("*.sql")):
            try:
                con.execute(sql_file.read_text())
            except Exception:
                # Macro may depend on an extension not yet loaded
                # (e.g., llm_adapt needs blobtemplates).  Skip silently —
                # the user can call setup() again after loading deps.
                pass
