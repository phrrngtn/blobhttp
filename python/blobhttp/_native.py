"""ctypes binding to libbhttp — the C ABI declared in include/blobhttp.h.

This replaces the nanobind extension module. The C ABI now exists and both
other hosts bind to it, so the Python layer has nothing to gain from a compiled
shim: ctypes over the same symbols removes nanobind, scikit-build-core, the
Python development headers, and the wheel-per-CPython matrix.

**No borrowed pointer ever escapes into Python.** Several ABI functions return
`const void *` into the batch, valid only until `bh_batch_free`. Every wrapper
here copies with `ctypes.string_at(ptr, len)` before the free and hands back
`bytes`, so nothing a caller holds can dangle. That is why `perform` below is
written as a single function that creates, performs, materialises and frees
rather than exposing the handle.
"""

from __future__ import annotations

import ctypes
import pathlib

import blobzig

__all__ = ["lib", "Error", "library_path", "errmsg", "take"]

_PKG = pathlib.Path(__file__).resolve().parent
_artifacts = blobzig.Artifacts("bhttp", package_dir=_PKG, repo_root=_PKG.parents[1])

# nanobind raised nb::value_error, which surfaced as ValueError; blobzig.Error
# is a ValueError, so existing callers and pytest.raises keep working.
Error = blobzig.Error


def library_path() -> str:
    """Path to libbhttp, the shared library behind this module."""
    return _artifacts.library()


def duckdb_extension_path() -> str:
    return _artifacts.duckdb_extension()


def sqlite_extension_path() -> str:
    return _artifacts.sqlite_extension()


lib = _artifacts.load()

_P = ctypes.c_void_p
_S = ctypes.c_char_p
_SZ = ctypes.c_size_t
_PSZ = ctypes.POINTER(ctypes.c_size_t)

BH_REQUEST_HEADERS = 0
BH_RESPONSE_HEADERS = 1

lib.bh_free.argtypes = [_P]
lib.bh_free.restype = None
lib.bh_errmsg.argtypes = []
lib.bh_errmsg.restype = _S  # borrowed, never freed by us

lib.bh_batch_new.argtypes = [_S]
lib.bh_batch_new.restype = _P
lib.bh_batch_free.argtypes = [_P]
lib.bh_batch_free.restype = None
lib.bh_batch_add.argtypes = [_P, _S, _S, _S, _S, _P, _SZ, _S, ctypes.c_int, ctypes.c_int]
lib.bh_batch_add.restype = ctypes.c_int
lib.bh_batch_perform.argtypes = [_P]
lib.bh_batch_perform.restype = ctypes.c_int
lib.bh_batch_count.argtypes = [_P]
lib.bh_batch_count.restype = _SZ

# Borrowed results: c_void_p so ctypes hands back the pointer rather than
# converting to bytes and losing the length, which matters because a body may
# contain NUL and its length is authoritative.
for _name in ("bh_result_request_url", "bh_result_request_method", "bh_result_request_body",
              "bh_result_status_line", "bh_result_body", "bh_result_response_url"):
    getattr(lib, _name).argtypes = [_P, _SZ, _PSZ]
    getattr(lib, _name).restype = _P

lib.bh_result_status.argtypes = [_P, _SZ]
lib.bh_result_status.restype = ctypes.c_int
lib.bh_result_elapsed.argtypes = [_P, _SZ]
lib.bh_result_elapsed.restype = ctypes.c_double
lib.bh_result_redirect_count.argtypes = [_P, _SZ]
lib.bh_result_redirect_count.restype = ctypes.c_int

lib.bh_result_header_count.argtypes = [_P, _SZ, ctypes.c_int]
lib.bh_result_header_count.restype = _SZ
for _name in ("bh_result_header_name", "bh_result_header_value"):
    getattr(lib, _name).argtypes = [_P, _SZ, ctypes.c_int, _SZ, _PSZ]
    getattr(lib, _name).restype = _P

# Malloc'd results — c_void_p, never c_char_p, or ctypes drops the pointer and
# every call leaks.
blobzig.returns_string(lib.bh_result_json, [_P, _SZ])
blobzig.returns_string(lib.bh_rate_limit_stats_json, [])
lib.bh_negotiate_available.argtypes = []
lib.bh_negotiate_available.restype = ctypes.c_int

blobzig.returns_string(lib.bh_negotiate_auth_header, [_S])
blobzig.returns_string(lib.bh_negotiate_auth_header_json, [_S])
blobzig.returns_string(lib.bh_sso_jwt, [_S])
blobzig.returns_string(lib.bh_llm_complete, [_S])
blobzig.returns_string(lib.bh_llm_adapt, [_S])


def errmsg() -> str:
    """The last error on this thread, read immediately after a failure."""
    msg = lib.bh_errmsg()
    return msg.decode("utf-8", "replace") if msg else "unknown error"


def take(ptr: int | None) -> str:
    """Decode a malloc'd result string and free it. Raises if the call failed."""
    return blobzig.take(lib, ptr, errmsg="bh_errmsg", free="bh_free")


def copy_bytes(ptr: int | None, n: int) -> bytes:
    """Copy a borrowed buffer out before its batch is freed."""
    if not ptr or n == 0:
        return b""
    return ctypes.string_at(ptr, n)
