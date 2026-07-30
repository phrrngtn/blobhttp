"""blobhttp — enterprise HTTP client with rate limiting, SPNEGO auth, and connection pooling.

Bindings are ctypes over the C ABI in `include/blobhttp.h` — the same symbols
the DuckDB and SQLite extensions call, so all three hosts share one
implementation of config resolution, Vault lookup, rate limiting and the LLM
completion loop.

Results are plain Python: every response is a dict with a `response_body` str
and a `response_blob` bytes, mirroring the twelve fields of DuckDB's STRUCT.
No ctypes pointer is ever handed to a caller — see `_native`.
"""

from __future__ import annotations

import ctypes
import json
from typing import Iterable, Mapping, Sequence

from . import _native
from ._native import (
    BH_REQUEST_HEADERS,
    BH_RESPONSE_HEADERS,
    Error,
    duckdb_extension_path,
    library_path,
    sqlite_extension_path,
    take,
)

__version__ = "0.2.2"

lib = _native.lib


def _b(s) -> bytes | None:
    if s is None:
        return None
    return s if isinstance(s, bytes) else str(s).encode("utf-8")


def _json_arg(value) -> bytes | None:
    """Headers and config may be given as a dict or as a JSON string."""
    if value is None:
        return None
    if isinstance(value, (str, bytes)):
        return _b(value)
    return json.dumps(value).encode("utf-8")


def _headers(batch, i: int, which: int) -> dict:
    out = {}
    for k in range(lib.bh_result_header_count(batch, i, which)):
        nlen, vlen = ctypes.c_size_t(), ctypes.c_size_t()
        name = lib.bh_result_header_name(batch, i, which, k, ctypes.byref(nlen))
        value = lib.bh_result_header_value(batch, i, which, k, ctypes.byref(vlen))
        out[_native.copy_bytes(name, nlen.value).decode("utf-8", "replace")] = \
            _native.copy_bytes(value, vlen.value).decode("utf-8", "replace")
    return out


def _materialise(batch, i: int) -> dict:
    """Copy one result out of the batch into plain Python objects.

    Everything is copied here, before the caller ever sees it, because the
    accessors return pointers that die with the batch.
    """
    def text(fn) -> str:
        n = ctypes.c_size_t()
        ptr = fn(batch, i, ctypes.byref(n))
        return _native.copy_bytes(ptr, n.value).decode("utf-8", "replace")

    n = ctypes.c_size_t()
    body = _native.copy_bytes(lib.bh_result_body(batch, i, ctypes.byref(n)), n.value)

    return {
        "request_url": text(lib.bh_result_request_url),
        "request_method": text(lib.bh_result_request_method),
        "request_headers": _headers(batch, i, BH_REQUEST_HEADERS),
        "request_body": text(lib.bh_result_request_body),
        "response_status_code": lib.bh_result_status(batch, i),
        "response_status": text(lib.bh_result_status_line),
        "response_headers": _headers(batch, i, BH_RESPONSE_HEADERS),
        # Both, as DuckDB does: the str is convenient, the bytes are correct.
        # A non-UTF-8 body makes the str lossy and leaves the blob intact.
        "response_body": body.decode("utf-8", "replace"),
        "response_blob": body,
        "response_url": text(lib.bh_result_response_url),
        "elapsed": lib.bh_result_elapsed(batch, i),
        "redirect_count": lib.bh_result_redirect_count(batch, i),
    }


def request_many(requests: Sequence[Mapping], config=None) -> list[dict]:
    """Perform several requests together, fanning out as the config allows.

    Each mapping takes method, url, and optionally headers, params, body,
    content_type. This is the shape the ABI is built around — DuckDB uses it
    for a whole chunk at a time — and it is worth reaching for directly when
    fetching many URLs, rather than looping over `request`.
    """
    batch = lib.bh_batch_new(_json_arg(config) or b"{}")
    if not batch:
        raise Error(_native.errmsg())
    try:
        for r in requests:
            body = r.get("body")
            body_bytes = _b(body) or b""
            rc = lib.bh_batch_add(
                batch,
                _b(r["method"]), _b(r["url"]),
                _json_arg(r.get("headers")), _json_arg(r.get("params")),
                body_bytes or None, len(body_bytes),
                _b(r.get("content_type")),
                -1, -1,
            )
            if rc != 0:
                raise Error(_native.errmsg())
        if lib.bh_batch_perform(batch) != 0:
            raise Error(_native.errmsg())
        return [_materialise(batch, i) for i in range(lib.bh_batch_count(batch))]
    finally:
        lib.bh_batch_free(batch)


class HttpClient:
    """Scoped configuration plus request methods.

    The config lives on the instance and is passed to the core per batch, which
    is the same path the SQL layer takes through the bh_http_config variable.
    """

    def __init__(self) -> None:
        self._config: dict[str, str] = {}

    def config_set(self, scope: str, config_json: str) -> None:
        self._config[scope] = config_json

    def config_remove(self, scope: str) -> None:
        self._config.pop(scope, None)

    def config_get(self, scope: str) -> str | None:
        return self._config.get(scope)

    def request(self, method: str, url: str, headers=None, body=None,
                content_type=None, params=None) -> dict:
        return request_many(
            [{"method": method, "url": url, "headers": headers, "body": body,
              "content_type": content_type, "params": params}],
            config=self._config,
        )[0]

    def get(self, url: str, headers=None, params=None) -> dict:
        return self.request("GET", url, headers=headers, params=params)

    def post(self, url: str, body=None, headers=None, content_type=None) -> dict:
        return self.request("POST", url, headers=headers, body=body,
                            content_type=content_type)

    def rate_limit_stats(self) -> list:
        return json.loads(take(lib.bh_rate_limit_stats_json()))


# ── LLM ─────────────────────────────────────────────────────────────


def llm_complete(url: str, body, headers=None, http_config=None,
                 output_schema: str = "", max_continuations: int = 10,
                 max_retries: int = 3) -> dict:
    """Chat completion, continuing on truncation and retrying on schema failure.

    Returns {"content": str, "stats": {...}}. Previously DuckDB-only.
    """
    request = {
        "url": url,
        "body": body if isinstance(body, (dict, list)) else json.loads(body),
        "headers": headers or {},
        "http_config": http_config or {},
        "output_schema": output_schema,
        "max_continuations": max_continuations,
        "max_retries": max_retries,
    }
    return json.loads(take(lib.bh_llm_complete(json.dumps(request).encode("utf-8"))))


def llm_adapt(config) -> dict:
    """Run a pre-rendered adapter prompt. Returns {"data": ..., "_meta": {...}}."""
    payload = config if isinstance(config, (str, bytes)) else json.dumps(config)
    return json.loads(take(lib.bh_llm_adapt(_b(payload))))


# ── Negotiate ───────────────────────────────────────────────────────


def negotiate_token(url: str) -> str:
    """Pre-flight SPNEGO/Negotiate header for a URL ("Negotiate <base64>")."""
    return take(lib.bh_negotiate_auth_header(_b(url)))


def negotiate_info(url: str) -> dict:
    """As negotiate_token, with the SPN, provider and library for diagnostics."""
    return json.loads(take(lib.bh_negotiate_auth_header_json(_b(url))))


def negotiate_available() -> bool:
    """Whether a security provider (GSS-API or SSPI) is usable here.

    GSS-API is dlopen'd, so this is a runtime question rather than a build one —
    and it is deliberately independent of whether the host's libcurl was built
    --with-gssapi.
    """
    try:
        negotiate_token("https://example.com")
        return True
    except Error as e:
        return "not available" not in str(e).lower()


__all__ = [
    "Error",
    "HttpClient",
    "request_many",
    "llm_complete",
    "llm_adapt",
    "negotiate_token",
    "negotiate_info",
    "negotiate_available",
    "library_path",
    "duckdb_extension_path",
    "sqlite_extension_path",
]
