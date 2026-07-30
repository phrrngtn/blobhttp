"""Pick the wheel platform tag, and the artifacts that belong in that wheel.

Shared by ext/duckdb and ext/sqlite. In the other repos in the family these two
helpers live in the root hatch_build.py, because the root project builds a
wheel of its own. blobhttp has no root wheel yet — its Python bindings are
still nanobind over a C ABI that does not exist — so they live here instead.

A wheel is a zip plus metadata: the platform tag is a string, not something
derived from the binaries. So the wheel for any target can be produced on any
host, and the tag is `py3-none-<platform>` — platform-specific because it
carries a native library, ABI-agnostic because nothing binds to the CPython C
API.

`zig build` does not clear zig-out/lib between targets, so after a cross-build
it holds artifacts for both, and the names do not separate them: `bhttp.so`
there is the SQLite extension and is correct on macOS too. Hence the magic
number check — a macOS wheel shipping a Linux `.so` installs fine right up
until something tries to load it.
"""

from __future__ import annotations

import os
import sysconfig
from pathlib import Path


def wheel_platform() -> str:
    """The platform tag for this wheel; BLOB_WHEEL_PLATFORM overrides."""
    return (
        os.environ.get("BLOB_WHEEL_PLATFORM")
        or sysconfig.get_platform().replace("-", "_").replace(".", "_")
    )


def format_for(tag: str) -> str:
    """The binary format a wheel with this platform tag should carry."""
    if tag.startswith(("macosx", "darwin")):
        return "macho"
    if tag.startswith(("win", "cygwin")):
        return "pe"
    return "elf"  # linux, manylinux, musllinux, and the BSDs


def format_of(path: Path) -> str | None:
    """The binary format of one file, or None if it is not an object file."""
    with path.open("rb") as f:
        magic = f.read(4)
    if magic[:4] == b"\x7fELF":
        return "elf"
    # Mach-O thin (LE/BE, 32/64-bit) and universal.
    if magic in (b"\xcf\xfa\xed\xfe", b"\xce\xfa\xed\xfe",
                 b"\xfe\xed\xfa\xcf", b"\xfe\xed\xfa\xce",
                 b"\xca\xfe\xba\xbe", b"\xbe\xba\xfe\xca"):
        return "macho"
    if magic[:2] == b"MZ":
        return "pe"
    return None


def select(lib_dir: Path, names: tuple[str, ...], tag: str) -> list[str]:
    """Which of `names` in `lib_dir` are built for `tag`. Raises if none are."""
    want = format_for(tag)
    found = [n for n in names if (lib_dir / n).is_file() and format_of(lib_dir / n) == want]
    if not found:
        raise FileNotFoundError(
            f"no {want} artifact in {lib_dir} — "
            f"run `zig build` for that target at the repo root first"
        )
    return found
