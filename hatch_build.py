"""Bundle the artifacts `zig build` produced, tagged for the platform they are for.

A wheel is a zip plus metadata: the platform tag is a string, not something
derived from the binaries. So the wheel for any target can be produced on any
host — the usual reason to build wheels on the target platform is that the
*compiler* has to run there, and `zig build -Dtarget=...` removes that reason.

    zig build -Dtarget=x86_64-linux-gnu.2.17
    BLOB_WHEEL_PLATFORM=manylinux_2_17_x86_64 uv build --wheel

With no override the tag is inferred from the host, which is correct for the
ordinary native build.

The tag is `py3-none-<platform>`: platform-specific because it carries a native
library, but ABI-agnostic because ctypes binds at runtime rather than against
the CPython C API. That is the concrete win over the nanobind build, which
needed a separate wheel per interpreter version.

Which artifacts go in cannot be a plain `force-include` of `zig-out/lib`.
`zig build` does not clear that directory between targets, so after a
cross-build followed by a native one it holds both — and the names do not
separate them: `libbhttp.so` there may be a stale Linux core, while
`bhttp.so` is the SQLite extension and is genuinely correct on macOS
too. So each file is checked against the tag by its own magic number, which is
the only unambiguous signal. A macOS wheel shipping a Linux `.so` installs and
imports fine right up until the ctypes load, which is a bad place to find out.
"""

from __future__ import annotations

import os
import sysconfig
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

# The Python package directory, which is NOT the artifact name: the artifacts
# are bhttp.* (the SQL function prefix) while the importable package is
# blobhttp. Getting this wrong puts the libraries in a directory nothing looks
# in, and the wheel installs cleanly and fails at first import.
PACKAGE = "blobhttp"


def _host_platform_tag() -> str:
    return sysconfig.get_platform().replace("-", "_").replace(".", "_")


def _format_for(tag: str) -> str:
    if tag.startswith(("macosx", "darwin")):
        return "macho"
    if tag.startswith(("win", "cygwin")):
        return "pe"
    return "elf"  # linux, manylinux, musllinux, and the BSDs


def _format_of(path: Path) -> str | None:
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


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        # BLOB_WHEEL_PLATFORM is the family-wide override.
        platform = os.environ.get("BLOB_WHEEL_PLATFORM") or _host_platform_tag()
        build_data["pure_python"] = False
        build_data["infer_tag"] = False
        build_data["tag"] = f"py3-none-{platform}"

        want = _format_for(platform)
        lib = Path(self.root, "zig-out", "lib")
        included = []
        for path in sorted(lib.glob("*")):
            if path.is_file() and _format_of(path) == want:
                build_data["force_include"][str(path)] = f"{PACKAGE}/{path.name}"
                included.append(path.name)
        if not included:
            raise FileNotFoundError(
                f"no {want} artifacts in {lib} for a {platform} wheel — "
                f"run `zig build` for that target first"
            )
