"""Package the DuckDB extension built by `zig build` at the repo root.

There is no build step here. The artifacts are force-included from
`../../zig-out/lib` at wheel time, so this project never rebuilds what the root
build already produced — it used to carry its own CMakeLists and symlinks to do
exactly that.

Platform tagging and artifact selection live in tools/wheel_platform.py, shared
with the sibling ext project.
"""

from __future__ import annotations

import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from wheel_platform import select, wheel_platform  # noqa: E402

PACKAGE = "blobhttp_duckdb"
ARTIFACTS = ("bhttp.duckdb_extension",)


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        platform = wheel_platform()
        build_data["pure_python"] = False
        build_data["infer_tag"] = False
        build_data["tag"] = f"py3-none-{platform}"

        lib = Path(self.root, "..", "..", "zig-out", "lib").resolve()
        for name in select(lib, ARTIFACTS, platform):
            build_data["force_include"][str(lib / name)] = f"{PACKAGE}/{name}"
