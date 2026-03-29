#!/usr/bin/env python3
"""Append DuckDB extension metadata footer to a shared library."""
import platform
import shutil
import sys

def pad(s: str, n: int = 32) -> bytes:
    b = s.encode("ascii")[:n]
    return b + b"\x00" * (n - len(b))

def wasm_prefix() -> bytes:
    """WebAssembly custom section header required by DuckDB's metadata parser."""
    return (b"\x00"       # custom section id
            b"\x93\x04"   # section length (531 bytes)
            b"\x10"       # name length (16)
            b"duckdb_signature"
            b"\x80\x04")  # payload length (512)

def main():
    if len(sys.argv) < 3:
        print(f"usage: {sys.argv[0]} <input.so> <output.duckdb_extension>", file=sys.stderr)
        sys.exit(1)

    src, dst = sys.argv[1], sys.argv[2]

    machine = platform.machine()
    system = platform.system()
    if system == "Darwin":
        plat = "osx_arm64" if machine == "arm64" else "osx_amd64"
    elif system == "Linux":
        plat = "linux_arm64" if machine == "aarch64" else "linux_amd64"
    else:
        plat = "windows_amd64"

    shutil.copy2(src, dst)
    with open(dst, "ab") as f:
        f.write(wasm_prefix())
        # Fields written in reverse order (8 down to 1)
        f.write(pad(""))            # field8: reserved
        f.write(pad(""))            # field7: reserved
        f.write(pad(""))            # field6: reserved
        f.write(pad("C_STRUCT"))    # field5: abi type
        f.write(pad("v0.1.0"))      # field4: extension version
        f.write(pad("v1.2.0"))      # field3: duckdb version (stable API baseline)
        f.write(pad(plat))          # field2: platform
        f.write(pad("4"))           # field1: magic
        f.write(b"\x00" * 256)      # signature placeholder

if __name__ == "__main__":
    main()
