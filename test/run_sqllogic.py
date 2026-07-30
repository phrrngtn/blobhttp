#!/usr/bin/env python3
"""Run the sqllogictest files in test/sql against a built bhttp extension.

The upstream runner this repo referenced — duckdb-sqllogictest, pinned to a git
commit — installs a dist-info and no importable module, so the .test files had
no way to run. This covers the directives those files actually use:

    require <ext>          load the extension, or skip the file
    statement ok           run it; fail if it raises
    statement error        run it; fail unless it raises, and unless the
                           expected text (after ----) appears in the message
    query <cols>           run it; compare the result rows against the
                           expected block, tab- or space-separated, with NULL
                           spelled NULL

It is deliberately not a general sqllogictest implementation. If a .test file
grows a directive that is not handled, this exits non-zero rather than skipping
it quietly.

usage: run_sqllogic.py <extension.duckdb_extension> <file.test>...
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

HANDLED = {"require", "statement", "query", "halt", "loop", "endloop"}


def parse(path: Path):
    """Yield (directive, argument, sql, expected) for each record in a file."""
    lines = path.read_text().splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip() or line.startswith("#"):
            i += 1
            continue
        head = line.split()
        directive, arg = head[0], (head[1] if len(head) > 1 else "")
        if directive not in HANDLED:
            raise SystemExit(f"{path}:{i + 1}: unhandled directive {directive!r}")
        i += 1

        sql = []
        while i < len(lines) and lines[i].strip() and lines[i] != "----":
            sql.append(lines[i])
            i += 1

        expected = None
        if i < len(lines) and lines[i] == "----":
            i += 1
            expected = []
            while i < len(lines) and lines[i].strip():
                expected.append(lines[i])
                i += 1

        yield directive, arg, "\n".join(sql), expected


def cell(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"  # sqllogictest spells them lowercase
    return str(v)


def run(ext_path: str, path: Path) -> tuple[int, int]:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    passed = failed = 0

    for directive, arg, sql, expected in parse(path):
        if directive == "require":
            con.execute(f"LOAD '{ext_path}'")
            continue

        if directive == "statement" and arg == "error":
            try:
                con.execute(sql)
            except Exception as e:
                want = "\n".join(expected or []).strip()
                if want and want not in str(e):
                    print(f"FAIL {path.name}: expected error {want!r}, got {e}")
                    failed += 1
                else:
                    passed += 1
            else:
                print(f"FAIL {path.name}: expected an error from\n{sql}")
                failed += 1
            continue

        if directive == "statement":
            try:
                con.execute(sql)
                passed += 1
            except Exception as e:
                print(f"FAIL {path.name}: {e}\n{sql}")
                failed += 1
            continue

        if directive == "query":
            try:
                rows = con.execute(sql).fetchall()
            except Exception as e:
                print(f"FAIL {path.name}: {e}\n{sql}")
                failed += 1
                continue
            got = ["\t".join(cell(c) for c in row) for row in rows]
            want = [ln.strip() for ln in (expected or [])]
            if [g.replace("\t", " ") for g in got] != [w.replace("\t", " ") for w in want]:
                print(f"FAIL {path.name}: expected {want}, got {got}\n{sql}")
                failed += 1
            else:
                passed += 1

    return passed, failed


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__.strip().splitlines()[-1])
        return 2
    ext_path = sys.argv[1]
    total_pass = total_fail = 0
    for f in sys.argv[2:]:
        p, f_ = run(ext_path, Path(f))
        print(f"{Path(f).name}: {p} passed, {f_} failed")
        total_pass += p
        total_fail += f_
    print(f"total: {total_pass} passed, {total_fail} failed")
    return 1 if total_fail else 0


if __name__ == "__main__":
    sys.exit(main())
