"""Bump ``>=X.Y.Z`` floors in ``pyproject.toml`` to match ``uv.lock``.

Used both as a manual tool (``uv run python scripts/sync_dep_floors.py``) and via the
``sync-dep-floors-check`` prek hook, which runs this script with ``--check`` in pre-push
to detect drift between declared floors and what's actually resolved in the lockfile.

Scope: any line matching ``"name>=ver"`` is rewritten if the locked version is higher.
In practice this pattern only appears in ``[project.dependencies]`` and
``[dependency-groups.*]``, which is enough for our needs — the script is *not*
TOML-section-aware. Lines using operators other than ``>=`` (``==``, ``~=``) are left
untouched: they express intent that should not be silently rewritten.
"""

import argparse
import difflib
import re
import sys
import tomllib
from pathlib import Path

from packaging.utils import canonicalize_name
from packaging.version import Version

# --------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------


# One dependency line inside a TOML array of strings. Captures every part of the
# spec so we can rebuild the line preserving extras, quoting, and trailing commas
# / inline comments. Only ``>=`` floors are matched on purpose (see module docstring).
_DEP_LINE_RE = re.compile(
    r'^(?P<indent>\s*)"'
    r"(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)"
    r"(?P<extras>\[[^\]]+\])?"
    r">="
    r"(?P<version>[A-Za-z0-9._+!-]+)"
    r'"(?P<trailing>,?\s*(?:#.*)?)$'
)


# --------------------------------------------------------------------------------------
# Lockfile parsing
# --------------------------------------------------------------------------------------


def _load_locked_versions(lock_path: Path) -> dict[str, Version]:
    """Return mapping of canonical package name → resolved ``uv.lock`` version."""
    data = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    versions: dict[str, Version] = {}
    for pkg in data.get("package", []):
        versions[canonicalize_name(pkg["name"])] = Version(pkg["version"])
    return versions


# --------------------------------------------------------------------------------------
# Pyproject rewriting
# --------------------------------------------------------------------------------------


def bump_floors(source: str, locked: dict[str, Version]) -> tuple[str, list[tuple[str, str, str]]]:
    """Rewrite ``source`` bumping each ``>=`` floor that lags ``locked``.

    Returns
    -------
    updated_source
        The new ``pyproject.toml`` content.
    bumps
        List of ``(name, old_floor, new_floor)`` tuples for every line touched.
    """
    bumps: list[tuple[str, str, str]] = []
    out_lines: list[str] = []

    for raw_line in source.splitlines(keepends=True):
        # Strip the line terminator before matching, restore it after.
        line = raw_line.rstrip("\r\n")
        terminator = raw_line[len(line) :]

        match = _DEP_LINE_RE.match(line)
        if match is None:
            out_lines.append(raw_line)
            continue

        canonical = canonicalize_name(match.group("name"))
        installed = locked.get(canonical)
        if installed is None:
            out_lines.append(raw_line)
            continue

        floor = Version(match.group("version"))
        if installed <= floor:
            out_lines.append(raw_line)
            continue

        new_line = (
            f'{match.group("indent")}"{match.group("name")}'
            f'{match.group("extras") or ""}>={installed}"'
            f"{match.group('trailing')}{terminator}"
        )
        out_lines.append(new_line)
        bumps.append((match.group("name"), str(floor), str(installed)))

    return "".join(out_lines), bumps


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


def main() -> int:
    """Entry point: parse args, sync floors, report results."""
    parser = argparse.ArgumentParser(
        description="Bump pyproject.toml dep floors to uv.lock resolved versions."
    )
    parser.add_argument(
        "--pyproject",
        type=Path,
        default=Path("pyproject.toml"),
        help="Path to pyproject.toml (default: ./pyproject.toml)",
    )
    parser.add_argument(
        "--lock",
        type=Path,
        default=Path("uv.lock"),
        help="Path to uv.lock (default: ./uv.lock)",
    )
    parser.add_argument("--check", action="store_true", help="Exit 1 if any floor would be bumped")
    parser.add_argument("--diff", action="store_true", help="Print unified diff to stdout")
    args = parser.parse_args()

    locked = _load_locked_versions(args.lock)
    original = args.pyproject.read_text(encoding="utf-8")
    updated, bumps = bump_floors(original, locked)

    if not bumps:
        print("All floors up to date")
        return 0

    for name, old, new in bumps:
        print(f"  {name}: {old} → {new}")

    if args.check:
        print(f"\n{len(bumps)} floor(s) would be bumped")
        return 1

    if args.diff:
        sys.stdout.writelines(
            difflib.unified_diff(
                original.splitlines(keepends=True),
                updated.splitlines(keepends=True),
                fromfile=str(args.pyproject),
                tofile=str(args.pyproject),
            )
        )
        return 0

    args.pyproject.write_text(updated, encoding="utf-8")
    print(f"\nBumped {len(bumps)} floor(s) in {args.pyproject}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
