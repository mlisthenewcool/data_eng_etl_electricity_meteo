"""Run ``pip-audit`` with the project's advisory suppressions, expiring stale ones.

``pip-audit`` has no config file — ``--ignore-vuln`` is CLI-only — so the suppression
list would otherwise be duplicated between the ``pip-audit`` prek hook and the CI step,
which already drifted once.
This script is the single source of truth both call sites invoke.

Every suppression names the blocker that forces it: a dependency pinned below the
version carrying the fix. The blocker is re-checked against ``uv.lock`` before each
audit and the script exits 1 once it no longer holds — ``pip-audit`` reports an ignore
that matched nothing as a silent "0 ignored", so without this guard a suppression
outlives its justification indefinitely.

Usage::

    uv run python scripts/run_pip_audit.py
    uv run python scripts/run_pip_audit.py -- --desc   # extra pip-audit flags
"""

import argparse
import subprocess
import sys
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import Version

# --------------------------------------------------------------------------------------
# Suppression list
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Suppression:
    """One ``--ignore-vuln`` advisory, tied to the blocker that justifies it.

    Attributes
    ----------
    vuln_id
        Advisory identifier passed to ``pip-audit --ignore-vuln``.
    blocker
        Dependency whose resolved version decides whether the suppression still applies.
    applies_while
        Range of `blocker` versions for which the vulnerable code is unavoidable.
        Anything outside it means the fix is reachable and the entry must go.
    rationale
        Why the advisory is not exploitable here, printed on every run.
    """

    vuln_id: str
    blocker: str
    applies_while: SpecifierSet
    rationale: str


_SUPPRESSIONS: tuple[Suppression, ...] = (
    Suppression(
        vuln_id="GHSA-9xwg-3r6f-jcx2",
        blocker="pymdown-extensions",
        applies_while=SpecifierSet("<11.0.0"),
        rationale=(
            "CVE-2026-61632, path traversal in the `b64` extension, fixed in 11.0.0. "
            "marimo (dev group) caps `pymdown-extensions<11`. Not exploitable here: "
            "marimo only registers `pymdownx.b64` under Pyodide/WASM "
            "(marimo/_output/md.py, guarded by `is_pyodide()`), and we run marimo "
            "natively as a dev-only dependency — the vulnerable extension is never "
            "loaded."
        ),
    ),
)


# --------------------------------------------------------------------------------------
# Lockfile lookup
# --------------------------------------------------------------------------------------


def load_locked_versions(lock_path: Path) -> dict[str, Version]:
    """Return mapping of canonical package name → version resolved in ``uv.lock``."""
    data = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    return {canonicalize_name(p["name"]): Version(p["version"]) for p in data.get("package", [])}


# --------------------------------------------------------------------------------------
# Suppression expiry
# --------------------------------------------------------------------------------------


def find_stale(
    suppressions: Sequence[Suppression], locked: dict[str, Version]
) -> list[tuple[Suppression, str]]:
    """Return the suppressions whose blocker no longer holds, each with its reason.

    Parameters
    ----------
    suppressions
        Entries to re-check.
    locked
        Canonical package name → resolved version, as returned by
        `load_locked_versions`.

    Returns
    -------
    list[tuple[Suppression, str]]
        Pairs of stale suppression and a human-readable explanation.
    """
    stale: list[tuple[Suppression, str]] = []

    for suppression in suppressions:
        version = locked.get(canonicalize_name(suppression.blocker))
        if version is None:
            stale.append((suppression, f"{suppression.blocker} is no longer in the lockfile"))
        elif version not in suppression.applies_while:
            reason = (
                f"{suppression.blocker} resolves to {version}, outside {suppression.applies_while}"
            )
            stale.append((suppression, reason))

    return stale


# --------------------------------------------------------------------------------------
# pip-audit invocation
# --------------------------------------------------------------------------------------


def run_audit(vuln_ids: Sequence[str], *, extra_args: Sequence[str]) -> int:
    """Audit the active environment, ignoring `vuln_ids`.

    Runs ``pip-audit`` as a module of the current interpreter so the audited environment
    is always the one this script runs in.

    Returns
    -------
    int
        The ``pip-audit`` exit code (0 when no unsuppressed vulnerability is found).
    """
    command = [sys.executable, "-m", "pip_audit"]
    for vuln_id in vuln_ids:
        command += ["--ignore-vuln", vuln_id]
    command += extra_args

    return subprocess.run(command, check=False).returncode


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


def main() -> int:
    """Entry point: expire stale suppressions, then run the audit."""
    parser = argparse.ArgumentParser(
        description="Run pip-audit with the project's advisory suppressions."
    )
    parser.add_argument(
        "--lock",
        type=Path,
        default=Path("uv.lock"),
        help="Path to uv.lock (default: ./uv.lock)",
    )
    parser.add_argument(
        "pip_audit_args",
        nargs="*",
        help="Extra arguments forwarded to pip-audit (prefix the list with `--`)",
    )
    args = parser.parse_args()

    stale = find_stale(_SUPPRESSIONS, load_locked_versions(args.lock))
    if stale:
        print("Stale suppression(s) — the blocker that justified them is gone:")
        for suppression, reason in stale:
            print(f"  {suppression.vuln_id}: {reason}")
        print(f"\nDrop the entry from _SUPPRESSIONS in {Path(__file__).name} and re-run.")
        return 1

    # flush: our stdout is block-buffered when piped (CI logs, prek output) while the
    # pip-audit subprocess writes to the same fd unbuffered — without this the
    # rationale lands after the audit report it is supposed to introduce.
    for suppression in _SUPPRESSIONS:
        print(f"Ignoring {suppression.vuln_id} — {suppression.rationale}", flush=True)

    return run_audit([s.vuln_id for s in _SUPPRESSIONS], extra_args=args.pip_audit_args)


if __name__ == "__main__":
    raise SystemExit(main())
