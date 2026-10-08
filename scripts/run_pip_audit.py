"""Run ``pip-audit`` with the project's advisory suppressions, expiring stale ones.

``pip-audit`` has no config file — ``--ignore-vuln`` is CLI-only — so the suppression
list would otherwise be duplicated between the ``pip-audit`` prek hook and the CI step,
which already drifted once.
This script is the single source of truth both call sites invoke.

Every suppression names the blocker that forces it: a dependency pinned below the
version carrying the fix. The blocker is re-checked against ``uv.lock`` before each
audit and the script exits 1 once it no longer holds. Without that guard a suppression
outlives its justification indefinitely: an ``--ignore-vuln`` matching nothing is a
no-op for ``pip-audit``, whose only trace is a smaller count in its "N ignored" summary
line.

Usage::

    uv run python scripts/run_pip_audit.py
"""

import subprocess
import sys
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

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
    fixed_in
        First `blocker` release carrying the fix.
        The entry must go once ``uv.lock`` resolves that version or a later one.
    rationale
        Why the advisory is not exploitable here, printed on every run.
    """

    vuln_id: str
    blocker: str
    fixed_in: Version
    rationale: str


# No advisory is currently suppressed. Entries live here, never at a call site: a
# suppression is a claim about this project, and `find_stale` retires it automatically
# once `uv.lock` resolves its blocker at or past `fixed_in`.
_SUPPRESSIONS: tuple[Suppression, ...] = ()


# --------------------------------------------------------------------------------------
# Lockfile lookup
# --------------------------------------------------------------------------------------
# Resolved from this file rather than the working directory: both call sites happen to
# run from the repo root, but nothing enforces that.

_LOCK_PATH = Path(__file__).resolve().parents[1] / "uv.lock"


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
    stale
        Pairs of stale suppression and a human-readable explanation.
    """
    stale: list[tuple[Suppression, str]] = []

    for suppression in suppressions:
        version = locked.get(canonicalize_name(suppression.blocker))
        if version is None:
            stale.append((suppression, f"{suppression.blocker} is no longer in the lockfile"))
        elif version >= suppression.fixed_in:
            reason = f"{suppression.blocker} resolves to {version}, which carries the fix"
            stale.append((suppression, reason))

    return stale


# --------------------------------------------------------------------------------------
# pip-audit invocation
# --------------------------------------------------------------------------------------


def run_audit(vuln_ids: Sequence[str]) -> int:
    """Audit the active environment, ignoring `vuln_ids`.

    Runs ``pip-audit`` as a module of the current interpreter so the audited environment
    is always the one this script runs in.

    Returns
    -------
    exit_code
        The ``pip-audit`` exit code (0 when no unsuppressed vulnerability is found).
    """
    command = [sys.executable, "-m", "pip_audit"]
    for vuln_id in vuln_ids:
        command += ["--ignore-vuln", vuln_id]

    return subprocess.run(command, check=False).returncode


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


def main() -> int:
    """Entry point: expire stale suppressions, then run the audit."""
    stale = find_stale(_SUPPRESSIONS, locked=load_locked_versions(_LOCK_PATH))
    if stale:
        # stderr, not stdout: this is the failure diagnostic, while stdout carries the
        # rationale and the pip-audit report a successful run is read for.
        report = [
            "Stale suppression(s) — the blocker that justified them no longer holds:",
            *(f"  {suppression.vuln_id}: {reason}" for suppression, reason in stale),
            "",
            f"Drop the entry from _SUPPRESSIONS in {Path(__file__).name} and re-run.",
        ]
        print("\n".join(report), file=sys.stderr)
        return 1

    # flush: our stdout is block-buffered when piped (CI logs, prek output) while the
    # pip-audit subprocess writes to the same fd unbuffered — without this the
    # rationale lands after the audit report it is supposed to introduce.
    for suppression in _SUPPRESSIONS:
        print(f"Ignoring {suppression.vuln_id} — {suppression.rationale}", flush=True)

    return run_audit([s.vuln_id for s in _SUPPRESSIONS])


if __name__ == "__main__":
    raise SystemExit(main())
