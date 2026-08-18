"""Guard the single source of truth for pip-audit advisory suppressions.

``pip-audit`` has no config file, so before ``scripts/run_pip_audit.py`` existed the
``--ignore-vuln`` list was duplicated between ``prek.toml`` and
``.github/workflows/ci.yml`` and drifted between them: an advisory suppressed in one
place only makes the pre-push hook and CI disagree on a clean branch.

Both configs are parsed rather than grepped, so the assertions see the commands that
actually run — a wrapper named only in a prose comment no longer satisfies them.
"""

import tomllib
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from packaging.version import Version

from scripts.run_pip_audit import (
    _LOCK_PATH,
    _SUPPRESSIONS,
    Suppression,
    find_stale,
    load_locked_versions,
)

_ROOT_DIR = Path(__file__).resolve().parents[1]

_WRAPPER = "scripts/run_pip_audit.py"

# Any of these tokens in an executed command means a call site audits dependencies on
# its own terms instead of going through the wrapper — the duplication that drifted
# before. `pip_audit` is listed because `python -m pip_audit` is the underscore spelling
# of the same entry point; markers are matched against the command with the wrapper path
# stripped, so the wrapper's own filename never registers as a bypass.
_DIRECT_AUDIT_MARKERS = ("pip-audit", "pip_audit", "--ignore-vuln")


# --------------------------------------------------------------------------------------
# Executed commands, per call site
# --------------------------------------------------------------------------------------


def _prek_commands() -> list[str]:
    """Return the full command of every local prek hook: ``entry`` plus its ``args``.

    ``args`` is appended to ``entry`` by prek, so flags hidden there run just the same —
    the repo already splits a hook that way (``ruff-check``).
    """
    config = tomllib.loads((_ROOT_DIR / "prek.toml").read_text(encoding="utf-8"))

    return [
        " ".join([hook["entry"], *hook.get("args", [])])
        for repo in config["repos"]
        for hook in repo.get("hooks", [])
        if "entry" in hook
    ]


def _workflow_commands() -> list[str]:
    """Return the ``run:`` command of every step of every GitHub Actions workflow.

    Globbed rather than named: a workflow added later must not be able to audit
    dependencies on its own terms without tripping these tests.
    """
    commands: list[str] = []

    for workflow_path in sorted((_ROOT_DIR / ".github/workflows").glob("*.y*ml")):
        workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
        commands += [
            step["run"]
            for job in workflow["jobs"].values()
            for step in job.get("steps", [])
            if "run" in step
        ]

    return commands


_AUDIT_CALL_SITES = {
    "prek.toml": _prek_commands,
    ".github/workflows": _workflow_commands,
}


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("call_site", _AUDIT_CALL_SITES)
def test_call_site_defines_no_suppression(call_site: str) -> None:
    """No call site runs ``pip-audit`` itself, so none can carry its own ignore list."""
    offenders = [
        command
        for command in _AUDIT_CALL_SITES[call_site]()
        if any(marker in command.replace(_WRAPPER, "") for marker in _DIRECT_AUDIT_MARKERS)
    ]

    assert not offenders, (
        f"{call_site} audits dependencies without the wrapper: {offenders}. "
        f"Suppressions belong in _SUPPRESSIONS in {_WRAPPER} — duplicating them here "
        "is what drifted before."
    )


@pytest.mark.parametrize("call_site", _AUDIT_CALL_SITES)
def test_call_site_invokes_the_wrapper(call_site: str) -> None:
    """Every call site runs the audit through ``scripts/run_pip_audit.py``."""
    commands = _AUDIT_CALL_SITES[call_site]()

    assert any(_WRAPPER in command for command in commands), (
        f"{call_site} no longer invokes {_WRAPPER} — the audit would run without the "
        "project's suppressions and without the staleness check."
    )


# --------------------------------------------------------------------------------------
# Staleness logic
# --------------------------------------------------------------------------------------
# The call-site tests above guard the plumbing; these guard the decision that plumbing
# exists to deliver. `find_stale` is what stops a suppression from outliving the blocker
# that justified it, and an audit only ever exercises the "nothing is stale" branch.


_BLOCKER = "sqlparse"

_ENTRY = Suppression(
    vuln_id="CVE-0000-00000",
    blocker=_BLOCKER,
    fixed_in=Version("0.6.0"),
    rationale="test fixture",
)


class TestFindStale:
    @pytest.mark.parametrize(
        argnames="resolved, is_stale",
        argvalues=[
            ("0.5.5", False),  # the version the suppression was written against
            ("0.5.99", False),  # moved, but still short of the fix
            ("0.6.0rc1", False),  # a pre-release does not carry the released fix
            ("0.6.0", True),  # exactly the release carrying the fix
            ("0.6.1", True),  # past it
            ("1.0.0", True),  # a major bump must not read as "older"
        ],
    )
    def test_expires_once_the_blocker_reaches_fixed_in(self, resolved: str, is_stale: bool) -> None:
        stale = find_stale([_ENTRY], locked={_BLOCKER: Version(resolved)})

        assert bool(stale) is is_stale

    def test_expires_when_the_blocker_leaves_the_lockfile(self) -> None:
        stale = find_stale([_ENTRY], locked={})

        assert [suppression.vuln_id for suppression, _ in stale] == [_ENTRY.vuln_id]

    def test_matches_the_blocker_on_its_canonical_name(self) -> None:
        """A blocker spelled `Foo_Bar` must still find `foo-bar` in the lockfile."""
        entry = replace(_ENTRY, blocker="Pymdown_Extensions", fixed_in=Version("11.0.0"))

        stale = find_stale([entry], locked={"pymdown-extensions": Version("11.0.1")})

        assert len(stale) == 1


def test_declared_suppressions_are_still_justified() -> None:
    """Run the shipped list against the real lockfile, offline.

    The prek hook and CI already fail on a stale suppression, but only after
    ``pip-audit`` has reached the network. This is the offline half of the same guard: a
    dependency bump that retires a suppression now fails ``pytest`` too.
    """
    stale = find_stale(_SUPPRESSIONS, locked=load_locked_versions(_LOCK_PATH))

    assert not stale, "Stale suppression(s): " + "; ".join(
        f"{suppression.vuln_id} — {reason}" for suppression, reason in stale
    )
