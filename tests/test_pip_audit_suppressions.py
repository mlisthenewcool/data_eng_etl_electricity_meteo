"""Guard the single source of truth for pip-audit advisory suppressions.

``pip-audit`` has no config file, so before ``scripts/run_pip_audit.py`` existed the
``--ignore-vuln`` list was duplicated between ``prek.toml`` and
``.github/workflows/ci.yml`` and drifted between them: an advisory suppressed in one
place only makes the pre-push hook and CI disagree on a clean branch.

These tests fail if a call site starts passing its own suppressions again instead of
going through the wrapper.
"""

from pathlib import Path

import pytest

_ROOT_DIR = Path(__file__).resolve().parents[1]

# Every place that runs the dependency audit, and must do so via the wrapper.
_AUDIT_CALL_SITES = (
    Path("prek.toml"),
    Path(".github/workflows/ci.yml"),
)


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def _strip_comments(content: str) -> str:
    """Drop whole-line comments from TOML or YAML, which share the ``#`` prefix.

    Both call sites mention ``--ignore-vuln`` in prose to explain why the wrapper
    exists, so the flag must only be looked for in what actually runs.
    """
    return "\n".join(line for line in content.splitlines() if not line.lstrip().startswith("#"))


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("call_site", _AUDIT_CALL_SITES, ids=str)
def test_call_site_defines_no_suppression(call_site: Path) -> None:
    """No call site carries its own ``--ignore-vuln`` flags."""
    content = _strip_comments((_ROOT_DIR / call_site).read_text(encoding="utf-8"))

    assert "--ignore-vuln" not in content, (
        f"{call_site} passes --ignore-vuln directly. Suppressions belong in "
        f"_SUPPRESSIONS in scripts/run_pip_audit.py — duplicating them here is what "
        f"drifted before."
    )


@pytest.mark.parametrize("call_site", _AUDIT_CALL_SITES, ids=str)
def test_call_site_invokes_the_wrapper(call_site: Path) -> None:
    """Every call site runs the audit through ``scripts/run_pip_audit.py``."""
    content = (_ROOT_DIR / call_site).read_text(encoding="utf-8")

    assert "scripts/run_pip_audit.py" in content, (
        f"{call_site} no longer invokes scripts/run_pip_audit.py — the audit would "
        f"run without the project's suppressions and without the staleness check."
    )
