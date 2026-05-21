"""Smoke test for the simpl.E package skeleton."""

import simple_e


def test_package_imports_and_has_version() -> None:
    """The package imports cleanly and exposes a non-empty version string."""
    assert simple_e.__version__
