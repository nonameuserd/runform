"""Smoke test for the akc package."""

from akc import __version__


def test_version_is_string() -> None:
    """Package version is a non-empty string."""
    assert isinstance(__version__, str)
    assert len(__version__) > 0
