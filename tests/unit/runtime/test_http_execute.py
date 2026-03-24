from __future__ import annotations

from akc.runtime.http_execute import pattern_allows_url, redact_url_for_evidence, url_allowed_by_lists


def test_redact_url_strips_query_and_fragment() -> None:
    assert redact_url_for_evidence("https://api.example.com/v1/x?token=secret#frag") == "https://api.example.com/v1/x"


def test_pattern_url_prefix_does_not_prefix_match_sibling_host() -> None:
    assert pattern_allows_url("https://api.example.com", "https://api.example.com/v1")
    assert not pattern_allows_url("https://api.example.com", "https://api.example.com.evil/v1")


def test_url_allowed_by_lists_requires_bundle_and_respects_envelope() -> None:
    assert not url_allowed_by_lists("https://a.com/x", bundle_patterns=(), envelope_patterns=())
    assert url_allowed_by_lists(
        "https://a.com/x",
        bundle_patterns=("a.com",),
        envelope_patterns=(),
    )
    assert not url_allowed_by_lists(
        "https://a.com/x",
        bundle_patterns=("a.com",),
        envelope_patterns=("b.com",),
    )
    assert url_allowed_by_lists(
        "https://a.com/x",
        bundle_patterns=("a.com",),
        envelope_patterns=("https://a.com",),
    )
