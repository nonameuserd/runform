from __future__ import annotations

import pytest

from akc.adopt.trust_ladder import (
    AdoptionLevel,
    AdvisorEvidence,
    adoption_level_index,
    parse_adoption_level,
    recommended_compile_realization_mode,
)

# ---------------------------------------------------------------------------
# parse_adoption_level: string tokens
# ---------------------------------------------------------------------------


class TestParseAdoptionLevelStringTokens:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("observer", "observer"),
            ("advisor", "advisor"),
            ("copilot", "copilot"),
            ("compiler", "compiler"),
            ("autonomy", "autonomy"),
            # Aliases
            ("read_only", "observer"),
            ("readonly", "observer"),
            ("read-only", "observer"),
            ("artifact_only", "advisor"),
            ("artifact-only", "advisor"),
            ("co_pilot", "copilot"),
            ("co-pilot", "copilot"),
            ("scoped_apply", "copilot"),
            ("scoped-apply", "copilot"),
            ("full_autonomy", "autonomy"),
            ("full-autonomy", "autonomy"),
            # Case insensitivity
            ("Observer", "observer"),
            ("ADVISOR", "advisor"),
            ("CoPilot", "copilot"),
        ],
    )
    def test_valid_strings(self, raw: str, expected: AdoptionLevel) -> None:
        assert parse_adoption_level(raw) == expected


class TestParseAdoptionLevelNumeric:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            (0, "observer"),
            (1, "advisor"),
            (2, "copilot"),
            (3, "compiler"),
            (4, "autonomy"),
            # String-encoded numbers
            ("0", "observer"),
            ("1", "advisor"),
            ("2", "copilot"),
            ("3", "compiler"),
            ("4", "autonomy"),
            # Level-prefixed
            ("level0", "observer"),
            ("l0", "observer"),
            ("level4", "autonomy"),
            ("l4", "autonomy"),
        ],
    )
    def test_valid_numeric(self, raw: str | int, expected: AdoptionLevel) -> None:
        assert parse_adoption_level(raw) == expected


class TestParseAdoptionLevelInvalid:
    @pytest.mark.parametrize(
        "raw",
        [
            None,
            "",
            "  ",
            "bogus",
            5,
            -1,
            True,
            False,
        ],
    )
    def test_returns_none(self, raw: str | int | None | bool) -> None:
        assert parse_adoption_level(raw) is None


# ---------------------------------------------------------------------------
# adoption_level_index
# ---------------------------------------------------------------------------


class TestAdoptionLevelIndex:
    @pytest.mark.parametrize(
        "level,expected",
        [
            ("observer", 0),
            ("advisor", 1),
            ("copilot", 2),
            ("compiler", 3),
            ("autonomy", 4),
        ],
    )
    def test_index(self, level: AdoptionLevel, expected: int) -> None:
        assert adoption_level_index(level) == expected

    def test_monotonic(self) -> None:
        levels: list[AdoptionLevel] = ["observer", "advisor", "copilot", "compiler", "autonomy"]
        indices = [adoption_level_index(lvl) for lvl in levels]
        assert indices == sorted(indices)
        assert len(set(indices)) == len(indices)  # all unique


# ---------------------------------------------------------------------------
# recommended_compile_realization_mode
# ---------------------------------------------------------------------------


class TestRecommendedRealizationMode:
    @pytest.mark.parametrize(
        "level,expected",
        [
            ("observer", "artifact_only"),
            ("advisor", "artifact_only"),
            ("copilot", "scoped_apply"),
            ("compiler", "scoped_apply"),
            ("autonomy", "scoped_apply"),
        ],
    )
    def test_mode(self, level: AdoptionLevel, expected: str) -> None:
        assert recommended_compile_realization_mode(level) == expected


# ---------------------------------------------------------------------------
# AdvisorEvidence dataclass
# ---------------------------------------------------------------------------


class TestAdvisorEvidence:
    def test_construction_defaults(self) -> None:
        ev = AdvisorEvidence(run_id="run-1", manifest_relpath="out/manifest.json")
        assert ev.run_id == "run-1"
        assert ev.manifest_relpath == "out/manifest.json"
        assert ev.approved is None

    def test_construction_approved(self) -> None:
        ev = AdvisorEvidence(run_id="run-2", manifest_relpath="out/m.json", approved=True)
        assert ev.approved is True

    def test_frozen(self) -> None:
        ev = AdvisorEvidence(run_id="run-1", manifest_relpath="m.json")
        with pytest.raises(AttributeError):
            ev.run_id = "x"  # type: ignore[misc]
