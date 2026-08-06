# CrossWatch test scripts
from __future__ import annotations

import pytest

from cw_platform.anime_mapping.coordinates import (
    covers,
    parse_source,
    parse_target,
    translate,
)


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("1-12", (1, 12)),
        ("5", (5, 5)),
        ("14-", (14, None)),
        ("12-3", None),
        ("0", None),
        ("", None),
        ("abc", None),
        ("1-12x", None),
    ],
)
def test_parse_source(spec: str, expected: tuple[int, int | None] | None) -> None:
    assert parse_source(spec) == expected


def test_parse_target_rejects_ratio_edges() -> None:
    assert parse_target("14-|2") is None
    assert parse_target("1-6,8-13") == [(1, 6), (8, 13)]


def test_translate_offsets_within_span() -> None:
    assert translate("1-35", "40-74", 1) == 40
    assert translate("1-35", "40-74", 13) == 52
    assert translate("40-74", "1-35", 52) == 13


def test_translate_rejects_out_of_range() -> None:
    assert translate("1-35", "40-74", 36) is None
    assert translate("1-35", "40-74", 0) is None


def test_translate_walks_non_contiguous_targets() -> None:
    assert translate("1-12", "1-6,8-13", 6) == 6
    assert translate("1-12", "1-6,8-13", 7) == 8


def test_translate_supports_open_ended_ranges() -> None:
    assert translate("14-", "13-", 14) == 13
    assert translate("14-", "13-", 20) == 19


def test_translate_refuses_ratio_edges() -> None:
    assert translate("13-", "14-|2", 13) is None


def test_covers() -> None:
    assert covers("1-35", 13) is True
    assert covers("1-35", 40) is False
    assert covers("14-", 999) is True
