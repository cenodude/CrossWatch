from __future__ import annotations

from typing import Any

import pytest

from cw_platform.orchestrator._pairs_utils import (
    config_with_pair_libraries,
    pair_feature_libraries,
)


def _cfg() -> dict[str, Any]:
    return {
        "plex": {"server_url": "http://pms", "history": {"libraries": ["2", "3", "4", "5", "8", "9"]}},
        "emby": {"server": "http://emby", "history": {"libraries": []}},
    }


def test_pair_libraries_narrow_the_provider_scope() -> None:
    cfg = _cfg()
    fcfg = {"libraries": {"PLEX": ["3"]}}

    out = config_with_pair_libraries(cfg, fcfg, "history", ("EMBY", "PLEX"))

    assert out["plex"]["history"]["libraries"] == ["3"]


def test_original_config_is_not_mutated() -> None:
    cfg = _cfg()
    fcfg = {"libraries": {"PLEX": ["3"]}}

    config_with_pair_libraries(cfg, fcfg, "history", ("EMBY", "PLEX"))

    assert cfg["plex"]["history"]["libraries"] == ["2", "3", "4", "5", "8", "9"]


def test_other_provider_settings_survive() -> None:
    cfg = _cfg()
    fcfg = {"libraries": {"PLEX": ["3"]}}

    out = config_with_pair_libraries(cfg, fcfg, "history", ("EMBY", "PLEX"))

    assert out["plex"]["server_url"] == "http://pms"
    assert out["emby"]["server"] == "http://emby"


def test_both_sides_are_narrowed() -> None:
    cfg = _cfg()
    fcfg = {"libraries": {"PLEX": ["3"], "EMBY": ["4255"]}}

    out = config_with_pair_libraries(cfg, fcfg, "history", ("EMBY", "PLEX"))

    assert out["plex"]["history"]["libraries"] == ["3"]
    assert out["emby"]["history"]["libraries"] == ["4255"]


def test_empty_pair_selection_falls_back_to_connection_scope() -> None:
    cfg = _cfg()
    fcfg = {"libraries": {"PLEX": []}}

    out = config_with_pair_libraries(cfg, fcfg, "history", ("EMBY", "PLEX"))

    assert out["plex"]["history"]["libraries"] == ["2", "3", "4", "5", "8", "9"]


@pytest.mark.parametrize("feature", ["watchlist", "playlists"])
def test_features_without_library_scope_are_untouched(feature) -> None:
    cfg = _cfg()
    fcfg = {"libraries": {"PLEX": ["3"]}}

    out = config_with_pair_libraries(cfg, fcfg, feature, ("EMBY", "PLEX"))

    assert out["plex"]["history"]["libraries"] == ["2", "3", "4", "5", "8", "9"]


@pytest.mark.parametrize("feature", ["history", "ratings", "progress"])
def test_all_library_scoped_features_are_handled(feature) -> None:
    cfg = {"plex": {feature: {"libraries": ["1", "2"]}}}
    fcfg = {"libraries": {"PLEX": ["2"]}}

    out = config_with_pair_libraries(cfg, fcfg, feature, ("PLEX",))

    assert out["plex"][feature]["libraries"] == ["2"]


def test_non_library_providers_are_ignored() -> None:
    cfg = {"trakt": {"history": {}}, "plex": {"history": {"libraries": ["1"]}}}
    fcfg = {"libraries": {"TRAKT": ["x"], "PLEX": ["1"]}}

    out = config_with_pair_libraries(cfg, fcfg, "history", ("TRAKT", "PLEX"))

    assert "libraries" not in out["trakt"]["history"]


def test_kodi_path_style_libraries_pass_through() -> None:
    cfg = {"kodi": {"history": {"libraries": []}}}
    fcfg = {"libraries": {"KODI": ["/media/movies/", "/media/tv/"]}}

    out = config_with_pair_libraries(cfg, fcfg, "history", ("KODI",))

    assert out["kodi"]["history"]["libraries"] == ["/media/movies/", "/media/tv/"]


def test_missing_provider_block_is_created() -> None:
    out = config_with_pair_libraries({}, {"libraries": {"PLEX": ["3"]}}, "history", ("PLEX",))

    assert out["plex"]["history"]["libraries"] == ["3"]


@pytest.mark.parametrize("fcfg", [None, {}, {"libraries": None}, {"libraries": {"PLEX": "3"}}])
def test_malformed_pair_config_is_ignored(fcfg) -> None:
    cfg = _cfg()

    out = config_with_pair_libraries(cfg, fcfg, "history", ("PLEX",))

    assert out["plex"]["history"]["libraries"] == ["2", "3", "4", "5", "8", "9"]


def test_pair_feature_libraries_accepts_either_case() -> None:
    assert pair_feature_libraries({"libraries": {"PLEX": ["3"]}}, "plex") == ["3"]
    assert pair_feature_libraries({"libraries": {"plex": ["3"]}}, "PLEX") == ["3"]
    assert pair_feature_libraries({"libraries": {"PLEX": [" 3 ", ""]}}, "PLEX") == ["3"]
