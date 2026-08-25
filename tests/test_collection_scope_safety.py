from __future__ import annotations

from cw_platform.orchestrator import _pairs_oneway as oneway


def _mixed_index() -> dict[str, dict]:
    return {
        "tmdb:1": {"type": "movie", "title": "Movie"},
        "tmdb:2": {"type": "show", "title": "Show"},
        "tmdb:2#season:1": {"type": "season", "title": "Show", "season": 1},
        "tmdb:2#s01e01": {"type": "episode", "title": "Show", "season": 1, "episode": 1},
    }


def test_collection_media_filter_defaults_to_movies() -> None:
    assert set(oneway._media_type_filter_index(_mixed_index(), {})) == {"tmdb:1"}


def test_collection_media_filter_accepts_explicit_all() -> None:
    assert set(oneway._media_type_filter_index(_mixed_index(), {"types": ["all"]})) == set(_mixed_index())


def test_collection_media_filter_accepts_string_type() -> None:
    assert set(oneway._media_type_filter_index(_mixed_index(), {"types": "episodes"})) == {"tmdb:2#s01e01"}


def test_one_way_collection_is_library_scoped() -> None:
    cfg = {"plex": {"collection": {"libraries": ["1", "2"]}}}

    assert oneway._effective_library_whitelist(cfg, "PLEX", "collection", {}) == ["1", "2"]


def test_previous_collection_baseline_without_library_id_survives_pair_scope() -> None:
    prev = {"tmdb:1": {"type": "movie", "ids": {"tmdb": "1"}}}

    kept = oneway._filter_index_by_libraries(prev, ["19"], allow_unknown=True)

    assert kept == prev


def test_collection_state_store_round_trips_non_default_instance(tmp_path) -> None:
    from cw_platform.orchestrator._state_store import StateStore

    store = StateStore(tmp_path)
    store.save_feature_blocks(
        {
            ("PLEX", "PLEX-P01", "collection"): {
                "baseline": {"items": {"tmdb:1": {"type": "movie", "ids": {"tmdb": "1"}, "collected_at": "2026-05-14T08:50:05Z"}}},
                "checkpoint": None,
            }
        }
    )

    state = store.load_state_features({"collection"})

    items = state["providers"]["PLEX"]["instances"]["PLEX-P01"]["collection"]["baseline"]["items"]
    assert set(items) == {"tmdb:1"}
    assert items["tmdb:1"]["collected_at"] == "2026-05-14T08:50:05Z"


def test_api_normalizes_collection_types_to_movies_by_default() -> None:
    from api.syncAPI import _normalize_features

    out = _normalize_features({"collection": {"enable": True, "add": True, "remove": True}})

    assert out["collection"]["types"] == ["movies"]


def test_config_loader_normalizes_collection_types_to_movies_by_default() -> None:
    from cw_platform.config_base import _normalize_features_map

    out = _normalize_features_map({"collection": {"enable": True, "add": True, "remove": True}})

    assert out["collection"]["types"] == ["movies"]
