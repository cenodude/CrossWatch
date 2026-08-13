from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Mapping


class Adapter:
    def __init__(self, *, anime_mapping: bool = True) -> None:
        self.config: dict[str, Any] = {
            "anime_mapping": {"enabled": True, "features": ["history"], "release_tag": "v3"},
            "_cw_pair_feature_options": {
                "feature": "history",
                "use_anime_mapping": anime_mapping,
                "anime_only_sync": False,
            },
        }


def _episode(
    *,
    show_tmdb: str = "12609",
    season: int = 1,
    episode: int = 14,
    watched_at: str = "2024-01-01T00:00:00Z",
    ids: Mapping[str, Any] | None = None,
    show_ids: Mapping[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "type": "episode",
        "series_title": "Dragon Ball",
        "season": season,
        "episode": episode,
        "watched_at": watched_at,
        "ids": dict(ids or {"tvdb": "1210301"}),
        "show_ids": dict(show_ids or {"tmdb": show_tmdb, "anidb": "231"}),
        **extra,
    }


def test_destination_comparison_view_adds_anime_coordinate_alias(monkeypatch: Any) -> None:
    from providers.sync.mdblist import _history as m

    monkeypatch.setattr(
        m,
        "resolve_absolute",
        lambda *_args, **_kwargs: SimpleNamespace(absolute=14, namespace="anidb", target_id="231"),
    )
    monkeypatch.setattr(m, "resolve_axis_coordinates", lambda *_args, **_kwargs: {(1, 14), (2, 1)})
    monkeypatch.setattr(m, "resolve_source_coordinate", lambda *_args, **_kwargs: None)

    adapter = Adapter(anime_mapping=True)
    raw = _episode(season=1, episode=14)
    index = {m._history_key(adapter, raw): raw}

    out = m.destination_comparison_view(index, adapter)

    assert out["tmdb:12609#s01e14"]["season"] == 2
    assert out["tmdb:12609#s01e14"]["episode"] == 1
    assert "tmdb:12609#s02e01" in out
    assert out["tmdb:12609#s02e01"]["ids"]["tvdb"] == "1210301"


def test_destination_comparison_view_respects_pair_toggle(monkeypatch: Any) -> None:
    from providers.sync.mdblist import _history as m

    monkeypatch.setattr(
        m,
        "resolve_absolute",
        lambda *_args, **_kwargs: SimpleNamespace(absolute=14, namespace="anidb", target_id="231"),
    )
    monkeypatch.setattr(m, "resolve_axis_coordinates", lambda *_args, **_kwargs: {(1, 14), (2, 1)})

    adapter = Adapter(anime_mapping=False)
    raw = _episode(season=1, episode=14)
    index = {m._history_key(adapter, raw): raw}

    assert m.destination_comparison_view(index, adapter) == index


def test_bucketize_prefers_tmdb_anime_coordinate_for_mdblist(monkeypatch: Any) -> None:
    from providers.sync.mdblist import _history as m

    monkeypatch.setattr(
        m,
        "query_edges",
        lambda *_args, **_kwargs: [
            {
                "target_provider": "tmdb",
                "target_kind": "show",
                "target_id": "12609",
                "target_scope": "s1",
                "source_range": "1-153",
                "target_range": "1-153",
            }
        ],
    )
    monkeypatch.setattr(m, "resolve_source_coordinate", lambda *_args, **_kwargs: None)

    adapter = Adapter(anime_mapping=True)
    item = _episode(
        season=2,
        episode=1,
        simkl_bucket="anime",
        _simkl_episode_number=14,
        show_ids={"tmdb": "12609", "anidb": "231", "mal": "223"},
    )

    body, accepted = m._bucketize(adapter, [item], unwatch=False)

    season = body["shows_nested"][0]["seasons"][0]
    assert season["number"] == 1
    assert season["episodes"][0]["number"] == 14
    assert accepted[0]["season"] == 1
    assert accepted[0]["episode"] == 14


def test_bucketize_falls_back_to_reverse_source_coordinate(monkeypatch: Any) -> None:
    from providers.sync.mdblist import _history as m

    monkeypatch.setattr(m, "query_edges", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        m,
        "resolve_source_coordinate",
        lambda *_args, **_kwargs: SimpleNamespace(season=2, episode=1),
    )

    adapter = Adapter(anime_mapping=True)
    item = _episode(
        show_tmdb="12971",
        season=1,
        episode=40,
        ids={},
        show_ids={"tmdb": "12971", "tvdb": "81472", "mal": "813", "anidb": "1530"},
        series_title="Dragon Ball Z",
        simkl_bucket="anime",
        _simkl_episode_number=40,
    )

    body, accepted = m._bucketize(adapter, [item], unwatch=False)

    season = body["shows_nested"][0]["seasons"][0]
    assert season["number"] == 2
    assert season["episodes"][0]["number"] == 1
    assert accepted[0]["season"] == 2
    assert accepted[0]["episode"] == 1


def test_mdblist_module_exposes_history_comparison_view(monkeypatch: Any) -> None:
    import providers.sync._mod_MDBLIST as mod
    from providers.sync.mdblist import _history as history

    monkeypatch.setattr(history, "destination_comparison_view", lambda index, adapter=None: {"alias": {"type": "movie"}})

    cfg = {
        "mdblist": {"enabled": True, "api_key": "k"},
        "anime_mapping": {"enabled": True, "features": ["history"]},
        "_cw_pair_feature_options": {"feature": "history", "use_anime_mapping": True},
    }

    assert mod.OPS.destination_comparison_view(cfg, feature="history", index={}) == {"alias": {"type": "movie"}}
