from __future__ import annotations

from copy import deepcopy

import pytest

from services import dashboard_widgets


def _movie(idx: int) -> dict:
    return {
        "type": "movie",
        "title": f"Movie {idx}",
        "year": 2020,
        "watched_at": "2026-09-01T00:00:00Z",
        "ids": {"tmdb": str(5000 + idx)},
    }


def _state(count: int) -> dict:
    items = {f"tmdb:{5000 + i}": _movie(i) for i in range(count)}
    return {
        "providers": {
            "CROSSWATCH": {"history": {"baseline": {"items": dict(items)}}},
            "TRAKT": {"history": {"baseline": {"items": dict(items)}}},
        }
    }


def _providers_of(row) -> set[str]:
    return {str(s.get("provider") or "").upper() for s in row.get("sources") or []}


def test_history_rows_keep_every_provider_past_the_row_window(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **_k: rows)
    # more items per provider than the window, all sharing one watched_at
    count = dashboard_widgets._ROW_WINDOW + 50
    payload = dashboard_widgets.recent_history_widget(_state(count), limit=6)

    rows = payload["items"]
    assert rows
    for row in rows:
        assert _providers_of(row) == {"CROSSWATCH", "TRAKT"}, row.get("title")


@pytest.fixture
def plex_history(monkeypatch):
    from cw_platform import config_base

    cfg = {"pairs": [{"source": "PLEX", "target": "PLEX", "source_instance": "default",
                      "target_instance": "PLEX-P01", "features": {"history": {"enable": True}}}]}
    monkeypatch.setattr(config_base, "load_config", lambda: cfg)
    monkeypatch.setattr(dashboard_widgets, "_history_alias_representatives", lambda: {})
    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **kw: rows)

    def create(kind="movie"):
        source = {}
        for i in range(dashboard_widgets._ROW_WINDOW + 50):
            item = {**_movie(i), "watched_at": 1600000000 + i}
            item["ids"]["plex"] = str(i + 1)
            if kind == "episode":
                item.update(type="episode", series_title="Same Show", season=i // 20 + 1,
                            episode=i % 20 + 1, show_ids={"tmdb": "777"})
            source[str(i)] = item
        destination = {key: {**item, "watched_at": 1789947000} for key, item in source.items()}
        return {"providers": {"PLEX": {"history": {"baseline": {"items": source}},
                "instances": {"PLEX-P01": {"history": {"baseline": {"items": destination}}}}}}}

    return create, cfg


def _without_icons(payload):
    return {**payload, "items": [{k: v for k, v in row.items() if k != "sources"} for row in payload["items"]]}


@pytest.mark.parametrize("kind", ["movie", "episode"])
def test_existing_plex_history_gets_both_icons_without_changing_dates(plex_history, monkeypatch, kind):
    create, _ = plex_history
    state = create(kind)
    original = deepcopy(state)

    actual = dashboard_widgets.recent_history_widget(state, limit=6)

    for row in actual["items"]:
        assert {(s["provider"], s["instance"]) for s in row["sources"]} == {("PLEX", "default"), ("PLEX", "PLEX-P01")}
        assert row["watched_at"] == 1789947000
    monkeypatch.setattr(dashboard_widgets, "_complete_plex_history_sources", lambda *a: None)
    assert _without_icons(actual) == _without_icons(dashboard_widgets.recent_history_widget(state, limit=6))
    assert state == original


@pytest.mark.parametrize("case", ["missing_episode", "different_episode", "native_id_collision"])
def test_plex_badges_require_matching_media(plex_history, case):
    create, _ = plex_history
    state = create("episode" if case != "native_id_collision" else "movie")
    source = state["providers"]["PLEX"]["history"]["baseline"]["items"]
    if case == "missing_episode":
        del source["0"]
    elif case == "different_episode":
        source["0"] = {**source["0"], "episode": 99}
    else:
        source["0"] = {**source["0"], "ids": {"plex": "1"}}

    rows = dashboard_widgets.recent_history_widget(state, limit=6)["items"]

    assert rows[0]["sources"] == [{"provider": "PLEX", "instance": "PLEX-P01"}]
    assert len(rows[1]["sources"]) == 2


@pytest.mark.parametrize("case", ["no_pair", "disabled_pair", "disabled_history", "same_instance", "profile_filter",
                                  "plex_trakt", "trakt_plex", "emby_emby", "jellyfin_jellyfin"])
def test_unrelated_history_keeps_identical_output(plex_history, monkeypatch, case):
    create, cfg = plex_history
    state = create()
    pair = cfg["pairs"][0]
    user_filter = None
    if case == "no_pair":
        cfg["pairs"] = []
    elif case == "disabled_pair":
        pair["enabled"] = False
    elif case == "disabled_history":
        pair["features"]["history"]["enable"] = False
    elif case == "same_instance":
        pair["target_instance"] = "default"
    elif case == "profile_filter":
        user_filter = {"PLEX": ["PLEX-P01"]}
    else:
        src, dst = case.upper().split("_")
        pair.update(source=src, target=dst)
        plex = state["providers"].pop("PLEX")
        destination = plex.pop("instances")["PLEX-P01"]
        if src == dst:
            state["providers"][src] = {**plex, "instances": {"PLEX-P01": destination}}
        else:
            state["providers"].update({src: plex, dst: {"instances": {"PLEX-P01": destination}}})

    actual = dashboard_widgets.recent_history_widget(state, user_filter=user_filter)
    monkeypatch.setattr(dashboard_widgets, "_complete_plex_history_sources", lambda *a: None)
    assert actual == dashboard_widgets.recent_history_widget(state, user_filter=user_filter)


def test_plex_rewatch_dates_and_counts_are_unchanged(plex_history, monkeypatch):
    create, _ = plex_history
    state = create("episode")
    destination = state["providers"]["PLEX"]["instances"]["PLEX-P01"]["history"]["baseline"]["items"]
    destination["rewatch"] = {**destination["0"], "watched_at": 1789957000}

    actual = dashboard_widgets.recent_history_widget(state)

    assert actual["items"][0]["watched_at"] == 1789957000
    assert actual["items"][0]["watch_count"] == 2
    monkeypatch.setattr(dashboard_widgets, "_complete_plex_history_sources", lambda *a: None)
    assert _without_icons(actual) == _without_icons(dashboard_widgets.recent_history_widget(state))


def test_ratings_rows_keep_every_provider_past_the_row_window(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **_k: rows)
    count = dashboard_widgets._ROW_WINDOW + 50
    items = {
        f"tmdb:{6000 + i}": {
            "type": "movie",
            "title": f"Rated {i}",
            "year": 2020,
            "rating": 8,
            "rated_at": "2026-08-01T00:00:00Z",
            "ids": {"tmdb": str(6000 + i)},
        }
        for i in range(count)
    }
    state = {
        "providers": {
            "CROSSWATCH": {"ratings": {"baseline": {"items": dict(items)}}},
            "TRAKT": {"ratings": {"baseline": {"items": dict(items)}}},
        }
    }
    payload = dashboard_widgets.latest_ratings_widget(state, limit=6)

    rows = payload["items"]
    assert rows
    for row in rows:
        assert _providers_of(row) == {"CROSSWATCH", "TRAKT"}, row.get("title")
