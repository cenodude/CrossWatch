from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from api import profileAPI
from services import dashboard_widgets, profile_history


def _history(items: dict[str, Any]) -> dict[str, Any]:
    return {"history": {"baseline": {"items": items}}}


def _ratings(items: dict[str, Any]) -> dict[str, Any]:
    return {"ratings": {"baseline": {"items": items}}}


def _movie(tmdb: int, title: str, year: int, watched_at: str) -> dict[str, Any]:
    return {"type": "movie", "title": title, "year": year, "ids": {"tmdb": tmdb}, "watched_at": watched_at}


TURNING_POINT = _movie(10, "Turning Point", 2026, "2026-09-13T20:29:00Z")
KUSAMA = _movie(20, "Kusama: Infinity", 2018, "2026-08-01T10:00:00Z")
TED_LASSO = {
    "type": "episode",
    "title": "Ted Lasso",
    "series_title": "Ted Lasso",
    "season": 4,
    "episode": 4,
    "ids": {"tvdb": 555},
    "show_ids": {"tmdb": 97546, "tvdb": 383203},
    "watched_at": "2026-09-08T17:35:00Z",
}
HEAT = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": 949}, "rating": 9, "rated_at": "2026-09-10T20:00:00Z"}
MATRIX = {"type": "movie", "title": "The Matrix", "year": 1999, "ids": {"tmdb": 603}, "rating": 8, "rated_at": "2026-08-01T10:00:00Z"}
PULP = {"type": "movie", "title": "Pulp Fiction", "year": 1994, "ids": {"tmdb": 680}, "rating": 10}


def _state() -> dict[str, Any]:
    return {
        "providers": {
            "PLEX": _history({"tmdb:10@2026-09-13T20:29:00Z": TURNING_POINT}),
            "SIMKL": _history({
                "tmdb:10@2026-09-13T20:29:00Z": TURNING_POINT,
                "tvdb:383203:4:4@2026-09-08T17:35:00Z": TED_LASSO,
            }),
            "TRAKT": _history({
                "tmdb:10@2026-09-13T20:29:00Z": TURNING_POINT,
                "tmdb:20@2026-08-01T10:00:00Z": KUSAMA,
            }),
        }
    }


def _ratings_state() -> dict[str, Any]:
    return {
        "providers": {
            "PLEX": _ratings({"tmdb:949": HEAT, "tmdb:603": MATRIX}),
            "SIMKL": _ratings({"tmdb:949": HEAT, "tmdb:680": PULP}),
            "TRAKT": _ratings({"tmdb:949": {**HEAT, "rating": 7, "rated_at": "2026-09-11T20:00:00Z"}}),
        }
    }


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(dashboard_widgets, "_history_alias_representatives", lambda: {})
    monkeypatch.setattr(dashboard_widgets, "_metadata_manager", lambda: None)
    profile_history.clear_history_cache()
    yield
    profile_history.clear_history_cache()


def _synced_index(user_filter: dict[str, Any] | None = None) -> dict[str, Any]:
    return profile_history.build_history_index("synced", state=_state(), tracker_items={}, user_filter=user_filter or {})


def _ratings_index() -> dict[str, Any]:
    return profile_history.build_history_index("ratings", state=_ratings_state(), tracker_items={}, user_filter={})


def test_synced_history_reports_where_each_play_lives() -> None:
    payload = profile_history.build_history_payload(_synced_index(), resolve_art=False)

    assert payload["source"] == "synced"
    assert payload["total"] == 3
    assert [item["title"] for item in payload["items"]] == ["Turning Point", "Ted Lasso", "Kusama: Infinity"]
    first = payload["items"][0]
    assert {ref["provider"] for ref in first["present"]} == {"PLEX", "SIMKL", "TRAKT"}
    assert first["missing"] == []
    assert all(not key.startswith("_") for key in first)
    kusama = payload["items"][2]
    assert kusama["present"] == [{"provider": "TRAKT", "instance": "default"}]
    assert {ref["provider"] for ref in kusama["missing"]} == {"PLEX", "SIMKL"}
    assert payload["counts"] == {"all": 3, "movie": 2, "episode": 1}
    assert payload["stats"] == {"plays": 3, "movies": 2, "episodes": 1, "endpoints": 3, "full": 1, "partial": 2}
    assert payload["providers"] == [
        {"provider": "plex", "count": 1},
        {"provider": "simkl", "count": 2},
        {"provider": "trakt", "count": 2},
    ]
    assert payload["months"] == [{"month": "2026-09", "count": 2}, {"month": "2026-08", "count": 1}]


def test_synced_history_filters_and_jumps_to_month() -> None:
    index = _synced_index()

    def titles(**kwargs: Any) -> list[str]:
        return [item["title"] for item in profile_history.build_history_payload(index, resolve_art=False, **kwargs)["items"]]

    assert titles(coverage="full") == ["Turning Point"]
    assert titles(coverage="partial") == ["Ted Lasso", "Kusama: Infinity"]
    assert titles(provider="simkl") == ["Turning Point", "Ted Lasso"]
    assert titles(media_type="episode") == ["Ted Lasso"]
    assert titles(search="lasso") == ["Ted Lasso"]

    jumped = profile_history.build_history_payload(index, resolve_art=False, page_size=1, month="2026-08")
    assert jumped["page"] == 3
    assert jumped["page_count"] == 3
    assert [item["title"] for item in jumped["items"]] == ["Kusama: Infinity"]


def test_synced_history_is_scoped_to_user_profile_endpoints() -> None:
    payload = profile_history.build_history_payload(_synced_index({"PLEX": ["default"]}), resolve_art=False)

    assert payload["total"] == 1
    assert payload["endpoints"] == [{"provider": "PLEX", "instance": "default"}]
    assert payload["items"][0]["missing"] == []


def test_scrobble_history_keeps_route_and_hours(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {
            "ok": True,
            "items": [
                {
                    "id": "event-1",
                    "kind": "scrobble",
                    "method": "watcher",
                    "event": "scrobble_stop",
                    "media_type": "movie",
                    "title": "Heat",
                    "year": 1995,
                    "source": "plex",
                    "targets": [
                        {"target": "trakt", "target_instance": "default"},
                        {"target": "simkl", "target_instance": "default"},
                    ],
                    "ids": {"tmdb": 949},
                    "runtime_minutes": 170,
                    "watched_at": 1767225600,
                    "captured_at": 1767229200,
                    "status": "ok",
                },
                {"id": "event-2", "kind": "sync", "media_type": "movie", "title": "Ignored", "captured_at": 1767229300},
            ],
        },
    )

    index = profile_history.build_history_index("scrobble", user_filter={})
    payload = profile_history.build_history_payload(index, resolve_art=False)

    assert payload["source"] == "scrobble"
    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["source"] == {"provider": "PLEX", "instance": "default"}
    assert [target["provider"] for target in item["targets"]] == ["TRAKT", "SIMKL"]
    assert "present" not in item
    assert payload["stats"]["hours"] == pytest.approx(2.8)


def test_ratings_keep_each_provider_score_and_flag_conflicts() -> None:
    payload = profile_history.build_history_payload(_ratings_index(), resolve_art=False)

    assert payload["source"] == "ratings"
    assert [item["title"] for item in payload["items"]] == ["Heat", "The Matrix", "Pulp Fiction"]
    heat = payload["items"][0]
    assert heat["rating"] == 7
    assert heat["agree"] is False
    assert {ref["provider"]: ref["rating"] for ref in heat["present"]} == {"PLEX": 9, "SIMKL": 9, "TRAKT": 7}
    assert heat["missing"] == []
    pulp = payload["items"][2]
    assert pulp["sort_epoch"] == 0
    assert pulp["agree"] is True
    assert {ref["provider"] for ref in pulp["missing"]} == {"PLEX", "TRAKT"}
    assert payload["counts"]["show"] == 0
    assert payload["counts"]["ratings"]["10"] == 1
    assert payload["counts"]["ratings"]["7"] == 1
    assert payload["stats"]["mismatch"] == 1
    assert payload["stats"]["full"] == 1
    assert payload["stats"]["average"] == pytest.approx(8.3)
    assert payload["months"] == [{"month": "2026-09", "count": 1}, {"month": "2026-08", "count": 1}]


def test_ratings_filter_by_score_and_conflict() -> None:
    index = _ratings_index()

    def titles(**kwargs: Any) -> list[str]:
        return [item["title"] for item in profile_history.build_history_payload(index, resolve_art=False, **kwargs)["items"]]

    assert titles(coverage="mismatch") == ["Heat"]
    assert titles(rating="10") == ["Pulp Fiction"]
    assert titles(coverage="partial") == ["The Matrix", "Pulp Fiction"]
    assert titles(provider="trakt") == ["Heat"]


def test_history_index_is_built_once_per_cache_key() -> None:
    builds: list[int] = []

    def builder() -> dict[str, Any]:
        builds.append(1)
        return _synced_index()

    profile_history.cached_history_index(("synced", "", "v1"), builder)
    profile_history.cached_history_index(("synced", "", "v1"), builder)
    assert len(builds) == 1

    profile_history.cached_history_index(("synced", "", "v2"), builder)
    assert len(builds) == 2


def test_profile_history_payload_scopes_unknown_profile_to_nothing(monkeypatch) -> None:
    monkeypatch.setattr(profileAPI, "_history_state", _state)
    monkeypatch.setattr(profileAPI, "_ratings_state", _ratings_state)
    monkeypatch.setattr(profileAPI, "_tracker_feature_items", lambda _kind: {})
    monkeypatch.setattr(profileAPI, "_dashboard_widgets_version", lambda *_args, **_kwargs: "")

    everyone = profileAPI.build_profile_history_payload({}, source="synced")
    assert everyone["total"] == 3
    assert everyone["items"][0]["poster"].startswith("/art/tmdb/")
    assert profileAPI.build_profile_history_payload({}, profile_id="ghost")["total"] == 0

    ratings = profileAPI.build_profile_history_payload({}, source="ratings", rating="9")
    assert ratings["source"] == "ratings"
    assert ratings["total"] == 0
    assert profileAPI.build_profile_history_payload({}, source="ratings")["total"] == 3


def test_profile_timeline_ui_is_registered() -> None:
    root = Path(__file__).resolve().parents[1]
    js = (root / "assets/js/profile-page.js").read_text(encoding="utf-8")
    css = (root / "assets/css/profile-page.css").read_text(encoding="utf-8")
    widgets = (root / "assets/js/dashboard-widgets.js").read_text(encoding="utf-8")
    from ui_frontend import get_profile_html

    html = get_profile_html(user={"is_admin": True, "username": "admin"})

    assert 'data-profile-tab="history"' in html
    assert 'data-profile-tab="ratings"' in html
    assert html.index('data-profile-tab="history"') < html.index('data-profile-tab="ratings"') < html.index('data-profile-tab="collection"')
    assert 'id="profile-panel-history"' in html
    assert 'id="profile-panel-ratings"' in html
    assert 'data-timeline-source="scrobble"' in html
    assert 'id="profile-ratings-scores"' in html
    assert 'value="mismatch"' in html
    assert 'data-cw-profile-menu-action="ratings"' in html
    assert "/api/profile/history" in js
    assert "/api/profile/ratings" in js
    assert "historyPanel.wire();" in js
    assert "ratingsPanel.wire();" in js
    assert ".cw-hist-timeline" in css
    assert ".cw-hist-scores" in css
    assert "/profile#history/synced" in widgets
    assert "/profile#history/scrobble" in widgets
    assert "/profile#ratings" in widgets
