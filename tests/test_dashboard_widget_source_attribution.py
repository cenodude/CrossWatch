from __future__ import annotations

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
