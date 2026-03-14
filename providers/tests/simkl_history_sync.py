from __future__ import annotations

from types import SimpleNamespace


def test_history_floor():
    import sync.simkl._history as m

    assert m._rewind_iso("1900-01-01T00:00:00Z", seconds=2) == "1900-01-01T00:00:00Z"


def test_history_markers():
    import sync.simkl._history as m

    acts = {
        "movies": {"all": "2026-03-14T05:54:07Z"},
        "shows": {"all": "2026-03-14T05:54:08Z"},
        "anime": {"all": "2026-03-14T05:54:09Z"},
    }

    movie_latest, show_latest, anime_latest = m._history_activity_markers(acts)

    assert movie_latest == "2026-03-14T05:54:07Z"
    assert show_latest == "2026-03-14T05:54:08Z"
    assert anime_latest == "2026-03-14T05:54:09Z"


def test_history_index_empty_delta(monkeypatch):
    import sync.simkl._history as m

    watermark_updates: list[tuple[str, str | None]] = []

    monkeypatch.setattr(m, "normalize_flat_watermarks", lambda: None)
    monkeypatch.setattr(
        m,
        "fetch_activities",
        lambda *_a, **_kw: ({"movies": {"all": "2026-03-14T05:54:07Z"}}, {}),
    )
    monkeypatch.setattr(m, "get_watermark", lambda feature: "2026-03-14T04:12:34Z" if feature == "history" else None)
    monkeypatch.setattr(m, "_shadow_merge_into", lambda out, thaw: None)
    monkeypatch.setattr(m, "_shadow_put_all", lambda _values: None)
    monkeypatch.setattr(m, "_dedupe_history_movies", lambda out: None)
    monkeypatch.setattr(m, "_apply_since_limit", lambda out, *, since=None, limit=None: None)
    monkeypatch.setattr(m, "_unfreeze", lambda thaw: None)
    monkeypatch.setattr(m, "_fetch_kind", lambda *_a, **_kw: [])
    monkeypatch.setattr(
        m,
        "update_watermark_if_new",
        lambda feature, iso_ts: watermark_updates.append((feature, iso_ts)) or iso_ts,
    )
    adapter = SimpleNamespace(client=SimpleNamespace(session=object()), cfg=SimpleNamespace(timeout=5, api_key="k", access_token="tok"))
    out = m.build_index(adapter)

    assert out == {}
    assert watermark_updates == []


def test_history_show_needs_episode(monkeypatch):
    import sync.simkl._history as m

    monkeypatch.setattr(m, "normalize_flat_watermarks", lambda: None)
    monkeypatch.setattr(m, "fetch_activities", lambda *_a, **_kw: (None, {}))
    monkeypatch.setattr(m, "_shadow_merge_into", lambda out, thaw: None)
    monkeypatch.setattr(m, "_shadow_put_all", lambda _values: None)
    monkeypatch.setattr(m, "_dedupe_history_movies", lambda out: None)
    monkeypatch.setattr(m, "_unfreeze", lambda thaw: None)
    monkeypatch.setattr(m, "update_watermark_if_new", lambda *_a, **_kw: None)

    row = {
        "show": {
            "title": "Gold Rush",
            "year": 2010,
            "ids": {"tmdb": "34634", "imdb": "tt1800864", "simkl": "23046"},
        },
        "last_watched_at": "2026-03-14T13:47:29Z",
        "seasons": [],
    }

    def _fetch_kind(_session, _headers, *, kind, status=None, since_iso, timeout):
        return [row] if kind == "shows" and status == "watching" else []

    monkeypatch.setattr(m, "_fetch_kind", _fetch_kind)

    adapter = SimpleNamespace(client=SimpleNamespace(session=object()), cfg=SimpleNamespace(timeout=5, api_key="k", access_token="tok"))
    out = m.build_index(adapter)

    assert out == {}


def test_history_movies_completed(monkeypatch):
    import sync.simkl._history as m

    monkeypatch.setattr(m, "normalize_flat_watermarks", lambda: None)
    monkeypatch.setattr(m, "fetch_activities", lambda *_a, **_kw: ({"movies": {"all": "2026-03-14T13:57:05Z"}}, {}))
    monkeypatch.setattr(m, "get_watermark", lambda feature: "2026-03-14T13:51:55Z" if feature == "history" else None)
    monkeypatch.setattr(m, "_shadow_merge_into", lambda out, thaw: None)
    monkeypatch.setattr(m, "_shadow_put_all", lambda _values: None)
    monkeypatch.setattr(m, "_dedupe_history_movies", lambda out: None)
    monkeypatch.setattr(m, "_unfreeze", lambda thaw: None)
    monkeypatch.setattr(m, "update_watermark_if_new", lambda *_a, **_kw: None)

    def _fetch_kind(_session, _headers, *, kind, status=None, since_iso, timeout):
        if kind == "movies" and status is None:
            return [{
                "movie": {
                    "title": "New Movie",
                    "year": 2026,
                    "ids": {"tmdb": "999", "imdb": "tt999", "simkl": "999"},
                },
                "last_watched_at": "2026-03-14T13:57:05Z",
            }]
        return []

    monkeypatch.setattr(m, "_fetch_kind", _fetch_kind)

    adapter = SimpleNamespace(client=SimpleNamespace(session=object()), cfg=SimpleNamespace(timeout=5, api_key="k", access_token="tok"))
    out = m.build_index(adapter)

    assert len(out) == 1
    item = next(iter(out.values()))
    assert item.get("type") == "movie"
    assert item.get("title") == "New Movie"
    assert item.get("watched_at") == "2026-03-14T13:57:05Z"
