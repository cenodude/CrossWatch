from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from cw_platform.anime_mapping.episodes import Resolution


@pytest.fixture()
def m():
    from sync.simkl import _history

    return _history


class _Resp:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._payload


class _Session:
    def __init__(self, redirect_url, redirects=None):
        self._url = redirect_url
        self.gets = []
        self._redirects = redirects or {}

    def get(self, url, headers=None, params=None, timeout=None, allow_redirects=None):
        self.gets.append(dict(params or {}))
        if url != self._url:
            return _Resp(404)
        for ns in ("anidb", "mal", "anilist", "kitsu"):
            value = str((params or {}).get(ns) or "")
            if not value:
                continue
            target = self._redirects.get(f"{ns}:{value}")
            if target is None:
                return _Resp(301, headers={"Location": "https://simkl.com?x=1"})
            return _Resp(301, headers={"Location": f"https://simkl.com/anime/{target}/slug"})
        return _Resp(404)


@pytest.fixture(autouse=True)
def _fs(monkeypatch, m):
    store: dict[str, str] = {}
    monkeypatch.setattr(m, "state_file", lambda name: name)
    monkeypatch.setattr(m, "_load_json", lambda path: json.loads(store.get(path) or "{}"))
    monkeypatch.setattr(m, "_save_json", lambda path, data: store.__setitem__(path, json.dumps(data)))
    monkeypatch.setattr(m, "simkl_api_params_from_headers", lambda headers=None, **k: dict(k))


def _mapped_item(namespace="anidb", target_id="1530", absolute=52):
    return {
        "type": "episode",
        "season": 2,
        "episode": 13,
        "show_ids": {"tvdb": "81472"},
        "_cw_anime_map": {"absolute": absolute, "namespace": namespace, "target_id": target_id},
    }


def _absolute(m, item, ids, session):
    return m._anibridge_absolute(
        item,
        ids,
        session=session,
        headers={},
        timeout=5,
        resolve_state=m._AnimeResolveState({}, {}),
    )


def test_accepts_when_redirect_matches_target_entry(m):
    session = _Session(m.URL_REDIRECT, {"anidb:1530": "41487"})
    assert _absolute(m, _mapped_item(), {"simkl": "41487"}, session) == 52


def test_rejects_when_redirect_points_at_a_different_entry(m):
    session = _Session(m.URL_REDIRECT, {"anidb:1530": "999999"})
    assert _absolute(m, _mapped_item(), {"simkl": "41487"}, session) is None


def test_rejects_when_redirect_is_not_an_anime_url(m):
    session = _Session(m.URL_REDIRECT, {})
    assert _absolute(m, _mapped_item(), {"simkl": "41487"}, session) is None


def test_rejects_without_simkl_id(m):
    session = _Session(m.URL_REDIRECT, {"anidb:1530": "41487"})
    assert _absolute(m, _mapped_item(), {}, session) is None


def test_ignores_items_without_a_mapping(m):
    session = _Session(m.URL_REDIRECT, {"anidb:1530": "41487"})
    item = {"type": "episode", "season": 2, "episode": 13, "show_ids": {"tvdb": "81472"}}
    assert _absolute(m, item, {"simkl": "41487"}, session) is None
    assert session.gets == []


def test_redirect_result_is_cached_across_calls(m):
    session = _Session(m.URL_REDIRECT, {"mal:813": "41487"})
    state = m._AnimeResolveState({}, {})
    for _ in range(3):
        assert m._anibridge_absolute(
            _mapped_item("mal", "813"),
            {"simkl": "41487"},
            session=session,
            headers={},
            timeout=5,
            resolve_state=state,
        ) == 52
    assert len(session.gets) == 1


def test_negative_redirect_is_cached(m):
    session = _Session(m.URL_REDIRECT, {})
    state = m._AnimeResolveState({}, {})
    for _ in range(3):
        assert m._anibridge_absolute(
            _mapped_item(),
            {"simkl": "41487"},
            session=session,
            headers={},
            timeout=5,
            resolve_state=state,
        ) is None
    assert len(session.gets) == 1


def _adapter(cfg):
    return SimpleNamespace(raw_cfg=cfg, client=SimpleNamespace(session=None), cfg=SimpleNamespace(timeout=5))


def test_mapping_is_skipped_when_disabled(monkeypatch, m):
    monkeypatch.setattr(m, "resolve_absolute", lambda *a, **k: Resolution(52, "anidb", "1530", "b", "e"))
    items = [{"type": "episode", "season": 2, "episode": 13, "show_ids": {"tvdb": "81472"}}]
    out = m._apply_anibridge_maps(_adapter({"anime_mapping": {"enabled": False}}), items)
    assert "_cw_anime_map" not in out[0]


def test_mapping_is_applied_when_enabled(monkeypatch, m):
    monkeypatch.setattr(m, "resolve_absolute", lambda *a, **k: Resolution(52, "anidb", "1530", "b", "e"))
    items = [{"type": "episode", "season": 2, "episode": 13, "show_ids": {"tvdb": "81472"}}]
    out = m._apply_anibridge_maps(_adapter({"anime_mapping": {"enabled": True, "features": ["history"]}}), items)
    assert out[0]["_cw_anime_map"]["absolute"] == 52


def test_trakt_number_abs_wins_and_skips_anibridge(monkeypatch, m):
    calls = []

    def _resolve(*a, **k):
        calls.append(1)
        return Resolution(52, "anidb", "1530", "b", "e")

    monkeypatch.setattr(m, "resolve_absolute", _resolve)
    items = [{"type": "episode", "season": 2, "episode": 13, "show_ids": {"tvdb": "81472"}, "_trakt_number_abs": 52}]
    out = m._apply_anibridge_maps(_adapter({"anime_mapping": {"enabled": True, "features": ["history"]}}), items)
    assert "_cw_anime_map" not in out[0]
    assert calls == []


def test_pair_option_can_disable_mapping(monkeypatch, m):
    monkeypatch.setattr(m, "resolve_absolute", lambda *a, **k: Resolution(52, "anidb", "1530", "b", "e"))
    cfg = {
        "anime_mapping": {"enabled": True, "features": ["history"]},
        m.PAIR_FEATURE_OPTIONS_KEY: {"use_anime_mapping": False, "feature": "history"},
    }
    items = [{"type": "episode", "season": 2, "episode": 13, "show_ids": {"tvdb": "81472"}}]
    out = m._apply_anibridge_maps(_adapter(cfg), items)
    assert "_cw_anime_map" not in out[0]


def test_movies_are_not_mapped(monkeypatch, m):
    monkeypatch.setattr(m, "resolve_absolute", lambda *a, **k: Resolution(52, "anidb", "1530", "b", "e"))
    items = [{"type": "movie", "ids": {"tmdb": "1"}}]
    out = m._apply_anibridge_maps(_adapter({"anime_mapping": {"enabled": True, "features": ["history"]}}), items)
    assert "_cw_anime_map" not in out[0]


def test_history_feature_must_be_enabled(monkeypatch, m):
    monkeypatch.setattr(m, "resolve_absolute", lambda *a, **k: Resolution(52, "anidb", "1530", "b", "e"))
    cfg = {"anime_mapping": {"enabled": True, "features": ["watchlist", "ratings"]}}
    items = [{"type": "episode", "season": 2, "episode": 13, "show_ids": {"tvdb": "81472"}}]
    out = m._apply_anibridge_maps(_adapter(cfg), items)
    assert "_cw_anime_map" not in out[0]
