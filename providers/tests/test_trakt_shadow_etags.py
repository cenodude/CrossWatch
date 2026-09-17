from __future__ import annotations

import importlib
import json
from pathlib import Path

_wl = importlib.import_module("providers.sync.trakt._watchlist")
_col = importlib.import_module("providers.sync.trakt._collection")


def _prepare(monkeypatch, tmp_path: Path, mod=_wl, name: str = "trakt_watchlist.shadow.json") -> Path:
    target = tmp_path / name
    monkeypatch.setattr(mod, "_shadow_path", lambda: target)
    monkeypatch.setattr(mod, "_is_capture_mode", lambda: False)
    monkeypatch.setattr(mod, "_pair_scope", lambda: "pair")
    return target


def test_shadow_save_omits_a_missing_etag(monkeypatch, tmp_path):
    target = _prepare(monkeypatch, tmp_path)
    _wl._shadow_save(None, {"tmdb:1": {"type": "movie"}})
    data = json.loads(target.read_text("utf-8"))
    assert "etag" not in data
    assert data["items"] == {"tmdb:1": {"type": "movie"}}


def test_shadow_save_keeps_a_real_etag(monkeypatch, tmp_path):
    target = _prepare(monkeypatch, tmp_path)
    _wl._shadow_save('"abc"', {"tmdb:1": {"type": "movie"}})
    assert json.loads(target.read_text("utf-8"))["etag"] == '"abc"'


def test_shadow_save_omits_a_blank_etag(monkeypatch, tmp_path):
    target = _prepare(monkeypatch, tmp_path)
    _wl._shadow_save("  ", {})
    assert "etag" not in json.loads(target.read_text("utf-8"))


def test_shadow_load_survives_a_saved_file(monkeypatch, tmp_path):
    _prepare(monkeypatch, tmp_path)
    _wl._shadow_save(None, {"tmdb:2": {"type": "show"}})
    loaded = _wl._shadow_load()
    assert loaded.get("etag") is None
    assert loaded["items"] == {"tmdb:2": {"type": "show"}}


def test_collection_shadow_save_drops_missing_etags(monkeypatch, tmp_path):
    target = _prepare(monkeypatch, tmp_path, _col, "trakt_collection.shadow.json")
    _col._shadow_save({"movies": None, "shows": '"s"'}, {"tmdb:1": {}}, {"movies": {}, "shows": {"tmdb:1": {}}})
    data = json.loads(target.read_text("utf-8"))
    assert "etag" not in data
    assert data["etags"] == {"shows": '"s"'}
    assert data["items"] == {"tmdb:1": {}}
    assert set(data["bucket_items"]) == {"movies", "shows"}


def test_collection_shadow_round_trips(monkeypatch, tmp_path):
    _prepare(monkeypatch, tmp_path, _col, "trakt_collection.shadow.json")
    _col._shadow_save({"media": None}, {"tmdb:9": {}}, {"media": {"tmdb:9": {}}})
    loaded = _col._shadow_load()
    assert loaded["etags"] == {}
    assert loaded["items"] == {"tmdb:9": {}}
