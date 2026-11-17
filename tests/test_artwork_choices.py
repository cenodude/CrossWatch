# tests/test_artwork_choices.py
# CrossWatch - Saved artwork choices, pinned copies and the art route
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from api import metaAPI
from services import artwork_choices

FAKE_REQUEST: Any = SimpleNamespace(headers={}, client=None, url=SimpleNamespace(path="/art/tmdb"))
FANART_URL = "https://assets.fanart.tv/fanart/movies/603/movieposter/a.jpg"


class FakeStream:
    def __init__(self, status: int = 200, body: bytes = b"img", content_type: str = "image/jpeg") -> None:
        self.status_code = status
        self.headers = {"Content-Type": content_type}
        self._body = body

    def __enter__(self) -> "FakeStream":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")

    def iter_content(self, size: int):
        yield self._body


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path / "cfg"))
    artwork_choices.clear_memory()
    calls: list[str] = []
    monkeypatch.setattr(artwork_choices.requests, "get", lambda url, **kw: (calls.append(url), FakeStream())[1])
    yield calls
    artwork_choices.clear_memory()


def _route(monkeypatch, tmp_path, **kw: Any):
    monkeypatch.setattr(metaAPI, "load_config", lambda: {"tmdb": {"api_key": "t"}})
    monkeypatch.setattr(metaAPI, "_env", lambda: (None, tmp_path / "cache", lambda: {}))
    auto = tmp_path / "cache" / "art" / "auto.jpg"
    auto.parent.mkdir(parents=True, exist_ok=True)
    auto.write_bytes(b"auto")
    monkeypatch.setattr(metaAPI, "get_art_file", lambda *a, **k: (str(auto), "image/jpeg"))
    args = {"typ": "movie", "tmdb_id": 603, "size": "w342", "kind": "poster", "season": None, "episode": None, "locale": "en-US", "title": None, "year": None, "av": None, "ov": None}
    args.update(kw)
    return metaAPI.api_tmdb_art(FAKE_REQUEST, **args)


def _download_into_cache(monkeypatch, fail: bool = False) -> list[str]:
    urls: list[str] = []

    def download(url: str, dest: Path, timeout: float = 15.0):
        urls.append(url)
        if fail:
            raise RuntimeError("gone")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"chosen")
        return dest, "image/jpeg"

    monkeypatch.setattr(metaAPI, "_cache_download", download)
    return urls


def test_set_choice_pins_and_reset_clears(store, tmp_path) -> None:
    rev = artwork_choices.set_choice("movie", 603, "poster", "tmdb", "/abcd1234.jpg", lang="EN")
    assert rev and artwork_choices.revision("movie", "603") == rev
    assert store == ["https://image.tmdb.org/t/p/w780/abcd1234.jpg"]
    pinned = artwork_choices.pin_file("movie", 603, "poster")
    assert pinned and pinned[0].read_bytes() == b"img" and pinned[1] == "image/jpeg"
    saved = json.loads((tmp_path / "cfg" / ".cw_state" / "artwork" / "choices.json").read_text("utf-8"))
    assert saved["items"]["movie:603"]["poster"]["ref"] == "/abcd1234.jpg"
    assert saved["items"]["movie:603"]["poster"]["lang"] == "en"

    artwork_choices.clear_memory()
    assert artwork_choices.choice("movie", 603, "poster")["source"] == "tmdb"
    assert artwork_choices.revisions() == {"movie:603": rev}
    assert artwork_choices.revision("tv", 603) == ""

    assert artwork_choices.reset("movie", 603, "poster") == ""
    assert artwork_choices.pin_file("movie", 603, "poster") is None
    assert artwork_choices.revisions() == {}


def test_invalid_choices_are_rejected(store) -> None:
    for args in (
        ("movie", 603, "poster", "tmdb", "../etc/passwd"),
        ("movie", 603, "poster", "fanart", "https://evil.example/fanart/a.jpg"),
        ("movie", 603, "banner", "tmdb", "/abcd1234.jpg"),
        ("movie", "x", "poster", "tmdb", "/abcd1234.jpg"),
        ("movie", 603, "poster", "imdb", "/abcd1234.jpg"),
    ):
        with pytest.raises(artwork_choices.ArtworkChoiceError):
            artwork_choices.set_choice(*args)
    assert store == []
    assert artwork_choices.pin_file("movie", "../1", "poster") is None
    assert artwork_choices.revisions() == {}


def test_failed_download_keeps_previous_choice(store, monkeypatch) -> None:
    artwork_choices.set_choice("tv", 1399, "logo", "fanart", FANART_URL.replace(".jpg", ".png"))
    before = artwork_choices.revision("tv", 1399)
    monkeypatch.setattr(artwork_choices.requests, "get", lambda url, **kw: FakeStream(status=404))
    with pytest.raises(Exception):
        artwork_choices.set_choice("tv", 1399, "logo", "tmdb", "/other123.png")
    assert artwork_choices.revision("tv", 1399) == before
    assert artwork_choices.choice("tv", 1399, "logo")["source"] == "fanart"
    pinned = artwork_choices.pin_file("tv", 1399, "logo")
    assert pinned and pinned[0].suffix == ".png"


def test_art_route_serves_choice_with_matching_cache(store, tmp_path, monkeypatch) -> None:
    urls = _download_into_cache(monkeypatch)
    rev = artwork_choices.set_choice("movie", 603, "poster", "tmdb", "/abcd1234.jpg")

    fresh = _route(monkeypatch, tmp_path, ov=rev)
    assert Path(fresh.path).read_bytes() == b"chosen"
    assert urls == ["https://image.tmdb.org/t/p/w342/abcd1234.jpg"]
    assert "immutable" in fresh.headers["cache-control"]
    assert _route(monkeypatch, tmp_path).headers["cache-control"] == "no-cache"
    assert _route(monkeypatch, tmp_path, ov="old").headers["cache-control"] == "no-store"

    logo = _route(monkeypatch, tmp_path, kind="logo", size="w500")
    assert Path(logo.path).name == "auto.jpg"

    artwork_choices.reset("movie", 603)
    after = _route(monkeypatch, tmp_path, ov=rev)
    assert Path(after.path).name == "auto.jpg" and after.headers["cache-control"] == "no-store"
    plain = _route(monkeypatch, tmp_path)
    assert "immutable" in plain.headers["cache-control"]


def test_art_route_falls_back_to_pin_then_marks_missing(store, tmp_path, monkeypatch) -> None:
    _download_into_cache(monkeypatch, fail=True)
    rev = artwork_choices.set_choice("movie", 603, "poster", "tmdb", "/abcd1234.jpg")

    pinned = _route(monkeypatch, tmp_path, ov=rev)
    assert Path(pinned.path).read_bytes() == b"img"
    assert artwork_choices.revision("movie", 603) == rev

    artwork_choices.pin_file("movie", 603, "poster")[0].unlink()
    monkeypatch.setattr(artwork_choices.requests, "get", lambda url, **kw: FakeStream(status=404))
    gone = _route(monkeypatch, tmp_path, ov=rev)
    assert Path(gone.path).name == "auto.jpg"
    row = artwork_choices.choice("movie", 603, "poster")
    assert row["missing"] is True and row["missing_at"] > 0
    assert artwork_choices.revision("movie", 603) != rev


def test_fanart_choice_is_served_from_pin_and_repinned(store, tmp_path, monkeypatch) -> None:
    urls = _download_into_cache(monkeypatch)
    rev = artwork_choices.set_choice("movie", 603, "backdrop", "fanart", FANART_URL)
    res = _route(monkeypatch, tmp_path, kind="backdrop", size="w1280", ov=rev)
    assert Path(res.path).read_bytes() == b"img" and urls == []

    artwork_choices.pin_file("movie", 603, "backdrop")[0].unlink()
    again = _route(monkeypatch, tmp_path, kind="backdrop", size="w1280", ov=rev)
    assert Path(again.path).read_bytes() == b"img"
    assert store[-1] == FANART_URL


def test_options_merge_tmdb_and_fanart(store, monkeypatch) -> None:
    metaAPI._ARTWORK_OPTIONS.clear()
    monkeypatch.setattr(metaAPI, "load_config", lambda: {"tmdb": {"api_key": "t"}})
    images = {
        "posters": [{"file_path": "/poster111.jpg", "iso_639_1": "en", "width": 1000, "height": 1500, "vote_average": 5.3}],
        "logos": [{"file_path": "/logo2222.svg", "iso_639_1": None}, {"file_path": "../bad", "iso_639_1": "en"}],
        "backdrops": [],
    }
    calls: list[str] = []
    monkeypatch.setattr(metaAPI, "_tmdb_get_json", lambda url, params=None, **kw: (calls.append(url), images)[1])
    fanart = SimpleNamespace(
        configured=lambda: True,
        lookup=lambda t, ids: ({"x": 1}, True),
        images_from=lambda t, data: {"logo": [{"url": FANART_URL.replace(".jpg", ".png"), "lang": "en", "likes": 4}, {"url": "https://evil.example/a.png", "likes": 9}]},
    )
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: fanart)
    monkeypatch.setattr(metaAPI, "_fanart_ids", lambda prov, t, tmdb_id, **kw: ({"tmdb": str(tmdb_id)}, True))

    res = json.loads(metaAPI.api_artwork_options("movie", 603, "logo").body)
    assert res["ok"] and res["fanart"] is True and res["current"] is None
    assert [(o["source"], o["ref"]) for o in res["options"]] == [("tmdb", "/logo2222.png"), ("fanart", FANART_URL.replace(".jpg", ".png"))]
    assert res["options"][0]["thumb"] == "/art/tmdb/image?path=/logo2222.png&size=w185"
    assert res["options"][1]["thumb"].startswith("/api/artwork/fanart-thumb?u=https%3A%2F%2Fassets.fanart.tv")

    metaAPI.api_artwork_options("movie", 603, "poster")
    assert len(calls) == 1
    assert json.loads(metaAPI.api_artwork_options("movie", 603, "banner").body)["ok"] is False


def test_choose_and_reset_endpoints(store) -> None:
    res = json.loads(metaAPI.api_artwork_choose("show", 1399, {"kind": "poster", "source": "tmdb", "ref": "/abcd1234.jpg"}).body)
    assert res["ok"] and res["rev"] == artwork_choices.revision("tv", 1399)
    bad = metaAPI.api_artwork_choose("tv", 1399, {"kind": "poster", "source": "tmdb", "ref": "nope"})
    assert bad.status_code == 400
    assert json.loads(metaAPI.api_artwork_reset("tv", 1399, None).body) == {"ok": True, "rev": ""}
    assert metaAPI.api_artwork_reset("book", 1399, None).status_code == 400


DISC_URL = "https://assets.fanart.tv/fanart/movies/603/moviedisc/bd.png"


def test_disc_choice_is_fanart_and_movie_only(store) -> None:
    with pytest.raises(artwork_choices.ArtworkChoiceError):
        artwork_choices.set_choice("movie", 603, "disc", "tmdb", "/abcd1234.png")
    with pytest.raises(artwork_choices.ArtworkChoiceError):
        artwork_choices.set_choice("tv", 1399, "disc", "fanart", DISC_URL)
    rev = artwork_choices.set_choice("movie", 603, "disc", "fanart", DISC_URL)
    assert rev and artwork_choices.pin_file("movie", 603, "disc")[0].name == "movie_603_disc.png"
    assert artwork_choices.reset("movie", 603, "disc") == ""
    assert artwork_choices.pin_file("movie", 603, "disc") is None


def test_fanart_disc_route_serves_chosen_disc(store, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(metaAPI, "_env", lambda: (None, tmp_path / "cache", lambda: {}))
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: None)
    args = {"typ": "movie", "tmdb_id": 603, "kind": "disc", "disc": "bluray", "season": None, "tvdb": None, "imdb": None, "locale": None}
    assert metaAPI.api_fanart_art(**args, ov=None).status_code == 404
    rev = artwork_choices.set_choice("movie", 603, "disc", "fanart", DISC_URL)
    fresh = metaAPI.api_fanart_art(**args, ov=rev)
    assert Path(fresh.path).read_bytes() == b"img" and "immutable" in fresh.headers["cache-control"]
    assert metaAPI.api_fanart_art(**{**args, "disc": "dvd"}, ov="old").headers["cache-control"] == "no-cache"


def test_disc_options_come_from_fanart_only(store, monkeypatch) -> None:
    metaAPI._ARTWORK_OPTIONS.clear()
    monkeypatch.setattr(metaAPI, "load_config", lambda: {"tmdb": {"api_key": "t"}})
    calls: list[str] = []
    monkeypatch.setattr(metaAPI, "_tmdb_get_json", lambda url, params=None, **kw: (calls.append(url), {})[1])
    fanart = SimpleNamespace(
        configured=lambda: True,
        lookup=lambda t, ids: ({"x": 1}, True),
        images_from=lambda t, data: {"disc": [{"url": DISC_URL, "lang": "en", "likes": 2, "disc_type": "bluray"}]},
    )
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: fanart)
    monkeypatch.setattr(metaAPI, "_fanart_ids", lambda prov, t, tmdb_id, **kw: ({"tmdb": str(tmdb_id)}, True))
    res = json.loads(metaAPI.api_artwork_options("movie", 603, "disc").body)
    assert [(o["source"], o["disc_type"]) for o in res["options"]] == [("fanart", "bluray")] and calls == []
    assert metaAPI.api_artwork_options("tv", 1399, "disc").status_code == 400


def test_page_and_widget_urls_carry_choice_version(store, monkeypatch) -> None:
    import ui_frontend
    from services import dashboard_widgets

    rev = artwork_choices.set_choice("movie", 603, "poster", "tmdb", "/abcd1234.jpg")
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: {})
    html = ui_frontend._seed_media_card_style('<html lang="en">')
    assert f'data-cw-art-ov="movie:603={rev}"' in html
    assert dashboard_widgets._art_choice("movie", 603) == f"&ov={rev}"
    assert dashboard_widgets._art_choice("tv", 603) == ""


def test_fanart_nudge_dismissal_is_saved(monkeypatch) -> None:
    saved: dict[str, Any] = {"fanart": {}}
    monkeypatch.setattr(metaAPI, "load_config", lambda: {"fanart": dict(saved["fanart"])})
    monkeypatch.setattr(metaAPI, "save_config", lambda cfg: saved.update(cfg))
    assert metaAPI.api_fanart_settings({"nudge_dismissed": True})["ok"] is True
    assert saved["fanart"] == {"nudge_dismissed": True}
