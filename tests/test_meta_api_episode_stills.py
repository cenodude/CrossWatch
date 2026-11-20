from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.responses import FileResponse

from api import metaAPI


def test_episode_still_art_is_fetched_and_cached(tmp_path, monkeypatch) -> None:
    fetch_calls: list[tuple[str | int, int, int]] = []
    download_calls: list[str] = []

    def fake_fetch(api_key: str, show_tmdb_id: str | int, season: int, episode: int):
        fetch_calls.append((show_tmdb_id, season, episode))
        return [
            {
                "path": "/episode-still.jpg",
                "url": "https://image.tmdb.org/t/p/original/episode-still.jpg",
                "vote_average": 8.5,
                "vote_count": 10,
            }
        ]

    def fake_download(url: str, dest_path: Path, timeout: float = 15.0):
        download_calls.append(url)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(b"image")
        return dest_path, "image/jpeg"

    monkeypatch.setattr(metaAPI, "_tmdb_fetch_episode_stills", fake_fetch)
    monkeypatch.setattr(metaAPI, "_cache_download", fake_download)

    path, mime = metaAPI.get_episode_still_file("tmdb-key", 95738, 3, 2, "w300", tmp_path)
    path2, mime2 = metaAPI.get_episode_still_file("tmdb-key", 95738, 3, 2, "w300", tmp_path)

    name = Path(path).name
    assert name.startswith("tv_still_")
    assert name.endswith(".jpg")
    assert path2 == path
    assert mime == "image/jpeg"
    assert mime2 == "image/jpeg"
    assert fetch_calls == [(95738, 3, 2)]
    assert download_calls == ["https://image.tmdb.org/t/p/w300/episode-still.jpg"]


def test_episode_still_falls_back_to_show_art_when_no_still(tmp_path, monkeypatch) -> None:
    art_calls: list[str] = []

    def fake_art_file(
        api_key: str,
        typ: str,
        tmdb_id: str | int,
        size: str,
        cache_dir: Path | str,
        locale: str | None = None,
        *,
        kind: str = "poster",
    ):
        art_calls.append(kind)
        if kind == "backdrop":
            return "/app/assets/img/placeholder_poster.svg", "image/svg+xml"
        return str(tmp_path / "show-poster.jpg"), "image/jpeg"

    monkeypatch.setattr(metaAPI, "_tmdb_fetch_episode_stills", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(metaAPI, "get_art_file", fake_art_file)

    path, mime = metaAPI.get_episode_still_file("tmdb-key", 95738, 3, 2, "w300", tmp_path)

    assert path == str(tmp_path / "show-poster.jpg")
    assert mime == "image/jpeg"
    assert art_calls == ["backdrop", "poster"]


@pytest.mark.parametrize("mode,tmdb_hit,fanart_hit,expected", [
    ("fanart", True, True, "fanart"),
    ("fanart", True, False, "backdrop"),
    ("tmdb_first", True, True, "backdrop"),
    ("tmdb_first", False, True, "fanart"),
    ("tmdb_first", False, False, "poster"),
    ("tmdb", True, True, "backdrop"),
    ("tmdb", False, True, "poster"),
])
def test_missing_episode_still_respects_show_background_provider_order(tmp_path, monkeypatch, mode, tmdb_hit, fanart_hit, expected):
    calls = []
    art_dir = tmp_path / "art"
    art_dir.mkdir()
    monkeypatch.setattr(metaAPI, "load_config", lambda: {"tmdb": {"api_key": "key"}})
    monkeypatch.setattr(metaAPI, "_env", lambda: (None, tmp_path, None))
    monkeypatch.setattr(metaAPI._artwork, "revision", lambda *_args: "")
    monkeypatch.setattr(metaAPI, "_fanart_mode", lambda kind: mode if kind == "backdrop" else "tmdb")
    monkeypatch.setattr(metaAPI, "_tmdb_fetch_episode_stills", lambda *_args: [])

    def fanart(*_args, **_kwargs):
        calls.append("fanart")
        return (FileResponse(art_dir / "fanart.jpg", media_type="image/jpeg") if fanart_hit else None), None

    def tmdb(*_args, **kwargs):
        kind = kwargs["kind"]
        calls.append(kind)
        if kind == "backdrop" and not tmdb_hit:
            return str(metaAPI._placeholder_poster()), "image/svg+xml"
        return str(art_dir / f"{kind}.jpg"), "image/jpeg"

    monkeypatch.setattr(metaAPI, "_fanart_tmdb_override", fanart)
    monkeypatch.setattr(metaAPI, "get_art_file", tmdb)
    request = SimpleNamespace(headers={}, client=None, url=SimpleNamespace(path="/art/tmdb/tv/213375"))
    response = metaAPI.api_tmdb_art(
        request, "tv", 213375, size="w300", kind="still", season=1, episode=2,
        locale="en-US", title=None, year=None, av=None, ov=None,
    )

    assert str(response.path) == str(art_dir / f"{expected}.jpg")
    assert response.headers["cache-control"] == "no-cache"
    if mode == "fanart":
        assert calls[0] == "fanart"
    else:
        assert calls[0] == "backdrop"
    if mode == "tmdb" or (mode == "tmdb_first" and tmdb_hit):
        assert "fanart" not in calls
