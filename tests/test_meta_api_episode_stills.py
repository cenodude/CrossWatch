from __future__ import annotations

from pathlib import Path

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

    assert Path(path).name == "tv_95738_s3_e2_still_w300.jpg"
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
