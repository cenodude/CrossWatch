# tests/test_ttl_dedupe_db.py
# CrossWatch - SQLite TTL dedupe tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from cw_platform.local_db import close_conn
from cw_platform.local_db.ttl_dedupe import clear_namespace, once_per_ttl


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "crosswatch.sqlite3"))
    monkeypatch.chdir(tmp_path)
    close_conn()
    yield tmp_path
    close_conn()


def test_ttl_dedupe_round_trips_through_database(isolated_db) -> None:
    assert once_per_ttl(isolated_db, "auto_remove_seen", "movie:tmdb:1", ttl_seconds=60) is True
    assert once_per_ttl(isolated_db, "auto_remove_seen", "movie:tmdb:1", ttl_seconds=60) is False

    assert clear_namespace(isolated_db, "auto_remove_seen") == 1
    assert once_per_ttl(isolated_db, "auto_remove_seen", "movie:tmdb:1", ttl_seconds=60) is True


def test_scrobble_auto_remove_dedupe_uses_database_not_json(isolated_db) -> None:
    from providers.scrobble.trakt import sink as trakt_sink

    assert trakt_sink._ar_seen("trakt:default|movie:tmdb:949") is False
    assert trakt_sink._ar_seen("trakt:default|movie:tmdb:949") is True
    assert not (isolated_db / ".cw_state" / "auto_remove_seen.json").exists()
