# tests/test_tmdb_metadata_key.py
# CrossWatch - TMDb metadata lookups use the metadata key, never the sync key
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import pytest

from api import manualAPI, wallAPI, watchlistAPI
from services import analyzer
from services.interactive_sync_episodes import metadata_key


SYNC = {"api_key": "sync-key", "session_id": "sync-session", "instances": {"TMDB-P01": {"api_key": "sync-p01"}}}
SYNC_ONLY = {"tmdb": {"api_key": ""}, "tmdb_sync": SYNC}
BOTH = {"tmdb": {"api_key": "metadata-key"}, "tmdb_sync": SYNC}


@pytest.mark.parametrize("lookup", [manualAPI._tmdb_api_key, watchlistAPI._tmdb_api_key, wallAPI._tmdb_api_key])
def test_metadata_lookups_never_borrow_the_sync_key(lookup):
    assert lookup(SYNC_ONLY) == ""
    assert lookup(BOTH) == "metadata-key"


@pytest.mark.parametrize(("cfg", "expected"), [(SYNC_ONLY, ""), (BOTH, "metadata-key")])
def test_analyzer_uses_only_the_metadata_key(monkeypatch, cfg, expected):
    monkeypatch.setattr(analyzer, "_cfg", lambda: cfg)
    assert analyzer._tmdb_key() == expected


@pytest.mark.parametrize(("cfg", "expected"), [(SYNC_ONLY, ""), (BOTH, "metadata-key")])
def test_episode_suggestions_for_tmdb_sync_rows_use_the_metadata_key(cfg, expected):
    assert metadata_key(cfg, {"provider": "TMDB", "instance": "default"}) == expected
