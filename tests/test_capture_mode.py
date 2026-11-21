# tests/test_capture_mode.py
# CrossWatch - Capture mode stays inside the operation and provider that asked for it
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import importlib
import os
import threading
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pytest

from cw_platform import capture_mode
from cw_platform.id_map import canonical_key
from cw_platform.log_context import log_run_id
from services import snapshots
from test_plex_history_options import isolated_state

ENV = ("CW_CAPTURE_MODE", "CW_CAPTURE_PROVIDER", "CW_CAPTURE_INSTANCE", "CW_CAPTURE_FEATURE", "CW_CAPTURE_ID", "CW_CAPTURE_PROGRESS_ID")
HELPERS = [
    ("providers.sync.anilist._common", "_is_capture_mode", "ANILIST"),
    ("providers.sync.crosswatch._common", "_capture_mode", "CROSSWATCH"),
    ("providers.sync.emby._common", "_is_capture_mode", "EMBY"),
    ("providers.sync.jellyfin._common", "_is_capture_mode", "JELLYFIN"),
    ("providers.sync.kodi._common", "_is_capture_mode", "KODI"),
    ("providers.sync.mdblist._common", "_is_capture_mode", "MDBLIST"),
    ("providers.sync.plex._common", "_is_capture_mode", "PLEX"),
    ("providers.sync.publicmetadb._common", "_is_capture_mode", "PUBLICMETADB"),
    ("providers.sync.simkl._common", "_is_capture_mode", "SIMKL"),
    ("providers.sync.stremio._common", "is_capture_mode", "STREMIO"),
    ("providers.sync.tautulli._common", "_is_capture_mode", "TAUTULLI"),
    ("providers.sync.tmdb._common", "_is_capture_mode", "TMDB"),
    ("providers.sync.tofa._common", "is_capture_mode", "TOFA"),
    ("providers.sync.trakt._common", "_is_capture_mode", "TRAKT"),
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("module,helper,provider", HELPERS)
def test_each_provider_only_follows_its_own_capture(module, helper, provider):
    check = getattr(importlib.import_module(module), helper)
    other = "TRAKT" if provider != "TRAKT" else "PLEX"
    assert check() is False
    with capture_mode.capture(other):
        assert check() is False
    with capture_mode.capture(provider):
        assert check() is True
        with capture_mode.writes():
            assert check() is False


def test_a_capture_is_invisible_to_other_threads():
    seen = {}
    with capture_mode.capture("PLEX"):
        worker = threading.Thread(target=lambda: seen.update(active=capture_mode.active("PLEX")))
        worker.start()
        worker.join(5)
    assert seen == {"active": False}


def test_pool_workers_get_the_capture_but_nothing_else():
    token = log_run_id.set("run-1")
    try:
        with capture_mode.capture("PLEX", progress_id="p1"), ThreadPoolExecutor(max_workers=2) as pool:
            seen = list(capture_mode.scoped_map(pool, lambda _: (capture_mode.active("PLEX"), capture_mode.progress_id(), log_run_id.get()), range(4)))
        after = list(ThreadPoolExecutor(max_workers=1).map(lambda _: capture_mode.active("PLEX"), range(1)))
    finally:
        log_run_id.reset(token)
    assert seen == [(True, "p1", "")] * 4
    assert after == [False]


@pytest.mark.parametrize("provider,expected", [("", {"PLEX": True, "EMBY": True}), ("PLEX", {"PLEX": True, "EMBY": False})])
def test_a_process_wide_flag_still_works_for_outside_callers(monkeypatch, provider, expected):
    monkeypatch.setenv("CW_CAPTURE_MODE", "1")
    monkeypatch.setenv("CW_CAPTURE_PROVIDER", provider)
    assert {name: capture_mode.active(name) for name in expected} == expected
    with capture_mode.writes():
        assert not capture_mode.active("PLEX")


def test_captures_and_cleanups_never_touch_the_process_environment():
    seen = []
    item = {"type": "movie", "ids": {"plex": "123", "tmdb": 949}}

    class Ops:
        def build_index(self, cfg, *, feature):
            seen.append((os.environ.get("CW_CAPTURE_MODE"), capture_mode.active("PLEX"), canonical_key(item)))
            return {}

    snapshots._build_index_capture_mode(ops=Ops(), cfg_view={}, pid="PLEX", instance="default", feat="history",
                                        ts=datetime(2026, 11, 21, tzinfo=timezone.utc))
    with snapshots._capture_mode_env(pid="PLEX", instance="default", feat="history"):
        Ops().build_index({}, feature="history")
    assert seen == [(None, True, "plex:123")] * 2
    assert canonical_key(item) == "tmdb:949"
    assert all(os.environ.get(name) is None for name in ENV)


def test_wetrakr_reads_its_instance_from_the_capture():
    from providers.sync import _mod_WETRAKR

    with capture_mode.capture("WETRAKR", "P01"):
        assert _mod_WETRAKR._current_instance_id({}) == "P01"


def test_nothing_reads_the_capture_environment_directly():
    root = Path(__file__).resolve().parents[1]
    allowed = {root / "cw_platform" / "capture_mode.py"}
    offenders = [
        str(path.relative_to(root)) for folder in ("api", "cw_platform", "providers", "services")
        for path in (root / folder).rglob("*.py")
        if path not in allowed and "tests" not in path.parts and "CW_CAPTURE_" in path.read_text(encoding="utf-8", errors="ignore")
    ]
    assert offenders == []


def test_plex_rating_workers_keep_the_capture(isolated_state, monkeypatch):
    from providers.sync.plex import _ratings as ratings

    rows = [{"type": "movie", "ratingKey": str(key), "title": f"Movie {key}", "year": 1990, "userRating": 8} for key in (41, 42, 43)]

    def get(url, *, params=None, **kwargs):
        found = rows if (params or {}).get("type") == 1 else []
        return SimpleNamespace(ok=True, status_code=200, headers={"Content-Type": "application/json"},
                               json=lambda: {"MediaContainer": {"Metadata": found, "totalSize": len(found)}})

    server = SimpleNamespace(baseurl="http://plex", _token="token", _session=SimpleNamespace(headers={}, get=get))
    adapter = SimpleNamespace(client=SimpleNamespace(server=server), libraries=lambda **kw: [], config={"plex": {"rating_workers": 2}})
    monkeypatch.setattr(ratings, "home_scope_enter", lambda _: (False, False, None, None))
    monkeypatch.setattr(ratings, "home_scope_exit", lambda *a: None)
    monkeypatch.setattr(ratings, "_fb_cache_flush", lambda: None)
    seen = []

    def resolved(row, **kwargs):
        seen.append((threading.current_thread().name.startswith("plex-ratings"), capture_mode.active("PLEX")))
        return {"type": "movie", "title": row["title"], "ids": {"plex": row["ratingKey"], "tmdb": int(row["ratingKey"]) + 1000}}

    monkeypatch.setattr(ratings, "minimal_from_history_row", resolved)
    with capture_mode.capture("PLEX"):
        captured = ratings.build_index(adapter)
    seen_after = list(seen)
    seen.clear()
    plain = ratings.build_index(adapter)
    assert sorted(captured) == ["plex:41", "plex:42", "plex:43"]
    assert seen_after and all(seen_after) and len(seen_after) == 3
    assert sorted(plain) == ["tmdb:1041", "tmdb:1042", "tmdb:1043"]
    assert seen == [(True, False)] * 3


def test_a_silo_capture_reads_its_state_but_never_writes_it(tmp_path, monkeypatch):
    from providers.sync.silo import _common as silo

    monkeypatch.setattr(silo, "STATE_DIR", tmp_path)
    silo.write_state("watchlist_series", "scope", {"series": ["series-1"]})
    with capture_mode.capture("SILO"):
        assert silo.read_state("watchlist_series", "scope") == {"series": ["series-1"]}
        silo.write_state("watchlist_series", "scope", {"series": []})
        silo.write_state("details", "scope", {"x": 1})
    with capture_mode.capture("PLEX"):
        silo.write_state("details", "scope", {"x": 2})
    assert silo.read_state("watchlist_series", "scope") == {"series": ["series-1"]}
    assert silo.read_state("details", "scope") == {"x": 2}
