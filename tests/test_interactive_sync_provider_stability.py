# tests/test_interactive_sync_provider_stability.py
# CrossWatch - Interactive Sync Provider Read Stability Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
import importlib
import json
from types import SimpleNamespace

import pytest

from cw_platform.id_map import canonical_key, minimal
from cw_platform.orchestrator._interactive import InteractivePlan


def assert_stable(feature, provider, first, second):
    from cw_platform.orchestrator._snapshots import canonicalize_index

    first = canonicalize_index(first, feature=feature)
    second = canonicalize_index(second, feature=feature)
    assert first and second
    conflict = InteractivePlan()
    conflict.conflict(feature, "item", "SRC", provider, next(iter(first.values())), {}, provider)
    repeated = InteractivePlan(choices=conflict.choices)
    repeated.conflict(feature, "item", "SRC", provider, next(iter(second.values())), {}, provider)
    assert set(conflict.conflicts) == set(repeated.conflicts)
    for operation in ("add", "update", "remove"):
        plan = InteractivePlan()
        plan.filter(feature, provider, "default", operation, [minimal(v) for v in first.values()], before=first)
        selected = InteractivePlan(preview=False, selected=set(plan.rows))
        kept = selected.filter(feature, provider, "default", operation, [minimal(v) for v in second.values()], before=second)
        assert len(kept) == len(first), (provider, feature, operation, first, second)


@pytest.mark.parametrize("kind", ["movies", "shows", "anime"])
@pytest.mark.parametrize("rated_at", ["", "2026-08-01T12:00:00Z"])
@pytest.mark.parametrize("interactive", [False, True])
def test_simkl_ratings_live_and_cached_reads_match(config_base, monkeypatch, kind, rated_at, interactive):
    from providers.sync.simkl import _ratings as ratings

    now = [1788650000]
    monkeypatch.setenv("CW_PAIR_KEY", "stability")
    watermark = {}
    monkeypatch.setattr(ratings, "_shadow_path", lambda: str(config_base / "ratings-shadow.json"))
    monkeypatch.setattr(ratings, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(ratings, "normalize_flat_watermarks", lambda: None)
    monkeypatch.setattr(ratings, "_now", lambda: now[0])
    monkeypatch.setattr(ratings, "get_watermark", lambda feature: watermark.get(feature))
    monkeypatch.setattr(ratings, "update_watermark_if_new", lambda feature, value: watermark.update({feature: max(watermark.get(feature, ""), value)}) if value else None)
    monkeypatch.setattr(ratings, "fetch_activities", lambda *a, **k: ({"movies": {"rated_at": "2026-09-01T12:00:00Z"}}, {}))
    media = "movie" if kind == "movies" else "show" if kind == "shows" else "anime"
    row = {media: dict(title="Example", year=2026, ids={"simkl": 123, "tmdb": 456}), "user_rating": 8, "user_rated_at": rated_at}
    rows = {bucket: [row] if bucket == kind else [] for bucket in ("movies", "shows", "anime")}
    fetches = []

    def fetch(*args, **kwargs):
        fetches.append(True)
        return deepcopy(rows), True

    monkeypatch.setattr(ratings, "_fetch_current", fetch)
    adapter = SimpleNamespace(client=SimpleNamespace(session=None), cfg=SimpleNamespace(timeout=5), config={"_cw_interactive_planned_at": now[0]} if interactive else {})
    first = ratings.build_index(adapter)
    now[0] += 3600
    second = ratings.build_index(adapter)
    if interactive or rated_at:
        assert_stable("ratings", "SIMKL", first, second)
    else:
        assert next(iter(first.values()))["rated_at"] == ""
        assert next(iter(second.values()))["rated_at"] == ratings._as_iso(1788650000)
    now[0] += 3600
    assert_stable("ratings", "SIMKL", second, ratings.build_index(adapter))
    assert len(fetches) == 1
    assert watermark["ratings"] == "2026-09-01T12:00:00Z"
    if interactive:
        watermark.clear()
        assert_stable("ratings", "SIMKL", second, ratings.build_index(adapter))


@pytest.mark.parametrize("provider,feature", [("trakt", "history"), ("trakt", "ratings"), ("trakt", "watchlist"), ("trakt", "collection"), ("mdblist", "history"), ("mdblist", "ratings"), ("mdblist", "watchlist"), ("mdblist", "collection"), ("simkl", "history"), ("simkl", "watchlist")])
@pytest.mark.parametrize("kind", ["movie", "episode"])
def test_provider_cache_roundtrip_keeps_review_identity(config_base, monkeypatch, provider, feature, kind):
    from cw_platform.history_events import history_event_key

    monkeypatch.setenv("CW_PAIR_KEY", "stability")
    monkeypatch.delenv("CW_CAPTURE_MODE", raising=False)
    mod = importlib.import_module(f"providers.sync.{provider}._{feature}")
    if feature == "watchlist" and kind == "episode":
        kind = "show"
    item = dict(type=kind, title="Example" if kind == "movie" else "S01E02", year=2026, ids={"tmdb": "456", "imdb": "tt1234567"})
    if kind == "episode":
        item.update(series_title="Example series", season=1, episode=2, show_ids={"tvdb": "99"})
    item.update({"history": dict(watched=True, watched_at="2026-08-01T12:00:00Z", provider_event_id="123"), "ratings": dict(rating=8, rated_at="2026-08-01T12:00:00Z"), "collection": dict(collected_at="2026-08-01T12:00:00Z"), "watchlist": {}}[feature])
    key = history_event_key(item) if feature == "history" else canonical_key(item)
    first = {key: item}
    is_cache = feature in ("history", "ratings")
    path_name = "_cache_path" if is_cache else "_shadow_path"
    monkeypatch.setattr(mod, path_name, lambda: config_base / "cache.json")
    if provider == "trakt" and is_cache:
        mod._save_cache_doc(first, "2026-08-01T12:00:00Z" if feature == "history" else {"movies": "2026-08-01T12:00:00Z"})
        second = mod._load_cache_doc()["items"]
    elif provider == "mdblist" and is_cache:
        mod._save_cache(first)
        second = mod._load_cache()
    elif provider == "simkl" and feature == "history":
        mod._cache_save(first, rewatches=True)
        second = mod._cache_load()
    else:
        if provider == "trakt" and feature == "collection":
            mod._shadow_save({"movies": "etag"}, first, {"movies": first})
        elif provider in ("trakt", "simkl"):
            mod._shadow_save("etag", first)
        else:
            mod._shadow_save(first)
        second = mod._shadow_load()["items"]
    assert_stable(feature, provider.upper(), first, second)


@pytest.mark.parametrize("feature", ["watchlist", "history", "ratings", "progress", "collection"])
def test_crosswatch_readonly_feature_reread_is_stable(config_base, monkeypatch, feature):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule
    from providers.tests.test_crosswatch_collection import _cfg
    from test_interactive_sync_features import feature_item

    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    monkeypatch.setenv("CW_PAIR_KEY", "stability")
    current = feature_item(feature, 1)
    path = config_base / f"{feature}.stability.json"
    path.write_text(json.dumps({"items": {canonical_key(current): current}}), encoding="utf-8")
    cfg = dict(_cfg(config_base), _cw_readonly=True, _cw_interactive_planned_at=1788650000)
    before = path.read_bytes()
    first = CROSSWATCHModule(cfg).build_index(feature)
    second = CROSSWATCHModule(cfg).build_index(feature)
    assert_stable(feature, "CROSSWATCH", first, second)
    assert path.read_bytes() == before


@pytest.mark.parametrize("feature", ["ratings", "progress"])
@pytest.mark.parametrize("interactive", [False, True])
def test_kodi_uncommitted_observation_times_are_stable(config_base, monkeypatch, feature, interactive):
    from providers.sync.kodi import _common as common
    from providers.tests.test_kodi_sync import FakeKodiClient, adapter, movie

    monkeypatch.setattr(common, "_load_kodi_feature_baseline", lambda *a: {})
    now = ["2026-09-01T12:00:00Z"]
    monkeypatch.setattr(common, "_utc_now_iso", lambda: now[0])

    def read():
        current = adapter(FakeKodiClient(movies=[movie()]))
        current.config = {"_cw_interactive_planned_at": 1788650000} if interactive else {}
        return common.feature_index(current, feature)

    first = read()
    now[0] = "2026-09-01T13:00:00Z"
    second = read()
    if interactive:
        assert_stable(feature, "KODI", first, second)
    else:
        field = "rated_at" if feature == "ratings" else "progress_at"
        assert next(iter(first.values()))[field] == "2026-09-01T12:00:00Z"
        assert next(iter(second.values()))[field] == "2026-09-01T13:00:00Z"


@pytest.mark.parametrize("feature", ["ratings", "progress"])
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_kodi_observation_time_survives_session_preflight_and_execution(config_base, monkeypatch, feature, mode):
    from api import syncAPI
    from providers.sync.kodi import _common as common
    from providers.tests.test_kodi_sync import FakeKodiClient, adapter, movie
    from services import interactive_sync as svc
    from test_interactive_sync_features import feature_setup

    cfg, src, dst = feature_setup(config_base, monkeypatch, feature, [], [], mode)
    monkeypatch.setattr(common, "_load_kodi_feature_baseline", lambda *a: {})
    times = iter(["2026-09-01T12:00:00Z", "2026-09-01T13:00:00Z", "2026-09-01T14:00:00Z"])

    def read(config, *, feature, **kwargs):
        current = adapter(FakeKodiClient(movies=[movie()]))
        current.config = config
        monkeypatch.setattr(common, "_utc_now_iso", lambda: next(times))
        return common.feature_index(current, feature)

    monkeypatch.setattr(src, "build_index", read)
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        assert session.store.counts["changes"] == 1
        svc.apply(session, cfg, session.store.selected_ids())
        assert session.status == "complete", session.message
        assert len(dst.add_calls) == 1
        assert session.summary["not_applied"] == 0
    finally:
        session.close()


@pytest.mark.parametrize("interactive", [False, True])
def test_crosswatch_readonly_ratings_without_dates_are_stable(config_base, monkeypatch, interactive):
    from providers.sync.crosswatch import _ratings as ratings
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule
    from providers.tests.test_crosswatch_collection import _cfg

    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    monkeypatch.setenv("CW_PAIR_KEY", "stability")
    cfg = dict(_cfg(config_base), _cw_readonly=True)
    if interactive:
        cfg["_cw_interactive_planned_at"] = 1788650000
    adapter = CROSSWATCHModule(cfg)
    path = ratings._ratings_path(adapter)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"items": {"tmdb:123": dict(type="movie", ids={"tmdb": "123"}, rating=8)}}), encoding="utf-8")
    before = path.read_bytes()
    monkeypatch.setattr(ratings, "_now_iso_z", lambda: "2026-09-01T12:00:00Z")
    first = ratings.build_index(adapter)
    monkeypatch.setattr(ratings, "_now_iso_z", lambda: "2026-09-01T13:00:00Z")
    second = ratings.build_index(adapter)
    if interactive:
        assert_stable("ratings", "CROSSWATCH", first, second)
    else:
        assert next(iter(first.values()))["rated_at"] == "2026-09-01T12:00:00Z"
        assert next(iter(second.values()))["rated_at"] == "2026-09-01T13:00:00Z"
    assert path.read_bytes() == before


def test_interactive_clock_does_not_leak_to_other_orchestrators(config_base, monkeypatch):
    from cw_platform.orchestrator import Orchestrator
    from test_interactive_sync import setup_ops
    from test_orchestrator_dry_run_no_side_effects import _cfg

    setup_ops(config_base, monkeypatch)
    cfg = _cfg(False)
    before = deepcopy(cfg)
    interactive = Orchestrator(cfg, interactive=InteractivePlan(planned_at=1788650000))
    assert interactive.context.config["_cw_interactive_planned_at"] == 1788650000
    assert "_cw_interactive_planned_at" not in interactive.cfg
    assert "_cw_interactive_planned_at" not in Orchestrator(cfg).context.config
    assert cfg == before


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("feature,field,value", [("watchlist", "title", "Corrected title"), ("history", "watched_at", "2026-09-01T12:00:00Z"), ("ratings", "rating", 4), ("progress", "progress_ms", 60000), ("collection", "collected_at", "2026-09-01T12:00:00Z")])
def test_real_feature_changes_still_invalidate_selection(config_base, monkeypatch, mode, feature, field, value):
    from test_interactive_sync import run
    from test_interactive_sync_features import feature_item, feature_setup

    cfg, src, dst = feature_setup(config_base, monkeypatch, feature, [feature_item(feature, 1)], [], mode)
    initial = InteractivePlan()
    run(cfg, initial)
    assert initial.rows
    next(iter(src.index.values()))[field] = value
    execution = InteractivePlan(preview=False, selected=set(initial.rows), planned_at=initial.planned_at)
    run(cfg, execution)
    assert not execution.seen
    assert not dst.add_calls
