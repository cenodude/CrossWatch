from copy import deepcopy
from contextlib import nullcontext
from types import SimpleNamespace
import json
import sys
import threading

import pytest
from fastapi import HTTPException

from api import editorAPI as api
from cw_platform.history_events import history_event_key
from cw_platform.id_map import canonical_key
from cw_platform.local_db import manual_policy, state as database
from cw_platform.pair_scope import pair_feature_scope
from services import editor_removal as removal


def movie(number=1, **extra):
    return dict(type="movie", title=f"Movie {number}", ids={"tmdb": str(number)}, **extra)


def episode(number, **extra):
    return dict(type="episode", title=f"Episode {number}", show_ids={"tmdb": "99"},
                ids={"tmdb": "99"}, season=1, episode=number, **extra)


class Ops:
    def __init__(self, provider):
        self.provider = provider
        self.live = {}
        self.calls = []
        self.fail = set()

    def features(self):
        return {feature: True for feature in removal.FEATURES}

    def capabilities(self):
        return {feature: {"remove": True}
                for feature in removal.FEATURES}

    def is_configured(self, cfg):
        return True

    def label(self):
        return self.provider.title()

    def build_index(self, cfg, *, feature):
        return deepcopy(self.live.get((cfg["instance"], feature), {}))

    def remove(self, cfg, items, *, feature, dry_run):
        self.calls.append((cfg["instance"], feature, deepcopy(items), dry_run))
        keys = []
        for item in items:
            key = item.get("_cw_event_key") or canonical_key(item)
            if key not in self.fail:
                if not dry_run:
                    self.live[(cfg["instance"], feature)].pop(key, None)
                keys.append(key)
        return dict(ok=True, confirmed_keys=keys, count=len(keys), unresolved=[])


@pytest.fixture
def setup(config_base, monkeypatch):
    cfg = {"pairs": [
        {"id": "p1", "source": "PLEX", "target": "TRAKT",
         "features": {f: {} for f in removal.FEATURES}},
        {"id": "p2", "source": "PLEX", "target": "TRAKT", "target_instance": "second",
         "features": {f: {} for f in removal.FEATURES}},
    ], "trakt": {"instances": {"second": {"label": "Family"}}}}
    ops = {name: Ops(name) for name in ("PLEX", "TRAKT", "MDBLIST")}
    monkeypatch.setattr(api, "_STATE_BASE", config_base)
    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(api, "sync_provider_names", lambda **kwargs: list(ops))
    monkeypatch.setattr(api, "load_sync_ops", ops.get)
    monkeypatch.setattr(removal, "load_sync_ops", ops.get)
    monkeypatch.setattr(removal, "build_provider_config_view",
                        lambda cfg, provider, instance: dict(provider=provider, instance=instance))
    monkeypatch.setattr(removal, "_sync_guard", nullcontext)
    scopes = {}

    def seed(pair_id, provider, instance, feature, items):
        pair = next(p for p in cfg["pairs"] if p["id"] == pair_id)
        scope = pair_feature_scope(cfg, pair, feature, cfg["pairs"].index(pair) + 1)
        scopes[(pair_id, feature)] = scope
        database.save_pair_blocks(config_base, scope, {
            (provider, instance, feature): {"baseline": {"items": deepcopy(items)}, "checkpoint": 123}
        })
        ops[provider].live[(instance, feature)] = deepcopy(items)

    def execute(payload, providers=None, **extra):
        preview = api.api_editor_remove_preview(payload, request=None)
        chosen = providers if providers is not None else [
            {k: p[k] for k in ("provider", "instance")} for p in preview["providers"]]
        return api.api_editor_send({**payload, "operation": "remove", "confirmed": True,
                                    "preview_id": preview["preview_id"], "providers": chosen, **extra}, request=None)

    return SimpleNamespace(cfg=cfg, ops=ops, seed=seed, execute=execute, root=config_base, scopes=scopes)


def test_targets_use_pair_baselines_and_friendly_instances_without_live_reads(setup):
    s = setup
    s.seed("p1", "PLEX", "default", "history", {"tmdb:1": movie()})
    s.seed("p1", "TRAKT", "default", "history", {"tmdb:1": movie()})
    s.seed("p2", "PLEX", "default", "history", {"tmdb:1": movie()})
    s.seed("p2", "TRAKT", "second", "history", {"tmdb:2": movie(2)})
    database.save_feature_baseline(s.root, provider="MDBLIST", feature="history", items={"tmdb:1": movie()})
    for ops in s.ops.values():
        ops.build_index = lambda *a, **k: pytest.fail("Preview must be local")
    payload = dict(kind="history", items=[movie(), movie(2)], source_provider="PLEX")
    preview = api.api_editor_remove_preview(payload, request=None)
    targets = {(p["provider"], p["instance"]): p for p in preview["providers"]}
    assert set(targets) == {("PLEX", "default"), ("TRAKT", "default"), ("TRAKT", "second")}
    assert targets[("PLEX", "default")]["count"] == 1
    assert targets[("TRAKT", "second")]["instance_label"] == "Family"
    assert targets[("TRAKT", "second")]["display"] == "Trakt (Family)"
    scoped = api.api_editor_remove_preview({**payload, "pair_id": "p1"}, request=None)
    assert all(p["instance"] == "default" for p in scoped["providers"])


def test_shared_state_without_pair_baselines_reports_missing_sync_evidence(setup):
    for provider in ("PLEX", "TRAKT", "MDBLIST"):
        database.save_feature_baseline(setup.root, provider=provider, feature="watchlist", items={"tmdb:1": movie()})
    preview = api.api_editor_remove_preview(dict(kind="watchlist", items=[movie()]), request=None)
    assert not preview["providers"]
    assert preview["empty_reason"] == "pair_baseline_unavailable"
    assert preview["missing_baselines"] > 0


def test_empty_saved_pair_is_distinct_from_a_missing_baseline(setup):
    setup.seed("p1", "PLEX", "default", "watchlist", {})
    setup.seed("p1", "TRAKT", "default", "watchlist", {})
    preview = api.api_editor_remove_preview(dict(kind="watchlist", pair_id="p1", items=[movie()]), request=None)
    assert not preview["providers"]
    assert preview["empty_reason"] == "no_matching_records"
    assert preview["missing_baselines"] == 0


@pytest.mark.parametrize("selector", ["watchlist", "multi"])
def test_targets_use_the_orchestrators_feature_selection_defaults(setup, selector):
    pair = setup.cfg["pairs"][0]
    pair.pop("features")
    pair["feature"] = selector
    setup.seed("p1", "TRAKT", "default", "watchlist", {"tmdb:1": movie()})
    preview = api.api_editor_remove_preview(dict(kind="watchlist", pair_id="p1", items=[movie()]), request=None)
    assert [p["provider"] for p in preview["providers"]] == ["TRAKT"]


def test_selective_removal_preserves_pair_history_checkpoint_and_policy(setup):
    s = setup
    rows = {"tmdb:1": movie(), "tmdb:2": movie(2)}
    s.seed("p1", "TRAKT", "default", "history", rows)
    policy = {"version": 1, "providers": {"TRAKT": {"history": {"blocks": ["tmdb:1"]}}}}
    manual_policy.save_policy(s.root, policy)
    result = s.execute(dict(kind="history", items=[movie()]))
    assert result["confirmed"] == 1
    assert list(s.ops["TRAKT"].live[("default", "history")]) == ["tmdb:2"]
    global_state = database.load_state_features(s.root, {"history"})
    assert list(removal._items(global_state, "TRAKT", "default", "history")) == ["tmdb:2"]
    assert global_state["providers"]["TRAKT"]["history"]["checkpoint"] == 123
    pair_state = database.load_pair_state(s.root, s.scopes[("p1", "history")], {"history"})
    assert len(removal._items(pair_state, "TRAKT", "default", "history")) == 2
    assert manual_policy.load_policy(s.root)["providers"]["TRAKT"]["history"]["blocks"] == ["tmdb:1"]
    again = s.execute(dict(kind="history", items=[movie()]))
    assert again["skipped"] == 1
    assert len(s.ops["TRAKT"].calls) == 1


@pytest.mark.parametrize("item", [movie(), episode(1)], ids=["movie", "episode"])
@pytest.mark.parametrize("outcome", ["cleared", "still_watched", "unchanged", "failed", "read_failed"])
@pytest.mark.parametrize("cached_dates", [False, True])
def test_floppy_history_removal_reuses_adapter_and_preserves_remaining_watches(setup, monkeypatch, item, outcome, cached_dates):
    from providers.sync._mod_FLOPPY import OPS
    from providers.sync.floppy import _history
    from test_floppy_sync import AdapterStub, ResponseStub
    s = setup
    s.cfg["pairs"][0]["target"] = "FLOPPY"
    ops = s.ops["FLOPPY"] = Ops("FLOPPY")
    ops.capabilities = OPS.capabilities
    latest = {**item, "watched_at": "2026-09-02T12:00:00Z", "_floppy_consumption_id": "42"}
    earlier_id = "42" if item["type"] == "movie" else "41"
    earlier = {**item, "watched_at": "2026-09-01T12:00:00Z", "_floppy_consumption_id": earlier_id}
    key = canonical_key(item)
    rows = {key: latest, "tmdb:2": movie(2)}
    s.seed("p1", "FLOPPY", "default", "history", rows)
    if cached_dates:
        database.save_feature_baseline(s.root, provider="FLOPPY", feature="history", checkpoint=123,
                                       items={history_event_key(v): v for v in (earlier, latest)} | {"tmdb:2": movie(2)})
    is_movie = item["type"] == "movie"
    path = "media/movie/tmdb/1/watch" if is_movie else "media/tv/tmdb/99/1/1/history/42"
    def delete(call):
        if outcome == "failed":
            return ResponseStub(500, {})
        if outcome == "cleared":
            ops.live[("default", "history")].pop(key)
        elif outcome == "still_watched":
            ops.live[("default", "history")][key] = earlier
        elif outcome == "read_failed":
            def failed_read(*args, **kwargs):
                raise RuntimeError("Provider unavailable")
            ops.build_index = failed_read
        return ResponseStub(204, {})
    remaining = [] if outcome == "cleared" else [{"consumption_id": 41, "end_date": earlier["watched_at"]}]
    adapter = AdapterStub({
        ("DELETE", path): delete,
        ("GET", "media/movie/tmdb/1/history"): {"results": remaining, "count": len(remaining)},
        ("PATCH", "media/movie/tmdb/1"): {"id": 1},
    })
    def remove(cfg, items, *, feature, dry_run):
        assert not cfg.get("_cw_history_rewatches")
        assert items[0]["_floppy_consumption_id"] == "42"
        adapter.config = cfg
        return _history.remove(adapter, items, dry_run=dry_run)
    monkeypatch.setattr(ops, "remove", remove)
    payload = dict(kind="history", items=[item], source_provider="FLOPPY")
    preview = api.api_editor_remove_preview(payload, request=None)
    assert [p["provider"] for p in preview["providers"]] == ["FLOPPY"]
    result = s.execute(payload)
    success = outcome in {"cleared", "still_watched"}
    assert result["ok"] is success
    assert result["confirmed"] == int(success)
    assert result["results"][0]["result"].get("still_watched", 0) == int(outcome == "still_watched")
    expected = [("DELETE", path)]
    if is_movie and outcome != "failed":
        expected.append(("GET", "media/movie/tmdb/1/history"))
        if outcome == "cleared":
            expected.append(("PATCH", "media/movie/tmdb/1"))
    assert [(c["method"], c["path"]) for c in adapter.client.session.calls] == expected
    state = database.load_state_features(s.root, {"history"})
    saved = removal._items(state, "FLOPPY", "default", "history")
    assert saved["tmdb:2"] == movie(2)
    assert state["providers"]["FLOPPY"]["history"]["checkpoint"] == 123
    if outcome == "cleared":
        assert set(saved) == {"tmdb:2"}
    elif outcome == "still_watched":
        assert [v["watched_at"] for k, v in saved.items() if k != "tmdb:2"] == [earlier["watched_at"]]
    else:
        assert any(v.get("watched_at") == latest["watched_at"] for v in saved.values())
    pair_state = database.load_pair_state(s.root, s.scopes[("p1", "history")], {"history"})
    assert removal._items(pair_state, "FLOPPY", "default", "history")[key]["_floppy_consumption_id"] == "42"


@pytest.mark.parametrize("feature", ["watchlist", "ratings", "progress", "collection"])
def test_floppy_still_allows_other_editor_removals(feature):
    from providers.sync._mod_FLOPPY import OPS
    assert removal._can_remove(OPS, feature, "tmdb:1", movie())


def test_partial_failure_never_prunes_unconfirmed_rows(setup):
    s = setup
    rows = {"tmdb:1": movie(), "tmdb:2": movie(2)}
    s.seed("p1", "TRAKT", "default", "history", rows)
    s.ops["TRAKT"].fail.add("tmdb:2")
    result = s.execute(dict(kind="history", items=list(rows.values())))
    assert result["ok"] is False
    assert result["confirmed"] == 1
    assert result["unresolved"] == 1
    assert list(removal._items(database.load_state_features(s.root, {"history"}), "TRAKT", "default", "history")) == ["tmdb:2"]


@pytest.mark.parametrize("provider", ["MDBLIST", "PLEX", "SIMKL", "TRAKT"])
@pytest.mark.parametrize("outcome", ["removed", "still_present", "wrong_confirmation"])
def test_removal_confirmation_uses_the_provider_write_identity(setup, provider, outcome):
    s = setup
    s.cfg["pairs"][0]["target"] = provider
    ops = s.ops.setdefault(provider, Ops(provider))
    item = movie()
    item["ids"].update(imdb="tt39316472", **{provider.lower(): "501"})
    assert canonical_key(item) == "tmdb:1"
    rows = {"tmdb:1": item, **{f"tmdb:{i}": movie(i) for i in range(2, 7)}}
    s.seed("p1", provider, "default", "watchlist", rows)
    def remove(cfg, items, *, feature, dry_run):
        # Production adapter confirmations use canonical_key while capture mode
        # prefers the destination's native ID over the usual TMDB ID.
        confirmed = canonical_key(items[0])
        assert confirmed == f"{provider.lower()}:501"
        ops.calls.append(confirmed)
        if outcome != "still_present":
            ops.live[("default", feature)].pop("tmdb:1")
        return dict(ok=True, count=1, confirmed_keys=[confirmed if outcome != "wrong_confirmation" else "tmdb:999"])
    ops.remove = remove
    result = s.execute(dict(kind="watchlist", items=[item], source_provider=provider))
    assert result["errors"] == 0
    assert ops.calls == [f"{provider.lower()}:501"]
    assert result["confirmed"] == (1 if outcome == "removed" else 0)
    assert result["unresolved"] == (0 if outcome == "removed" else 1)
    saved = removal._items(database.load_state_features(s.root, {"watchlist"}), provider, "default", "watchlist")
    assert ("tmdb:1" in saved) is (outcome != "removed")
    assert set(saved) - {"tmdb:1"} == {f"tmdb:{i}" for i in range(2, 7)}


@pytest.mark.parametrize("event_field", ["key", "_cw_event_key"])
@pytest.mark.parametrize("item", [movie(), episode(1)], ids=["movie", "episode"])
def test_dated_history_selection_never_clears_a_status_only_title(setup, item, event_field):
    s = setup
    dated = {**item, "watched_at": "2026-09-01T12:00:00Z", "_trakt_history_id": "101"}
    s.seed("p1", "TRAKT", "default", "history", {canonical_key(item): dated, "tmdb:2": movie(2)})
    selected = {**dated, event_field: history_event_key(dated)}
    preview = api.api_editor_remove_preview(dict(kind="history", items=[selected, selected], source_provider="TRAKT"), request=None)
    assert preview["selected"] == 1
    assert not preview["providers"]


def _viewing_setup(s, provider="TRAKT"):
    first = movie(watched_at="2026-09-01T12:00:00Z", _trakt_history_id="101")
    second = movie(watched_at="2026-09-02T12:00:00Z", _trakt_history_id="102")
    baseline = {history_event_key(v): v for v in (first, second)}
    s.seed("p1", provider, "default", "history", baseline)
    database.save_feature_baseline(s.root, provider=provider, feature="history", items=deepcopy(baseline))
    return first, second, baseline


@pytest.mark.parametrize("offset", ["00", "30"], ids=["exact", "within_tolerance"])
def test_selected_viewing_removes_only_that_watch(setup, offset):
    s = setup
    first, second, baseline = _viewing_setup(s)
    selected = {**first, "watched_at": f"2026-09-01T12:00:{offset}Z", "key": history_event_key(first)}
    preview = api.api_editor_remove_preview(dict(kind="history", items=[selected], source_provider="TRAKT"), request=None)
    assert [(p["provider"], p["count"], p["viewings"]) for p in preview["providers"]] == [("TRAKT", 1, 1)]
    result = s.execute(dict(kind="history", items=[selected], source_provider="TRAKT"))
    assert result["confirmed"] == 1
    sent = s.ops["TRAKT"].calls[0][2]
    assert [v["_trakt_history_id"] for v in sent] == ["101"]
    assert list(s.ops["TRAKT"].live[("default", "history")]) == [history_event_key(second)]
    saved = removal._items(database.load_state_features(s.root, {"history"}), "TRAKT", "default", "history")
    assert list(saved) == [history_event_key(second)]


def test_selected_viewing_skips_providers_without_rewatch_writes(setup):
    s = setup
    first, _, _ = _viewing_setup(s, provider="PLEX")
    selected = {**first, "key": history_event_key(first)}
    preview = api.api_editor_remove_preview(dict(kind="history", items=[selected], source_provider="PLEX"), request=None)
    assert not preview["providers"]


def test_selected_viewing_is_unresolved_when_the_provider_reads_no_viewings(setup):
    s = setup
    first, second, _ = _viewing_setup(s)
    s.ops["TRAKT"].live[("default", "history")] = {"tmdb:1": second}
    selected = {**first, "key": history_event_key(first)}
    result = s.execute(dict(kind="history", items=[selected], source_provider="TRAKT"))
    assert result["confirmed"] == 0
    assert result["unresolved"] == 1
    assert not s.ops["TRAKT"].calls
    assert s.ops["TRAKT"].live[("default", "history")] == {"tmdb:1": second}


def test_rewatch_baseline_offers_the_title_and_clears_every_watch(setup):
    s = setup
    first = movie(watched_at="2026-09-01T12:00:00Z", _trakt_history_id="101")
    second = movie(watched_at="2026-09-02T12:00:00Z", _trakt_history_id="102")
    baseline = {history_event_key(v): v for v in (first, second)}
    assert all("@" in key for key in baseline)
    s.seed("p1", "TRAKT", "default", "history", baseline)
    s.ops["TRAKT"].live[("default", "history")] = {"tmdb:1": second}
    database.save_feature_baseline(s.root, provider="TRAKT", feature="history", items=deepcopy(baseline))
    preview = api.api_editor_remove_preview(dict(kind="history", items=[movie()]), request=None)
    assert [(p["provider"], p["count"]) for p in preview["providers"]] == [("TRAKT", 1)]
    result = s.execute(dict(kind="history", items=[movie()]))
    assert result["confirmed"] == 1
    assert not removal._items(database.load_state_features(s.root, {"history"}), "TRAKT", "default", "history")


def test_server_rechecks_scope_confirmation_and_preview(setup):
    s = setup
    s.seed("p1", "TRAKT", "default", "history", {"tmdb:1": movie()})
    payload = dict(kind="history", items=[movie()])
    preview = api.api_editor_remove_preview(payload, request=None)
    base = {**payload, "operation": "remove", "providers": [{"provider": "TRAKT", "instance": "default"}],
            "preview_id": preview["preview_id"]}
    with pytest.raises(HTTPException) as exc:
        api.api_editor_send(base, request=None)
    assert exc.value.status_code == 400
    with pytest.raises(HTTPException):
        api.api_editor_send({**base, "confirmed": True, "providers": [{"provider": "MDBLIST"}]}, request=None)
    s.seed("p1", "TRAKT", "default", "history", {})
    with pytest.raises(HTTPException) as exc:
        api.api_editor_send({**base, "confirmed": True}, request=None)
    assert exc.value.status_code == 409
    assert not s.ops["TRAKT"].calls


@pytest.mark.parametrize("feature", ["history", "ratings", "progress", "collection"])
def test_removal_does_not_require_add_values_and_dry_run_does_not_mutate(setup, feature):
    s = setup
    s.seed("p1", "TRAKT", "default", feature, {"tmdb:1": movie()})
    before = database.load_state_features(s.root, {feature})
    result = s.execute(dict(kind=feature, items=[movie()]), dry_run=True)
    assert result["dry_run"] is True
    assert result["confirmed"] == 0
    assert database.load_state_features(s.root, {feature}) == before
    assert "tmdb:1" in s.ops["TRAKT"].live[("default", feature)]


def test_provider_read_failure_is_reported_without_removing_or_pruning(setup):
    s = setup
    s.seed("p1", "TRAKT", "default", "history", {"tmdb:1": movie()})
    s.ops["TRAKT"].build_index = lambda *a, **k: None
    result = s.execute(dict(kind="history", items=[movie()]))
    assert result["errors"] == 1
    assert not s.ops["TRAKT"].calls
    assert "tmdb:1" in removal._items(database.load_state_features(s.root, {"history"}), "TRAKT", "default", "history")


def test_unauthorized_pairs_and_read_only_users_cannot_preview(setup, monkeypatch):
    setup.seed("p1", "TRAKT", "default", "history", {"tmdb:1": movie()})
    monkeypatch.setattr(removal, "user_can_access_pair", lambda *args: False)
    assert not api.api_editor_remove_preview(dict(kind="history", items=[movie()]), request=None)["providers"]
    monkeypatch.setattr(removal, "request_user", lambda request: {"permissions": {"write": False}})
    with pytest.raises(HTTPException) as exc:
        api.api_editor_remove_preview(dict(kind="history", items=[movie()]), request=None)
    assert exc.value.status_code == 403


def test_sync_guard_reuses_runtime_coordination(monkeypatch):
    from api import syncAPI
    lock = threading.Lock()
    monkeypatch.setattr(syncAPI, "_rt", lambda: ({}, {}, lock))
    monkeypatch.setattr(syncAPI, "_is_sync_running", lambda: True)
    with pytest.raises(HTTPException) as exc:
        with removal._sync_guard():
            pytest.fail("An active sync must block removal")
    assert exc.value.status_code == 409
    assert not lock.locked()


def test_crosswatch_watched_status_preserves_unrelated_repeat_viewings(config_base, monkeypatch):
    from providers.sync._mod_CROSSWATCH import OPS
    rows = {history_event_key(item): item for number in (1, 2) for day in (1, 2)
            for item in [movie(number, watched_at=f"2026-09-0{day}T12:00:00Z")]}
    root = config_base / "tracker"
    root.mkdir()
    path = root / "history.json"
    path.write_text(json.dumps({"items": rows}), encoding="utf-8")
    monkeypatch.setattr(api, "_STATE_BASE", config_base)
    monkeypatch.setattr(removal, "load_sync_ops", lambda name: OPS)
    cfg = {"crosswatch": {"root_dir": str(root), "connected": True}}
    target = dict(provider="CROSSWATCH", instance="default", records={"tmdb:1": movie()})
    result = removal._remove_target(cfg, "history", target, dry_run=False)
    assert result["confirmed"] == 1
    saved = json.loads(path.read_text(encoding="utf-8"))["items"]
    assert set(saved) == {key for key, item in rows.items() if item["ids"]["tmdb"] == "2"}
    assert len(saved) == 2


def test_removal_logs_to_the_app_sync_buffer(monkeypatch):
    logs = []
    monkeypatch.setitem(sys.modules, "crosswatch", SimpleNamespace(_append_log=lambda *args: logs.append(args)))
    removal._emit("apply:remove", dst="TRAKT", instance="family", feature="history", count=2)
    assert logs == [("SYNC", "[EDITOR] apply:remove TRAKT[family] history count=2")]


@pytest.mark.parametrize("disable", ["pair", "feature"])
def test_disabled_automatic_sync_retains_manual_removal_of_synced_records(setup, disable):
    s = setup
    pair = s.cfg["pairs"][0]
    if disable == "pair":
        pair["enabled"] = False
    else:
        pair["features"]["history"]["enable"] = False
    s.seed("p1", "TRAKT", "default", "history", {"tmdb:1": movie()})
    result = s.execute(dict(kind="history", items=[movie()]))
    assert result["confirmed"] == 1


def test_collection_removal_preserves_other_collection_items(setup):
    s = setup
    s.seed("p1", "TRAKT", "default", "collection", {"tmdb:1": movie(), "tmdb:2": movie(2)})
    result = s.execute(dict(kind="collection", items=[movie()]))
    assert result["confirmed"] == 1
    assert set(s.ops["TRAKT"].live[("default", "collection")]) == {"tmdb:2"}


def test_history_adapter_returning_events_can_remove_a_state_baseline(setup):
    s = setup
    item = movie(watched_at="2026-09-01T12:00:00Z", _trakt_history_id="1")
    s.seed("p1", "TRAKT", "default", "history", {"tmdb:1": item})
    # Trakt returns event keys even when ordinary sync later collapses its index.
    s.ops["TRAKT"].live[("default", "history")] = {history_event_key(item): item}
    def remove(cfg, items, *, feature, dry_run):
        s.ops["TRAKT"].calls.append(items)
        assert "_trakt_history_id" not in items[0]
        s.ops["TRAKT"].live[("default", "history")].clear()
        return {"ok": True, "count": 1, "confirmed_keys": ["tmdb:1"]}
    s.ops["TRAKT"].remove = remove
    result = s.execute(dict(kind="history", items=[item]))
    assert result["confirmed"] == 1
    assert s.ops["TRAKT"].calls


@pytest.mark.parametrize("partial", [False, True])
@pytest.mark.parametrize("cached_viewings", [False, True])
def test_watched_status_removal_clears_all_plays_and_checks_media_presence(setup, partial, cached_viewings):
    s = setup
    first = movie(watched_at="2026-09-01T12:00:00Z", _trakt_history_id="1")
    latest = movie(watched_at="2026-09-02T12:00:00Z", _trakt_history_id="2")
    s.seed("p1", "TRAKT", "default", "history", {"tmdb:1": latest})
    live = {history_event_key(it): it for it in (first, latest)}
    if cached_viewings:
        database.save_feature_baseline(s.root, provider="TRAKT", feature="history", items={**live, "tmdb:2": movie(2)})
    s.ops["TRAKT"].live[("default", "history")] = live
    def remove(cfg, items, *, feature, dry_run):
        from providers.sync.trakt._history import _batch_remove
        body, *_ = _batch_remove(SimpleNamespace(), items)
        assert body == {"movies": [{"ids": {"tmdb": 1}}]}
        live.pop(history_event_key(latest))
        if not partial:
            live.clear()
        return {"ok": True, "count": 1, "confirmed_keys": ["tmdb:1"]}
    s.ops["TRAKT"].remove = remove
    result = s.execute(dict(kind="history", items=[latest]))
    assert result["errors"] == 0
    assert result["confirmed"] == (0 if partial else 1)
    assert result["ok"] is not partial
    saved = removal._items(database.load_state_features(s.root, {"history"}), "TRAKT", "default", "history")
    if cached_viewings:
        assert "tmdb:2" in saved
        assert bool(set(saved) - {"tmdb:2"}) is partial
    else:
        assert ("tmdb:1" in saved) is partial


def test_episode_siblings_with_mixed_show_id_namespaces_never_match(setup):
    first = episode(1, watched_at="2026-09-01T12:00:00Z")
    first.update(ids={"tmdb": "1399"}, show_ids={"tvdb": "121361"})
    sibling = {**first, "episode": 2, "_trakt_history_id": "2"}
    setup.seed("p1", "TRAKT", "default", "history", {canonical_key(sibling): sibling})
    preview = api.api_editor_remove_preview(dict(kind="history", items=[{**first, "key": canonical_key(first)}]), request=None)
    assert not preview["providers"]


def test_empty_live_index_does_not_prune_without_confirmed_removal(setup):
    setup.seed("p1", "TRAKT", "default", "watchlist", {"tmdb:1": movie()})
    setup.ops["TRAKT"].live[("default", "watchlist")] = {}
    result = setup.execute(dict(kind="watchlist", items=[movie()]))
    assert result["confirmed"] == 0
    assert not setup.ops["TRAKT"].calls
    assert "tmdb:1" in removal._items(database.load_state_features(setup.root, {"watchlist"}), "TRAKT", "default", "watchlist")


def _tracker_only(s, rows):
    s.ops["CROSSWATCH"] = Ops("CROSSWATCH")
    s.ops["CROSSWATCH"].live[("default", "history")] = deepcopy(rows)
    return s.ops["CROSSWATCH"]


def test_viewed_tracker_offers_rows_that_no_pair_has_synced_yet(setup):
    ops = _tracker_only(setup, {"tmdb:1": movie(), "tmdb:2": movie(2)})
    payload = dict(kind="history", items=[movie()], source_provider="CROSSWATCH")
    preview = api.api_editor_remove_preview(payload, request=None)
    assert [(p["provider"], p["count"]) for p in preview["providers"]] == [("CROSSWATCH", 1)]
    result = setup.execute(payload)
    assert result["confirmed"] == 1
    assert list(ops.live[("default", "history")]) == ["tmdb:2"]


def test_live_check_removes_a_remote_record_added_after_the_last_sync(setup):
    setup.ops["MDBLIST"].live[("default", "history")] = {"tmdb:1": movie(), "tmdb:2": movie(2)}
    payload = dict(kind="history", items=[movie()])
    assert not api.api_editor_remove_preview(payload, request=None)["providers"]
    payload["live_check"] = [{"provider": "MDBLIST", "instance": "default"}]
    preview = api.api_editor_remove_preview(payload, request=None)
    assert [(p["provider"], p["count"]) for p in preview["providers"]] == [("MDBLIST", 1)]
    result = setup.execute(payload)
    assert result["confirmed"] == 1
    assert list(setup.ops["MDBLIST"].live[("default", "history")]) == ["tmdb:2"]


def test_live_check_reports_a_missing_record_instead_of_asking_for_a_sync(setup):
    payload = dict(kind="history", items=[movie()], live_check=[{"provider": "MDBLIST", "instance": "default"}])
    preview = api.api_editor_remove_preview(payload, request=None)
    assert not preview["providers"]
    assert preview["empty_reason"] == "no_matching_records"


def test_live_check_never_matches_server_local_ids_from_another_account(setup):
    local = dict(type="movie", title="Local", ids={"plex": "123"})
    setup.ops["TRAKT"].live[("second", "history")] = {"plex:123": dict(local)}
    payload = dict(kind="history", items=[local], source_provider="PLEX",
                   live_check=[{"provider": "TRAKT", "instance": "second"}])
    assert not api.api_editor_remove_preview(payload, request=None)["providers"]


def test_tracker_rows_are_only_offered_when_viewing_that_tracker(setup):
    _tracker_only(setup, {"tmdb:1": movie()})
    for source in ("", "PLEX"):
        preview = api.api_editor_remove_preview(dict(kind="history", items=[movie()], source_provider=source), request=None)
        assert not preview["providers"]


def test_tracker_row_already_in_a_baseline_is_counted_once(setup):
    _tracker_only(setup, {})
    setup.cfg["pairs"][0]["target"] = "CROSSWATCH"
    setup.seed("p1", "CROSSWATCH", "default", "history", {"tmdb:1": movie()})
    preview = api.api_editor_remove_preview(dict(kind="history", items=[movie()], source_provider="CROSSWATCH"), request=None)
    assert [(p["provider"], p["count"], p["matched"]) for p in preview["providers"]] == [("CROSSWATCH", 1, 1)]


def test_unreadable_tracker_falls_back_to_baselines(setup):
    ops = _tracker_only(setup, {"tmdb:1": movie(), "tmdb:2": movie(2)})
    setup.cfg["pairs"][0]["target"] = "CROSSWATCH"
    setup.seed("p1", "CROSSWATCH", "default", "history", {"tmdb:1": movie()})
    setup.seed("p1", "PLEX", "default", "history", {"tmdb:2": movie(2)})
    ops.build_index = lambda *a, **k: (_ for _ in ()).throw(OSError("unreadable"))
    payload = dict(kind="history", items=[movie(), movie(2)], source_provider="CROSSWATCH")
    preview = api.api_editor_remove_preview(payload, request=None)
    targets = {p["provider"]: (p["count"], p["matched"]) for p in preview["providers"]}
    assert targets == {"CROSSWATCH": (1, 1), "PLEX": (1, 1)}


def test_viewed_tracker_removes_one_unsynced_watch_and_keeps_the_other(setup):
    first = movie(watched_at="2026-09-01T12:00:00Z")
    second = movie(watched_at="2026-09-02T12:00:00Z")
    ops = _tracker_only(setup, {history_event_key(v): v for v in (first, second)})
    payload = dict(kind="history", items=[{**first, "key": history_event_key(first)}], source_provider="CROSSWATCH")
    preview = api.api_editor_remove_preview(payload, request=None)
    assert [(p["provider"], p["count"], p["viewings"]) for p in preview["providers"]] == [("CROSSWATCH", 1, 1)]
    result = setup.execute(payload)
    assert result["confirmed"] == 1
    assert list(ops.live[("default", "history")]) == [history_event_key(second)]


def _gone_from_provider(s, provider):
    s.cfg["pairs"][0]["target"] = provider
    s.ops.setdefault(provider, Ops(provider))
    rows = {"tmdb:1": movie(), "tmdb:2": movie(2)}
    s.seed("p1", provider, "default", "watchlist", rows)
    database.save_feature_baseline(s.root, provider=provider, feature="watchlist", checkpoint=123, items=deepcopy(rows))
    s.ops[provider].live[("default", "watchlist")] = {"tmdb:2": movie(2)}
    return s.ops[provider]


def _cached_watchlist(s, provider):
    state = database.load_state_features(s.root, {"watchlist"})
    return state, list(removal._items(state, provider, "default", "watchlist"))


def _real_crosswatch(s, monkeypatch, tracker):
    from providers.sync._mod_CROSSWATCH import OPS
    root = s.root / "tracker"
    root.mkdir()
    if tracker is not None:
        (root / "watchlist.json").write_text(json.dumps(tracker), encoding="utf-8")
    ops = _gone_from_provider(s, "CROSSWATCH")
    view = {"crosswatch": {"root_dir": str(root), "connected": True}}
    ops.watchlist_absent = lambda cfg, items: OPS.watchlist_absent(view, items)
    return ops


def test_row_gone_from_the_crosswatch_tracker_is_cleared_from_the_cached_inventory(setup, monkeypatch):
    s = setup
    ops = _real_crosswatch(s, monkeypatch, {"ts": 1, "items": {"tmdb:2": movie(2)}})
    result = s.execute(dict(kind="watchlist", items=[movie()]))
    assert result["skipped"] == 1
    assert result["confirmed"] == 0
    assert not ops.calls
    state, cached = _cached_watchlist(s, "CROSSWATCH")
    assert cached == ["tmdb:2"]
    assert state["providers"]["CROSSWATCH"]["watchlist"]["checkpoint"] == 123
    pair_state = database.load_pair_state(s.root, s.scopes[("p1", "watchlist")], {"watchlist"})
    assert len(removal._items(pair_state, "CROSSWATCH", "default", "watchlist")) == 2


@pytest.mark.parametrize("tracker", [
    None,
    {"ts": 1, "items": {"tmdb:1": movie(), "tmdb:2": movie(2)}},
    {"ts": 1, "items": {"tmdb:1": {"type": "movie", "title": "Movie 1"}}},
    [{"type": "movie", "title": "Movie 1"}],
    {"ts": 1, "items": {"title:example": {"type": "movie", "title": "Example"}}},
], ids=["unreadable", "still_listed", "id_only_in_record_key", "legacy_row_without_identity", "row_without_identity"])
def test_absent_row_is_kept_when_the_crosswatch_tracker_cannot_confirm_it(setup, monkeypatch, tracker):
    s = setup
    _real_crosswatch(s, monkeypatch, tracker)
    result = s.execute(dict(kind="watchlist", items=[movie()]))
    assert result["skipped"] == 1
    assert _cached_watchlist(s, "CROSSWATCH")[1] == ["tmdb:1", "tmdb:2"]


def test_absent_row_on_a_remote_provider_is_never_pruned_from_a_partial_read(setup):
    s = setup
    _gone_from_provider(s, "TRAKT")
    result = s.execute(dict(kind="watchlist", items=[movie()]))
    assert result["skipped"] == 1
    assert _cached_watchlist(s, "TRAKT")[1] == ["tmdb:1", "tmdb:2"]


def test_server_local_ids_cannot_match_across_instances(setup):
    s = setup
    item = dict(type="movie", title="Local identity", ids={"plex": "123"})
    s.seed("p1", "PLEX", "default", "history", {"plex:123": item})
    payload = dict(kind="history", items=[{**item, "key": "plex:123"}], source_provider="PLEX")
    assert api.api_editor_remove_preview(payload, request=None)["providers"]
    assert not api.api_editor_remove_preview({**payload, "source_instance": "second"}, request=None)["providers"]


@pytest.mark.parametrize("removals_enabled", [True, False])
def test_next_sync_observes_manual_removal_using_existing_pair_rules(config_base, monkeypatch, removals_enabled):
    from cw_platform.orchestrator import Orchestrator
    from cw_platform.orchestrator._interactive import InteractivePlan
    from test_interactive_sync_features import feature_item, feature_setup

    common, selected = feature_item("history", 1), feature_item("history", 2)
    cfg, src, dst = feature_setup(config_base, monkeypatch, "history", [common, selected], [common, selected])
    cfg["sync"]["allow_mass_delete"] = True
    cfg["pairs"][0]["features"]["history"]["remove"] = removals_enabled
    assert not Orchestrator(cfg).run()["errors"]
    monkeypatch.setattr(api, "_STATE_BASE", config_base)
    monkeypatch.setattr(removal, "load_sync_ops", lambda name: src)
    def remove(cfg, items, *, feature, dry_run):
        for item in items:
            src.index.pop(canonical_key(item))
        return {"ok": True, "count": len(items)}
    src.remove = remove
    key = canonical_key(selected)
    result = removal._remove_target(cfg, "history", dict(provider="SRC", instance="default", records={key: selected}), dry_run=False)
    assert result["confirmed"] == 1
    plan = InteractivePlan()
    assert not Orchestrator(cfg, interactive=plan).run(dry_run=True, write_state_json=False)["errors"]
    operations = {(r["operation"], r["provider"], r["key"]) for r in plan.rows.values()}
    if removals_enabled:
        assert ("remove", "DST", key) in operations
        assert not any(r[0] == "add" for r in operations)
    else:
        assert not any(r[0] == "remove" for r in operations)


def _rewatch_caps(write):
    def capabilities():
        caps = {feature: {"remove": True} for feature in removal.FEATURES}
        caps["history"]["rewatches"] = {"read": True, "write": write, "account_gate": "pro_vip" if write else False}
        return caps
    return capabilities


def test_item_targets_follow_sync_pairs_capabilities_and_media_type(setup, monkeypatch):
    s = setup
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: dict(provider=provider, instance=instance))
    s.cfg["pairs"].append({"id": "p3", "source": "MDBLIST", "target": "PLEX", "features": {"watchlist": {}}})

    def read_only():
        caps = {feature: {"remove": True} for feature in removal.FEATURES}
        caps["history"].update(write=False)
        caps["watchlist"].update(types={"movies": True, "shows": False})
        return caps

    s.ops["MDBLIST"].capabilities = read_only
    ids = lambda rows: sorted((row["provider"], row["instance"]) for row in rows)
    history, hidden = api.item_action_targets(s.cfg, None, "history", media_type="movie")
    assert ids(history) == [("PLEX", "default"), ("TRAKT", "default"), ("TRAKT", "second")]
    assert hidden == []
    watchlist, hidden = api.item_action_targets(s.cfg, None, "watchlist", media_type="show")
    assert ("MDBLIST", "default") not in ids(watchlist)
    assert [(row["provider"], row["reason"]) for row in hidden] == [("MDBLIST", "type_not_supported")]
    s.cfg["pairs"].append({"id": "p4", "source": "MDBLIST", "target": "PLEX", "features": {"history": {}}})
    _, hidden = api.item_action_targets(s.cfg, None, "history", media_type="movie")
    assert [(row["provider"], row["reason"]) for row in hidden] == [("MDBLIST", "read_only")]


def test_item_targets_keep_anime_only_providers_for_anime(setup, monkeypatch):
    s = setup
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: dict(provider=provider, instance=instance))
    s.ops["ANILIST"] = Ops("ANILIST")
    monkeypatch.setattr(api, "sync_provider_names", lambda **kwargs: list(s.ops))
    s.cfg["pairs"].append({"id": "p5", "source": "ANILIST", "target": "TRAKT", "features": {"watchlist": {}}})
    regular, hidden = api.item_action_targets(s.cfg, None, "watchlist", media_type="movie")
    assert "ANILIST" not in {row["provider"] for row in regular}
    assert [(row["provider"], row["reason"]) for row in hidden] == [("ANILIST", "anime_only")]
    anime, _ = api.item_action_targets(s.cfg, None, "watchlist", media_type="movie", anime=True)
    assert "ANILIST" in {row["provider"] for row in anime}


def test_item_targets_use_the_profiles_own_accounts(setup, monkeypatch):
    s = setup
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: dict(provider=provider, instance=instance))
    import cw_platform.provider_instances as instances
    monkeypatch.setattr(instances, "instances_for_user_profile", lambda cfg, pid: {"TRAKT": ["second"]} if pid == "P01" else {})
    rows, _ = api.item_action_targets(s.cfg, None, "history", media_type="movie", user_profile="P01")
    assert [(row["provider"], row["instance"]) for row in rows] == [("TRAKT", "second")]


def test_send_targets_report_rewatch_support_for_history_only(setup, monkeypatch):
    s = setup
    s.ops["TRAKT"].capabilities = _rewatch_caps(True)
    s.ops["PLEX"].capabilities = _rewatch_caps(False)
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: dict(provider=provider, instance=instance))
    rows = {row["provider"]: row for row in api.api_editor_send_providers(kind="history", request=None)["providers"]}
    assert rows["TRAKT"]["rewatch_write"] is True and rows["TRAKT"]["rewatch_gate"] == "pro_vip"
    assert rows["PLEX"]["rewatch_write"] is False and rows["PLEX"]["rewatch_gate"] == ""
    watchlist = api.api_editor_send_providers(kind="watchlist", request=None)["providers"]
    assert all("rewatch_write" not in row for row in watchlist)


def test_rewatch_send_skips_single_play_providers_and_flags_the_rest(setup, monkeypatch):
    s = setup
    s.ops["TRAKT"].capabilities = _rewatch_caps(True)
    s.ops["PLEX"].capabilities = _rewatch_caps(False)
    seen = []
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: dict(provider=provider, instance=instance))
    monkeypatch.setattr(api, "apply_add", lambda **kw: seen.append((kw["dst_name"], dict(kw["cfg"]))) or {"ok": True, "confirmed": 1})
    monkeypatch.setattr(api, "_merge_sent_items_into_state", lambda *a, **k: None)
    payload = dict(kind="history", items=[movie(watched_at="2026-11-01T12:00:00Z")],
                   providers=[{"provider": "PLEX", "instance": "default"}, {"provider": "TRAKT", "instance": "default"}])
    result = api.api_editor_send({**payload, "rewatch": True}, request=None)
    rows = {row["provider"]: row for row in result["results"]}
    assert rows["PLEX"]["error"] == "rewatch_not_supported" and rows["PLEX"]["ok"] is False
    assert rows["TRAKT"]["ok"] is True
    assert seen == [("TRAKT", {"provider": "TRAKT", "instance": "default", "_cw_history_rewatches": True})]
    seen.clear()
    api.api_editor_send(payload, request=None)
    assert [name for name, _ in seen] == ["PLEX", "TRAKT"]
    assert not any(cfg.get("_cw_history_rewatches") for _, cfg in seen)


def test_rewatch_sends_event_identities_and_merges_event_receipts(setup, monkeypatch):
    s = setup
    s.ops["MDBLIST"].capabilities = _rewatch_caps(True)
    sent, merged = [], []
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: dict(provider=provider, instance=instance))

    def fake_add(**kw):
        sent.extend(deepcopy(kw["items"]))
        return {"ok": True, "confirmed": 1, "confirmed_keys": [kw["items"][0]["_cw_event_key"]]}

    monkeypatch.setattr(api, "apply_add", fake_add)
    monkeypatch.setattr(api, "_merge_sent_items_into_state", lambda provider, instance, feature, items: merged.extend(items))
    api.api_editor_send(dict(kind="history", rewatch=True, items=[movie(watched_at="2026-11-01T12:00:00Z")],
                             providers=[{"provider": "MDBLIST", "instance": "default"}]), request=None)
    assert sent[0]["_cw_event_key"] == history_event_key(movie(watched_at="2026-11-01T12:00:00Z"))
    assert sent[0]["_cw_rewatch_sync"] is True
    assert len(merged) == 1 and merged[0]["_cw_event_key"] == sent[0]["_cw_event_key"]


def test_collapsed_trakt_receipt_replaces_the_titles_stored_watches(setup, monkeypatch):
    s = setup
    s.ops["TRAKT"].capabilities = _rewatch_caps(True)
    old = movie(watched_at="2026-05-03T21:10:00Z")
    database.save_feature_baseline(s.root, provider="TRAKT", feature="history", items={
        history_event_key(old): old,
        "tmdb:2@1777842600": movie(2, watched_at="2026-05-03T21:10:00Z"),
    })
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: dict(provider=provider, instance=instance))
    monkeypatch.setattr(api, "apply_add", lambda **kw: {"ok": True, "confirmed": 1, "confirmed_keys": ["tmdb:1"]})
    newer, older = movie(watched_at="2026-11-01T12:00:00Z"), movie(watched_at="2026-10-01T12:00:00Z")
    api.api_editor_send(dict(kind="history", rewatch=True, items=[newer, older],
                             providers=[{"provider": "TRAKT", "instance": "default"}]), request=None)
    stored = api._load_state_items("history", "TRAKT", "default", raw_state=api._state_store().load_state_features({"history"}))
    title_rows = {k: v for k, v in stored.items() if k.startswith("tmdb:1") and not k.startswith("tmdb:1@1777842600")}
    assert len(title_rows) == 1
    assert next(iter(title_rows.values()))["watched_at"] == "2026-11-01T12:00:00Z"
    assert "tmdb:2@1777842600" in stored


def test_undated_rewatch_is_refused_instead_of_using_event_mode(setup, monkeypatch):
    s = setup
    s.ops["TRAKT"].capabilities = _rewatch_caps(True)
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: dict(provider=provider, instance=instance))
    monkeypatch.setattr(api, "apply_add", lambda **kw: pytest.fail("Undated rewatches must not be written"))
    result = api.api_editor_send(dict(kind="history", rewatch=True, items=[movie(watched=True, _cw_watched_state=True)],
                                      providers=[{"provider": "TRAKT", "instance": "default"}]), request=None)
    assert result["results"][0]["error"] == "undated_rewatch_not_supported"


def test_undated_history_only_goes_to_providers_that_store_unknown_dates(setup, monkeypatch):
    s = setup
    s.ops["TRAKT"].capabilities = _rewatch_caps(True)
    seen = []
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: dict(provider=provider, instance=instance))
    monkeypatch.setattr(api, "apply_add", lambda **kw: seen.append((kw["dst_name"], deepcopy(kw["items"]))) or {"ok": True, "confirmed": 1})
    monkeypatch.setattr(api, "_merge_sent_items_into_state", lambda *a, **k: None)
    item = movie(watched=True, _cw_watched_state=True)
    result = api.api_editor_send(dict(kind="history", items=[item], providers=[
        {"provider": "PLEX", "instance": "default"}, {"provider": "TRAKT", "instance": "default"}]), request=None)
    rows = {row["provider"]: row for row in result["results"]}
    assert rows["PLEX"]["error"] == "undated_not_supported"
    assert [name for name, _ in seen] == ["TRAKT"]
    assert "watched_at" not in seen[0][1][0] and seen[0][1][0]["_cw_watched_state"] is True
    rows = {row["provider"]: row for row in api.api_editor_send_providers(kind="history", request=None)["providers"]}
    assert rows["TRAKT"]["undated_write"] is True and rows["PLEX"]["undated_write"] is False
    assert api._normalize_send_item(movie(_cw_watched_state=True), "history") is None


def test_history_notes_report_trakt_single_play_setting(setup, monkeypatch):
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: dict(provider=provider, instance=instance))
    monkeypatch.setattr(api, "_trakt_keeps_one_play", lambda view: view["instance"] == "default")
    assert api.api_editor_history_notes(provider="trakt", instance="default", request=None)["keeps_one_play"] is True
    assert api.api_editor_history_notes(provider="PLEX", instance="default", request=None)["keeps_one_play"] is None
    with pytest.raises(HTTPException):
        api.api_editor_history_notes(provider="NOPE", instance="default", request=None)


@pytest.mark.parametrize("feature,flagged", [("history", True), ("watchlist", False)])
def test_jellyfin_history_removal_updates_every_copy(setup, feature, flagged):
    s = setup
    s.cfg["pairs"].append({"id": "p3", "source": "PLEX", "target": "JELLYFIN",
                           "features": {f: {} for f in removal.FEATURES}})
    jf = s.ops["JELLYFIN"] = Ops("JELLYFIN")
    seen = {"read": [], "write": []}
    read, write = jf.build_index, jf.remove
    jf.build_index = lambda cfg, **k: seen["read"].append(cfg.get("_cw_history_all_copies")) or read(cfg, **k)
    jf.remove = lambda cfg, items, **k: seen["write"].append(cfg.get("_cw_history_all_copies")) or write(cfg, items, **k)
    s.seed("p3", "JELLYFIN", "default", feature, {"tmdb:1": movie()})
    s.seed("p1", "TRAKT", "default", feature, {"tmdb:1": movie()})
    s.execute(dict(kind=feature, items=[movie()]))
    assert seen["write"] == [True if flagged else None]
    assert seen["read"] and not any(seen["read"])
