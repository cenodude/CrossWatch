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
def test_floppy_history_removal_is_unavailable(setup, item):
    from providers.sync._mod_FLOPPY import OPS
    s = setup
    s.cfg["pairs"][0]["target"] = "FLOPPY"
    ops = s.ops["FLOPPY"] = Ops("FLOPPY")
    ops.capabilities = OPS.capabilities
    s.seed("p1", "FLOPPY", "default", "history", {canonical_key(item): item})
    payload = dict(kind="history", items=[item], source_provider="FLOPPY")
    preview = api.api_editor_remove_preview(payload, request=None)
    assert not preview["providers"]
    # A stale or hand-crafted submission cannot bypass the preview restriction.
    with pytest.raises(HTTPException) as exc:
        api.api_editor_send({**payload, "operation": "remove", "confirmed": True,
                             "preview_id": preview["preview_id"], "providers": [{"provider": "FLOPPY"}]}, request=None)
    assert exc.value.status_code == 400
    assert not ops.calls
    assert canonical_key(item) in removal._items(database.load_state_features(s.root, {"history"}), "FLOPPY", "default", "history")


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
def test_dated_history_selection_is_rejected_without_clearing_watched_status(setup, item, event_field):
    s = setup
    dated = {**item, "watched_at": "2026-09-01T12:00:00Z", "_trakt_history_id": "101"}
    s.seed("p1", "TRAKT", "default", "history", {canonical_key(item): dated, "tmdb:2": movie(2)})
    selected = {**dated, event_field: history_event_key(dated)}
    payload = dict(kind="history", items=[movie(2), selected], source_provider="TRAKT")
    for action in (api.api_editor_remove_preview, api.api_editor_send):
        with pytest.raises(HTTPException) as exc:
            action({**payload, "operation": "remove", "confirmed": True,
                    "preview_id": "stale", "providers": [{"provider": "TRAKT"}]}, request=None)
        assert exc.value.status_code == 400
        assert "Individual watch dates" in exc.value.detail
    assert not s.ops["TRAKT"].calls
    assert len(removal._items(database.load_state_features(s.root, {"history"}), "TRAKT", "default", "history")) == 2


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
