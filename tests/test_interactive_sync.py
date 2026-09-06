# tests/test_interactive_sync.py
# CrossWatch - Interactive Sync Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy

import pytest

from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan
from test_orchestrator_dry_run_no_side_effects import FakeOps, _cfg, _install
from test_playlists_pending_create import world


def item(number, **extra):
    return dict(type="movie", title=f"Movie {number}", ids={"imdb": f"tt{number:07}"}, **extra)


def setup_ops(config_base, monkeypatch, source=None, target=None):
    from cw_platform.id_map import canonical_key

    src = FakeOps("SRC", {canonical_key(x): x for x in (source or [])})
    dst = FakeOps("DST", {canonical_key(x): x for x in (target or [])})
    _install(monkeypatch, src, dst, config_base / ".cw_state")
    return src, dst


def run(cfg, plan):
    return Orchestrator(cfg, interactive=plan).run(dry_run=plan.preview, pair_scope_ids=["p1"], write_state_json=not plan.preview)


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_preview_never_calls_provider_writes(config_base, monkeypatch, mode):
    src, dst = setup_ops(config_base, monkeypatch, [item(1), item(2)])
    cfg = _cfg(False)
    cfg["pairs"][0]["mode"] = mode
    plan = InteractivePlan()
    result = run(cfg, plan)
    assert result["ok"]
    assert len(plan.rows) == 2
    assert not src.add_calls and not dst.add_calls
    assert not dst.index


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_only_selected_items_reach_provider(config_base, monkeypatch, mode):
    src, dst = setup_ops(config_base, monkeypatch, [item(1), item(2)])
    cfg = _cfg(False)
    cfg["pairs"][0]["mode"] = mode
    plan = InteractivePlan()
    run(cfg, plan)
    rid = next(rid for rid, row in plan.rows.items() if row["item"]["title"] == "Movie 2")
    run(cfg, InteractivePlan(preview=False, selected={rid}))
    assert len(dst.add_calls) == 1
    assert [x["title"] for x in dst.add_calls[0]] == ["Movie 2"]


def test_changed_payload_is_not_applied(config_base, monkeypatch):
    src, dst = setup_ops(config_base, monkeypatch, [item(1)])
    plan = InteractivePlan()
    run(_cfg(False), plan)
    next(iter(src.index.values()))["title"] = "Changed"
    run(_cfg(False), InteractivePlan(preview=False, selected=set(plan.rows)))
    assert not dst.add_calls


def test_selection_does_not_bypass_disabled_adds(config_base, monkeypatch):
    src, dst = setup_ops(config_base, monkeypatch, [item(1)])
    cfg = _cfg(False)
    plan = InteractivePlan()
    run(cfg, plan)
    cfg["pairs"][0]["features"]["watchlist"]["add"] = False
    run(cfg, InteractivePlan(preview=False, selected=set(plan.rows)))
    assert not dst.add_calls


def test_mass_delete_protection_precedes_review(config_base, monkeypatch):
    setup_ops(config_base, monkeypatch, [item(1)], [item(1), item(2), item(3)])
    cfg = _cfg(False)
    cfg["sync"].update(enable_remove=True, allow_mass_delete=False)
    cfg["pairs"][0]["features"]["watchlist"].update(remove=True, remove_mode="mirror")
    plan = InteractivePlan()
    run(cfg, plan)
    assert not any(row["operation"] == "remove" for row in plan.rows.values())


def test_editor_mapping_recalculates_plan(config_base, monkeypatch):
    from api import editorAPI

    setup_ops(config_base, monkeypatch, [item(1)])
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    cfg = _cfg(False)
    original = InteractivePlan()
    run(cfg, original)
    old_key = next(iter(original.rows.values()))["key"]
    corrected = item(2)
    editorAPI._save_policy_manual("watchlist", "SRC", {"imdb:tt0000002": corrected}, [old_key], merge=True)
    refreshed = InteractivePlan()
    run(cfg, refreshed)
    assert {row["key"] for row in refreshed.rows.values()} == {"imdb:tt0000002"}
    assert not set(original.rows) & set(refreshed.rows)


def test_fingerprint_includes_destination_state():
    plan = InteractivePlan()
    plan.filter("ratings", "DST", "default", "update", [item(1, rating=8)], before={"imdb:tt0000001": item(1, rating=3)})
    execution = InteractivePlan(preview=False, selected=set(plan.rows))
    assert not execution.filter("ratings", "DST", "default", "update", [item(1, rating=8)], before={"imdb:tt0000001": item(1, rating=6)})


def test_conflict_choice_is_bound_to_values():
    plan = InteractivePlan()
    assert plan.conflict("ratings", "key", "A", "B", item(1, rating=4), item(1, rating=9), "A") == "A"
    cid = next(iter(plan.conflicts))
    changed = InteractivePlan(choices={cid: "B"})
    assert changed.conflict("ratings", "key", "A", "B", item(1, rating=4), item(1, rating=9), "A") == "B"
    assert changed.conflict("ratings", "key", "A", "B", item(1, rating=5), item(1, rating=9), "A") == "A"


def test_instance_mapping_is_loaded_only_for_its_account(config_base, monkeypatch):
    from api import editorAPI
    from cw_platform.orchestrator._state_store import StateStore
    from cw_platform.orchestrator._pairs_utils import manual_policy

    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    editorAPI._save_policy_manual("watchlist", "SRC", {"imdb:tt0000002": item(2)}, ["imdb:tt0000001"], "second", merge=True)
    state = StateStore(config_base).load_state_features({"watchlist"})
    assert manual_policy(state, "SRC", "watchlist", "default") == ({}, set())
    adds, blocks = manual_policy(state, "SRC", "watchlist", "second")
    assert set(adds) == {"imdb:tt0000002"}
    assert blocks == {"imdb:tt0000001"}
    editorAPI._save_policy_manual("watchlist", "SRC", {"imdb:tt0000003": item(3)}, [], "second", merge=True)
    state = StateStore(config_base).load_state_features({"watchlist"})
    assert set(manual_policy(state, "SRC", "watchlist", "second")[0]) == {"imdb:tt0000002", "imdb:tt0000003"}


def test_preview_does_not_record_observed_deletions(config_base, monkeypatch):
    from cw_platform.orchestrator._state_store import StateStore

    src, dst = setup_ops(config_base, monkeypatch, [item(1), item(2)], [item(1), item(2)])
    cfg = _cfg(False)
    cfg["pairs"][0]["mode"] = "two-way"
    cfg["sync"].update(enable_remove=True, include_observed_deletes=True)
    cfg["pairs"][0]["features"]["watchlist"]["remove"] = True
    run(cfg, InteractivePlan(preview=False))
    src.index.pop("imdb:tt0000002")
    store = StateStore(config_base)
    before = deepcopy(store.load_tomb())
    before_state = deepcopy(store.load_state())
    run(cfg, InteractivePlan())
    assert store.load_tomb() == before
    assert store.load_state() == before_state


@pytest.mark.parametrize("age_days,expected_operation", [(31, "add"), (30, "add"), (1, "remove")])
def test_two_way_preview_matches_normal_plan_for_tombstone_expiry(config_base, monkeypatch, age_days, expected_operation):
    import json
    import time
    from cw_platform.orchestrator._state_store import StateStore

    setup_ops(config_base, monkeypatch, [item(1), item(2)], [item(1)])
    cfg = _cfg(False)
    cfg["pairs"][0]["mode"] = "two-way"
    cfg["sync"].update(enable_remove=True, tombstone_ttl_days=30)
    cfg["pairs"][0]["features"]["watchlist"]["remove"] = True
    run(cfg, InteractivePlan(preview=False))
    store = StateStore(config_base)
    tomb = {"keys": {"watchlist:DST-SRC|imdb:tt0000002": int(time.time()) - age_days * 86400}}
    store.save_tomb(tomb)
    before_tomb = deepcopy(store.load_tomb())
    plan = InteractivePlan()
    run(cfg, plan)
    assert store.load_tomb() == before_tomb
    events = []
    Orchestrator(cfg, on_progress=lambda line: events.append(json.loads(line)) if line.startswith("{") else None).run(dry_run=True, pair_scope_ids=["p1"], write_state_json=False)
    normal = next(event for event in events if event.get("event") == "two:plan")
    assert normal["add_to_B"] == int(expected_operation == "add")
    assert normal["rem_from_A"] == int(expected_operation == "remove")
    assert [(row["operation"], row["provider"]) for row in plan.rows.values()] == [(expected_operation, "DST" if expected_operation == "add" else "SRC")]


@pytest.mark.parametrize("new_on", ["SRC", "DST"])
def test_two_way_shared_watchlist_with_one_new_item_only_proposes_one_add(config_base, monkeypatch, new_on):
    import json
    from cw_platform.id_map import canonical_key

    common = [{**item(n), "ids": {"tmdb": str(1000 + n), "imdb": f"tt{n:07}"}} for n in range(1, 7)]
    target = [{**item(n), "ids": {"tmdb": str(1000 + n)}} for n in range(1, 7)]
    src, dst = setup_ops(config_base, monkeypatch, common, target)
    cfg = _cfg(False)
    cfg["pairs"][0]["mode"] = "two-way"
    cfg["pairs"][0]["features"]["watchlist"]["remove"] = True
    cfg["sync"].update(enable_remove=True, include_observed_deletes=True)
    run(cfg, InteractivePlan(preview=False))
    added = {**item(7), "ids": {"tmdb": "1007"}}
    (src if new_on == "SRC" else dst).index[canonical_key(added)] = added
    plan = InteractivePlan()
    run(cfg, plan)
    expected_target = "DST" if new_on == "SRC" else "SRC"
    assert [(row["operation"], row["provider"], row["key"]) for row in plan.rows.values()] == [("add", expected_target, "tmdb:1007")]
    events = []
    Orchestrator(cfg, on_progress=lambda line: events.append(json.loads(line)) if line.startswith("{") else None).run(dry_run=True, pair_scope_ids=["p1"], write_state_json=False)
    normal = next(event for event in events if event.get("event") == "two:plan")
    assert normal["rem_from_A"] == normal["rem_from_B"] == 0
    assert normal["add_to_A"] + normal["add_to_B"] == 1


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_deselected_observed_remove_can_be_reviewed_again(config_base, monkeypatch, mode):
    src, dst = setup_ops(config_base, monkeypatch, [item(1), item(2)], [item(1), item(2)])
    cfg = _cfg(False)
    cfg["pairs"][0]["mode"] = mode
    cfg["sync"].update(enable_remove=True, include_observed_deletes=True)
    cfg["pairs"][0]["features"]["watchlist"]["remove"] = True
    run(cfg, InteractivePlan(preview=False))
    src.index.pop("imdb:tt0000002")
    plan = InteractivePlan()
    run(cfg, plan)
    assert any(r["operation"] == "remove" for r in plan.rows.values())
    run(cfg, InteractivePlan(preview=False))
    again = InteractivePlan()
    run(cfg, again)
    assert any(r["operation"] == "remove" for r in again.rows.values())


def test_playlists_only_apply_selected_membership(config_base):
    from cw_platform import playlists_runner as runner
    from test_playlists_runner import _providers, _mapping, _cfg as playlist_cfg, _movie

    providers = _providers({"L1": {"items": [_movie(1), _movie(2)]}}, {"T1": {"items": [_movie(3)]}})
    mapping = _mapping(membership="mirror", order="preserve")
    plan = InteractivePlan()
    runner.run_mapping(playlist_cfg(), mapping, dry_run=True, providers=providers, interactive=plan)
    assert not providers["PLEX"].calls
    selected = {rid for rid, row in plan.rows.items() if row["operation"] == "add" and row["key"] == "tmdb:2"}
    assert selected
    runner.run_mapping(playlist_cfg(), mapping, providers=providers, interactive=InteractivePlan(preview=False, selected=selected))
    assert providers["PLEX"].calls == [("add", "T1", ["tmdb:2"])]


def test_ruleset_selection_preserves_assignment_bookkeeping(config_base):
    from cw_platform import playlists_runner as runner
    from test_playlists_rulesets import providers, cfg, mapping, movie

    ops = providers([movie(1), movie(2), movie(3)])
    plan = InteractivePlan()
    runner.run_mapping(cfg(), mapping(), providers=ops, dry_run=True, interactive=plan)
    selected = {rid for rid, row in plan.rows.items() if row["key"] == "tmdb:2"}
    assert selected
    runner.run_mapping(cfg(), mapping(), providers=ops, interactive=InteractivePlan(preview=False, selected=selected))
    assert ops["TRAKT"].calls == [("add", "T1", ["tmdb:2"])]
    entry = runner._state_entry(mapping())
    assert set(entry["assignments"]) == {"tmdb:2"}
    again = InteractivePlan()
    runner.run_mapping(cfg(), mapping(), providers=ops, dry_run=True, interactive=again)
    assert {row["key"] for row in again.rows.values()} == {"tmdb:1", "tmdb:3"}


def test_mapping_change_at_execution_boundary_excludes_writes():
    plan = InteractivePlan()
    plan.filter("watchlist", "DST", "default", "add", [item(1)])
    execution = InteractivePlan(preview=False, selected=set(plan.rows), valid=lambda: False)
    assert not execution.filter("watchlist", "DST", "default", "add", [item(1)])
    assert not execution.seen


def test_session_preflight_drift_returns_to_review(config_base, monkeypatch):
    from services import interactive_sync as svc
    from api import syncAPI

    src, dst = setup_ops(config_base, monkeypatch, [item(1)])
    session = svc.Session(pair_id="p1", owner="local")
    cfg = _cfg(False)
    svc.refresh(session, cfg, {})
    chosen = set(session.plan.rows)
    next(iter(src.index.values()))["title"] = "Changed"
    monkeypatch.setattr(syncAPI, "_run_pairs_thread", lambda *a, **k: pytest.fail("stale plan reached execution"))
    svc.apply(session, cfg, chosen)
    assert session.status == "review"
    assert session.revision == 2
    assert not dst.add_calls


def test_preflight_explains_selection_loss_without_a_third_provider_read(config_base, monkeypatch, caplog):
    from api import syncAPI
    from services import interactive_sync as svc
    from services import interactive_sync_progress as progress

    src, dst = setup_ops(config_base, monkeypatch, [item(n) for n in range(1431)])
    session = svc.Session(pair_id="p1", owner="local")
    cfg = _cfg(False)
    reads = []
    original_read = src.build_index
    now = [1000.0]
    monkeypatch.setattr(progress.time, "monotonic", lambda: now[0])
    caplog.set_level("INFO", logger="services.interactive_sync")

    def read(*args, **kwargs):
        reads.append(True)
        now[0] += 3000
        return original_read(*args, **kwargs)

    monkeypatch.setattr(src, "build_index", read)
    monkeypatch.setattr(syncAPI, "_run_pairs_thread", lambda *a, **k: pytest.fail("stale plan reached execution"))
    try:
        svc.refresh(session, cfg, {})
        selected = session.store.selected_ids()
        for row in list(src.index.values())[:1151]:
            row["title"] += " changed"
        svc.apply(session, cfg, selected)
        assert len(reads) == 2
        assert session.status == "review"
        assert session.public()["counts"]["changes"] == 1431
        assert session.public()["counts"]["selected"] == 280
        result = session.public()["apply_review"]
        assert {k: result[k] for k in ("requested", "retained", "needs_review", "applied")} == dict(requested=1431, retained=280, needs_review=1151, applied=0)
        assert session.message.startswith("Nothing was applied.")
        assert session.public()["progress"]["elapsed_seconds"] == 3000
        assert not dst.add_calls
        diagnostics = [record.message for record in caplog.records if "interactive_sync_proposal_changed" in record.message]
        assert len(diagnostics) == 5
        assert all('"item.title"' in message for message in diagnostics)
    finally:
        session.close()


def test_failed_preflight_does_not_report_provider_changes(config_base, monkeypatch):
    from api import syncAPI
    from services import interactive_sync as svc

    setup_ops(config_base, monkeypatch, [item(1)])
    session = svc.Session(pair_id="p1", owner="local")
    cfg = _cfg(False)
    try:
        svc.refresh(session, cfg, {})
        original_build = svc.build

        def failed_build(*args, **kwargs):
            plan, summary = original_build(*args, **kwargs)
            return plan, dict(summary, ok=False)

        monkeypatch.setattr(svc, "build", failed_build)
        monkeypatch.setattr(syncAPI, "_run_pairs_thread", lambda *a, **k: pytest.fail("failed preflight reached execution"))
        svc.apply(session, cfg, session.store.selected_ids())
        assert session.status == "error"
        assert "recheck could not be completed" in session.message
        assert session.public()["apply_review"]["applied"] == 0
    finally:
        session.close()


@pytest.fixture
def api_client(config_base, monkeypatch):
    from fastapi import FastAPI, Request
    from fastapi.testclient import TestClient
    from api import interactiveSyncAPI as api
    from services import interactive_sync as svc

    cfg = _cfg(False)
    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(svc, "SESSIONS", {})
    session = svc.Session(pair_id="p1", owner="alice", status="review", revision=1)
    session.store = svc.ReviewStore()
    session.plan.rows = session.store.rows
    session.plan.conflicts = session.store.conflicts
    session.plan.filter("watchlist", "DST", "default", "add", [item(1)], source="SRC")
    session.store.finish()
    svc.SESSIONS[session.id] = session
    app = FastAPI()

    @app.middleware("http")
    async def user(request: Request, call_next):
        request.state.user = dict(id=request.headers.get("x-test-user", "alice"), is_admin=True)
        return await call_next(request)

    app.include_router(api.router)
    with TestClient(app) as client:
        yield client, session, api
    session.close()


def test_api_rejects_cross_user_session(api_client):
    client, session, api = api_client
    assert client.get(f"/api/interactive-sync/{session.id}", headers={"x-test-user": "bob"}).status_code == 404


@pytest.mark.parametrize("payload, status", [({"revision": 0, "selection_version": 0}, 409), ({"revision": 1, "selection_version": 9}, 409), ({"revision": 1, "selection_version": 0, "selected": ["forged"]}, 422)])
def test_api_rejects_stale_or_invalid_selection(api_client, payload, status):
    client, session, api = api_client
    assert client.post(f"/api/interactive-sync/{session.id}/apply", json=payload).status_code == status


def test_api_cannot_apply_twice(api_client, monkeypatch):
    client, session, api = api_client

    def launch(session, *a, **k):
        session.status = "applying"

    monkeypatch.setattr(api, "launch", launch)
    payload = dict(revision=1, selection_version=0)
    assert client.post(f"/api/interactive-sync/{session.id}/apply", json=payload).status_code == 200
    assert client.post(f"/api/interactive-sync/{session.id}/apply", json=payload).status_code == 409


def test_api_expires_reviews(api_client, monkeypatch):
    from services import interactive_sync as svc

    client, session, api = api_client
    session.touched = -svc.SESSION_TTL
    assert client.get(f"/api/interactive-sync/{session.id}").status_code == 404


@pytest.mark.parametrize("ruleset", [False, True])
def test_pending_playlist_creates_only_selected_items(world, ruleset):
    from cw_platform import playlists_runner as runner

    cfg, src, dst, providers = world
    mapping = runner.resolve_mapping_by_id(cfg, "MAP-01")
    if ruleset:
        from test_playlists_rulesets import custom_rule

        rule = custom_rule(250)
        cfg["playlists"]["rulesets"] = [rule]
        mapping["ruleset_id"] = rule["id"]
    plan = InteractivePlan()
    runner.run_mapping(cfg, mapping, providers=providers, dry_run=True, interactive=plan)
    assert not providers["PLEX"].created
    selected = {rid for rid, row in plan.rows.items() if row["key"] == "tmdb:2"}
    assert selected
    result = runner.run_mapping(cfg, mapping, providers=providers, interactive=InteractivePlan(preview=False, selected=selected))
    if not ruleset:
        assert result["created"]
    assert result["added"] == 1
    assert providers["PLEX"].created[0]["seed"] == ["tmdb:2"]


def test_no_selection_does_not_create_pending_playlist(world):
    from cw_platform import playlists_runner as runner

    cfg, src, dst, providers = world
    runner.run_mapping(cfg, runner.resolve_mapping_by_id(cfg, "MAP-01"), providers=providers, interactive=InteractivePlan(preview=False))
    assert not providers["PLEX"].created


def test_rating_conflict_choice_replans_existing_engine(config_base, monkeypatch):
    src, dst = setup_ops(config_base, monkeypatch, [item(1, rating=3)], [item(1, rating=8)])
    for ops in (src, dst):
        ops.features = lambda: {"ratings": True}
        ops.health = lambda *_a, **_k: {"ok": True, "status": "ok", "features": {"ratings": True}}
    cfg = _cfg(False)
    cfg["pairs"][0].update(mode="two-way", feature="ratings", features={"ratings": {"enable": True, "add": True, "mode": "all"}})
    first = InteractivePlan()
    run(cfg, first)
    assert len(first.conflicts) == 1
    assert {row["provider"] for row in first.rows.values()} == {"DST"}
    chosen = InteractivePlan(choices={next(iter(first.conflicts)): "DST"})
    run(cfg, chosen)
    assert {row["provider"] for row in chosen.rows.values()} == {"SRC"}
    run(cfg, InteractivePlan(preview=False, selected=set(chosen.rows), choices=chosen.choices))
    assert src.add_calls[0][0]["rating"] == 8
    assert not dst.add_calls


def test_provider_capability_checks_are_preserved(config_base, monkeypatch):
    src, dst = setup_ops(config_base, monkeypatch, [item(1)])
    dst.features = lambda: {"watchlist": False}
    dst.capabilities = lambda: {"features": {"watchlist": False}}
    plan = InteractivePlan()
    run(_cfg(False), plan)
    assert not plan.rows
    assert not dst.add_calls


def test_anime_coordinates_participate_in_preview(config_base, monkeypatch):
    import json
    from cw_platform.anime_mapping import storage
    from test_anime_history_coords_orchestrator import FakeOps as HistoryOps, _episode, _cfg as anime_cfg, MAPPINGS, SRC_SHOW_IDS, DST_SHOW_IDS

    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    src = HistoryOps("CROSSWATCH", {"tmdb:12609#s02e01": _episode(SRC_SHOW_IDS, 2, 1, absolute=14)})
    dst = HistoryOps("MDBLIST", {"tmdb:12609#s01e14": _episode(DST_SHOW_IDS, 1, 14)})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"CROSSWATCH": src, "MDBLIST": dst})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *_: True)
    plan = InteractivePlan()
    run(anime_cfg(True), plan)
    assert not plan.rows
    assert not dst.add_calls


def test_session_apply_uses_normal_run_path(config_base, monkeypatch):
    from api import syncAPI
    from services import interactive_sync as svc

    src, dst = setup_ops(config_base, monkeypatch, [item(1), item(2)])
    cfg = _cfg(False)
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    session = svc.Session(pair_id="p1", owner="local")
    svc.refresh(session, cfg, {})
    chosen = {rid for rid, row in session.plan.rows.items() if row["key"] == "imdb:tt0000002"}
    assert session.mapping_version == svc.mapping_version(cfg)
    assert chosen
    progress_events = []
    original_progress = session.progress.event

    def progress(event):
        progress_events.append(event)
        original_progress(event)

    monkeypatch.setattr(session.progress, "event", progress)
    svc.apply(session, cfg, chosen)
    assert session.status == "complete", session.message
    assert session.summary["added"] == 1
    assert len(dst.add_calls) == 1
    assert dst.add_calls[0][0]["title"] == "Movie 2"
    assert any('"event":"apply:add:start"' in event for event in progress_events if isinstance(event, str))
    assert session.public()["progress"]["stage"] == "complete"


def test_unidentified_rows_offer_mapping_without_disabling_title_resolution():
    plan = InteractivePlan()
    plan.filter("watchlist", "DST", "default", "add", [{"type": "movie", "title": "Unknown movie"}, {}])
    rows = list(plan.rows.values())
    assert rows[0]["result"] == "unresolved" and rows[0]["selectable"]
    assert rows[1]["result"] == "blocked" and not rows[1]["selectable"]


@pytest.mark.parametrize("enrichment", [False, True])
def test_mapping_api_persists_and_recalculates(api_client, config_base, monkeypatch, enrichment):
    from api import editorAPI
    from services import interactive_sync as svc

    client, session, api = api_client
    setup_ops(config_base, monkeypatch, [item(1, _trakt_history_id=9876)])
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    svc.refresh(session, _cfg(False), {})
    corrected = item(2)
    if enrichment:
        corrected = item(1)
        corrected["ids"]["tmdb"] = "2"

    def launch(current, task, *args, prepare=None, **kwargs):
        if prepare is not None:
            prepare()
        task(current, *args)

    monkeypatch.setattr(api, "launch", launch)
    response = client.post(f"/api/interactive-sync/{session.id}/mapping", json={
        "revision": session.revision, "row_id": next(iter(session.plan.rows)), "item": corrected,
    })
    assert response.status_code == 200, response.text
    assert len(session.plan.rows) == 1
    row = next(iter(session.plan.rows.values()))
    assert row["key"] == ("tmdb:2" if enrichment else "imdb:tt0000002")
    adds, blocks = editorAPI._load_policy_manual("watchlist", "SRC")
    assert "_trakt_history_id" not in next(iter(adds.values()))
    assert blocks == ([] if enrichment else ["imdb:tt0000001"])


def test_mapping_api_rejects_unusable_ids(api_client):
    client, session, api = api_client
    corrected = item(1)
    corrected["ids"] = {"tmdb": "not-an-id"}
    response = client.post(f"/api/interactive-sync/{session.id}/mapping", json={
        "revision": session.revision, "row_id": next(iter(session.plan.rows)), "item": corrected,
    })
    assert response.status_code == 400


def test_playlist_reorder_keeps_deselected_membership(config_base):
    from cw_platform import playlists_runner as runner
    from test_playlists_runner import _providers, _mapping, _cfg as playlist_cfg, _movie

    providers = _providers({"L1": {"items": [_movie(1), _movie(2)]}}, {"T1": {"items": [_movie(3), _movie(1)]}})
    mapping = _mapping(membership="mirror", order="preserve")
    plan = InteractivePlan()
    runner.run_mapping(playlist_cfg(), mapping, dry_run=True, providers=providers, interactive=plan)
    selected = {rid for rid, row in plan.rows.items() if row["operation"] == "update"}
    assert selected
    runner.run_mapping(playlist_cfg(), mapping, providers=providers, interactive=InteractivePlan(preview=False, selected=selected))
    assert providers["PLEX"].calls == [("reorder", "T1", ["tmdb:1", "tmdb:3"])]


def test_cancel_during_selected_execution_prevents_writes(config_base, monkeypatch):
    from cw_platform import run_control

    src, dst = setup_ops(config_base, monkeypatch, [item(1)])
    plan = InteractivePlan()
    run(_cfg(False), plan)
    original = dst.build_index

    def cancel(*args, **kwargs):
        result = original(*args, **kwargs)
        run_control.request_cancel()
        return result

    monkeypatch.setattr(dst, "build_index", cancel)
    try:
        result = run(_cfg(False), InteractivePlan(preview=False, selected=set(plan.rows)))
        assert result["cancelled"]
        assert not dst.add_calls
    finally:
        run_control.clear_cancel()
