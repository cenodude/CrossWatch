# tests/test_status_mapping_completeness.py
# CrossWatch - Status preservation completeness and preview execution boundaries.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from cw_platform.orchestrator import _status_mapping as runtime
from cw_platform.status_mapping import Observation, ReadState, Status
from cw_platform.status_mapping_store import StatusStore
from providers.sync._status_mapping import NativeStatusAdapter, Record, adapter_class


ENTRIES = {
    "kitsu": {"status": "current", "progress": 0, "reconsuming": False, "reconsumeCount": 0,
              "notes": None, "private": False, "startedAt": None, "finishedAt": None,
              "progressedAt": None, "ratingTwenty": None},
    "myanimelist": {"status": "watching", "score": 0, "num_episodes_watched": 0, "is_rewatching": False,
                    "num_times_rewatched": 0, "rewatch_value": 0, "priority": 0, "tags": [], "comments": ""},
    "anilist": {"id": 10, "mediaId": 1, "status": "CURRENT", "progress": 0, "repeat": 0, "score": 0,
                "notes": None, "private": False, "hiddenFromStatusLists": False, "customLists": {}, "advancedScores": {},
                "startedAt": {"year": None, "month": None, "day": None}, "completedAt": {"year": None, "month": None, "day": None}},
}


def adapter_case(monkeypatch, provider):
    cls = adapter_class(provider)
    entry = deepcopy(ENTRIES[provider])
    state = SimpleNamespace(entry=entry, reads=0, writes=[], alter=lambda value: None)
    namespace = "mal" if provider == "myanimelist" else provider
    def snapshot():
        state.reads += 1
        result = deepcopy(entry)
        state.alter(result)
        return result
    def save(ident, before, patch):
        state.writes.append(deepcopy(patch))
        entry.update(patch)
    if provider == "kitsu":
        media = {"id": "1", "attributes": {"subtype": "TV", "canonicalTitle": "Anime"}}
        client = SimpleNamespace(entries=lambda: [({"id": "10", "attributes": snapshot()}, media)], save=save)
    elif provider == "myanimelist":
        media = {"id": 1, "title": "Anime", "media_type": "tv", "num_episodes": 12}
        client = SimpleNamespace(entries=lambda: [(entry, media)], media=lambda ident: {**media, "my_list_status": snapshot()}, save=save)
    else:
        media = {"id": 1, "format": "TV", "title": {"english": "Anime"}}
        def gql(query, variables, **kwargs):
            if "mutation" in query:
                save("1", entry, {"status": variables["status"]})
                return {"SaveMediaListEntry": {"id": 10, "status": entry["status"]}}
            return {"MediaListCollection": {"lists": [{"entries": [{**snapshot(), "media": media}]}]}}
        client = SimpleNamespace(viewer=lambda: {"id": 9}, gql=gql)
    monkeypatch.setattr(cls, "client", lambda self: client)
    state.adapter = cls({})
    state.namespace = namespace
    state.cls = cls
    return state


@pytest.mark.parametrize("provider,field", [(provider, field) for provider, entry in ENTRIES.items() for field in entry if field != "status"])
@pytest.mark.parametrize("invalid", ["omitted", "wrong_type"])
@pytest.mark.parametrize("stage", ["initial", "preflight", "readback"])
def test_incomplete_preservation_never_authorizes_or_confirms_write(monkeypatch, tmp_path, config_base, provider, field, stage, invalid):
    case = adapter_case(monkeypatch, provider)
    class Source(NativeStatusAdapter):
        capabilities = case.cls.capabilities

        def read(self):
            return {"1": Record("1", "shows", {"type": "show", "ids": {case.namespace: "1"}, "title": "Anime"},
                                Observation(ReadState.PRESENT, Status.DROPPED, "dropped", "source", False, False), {})}
    monkeypatch.setattr(runtime, "adapter_class", lambda name: Source if name == "source" else case.cls)
    monkeypatch.setattr(runtime, "build_provider_config_view", lambda *args: {})
    threshold = {"initial": 1, "preflight": 2, "readback": 3}[stage]
    def alter(value):
        if case.reads >= threshold:
            if invalid == "omitted":
                value.pop(field)
            else:
                value[field] = [] if not isinstance(value[field], list) else {}
    case.alter = alter
    pair = {"id": "test", "source": "SOURCE", "target": provider.upper(), "mode": "one-way",
            "features": {"history": {"enable": True, "use_source_status": True}},
            "status_mapping": {"version": 1, "a": {"provider": "source", "instance": "default"},
                               "b": {"provider": provider, "instance": "default"},
                               "directions": {"a_to_b": {"shows": {"dropped": {"include": True, "target": "dropped"}}}}}}
    store = StatusStore(tmp_path / "status.sqlite3")
    ctx = SimpleNamespace(dry_run=False, interactive=None, emit=lambda *args, **kwargs: None)
    result = runtime.run_status_mappings(ctx, [pair], {}, {}, store=store)
    assert result["updated"] == 0
    assert len(case.writes) == int(stage == "readback")
    item = runtime.ItemScope(runtime.pair_scope(pair), "shows", "1", "1")
    state = store.get(item)
    assert "baseline" not in state
    assert bool(state.get("pending")) == (stage == "readback")
    if stage == "readback":
        before = deepcopy(state)
        runtime.run_status_mappings(ctx, [pair], {}, {}, store=store)
        assert store.get(item) == before
        assert len(case.writes) == 1


@pytest.mark.parametrize("provider,field", [("kitsu", "progress"), ("kitsu", "reconsuming"), ("myanimelist", "is_rewatching"), ("anilist", "progress")])
@pytest.mark.parametrize("value", [None, "false", "0", -1, 0.5])
def test_guard_fields_reject_null_and_coerced_values(monkeypatch, provider, field, value):
    case = adapter_case(monkeypatch, provider)
    case.entry[field] = value
    assert case.adapter.read()["1"].observation.state == ReadState.UNKNOWN


@pytest.mark.parametrize("provider", ENTRIES)
def test_explicit_safe_values_remain_eligible(monkeypatch, provider):
    case = adapter_case(monkeypatch, provider)
    record = case.adapter.read()["1"]
    assert record.observation.complete
    assert record.observation.rewatching is False
    assert not case.adapter.blockers(record, Status.DROPPED)


@pytest.mark.parametrize("provider,field", [("kitsu", "reconsuming"), ("myanimelist", "is_rewatching")])
def test_complete_rewatch_flag_retains_blocker(monkeypatch, provider, field):
    case = adapter_case(monkeypatch, provider)
    case.entry[field] = True
    record = case.adapter.read()["1"]
    assert record.observation.complete
    assert "Rewatching" in case.adapter.blockers(record, Status.DROPPED)[0]


@pytest.mark.parametrize("provider", ["kitsu", "anilist"])
def test_complete_progress_retains_date_blocker(monkeypatch, provider):
    case = adapter_case(monkeypatch, provider)
    case.entry["progress"] = 1
    record = case.adapter.read()["1"]
    assert record.observation.complete
    assert "history date" in case.adapter.blockers(record, Status.DROPPED)[0]


@pytest.mark.parametrize("existing", [False, True])
def test_crosswatch_readonly_writer_rejects_even_valid_transition(tmp_path, existing):
    from cw_platform.tracker_store import TrackerStore
    from providers.sync.crosswatch._status import StatusAdapter
    store = TrackerStore(tmp_path)
    key = "movie:tmdb:1"
    if existing:
        store.save_note(key, {"type": "movie", "tmdb": "1", "title": "Movie", "status": "watching"})
    before = store.note(key)
    adapter = StatusAdapter({"_cw_readonly": True, "crosswatch": {"root_dir": str(tmp_path)}})
    source = Record("source", "movies", {"type": "movie", "ids": {"tmdb": "1"}}, Observation(ReadState.UNKNOWN), {})
    record = adapter.read().get(key) or adapter.missing_record(source, {})
    assert not adapter.blockers(record, Status.ON_HOLD)
    with pytest.raises(RuntimeError, match="read-only"):
        adapter.write(record, Status.ON_HOLD)
    assert store.note(key) == before


@pytest.mark.parametrize("existing", [False, True])
def test_service_preview_and_apply_crosswatch_status(tmp_path, config_base, monkeypatch, existing):
    from api import syncAPI
    from cw_platform.tracker_store import TrackerStore
    from services import interactive_sync as svc
    left, right = TrackerStore(tmp_path / "source"), TrackerStore(tmp_path / "target")
    left.write_state("history", {})
    right.write_state("history", {})
    key = "movie:tmdb:1"
    left.save_note(key, {"type": "movie", "title": "Movie", "tmdb": "1", "status": "dropped"})
    if existing:
        right.save_note(key, {"type": "movie", "title": "Movie", "tmdb": "1", "status": "watching", "notes": "Keep"})
    before = right.note(key)
    pair = {"id": "local", "enabled": True, "source": "CROSSWATCH", "target": "CROSSWATCH", "target_instance": "P01", "mode": "one-way",
            "features": {"history": {"enable": True, "use_source_status": True}},
            "status_mapping": {"version": 1, "a": {"provider": "crosswatch", "instance": "default"},
                               "b": {"provider": "crosswatch", "instance": "P01"},
                               "directions": {"a_to_b": {"movies": {"dropped": {"include": True, "target": "on_hold"}}}}}}
    cfg = {"crosswatch": {"connected": True, "root_dir": str(tmp_path / "source"), "instances": {"P01": {"connected": True, "root_dir": str(tmp_path / "target")}}}, "pairs": [pair]}
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    session = svc.Session(pair_id="local", owner="local", pair=pair)
    try:
        svc.refresh(session, cfg, {})
        assert right.note(key) == before
        proposals = {ident for ident, value in session.plan.rows.items() if value["item"].get("_cw_status_mapping")}
        assert len(proposals) == 1, session.plan.notices
        svc.apply(session, cfg, proposals)
        assert session.status == "complete", session.message
        assert right.note(key)["status"] == "on_hold"
        if existing:
            assert right.note(key)["notes"] == "Keep"
        assert left.note(key)["status"] == "dropped"
        assert right.read_current("history")["items"] == {}
        assert not right.has_state("watchlist")
    finally:
        session.close()
