# tests/test_anilist_scrobble.py
# CrossWatch test scripts
from __future__ import annotations

from datetime import date
from typing import Any

import pytest

import providers.scrobble.anilist.sink as anilist_sink
import providers.scrobble.watch_manager as watch_manager
import providers.sync.anilist._progress as progress_mod
from providers.scrobble.anilist.sink import AniListSink
from providers.scrobble.scrobble import ScrobbleEvent
from providers.sync._mod_ANILIST import ANILISTAuthError
from providers.webhooks.config import sink_configured

TODAY = date(2026, 9, 29)
FUZZY_TODAY = {"year": 2026, "month": 9, "day": 29}


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: Any) -> dict[str, list[Any]]:
    anilist_sink._DONE.clear()
    anilist_sink._WARNED.clear()
    seen: dict[str, list[Any]] = {"watch": [], "activity": [], "apply": []}

    class _Ready:
        installed = True

        def __init__(self, cfg: Any = None) -> None:
            self.cfg = cfg

        def ready(self) -> bool:
            return _Ready.installed

    class _Module:
        def __init__(self, cfg: Any) -> None:
            self.client = object()

    monkeypatch.setattr(anilist_sink, "AnimeMappingService", _Ready)
    monkeypatch.setattr(anilist_sink, "ANILISTModule", _Module)
    monkeypatch.setattr(anilist_sink, "record_watch", lambda *a, **k: seen["watch"].append(k))
    monkeypatch.setattr(anilist_sink, "record_scrobble_event", lambda *a, **k: seen["activity"].append(k))
    return seen


def _cfg() -> dict[str, Any]:
    return {
        "anilist": {"access_token": "token"},
        "anime_mapping": {"enabled": True},
        "scrobble": {"trakt": {"watched_at": 90}},
    }


def _episode(action: str = "stop", progress: float = 95.0, season: int = 3, number: int = 15) -> ScrobbleEvent:
    return ScrobbleEvent(
        action=action,
        media_type="episode",
        ids={"tvdb_show": "267440"},
        title="Attack on Titan",
        year=2013,
        season=season,
        number=number,
        progress=progress,
        account="Living Room",
        server_uuid="server-1",
        session_key="session-1",
        raw={},
    )


def _media(**entry: Any) -> dict[str, Any]:
    media: dict[str, Any] = {"id": 104578, "episodes": 10, "status": "FINISHED"}
    media["mediaListEntry"] = entry or None
    return media


def test_plan_new_entry_sets_current_and_started() -> None:
    plan = progress_mod.plan_progress(_media(), 3, today=TODAY)

    assert plan["variables"] == {"mediaId": 104578, "progress": 3, "status": "CURRENT", "startedAt": FUZZY_TODAY}


def test_plan_planning_entry_moves_to_current() -> None:
    plan = progress_mod.plan_progress(_media(status="PLANNING", progress=0), 1, today=TODAY)

    assert plan["variables"]["status"] == "CURRENT"


def test_plan_keeps_existing_started_date() -> None:
    plan = progress_mod.plan_progress(_media(status="CURRENT", progress=2, startedAt={"year": 2020, "month": 1, "day": 1}), 3, today=TODAY)

    assert "startedAt" not in plan["variables"]


def test_plan_never_lowers_progress() -> None:
    plan = progress_mod.plan_progress(_media(status="CURRENT", progress=5), 4, today=TODAY)

    assert plan["skip"] == "not_newer"


def test_plan_same_episode_is_not_newer() -> None:
    assert progress_mod.plan_progress(_media(status="CURRENT", progress=5), 5, today=TODAY)["skip"] == "not_newer"


def test_plan_skips_completed_entry() -> None:
    plan = progress_mod.plan_progress(_media(status="COMPLETED", progress=10), 1, today=TODAY)

    assert plan["skip"] == "already_completed"


def test_plan_caps_progress_at_total_and_completes_finished_media() -> None:
    plan = progress_mod.plan_progress(_media(status="CURRENT", progress=9, startedAt={"year": 2020}), 12, today=TODAY)

    assert plan["variables"] == {"mediaId": 104578, "progress": 10, "status": "COMPLETED", "completedAt": FUZZY_TODAY}


def test_plan_releasing_media_stays_current_at_known_total() -> None:
    media = _media(status="CURRENT", progress=9, startedAt={"year": 2026})
    media["status"] = "RELEASING"

    assert progress_mod.plan_progress(media, 10, today=TODAY)["variables"]["status"] == "CURRENT"


def test_plan_unknown_total_does_not_cap() -> None:
    media = _media(status="CURRENT", progress=1100, startedAt={"year": 1999})
    media["episodes"] = None

    assert progress_mod.plan_progress(media, 1101, today=TODAY)["variables"] == {"mediaId": 104578, "progress": 1101, "status": "CURRENT"}


def test_plan_repeating_keeps_status_and_dates() -> None:
    plan = progress_mod.plan_progress(_media(status="REPEATING", progress=3), 4, today=TODAY)

    assert plan["variables"] == {"mediaId": 104578, "progress": 4, "status": "REPEATING"}


def test_plan_paused_entry_resumes_as_current() -> None:
    plan = progress_mod.plan_progress(_media(status="PAUSED", progress=3, startedAt={"year": 2025}), 4, today=TODAY)

    assert plan["variables"]["status"] == "CURRENT"


def test_native_to_anilist_translates_regular_anidb_ranges(monkeypatch: Any) -> None:
    rows = [
        {"target_provider": "anilist", "target_id": "104578", "source_scope": "R", "source_range": "1-10", "target_range": "1-10"},
        {"target_provider": "anilist", "target_id": "999", "source_scope": "S", "source_range": "1-10", "target_range": "1-10"},
        {"target_provider": "mal", "target_id": "38524", "source_scope": "R", "source_range": "1-10", "target_range": "1-10"},
    ]
    monkeypatch.setattr(progress_mod, "query_edges", lambda tag, ns, ident: rows)

    assert progress_mod.native_to_anilist("v3", "anidb", "14444", 3) == (104578, 3)


def test_native_to_anilist_rejects_ambiguous_targets(monkeypatch: Any) -> None:
    rows = [
        {"target_provider": "anilist", "target_id": "1", "source_scope": "", "source_range": "1-12", "target_range": "1-12"},
        {"target_provider": "anilist", "target_id": "2", "source_scope": "", "source_range": "1-12", "target_range": "1-12"},
    ]
    monkeypatch.setattr(progress_mod, "query_edges", lambda tag, ns, ident: rows)

    assert progress_mod.native_to_anilist("v3", "mal", "5", 1) is None


def _aired_rows(season_rows: dict[str, list[dict[str, Any]]]) -> Any:
    return lambda tag, provider, ident, scope=None: season_rows.get(f"{provider}:{ident}:{scope}", [])


def test_aired_to_anilist_maps_long_runner_season_to_absolute(monkeypatch: Any) -> None:
    rows = {"tvdb:81797:s22": [
        {"source_kind": "show", "target_provider": "anidb", "target_id": "69", "source_range": "1-70", "target_range": "1086-1155"},
        {"source_kind": "show", "target_provider": "anilist", "target_id": "21", "source_range": "1-70", "target_range": "1086-1155"},
        {"source_kind": "show", "target_provider": "anilist", "target_id": "21", "source_range": "1-70", "target_range": "1086-1155"},
    ]}
    monkeypatch.setattr(progress_mod, "query_edges", _aired_rows(rows))

    assert progress_mod.aired_to_anilist("v3", {"tvdb": "81797"}, 22, 28) == (21, 1113)


def test_aired_to_anilist_rejects_ambiguous_targets(monkeypatch: Any) -> None:
    rows = {"tvdb:1:s1": [
        {"source_kind": "show", "target_provider": "anilist", "target_id": "10", "source_range": "1-12", "target_range": "1-12"},
        {"source_kind": "show", "target_provider": "anilist", "target_id": "11", "source_range": "1-12", "target_range": "1-12"},
    ]}
    monkeypatch.setattr(progress_mod, "query_edges", _aired_rows(rows))

    assert progress_mod.aired_to_anilist("v3", {"tvdb": "1"}, 1, 3) is None


def test_aired_to_anilist_maps_specials_to_their_own_entry(monkeypatch: Any) -> None:
    rows = {"tvdb:267440:s0": [
        {"source_kind": "show", "target_provider": "anilist", "target_id": "18397", "source_range": "7", "target_range": "1"},
        {"source_kind": "show", "target_provider": "anilist", "target_id": "18397", "source_range": "12-13", "target_range": "2-3"},
        {"source_kind": "show", "target_provider": "anilist", "target_id": "20691", "source_range": "14", "target_range": "1"},
    ]}
    monkeypatch.setattr(progress_mod, "query_edges", _aired_rows(rows))

    assert progress_mod.aired_to_anilist("v3", {"tvdb": "267440"}, 0, 13) == (18397, 3)
    assert progress_mod.aired_to_anilist("v3", {"tvdb": "267440"}, 0, 14) == (20691, 1)
    assert progress_mod.aired_to_anilist("v3", {"tvdb": "267440"}, 0, 99) is None


def test_aired_to_anilist_rejects_invalid_coordinates(monkeypatch: Any) -> None:
    monkeypatch.setattr(progress_mod, "query_edges", lambda *a, **k: pytest.fail("must not query"))

    assert progress_mod.aired_to_anilist("v3", {"tvdb": "1"}, None, 3) is None
    assert progress_mod.aired_to_anilist("v3", {"tvdb": "1"}, -1, 3) is None
    assert progress_mod.aired_to_anilist("v3", {"tvdb": "1"}, 0, 0) is None
    assert progress_mod.aired_to_anilist("v3", {"tvdb": "1"}, 0, None) is None


class _Res:
    def __init__(self, namespace: str, target_id: str, absolute: int, basis: str = "anibridge_absolute") -> None:
        self.namespace, self.target_id, self.absolute, self.basis = namespace, target_id, absolute, basis


def _resolve_env(monkeypatch: Any, res: Any, direct: Any, hop: Any, enriched: dict[str, str] | None = None) -> list[str]:
    seen: list[str] = []

    class _Svc:
        def __init__(self, cfg: Any = None) -> None:
            pass

        def ready(self) -> bool:
            return True

        def enrich_ids(self, ids: Any, *, media_type: Any = None) -> dict[str, Any]:
            return {"ids": {**dict(ids), **(enriched or {})}}

    monkeypatch.setattr(progress_mod, "AnimeMappingService", _Svc)
    monkeypatch.setattr(progress_mod, "resolve_absolute", lambda item, release_tag="v3": res)
    monkeypatch.setattr(progress_mod, "aired_to_anilist", lambda *a: seen.append("direct") or direct)
    monkeypatch.setattr(progress_mod, "native_to_anilist", lambda *a: seen.append("hop") or hop)
    return seen


_EP = {"type": "episode", "show_ids": {"tvdb": "81797"}, "season": 22, "episode": 28}


def test_resolve_target_prefers_direct_anilist_edge(monkeypatch: Any) -> None:
    seen = _resolve_env(monkeypatch, _Res("anidb", "69", 1113), (21, 1113), None)

    assert progress_mod.resolve_target({}, _EP) == (21, 1113)
    assert seen == ["direct"]


def test_resolve_target_falls_back_to_native_hop(monkeypatch: Any) -> None:
    seen = _resolve_env(monkeypatch, _Res("anidb", "14444", 3), None, (104578, 3))

    assert progress_mod.resolve_target({}, _EP) == (104578, 3)
    assert seen == ["direct", "hop"]


def test_resolve_target_user_override_wins_over_direct(monkeypatch: Any) -> None:
    seen = _resolve_env(monkeypatch, _Res("anilist", "999", 5, basis="user_override"), (21, 1113), (999, 5))

    assert progress_mod.resolve_target({}, _EP) == (999, 5)
    assert seen == ["hop"]


def test_resolve_target_enriched_anilist_id_does_not_veto_direct(monkeypatch: Any) -> None:
    _resolve_env(monkeypatch, _Res("anidb", "14444", 3), (104578, 3), None, enriched={"anilist": "16498"})
    item = {"type": "episode", "show_ids": {"tvdb": "267440"}, "season": 3, "episode": 15}

    assert progress_mod.resolve_target({}, item) == (104578, 3)


def test_resolve_target_event_anilist_id_vetoes_conflicting_direct(monkeypatch: Any) -> None:
    seen = _resolve_env(monkeypatch, _Res("anilist", "500", 3), (104578, 3), (500, 3))
    item = {"type": "episode", "show_ids": {"tvdb": "267440", "anilist": "500"}, "season": 3, "episode": 15}

    assert progress_mod.resolve_target({}, item) == (500, 3)
    assert seen == ["direct", "hop"]


def test_native_to_anilist_passes_anilist_through() -> None:
    assert progress_mod.native_to_anilist("v3", "anilist", "104578", 3) == (104578, 3)


def test_apply_progress_writes_planned_mutation() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class _Client:
        def gql(self, query: str, variables: dict[str, Any], **_: Any) -> dict[str, Any]:
            calls.append((query, variables))
            if query == progress_mod.GQL_MEDIA_ENTRY:
                return {"Media": {**_media(status="CURRENT", progress=2, startedAt={"year": 2026}), "title": {"english": "Attack on Titan Season 3 Part 2"}}}
            return {"SaveMediaListEntry": {"id": 1, "status": "CURRENT", "progress": 3, "repeat": 0}}

    result = progress_mod.apply_progress(_Client(), 104578, 3, today=TODAY)

    assert calls[1] == (progress_mod.GQL_SAVE_PROGRESS, {"mediaId": 104578, "progress": 3, "status": "CURRENT"})
    assert result["ok"] is True and result["progress"] == 3 and result["previous_progress"] == 2


def test_apply_progress_skip_makes_no_write() -> None:
    calls: list[str] = []

    class _Client:
        def gql(self, query: str, variables: dict[str, Any], **_: Any) -> dict[str, Any]:
            calls.append(query)
            return {"Media": _media(status="COMPLETED", progress=10)}

    result = progress_mod.apply_progress(_Client(), 104578, 3, today=TODAY)

    assert calls == [progress_mod.GQL_MEDIA_ENTRY]
    assert result["skipped"] is True and result["reason"] == "already_completed"


def test_watch_manager_can_create_anilist_sink() -> None:
    assert isinstance(watch_manager._make_sink("anilist", _cfg, "default"), AniListSink)


def test_anilist_counts_as_configured_destination() -> None:
    assert sink_configured(_cfg(), "anilist", "default") is True
    assert sink_configured({"anilist": {}}, "anilist", "default") is False


@pytest.mark.parametrize("event", [_episode(action="start", progress=5), _episode(action="pause", progress=95), _episode(progress=50)])
def test_sink_ignores_non_final_events(monkeypatch: Any, event: ScrobbleEvent, _isolate: dict[str, list[Any]]) -> None:
    monkeypatch.setattr(anilist_sink, "resolve_target", lambda *a: pytest.fail("must not resolve"))

    assert AniListSink(cfg_provider=_cfg).send(event) == {"ok": True, "skipped": True, "reason": "not_watched"}
    assert _isolate["watch"] == []


def test_sink_requires_anime_mapping_enabled_even_when_installed(monkeypatch: Any) -> None:
    cfg = _cfg()
    cfg["anime_mapping"]["enabled"] = False
    monkeypatch.setattr(anilist_sink, "resolve_target", lambda *a: pytest.fail("must not resolve"))

    assert AniListSink(cfg_provider=lambda: cfg).send(_episode())["reason"] == "anime_mapping_unavailable"


def test_sink_requires_anime_mapping_installed(monkeypatch: Any) -> None:
    monkeypatch.setattr(anilist_sink.AnimeMappingService, "installed", False)
    monkeypatch.setattr(anilist_sink, "resolve_target", lambda *a: pytest.fail("must not resolve"))

    assert AniListSink(cfg_provider=_cfg).send(_episode())["reason"] == "anime_mapping_unavailable"


def test_sink_requires_connected_account() -> None:
    assert AniListSink(cfg_provider=lambda: {"anime_mapping": {"enabled": True}}).send(_episode())["reason"] == "not_configured"


def test_sink_skips_unmapped_titles(monkeypatch: Any, _isolate: dict[str, list[Any]]) -> None:
    monkeypatch.setattr(anilist_sink, "resolve_target", lambda cfg, item: None)

    assert AniListSink(cfg_provider=_cfg).send(_episode())["reason"] == "not_mapped"
    assert _isolate["watch"] == []


def test_sink_writes_mapped_episode_and_records_events(monkeypatch: Any, _isolate: dict[str, list[Any]]) -> None:
    items: list[dict[str, Any]] = []
    monkeypatch.setattr(anilist_sink, "resolve_target", lambda cfg, item: items.append(item) or (104578, 3))
    monkeypatch.setattr(anilist_sink, "apply_progress", lambda client, aid, ep: _isolate["apply"].append((aid, ep)) or {"ok": True, "status": "CURRENT", "progress": ep})

    assert AniListSink(cfg_provider=_cfg).send(_episode()) == {"ok": True}
    assert items[0]["show_ids"] == {"tvdb": "267440"} and items[0]["season"] == 3 and items[0]["episode"] == 15
    assert _isolate["apply"] == [(104578, 3)]
    assert _isolate["watch"][0]["destination_provider"] == "anilist"
    assert _isolate["activity"][0]["target"] == "anilist"


def test_sink_deduplicates_repeated_stops(monkeypatch: Any, _isolate: dict[str, list[Any]]) -> None:
    monkeypatch.setattr(anilist_sink, "resolve_target", lambda cfg, item: (104578, 3))
    monkeypatch.setattr(anilist_sink, "apply_progress", lambda client, aid, ep: _isolate["apply"].append((aid, ep)) or {"ok": True})
    sink = AniListSink(cfg_provider=_cfg)

    sink.send(_episode())
    second = sink.send(_episode())

    assert second["reason"] == "session_completed"
    assert _isolate["apply"] == [(104578, 3)]


def test_sink_does_not_record_skipped_writes(monkeypatch: Any, _isolate: dict[str, list[Any]]) -> None:
    monkeypatch.setattr(anilist_sink, "resolve_target", lambda cfg, item: (104578, 3))
    monkeypatch.setattr(anilist_sink, "apply_progress", lambda client, aid, ep: {"ok": True, "skipped": True, "reason": "not_newer"})

    assert AniListSink(cfg_provider=_cfg).send(_episode())["reason"] == "not_newer"
    assert _isolate["watch"] == [] and _isolate["activity"] == []


def test_sink_auth_failure_is_not_retried(monkeypatch: Any, _isolate: dict[str, list[Any]]) -> None:
    def _raise(*_: Any) -> dict[str, Any]:
        raise ANILISTAuthError("AniList unauthorized")

    monkeypatch.setattr(anilist_sink, "resolve_target", lambda cfg, item: (104578, 3))
    monkeypatch.setattr(anilist_sink, "apply_progress", _raise)

    assert AniListSink(cfg_provider=_cfg).send(_episode()) == {"ok": False, "error": "unauthorized", "retryable": False}
    assert _isolate["watch"][0]["status"] == "fail"


def test_sink_transient_failure_is_retryable_and_not_marked_done(monkeypatch: Any) -> None:
    attempts: list[int] = []

    def _flaky(client: Any, aid: int, ep: int) -> dict[str, Any]:
        attempts.append(ep)
        if len(attempts) == 1:
            raise RuntimeError("AniList http:500")
        return {"ok": True}

    monkeypatch.setattr(anilist_sink, "resolve_target", lambda cfg, item: (104578, 3))
    monkeypatch.setattr(anilist_sink, "apply_progress", _flaky)
    sink = AniListSink(cfg_provider=_cfg)

    assert sink.send(_episode())["retryable"] is True
    assert sink.send(_episode()) == {"ok": True}
    assert attempts == [3, 3]
