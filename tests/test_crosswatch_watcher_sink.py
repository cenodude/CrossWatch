# tests/test_crosswatch_watcher_sink.py
# CrossWatch - Local tracker watcher sink tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from pathlib import Path
from typing import Any

import providers.scrobble.crosswatch.sink as crosswatch_sink
import providers.scrobble.watch_manager as watch_manager
from providers.scrobble.crosswatch.sink import CrossWatchSink
from providers.scrobble.routes import build_route_cfg, normalize_route
from providers.scrobble.scrobble import ScrobbleEvent


ROOT = Path(__file__).resolve().parents[1]


class _FakeCrossWatchOps:
    def __init__(self) -> None:
        self.added: list[dict[str, Any]] = []
        self.removed: list[dict[str, Any]] = []

    def is_configured(self, cfg: dict[str, Any]) -> bool:
        return (cfg.get("crosswatch") or {}).get("enabled") is not False

    def add(self, cfg: dict[str, Any], items, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        self.added.append({"root": (cfg.get("crosswatch") or {}).get("root_dir"), "feature": feature, "items": list(items)})
        return {"ok": True, "count": len(self.added[-1]["items"])}

    def remove(self, cfg: dict[str, Any], items, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        self.removed.append({"root": (cfg.get("crosswatch") or {}).get("root_dir"), "feature": feature, "items": list(items)})
        return {"ok": True, "count": len(self.removed[-1]["items"])}


def _episode_event(action: str = "start", progress: float = 12.0) -> ScrobbleEvent:
    return ScrobbleEvent(
        action=action,
        media_type="episode",
        ids={"tvdb_episode": "9621656"},
        title="Love & Death",
        year=2023,
        season=1,
        number=7,
        progress=progress,
        account="Living Room",
        server_uuid="server-1",
        session_key="session-1",
        raw={"duration": 3000000},
    )


def test_watcher_routes_accept_crosswatch_profile_sink(tmp_path: Path) -> None:
    root = tmp_path / "cw_provider"
    cfg = {
        "plex": {"account_token": "source-token"},
        "crosswatch": {"root_dir": str(root), "instances": {"CW-P01": {"label": "Desk"}}},
    }
    route = normalize_route(
        {
            "id": "R1",
            "provider": "plex",
            "sink": "crosswatch",
            "sink_instance": "CW-P01",
        },
        "R1",
    )
    view = build_route_cfg(cfg, route)

    assert route["sink"] == "crosswatch"
    assert view["crosswatch"]["root_dir"].replace("\\", "/").endswith("/cw_provider/profiles/CW-P01")


def test_watch_manager_can_create_crosswatch_sink() -> None:
    sink = watch_manager._make_sink("crosswatch", lambda: {"crosswatch": {}}, "CW-P01")

    assert isinstance(sink, CrossWatchSink)


def test_crosswatch_watcher_sink_writes_progress_and_history(monkeypatch, tmp_path: Path) -> None:
    ops = _FakeCrossWatchOps()
    root = tmp_path / "cw_provider"
    cfg = {
        "crosswatch": {"root_dir": str(root), "instances": {"CW-P01": {"label": "Desk"}}},
        "scrobble": {
            "watch": {
                "route_provider": "kodi",
                "route_provider_instance": "living-room",
                "route_sink": "crosswatch",
                "route_sink_instance": "CW-P01",
            },
            "trakt": {"watched_at": 90, "progress_step": 15},
        },
    }
    events: list[dict[str, Any]] = []

    monkeypatch.setattr(crosswatch_sink, "CROSSWATCH_OPS", ops)
    monkeypatch.setattr(crosswatch_sink, "_tmdb_enrich", lambda *_args, **_kwargs: {"ids": {"tmdb": "124800"}, "title": "Love & Death", "year": 2023})
    monkeypatch.setattr(crosswatch_sink, "record_watch", lambda *args, **kwargs: events.append({"kind": "watch", **kwargs}))
    monkeypatch.setattr(crosswatch_sink, "record_scrobble_event", lambda *args, **kwargs: events.append({"kind": "activity", **kwargs}))

    sink = CrossWatchSink(cfg_provider=lambda: cfg, instance_id="CW-P01")

    sink.send(_episode_event("start", 12.0))
    assert sink.send(_episode_event("start", 13.0)) == {
        "ok": True, "log_status": "skipped", "reason": "progress_step", "previous_progress": 12.0, "progress_step": 15.0}
    assert len(ops.added) == 1
    sink.send(_episode_event("pause", 30.0))
    sink.send(_episode_event("stop", 92.0))
    assert sink.send(_episode_event("stop", 92.0))["reason"] == "session_completed"

    expected_root = str(root / "profiles" / "CW-P01").replace("\\", "/")

    assert [call["feature"] for call in ops.added] == ["progress", "progress", "history"]
    assert [call["feature"] for call in ops.removed] == ["progress"]
    assert all(str(call["root"]).replace("\\", "/") == expected_root for call in ops.added + ops.removed)
    progress_item = ops.added[0]["items"][0]
    history_item = ops.added[-1]["items"][0]
    assert progress_item["type"] == "episode"
    assert progress_item["show_ids"] == {"tmdb": "124800"}
    assert progress_item["ids"] == {"tvdb": "9621656"}
    assert history_item["ids"] == {"tvdb": "9621656"}
    assert progress_item["season"] == 1
    assert progress_item["episode"] == 7
    assert progress_item["progress_percent"] == 12.0
    assert progress_item["duration_ms"] == 3000000
    assert history_item["watched_at"]
    assert {"kind": "watch", "action": "stop", "source_provider": "kodi", "source_instance": "living-room", "destination_provider": "crosswatch", "destination_instance": "CW-P01", "progress": 92.0} in events
    assert {"kind": "activity", "source": "kodi", "source_instance": "living-room", "target": "crosswatch", "target_instance": "CW-P01", "progress": 92.0} in events


def test_crosswatch_sink_adds_up_small_progress_steps(monkeypatch, tmp_path: Path) -> None:
    ops = _FakeCrossWatchOps()
    cfg = {
        "crosswatch": {"root_dir": str(tmp_path / "cw_provider"), "instances": {"CW-P01": {"label": "Desk"}}},
        "scrobble": {"watch": {"route_provider": "jellyfin", "route_sink": "crosswatch", "route_sink_instance": "CW-P01"},
                     "trakt": {"watched_at": 90, "progress_step": 15}},
    }
    monkeypatch.setattr(crosswatch_sink, "CROSSWATCH_OPS", ops)
    monkeypatch.setattr(crosswatch_sink, "_tmdb_enrich", lambda *_args, **_kwargs: {"ids": {"tmdb": "124800"}})
    monkeypatch.setattr(crosswatch_sink, "record_watch", lambda *args, **kwargs: None)
    monkeypatch.setattr(crosswatch_sink, "record_scrobble_event", lambda *args, **kwargs: None)
    sink = CrossWatchSink(cfg_provider=lambda: cfg, instance_id="CW-P01")

    for progress in (10.0, 15.0, 20.0, 25.0, 30.0):
        sink.send(_episode_event("start", progress))
    sink.send(_episode_event("pause", 31.0))
    sink.send(_episode_event("pause", 31.0))
    sink.send(_episode_event("stop", 40.0))

    assert [call["items"][0]["progress_percent"] for call in ops.added] == [10.0, 25.0, 31.0, 40.0]


def test_crosswatch_sink_retries_progress_after_a_failed_write(monkeypatch, tmp_path: Path) -> None:
    ops = _FakeCrossWatchOps()
    outcomes = [True, False, True]
    real_add = ops.add

    def add(cfg: dict[str, Any], items, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        result = real_add(cfg, items, feature=feature, dry_run=dry_run)
        return {**result, "ok": outcomes.pop(0)}

    ops.add = add  # type: ignore[method-assign]
    cfg = {
        "crosswatch": {"root_dir": str(tmp_path / "cw_provider"), "instances": {"CW-P01": {"label": "Desk"}}},
        "scrobble": {"watch": {"route_provider": "jellyfin", "route_sink": "crosswatch", "route_sink_instance": "CW-P01"},
                     "trakt": {"watched_at": 90, "progress_step": 15}},
    }
    monkeypatch.setattr(crosswatch_sink, "CROSSWATCH_OPS", ops)
    monkeypatch.setattr(crosswatch_sink, "_tmdb_enrich", lambda *_args, **_kwargs: {"ids": {"tmdb": "124800"}})
    monkeypatch.setattr(crosswatch_sink, "record_watch", lambda *args, **kwargs: None)
    monkeypatch.setattr(crosswatch_sink, "record_scrobble_event", lambda *args, **kwargs: None)
    sink = CrossWatchSink(cfg_provider=lambda: cfg, instance_id="CW-P01")

    sink.send(_episode_event("start", 10.0))
    sink.send(_episode_event("start", 25.0))
    sink.send(_episode_event("pause", 25.0))
    sink.send(_episode_event("pause", 25.0))

    assert [call["items"][0]["progress_percent"] for call in ops.added] == [10.0, 25.0, 25.0]


def test_crosswatch_sink_completion_keeps_saved_rewatches(monkeypatch, tmp_path: Path) -> None:
    from cw_platform.tracker_store import TrackerStore
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    cfg = {
        "crosswatch": {"root_dir": str(tmp_path / "cw_provider"), "instances": {"CW-P01": {"label": "Desk", "connected": True}}},
        "scrobble": {"watch": {"route_provider": "kodi", "route_sink": "crosswatch", "route_sink_instance": "CW-P01"},
                     "trakt": {"watched_at": 90, "progress_step": 15}},
    }
    monkeypatch.setattr(crosswatch_sink, "_tmdb_enrich", lambda *_args, **_kwargs: {"ids": {"tmdb": "124800"}})
    monkeypatch.setattr(crosswatch_sink, "record_watch", lambda *args, **kwargs: None)
    monkeypatch.setattr(crosswatch_sink, "record_scrobble_event", lambda *args, **kwargs: None)
    sink = CrossWatchSink(cfg_provider=lambda: cfg, instance_id="CW-P01")
    view = sink._config_view(cfg)
    heat = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}}
    plays = [{**heat, "watched_at": "2026-01-01T00:00:00Z"}, {**heat, "watched_at": "2026-02-01T00:00:00Z"}]
    CROSSWATCHModule({**view, "_cw_history_rewatches": True}).add("history", plays)

    sink.send(_episode_event("start", 50.0))
    sink.send(_episode_event("stop", 95.0))
    sink.send(_episode_event("stop", 95.0))

    stored = TrackerStore(Path(view["crosswatch"]["root_dir"])).read_current("history")["items"].values()
    assert sorted(item["watched_at"] for item in stored if item["ids"].get("tmdb") == "949") == [
        "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"]
    assert len([item for item in stored if item["ids"].get("tvdb") == "9621656"]) == 1


def _tracker_sink(monkeypatch, tmp_path: Path, watch_options: dict[str, Any]) -> tuple[Any, Path, list[dict[str, Any]]]:
    from dataclasses import replace

    cfg = {
        "crosswatch": {"root_dir": str(tmp_path / "cw_provider"), "instances": {"CW-P01": {"label": "Desk", "connected": True}}},
        "scrobble": {"watch": {"route_provider": "kodi", "route_sink": "crosswatch", "route_sink_instance": "CW-P01",
                               "route_options": {"watch": watch_options}},
                     "trakt": {"watched_at": 90, "progress_step": 15}},
    }
    clock = ["2026-03-01T20:00:00Z"]
    events: list[dict[str, Any]] = []
    monkeypatch.setattr(crosswatch_sink, "_tmdb_enrich", lambda *_args, **_kwargs: {"ids": {"tmdb": "124800"}})
    monkeypatch.setattr(crosswatch_sink, "record_watch", lambda *args, **kwargs: events.append({"kind": "watch", **kwargs}))
    monkeypatch.setattr(crosswatch_sink, "record_scrobble_event", lambda *args, **kwargs: events.append({"kind": "activity", **kwargs}))
    monkeypatch.setattr(crosswatch_sink, "utc_now_iso", lambda: clock[0])
    sink = CrossWatchSink(cfg_provider=lambda: cfg, instance_id="CW-P01")
    root = Path(sink._config_view(cfg)["crosswatch"]["root_dir"])

    def watch(at: str, session: str, number: int = 7) -> Any:
        clock[0] = at
        return sink.send(replace(_episode_event("stop", 95.0), session_key=session, number=number, ids={"tvdb_episode": f"96216{number:02d}"}))

    return watch, root, events


def _episode_plays(root: Path, number: int = 7) -> list[str]:
    from cw_platform.tracker_store import TrackerStore

    stored = TrackerStore(root).read_current("history")["items"].values()
    return sorted(item["watched_at"] for item in stored if item.get("episode") == number)


def test_crosswatch_sink_without_rewatches_replaces_the_latest_play(monkeypatch, tmp_path: Path) -> None:
    watch, root, _ = _tracker_sink(monkeypatch, tmp_path, {})
    watch("2026-03-01T20:00:00Z", "s1")
    watch("2026-03-05T20:00:00Z", "s2")

    assert _episode_plays(root) == ["2026-03-05T20:00:00Z"]


def test_crosswatch_sink_rewatches_merge_inside_the_window(monkeypatch, tmp_path: Path) -> None:
    watch, root, events = _tracker_sink(monkeypatch, tmp_path, {"crosswatch_rewatches": True})
    watch("2026-03-01T20:00:00Z", "s1")
    events.clear()
    assert watch("2026-03-01T21:30:00Z", "s2") == {"ok": True, "log_status": "skipped", "reason": "rewatch_window", "rewatch_window_hours": 12}
    assert [(event["kind"], event.get("status"), event.get("reason")) for event in events] == [("watch", "skipped", "rewatch_window")]
    watch("2026-03-01T21:40:00Z", "s3", number=8)
    watch("2026-03-02T09:00:00Z", "s4")

    assert _episode_plays(root) == ["2026-03-01T20:00:00Z", "2026-03-02T09:00:00Z"]
    assert _episode_plays(root, 8) == ["2026-03-01T21:40:00Z"]
    assert [event["kind"] for event in events[1:]] == ["watch", "activity", "watch", "activity"]


def test_crosswatch_sink_same_second_completion_is_merged(monkeypatch, tmp_path: Path) -> None:
    watch, root, events = _tracker_sink(monkeypatch, tmp_path, {"crosswatch_rewatches": True})
    watch("2026-03-01T20:00:00Z", "s1")
    events.clear()

    assert watch("2026-03-01T20:00:00Z", "s2")["reason"] == "rewatch_window"
    assert [(event["kind"], event.get("status")) for event in events] == [("watch", "skipped")]
    assert _episode_plays(root) == ["2026-03-01T20:00:00Z"]


def test_crosswatch_sink_rewatch_window_is_configurable(monkeypatch, tmp_path: Path) -> None:
    watch, root, _ = _tracker_sink(monkeypatch, tmp_path, {"crosswatch_rewatches": True, "crosswatch_rewatch_window_hours": 48})
    watch("2026-03-01T20:00:00Z", "s1")
    watch("2026-03-02T20:00:00Z", "s2")
    watch("2026-03-04T20:00:00Z", "s3")

    assert _episode_plays(root) == ["2026-03-01T20:00:00Z", "2026-03-04T20:00:00Z"]


def test_crosswatch_rewatch_options_are_normalized() -> None:
    from providers.scrobble.routes import normalize_route_options

    watch = normalize_route_options({"watch": {"crosswatch_rewatches": "true", "crosswatch_rewatch_window_hours": 500}})["watch"]
    assert watch == {"crosswatch_rewatches": False}
    watch = normalize_route_options({"watch": {"crosswatch_rewatches": True, "crosswatch_rewatch_window_hours": "24"}})["watch"]
    assert watch == {"crosswatch_rewatches": True, "crosswatch_rewatch_window_hours": 24}


def test_crosswatch_rewatch_webhook_settings_reach_the_sink() -> None:
    from api.scrobblerManagementAPI import _normalize_webhook_settings
    from providers.webhooks.dispatch import _route_cfg

    cfg: dict[str, Any] = {"scrobble": {}}
    settings = _normalize_webhook_settings(cfg, "plex", {"sinks": ["crosswatch"], "crosswatch_rewatches": True, "crosswatch_rewatch_window_hours": 6})
    assert settings["crosswatch_rewatches"] is True and settings["crosswatch_rewatch_window_hours"] == 6
    assert _normalize_webhook_settings(cfg, "plex", {"sinks": ["simkl"], "crosswatch_rewatches": True})["crosswatch_rewatches"] is False
    cfg["scrobble"]["webhook"] = {"profiles": {"plex": {"P01": settings}}}

    watch = crosswatch_sink._rewatch_policy(_route_cfg(cfg, "plex", "P01", "crosswatch", "default"))
    assert watch == (True, 6)
    assert crosswatch_sink._rewatch_policy(_route_cfg(cfg, "plex", "P02", "crosswatch", "default")) == (False, 12)


def test_scrobbler_route_modal_lists_crosswatch_sink() -> None:
    text = (ROOT / "assets" / "js" / "modals" / "scrobbler-route" / "index.js").read_text("utf-8")
    scrobbler = (ROOT / "assets" / "js" / "scrobbler.js").read_text("utf-8")
    css = (ROOT / "assets" / "css" / "providers.css").read_text("utf-8")
    meta = (ROOT / "assets" / "helpers" / "provider-meta.js").read_text("utf-8")

    assert '"crosswatch"' in text.split("const sinks")[1].split("]")[0]
    assert '"crosswatch"' in text.split("const ratingSinks")[1].split("]")[0]
    assert 'crosswatch: "CrossWatch"' in text
    assert "window.CW?.ProviderMeta?.logoPath?.(provider)" in text
    assert ".scrm-provider-card.provider-crosswatch" in css
    assert ".scrm-provider-card.provider-crosswatch::after{background-image:url(/assets/img/CROSSWATCH.svg)}" in css
    assert "providerMeta().logoPath?.(v)" in scrobbler
    assert '"crosswatch"' in scrobbler.split("plexRatingSinks")[1].split("]")[0]
    assert 'mdblist: "/assets/img/MDBLIST.svg"' not in scrobbler
    assert "CROSSWATCH:" in meta
    assert "scrobblerSink: true" in meta


def test_scrobbler_webhook_modal_lists_crosswatch_sink() -> None:
    text = (ROOT / "assets" / "js" / "modals" / "scrobbler-webhook" / "index.js").read_text("utf-8")

    assert '"crosswatch"' in text.split("const sinks")[1].split("]")[0]
    assert '"crosswatch"' in text.split("const ratingSinks")[1].split("]")[0]
    assert 'crosswatch: "CrossWatch"' in text
    assert 'crosswatch: "/assets/img/CROSSWATCH.svg"' in text
    assert "for (const sink of ratingSinks)" in text
