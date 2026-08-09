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
            "trakt": {"watched_at": 90, "progress_step": 5},
        },
    }
    events: list[dict[str, Any]] = []

    monkeypatch.setattr(crosswatch_sink, "CROSSWATCH_OPS", ops)
    monkeypatch.setattr(crosswatch_sink, "_tmdb_enrich", lambda *_args, **_kwargs: {"ids": {"tmdb": "124800"}, "title": "Love & Death", "year": 2023})
    monkeypatch.setattr(crosswatch_sink, "record_watch", lambda *args, **kwargs: events.append({"kind": "watch", **kwargs}))
    monkeypatch.setattr(crosswatch_sink, "record_scrobble_event", lambda *args, **kwargs: events.append({"kind": "activity", **kwargs}))

    sink = CrossWatchSink(cfg_provider=lambda: cfg, instance_id="CW-P01")

    sink.send(_episode_event("start", 12.0))
    sink.send(_episode_event("pause", 20.0))
    sink.send(_episode_event("stop", 92.0))

    expected_root = str(root / "profiles" / "CW-P01").replace("\\", "/")

    assert [call["feature"] for call in ops.added] == ["progress", "progress", "history"]
    assert [call["feature"] for call in ops.removed] == ["progress"]
    assert all(str(call["root"]).replace("\\", "/") == expected_root for call in ops.added + ops.removed)
    progress_item = ops.added[0]["items"][0]
    history_item = ops.added[-1]["items"][0]
    assert progress_item["type"] == "episode"
    assert progress_item["show_ids"] == {"tmdb": "124800"}
    assert progress_item["season"] == 1
    assert progress_item["episode"] == 7
    assert progress_item["progress_percent"] == 12.0
    assert progress_item["duration_ms"] == 3000000
    assert history_item["watched_at"]
    assert {"kind": "watch", "action": "stop", "source_provider": "kodi", "source_instance": "living-room", "destination_provider": "crosswatch", "destination_instance": "CW-P01", "progress": 92.0} in events
    assert {"kind": "activity", "source": "kodi", "source_instance": "living-room", "target": "crosswatch", "target_instance": "CW-P01", "progress": 92.0} in events


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
