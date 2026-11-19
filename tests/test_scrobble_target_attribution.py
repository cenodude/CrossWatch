# tests/test_scrobble_target_attribution.py
# CrossWatch - Scrobble and rating target attribution tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from importlib import import_module
from types import SimpleNamespace
from typing import Any

import pytest


class _Resp:
    def __init__(self, status_code: int = 200, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = "{}"
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._payload


def _event(**kw: Any) -> SimpleNamespace:
    base = {
        "action": "start", "media_type": "movie", "ids": {"tmdb": "550", "imdb": "tt0137523"}, "title": "Fight Club",
        "year": 1999, "season": None, "number": None, "progress": 10.0, "account": "viewer", "server_uuid": "srv-1",
        "session_key": "sess-1", "raw": {}, "position_ms": None, "duration_ms": None,
    }
    base.update(kw)
    return SimpleNamespace(**base)


def _rate_payload(rating_key: str = "rk-1") -> dict[str, Any]:
    return {
        "event": "media.rate",
        "Account": {"title": "viewer"},
        "Metadata": {"type": "movie", "title": "Fight Club", "year": 1999, "ratingKey": rating_key, "userRating": 8, "Guid": [{"id": "tmdb://550"}]},
    }


@pytest.mark.parametrize("module,cls", [("providers.scrobble.punchplay.sink", "PunchPlaySink"), ("providers.scrobble.scrob.sink", "ScrobSink")])
def test_webhook_dispatch_cfg_keyword_is_accepted(module, cls):
    sink = getattr(import_module(module), cls)(cfg_provider=lambda: {})

    result = sink.send(_event(), cfg={})

    assert result["reason"] == "not_configured"


@pytest.mark.parametrize("status,ok", [(503, False), (200, True)])
def test_bingebase_reports_and_logs_delivery_outcome(monkeypatch, status, ok):
    from providers.scrobble.bingebase import sink as sink_mod

    recorded: list[dict[str, Any]] = []
    monkeypatch.setattr(sink_mod, "record_watch", lambda ev, **kw: recorded.append(kw))
    sink = sink_mod.BingeBaseSink(cfg_provider=lambda: {"bingebase": {"webhook_url": "https://bingebase.test/hook"}})
    monkeypatch.setattr(sink.session, "post", lambda url, **kw: _Resp(status))

    result = sink.send(_event())

    assert result["ok"] is ok
    assert len(recorded) == 1
    assert recorded[0]["destination_provider"] == "bingebase"
    assert recorded[0]["status"] == ("ok" if ok else "fail")


def test_kitsu_failed_write_is_logged(monkeypatch):
    from providers.scrobble.kitsu import sink as kitsu_sink

    class FailingModule:
        def __init__(self, view: Any) -> None:
            pass

        def add(self, feature: str, items: list[Any]) -> dict[str, Any]:
            return {"unresolved": [{"reason": "request_failed:500"}]}

    recorded: list[dict[str, Any]] = []
    monkeypatch.setattr(kitsu_sink, "record_watch", lambda ev, **kw: recorded.append(kw))
    monkeypatch.setattr(kitsu_sink.OPS, "is_configured", lambda view: True)
    monkeypatch.setattr(kitsu_sink, "KITSUModule", FailingModule)
    monkeypatch.setattr(kitsu_sink, "_event_item", lambda ev: {"type": "episode"})
    cfg = {"scrobble": {"watch": {"route_provider": "plex", "route_provider_instance": "P2"}}}

    result = kitsu_sink.KitsuSink(instance_id="K2").send(_event(action="stop", progress=95.0, season=1, number=1), cfg=cfg)

    assert result["ok"] is False
    assert recorded == [{
        "action": "stop", "source_provider": "plex", "source_instance": "P2", "destination_provider": "kitsu",
        "destination_instance": "K2", "status": "fail", "progress": 95.0, "reason": "request_failed:500",
    }]


def test_missing_destination_instance_never_borrows_the_default_account(monkeypatch):
    from providers.scrobble.trakt import sink as trakt_sink
    from providers.webhooks.config import sink_configured

    monkeypatch.setattr(trakt_sink, "_TOKEN_OVERRIDE", {"default": "refreshed-default"})
    cfg = {"trakt": {"client_id": "cid", "access_token": "default-token", "instances": {"T2": {"access_token": "t2-token"}}}}

    assert sink_configured(cfg, "trakt", "default") is True
    assert sink_configured(cfg, "trakt", "T2") is True
    assert sink_configured(cfg, "trakt", "T3") is False
    assert trakt_sink._hdr(cfg, "T2")["Authorization"] == "Bearer t2-token"
    assert "Authorization" not in trakt_sink._hdr(cfg, "T3")


def test_plex_webhook_ratings_use_the_selected_accounts(monkeypatch):
    from providers.scrobble.simkl import sink as simkl_sink
    from providers.scrobble.trakt import sink as trakt_sink
    from providers.webhooks import plex

    trakt_calls: list[Any] = []
    simkl_tokens: list[Any] = []
    monkeypatch.setattr(trakt_sink, "_post", lambda path, body, cfg, instance_id=None: trakt_calls.append(instance_id) or _Resp(201))
    monkeypatch.setattr(simkl_sink, "_post", lambda path, body, cfg, **kw: simkl_tokens.append(cfg["simkl"].get("access_token")) or _Resp(201))
    monkeypatch.setattr(plex, "_save_config", lambda _cfg: None)
    monkeypatch.setattr(plex, "_sinks_from_webhook_cfg", lambda *_a: {"trakt", "simkl"})
    monkeypatch.setattr(plex, "_rating_payload", lambda media_type, md, ids, rating, cfg, logger=None: {"movies": [{"ids": {"tmdb": 550}, "rating": rating}]})
    plex._LAST_RATING_BY_ACC.clear()
    cfg = {
        "scrobble": {"enabled": True, "sources": {"webhook": True}, "webhook": {
            "sinks": ["trakt", "simkl"], "sink_instances": {"trakt": "T2", "simkl": "S2"},
            "plex_trakt_ratings": True, "plex_simkl_ratings": True,
        }},
        "trakt": {"client_id": "cid", "access_token": "default-trakt", "instances": {"T2": {"access_token": "t2-trakt"}}},
        "simkl": {"client_id": "sid", "access_token": "default-simkl", "instances": {"S2": {"access_token": "s2-simkl"}}},
    }

    result = plex.process_webhook(_rate_payload(), {}, cfg=cfg)

    assert result["trakt"]["ok"] and result["simkl"]["ok"]
    assert trakt_calls == ["T2"]
    assert simkl_tokens == ["s2-simkl"]


def test_route_ratings_are_logged_per_target(monkeypatch):
    import cw_platform.event_archive as event_archive
    from providers.scrobble.plex import ratings_sync, watch

    archived: list[dict[str, Any]] = []
    monkeypatch.setattr(ratings_sync, "send_rating", lambda *a: {"ok": True})
    monkeypatch.setattr(event_archive, "record_webhook", lambda **kw: archived.append(kw))
    watch._LAST_RATING_BY_ACC.clear()
    cfg = {"scrobble": {"enabled": True, "sources": {"watcher": True}, "watch": {
        "route_provider": "plex", "route_provider_instance": "P2", "route_sink_instance": "CW-P01", "route_id": "R4",
        "route_options": {"ratings": {"mode": "custom", "targets": ["crosswatch"]}},
    }}}

    result = watch.process_rating_webhook(_rate_payload(), {}, cfg_override=cfg, route_hook={})

    assert result["crosswatch"]["ok"]
    assert [(r["event_type"], r["destination_provider"], r["destination_instance"], r["source_instance"], r["account"]) for r in archived] == [
        ("rating_applied", "crosswatch", "CW-P01", "P2", "viewer"),
    ]


def test_plex_rating_dedup_is_scoped_per_plex_instance(monkeypatch):
    from providers.scrobble.plex import ratings_sync
    from providers.webhooks import plex

    sent: list[str] = []
    monkeypatch.setattr(ratings_sync, "send_rating", lambda provider, cfg, instance, item, rating: sent.append(instance) or {"ok": True})
    monkeypatch.setattr(plex, "_save_config", lambda _cfg: None)
    monkeypatch.setattr(plex, "_sinks_from_webhook_cfg", lambda *_a: {"crosswatch"})
    plex._LAST_RATING_BY_ACC.clear()
    cfg = {"scrobble": {"enabled": True, "sources": {"webhook": True}, "webhook": {"sinks": ["crosswatch"], "plex_crosswatch_ratings": True}}}

    plex.process_webhook(_rate_payload("rk-9"), {}, cfg=cfg, provider_instance="P1")
    plex.process_webhook(_rate_payload("rk-9"), {}, cfg=cfg, provider_instance="P2")

    assert len(sent) == 2


def test_unknown_route_provider_is_disabled_not_rewritten():
    from providers.scrobble.routes import normalize_route

    unknown = normalize_route({"id": "R1", "provider": "jellyfinn", "sink": "trakt", "enabled": True}, "R1")

    assert unknown["provider"] == "jellyfinn"
    assert unknown["enabled"] is False
    assert normalize_route({"id": "R2", "sink": "trakt"}, "R2")["provider"] == "plex"


def test_export_requires_a_provider():
    from fastapi import HTTPException

    from services import export

    with pytest.raises(HTTPException) as exc:
        export._export_file("", "all", "watchlist", "letterboxd", "movie", True, True, "", "", None)

    assert exc.value.status_code == 400


def test_trakt_queued_delivery_keeps_the_selected_account(monkeypatch):
    from providers.scrobble.scrobble import ScrobbleEvent
    from providers.scrobble.trakt import sink as trakt_sink

    jobs: list[dict[str, Any]] = []
    monkeypatch.setattr(trakt_sink, "_TOKEN_OVERRIDE", {})
    monkeypatch.setattr(trakt_sink, "record_watch", lambda *a, **kw: None)
    target = trakt_sink.TraktSink(instance_id="T2")
    monkeypatch.setattr(target, "_enqueue", jobs.append)
    cfg = {"trakt": {"client_id": "cid", "access_token": "default-token", "instances": {"T2": {"access_token": "t2-token"}}}}
    ev = ScrobbleEvent(action="start", media_type="movie", ids={"tmdb": 550}, title="Fight Club", year=1999, season=None,
                       number=None, progress=10, account="viewer", server_uuid="srv", session_key="sess-t2", raw={})

    target.send(ev, cfg)

    assert len(jobs) == 1
    headers = trakt_sink._hdr(jobs[0]["cfg"], "T2")
    assert headers["trakt-api-key"] == "cid"
    assert headers["Authorization"] == "Bearer t2-token"


def test_mdblist_scrobble_to_a_missing_account_is_not_sent():
    from providers.scrobble.mdblist.sink import MDBListSink

    cfg = {"mdblist": {"api_key": "default-key", "instances": {"M2": {"api_key": "m2-key"}}}}

    result = MDBListSink(instance_id="M3").send(_event(), cfg)

    assert result["reason"] == "not_configured"


def test_scrob_sink_prepares_a_supplied_route_config(monkeypatch):
    from providers.scrobble.scrob import sink as scrob_sink

    seen: list[str] = []
    target = scrob_sink.ScrobSink(instance_id="default")
    original_block = target._block
    monkeypatch.setattr(target, "_block", lambda cfg: seen.append(original_block(cfg).get("server_url")) or original_block(cfg))
    monkeypatch.setattr(target, "_kodi_item", lambda *a: None)
    cfg = {
        "scrob": {"server_url": "https://source.test", "api_key": "source-key"},
        "_cw_scrobble_sink": {"provider": "scrob", "instance": "default", "config": {"server_url": "https://target.test", "api_key": "target-key"}},
    }

    target.send(_event(), cfg=cfg)

    assert seen[0] == "https://target.test"
