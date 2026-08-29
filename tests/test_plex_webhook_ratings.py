# CrossWatch test scripts
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def _js_const_array(relative_path: str, const_name: str) -> list[str]:
    text = (ROOT / relative_path).read_text(encoding="utf-8")
    match = re.search(rf"\bconst\s+{re.escape(const_name)}\s*=\s*(\[[^\]]*\])", text)
    assert match, f"{const_name} not found in {relative_path}"
    return list(json.loads(match.group(1)))


def _request(path: str = "/api/scrobbler/overview"):
    from starlette.requests import Request

    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "server": ("testserver", 80),
            "scheme": "http",
            "client": ("testclient", 50000),
            "app": object(),
        }
    )


def test_plex_webhook_ratings_forward_to_crosswatch_and_floppy(monkeypatch) -> None:
    from providers.scrobble.plex import ratings_sync
    from providers.webhooks import plex

    sent: list[dict[str, Any]] = []

    def fake_send_rating(provider: str, cfg: dict[str, Any], instance: str, item: dict[str, Any], rating: int) -> dict[str, Any]:
        sent.append({"provider": provider, "instance": instance, "item": item, "rating": rating})
        return {"ok": True, "resp": {"confirmed_keys": ["tmdb:550"]}}

    monkeypatch.setattr(plex, "_save_config", lambda _cfg: None)
    monkeypatch.setattr(ratings_sync, "send_rating", fake_send_rating)
    plex._LAST_RATING_BY_ACC.clear()

    cfg = {
        "scrobble": {
            "enabled": True,
            "sources": {"webhook": True},
            "webhook": {
                "sinks": ["crosswatch", "floppy"],
                "sink_instances": {"crosswatch": "CW-P01", "floppy": "F1"},
                "plex_crosswatch_ratings": True,
                "plex_floppy_ratings": True,
            },
        },
        "crosswatch": {"connected": True, "instances": {"CW-P01": {"connected": True}}},
        "floppy": {
            "server_url": "http://floppy.test",
            "api_token": "token",
            "instances": {"F1": {"server_url": "http://floppy.test", "api_token": "token"}},
        },
    }
    payload = {
        "event": "media.rate",
        "Account": {"title": "pasca"},
        "Metadata": {
            "type": "movie",
            "title": "Fight Club",
            "year": 1999,
            "ratingKey": "rk-550",
            "userRating": 8,
            "Guid": [{"id": "tmdb://550"}],
        },
    }

    result = plex.process_webhook(payload, {}, cfg=cfg)

    assert result["ok"] is True
    assert {call["provider"] for call in sent} == {"crosswatch", "floppy"}
    assert {call["instance"] for call in sent} == {"CW-P01", "F1"}
    assert all(call["item"]["ids"]["tmdb"] == 550 for call in sent)
    assert all(call["rating"] == 8 for call in sent)


def test_global_plex_rating_surfaces_cover_dispatcher_sinks(monkeypatch) -> None:
    from api import scrobblerManagementAPI as management
    from providers.scrobble.plex import ratings_sync
    from providers.scrobble.plex.ratings_sync import RATING_SINKS
    from providers.sync._mod_SCROB import OPS as SCROB_OPS

    rating_sinks = set(RATING_SINKS)

    assert ratings_sync._ops("scrob") is SCROB_OPS
    assert SCROB_OPS.features()["ratings"] is True
    assert set(_js_const_array("assets/js/scrobbler.js", "plexRatingSinks")) == rating_sinks
    assert rating_sinks <= set(_js_const_array("assets/js/modals/scrobbler-webhook/index.js", "ratingSinks"))
    assert rating_sinks <= set(_js_const_array("assets/js/modals/scrobbler-route/index.js", "ratingSinks"))

    cfg = {
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True, "webhook": False},
            "watch": {f"plex_{sink}_ratings": True for sink in RATING_SINKS},
        },
        "security": {"webhook_ids": {"plexwatcher": "ratings-token"}},
    }
    monkeypatch.setattr(management, "_runtime_status", lambda _request, _cfg: {})
    monkeypatch.setattr(management, "request_user", lambda _request: None)

    overview = management.build_overview(cfg, _request())
    global_ratings = overview["source_state"]["global_plex_ratings"]

    assert set(global_ratings) - {"endpoint_url"} == rating_sinks
    assert all(global_ratings[sink] is True for sink in RATING_SINKS)
    assert global_ratings["endpoint_url"] == "http://testserver/webhook/plexwatcher?token=ratings-token"

    saved: dict[str, Any] = {}
    monkeypatch.setattr(management, "load_config", lambda: {"scrobble": {"watch": {}}})
    monkeypatch.setattr(management, "save_config", lambda cfg: saved.update(cfg))
    monkeypatch.setattr(management, "_after_config_save", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(management, "_watch_runtime_changed", lambda _before, _after: False)

    response = management.api_scrobbler_settings(
        _request("/api/scrobbler/settings"),
        {"global_plex_ratings": {sink: True for sink in RATING_SINKS}},
    )
    body = json.loads(response.body)

    assert body["ok"] is True
    assert all(saved["scrobble"]["watch"][f"plex_{sink}_ratings"] is True for sink in RATING_SINKS)


def test_scrobbler_overview_reuses_runtime_status(monkeypatch) -> None:
    from api import scrobblerManagementAPI as management

    calls = 0

    def fake_runtime(_request, _cfg):
        nonlocal calls
        calls += 1
        return {
            "running": True,
            "alive": True,
            "groups": [],
            "routes": [{"id": "R1", "running": True, "sink": "trakt"}],
        }

    cfg = {
        "trakt": {"access_token": "token"},
        "plex": {"token": "token", "baseurl": "http://plex"},
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True},
            "watch": {
                "routes": [
                    {
                        "id": "R1",
                        "enabled": True,
                        "provider": "plex",
                        "provider_instance": "default",
                        "sink": "trakt",
                        "sink_instance": "default",
                    }
                ]
            },
        },
    }
    monkeypatch.setattr(management, "_runtime_status", fake_runtime)
    monkeypatch.setattr(management, "_webhook_cards", lambda _cfg, _request: [])
    monkeypatch.setattr(management, "_eligible_sources", lambda _cfg: [])
    monkeypatch.setattr(management, "_destination_availability", lambda _cfg: [])
    monkeypatch.setattr(management, "request_user", lambda _request: None)

    overview = management.build_overview(cfg, _request())

    assert calls == 1
    assert overview["watcher_runtime"]["running"] is True
    assert overview["routes"][0]["runtime"]["running"] is True


def test_global_plex_rating_targets_only_show_configured_or_enabled_sinks() -> None:
    js = (ROOT / "assets" / "js" / "scrobbler.js").read_text(encoding="utf-8")
    block = js.split("function renderRatingsWebhook(o)", 1)[1].split("function renderWebhookDefaults(o)", 1)[0]

    assert "const configured = new Set((o.destination_availability || [])" in block
    assert ".filter((g) => (g?.profiles || []).some((p) => p?.configured))" in block
    assert "const visibleSinks = plexRatingSinks.filter((k) => configured.has(k) || r[k]);" in block
    assert "visibleSinks.map((k)" in block
    assert "plexRatingSinks.map((k)" not in block
    assert "Configure a ratings destination first." in block


def test_scrobbler_rating_modals_keep_enabled_unconfigured_sinks_visible() -> None:
    webhook = (ROOT / "assets" / "js" / "modals" / "scrobbler-webhook" / "index.js").read_text(encoding="utf-8")
    route = (ROOT / "assets" / "js" / "modals" / "scrobbler-route" / "index.js").read_text(encoding="utf-8")

    webhook_block = webhook.split("function visibleRatingSinks(selected = [])", 1)[1].split("function selectedSinkKey()", 1)[0]
    assert "const available = availableRatingSinks();" in webhook_block
    assert "ratingSinks.includes(x) && x !== self" in webhook_block
    assert "return [...selectedList.filter((x) => !available.includes(x)), ...available];" in webhook_block

    webhook_panel = webhook.split("function ratingsPanel(provider, ratingsTargets)", 1)[1].split("function optionsPanel()", 1)[0]
    assert "const targets = visibleRatingSinks(ratingsTargets);" in webhook_panel
    assert 'class="scrm-target${configured ? "" : " is-unconfigured"}"' in webhook_panel
    assert " - not configured" in webhook_panel

    route_render = route.split("function enableSelectedUnavailableRatingTargets()", 1)[1].split("function normalizeActiveTab", 1)[0]
    assert '.scrm-target.is-disabled input[data-rating-target]:checked' in route_render
    assert "input.disabled = false;" in route_render
    assert 'target?.classList.add("is-unconfigured");' in route_render


def test_scrobbler_loader_waits_for_auth_bootstrap_before_blocking() -> None:
    scrobbler = (ROOT / "assets" / "js" / "scrobbler.js").read_text(encoding="utf-8")
    core = (ROOT / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")

    auth_block = scrobbler.split("async function authSetupBlocked()", 1)[1].split("async function j", 1)[0]
    assert "w.__cwAuthBootstrapState" in auth_block
    assert "w.__cwAuthBootstrapPromise" in auth_block
    assert "await pending.catch(() => null)" in auth_block
    assert "return resolved.blocked;" in auth_block
    assert "if (await authSetupBlocked()) throw authPendingError();" in scrobbler
    assert 'err.code = "auth_setup_pending";' in scrobbler
    assert "renderAuthPending();" in scrobbler
    assert 'Promise.resolve(boot).catch((err) => console.warn("[scrobbler] init failed", err));' in core
    assert 'Promise.resolve(window.Scrobbler?.refresh?.()).catch((err) => console.warn("[scrobbler] refresh failed", err));' in core


def test_send_rating_supports_scrob_ops_sink(monkeypatch) -> None:
    from providers.scrobble.plex.ratings_sync import send_rating
    from providers.sync.scrob import _ratings as scrob_ratings

    class Resp:
        status_code = 200
        text = "{}"

        def json(self) -> dict[str, Any]:
            return {"ok": True}

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(scrob_ratings, "scrob_request", lambda adapter, method, path, **kwargs: calls.append({"method": method, "path": path, **kwargs}) or Resp())

    cfg = {"scrob": {"server_url": "http://scrob.test", "api_key": "scrob-token", "username": "pasca", "password": "secret"}}
    item = {"type": "movie", "ids": {"tmdb": "550"}, "title": "Fight Club", "rating": 8}

    result = send_rating("scrob", cfg, "default", item, 8)

    assert result["ok"] is True
    assert calls == [{"method": "POST", "path": scrob_ratings.PATH_RATINGS, "json": {"tmdb_id": 550, "media_type": "movie", "rating": 8.0}}]


def test_plex_webhook_ratings_forward_to_every_dispatcher_sink(monkeypatch) -> None:
    from providers.scrobble.plex import ratings_sync
    from providers.scrobble.plex import watch as plex_watch
    from providers.webhooks import plex

    class Resp:
        status_code = 200
        text = "{}"

        def json(self) -> dict[str, Any]:
            return {"ok": True}

    trakt_calls: list[dict[str, Any]] = []
    ops_calls: list[dict[str, Any]] = []

    def fake_send_rating(provider: str, cfg: dict[str, Any], instance: str, item: dict[str, Any], rating: int) -> dict[str, Any]:
        ops_calls.append({"provider": provider, "instance": instance, "item": item, "rating": rating})
        return {"ok": True, "resp": {"confirmed_keys": ["tmdb:550"]}}

    monkeypatch.setattr(plex, "_save_config", lambda _cfg: None)
    monkeypatch.setattr(plex, "_post_trakt", lambda path, body, cfg: trakt_calls.append({"path": path, "body": body}) or Resp())
    monkeypatch.setattr(plex_watch, "_simkl_send_rating", lambda media_type, ids, rating, cfg, logger: {"ok": True, "provider": "simkl"})
    monkeypatch.setattr(plex_watch, "_mdblist_send_rating", lambda media_type, ids, rating, cfg, logger: {"ok": True, "provider": "mdblist"})
    monkeypatch.setattr(ratings_sync, "send_rating", fake_send_rating)
    plex._LAST_RATING_BY_ACC.clear()

    flags = {f"plex_{sink}_ratings": True for sink in ratings_sync.RATING_SINKS}
    cfg = {
        "scrobble": {
            "enabled": True,
            "sources": {"webhook": True},
            "webhook": {
                "sinks": list(ratings_sync.RATING_SINKS),
                "sink_instances": {sink: f"{sink}-profile" for sink in ratings_sync.RATING_SINKS},
                "pause_debounce_seconds": 5,
                "suppress_start_at": 99,
                "probe_session_progress": True,
                **flags,
            },
            "trakt": {"stop_pause_threshold": 80, "force_stop_at": 95, "regress_tolerance_percent": 5},
        },
        "trakt": {"access_token": "trakt-token", "instances": {"trakt-profile": {"access_token": "trakt-token"}}},
        "simkl": {"access_token": "simkl-token", "instances": {"simkl-profile": {"access_token": "simkl-token"}}},
        "mdblist": {"api_key": "mdblist-token", "instances": {"mdblist-profile": {"api_key": "mdblist-token"}}},
        "crosswatch": {"enabled": True, "instances": {"crosswatch-profile": {"enabled": True}}},
        "floppy": {"instances": {"floppy-profile": {"server_url": "http://floppy.test", "api_token": "floppy-token"}}},
        "punchplay": {"access_token": "punchplay-token", "instances": {"punchplay-profile": {"access_token": "punchplay-token"}}},
        "flicklist": {"api_key": "flicklist-token", "instances": {"flicklist-profile": {"api_key": "flicklist-token"}}},
        "scrob": {
            "server_url": "http://scrob.test",
            "api_key": "scrob-token",
            "username": "pasca",
            "password": "secret",
            "instances": {
                "scrob-profile": {
                    "server_url": "http://scrob.test",
                    "api_key": "scrob-token",
                    "username": "pasca",
                    "password": "secret",
                }
            },
        },
    }
    payload = {
        "event": "media.rate",
        "Account": {"title": "pasca"},
        "Metadata": {
            "type": "movie",
            "title": "Fight Club",
            "year": 1999,
            "ratingKey": "rk-550-all",
            "userRating": 8,
            "Guid": [{"id": "tmdb://550"}],
        },
    }

    result = plex.process_webhook(payload, {}, cfg=cfg)

    assert result["ok"] is True
    assert set(result) >= {"trakt", "simkl", "mdblist", *ratings_sync.OPS_RATING_SINKS}
    assert trakt_calls and trakt_calls[0]["path"] == "/sync/ratings"
    assert {call["provider"] for call in ops_calls} == set(ratings_sync.OPS_RATING_SINKS)
    assert {call["instance"] for call in ops_calls} == {f"{sink}-profile" for sink in ratings_sync.OPS_RATING_SINKS}
