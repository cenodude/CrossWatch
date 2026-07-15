# providers/webhooks/dispatch.py
# CrossWatch - Media-server webhook scrobble dispatcher
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
from collections.abc import Callable, Mapping
from typing import Any

from cw_platform.provider_instances import normalize_instance_id
from providers.scrobble.scrobble import ScrobbleAction, ScrobbleEvent
from providers.webhooks.config import sink_configured, webhook_settings, webhook_sink_instance, webhook_sinks


class DispatchResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = int(status_code)
        self._payload = dict(payload)
        self.text = str(payload)

    def json(self) -> dict[str, Any]:
        return dict(self._payload)


_SINKS: dict[tuple[str, str, str], Any] = {}


def _emit(logger: Callable[..., None] | Any | None, msg: str, level: str = "INFO") -> None:
    if not logger:
        return
    try:
        logger(msg, level=level)
    except TypeError:
        try:
            logger(msg, level)
        except Exception:
            pass
    except Exception:
        pass


def _action(path: str) -> ScrobbleAction:
    tail = str(path or "").strip().lower().rsplit("/", 1)[-1]
    if tail == "pause":
        return "pause"
    if tail == "stop":
        return "stop"
    return "start"


def _int(v: Any) -> int | None:
    try:
        return int(v) if v not in (None, "") else None
    except Exception:
        return None


def _make_sink(name: str, instance_id: str, cfg_provider: Callable[[], dict[str, Any]]) -> Any:
    sink = str(name or "").strip().lower()
    key = (sink, normalize_instance_id(instance_id), "webhook")
    if key in _SINKS:
        return _SINKS[key]
    cls: Any
    if sink == "trakt":
        from providers.scrobble.trakt.sink import TraktSink

        cls = TraktSink
    elif sink == "simkl":
        from providers.scrobble.simkl.sink import SimklSink

        cls = SimklSink
    elif sink == "mdblist":
        from providers.scrobble.mdblist.sink import MDBListSink

        cls = MDBListSink
    else:
        raise ValueError(f"Unknown sink: {sink}")
    for kwargs in (
        {"cfg_provider": cfg_provider, "instance_id": instance_id},
        {"cfg_provider": cfg_provider},
        {"instance_id": instance_id},
        {},
    ):
        try:
            obj = cls(**kwargs)
            _SINKS[key] = obj
            return obj
        except TypeError:
            continue
    obj = cls()
    _SINKS[key] = obj
    return obj


def _route_cfg(cfg: dict[str, Any], provider: str, provider_instance: str, sink: str, sink_instance: str) -> dict[str, Any]:
    out = copy.deepcopy(cfg) if isinstance(cfg, dict) else {}
    sc = out.setdefault("scrobble", {})
    watch = sc.setdefault("watch", {})
    watch["route_id"] = f"webhook:{provider}:{normalize_instance_id(provider_instance)}:{sink}:{sink_instance}"
    watch["route_provider"] = provider
    watch["route_provider_instance"] = normalize_instance_id(provider_instance)
    watch["route_sink"] = sink
    watch["route_sink_instance"] = sink_instance
    watch["event_method"] = "webhook"
    return out


def _event(
    *,
    provider: str,
    path: str,
    media_type: str,
    ids: Mapping[str, Any] | None,
    title: Any,
    year: Any,
    season: Any,
    episode: Any,
    progress: Any,
    account: Any,
    server_uuid: Any,
    session_key: Any,
    raw: Mapping[str, Any] | None,
) -> ScrobbleEvent:
    raw2 = dict(raw or {})
    raw2["_cw_activity_method"] = "webhook"
    raw2["_cw_source_provider"] = provider
    return ScrobbleEvent(
        action=_action(path),
        media_type="episode" if str(media_type or "").strip().lower() == "episode" else "movie",
        ids={str(k): str(v) for k, v in dict(ids or {}).items() if v not in (None, "")},
        title=str(title).strip() if title not in (None, "") else None,
        year=_int(year),
        season=_int(season),
        number=_int(episode),
        progress=max(0.0, min(100.0, float(progress or 0))),
        account=str(account).strip() if account not in (None, "") else None,
        server_uuid=str(server_uuid).strip() if server_uuid not in (None, "") else None,
        session_key=str(session_key).strip() if session_key not in (None, "") else None,
        raw=raw2,
    )


def dispatch_scrobble(
    provider: str,
    path: str,
    *,
    media_type: str,
    ids: Mapping[str, Any] | None,
    title: Any = None,
    year: Any = None,
    season: Any = None,
    episode: Any = None,
    progress: Any = 0,
    account: Any = None,
    server_uuid: Any = None,
    session_key: Any = None,
    raw: Mapping[str, Any] | None = None,
    cfg: dict[str, Any] | None = None,
    provider_instance: str | None = None,
    logger: Callable[..., None] | Any | None = None,
) -> DispatchResponse:
    cfg = cfg if isinstance(cfg, dict) else {}
    provider_lc = str(provider or "").strip().lower() or "webhook"
    provider_inst = normalize_instance_id(provider_instance)
    wh = webhook_settings(cfg, provider_lc, provider_inst)
    sinks = webhook_sinks(cfg, provider_lc, provider_inst)
    ev = _event(
        provider=provider_lc,
        path=path,
        media_type=media_type,
        ids=ids,
        title=title,
        year=year,
        season=season,
        episode=episode,
        progress=progress,
        account=account,
        server_uuid=server_uuid,
        session_key=session_key,
        raw=raw,
    )
    targets: list[dict[str, Any]] = []
    dispatched: list[str] = []
    for sink in sinks:
        inst = webhook_sink_instance(wh, sink)
        if not sink_configured(cfg, sink, inst):
            targets.append({"target": sink, "target_instance": inst, "ok": False, "skipped": True, "error": "not_configured"})
            _emit(logger, f"webhook sink {sink}:{inst} skipped: not configured in Connections", "WARNING")
            continue
        dispatched.append(sink)
        route_cfg = _route_cfg(cfg, provider_lc, provider_inst, sink, inst)

        def _provider(route_cfg: dict[str, Any] = route_cfg) -> dict[str, Any]:
            return route_cfg

        target = {"target": sink, "target_instance": inst, "ok": True}
        try:
            _make_sink(sink, inst, _provider).send(ev, cfg=route_cfg)
        except Exception as e:
            target["ok"] = False
            target["error"] = str(e)
            _emit(logger, f"webhook sink {sink}:{inst} failed: {e}", "ERROR")
        targets.append(target)
    ok = not targets or any(bool(t.get("ok")) for t in targets)
    payload = {
        "action": path,
        "status": 200 if ok else 502,
        "route_dispatch": True,
        "activity_recorded": True,
        "targets": targets,
    }
    _emit(logger, f"webhook dispatch {path} -> {','.join(dispatched) or 'none'} status={payload['status']}", "DEBUG")
    return DispatchResponse(payload["status"], payload)
