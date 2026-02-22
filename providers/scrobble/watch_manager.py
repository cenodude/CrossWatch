# providers/scrobble/watch_manager.py
# CrossWatch - Watch mode route manager (grouped watchers)
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import inspect
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, cast

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from cw_platform.config_base import load_config
from providers.scrobble.routes import build_route_cfg, normalize_routes
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="WATCHM")
            return
        except Exception:
            pass
    try:
        print(f"[WATCHM:{lvl}] {msg}")
    except Exception:
        pass


def _supports_kw(fn: Any, name: str) -> bool:
    try:
        return name in inspect.signature(fn).parameters
    except Exception:
        return False


def _stop_blocking(w: Any, timeout: float = 6.0) -> bool:
    if not w:
        return True
    try:
        if hasattr(w, "stop"):
            w.stop()
    except Exception:
        pass
    end = time.monotonic() + max(0.2, float(timeout))
    while time.monotonic() < end:
        try:
            alive = bool(getattr(w, "is_alive", lambda: False)())
        except Exception:
            return True
        if not alive:
            return True
        time.sleep(0.05)
    return False


class MultiDispatcher:
    def __init__(self, dispatchers: list[Dispatcher]) -> None:
        self._dispatchers = list(dispatchers or [])

    def dispatch(self, event: ScrobbleEvent) -> None:
        for d in self._dispatchers:
            try:
                d.dispatch(event)
            except Exception as e:
                _log(f"Route dispatcher error: {e}", "ERROR")


class _DispatchSink:
    def __init__(self, dispatcher: MultiDispatcher) -> None:
        self._dispatcher = dispatcher

    def send(self, event: ScrobbleEvent, *args: Any, **kwargs: Any) -> None:
        self._dispatcher.dispatch(event)


@dataclass
class RouteRunner:
    route: dict[str, Any]
    sink: Any
    dispatcher: Dispatcher


@dataclass
class WatchGroup:
    provider: str
    provider_instance: str
    watcher: Any
    routes: list[RouteRunner]
    started_at: float


def _make_sink(name: str, cfg_provider: Callable[[], dict[str, Any]], instance_id: str) -> Any:
    sink = (name or "").strip().lower()
    if not sink:
        raise ValueError("Empty sink")
    cls: Any | None = None
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
            return cls(**kwargs)
        except TypeError:
            continue
    return cls()


def _make_watcher(provider: str, group_dispatcher: MultiDispatcher, cfg_provider: Callable[[], dict[str, Any]], instance_id: str) -> Any:
    prov = (provider or "plex").strip().lower() or "plex"
    make_watch: Any
    if prov == "emby":
        from providers.scrobble.emby.watch import make_default_watch as make_watch
    elif prov == "jellyfin":
        from providers.scrobble.jellyfin.watch import make_default_watch as make_watch
    else:
        from providers.scrobble.plex.watch import make_default_watch as make_watch
        prov = "plex"

    if _supports_kw(make_watch, "dispatcher") or _supports_kw(make_watch, "cfg_provider") or _supports_kw(make_watch, "instance_id"):
        kwargs: dict[str, Any] = {}
        if _supports_kw(make_watch, "dispatcher"):
            kwargs["dispatcher"] = cast(Any, group_dispatcher)
        if _supports_kw(make_watch, "cfg_provider"):
            kwargs["cfg_provider"] = cfg_provider
        if _supports_kw(make_watch, "instance_id"):
            kwargs["instance_id"] = instance_id
        if _supports_kw(make_watch, "sinks"):
            kwargs["sinks"] = []
        return make_watch(**kwargs)

    if prov == "plex":
        try:
            from providers.scrobble.plex.watch import WatchService

            if _supports_kw(WatchService.__init__, "dispatcher"):
                kwargs: dict[str, Any] = {"dispatcher": cast(Any, group_dispatcher)}
                if _supports_kw(WatchService.__init__, "cfg_provider"):
                    kwargs["cfg_provider"] = cfg_provider
                if _supports_kw(WatchService.__init__, "instance_id"):
                    kwargs["instance_id"] = instance_id
                return WatchService(**kwargs)
        except Exception:
            pass

    return make_watch(sinks=[_DispatchSink(group_dispatcher)])


class WatchManager:
    def __init__(self, app: Any) -> None:
        self._app = app
        self._lock = threading.RLock()

    def start_from_config(self) -> dict[str, Any]:
        with self._lock:
            self.stop_all()

            cfg = load_config() or {}
            sc = (cfg.get("scrobble") or {}) or {}
            if not bool(sc.get("enabled")) or str(sc.get("mode") or "").lower() != "watch":
                self._app.state.watch_groups = {}
                return self.status()

            routes = [r for r in normalize_routes(cfg) if isinstance(r, dict) and bool(r.get("enabled"))]
            grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for r in routes:
                prov = str(r.get("provider") or "plex").strip().lower() or "plex"
                inst = str(r.get("provider_instance") or "default").strip() or "default"
                grouped.setdefault((prov, inst), []).append(r)

            watch_groups: dict[str, WatchGroup] = {}
            for (prov, inst), rs in grouped.items():
                runners: list[RouteRunner] = []

                for route in rs:
                    def route_cfg_provider(route_obj: dict[str, Any] = route) -> dict[str, Any]:
                        return build_route_cfg(load_config() or {}, route_obj)

                    sink_name = str(route.get("sink") or "").strip().lower()
                    if not sink_name:
                        continue
                    sink_inst = str(route.get("sink_instance") or "default").strip() or "default"
                    try:
                        sink = _make_sink(sink_name, route_cfg_provider, sink_inst)
                    except Exception as e:
                        _log(f"Skipping route with invalid sink '{sink_name}': {e}", "WARNING")
                        continue
                    disp = Dispatcher([sink], cfg_provider=route_cfg_provider)
                    runners.append(RouteRunner(route=route, sink=sink, dispatcher=disp))

                if not runners:
                    continue

                md = MultiDispatcher([rr.dispatcher for rr in runners])

                def group_cfg_provider(p: str = prov, i: str = inst) -> dict[str, Any]:
                    return build_route_cfg(
                        load_config() or {},
                        {"provider": p, "provider_instance": i, "sink": "", "sink_instance": "default", "filters": {}},
                    )

                watcher = _make_watcher(prov, md, group_cfg_provider, inst)
                if hasattr(watcher, "start_async"):
                    watcher.start_async()
                else:
                    threading.Thread(target=watcher.start, daemon=True).start()

                key = f"{prov}:{inst}"
                watch_groups[key] = WatchGroup(
                    provider=prov,
                    provider_instance=inst,
                    watcher=watcher,
                    routes=runners,
                    started_at=time.time(),
                )

            self._app.state.watch_groups = watch_groups
            return self.status()

    def stop_all(self) -> dict[str, Any]:
        with self._lock:
            groups = getattr(self._app.state, "watch_groups", None)
            if isinstance(groups, dict):
                for g in list(groups.values()):
                    try:
                        _stop_blocking(getattr(g, "watcher", None))
                    except Exception:
                        pass
            self._app.state.watch_groups = {}
            return self.status()

    def status(self) -> dict[str, Any]:
        groups = getattr(self._app.state, "watch_groups", None)
        if not isinstance(groups, dict):
            groups = {}

        out_groups: list[dict[str, Any]] = []
        out_routes: list[dict[str, Any]] = []

        for k, g in groups.items():
            try:
                alive = bool(getattr(g.watcher, "is_alive", lambda: False)())
            except Exception:
                alive = False
            out_groups.append(
                {
                    "id": k,
                    "provider": g.provider,
                    "provider_instance": g.provider_instance,
                    "running": bool(alive),
                    "routes": [str(rr.route.get("id") or "") for rr in (g.routes or [])],
                }
            )
            for rr in (g.routes or []):
                r = rr.route or {}
                out_routes.append(
                    {
                        "id": str(r.get("id") or ""),
                        "provider": str(r.get("provider") or g.provider),
                        "provider_instance": str(r.get("provider_instance") or g.provider_instance),
                        "sink": str(r.get("sink") or ""),
                        "sink_instance": str(r.get("sink_instance") or "default"),
                        "enabled": bool(r.get("enabled", True)),
                        "running": bool(alive) and bool(r.get("enabled", True)),
                    }
                )

        return {"groups": out_groups, "routes": out_routes}


def get_manager(app: Any) -> WatchManager:
    mgr = getattr(app.state, "watch_manager", None)
    if isinstance(mgr, WatchManager):
        return mgr
    mgr = WatchManager(app)
    app.state.watch_manager = mgr
    return mgr


def start_from_config(app: Any) -> dict[str, Any]:
    return get_manager(app).start_from_config()


def stop_all(app: Any) -> dict[str, Any]:
    return get_manager(app).stop_all()


def status(app: Any) -> dict[str, Any]:
    return get_manager(app).status()


__all__ = (
    "WatchManager",
    "WatchGroup",
    "RouteRunner",
    "MultiDispatcher",
    "get_manager",
    "start_from_config",
    "stop_all",
    "status",
)
