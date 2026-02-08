# /api/scrobbleAPI.py
# CrossWatch - Scrobbling and Webhook API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import time
import re
import threading
import urllib.parse
from typing import Any

from fastapi import APIRouter, Query, Request, HTTPException
from fastapi.responses import JSONResponse
from urllib.parse import parse_qs

from cw_platform.config_base import load_config
from cw_platform.provider_instances import build_provider_config_view, normalize_instance_id
from providers.scrobble.currently_watching import state_file as _cw_state_file

try:
    from providers.scrobble.watch_manager import start_from_config as _wm_start_from_config
    from providers.scrobble.watch_manager import status as _wm_status
    from providers.scrobble.watch_manager import stop_all as _wm_stop_all
    HAVE_WATCH_MANAGER = True
except Exception:
    _wm_start_from_config = None  # type: ignore[assignment]
    _wm_status = None  # type: ignore[assignment]
    _wm_stop_all = None  # type: ignore[assignment]
    HAVE_WATCH_MANAGER = False


try:
    from .maintenanceAPI import reset_currently_watching as _reset_currently_watching  # type: ignore[attr-defined]
except Exception:
    _reset_currently_watching = None  # type: ignore[assignment]

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

try:
    from plexapi.myplex import MyPlexAccount
    HAVE_PLEXAPI = True
except Exception:
    MyPlexAccount = None  # type: ignore[assignment]
    HAVE_PLEXAPI = False

router = APIRouter(tags=["scrobbler"])


def _env_logs(request: Request | None = None) -> tuple[dict[str, list[str]], int]:
    if request is not None:
        try:
            lb = getattr(request.app.state, "LOG_BUFFERS", None)
            ml = getattr(request.app.state, "MAX_LOG_LINES", None)
            if isinstance(lb, dict) and isinstance(ml, int):
                return lb, ml
        except Exception:
            pass
    try:
        import crosswatch as CW

        return getattr(CW, "LOG_BUFFERS", {}), getattr(CW, "MAX_LOG_LINES", 2000)
    except Exception:
        return {}, 2000


def _debug_on() -> bool:
    try:
        cfg = load_config() or {}
        rt = (cfg.get("runtime") or {}) or {}
        return bool(rt.get("debug") or rt.get("debug_mods"))
    except Exception:
        return False

def _stop_watch_blocking(w: Any, timeout: float = 6.0) -> bool:
    try:
        w.stop()
    except Exception:
        pass
    end = time.monotonic() + max(0.2, float(timeout))
    while time.monotonic() < end:
        try:
            if not bool(getattr(w, "is_alive", lambda: False)()):
                return True
        except Exception:
            return True
        time.sleep(0.05)
    return False

def _watch_kind(w: Any) -> str | None:
    try:
        name = getattr(getattr(w, "__class__", None), "__name__", "") or ""
        n = name.lower()
        if "emby" in n:
            return "emby"
        if "jellyfin" in n:
            return "jellyfin"
        if "plex" in n:
            return "plex"
    except Exception:
        pass
    return None


def _plex_token(cfg: dict[str, Any]) -> str:
    return ((cfg.get("plex") or {}).get("account_token") or "").strip()


def _plex_client_id(cfg: dict[str, Any]) -> str:
    return (cfg.get("plex") or {}).get("client_id") or "crosswatch"


def _account(cfg: dict[str, Any]) -> Any:
    tok = _plex_token(cfg)
    if not HAVE_PLEXAPI or not tok:
        return None
    try:
        return MyPlexAccount(token=tok)  # type: ignore[call-arg]
    except Exception:
        return None


def _resolve_plex_server_uuid(cfg: dict[str, Any]) -> str:
    plex = cfg.get("plex") or {}
    if plex.get("server_uuid"):
        return str(plex["server_uuid"]).strip()

    acc = _account(cfg)
    if not acc:
        return ""

    host_hint = ""
    base = (plex.get("server_url") or "").strip()
    if base:
        try:
            host_hint = urllib.parse.urlparse(base).hostname or ""
        except Exception:
            host_hint = ""

    try:
        servers = [
            r
            for r in acc.resources()
            if "server" in (r.provides or "")
            and (r.product or "") == "Plex Media Server"
        ]
        owned = [r for r in servers if getattr(r, "owned", False)]

        def matches_host(res: Any) -> bool:
            if not host_hint:
                return False
            for c in (res.connections or []):
                if host_hint in (c.uri or "") or host_hint == (c.address or ""):
                    return True
            return False

        for res in owned:
            if matches_host(res):
                return res.clientIdentifier or ""
        if owned:
            return owned[0].clientIdentifier or ""
        return servers[0].clientIdentifier if servers else ""
    except Exception:
        return ""


def _fetch_owner_and_managed(cfg: dict[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    acc = _account(cfg)
    if not acc:
        return None, []

    owner: dict[str, Any] = {
        "id": str(getattr(acc, "id", "") or ""),
        "username": (
            getattr(acc, "username", "")
            or getattr(acc, "title", "")
            or getattr(acc, "email", "")
            or ""
        ).strip(),
        "title": (
            getattr(acc, "title", "")
            or getattr(acc, "username", "")
            or ""
        ).strip(),
        "email": (getattr(acc, "email", "") or "").strip(),
        "type": "owner",
    }

    managed: list[dict[str, Any]] = []
    try:
        home = getattr(acc, "home", None)
        if home:
            for u in home.users():
                uid = str(getattr(u, "id", "") or "").strip()
                if not uid:
                    continue
                uname = (
                    getattr(u, "username", "")
                    or getattr(u, "title", "")
                    or ""
                ).strip()
                managed.append(
                    {
                        "id": uid,
                        "username": uname,
                        "title": (getattr(u, "title", "") or uname).strip(),
                        "email": (getattr(u, "email", "") or "").strip(),
                        "type": "managed",
                    }
                )
    except Exception:
        pass
    return owner, managed


def _list_plex_users(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    users: list[dict[str, Any]] = []
    acc = _account(cfg)
    if acc:
        try:
            for u in acc.users():
                users.append(
                    {
                        "id": str(getattr(u, "id", "") or ""),
                        "username": (u.username or u.title or u.email or "").strip(),
                        "title": (u.title or u.username or "").strip(),
                        "email": (getattr(u, "email", "") or "").strip(),
                        "type": "friend",
                    }
                )
        except Exception:
            pass

    owner, managed = _fetch_owner_and_managed(cfg)
    if owner:
        users.append(owner)
    users.extend(managed)

    rank = {"owner": 3, "managed": 2, "friend": 1}
    out: dict[str, dict[str, Any]] = {}
    for u in users:
        uid = str(u.get("id") or "")
        if not uid:
            continue
        cur = out.get(uid)
        if not cur or rank.get(u.get("type", "friend"), 0) >= rank.get(cur.get("type", "friend"), 0):
            out[uid] = u
    return list(out.values())


def _filter_users_with_server_access(
    cfg: dict[str, Any],
    users: list[dict[str, Any]],
    server_uuid: str,
) -> list[dict[str, Any]]:
    del cfg, server_uuid
    if not users:
        return users
    allowed = {"owner", "managed", "friend"}
    out: list[dict[str, Any]] = []
    for u in users:
        if u.get("type") in allowed:
            v = dict(u)
            v["has_access"] = True
            out.append(v)
    return out


def _list_pms_servers(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    acc = _account(cfg)
    if not acc:
        return []

    plex = cfg.get("plex") or {}
    host_hint = ""
    base = (plex.get("server_url") or "").strip()
    if base:
        try:
            host_hint = urllib.parse.urlparse(base).hostname or ""
        except Exception:
            host_hint = ""

    servers: list[dict[str, Any]] = []
    try:
        for r in acc.resources():
            if "server" not in (r.provides or "") or (r.product or "") != "Plex Media Server":
                continue

            conns: list[dict[str, Any]] = []
            for c in (r.connections or []):
                conns.append(
                    {
                        "uri": c.uri or "",
                        "address": c.address or "",
                        "port": c.port or "",
                        "protocol": c.protocol or "",
                        "local": bool(getattr(c, "local", False)),
                        "relay": bool(getattr(c, "relay", False)),
                    }
                )

            def pick_best() -> str:
                if host_hint:
                    for c in (r.connections or []):
                        if host_hint in (c.uri or "") or host_hint == (c.address or ""):
                            return c.uri or ""
                for c in (r.connections or []):
                    if (
                        not c.relay
                        and (c.protocol or "").lower() == "https"
                        and not getattr(c, "local", False)
                    ):
                        return c.uri or ""
                for c in (r.connections or []):
                    if not c.relay and (c.protocol or "").lower() == "https":
                        return c.uri or ""
                for c in (r.connections or []):
                    if not c.relay:
                        return c.uri or ""
                return (r.connections[0].uri if r.connections else "") or ""

            servers.append(
                {
                    "id": r.clientIdentifier or "",
                    "name": r.name or r.product or "Plex Media Server",
                    "owned": bool(getattr(r, "owned", False)),
                    "platform": r.platform or "",
                    "product": r.product or "",
                    "device": r.device or "",
                    "version": r.productVersion or "",
                    "connections": conns,
                    "best_url": pick_best(),
                }
            )
    except Exception:
        pass
    return servers


@router.get("/api/plex/server_uuid")
def api_plex_server_uuid(instance: str | None = Query(None)) -> JSONResponse:
    inst = normalize_instance_id(instance)
    cfg0 = load_config() or {}
    cfg = build_provider_config_view(cfg0, "plex", inst)
    uid = _resolve_plex_server_uuid(cfg)
    return JSONResponse(
        {"server_uuid": uid or None, "instance": inst},
        headers={"Cache-Control": "no-store"},
    )



@router.get("/api/plex/users")
def api_plex_users(
    only_with_server_access: bool = Query(False),
    only_home_or_owner: bool = Query(False),
    instance: str | None = Query(None),
) -> JSONResponse:
    inst = normalize_instance_id(instance)
    cfg0 = load_config() or {}
    cfg = build_provider_config_view(cfg0, "plex", inst)
    users = _list_plex_users(cfg)

    if only_with_server_access:
        server_uuid = _resolve_plex_server_uuid(cfg)
        users = _filter_users_with_server_access(cfg, users, server_uuid)

    if only_home_or_owner:
        users = [u for u in users if u.get("type") in {"owner", "managed"}]

    return JSONResponse(
        {"users": users, "count": len(users), "instance": inst},
        headers={"Cache-Control": "no-store"},
    )


@router.get("/api/plex/pms")
def api_plex_pms(instance: str | None = Query(None)) -> JSONResponse:
    inst = normalize_instance_id(instance)
    cfg0 = load_config() or {}
    cfg = build_provider_config_view(cfg0, "plex", inst)
    servers = _list_pms_servers(cfg)
    return JSONResponse(
        {"servers": servers, "count": len(servers), "instance": inst},
        headers={"Cache-Control": "no-store"},
    )



@router.get("/api/watch/currently_watching")
def api_currently_watching() -> JSONResponse:
    data: Any = None
    streams: list[dict[str, Any]] = []
    streams_count = 0
    try:
        path = _cw_state_file()
    except Exception:
        path = None

    if path is not None and path.exists():
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw) if raw.strip() else None
        except Exception as e:
            if BASE_LOG:
                try:
                    BASE_LOG(
                        f"currently_watching read failed: {e}",
                        level="ERROR",
                        module="SCROBBLE",
                    )
                except Exception:
                    pass

    # v2 state: { "v": 2, "streams": { "<key>": {payload}, ... } }
    if isinstance(data, dict) and int(data.get("v") or 0) == 2 and isinstance(data.get("streams"), dict):
        try:
            s = data.get("streams") or {}
            items: list[dict[str, Any]] = []
            for k, v in (s.items() if isinstance(s, dict) else []):
                if isinstance(v, dict):
                    vv = dict(v)
                    vv["_key"] = str(k)
                    items.append(vv)
            items.sort(key=lambda x: int(x.get("updated") or 0), reverse=True)
            streams = items


            def _is_active(st: Any) -> bool:
                v = str(st or "").lower()
                return v in ("playing", "paused", "buffering")

            active_items = [it for it in items if _is_active(it.get("state"))]
            streams_count = len(active_items)

            primary = active_items[0] if active_items else (items[0] if items else None)
            data = primary
        except Exception:
            pass

    if not streams_count:
        streams_count = len(streams) if streams else (1 if isinstance(data, dict) and data.get("title") else 0)

    payload: dict[str, Any] = {"ok": True, "currently_watching": data, "streams_count": streams_count, "ts": int(time.time())}
    if streams:
        payload["streams"] = streams

    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.get("/api/watch/logs")
def debug_watch_logs(
    request: Request,
    tail: int = Query(50, ge=1, le=3000),
    tag: str | None = Query(None, description="Single tag"),
    tags: str = Query("*", description="CSV or * for all"),
) -> JSONResponse:
    LOG_BUFFERS, max_lines = _env_logs(request)
    sel = [
        t.strip().upper()
        for t in ([tag] if tag else tags.split(","))
        if t and t.strip()
    ]
    if sel == ["*"]:
        sel = sorted(LOG_BUFFERS.keys())

    tail = max(1, min(int(tail or 50), int(max_lines)))
    merged: list[str] = []
    for t in sel:
        merged.extend(LOG_BUFFERS.get(t, []))

    return JSONResponse(
        {"tags": sel, "tail": tail, "lines": merged[-tail:]},
        headers={"Cache-Control": "no-store"},
    )


@router.get("/api/watch/status")
def debug_watch_status(request: Request) -> dict[str, Any]:
    wm: dict[str, Any] = {"groups": [], "routes": []}
    if HAVE_WATCH_MANAGER and callable(_wm_status):
        try:
            x = _wm_status(request.app)  # type: ignore[misc]
            if isinstance(x, dict):
                wm = x
        except Exception:
            wm = {"groups": [], "routes": []}

    legacy_w = getattr(request.app.state, "watch", None)
    try:
        legacy_alive = bool(getattr(legacy_w, "is_alive", lambda: False)()) if legacy_w else False
    except Exception:
        legacy_alive = False

    groups = wm.get("groups") or []
    routes = wm.get("routes") or []

    try:
        route_running = any(bool((r or {}).get("running")) for r in (routes if isinstance(routes, list) else []))
    except Exception:
        route_running = False

    alive = bool(route_running or legacy_alive)

    provider = ""
    if isinstance(groups, list) and len(groups) == 1:
        provider = str((groups[0] or {}).get("provider") or "").strip().lower()
    if not provider:
        provider = _watch_kind(legacy_w) or ("multi" if groups else "")

    sinks = sorted({str((r or {}).get("sink") or "").strip().lower() for r in (routes if isinstance(routes, list) else []) if (r or {}).get("sink")})

    out: dict[str, Any] = dict(wm) if isinstance(wm, dict) else {"groups": [], "routes": []}
    out.update(
        {
            "has_watch": bool(groups) or bool(legacy_w),
            "alive": bool(alive),
            "stop_set": False,
            "provider": provider or None,
            "sinks": sinks,
        }
    )
    if legacy_w and not groups:
        out["legacy"] = {"provider": _watch_kind(legacy_w), "alive": bool(legacy_alive)}
    return out


def _stop_legacy_watch(request: Request) -> bool:
    w = getattr(request.app.state, "watch", None)
    if not w:
        return True
    stopped = _stop_watch_blocking(w)
    if stopped:
        request.app.state.watch = None
        request.app.state.watch_meta = None
    return bool(stopped)





def _ensure_watch_started(
    request: Request,
    provider: str | None = None,
    sink: str | None = None,
) -> Any:
    w = getattr(request.app.state, "watch", None)
    meta = getattr(request.app.state, "watch_meta", None) or {}

    cfg = load_config() or {}
    scrobble = (cfg.get("scrobble") or {}) or {}
    watch_cfg = (scrobble.get("watch") or {}) or {}

    prov = (provider or watch_cfg.get("provider") or "plex").lower().strip()

    if sink is not None:
        sink_cfg = sink
    else:
        sink_cfg = (watch_cfg.get("sink") or "") if ("sink" in watch_cfg) else "trakt"
    if not str(sink_cfg).strip():
        raise HTTPException(status_code=400, detail="No sinks configured")

    names = [s.strip().lower() for s in re.split(r"[,&+]", str(sink_cfg)) if s and s.strip()]
    want_sinks = sorted(set(names or ["trakt"]))

    if w and getattr(w, "is_alive", lambda: False)():
        cur_prov = str(meta.get("provider") or _watch_kind(w) or prov).lower().strip()
        cur_sinks = sorted(set(meta.get("sinks") or []))

        if cur_prov == prov and cur_sinks == want_sinks:
            filters = dict((watch_cfg.get("filters") or {}) or {})
            try:
                if filters and hasattr(w, "set_filters"):
                    w.set_filters(filters)
            except Exception:
                pass
            return w

        if not _stop_watch_blocking(w):
            return w

        request.app.state.watch = None
        request.app.state.watch_meta = None
        w = None

    added: set[str] = set()
    sinks: list[Any] = []

    for name in want_sinks:
        if name == "trakt" and "trakt" not in added:
            from providers.scrobble.trakt.sink import TraktSink
            sinks.append(TraktSink())
            added.add("trakt")
        elif name == "simkl" and "simkl" not in added:
            from providers.scrobble.simkl.sink import SimklSink
            sinks.append(SimklSink())
            added.add("simkl")
        elif name == "mdblist" and "mdblist" not in added:
            from providers.scrobble.mdblist.sink import MDBListSink
            sinks.append(MDBListSink())
            added.add("mdblist")

    if not sinks:
        from providers.scrobble.trakt.sink import TraktSink
        sinks = [TraktSink()]
        added = {"trakt"}

    make_watch: Any | None = None
    if prov == "emby":
        try:
            from providers.scrobble.emby.watch import make_default_watch as _mk
            make_watch = _mk
        except Exception:
            make_watch = None

    elif prov == "jellyfin":
        try:
            from providers.scrobble.jellyfin.watch import make_default_watch as _mk
            make_watch = _mk
        except Exception:
            make_watch = None

    if make_watch is None:
        from providers.scrobble.plex.watch import make_default_watch as _mk
        make_watch = _mk
        prov = "plex"

    w = make_watch(sinks=sinks)

    filters = dict((watch_cfg.get("filters") or {}) or {})
    try:
        if filters and hasattr(w, "set_filters"):
            w.set_filters(filters)
    except Exception:
        pass

    if hasattr(w, "start_async"):
        w.start_async()
    else:
        threading.Thread(target=w.start, daemon=True).start()

    request.app.state.watch = w
    request.app.state.watch_meta = {"provider": prov, "sinks": sorted(added)}
    return w

@router.post("/api/watch/start")
def debug_watch_start(
    request: Request,
    provider: str | None = Query(None),
    sink: str | None = Query(None),
) -> dict[str, Any]:
    if callable(_reset_currently_watching):
        try:
            _reset_currently_watching()
        except Exception:
            pass

    _stop_legacy_watch(request)

    if HAVE_WATCH_MANAGER and callable(_wm_start_from_config):
        try:
            _wm_start_from_config(request.app)  # type: ignore[misc]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"WatchManager start failed: {e}")
        out = debug_watch_status(request)
        out["ok"] = True
        return out

    w = _ensure_watch_started(request, provider, sink)
    alive = bool(getattr(w, "is_alive", lambda: False)())
    meta = getattr(request.app.state, "watch_meta", None) or {}
    return {"ok": True, "alive": alive, "provider": _watch_kind(w), "sinks": meta.get("sinks") or []}

@router.post("/api/watch/stop")
def debug_watch_stop(request: Request) -> dict[str, Any]:
    stopped = True
    if HAVE_WATCH_MANAGER and callable(_wm_stop_all):
        try:
            _wm_stop_all(request.app)  # type: ignore[misc]
        except Exception:
            stopped = False

    legacy_stopped = _stop_legacy_watch(request)

    if callable(_reset_currently_watching):
        try:
            _reset_currently_watching()
        except Exception:
            pass

    out = debug_watch_status(request)
    out.update({"ok": True, "stopped": bool(stopped and legacy_stopped)})
    return out

@router.post("/webhook/jellyfintrakt")
async def webhook_jellyfintrakt(request: Request) -> JSONResponse:
    from crosswatch import _UIHostLogger

    try:
        from providers.webhooks.jellyfintrakt import process_webhook as jf_process_webhook
    except Exception:
        try:
            from crosswatch import process_webhook_jellyfin as jf_process_webhook
        except Exception:
            from jellyfintrakt import process_webhook as jf_process_webhook  # type: ignore[import]

    logger = _UIHostLogger("TRAKT", "SCROBBLE")

    def log(msg: str, level: str = "INFO") -> None:
        lvl_raw = str(level or "INFO")
        lvl_up = lvl_raw.upper()
        if lvl_up == "DEBUG" and not _debug_on():
            return
        try:
            logger(msg, level=lvl_raw, module="SCROBBLE")
        except Exception:
            pass
        try:
            if BASE_LOG:
                logr = BASE_LOG.child("SCROBBLE")
                if lvl_up == "DEBUG":
                    logr.debug(msg)
                elif lvl_up == "INFO":
                    logr.info(msg)
                elif lvl_up == "WARN":
                    logr.warn(msg)
                elif lvl_up == "ERROR":
                    logr.error(msg)
                else:
                    logr(msg, level=lvl_raw)
                return
        except Exception:
            pass
        try:
            print(f"[SCROBBLE] {lvl_up} {msg}")
        except Exception:
            pass

    raw = await request.body()
    ct = (request.headers.get("content-type") or "").lower()
    log(f"jf-webhook: received | content-type='{ct}' bytes={len(raw)}", "DEBUG")

    payload: dict[str, Any] = {}
    try:
        if "application/x-www-form-urlencoded" in ct:
            d = parse_qs(raw.decode("utf-8", errors="replace"))
            blob = d.get("payload") or d.get("data") or d.get("json")
            payload = (
                json.loads(
                    (blob[0] if isinstance(blob, list) else blob) or "{}"
                )
                if blob
                else {}
            )
            log("jf-webhook: parsed urlencoded payload", "DEBUG")
        else:
            payload = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
            log("jf-webhook: parsed json payload", "DEBUG")
    except Exception as e:
        snippet = (
            raw[:200].decode("utf-8", errors="replace") if raw else "<no body>"
        )
        log(
            f"jf-webhook: failed to parse payload: {e} | body[:200]={snippet}",
            "ERROR",
        )
        return JSONResponse({"ok": True}, status_code=200)

    md = (
        payload.get("Item")
        or payload.get("item")
        or payload.get("Metadata")
        or {}
    ) or {}
    event = (payload.get("NotificationType") or payload.get("Event") or "").strip() or "?"
    user = (
        ((payload.get("User") or {}).get("Name"))
        or payload.get("UserName")
        or ((payload.get("Server") or {}).get("UserName"))
        or ""
    ).strip()

    mtype = (md.get("Type") or md.get("type") or "").strip().lower()
    if mtype == "episode":
        series = (md.get("SeriesName") or md.get("SeriesTitle") or "").strip()
        ep_name = (md.get("Name") or md.get("EpisodeTitle") or "").strip()
        season = md.get("ParentIndexNumber") or md.get("SeasonIndexNumber")
        number = md.get("IndexNumber")
        if isinstance(season, int) and isinstance(number, int):
            title = f"{series} S{season:02}E{number:02}" + (
                f" — {ep_name}" if ep_name else ""
            )
        else:
            title = ep_name or series or "?"
    elif mtype == "movie":
        name = (md.get("Name") or md.get("title") or "").strip()
        year = md.get("ProductionYear") or md.get("year")
        title = f"{name} ({year})" if (name and year) else (name or "?")
    else:
        title = (
            md.get("Name")
            or md.get("title")
            or md.get("SeriesName")
            or "?"
        )

    log(
        f"jf-webhook: payload summary event='{event}' user='{user}' media='{title}'",
        "DEBUG",
    )

    try:
        res = jf_process_webhook(
            payload=payload,
            headers=dict(request.headers),
            raw=raw,
            logger=log,
        )
    except Exception as e:
        log(f"jf-webhook: process_webhook raised: {e}", "ERROR")
        return JSONResponse({"ok": True, "error": "internal"}, status_code=200)

    if res.get("error"):
        log(f"jf-webhook: result error={res['error']}", "WARN")
    elif res.get("ignored"):
        log("jf-webhook: ignored by filters/rules", "DEBUG")
    elif res.get("debounced"):
        log("jf-webhook: debounced pause", "DEBUG")
    elif res.get("suppressed"):
        log("jf-webhook: suppressed late start", "DEBUG")
    elif res.get("dedup"):
        log("jf-webhook: duplicate event suppressed", "DEBUG")

    log(
        f"jf-webhook: done action={res.get('action')} status={res.get('status')}",
        "DEBUG",
    )
    return JSONResponse(
        {"ok": True, **{k: v for k, v in res.items() if k != "error"}},
        status_code=200,
    )


@router.post("/webhook/embytrakt")
async def webhook_embytrakt(request: Request) -> JSONResponse:
    from crosswatch import _UIHostLogger

    try:
        from providers.webhooks.embytrakt import process_webhook as emby_process_webhook
    except Exception:
        try:
            from crosswatch import process_webhook_emby as emby_process_webhook  # type: ignore[attr-defined]
        except Exception:
            from embytrakt import process_webhook as emby_process_webhook  # type: ignore[import]

    logger = _UIHostLogger("TRAKT", "SCROBBLE")

    def log(msg: str, level: str = "INFO") -> None:
        lvl_raw = str(level or "INFO")
        lvl_up = lvl_raw.upper()
        if lvl_up == "DEBUG" and not _debug_on():
            return
        try:
            logger(msg, level=lvl_raw, module="SCROBBLE")
        except Exception:
            pass
        try:
            if BASE_LOG:
                logr = BASE_LOG.child("SCROBBLE")
                if lvl_up == "DEBUG":
                    logr.debug(msg)
                elif lvl_up == "INFO":
                    logr.info(msg)
                elif lvl_up == "WARN":
                    logr.warn(msg)
                elif lvl_up == "ERROR":
                    logr.error(msg)
                else:
                    logr(msg, level=lvl_raw)
                return
        except Exception:
            pass
        try:
            print(f"[SCROBBLE] {lvl_up} {msg}")
        except Exception:
            pass

    raw = await request.body()
    ct = (request.headers.get("content-type") or "").lower()
    log(f"emby-webhook: received | content-type='{ct}' bytes={len(raw)}", "DEBUG")

    payload: dict[str, Any] = {}
    try:
        if "application/x-www-form-urlencoded" in ct:
            d = parse_qs(raw.decode("utf-8", errors="replace"))
            blob = d.get("payload") or d.get("data") or d.get("json")
            payload = (
                json.loads(
                    (blob[0] if isinstance(blob, list) else blob) or "{}"
                )
                if blob
                else {}
            )
            log("emby-webhook: parsed urlencoded payload", "DEBUG")
        else:
            payload = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
            log("emby-webhook: parsed json payload", "DEBUG")
    except Exception as e:
        snippet = (
            raw[:200].decode("utf-8", errors="replace") if raw else "<no body>"
        )
        log(
            f"emby-webhook: failed to parse payload: {e} | body[:200]={snippet}",
            "ERROR",
        )
        return JSONResponse({"ok": True}, status_code=200)

    md = (
        payload.get("Item")
        or payload.get("item")
        or payload.get("Metadata")
        or {}
    ) or {}
    event = (payload.get("NotificationType") or payload.get("Event") or "").strip() or "?"
    user = (
        ((payload.get("User") or {}).get("Name"))
        or payload.get("UserName")
        or ((payload.get("Server") or {}).get("UserName"))
        or ""
    ).strip()

    mtype = (md.get("Type") or md.get("type") or "").strip().lower()
    if mtype == "episode":
        series = (md.get("SeriesName") or md.get("SeriesTitle") or "").strip()
        ep_name = (md.get("Name") or md.get("EpisodeTitle") or "").strip()
        season = md.get("ParentIndexNumber") or md.get("SeasonIndexNumber")
        number = md.get("IndexNumber")
        if isinstance(season, int) and isinstance(number, int):
            title = f"{series} S{season:02}E{number:02}" + (
                f" — {ep_name}" if ep_name else ""
            )
        else:
            title = ep_name or series or "?"
    elif mtype == "movie":
        name = (md.get("Name") or md.get("title") or "").strip()
        year = md.get("ProductionYear") or md.get("year")
        title = f"{name} ({year})" if (name and year) else (name or "?")
    else:
        title = (
            md.get("Name")
            or md.get("title")
            or md.get("SeriesName")
            or "?"
        )

    log(
        f"emby-webhook: payload summary event='{event}' user='{user}' media='{title}'",
        "DEBUG",
    )

    try:
        res = emby_process_webhook(
            payload=payload,
            headers=dict(request.headers),
            raw=raw,
            logger=log,
        )
    except Exception as e:
        log(f"emby-webhook: process_webhook raised: {e}", "ERROR")
        return JSONResponse({"ok": True, "error": "internal"}, status_code=200)

    if res.get("error"):
        log(f"emby-webhook: result error={res['error']}", "WARN")
    elif res.get("ignored"):
        log("emby-webhook: ignored by filters/rules", "DEBUG")
    elif res.get("debounced"):
        log("emby-webhook: debounced pause", "DEBUG")
    elif res.get("suppressed"):
        log("emby-webhook: suppressed late start", "DEBUG")
    elif res.get("dedup"):
        log("emby-webhook: duplicate event suppressed", "DEBUG")

    log(
        f"emby-webhook: done action={res.get('action')} status={res.get('status')}",
        "DEBUG",
    )
    return JSONResponse(
        {"ok": True, **{k: v for k, v in res.items() if k != "error"}},
        status_code=200,
    )


@router.post("/webhook/plextrakt")
async def webhook_trakt(request: Request) -> JSONResponse:
    from crosswatch import _UIHostLogger

    try:
        from providers.scrobble.trakt.webhook import process_webhook  # type: ignore[import]
    except Exception:
        from crosswatch import process_webhook

    logger = _UIHostLogger("TRAKT", "SCROBBLE")

    def log(msg: str, level: str = "INFO") -> None:
        lvl_raw = str(level or "INFO")
        lvl_up = lvl_raw.upper()
        if lvl_up == "DEBUG" and not _debug_on():
            return
        try:
            logger(msg, level=lvl_raw, module="SCROBBLE")
        except Exception:
            pass
        try:
            if BASE_LOG:
                logr = BASE_LOG.child("SCROBBLE")
                if lvl_up == "DEBUG":
                    logr.debug(msg)
                elif lvl_up == "INFO":
                    logr.info(msg)
                elif lvl_up == "WARN":
                    logr.warn(msg)
                elif lvl_up == "ERROR":
                    logr.error(msg)
                else:
                    logr(msg, level=lvl_raw)
                return
        except Exception:
            pass
        try:
            print(f"[SCROBBLE] {lvl_up} {msg}")
        except Exception:
            pass

    raw = await request.body()
    ct = (request.headers.get("content-type") or "").lower()
    log(f"plex-webhook: received | content-type='{ct}' bytes={len(raw)}", "DEBUG")

    payload: dict[str, Any] | None = None
    try:
        if "multipart/form-data" in ct:
            form = await request.form()
            part = form.get("payload")
            if part is None:
                raise ValueError("multipart: no 'payload' part")
            if isinstance(part, (bytes, bytearray)):
                payload = json.loads(part.decode("utf-8", errors="replace"))
            elif hasattr(part, "read"):
                data = await part.read()  # type: ignore[attr-defined]
                payload = json.loads(data.decode("utf-8", errors="replace"))
            else:
                payload = json.loads(str(part))
            log("plex-webhook: parsed multipart payload", "DEBUG")
        elif "application/x-www-form-urlencoded" in ct:
            d = parse_qs(raw.decode("utf-8", errors="replace"))
            if not d.get("payload"):
                raise ValueError("urlencoded: no 'payload' key")
            payload = json.loads(d["payload"][0])
            log("plex-webhook: parsed urlencoded payload", "DEBUG")
        else:
            payload = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
            log("plex-webhook: parsed json payload", "DEBUG")
    except Exception as e:
        snippet = (
            raw[:200].decode("utf-8", errors="replace") if raw else "<no body>"
        )
        log(
            f"plex-webhook: failed to parse payload: {e} | body[:200]={snippet}",
            "ERROR",
        )
        return JSONResponse({"ok": True}, status_code=200)

    payload = payload or {}
    acc = ((payload.get("Account") or {}).get("title") or "").strip()
    srv = ((payload.get("Server") or {}).get("uuid") or "").strip()
    md = payload.get("Metadata") or {}
    title = md.get("title") or md.get("grandparentTitle") or "?"
    log(
        f"plex-webhook: payload summary user='{acc}' server='{srv}' media='{title}'",
        "DEBUG",
    )

    try:
        res = process_webhook(
            payload=payload,
            headers=dict(request.headers),
            raw=raw,
            logger=log,
        )
    except Exception as e:
        log(f"webhook: process_webhook raised: {e}", "ERROR")
        return JSONResponse({"ok": True, "error": "internal"}, status_code=200)

    if res.get("error"):
        log(f"plex-webhook: result error={res['error']}", "WARN")
    elif res.get("ignored"):
        log("plex-webhook: ignored by filters/rules", "DEBUG")
    elif res.get("debounced"):
        log("plex-webhook: debounced pause", "DEBUG")
    elif res.get("suppressed"):
        log("plex-webhook: suppressed late start", "DEBUG")
    elif res.get("dedup"):
        log("plex-webhook: duplicate event suppressed", "DEBUG")

    log(
        f"plex-webhook: done action={res.get('action')} status={res.get('status')}",
        "DEBUG",
    )
    return JSONResponse(
        {"ok": True, **{k: v for k, v in res.items() if k != "error"}},
        status_code=200,
    )

@router.post("/webhook/plexwatcher")
async def webhook_plexwatcher(request: Request) -> JSONResponse:
    from crosswatch import _UIHostLogger

    logger = _UIHostLogger("PLEX-WATCHER", "SCROBBLE")

    def log(msg: str, level: str = "INFO") -> None:
        lvl_raw = str(level or "INFO")
        lvl_up = lvl_raw.upper()
        if lvl_up == "DEBUG" and not _debug_on():
            return
        try:
            logger(msg, level=lvl_raw, module="SCROBBLE")
        except Exception:
            pass
        try:
            if BASE_LOG:
                logr = BASE_LOG.child("SCROBBLE")
                if lvl_up == "DEBUG":
                    logr.debug(msg)
                elif lvl_up == "INFO":
                    logr.info(msg)
                elif lvl_up == "WARN":
                    (logr.warning if hasattr(logr, "warning") else logr.warn)(msg)
                elif lvl_up == "ERROR":
                    logr.error(msg)
                else:
                    logr(msg, level=lvl_raw)
        except Exception:
            pass

    try:
        from providers.scrobble.plex.watch import process_rating_webhook as pxw_process
    except Exception as e:
        log(f"/webhook/plexwatcher: missing plex watch module: {e}", "ERROR")
        return JSONResponse({"ok": True, "ignored": True}, status_code=200)

    raw = await request.body()
    ct = (request.headers.get("content-type") or "").lower()

    payload: dict[str, Any] = {}
    try:
        if "multipart/form-data" in ct:
            form = await request.form()
            part = form.get("payload")
            if part is None:
                payload = {}
            elif isinstance(part, (bytes, bytearray)):
                payload = json.loads(part.decode("utf-8", errors="replace"))
            elif hasattr(part, "read"):
                data = await part.read()  # type: ignore[attr-defined]
                payload = json.loads(data.decode("utf-8", errors="replace"))
            else:
                payload = json.loads(str(part))
        elif "application/x-www-form-urlencoded" in ct:
            qs = parse_qs(raw.decode("utf-8", errors="replace"))
            payload = json.loads((qs.get("payload") or ["{}"])[0])
        else:
            payload = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
    except Exception as e:
        log(f"plexwatcher-webhook: failed to parse payload: {e}", "ERROR")
        payload = {}

    if not isinstance(payload, dict):
        payload = {}

    event = str(payload.get("event") or "")
    if event and event != "media.rate":
        return JSONResponse({"ok": True, "ignored": True}, status_code=200)

    res = pxw_process(payload, dict(request.headers), raw=raw, logger=log)

    if res.get("invalid_signature"):
        log("plexwatcher-webhook: invalid signature", "WARN")
    elif res.get("ignored"):
        log("plexwatcher-webhook: ignored", "DEBUG")
    elif res.get("dedup"):
        log("plexwatcher-webhook: duplicate rating suppressed", "DEBUG")
    elif res.get("trakt") and isinstance(res.get("trakt"), dict) and not res["trakt"].get("ok"):
        log(f"plexwatcher-webhook: trakt failure status={res['trakt'].get('status')}", "WARN")
    elif res.get("simkl") and isinstance(res.get("simkl"), dict) and not res["simkl"].get("ok"):
        log(f"plexwatcher-webhook: simkl failure status={res['simkl'].get('status')}", "WARN")
    else:
        log(
            f"plexwatcher-webhook: processed media_type={res.get('media_type')} rating={res.get('rating')}",
            "INFO",
        )

    return JSONResponse(
        {"ok": True, **{k: v for k, v in res.items() if k != "error"}},
        status_code=200,
    )
    