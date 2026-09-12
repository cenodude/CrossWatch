from __future__ import annotations

import hashlib
import json
import threading
import time
from typing import Any, Callable

import requests

from cw_platform.local_db.db import get_conn

_LOCK = threading.RLock()
_PLANS: dict[str, tuple[float, str, str, str, int]] = {}
_TTL = 48 * 3600
_PENDING = "simkl_scrobble_rewatch_pending"
_DONE = "simkl_scrobble_rewatch_done"


def enabled(cfg: dict[str, Any]) -> bool:
    watch = (cfg.get("scrobble") or {}).get("watch") or {}
    options = (watch.get("route_options") or {}).get("watch") or {}
    return options.get("simkl_rewatches") is True


def account_key(cfg: dict[str, Any]) -> str:
    block = cfg.get("simkl") or {}
    return hashlib.sha256(str(block.get("access_token") or "").encode()).hexdigest()


def account(cfg: dict[str, Any], post: Callable[..., Any], diagnostic: Callable[..., None] | None = None) -> tuple[str, str]:
    key = account_key(cfg)
    with _LOCK:
        now = time.time()
        cached = _PLANS.get(key)
        if cached and cached[0] > now:
            if diagnostic:
                diagnostic("plan", cache="hit", plan=cached[1], reason=cached[3], http_status=cached[4], expires_in_s=round(cached[0] - now))
            return cached[1], cached[2]
        started = time.monotonic()
        status = 0
        reason = "verified"
        try:
            response = post("/users/settings", {}, cfg)
            status = response.status_code
            response.raise_for_status()
            settings = response.json()
            plan = str(settings["account"]["type"]).lower()
            identity = str(settings["account"].get("id") or key)
            if plan not in {"free", "pro", "vip"}:
                plan = "unknown"
                reason = "invalid_plan"
        except requests.Timeout:
            plan, identity, reason = "unknown", key, "timeout"
        except requests.ConnectionError:
            plan, identity, reason = "unknown", key, "connection_error"
        except requests.HTTPError:
            plan, identity, reason = "unknown", key, "http_error"
        except (ValueError, KeyError, TypeError, AttributeError):
            plan, identity, reason = "unknown", key, "invalid_response"
        except Exception:
            plan, identity, reason = "unknown", key, "request_error"
        _PLANS[key] = (now + (60 if plan == "unknown" else 300), plan, identity, reason, status)
        if diagnostic:
            diagnostic("plan", cache="miss", plan=plan, reason=reason, http_status=status, elapsed_ms=round((time.monotonic() - started) * 1000))
        for old in list(_PLANS):
            if _PLANS[old][0] <= now:
                del _PLANS[old]
        return plan, identity


def downgrade(cfg: dict[str, Any]) -> None:
    key = account_key(cfg)
    with _LOCK:
        identity = _PLANS.get(key, (0, "", key, "", 0))[2]
        _PLANS[key] = (time.time() + 300, "free", identity, "pro_required", 0)


def completion_key(cfg: dict[str, Any], ev: Any, identity: str, media: str) -> str | None:
    session = getattr(ev, "session_key", None) or getattr(ev, "session", None)
    if not session or str(session) == "?":
        return None
    watch = (cfg.get("scrobble") or {}).get("watch") or {}
    parts = [identity, watch.get("route_provider"), watch.get("route_provider_instance", "default"),
             watch.get("route_effective_profile_id"), getattr(ev, "server_uuid", None),
             str(getattr(ev, "account", None) or "").casefold(), media, str(session)]
    return hashlib.sha256(json.dumps(parts, sort_keys=True).encode()).hexdigest()


def claim(key: str) -> str:
    conn = get_conn()
    if conn is None:
        raise RuntimeError("rewatch_dedupe_unavailable")
    now = time.time()
    with conn:
        conn.execute("DELETE FROM ttl_dedupe_entries WHERE namespace IN (?,?) AND expires_at<=?", (_PENDING, _DONE, now))
        row = conn.execute("SELECT namespace FROM ttl_dedupe_entries WHERE namespace IN (?,?) AND dedupe_key=?", (_PENDING, _DONE, key)).fetchone()
        if row:
            return "done" if row["namespace"] == _DONE else "uncertain"
        conn.execute("INSERT INTO ttl_dedupe_entries(namespace,dedupe_key,seen_at,expires_at,updated_at) VALUES(?,?,?,?,?)",
                     (_PENDING, key, now, now + _TTL, time.time_ns()))
    return "new"


def finish(key: str, *, release: bool = False) -> None:
    conn = get_conn()
    if conn is None:
        raise RuntimeError("rewatch_dedupe_unavailable")
    with conn:
        if release:
            conn.execute("DELETE FROM ttl_dedupe_entries WHERE namespace=? AND dedupe_key=?", (_PENDING, key))
        else:
            conn.execute("UPDATE ttl_dedupe_entries SET namespace=? WHERE namespace=? AND dedupe_key=?", (_DONE, _PENDING, key))
