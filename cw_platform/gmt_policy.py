# cw_platform/gmt_policy.py
# GMT Policy Management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)from __future__ import annotations
from dataclasses import dataclass
from collections.abc import Mapping
from typing import Any, Literal, cast

# Types
ScopeList = Literal["watchlist", "ratings", "history", "playlists"]
ScopeDim = Literal["add", "remove", "rate", "unrate", "scrobble", "unscrobble"]


@dataclass(frozen=True)
class Scope:
    list: ScopeList
    dim: ScopeDim


# Operation
OPPOSITE: dict[str, str] = {
    "add": "remove",
    "remove": "add",
    "rate": "unrate",
    "unrate": "rate",
    "scrobble": "unscrobble",
    "unscrobble": "scrobble",
}

NEGATIVE_OPS: set[str] = {"remove", "unrate", "unscrobble"}


def opposing(op: str) -> str:
    return OPPOSITE.get((op or "").lower(), (op or "").lower())


def is_negative(op: str) -> bool:
    return (op or "").lower() in NEGATIVE_OPS


try:
    from cw_platform.id_map import canonical_key  # type: ignore
except Exception:  # pragma: no cover
    _ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid", "slug")

    def canonical_key(item: Mapping[str, Any]) -> str:
        ids = (item.get("ids") or {})
        for k in _ID_KEYS:
            v = ids.get(k)
            if v:
                return f"{k}:{str(v).lower()}"
        t = (item.get("type") or "").lower()
        ttl = str(item.get("title") or "").strip().lower()
        yr = item.get("year") or ""
        return f"{t}|title:{ttl}|year:{yr}"


# TTL
def _read(cfg: Mapping[str, Any] | None, *path: str, default: Any = None) -> Any:
    cur: Any = cfg or {}
    for key in path:
        if not isinstance(cur, Mapping):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def get_quarantine_ttl_sec(cfg: Mapping[str, Any] | None, feature: str) -> int:
    feat = (feature or "").lower()
    defaults_days: dict[str, int] = {
        "watchlist": 7,
        "ratings": 3,
        "history": 2,
        "playlists": 7,
    }
    # Per-feature seconds override
    sec_override = _read(cfg, "sync", "gmt", f"{feat}_sec", default=None)
    if isinstance(sec_override, (int, float)) and int(
        cast(int | float, sec_override)
    ) > 0:
        return int(cast(int | float, sec_override))

    # Per-feature days override
    days_override = _read(cfg, "sync", "gmt", f"{feat}_days", default=None)
    if isinstance(days_override, (int, float)) and int(
        cast(int | float, days_override)
    ) > 0:
        return int(cast(int | float, days_override)) * 24 * 3600

    # Global days override
    global_days = _read(cfg, "sync", "gmt_quarantine_days", default=None)
    if isinstance(global_days, (int, float)) and int(
        cast(int | float, global_days)
    ) > 0:
        return int(cast(int | float, global_days)) * 24 * 3600

    # Defaults
    return int(defaults_days.get(feat, 7)) * 24 * 3600


# Normalization
def negative_event_key(feature: str, op: str) -> str:
    feat = (feature or "").lower()
    _ = (op or "").lower()
    if feat == "ratings":
        return "unrate"
    if feat == "history":
        return "unscrobble"
    return "remove"


# Suppressed
def _coerce_scope(scope: Scope | Mapping[str, Any]) -> tuple[str, str]:
    if isinstance(scope, Scope):
        return scope.list, scope.dim
    if isinstance(scope, Mapping):
        return (
            str(scope.get("list", "")).lower(),
            str(scope.get("dim", "")).lower(),
        )
    raise TypeError("scope must be Scope or Mapping")


def _cfg_from_store(store: Any) -> Mapping[str, Any] | None:
    for attr in ("cfg", "config", "get_config"):
        try:
            v = getattr(store, attr, None)
            if callable(v):
                return cast(Mapping[str, Any], v())
            if isinstance(v, dict):
                return cast(Mapping[str, Any], v)
        except Exception:
            pass
    return None


def should_suppress_write(
    *,
    store: Any,
    entity: Mapping[str, Any],
    scope: Scope | Mapping[str, Any],
    pair_id: str | None = None,
    ttl_sec: int | None = None,
) -> bool:
    try:
        feat, write_dim = _coerce_scope(scope)
        key = canonical_key(entity)

        # TTL resolution
        cfg = _cfg_from_store(store)
        ttl = int(ttl_sec or get_quarantine_ttl_sec(cfg, feat))
        want_dim = opposing(write_dim)

        pred = getattr(store, "should_suppress_by_key", None)
        if callable(pred):
            return bool(
                pred(
                    key=key,
                    list=feat,
                    dim=want_dim,
                    ttl_sec=ttl,
                    pair_id=pair_id,
                )
            )

        last_ts = getattr(store, "last_negative_ts", None)
        if callable(last_ts):
            ts = last_ts(key=key, list=feat, dim=want_dim, pair_id=pair_id)
            if ts is not None:
                import time as _t

                val = cast(int | float, ts)
                return (_t.time() - int(val)) < ttl

        getrec = getattr(store, "get", None)
        if callable(getrec):
            rec = getrec(key=key, list=feat, dim=want_dim, pair_id=pair_id)
            if isinstance(rec, Mapping) and "ts" in rec:
                import time as _t

                rec_ts = cast(int | float, rec.get("ts", 0))
                return (_t.time() - int(rec_ts)) < ttl

    except Exception:
        return False

    return False
