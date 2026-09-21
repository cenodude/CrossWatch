# providers/sync/_mod_TRACEARR.py
# CrossWatch - Tracearr sync module (history only)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.app_version import user_agent as http_user_agent
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any
import os
import threading
import time

from ._log import log as cw_log
from ._mod_common import SimpleRateLimiter, build_session, request_with_retries, safe_json

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "0.1"
os.environ.setdefault("CW_TRACEARR_UA", http_user_agent("Tracearr", override_env="CW_TRACEARR_UA"))
__all__ = ["get_manifest", "TRACEARRModule", "OPS"]

API_PREFIX = "/api/v2/public"
_RATE_PER_SEC = 3.0
_SHOW_IDS_CACHE_MAX = 5000
_SHOW_IDS_CACHE: dict[tuple[str, str], dict[str, str]] = {}
_SHOW_IDS_LOCK = threading.Lock()
_LIMITER = SimpleRateLimiter(rates_per_sec={"TRACEARR": _RATE_PER_SEC})

_HISTORY_CAPABILITIES: dict[str, Any] = {
    "read": True,
    "write": False,
    "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
    "index_semantics": "event",
    "observed_deletes": False,
    "event_history": True,
    "rewatches": {"read": True, "write": False, "account_gate": False},
}


def _health(status: str, ok: bool, latency_ms: int) -> None:
    cw_log("TRACEARR", "health", "info", "health", latency_ms=latency_ms, ok=ok, status=status)


def _log(level: str, msg: str, **fields: Any) -> None:
    cw_log("TRACEARR", "module", level, msg, **fields)


def _dbg(msg: str, **fields: Any) -> None:
    _log("debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    _log("info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    _log("warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    _log("error", msg, **fields)


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "TRACEARR",
        "label": "Tracearr",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": False,
        "experimental": True,
        "features": {"watchlist": False, "ratings": False, "history": True, "playlists": False},
        "requires": [],
        "capabilities": {
            "bidirectional": False,
            "provides_ids": True,
            "index_semantics": "present",
            "can_source": True,
            "can_target": False,
            "read_only": True,
            "history": _HISTORY_CAPABILITIES,
        },
        "description": "Plex, Jellyfin and Emby monitoring (history only).",
    }


def _label(method: str, url: str, kw: Mapping[str, Any]) -> str:
    try:
        path = str(url).split(API_PREFIX, 1)[1].strip("/")
        head = path.split("/", 1)[0]
        return f"api:{head}" if head else "api"
    except Exception:
        return "api"


def _as_base(url: Any) -> str | None:
    s = str(url or "").strip()
    if not s:
        return None
    if not s.startswith(("http://", "https://")):
        s = "http://" + s
    return s.rstrip("/")


@dataclass(frozen=True)
class TRACEARRConfig:
    server_url: str
    api_key: str
    verify_ssl: bool = True
    timeout: float = 10.0
    max_retries: int = 3


@dataclass(frozen=True)
class _HistoryAdapter:
    cfg: Mapping[str, Any]
    client: Any


class TRACEARRClient:
    def __init__(self, cfg: TRACEARRConfig, raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self._show_misses: set[str] = set()
        self.session = build_session("TRACEARR", ctx, feature_label=_label)
        try:
            self.session.headers["User-Agent"] = os.environ.get("CW_TRACEARR_UA") or http_user_agent("Tracearr", override_env="CW_TRACEARR_UA")
            self.session.headers.setdefault("Accept", "application/json")
            self.session.headers["Authorization"] = f"Bearer {cfg.api_key}"
        except Exception:
            pass

    def _url(self, path: str) -> str:
        return f"{self.cfg.server_url}{API_PREFIX}/{str(path or '').lstrip('/')}"

    def get(self, path: str, **params: Any) -> Any:
        q = {k: v for k, v in (params or {}).items() if v is not None}
        _LIMITER.wait("TRACEARR")
        r = request_with_retries(
            self.session,
            "GET",
            self._url(path),
            params=q,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            verify=self.cfg.verify_ssl,
        )
        if r.status_code in (401, 403):
            _warn("http_failed", op=path, status=r.status_code, reason="invalid_api_key")
            raise RuntimeError("invalid_api_key")
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            _warn("http_failed", op=path, status=r.status_code)
            raise RuntimeError(f"HTTP {r.status_code}")
        return safe_json(r)

    def show_ids(self, show_media_id: Any) -> dict[str, str]:
        sid = str(show_media_id or "").strip()
        if not sid:
            return {}
        ck = (self.cfg.server_url, sid)
        with _SHOW_IDS_LOCK:
            hit = _SHOW_IDS_CACHE.get(ck)
        if hit is not None:
            return dict(hit)
        if sid in self._show_misses:
            return {}
        try:
            data = self.get(f"/media/{sid}")
        except Exception as e:
            _dbg("show_lookup_failed", show_media_id=sid, error=str(e))
            self._show_misses.add(sid)
            return {}
        ids: dict[str, str] = {}
        if isinstance(data, Mapping):
            imdb = str(data.get("imdb_id") or "").strip()
            if imdb.lower().startswith("tt"):
                ids["imdb"] = imdb
            for key in ("tmdb", "tvdb"):
                v = data.get(f"{key}_id")
                if isinstance(v, int) and not isinstance(v, bool) and v > 0:
                    ids[key] = str(v)
        if not ids:
            self._show_misses.add(sid)
            return {}
        with _SHOW_IDS_LOCK:
            if len(_SHOW_IDS_CACHE) >= _SHOW_IDS_CACHE_MAX:
                _SHOW_IDS_CACHE.pop(next(iter(_SHOW_IDS_CACHE)))
            _SHOW_IDS_CACHE[ck] = ids
        return dict(ids)


class TRACEARRModule:
    def __init__(self, cfg: Mapping[str, Any]):
        t = dict(cfg.get("tracearr") or {})
        base = _as_base(t.get("server_url") or t.get("server"))
        key = str(t.get("api_key") or "").strip()
        if not base or not key:
            _error("missing config", has_server=bool(base), has_key=bool(key))
            raise RuntimeError("Missing tracearr.server_url or tracearr.api_key")

        self.cfg = TRACEARRConfig(
            server_url=base,
            api_key=key,
            verify_ssl=bool(t.get("verify_ssl", True)),
            timeout=float(t.get("timeout", cfg.get("timeout", 10.0))),
            max_retries=int(t.get("max_retries", cfg.get("max_retries", 3))),
        )
        self.client = TRACEARRClient(self.cfg, cfg)

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return {"watchlist": False, "ratings": False, "history": True, "playlists": False}

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        start = time.perf_counter()
        try:
            data = self.client.get("/users", pageSize=1)
            if not isinstance(data, Mapping) or not isinstance(data.get("data"), list):
                raise RuntimeError("invalid response")
            latency_ms = int((time.perf_counter() - start) * 1000)
            _health("ok", True, latency_ms)
            return {"ok": True, "status": "ok", "latency_ms": latency_ms}
        except Exception as e:
            latency_ms = int((time.perf_counter() - start) * 1000)
            _health("down", False, latency_ms)
            return {"ok": False, "status": "down", "latency_ms": latency_ms, "reason": str(e)}

    def activities(self) -> Mapping[str, Any]:
        try:
            data = self.client.get("/history", pageSize=1) or {}
            rows = data.get("data") if isinstance(data, Mapping) else None
            ts: Any = None
            if isinstance(rows, list) and rows and isinstance(rows[0], Mapping):
                ts = rows[0].get("stopped_at") or rows[0].get("started_at")
            return {"history": str(ts or "0"), "updated_at": str(ts or "0")}
        except Exception as e:
            _dbg("activities_failed", error=str(e))
            return {"updated_at": "0"}

    def build_index(self, feature: str) -> Mapping[str, dict[str, Any]]:
        if feature != "history":
            _info("index_skipped", feature=feature, reason="disabled_or_missing")
            return {}
        from .tracearr import _history

        adapter = _HistoryAdapter(cfg=self.client.raw_cfg, client=self.client)
        return _history.build_index(adapter)

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        if feature != "history":
            return {"ok": True, "count": 0, "unresolved": [], "reason": "disabled_or_missing"}
        from .tracearr import _history

        return _history.add(self, items, dry_run=dry_run)

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        if feature != "history":
            return {"ok": True, "count": 0, "unresolved": [], "reason": "disabled_or_missing"}
        from .tracearr import _history

        return _history.remove(self, items, dry_run=dry_run)


class _TRACEARROPS:
    def name(self) -> str:
        return "TRACEARR"

    def label(self) -> str:
        return "Tracearr"

    def features(self) -> Mapping[str, bool]:
        return TRACEARRModule.supported_features()

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": False,
            "provides_ids": True,
            "index_semantics": "present",
            "can_source": True,
            "can_target": False,
            "read_only": True,
            "history": _HISTORY_CAPABILITIES,
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        t = cfg.get("tracearr") or {}
        return bool(_as_base(t.get("server_url") or t.get("server")) and str(t.get("api_key") or "").strip())

    def _adapter(self, cfg: Mapping[str, Any]) -> TRACEARRModule:
        return TRACEARRModule(cfg)

    def activities(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).activities()

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)


OPS = _TRACEARROPS()
