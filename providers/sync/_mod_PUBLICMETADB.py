# providers/sync/_mod_PUBLICMETADB.py
# CrossWatch PublicMetaDB sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from urllib.parse import urljoin

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._log import log as cw_log
from ._mod_common import HitSession, SimpleRateLimiter, build_session, parse_rate_limit, request_with_retries, safe_json
from .publicmetadb import _history as feat_history
from .publicmetadb import _progress as feat_progress
from .publicmetadb import _ratings as feat_ratings
from .publicmetadb import _watchlist as feat_watchlist
from .publicmetadb._common import enrich_index_metadata

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "0.3"
__all__ = ["get_manifest", "PUBLICMETADBModule", "OPS"]


def _label_publicmetadb(method: str, url: str, kw: Mapping[str, Any]) -> str:
    if "/api/external/lists" in str(url):
        return "watchlist"
    if "/api/external/resume" in str(url):
        return "progress"
    if "/api/external/watched" in str(url):
        return "history"
    if "/api/external/ratings" in str(url) or "/api/external/episode-ratings" in str(url):
        return "ratings"
    return "external"


def _confirmed_keys(items: Iterable[Mapping[str, Any]], unresolved: Any) -> list[str]:
    attempted = [canonical_key(id_minimal(it)) for it in items or [] if isinstance(it, Mapping)]
    unresolved_keys: set[str] = set()
    for u in unresolved or []:
        obj = u.get("item") if isinstance(u, Mapping) else u
        if isinstance(obj, Mapping):
            unresolved_keys.add(canonical_key(id_minimal(obj)))
    out: list[str] = []
    seen: set[str] = set()
    for k in attempted:
        if not k or k in unresolved_keys or k in seen:
            continue
        out.append(k)
        seen.add(k)
    return out


def _features_flags() -> dict[str, bool]:
    return {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "PUBLICMETADB",
        "label": "PublicMetaDB",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": _features_flags(),
        "requires": [],
        "capabilities": {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "watchlist": {
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": True,
                "remove": True,
                "requires_ids": ["tmdb"],
            },
            "history": {
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "requires_ids": ["tmdb"],
            },
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": False,
                "requires_ids": ["tmdb"],
                "scale": "1-10",
            },
            "progress": {
                "types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                "upsert": True,
                "remove": True,
                "observed_deletes": True,
                "requires_ids": ["tmdb"],
                "requires_duration": True,
                "server_completion_percent": 80,
            },
        },
    }


@dataclass
class PUBLICMETADBConfig:
    api_key: str
    base_url: str = "https://publicmetadb.com"
    timeout: float = 15.0
    max_retries: int = 3
    watchlist_name: str = "Watchlist"
    rate_get_per_sec: float = 20.0
    rate_post_per_sec: float = 3.0
    watchlist_page_size: int = 100
    history_per_page: int = 100
    history_max_pages: int = 1000
    progress_per_page: int = 100
    progress_max_pages: int = 1000
    ratings_label: str = "Overall"
    ratings_submit_per_hour: int = 200
    ratings_update_per_hour: int = 100


class PUBLICMETADBError(RuntimeError):
    pass


class PUBLICMETADBAuthError(PUBLICMETADBError):
    pass


class PUBLICMETADBClient:
    def __init__(self, cfg: PUBLICMETADBConfig, raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self.session: HitSession = build_session("PUBLICMETADB", ctx, feature_label=_label_publicmetadb)
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {cfg.api_key}",
                "User-Agent": f"CrossWatch PublicMetaDB/{__VERSION__}",
            }
        )
        self.session._rate_limiter = SimpleRateLimiter(
            rates_per_sec={"GET": cfg.rate_get_per_sec, "POST": cfg.rate_post_per_sec, "DELETE": cfg.rate_post_per_sec}
        )
        self.session._rate_limiter_meta = {
            "get_per_sec": cfg.rate_get_per_sec,
            "post_per_sec": cfg.rate_post_per_sec,
        }

    @property
    def base_url(self) -> str:
        return self.cfg.base_url.rstrip("/")

    def url(self, path: str) -> str:
        return urljoin(self.base_url + "/", str(path or "").lstrip("/"))

    def connect(self) -> "PUBLICMETADBClient":
        if not self.cfg.api_key:
            raise PUBLICMETADBAuthError("Missing PublicMetaDB api_key")
        return self

    def request(self, method: str, path: str, **kw: Any):
        return request_with_retries(
            self.session,
            method,
            self.url(path),
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            **kw,
        )

    def get(self, path: str, **kw: Any):
        return self.request("GET", path, **kw)

    def post(self, path: str, **kw: Any):
        return self.request("POST", path, **kw)

    def post_once(self, path: str, **kw: Any):
        return request_with_retries(
            self.session,
            "POST",
            self.url(path),
            timeout=self.cfg.timeout,
            max_retries=1,
            **kw,
        )

    def delete(self, path: str, **kw: Any):
        return self.request("DELETE", path, **kw)

    def get_json(self, path: str, **kw: Any) -> Mapping[str, Any]:
        r = self.get(path, **kw)
        if not (200 <= r.status_code < 300):
            return {"status": r.status_code}
        data = safe_json(r)
        return data if isinstance(data, Mapping) else {}

    def post_json(self, path: str, **kw: Any) -> Mapping[str, Any]:
        r = self.post(path, **kw)
        if not (200 <= r.status_code < 300):
            return {"status": r.status_code}
        data = safe_json(r)
        return data if isinstance(data, Mapping) else {}

    @staticmethod
    def safe_json(resp: Any) -> Any:
        return safe_json(resp)


class PUBLICMETADBModule:
    def __init__(self, cfg: Mapping[str, Any]):
        p = dict(cfg.get("publicmetadb") or {})
        rl_obj = p.get("rate_limit")
        rl: Mapping[str, Any] = rl_obj if isinstance(rl_obj, Mapping) else {}

        def _float(key: str, default: float, max_value: float) -> float:
            try:
                val = float(rl.get(key, default))
            except Exception:
                val = default
            if val < 0:
                return 0.0
            return min(val, max_value)

        def _int_range(key: str, default: int, lo: int, hi: int) -> int:
            try:
                val = int(p.get(key, default) or default)
            except Exception:
                val = default
            return max(lo, min(val, hi))

        self.cfg = PUBLICMETADBConfig(
            api_key=str(p.get("api_key") or "").strip(),
            base_url=str(p.get("base_url") or "https://publicmetadb.com").strip().rstrip("/"),
            timeout=float(p.get("timeout", cfg.get("timeout", 15.0))),
            max_retries=int(p.get("max_retries", cfg.get("max_retries", 3))),
            watchlist_name=str(p.get("watchlist_name") or "Watchlist").strip() or "Watchlist",
            rate_get_per_sec=_float("get_per_sec", 20.0, 20.0),
            rate_post_per_sec=_float("post_per_sec", 3.0, 3.0),
            watchlist_page_size=int(p.get("watchlist_page_size", 100) or 100),
            history_per_page=int(p.get("history_per_page", 100) or 100),
            history_max_pages=int(p.get("history_max_pages", 1000) or 1000),
            progress_per_page=int(p.get("progress_per_page", 100) or 100),
            progress_max_pages=int(p.get("progress_max_pages", 1000) or 1000),
            ratings_label=str(p.get("ratings_label") or "Overall").strip() or "Overall",
            ratings_submit_per_hour=_int_range("ratings_submit_per_hour", 200, 1, 200),
            ratings_update_per_hour=_int_range("ratings_update_per_hour", 100, 1, 100),
        )
        if not self.cfg.api_key:
            raise PUBLICMETADBAuthError("Missing PublicMetaDB api_key")
        if p.get("debug") in (True, "1", 1):
            os.environ.setdefault("CW_PUBLICMETADB_DEBUG", "1")
        self.client = PUBLICMETADBClient(self.cfg, cfg).connect()
        self.raw_cfg = cfg
        self.config = cfg

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return _features_flags()

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    @staticmethod
    def normalize(obj: Any) -> dict[str, Any]:
        return id_minimal(obj) if isinstance(obj, Mapping) else {}

    @staticmethod
    def key_of(obj: Any) -> str:
        return canonical_key(id_minimal(obj)) if isinstance(obj, Mapping) else ""

    def health(self) -> Mapping[str, Any]:
        start = time.perf_counter()
        code: int | None = None
        reason: str | None = None
        rate = {"limit": None, "remaining": None, "reset": None}
        try:
            r = self.client.get("/api/external/lists", params={"page": 1, "perPage": 1})
            code = r.status_code
            rate = parse_rate_limit(r.headers)
            if 200 <= r.status_code < 300:
                ok = True
                status = "ok"
            elif r.status_code in (401, 403):
                ok = False
                status = "auth_failed"
                reason = "unauthorized"
            else:
                ok = False
                status = "down"
                reason = f"http:{r.status_code}"
        except Exception as e:
            ok = False
            status = "down"
            reason = f"exception:{type(e).__name__}"
        latency_ms = int((time.perf_counter() - start) * 1000)
        cw_log("PUBLICMETADB", "health", "info", "health", ok=ok, status=status, latency_ms=latency_ms)
        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": {"watchlist": ok, "ratings": ok, "history": ok, "progress": ok, "playlists": False},
            "details": {"reason": reason} if reason else None,
            "api": {"lists": {"status": code, "rate": rate}},
        }

    def feature_names(self) -> tuple[str, ...]:
        return ("watchlist", "ratings", "history", "progress")

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, dict[str, Any]]:
        if feature == "watchlist":
            items = feat_watchlist.build_index(self)
        elif feature == "history":
            items = feat_history.build_index(self)
        elif feature == "ratings":
            items = feat_ratings.build_index(self)
        elif feature == "progress":
            items = feat_progress.build_index(self)
        else:
            return {}
        return enrich_index_metadata(self, items, feature=feature)

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        lst = list(items or [])
        if feature not in ("watchlist", "ratings", "history", "progress"):
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        if feature == "history":
            cnt, unresolved = feat_history.add(self, lst)
        elif feature == "progress":
            cnt, unresolved = feat_progress.add(self, lst)
        elif feature == "ratings":
            cnt, unresolved = feat_ratings.add(self, lst)
        else:
            cnt, unresolved = feat_watchlist.add(self, lst)
        return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": _confirmed_keys(lst, unresolved)}

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        lst = list(items or [])
        if feature not in ("watchlist", "ratings", "history", "progress"):
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        if feature == "history":
            cnt, unresolved = feat_history.remove(self, lst)
        elif feature == "progress":
            cnt, unresolved = feat_progress.remove(self, lst)
        elif feature == "ratings":
            cnt, unresolved = feat_ratings.remove(self, lst)
        else:
            cnt, unresolved = feat_watchlist.remove(self, lst)
        return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": _confirmed_keys(lst, unresolved)}


class _PUBLICMETADBOPS:
    def name(self) -> str:
        return "PUBLICMETADB"

    def label(self) -> str:
        return "PublicMetaDB"

    def features(self) -> Mapping[str, bool]:
        return PUBLICMETADBModule.supported_features()

    def state_read_features(self) -> Mapping[str, bool]:
        """Features for complete captures and provider-state imports."""
        features = dict(PUBLICMETADBModule.supported_features())
        # PublicMetaDB accepts rating writes but cannot enumerate a user's
        # complete ratings inventory. The local shadow is intentionally not
        # treated as authoritative provider state.
        features["ratings"] = False
        return features

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        p = (cfg or {}).get("publicmetadb") or {}
        return bool(str(p.get("api_key") or "").strip())

    def _adapter(self, cfg: Mapping[str, Any]) -> PUBLICMETADBModule:
        return PUBLICMETADBModule(cfg)

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()


OPS = _PUBLICMETADBOPS()
