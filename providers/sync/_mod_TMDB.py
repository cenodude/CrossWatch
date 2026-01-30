# providers/sync/_mod_TMDB.py
# CrossWatch -  TMDb sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from ._log import log as cw_log
from ._mod_common import build_session, make_snapshot_progress, parse_rate_limit, request_with_retries

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "0.1.0"
__all__ = ["get_manifest", "TMDBModule", "OPS"]


def _health(status: str, ok: bool, latency_ms: int, *, rate_limit: Mapping[str, Any] | None = None) -> None:
    cw_log("TMDB", "health", "info", "health", latency_ms=latency_ms, ok=ok, status=status, rate_limit=rate_limit or {})


def _dbg(msg: str, **fields: Any) -> None:
    if "feature" in fields:
        fields = {**fields, "sync_feature": fields.get("feature")}
        fields.pop("feature", None)
    cw_log("TMDB", "module", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    if "feature" in fields:
        fields = {**fields, "sync_feature": fields.get("feature")}
        fields.pop("feature", None)
    cw_log("TMDB", "module", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    if "feature" in fields:
        fields = {**fields, "sync_feature": fields.get("feature")}
        fields.pop("feature", None)
    cw_log("TMDB", "module", "warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    if "feature" in fields:
        fields = {**fields, "sync_feature": fields.get("feature")}
        fields.pop("feature", None)
    cw_log("TMDB", "module", "error", msg, **fields)


def _label_tmdb(*_args: Any, **_kwargs: Any) -> str:
    return "TMDB"


try:
    from .tmdb import _watchlist as feat_watchlist
except Exception as e:
    _warn("feature_import_failed", import_feature="watchlist", error=f"{type(e).__name__}: {e}")
    feat_watchlist = None

try:
    from .tmdb import _ratings as feat_ratings
except Exception as e:
    _warn("feature_import_failed", import_feature="ratings", error=f"{type(e).__name__}: {e}")
    feat_ratings = None

try:
    from .tmdb._common import key_of as _tmdb_key_of_impl
except Exception:
    _tmdb_key_of_impl = None


def _tmdb_key_of(_obj: Any) -> str:
    if _tmdb_key_of_impl is None:
        return ""
    if isinstance(_obj, Mapping):
        try:
            return str(_tmdb_key_of_impl(_obj) or "").strip()
        except Exception:
            return ""
    return ""


def _features_flags() -> dict[str, bool]:
    return {
        "watchlist": bool(feat_watchlist),
        "ratings": bool(feat_ratings),
        "history": False,
        "playlists": False,
    }


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "TMDB",
        "label": "TMDb",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": _features_flags(),
        "requires": ["api_key", "session_id"],
        "capabilities": {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "watchlist": {
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "add": True,
                "remove": True,
            },
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "add": True,
                "remove": True,
            },
        },
        "auth": {
            "config_key": "tmdb_sync",
            "fields": [
                {"key": "tmdb_sync.api_key", "label": "API Key", "type": "secret"},
                {"key": "tmdb_sync.session_id", "label": "Session ID", "type": "secret"},
                {"key": "tmdb_sync.account_id", "label": "Account ID", "type": "text", "optional": True},
            ],
        },
    }


@dataclass
class TMDBConfig:
    api_key: str
    session_id: str
    account_id: str
    timeout: float = 15.0
    max_retries: int = 3


class TMDBError(RuntimeError):
    pass


class TMDBAuthError(TMDBError):
    pass


class TMDBClient:
    BASE = "https://api.themoviedb.org/3"

    def __init__(self, cfg: TMDBConfig) -> None:
        self.cfg = cfg
        self.session = build_session("TMDB", ctx, feature_label=_label_tmdb)

    def connect(self) -> TMDBClient:
        if not self.cfg.api_key or not self.cfg.session_id:
            raise TMDBAuthError("Missing TMDb api_key/session_id")
        try:
            self.session.trust_env = False
        except Exception:
            pass
        try:
            self.session.headers.setdefault("Accept", "application/json")
            self.session.headers.setdefault("User-Agent", f"CrossWatch TMDB/{__VERSION__}")
        except Exception:
            pass
        return self

    def _abs_url(self, url_or_path: str) -> str:
        u = str(url_or_path or "")
        if u.startswith("http://") or u.startswith("https://"):
            return u
        if not u.startswith("/"):
            u = "/" + u
        return self.BASE + u

    def _params(self, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
        p: dict[str, Any] = {"api_key": self.cfg.api_key}
        if extra:
            p.update(dict(extra))
        return p

    def _user_params(self, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
        p = self._params(extra)
        p.setdefault("session_id", self.cfg.session_id)
        return p

    def get(self, url_or_path: str, *, timeout: float | None = None, max_retries: int | None = None, **kw: Any):
        return request_with_retries(
            self.session,
            "GET",
            self._abs_url(url_or_path),
            timeout=float(timeout if timeout is not None else self.cfg.timeout),
            max_retries=int(max_retries if max_retries is not None else self.cfg.max_retries),
            **kw,
        )

    def post(self, url_or_path: str, *, timeout: float | None = None, max_retries: int | None = None, **kw: Any):
        return request_with_retries(
            self.session,
            "POST",
            self._abs_url(url_or_path),
            timeout=float(timeout if timeout is not None else self.cfg.timeout),
            max_retries=int(max_retries if max_retries is not None else self.cfg.max_retries),
            **kw,
        )

    def delete(self, url_or_path: str, *, timeout: float | None = None, max_retries: int | None = None, **kw: Any):
        return request_with_retries(
            self.session,
            "DELETE",
            self._abs_url(url_or_path),
            timeout=float(timeout if timeout is not None else self.cfg.timeout),
            max_retries=int(max_retries if max_retries is not None else self.cfg.max_retries),
            **kw,
        )

    def get_json(self, path: str, *, params: Mapping[str, Any] | None = None, user: bool = True) -> Mapping[str, Any]:
        p = self._user_params(params) if user else self._params(params)
        r = self.get(path, params=p)
        return r.json() if (r.content or b"") else {}

    def post_json(self, path: str, *, params: Mapping[str, Any] | None = None, json_body: Any | None = None, user: bool = True) -> Mapping[str, Any]:
        p = self._user_params(params) if user else self._params(params)
        r = self.post(path, params=p, json=json_body)
        return r.json() if (r.content or b"") else {}

    def delete_json(self, path: str, *, params: Mapping[str, Any] | None = None, user: bool = True) -> Mapping[str, Any]:
        p = self._user_params(params) if user else self._params(params)
        r = self.delete(path, params=p)
        return r.json() if (r.content or b"") else {}

    def ensure_account_id(self) -> str:
        if self.cfg.account_id:
            return self.cfg.account_id
        data = self.get_json("/account", user=True)
        aid = data.get("id")
        if aid is None:
            raise TMDBAuthError("TMDb account id missing (invalid session_id?)")
        self.cfg.account_id = str(aid)
        return self.cfg.account_id

    def account_id(self) -> str:
        return self.ensure_account_id()

    def find_by_external(self, external_id: str, external_source: str) -> Mapping[str, Any]:
        return self.get_json(f"/find/{external_id}", params={"external_source": external_source}, user=False)


_FEATURES: dict[str, Any] = {}
if feat_watchlist:
    _FEATURES["watchlist"] = feat_watchlist
if feat_ratings:
    _FEATURES["ratings"] = feat_ratings


def _confirmed_keys(key_of, items: Iterable[Mapping[str, Any]], unresolved: Any) -> list[str]:
    unresolved_keys: set[str] = set()
    if unresolved:
        for u in unresolved:
            if isinstance(u, Mapping):
                k = u.get("key")
                if k:
                    unresolved_keys.add(str(k))
            elif isinstance(u, str):
                unresolved_keys.add(u)

    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        k = str(key_of(it) or "")
        if not k or k in seen or k in unresolved_keys:
            continue
        seen.add(k)
        out.append(k)
    return out


class TMDBModule:
    def __init__(self, cfg: Mapping[str, Any]) -> None:
        raw = (cfg or {}).get("tmdb_sync")
        m: Mapping[str, Any] = raw if isinstance(raw, Mapping) else {}

        api_key = str(m.get("api_key") or "").strip()
        session_id = str(m.get("session_id") or "").strip()
        account_id = str(m.get("account_id") or "").strip()
        timeout = float(m.get("timeout", 15.0) or 15.0)
        max_retries = int(m.get("max_retries", 3) or 3)

        self.cfg = TMDBConfig(api_key=api_key, session_id=session_id, account_id=account_id, timeout=timeout, max_retries=max_retries)

        if m.get("debug") in (True, "1", 1):
            os.environ.setdefault("CW_TMDB_DEBUG", "1")

        if not self.cfg.api_key or not self.cfg.session_id:
            raise TMDBAuthError("TMDb not configured (api_key + session_id required)")

        self.client = TMDBClient(self.cfg).connect()

        def _mk_prog(feature: str):
            try:
                return make_snapshot_progress(ctx, dst="TMDB", feature=feature)
            except Exception:
                class _Noop:
                    def tick(self, *_a: Any, **_kw: Any) -> None:
                        pass

                    def done(self, *_a: Any, **_kw: Any) -> None:
                        pass

                return _Noop()

        self.progress_factory: Callable[[str], Any] = _mk_prog

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return _features_flags()

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, dict[str, Any]]:
        mod = _FEATURES.get(feature)
        if not mod:
            return {}
        _info("build_index", sync_feature=feature)
        t0 = time.perf_counter()
        out = mod.build_index(self, **kwargs)
        _info("build_index_done", sync_feature=feature, count=len(out), ms=int((time.perf_counter() - t0) * 1000))
        return out

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        lst = list(items)
        if not lst:
            return {"ok": True, "count": 0}
        mod = _FEATURES.get(feature)
        if not mod:
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        _info("add", sync_feature=feature, count=len(lst))
        try:
            cnt, unresolved = mod.add(self, lst)
            confirmed_keys = _confirmed_keys(_tmdb_key_of, lst, unresolved)
            _info("add_done", sync_feature=feature, ok=True, applied=int(cnt), unresolved=len(unresolved or []))
            return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": confirmed_keys}
        except Exception as e:
            _error("add_failed", sync_feature=feature, error=f"{type(e).__name__}: {e}")
            return {"ok": False, "error": str(e)}

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        lst = list(items)
        if not lst:
            return {"ok": True, "count": 0}
        mod = _FEATURES.get(feature)
        if not mod:
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        _info("remove", sync_feature=feature, count=len(lst))
        try:
            cnt, unresolved = mod.remove(self, lst)
            confirmed_keys = _confirmed_keys(_tmdb_key_of, lst, unresolved)
            _info("remove_done", sync_feature=feature, ok=True, applied=int(cnt), unresolved=len(unresolved or []))
            return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": confirmed_keys}
        except Exception as e:
            _error("remove_failed", sync_feature=feature, error=f"{type(e).__name__}: {e}")
            return {"ok": False, "error": str(e)}

    def health(self) -> Mapping[str, Any]:
        start = time.perf_counter()
        try:
            r = self.client.get("/account", params=self.client._user_params(), timeout=max(2.0, min(self.cfg.timeout, 8.0)))
            ok = 200 <= r.status_code < 300
            status = "ok" if ok else f"http_{r.status_code}"
            rl = parse_rate_limit(r.headers)
            latency_ms = int((time.perf_counter() - start) * 1000)
            _health(status, ok, latency_ms, rate_limit=rl)
            return {"ok": ok, "status": status, "latency_ms": latency_ms, "rate_limit": rl}
        except Exception as e:
            latency_ms = int((time.perf_counter() - start) * 1000)
            _warn("health_failed", latency_ms=latency_ms, ok=False, error=f"{type(e).__name__}: {e}")
            return {"ok": False, "status": "error", "latency_ms": latency_ms, "error": str(e)}


class _TMDBOPS:
    def name(self) -> str:
        return "TMDB"

    def label(self) -> str:
        return "TMDb"

    def features(self) -> Mapping[str, bool]:
        return TMDBModule.supported_features()

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest().get("capabilities") or {}

    def activities(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return {}

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        raw = (cfg or {}).get("tmdb_sync")
        m: Mapping[str, Any] = raw if isinstance(raw, Mapping) else {}
        api_key = str(m.get("api_key") or "").strip()
        session_id = str(m.get("session_id") or "").strip()
        return bool(api_key and session_id)

    def _adapter(self, cfg: Mapping[str, Any]) -> TMDBModule:
        return TMDBModule(cfg)

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()


OPS = _TMDBOPS()
