# /providers/sync/_mod_MDBLIST.py
# CrossWatch MDBLIST module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from ._log import log as cw_log

from ._mod_common import (
    build_session,
    request_with_retries,
    parse_rate_limit,
    make_snapshot_progress,
)


def _confirmed_keys(key_of, items: Iterable[Mapping[str, Any]], unresolved: Any) -> list[str]:
    attempted: list[str] = []
    for it in items or []:
        try:
            k = str(key_of(it) or "").strip()
        except Exception:
            k = ""
        if k:
            attempted.append(k)

    unresolved_keys: set[str] = set()
    if unresolved:
        for u in unresolved:
            obj: Any = u
            if isinstance(u, Mapping):
                if isinstance(u.get("key"), str) and u.get("key"):
                    unresolved_keys.add(str(u.get("key")))
                    continue
                if "item" in u:
                    obj = u.get("item")
            if isinstance(obj, str) and obj:
                unresolved_keys.add(obj)
                continue
            if isinstance(obj, Mapping):
                try:
                    k = str(key_of(obj) or "").strip()
                except Exception:
                    k = ""
                if k:
                    unresolved_keys.add(k)

    out: list[str] = []
    seen: set[str] = set()
    for k in attempted:
        if k in unresolved_keys or k in seen:
            continue
        out.append(k)
        seen.add(k)
    return out

def _mdblist_key_of(obj: Any) -> str:
    try:
        from cw_platform.id_map import canonical_key, minimal as id_minimal
        if isinstance(obj, Mapping):
            return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        pass
    return ""

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "4.1.0"
__all__ = ["get_manifest", "MDBLISTModule", "OPS"]

def _health(status: str, ok: bool, latency_ms: int) -> None:
    cw_log("MDBLIST", "health", "info", "health", latency_ms=latency_ms, ok=ok, status=status)

def _dbg(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "module", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "module", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "module", "warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "module", "error", msg, **fields)


def _log(msg: str, **fields: Any) -> None:
    _dbg(msg, **fields)


def _label_mdblist(*_args: Any, **_kwargs: Any) -> str:
    return "MDBLIST"


try:
    from .mdblist import _watchlist as feat_watchlist
except Exception as e:
    _warn("feature_import_failed", import_feature="watchlist", error=f"{type(e).__name__}: {e}")
    feat_watchlist = None

try:
    from .mdblist import _ratings as feat_ratings
except Exception as e:
    _warn("feature_import_failed", import_feature="ratings", error=f"{type(e).__name__}: {e}")
    feat_ratings = None

try:
    from .mdblist import _history as feat_history
except Exception as e:
    _warn("feature_import_failed", import_feature="history", error=f"{type(e).__name__}: {e}")
    feat_history = None


_FEATURES: dict[str, Any] = {}
if feat_watchlist:
    _FEATURES["watchlist"] = feat_watchlist
if feat_ratings:
    _FEATURES["ratings"] = feat_ratings
if feat_history:
    _FEATURES["history"] = feat_history


def _features_flags() -> dict[str, bool]:
    return {
        "watchlist": "watchlist" in _FEATURES,
        "ratings": "ratings" in _FEATURES,
        "history": "history" in _FEATURES,
        "playlists": False,
    }


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "MDBLIST",
        "label": "MDBList",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": _features_flags(),
        "requires": [],
        "capabilities": {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "history": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True,
                "remove": True,
                "from_date": True,
            },
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True,
                "unrate": True,
                "from_date": True,
            },
        },
    }


@dataclass
class MDBLISTConfig:
    api_key: str
    timeout: float = 15.0
    max_retries: int = 3


class MDBLISTError(RuntimeError):
    pass


class MDBLISTAuthError(MDBLISTError):
    pass


class MDBLISTClient:
    BASE = "https://api.mdblist.com"

    def __init__(self, cfg: MDBLISTConfig, raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self.session = build_session("MDBLIST", ctx, feature_label=_label_mdblist)

    def connect(self) -> MDBLISTClient:
        if not self.cfg.api_key:
            raise MDBLISTAuthError("Missing MDBList api_key")
        try:
            self.session.trust_env = False
        except Exception:
            pass
        try:
            self.session.headers.setdefault("Accept", "application/json")
            self.session.headers.setdefault("User-Agent", f"CrossWatch MDBLIST/{__VERSION__}")
        except Exception:
            pass
        return self

    def get(self, url: str, **kw: Any):
        return request_with_retries(
            self.session,
            "GET",
            url,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            **kw,
        )

    def post(self, url: str, **kw: Any):
        return request_with_retries(
            self.session,
            "POST",
            url,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            **kw,
        )

    def last_activities(self) -> Mapping[str, Any]:
        r = self.get(f"{self.BASE}/sync/last_activities", params={"apikey": self.cfg.api_key})
        if 200 <= r.status_code < 300:
            try:
                data = r.json() if (r.text or "").strip() else {}
                return dict(data) if isinstance(data, Mapping) else {}
            except Exception:
                return {}
        return {"status": r.status_code}


class MDBLISTModule:
    def __init__(self, cfg: Mapping[str, Any]):
        m = dict(cfg.get("mdblist") or {})
        self.cfg = MDBLISTConfig(
            api_key=str(m.get("api_key") or "").strip(),
            timeout=float(m.get("timeout", cfg.get("timeout", 15.0))),
            max_retries=int(m.get("max_retries", cfg.get("max_retries", 3))),
        )
        if not self.cfg.api_key:
            raise MDBLISTAuthError("Missing MDBList api_key")

        if m.get("debug") in (True, "1", 1):
            os.environ.setdefault("CW_MDBLIST_DEBUG", "1")

        self.client = MDBLISTClient(self.cfg, cfg).connect()
        self.raw_cfg = cfg
        self.config = cfg

        def _mk_prog(feature: str):
            try:
                return make_snapshot_progress(ctx, dst="MDBLIST", feature=feature)
            except Exception:
                class _Noop:
                    def tick(self, *args: Any, **kwargs: Any) -> None:
                        pass

                    def done(self, *args: Any, **kwargs: Any) -> None:
                        pass

                return _Noop()

        self.progress_factory: Callable[[str], Any] = _mk_prog

    @staticmethod
    def supported_features() -> dict[str, bool]:
        toggles = {"watchlist": True, "ratings": True, "history": True, "playlists": False}
        present = _features_flags()
        return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}

    def _is_enabled(self, feature: str) -> bool:
        return bool(self.supported_features().get(feature, False))

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        enabled = self.supported_features()
        need_any = any(enabled.values())
        if not need_any:
            return {
                "ok": True,
                "status": "ok",
                "latency_ms": 0,
                "features": {},
                "details": {"disabled": ["watchlist", "ratings", "history"]},
                "api": {},
            }

        m = dict(self.raw_cfg.get("mdblist") or {})
        tmo = float(m.get("health_timeout", max(2.0, min(self.cfg.timeout, 8.0))))
        hr = int(m.get("health_max_retries", 1))
        hr = max(1, min(hr, self.cfg.max_retries))

        base = self.client.BASE
        sess = self.client.session
        start = time.perf_counter()

        def hit(path: str, *, params: dict[str, Any]) -> tuple[bool, int | None, int | None, dict[str, int | None], str | None]:
            ok = False
            code: int | None = None
            retry_after: int | None = None
            rate: dict[str, int | None] = {"limit": None, "remaining": None, "reset": None}
            err: str | None = None
            try:
                r = request_with_retries(
                    sess,
                    "GET",
                    f"{base}{path}",
                    params=params,
                    timeout=tmo,
                    max_retries=hr,
                )
                code = r.status_code
                if 200 <= r.status_code < 300:
                    ok = True
                elif r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    if ra:
                        try:
                            retry_after = int(ra)
                        except Exception:
                            pass
                rate = parse_rate_limit(r.headers)
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
            return ok, code, retry_after, rate, err

        user_ok, user_code, user_ra, user_rate, user_err = hit("/user", params={"apikey": self.cfg.api_key})

        wl_ok = False
        wl_code: int | None = None
        wl_ra: int | None = None
        wl_rate: dict[str, int | None] = user_rate
        wl_err: str | None = None
        if enabled.get("watchlist"):
            wl_ok, wl_code, wl_ra, wl_rate, wl_err = hit(
                "/watchlist/items",
                params={"apikey": self.cfg.api_key, "limit": 1, "offset": 0},
            )

        rt_ok = False
        rt_code: int | None = None
        rt_ra: int | None = None
        rt_rate: dict[str, int | None] = user_rate
        rt_err: str | None = None
        if enabled.get("ratings"):
            rt_ok, rt_code, rt_ra, rt_rate, rt_err = hit(
                "/sync/ratings",
                params={"apikey": self.cfg.api_key, "offset": 0, "limit": 1},
            )

        hs_ok = False
        hs_code: int | None = None
        hs_ra: int | None = None
        hs_rate: dict[str, int | None] = user_rate
        hs_err: str | None = None
        if enabled.get("history"):
            hs_ok, hs_code, hs_ra, hs_rate, hs_err = hit(
                "/sync/last_activities",
                params={"apikey": self.cfg.api_key},
            )
            if (not hs_ok) and hs_code in (404, 405):
                hs_ok, hs_code, hs_ra, hs_rate, hs_err = hit(
                    "/sync/watched",
                    params={"apikey": self.cfg.api_key, "offset": 0, "limit": 1, "since": "1900-01-01T00:00:00Z"},
                )

        latency_ms = int((time.perf_counter() - start) * 1000)

        features = {
            "watchlist": wl_ok if enabled.get("watchlist") else False,
            "ratings": rt_ok if enabled.get("ratings") else False,
            "history": hs_ok if enabled.get("history") else False,
            "playlists": False,
        }

        checks = [features[k] for k in ("watchlist", "ratings", "history") if enabled.get(k)]
        if not checks:
            status = "ok"
        elif all(checks):
            status = "ok"
        elif any(checks):
            status = "degraded"
        else:
            status = "down"

        ok = status in ("ok", "degraded")

        details: dict[str, Any] = {}
        disabled = [k for k, v in enabled.items() if not v]
        if disabled:
            details["disabled"] = disabled
        errs: dict[str, str] = {}
        if user_err:
            errs["user"] = user_err
        if wl_err:
            errs["watchlist"] = wl_err
        if rt_err:
            errs["ratings"] = rt_err
        if hs_err:
            errs["history"] = hs_err
        if errs:
            details["errors"] = errs

        api = {
            "user": {
                "status": user_code,
                "retry_after": user_ra,
                "rate": user_rate,
            },
            "watchlist": {
                "status": wl_code,
                "retry_after": wl_ra,
                "rate": wl_rate,
            },
            "ratings": {
                "status": rt_code,
                "retry_after": rt_ra,
                "rate": rt_rate,
            },
            "history": {
                "status": hs_code,
                "retry_after": hs_ra,
                "rate": hs_rate,
            },
        }

        _health(status, ok, latency_ms)
        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": features,
            "details": details or None,
            "api": api,
        }

    def feature_names(self) -> tuple[str, ...]:
        return tuple(k for k, v in self.supported_features().items() if v and k in _FEATURES)

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, dict[str, Any]]:
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _dbg("build_index_skipped", requested_feature=feature)
            return {}
        mod = _FEATURES.get(feature)
        return mod.build_index(self, **kwargs) if mod else {}

    def add(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        lst = list(items)
        if not lst:
            return {"ok": True, "count": 0}
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _dbg("add_skipped", requested_feature=feature)
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _warn("add_skipped_missing_module", requested_feature=feature)
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            cnt, unresolved = mod.add(self, lst)
            confirmed_keys = _confirmed_keys(_mdblist_key_of, lst, unresolved)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": confirmed_keys}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def remove(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        lst = list(items)
        if not lst:
            return {"ok": True, "count": 0}
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _dbg("remove_skipped", requested_feature=feature)
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _warn("remove_skipped_missing_module", requested_feature=feature)
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            cnt, unresolved = mod.remove(self, lst)
            confirmed_keys = _confirmed_keys(_mdblist_key_of, lst, unresolved)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": confirmed_keys}
        except Exception as e:
            return {"ok": False, "error": str(e)}


class _MDBLISTOPS:
    def name(self) -> str:
        return "MDBLIST"

    def label(self) -> str:
        return "MDBList"

    def features(self) -> Mapping[str, bool]:
        return MDBLISTModule.supported_features()

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "history": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True,
                "remove": True,
                "from_date": True,
            },
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True,
                "unrate": True,
                "from_date": True,
            },
        }


    _acts_memo: dict[str, tuple[float, dict[str, Any]]] = {}

    def activities(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        try:
            c = cfg or {}
            m = c.get("mdblist") or {}
            apikey = str(m.get("api_key") or m.get("apikey") or "").strip()
            if not apikey:
                return {}

            now = time.time()
            memo_key = apikey[-6:] if len(apikey) >= 6 else apikey
            ent = self._acts_memo.get(memo_key)
            if ent and (now - float(ent[0])) < 10.0:
                return dict(ent[1])

            sess = build_session("MDBLIST", ctx, feature_label=_label_mdblist)
            try:
                sess.trust_env = False
            except Exception:
                pass
            try:
                sess.headers.setdefault("Accept", "application/json")
                sess.headers.setdefault("User-Agent", f"CrossWatch MDBLIST/{__VERSION__}")
            except Exception:
                pass

            r = request_with_retries(
                sess,
                "GET",
                f"{MDBLISTClient.BASE}/sync/last_activities",
                params={"apikey": apikey},
                timeout=8.0,
                max_retries=1,
            )
            if not (200 <= r.status_code < 300):
                return {}

            raw = r.json() if (r.text or "").strip() else {}
            if not isinstance(raw, Mapping):
                return {}

            acts = dict(raw)
            watch = acts.get("watchlisted_at") or acts.get("watchlist") or acts.get("updated_at")
            rated = acts.get("rated_at") or acts.get("ratings") or acts.get("updated_at")
            hist_candidates = [
                acts.get("watched_at"),
                acts.get("season_watched_at"),
                acts.get("episode_watched_at"),
                acts.get("history"),
                acts.get("updated_at"),
            ]
            hist: Any = None
            for candidate in hist_candidates:
                if not candidate:
                    continue
                if not hist:
                    hist = candidate
                    continue
                try:
                    hist = candidate if str(candidate) > str(hist) else hist
                except Exception:
                    hist = hist or candidate
            updated = acts.get("updated_at") or hist or rated or watch

            out = {
                "watchlist": watch,
                "ratings": rated,
                "history": hist,
                "updated_at": updated,
            }
            self._acts_memo[memo_key] = (now, dict(out))
            return out
        except Exception:
            return {}

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        c = cfg or {}
        m = c.get("mdblist") or {}
        key = m.get("api_key") or m.get("apikey") or ""
        return bool(str(key).strip())

    def _adapter(self, cfg: Mapping[str, Any]) -> MDBLISTModule:
        return MDBLISTModule(cfg)

    def build_index(
        self,
        cfg: Mapping[str, Any],
        *,
        feature: str,
    ) -> Mapping[str, dict[str, Any]]:
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

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

OPS = _MDBLISTOPS()
