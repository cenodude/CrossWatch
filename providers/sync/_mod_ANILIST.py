# /providers/sync/_mod_ANILIST.py
# CrossWatch AniList module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from ._mod_common import build_session, make_snapshot_progress, request_with_retries
from ._log import log as cw_log
from cw_platform.id_map import canonical_key, minimal as id_minimal

__VERSION__ = "0.1.0"
__all__ = ["get_manifest", "ANILISTModule", "OPS"]

def _health(status: str, ok: bool, latency_ms: int) -> None:
    cw_log("ANILIST", "health", "info", "health", latency_ms=latency_ms, ok=ok, status=status)


if "ctx" not in globals():
    class _NullCtx:
        def emit(self, *args: Any, **kwargs: Any) -> None:
            pass

    ctx = _NullCtx()  # type: ignore[assignment]


try:
    from .anilist import _watchlist as feat_watchlist
except Exception as e:
    feat_watchlist = None
    # NOTE: 'feature' is reserved in cw_log; use a different field key.
    cw_log(
        "ANILIST",
        "module",
        "warn",
        "feature_import_failed",
        import_feature="watchlist",
        error_type=e.__class__.__name__,
        error=str(e),
    )


GQL_URL = "https://graphql.anilist.co"
UA = "CrossWatch/1.0"


class ANILISTError(RuntimeError):
    pass


class ANILISTAuthError(ANILISTError):
    pass


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "module", "debug", msg, **fields)

def _info(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "module", "info", msg, **fields)

def _warn(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "module", "warn", msg, **fields)

def _error(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "module", "error", msg, **fields)


def label_anilist(method: str, url: str, kw: Mapping[str, Any]) -> str:
    try:
        payload = kw.get("json")
        if isinstance(payload, Mapping):
            q = str(payload.get("query") or "")
            if "Viewer" in q:
                return "viewer"
            if "MediaListCollection" in q:
                return "watchlist:index"
            if "SaveMediaListEntry" in q:
                return "watchlist:add"
            if "DeleteMediaListEntry" in q:
                return "watchlist:remove"
            if "MediaList(" in q or " MediaList(" in q:
                return "watchlist:lookup"
            if "Media(idMal" in q:
                return "watchlist:resolve"
    except Exception:
        pass
    return "graphql"



@dataclass
class ANILISTConfig:
    access_token: str
    timeout: float = 15.0
    max_retries: int = 3


class ANILISTClient:
    def __init__(self, cfg: ANILISTConfig, raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self.session = build_session("ANILIST", ctx, feature_label=label_anilist)
        self._apply_headers(cfg.access_token)
        self._viewer_cache: dict[str, Any] | None = None

    def _apply_headers(self, tok: str) -> None:
        self.session.headers.update(
            {
                "Authorization": f"Bearer {tok}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": UA,
            }
        )

    def gql(
        self,
        query: str,
        variables: Mapping[str, Any] | None = None,
        *,
        feature: str | None = None,
        tolerate_errors: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"query": query}
        if variables is not None:
            payload["variables"] = dict(variables)

        r = request_with_retries(
            self.session,
            "POST",
            GQL_URL,
            json=payload,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
        )

        try:
            j = r.json() or {}
        except Exception:
            j = {}

        if r.status_code in (401, 403):
            raise ANILISTAuthError("AniList unauthorized")
        if r.status_code >= 400:
            raise ANILISTError(f"AniList http:{r.status_code}")

        errs = j.get("errors")
        if errs and not tolerate_errors:
            msg = None
            if isinstance(errs, list) and errs:
                first = errs[0]
                if isinstance(first, Mapping):
                    msg = first.get("message")
            raise ANILISTError(str(msg or "AniList GraphQL error"))

        data = j.get("data")
        return data if isinstance(data, dict) else {}

    def viewer(self) -> dict[str, Any]:
        if isinstance(self._viewer_cache, dict) and self._viewer_cache.get("id"):
            return self._viewer_cache
        q = "query { Viewer { id name } }"
        data = self.gql(q, feature="viewer", tolerate_errors=True)
        v = (data or {}).get("Viewer")
        self._viewer_cache = dict(v) if isinstance(v, Mapping) else {}
        return self._viewer_cache

    @staticmethod
    def normalize(obj: Any) -> dict[str, Any]:
        return id_minimal(obj)

    @staticmethod
    def key_of(obj: Any) -> str:
        m = id_minimal(obj)
        return canonical_key(m) or ""


def supported_features() -> dict[str, bool]:
    return {"watchlist": bool(feat_watchlist), "ratings": False, "history": False, "playlists": False}


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "ANILIST",
        "label": "AniList",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": supported_features(),
        "requires": [],
        "capabilities": {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "observed_deletes": False,
        },
    }


class ANILISTModule:
    def __init__(self, cfg: Mapping[str, Any]):
        an = dict(cfg.get("anilist") or {})
        au = (cfg.get("auth") or {}).get("anilist") or {}

        tok = str(
            an.get("access_token")
            or an.get("token")
            or (an.get("oauth") or {}).get("access_token")
            or (au.get("access_token") if isinstance(au, Mapping) else "")
            or (au.get("token") if isinstance(au, Mapping) else "")
            or (au.get("oauth") or {}).get("access_token")
            or ""
        ).strip()
        if not tok:
            raise ANILISTError("ANILIST requires access_token")

        if an.get("debug") in (True, "1", 1):
            os.environ.setdefault("CW_ANILIST_DEBUG", "1")

        self.cfg = ANILISTConfig(
            access_token=tok,
            timeout=float(an.get("timeout", cfg.get("timeout", 15.0))),
            max_retries=int(an.get("max_retries", cfg.get("max_retries", 3))),
        )

        self.client = ANILISTClient(self.cfg, an)
        self.raw_cfg = cfg
        self.progress_factory = (
            lambda feature, total=None, throttle_ms=300: make_snapshot_progress(
                ctx,
                dst="ANILIST",
                feature=str(feature),
                total=total,
                throttle_ms=int(throttle_ms),
            )
        )

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        start = time.perf_counter()
        code: int | None = None
        retry_after: int | None = None
        rate: dict[str, Any] = {"limit": None, "remaining": None, "reset": None}

        ok = False
        status = "down"
        reason: str | None = None
        try:
            r = request_with_retries(
                self.client.session,
                "POST",
                GQL_URL,
                json={"query": "query { Viewer { id } }"},
                timeout=max(3.0, min(self.cfg.timeout, 15.0)),
                max_retries=max(0, min(self.cfg.max_retries, 3)),
            )
            code = r.status_code
            if code in (401, 403):
                status = "auth_failed"
                reason = "unauthorized"
            elif 200 <= code < 300:
                j = {}
                try:
                    j = r.json() or {}
                except Exception:
                    j = {}
                if j.get("errors"):
                    status = "degraded"
                    reason = "graphql_errors"
                else:
                    status = "ok"
                ok = status in ("ok", "degraded")
            else:
                status = "down"
                reason = f"http:{code}"

            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    retry_after = int(ra)
                except Exception:
                    pass
            rate = {
                "limit": r.headers.get("X-RateLimit-Limit"),
                "remaining": r.headers.get("X-RateLimit-Remaining"),
                "reset": r.headers.get("X-RateLimit-Reset"),
            }
        except ANILISTAuthError:
            status = "auth_failed"
            reason = "unauthorized"
        except Exception as e:
            status = "down"
            reason = f"exception:{e.__class__.__name__}"

        latency_ms = int((time.perf_counter() - start) * 1000)
        feats = supported_features()
        features = {
            "watchlist": bool(feats.get("watchlist") and ok),
            "ratings": False,
            "history": False,
            "playlists": False,
        }

        details: dict[str, Any] = {}
        if reason:
            details["reason"] = reason
        if retry_after is not None:
            details["retry_after_s"] = retry_after

        _health(status, bool(ok), int(latency_ms))
        return {
            "ok": bool(ok),
            "status": status,
            "latency_ms": latency_ms,
            "features": features,
            "details": details or None,
            "api": {"graphql": {"status": code, "retry_after": retry_after, "rate": rate}},
        }

    @staticmethod
    def normalize(obj: Any) -> dict[str, Any]:
        return ANILISTClient.normalize(obj)

    @staticmethod
    def key_of(obj: Any) -> str:
        return ANILISTClient.key_of(obj)

    def feature_names(self) -> tuple[str, ...]:
        feats = supported_features()
        return tuple(k for k, v in feats.items() if v)

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, dict[str, Any]]:
        if feature != "watchlist" or not feat_watchlist:
            return {}
        return feat_watchlist.build_index(self)

    def add(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        lst = list(items or [])
        if not lst:
            return {"ok": True, "count": 0}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        if feature != "watchlist" or not feat_watchlist:
            return {"ok": True, "count": 0, "unresolved": []}
        if hasattr(feat_watchlist, "add_detailed"):
            res = feat_watchlist.add_detailed(self, lst)  # type: ignore[attr-defined]
            confirmed_keys = (res or {}).get("confirmed_keys") or []
            skipped_keys = (res or {}).get("skipped_keys") or []
            unresolved = (res or {}).get("unresolved") or []
            count = int((res or {}).get("confirmed", (res or {}).get("count", 0)) or 0)
            return {
                "ok": True,
                "count": int(count),
                "confirmed": int(count),
                "confirmed_keys": list(confirmed_keys) if isinstance(confirmed_keys, list) else [],
                "skipped_keys": list(skipped_keys) if isinstance(skipped_keys, list) else [],
                "unresolved": list(unresolved) if isinstance(unresolved, list) else [],
            }
        count, unresolved = feat_watchlist.add(self, lst)
        return {"ok": True, "count": int(count), "unresolved": unresolved}

    def remove(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        lst = list(items or [])
        if not lst:
            return {"ok": True, "count": 0}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        if feature != "watchlist" or not feat_watchlist:
            return {"ok": True, "count": 0, "unresolved": []}
        count, unresolved = feat_watchlist.remove(self, lst)
        return {"ok": True, "count": int(count), "unresolved": unresolved}


class _ANILISTOPS:
    def name(self) -> str:
        return "ANILIST"

    def label(self) -> str:
        return "AniList"

    def features(self) -> Mapping[str, bool]:
        return supported_features()

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "observed_deletes": False,
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        c = cfg or {}
        an = c.get("anilist") or {}
        au = (c.get("auth") or {}).get("anilist") or {}

        token = (
            an.get("access_token")
            or an.get("token")
            or (an.get("oauth") or {}).get("access_token")
            or (au.get("access_token") if isinstance(au, Mapping) else "")
            or (au.get("token") if isinstance(au, Mapping) else "")
            or (au.get("oauth") or {}).get("access_token")
            or ""
        )
        return bool(str(token).strip())

    def _adapter(self, cfg: Mapping[str, Any]) -> ANILISTModule:
        return ANILISTModule(cfg)

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

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()


OPS = _ANILISTOPS()
