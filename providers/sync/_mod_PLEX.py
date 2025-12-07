# /providers/sync/_mod_PLEX.py
# CrossWatch - Plex Sync Module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

__VERSION__ = "2.1.0"
__all__ = ["get_manifest", "PLEXModule", "PLEXClient", "PLEXError", "PLEXAuthError", "PLEXNotFound", "OPS"]

try:
    from plexapi.myplex import MyPlexAccount
    from plexapi.server import PlexServer
except Exception as e:
    raise RuntimeError("plexapi is required for _mod_PLEX") from e

from .plex._common import configure_plex_context
from .plex._common import (
    normalize as plex_normalize,
    key_of as plex_key_of,
    plex_headers,
    DISCOVER,
)
from .plex._utils import (
    resolve_user_scope,
    patch_history_with_account_id,
)
from ._mod_common import (
    build_session,
    request_with_retries,
    parse_rate_limit,
    label_plex,
    make_snapshot_progress,
)

try:  # type: ignore[name-defined]
    ctx  # type: ignore
except Exception:
    ctx = None  # type: ignore

try:
    from .plex import _watchlist as feat_watchlist
except Exception as e:
    feat_watchlist = None
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX] failed to import watchlist: {e}")

try:
    from .plex import _history as feat_history
except Exception as e:
    feat_history = None
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX] failed to import history: {e}")

try:
    from .plex import _ratings as feat_ratings
except Exception as e:
    feat_ratings = None
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX] failed to import ratings: {e}")

try:
    from .plex import _playlists as feat_playlists
except Exception as e:
    feat_playlists = None
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX] failed to import playlists: {e}")


class PLEXError(RuntimeError):
    pass


class PLEXAuthError(PLEXError):
    pass


class PLEXNotFound(PLEXError):
    pass


def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX] {msg}")


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "PLEX",
        "label": "Plex",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": {
            "watchlist": True,
            "history": True,
            "ratings": True,
            "playlists": False,
        },
        "requires": ["plexapi"],
        "capabilities": {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "watchlist": {"writes": "discover_first", "pms_fallback": True},
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True,
                "unrate": True,
                "from_date": False,
            },
        },
    }


@dataclass
class PLEXConfig:
    token: str | None = None
    baseurl: str | None = None
    client_id: str | None = None
    server_name: str | None = None
    machine_id: str | None = None
    username: str | None = None
    account_id: int | None = None
    password: str | None = None
    timeout: float = 10.0
    max_retries: int = 3
    watchlist_allow_pms_fallback: bool = True
    watchlist_page_size: int = 100


class PLEXClient:
    def __init__(self, cfg: PLEXConfig):
        self.cfg = cfg
        self.server: PlexServer | None = None
        self._account: MyPlexAccount | None = None
        self.session = build_session("PLEX", ctx, feature_label=label_plex)
        self.user_username: str | None = None
        self.user_account_id: int | None = None

    def connect(self) -> PLEXClient:
        try:
            if self.cfg.token:
                self._account = MyPlexAccount(token=self.cfg.token)
                _ = self._account.username
            elif self.cfg.username and self.cfg.password:
                self._account = MyPlexAccount(self.cfg.username, self.cfg.password)
                _ = self._account.username
            else:
                raise PLEXAuthError("Missing Plex auth (account token or username/password)")

            token = self.cfg.token or self._account.authenticationToken

            if self.cfg.baseurl:
                try:
                    self.server = PlexServer(self.cfg.baseurl, token, timeout=self.cfg.timeout)
                    server = self.server
                    if server is not None:
                        try:
                            server._session = self.session  # type: ignore[attr-defined]
                        except Exception:
                            pass
                except Exception as e:
                    _log(f"PMS baseurl connect failed: {e}; continuing account-only")
                self._post_connect_user_scope(token)
                return self

            try:
                res = self._pick_resource(self._account)
                self.server = res.connect(timeout=self.cfg.timeout)  # type: ignore[assignment]
                server = self.server
                if server is not None:
                    try:
                        server._session = self.session  # type: ignore[attr-defined]
                    except Exception:
                        pass
            except Exception as e:
                _log(f"No PMS resource bound: {e}; running account-only")
                self._post_connect_user_scope(token)
                return self

            self._post_connect_user_scope(token)
            return self

        except Exception as e:
            msg = str(e).lower()
            if "unauthorized" in msg or "401" in msg:
                raise PLEXAuthError("Plex authorization failed") from e
            raise PLEXError(f"Plex connect failed: {e}") from e

    def _pick_resource(self, acc: MyPlexAccount):
        servers = [r for r in acc.resources() if "server" in (r.provides or "")]
        if self.cfg.machine_id:
            for r in servers:
                if (r.clientIdentifier or "").lower() == self.cfg.machine_id.lower():
                    return r
        if self.cfg.server_name:
            for r in servers:
                if (r.name or "").lower() == self.cfg.server_name.lower():
                    return r
        for r in servers:
            if getattr(r, "owned", False):
                return r
        if servers:
            return servers[0]
        raise PLEXNotFound("No Plex Media Server resource found")

    def _post_connect_user_scope(self, token: str) -> None:
        try:
            self.user_username, self.user_account_id = resolve_user_scope(
                self._account,
                self.server,
                token,
                self.cfg.username,
                self.cfg.account_id,
            )
            if self.cfg.account_id is None:
                patch_history_with_account_id(self.server, self.user_account_id)
        except Exception as e:
            _log(f"user scope init failed: {e}")

    def account(self) -> MyPlexAccount:
        if not self._account:
            raise PLEXAuthError("MyPlexAccount not available (need account token or login).")
        return self._account

    def ping(self) -> bool:
        try:
            _ = self.account().username
            return True
        except Exception as e:
            raise PLEXError(f"Plex ping failed: {e}") from e

    def libraries(self, types: Iterable[str] = ("movie", "show")):
        s = self.server
        if not s:
            return
        wanted = {t.lower() for t in types}
        for sec in s.library.sections():
            if (sec.type or "").lower() in wanted:
                yield sec

    def fetch_by_rating_key(self, rating_key: Any):
        s = self.server
        if not s:
            return None
        try:
            return s.fetchItem(int(rating_key))
        except Exception:
            return None

    def _retry(self, fn, *a, **kw):
        tries = self.cfg.max_retries
        for i in range(tries):
            try:
                return fn(*a, **kw)
            except Exception:
                if i >= tries - 1:
                    raise
                time.sleep(0.5 * (i + 1))

    @staticmethod
    def normalize(obj) -> dict[str, Any]:
        return plex_normalize(obj)

    @staticmethod
    def key_of(obj) -> str:
        return plex_key_of(obj)


_FEATURES: dict[str, Any] = {
    "watchlist": feat_watchlist,
    "history": feat_history,
    "ratings": feat_ratings,
    "playlists": feat_playlists,
}


def _features_flags() -> dict[str, bool]:
    return {
        "watchlist": "watchlist" in _FEATURES and _FEATURES["watchlist"] is not None,
        "history": "history" in _FEATURES and _FEATURES["history"] is not None,
        "ratings": "ratings" in _FEATURES and _FEATURES["ratings"] is not None,
        "playlists": "playlists" in _FEATURES and _FEATURES["playlists"] is not None,
    }


class PLEXModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.config = cfg
        plex_cfg = dict(cfg.get("plex") or {})
        baseurl = plex_cfg.get("baseurl") or plex_cfg.get("server_url")
        self.cfg = PLEXConfig(
            token=plex_cfg.get("account_token") or plex_cfg.get("token"),
            baseurl=baseurl,
            client_id=plex_cfg.get("client_id"),
            server_name=plex_cfg.get("server_name") or plex_cfg.get("server"),
            machine_id=plex_cfg.get("machine_id"),
            username=plex_cfg.get("username"),
            account_id=(
                int(plex_cfg["account_id"])
                if str(plex_cfg.get("account_id", "")).strip().isdigit()
                else None
            ),
            password=plex_cfg.get("password"),
            timeout=float(plex_cfg.get("timeout", cfg.get("timeout", 10.0))),
            max_retries=int(plex_cfg.get("max_retries", cfg.get("max_retries", 3))),
            watchlist_allow_pms_fallback=bool(plex_cfg.get("watchlist_allow_pms_fallback", True)),
            watchlist_page_size=int(plex_cfg.get("watchlist_page_size", 100)),
        )
        baseurl_norm = self.cfg.baseurl or ""
        token_norm = self.cfg.token or ""
        configure_plex_context(
            baseurl=baseurl_norm,
            token=token_norm,
        )

        if self.cfg.client_id:
            cid = str(self.cfg.client_id)
            os.environ.setdefault("PLEX_CLIENT_IDENTIFIER", cid)
            os.environ.setdefault("CW_PLEX_CID", cid)
            try:
                from .plex import _common as plex_common

                plex_common.CLIENT_ID = cid  # type: ignore[assignment]
            except Exception:
                pass

        self.client = PLEXClient(self.cfg).connect()
        self.progress_factory = (
            lambda feature, total=None, throttle_ms=300: make_snapshot_progress(
                ctx,
                dst="PLEX",
                feature=str(feature),
                total=total,
                throttle_ms=int(throttle_ms),
            )
        )

    @staticmethod
    def supported_features() -> dict[str, bool]:
        toggles = {
            "watchlist": True,
            "ratings": True,
            "history": True,
            "playlists": False,
        }
        present = _features_flags()
        return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}

    def _is_enabled(self, feature: str) -> bool:
        return bool(self.supported_features().get(feature, False))

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def ping(self) -> bool:
        return self.client.ping()

    def libraries(self, types: Iterable[str] = ("movie", "show")):
        return self.client.libraries(types)

    def normalize(self, obj) -> dict[str, Any]:
        return self.client.normalize(obj)

    def key_of(self, obj) -> str:
        return self.client.key_of(obj)

    def account(self) -> MyPlexAccount:
        return self.client.account()

    def health(self) -> Mapping[str, Any]:
        enabled = self.supported_features()
        token = self.cfg.token
        tmo = max(3.0, min(self.cfg.timeout, 10.0))

        import time as _t
        started = _t.perf_counter()

        wl_needed = bool(enabled.get("watchlist"))
        lib_needed = any(enabled.get(k) for k in ("history", "ratings", "playlists"))

        discover_ok = False
        discover_reason: str | None = None
        retry_after: int | None = None
        disc_code: int | None = None
        disc_rate: dict[str, int | None] = {"limit": None, "remaining": None, "reset": None}

        if wl_needed:
            if token:
                try:
                    url = f"{DISCOVER}/library/sections/watchlist/all"
                    r = request_with_retries(
                        self.client.session,
                        "GET",
                        url,
                        headers=plex_headers(token),  # <-- change is here
                        params={"limit": 1},
                        timeout=tmo,
                        max_retries=self.cfg.max_retries,
                    )
                    disc_code = r.status_code
                    disc_rate = parse_rate_limit(r.headers)
                    if r.status_code in (401, 403):
                        discover_reason = "unauthorized"
                    elif 200 <= r.status_code < 300:
                        discover_ok = True
                    else:
                        discover_reason = f"http:{r.status_code}"
                    ra = r.headers.get("Retry-After")
                    if ra:
                        try:
                            retry_after = int(ra)
                        except Exception:
                            pass
                except Exception as e:
                    discover_reason = f"exception:{e.__class__.__name__}"
            else:
                discover_reason = "no_token"

        pms_ok = False
        pms_reason: str | None = None
        pms_code: int | None = None
        if lib_needed:
            srv = getattr(self.client, "server", None)
            if srv:
                try:
                    session = getattr(srv, "_session", None)
                    if not session:
                        session = self.client.session
                    rr = request_with_retries(
                        session,
                        "GET",
                        srv.url("/identity"),
                        timeout=tmo,
                        max_retries=self.cfg.max_retries,
                    )
                    pms_code = rr.status_code
                    if rr.status_code in (401, 403):
                        pms_reason = "unauthorized"
                    elif rr.ok:
                        pms_ok = True
                    else:
                        pms_reason = f"http:{rr.status_code}"
                except Exception as e:
                    pms_reason = f"exception:{e.__class__.__name__}"
            else:
                pms_reason = "no_pms"

        latency_ms = int((_t.perf_counter() - started) * 1000)

        features = {
            "watchlist": discover_ok if wl_needed else False,
            "history": pms_ok if enabled.get("history") else False,
            "ratings": pms_ok if enabled.get("ratings") else False,
            "playlists": pms_ok if enabled.get("playlists") else False,
        }

        checks: list[bool] = []
        if wl_needed:
            checks.append(discover_ok)
        if lib_needed:
            checks.append(pms_ok)

        disc_auth_failed = wl_needed and (
            disc_code in (401, 403) or discover_reason == "unauthorized"
        )
        pms_auth_failed = lib_needed and (
            pms_code in (401, 403) or pms_reason == "unauthorized"
        )

        if not checks:
            status = "ok"
        elif all(checks):
            status = "ok"
        elif any(checks):
            status = "degraded"
        else:
            status = "auth_failed" if (disc_auth_failed or pms_auth_failed) else "down"

        ok = status in ("ok", "degraded")

        details: dict[str, Any] = {}
        if wl_needed:
            details["account"] = bool(token) and discover_ok
        if lib_needed:
            details["pms"] = pms_ok
        disabled_list = [k for k, v in enabled.items() if not v]
        if disabled_list:
            details["disabled"] = disabled_list

        reasons: list[str] = []
        if wl_needed and not discover_ok:
            reasons.append(f"watchlist:{discover_reason or 'down'}")
        if lib_needed and not pms_ok:
            missing = [f for f in ("history", "ratings", "playlists") if enabled.get(f)]
            if missing:
                reasons.append(f"{'+'.join(missing)}:{pms_reason or 'down'}")
        if reasons:
            details["reason"] = "; ".join(reasons)
        if retry_after is not None:
            details["retry_after_s"] = retry_after

        api = {
            "discover": {
                "status": disc_code if wl_needed else None,
                "retry_after": retry_after if wl_needed else None,
                "rate": disc_rate
                if wl_needed
                else {"limit": None, "remaining": None, "reset": None},
            },
            "pms": {"status": pms_code if lib_needed else None},
        }

        _log(
            f"health status={status} ok={ok} latency_ms={latency_ms} "
            f"user={self.client.user_username}@{self.client.user_account_id}"
        )
        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": features,
            "details": details,
            "api": api,
        }

    def feature_names(self) -> tuple[str, ...]:
        return tuple(k for k, v in self.supported_features().items() if v and k in _FEATURES)

    def build_index(self, feature: str, **kwargs) -> dict[str, dict[str, Any]]:
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _log(f"build_index skipped: feature disabled or missing: {feature}")
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
            _log(f"add skipped: feature disabled or missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _log(f"add skipped: feature module missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            cnt, unresolved = mod.add(self, lst)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
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
            _log(f"remove skipped: feature disabled or missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _log(f"remove skipped: feature module missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            cnt, unresolved = mod.remove(self, lst)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
        except Exception as e:
            return {"ok": False, "error": str(e)}


class _PlexOPS:
    def name(self) -> str:
        return "PLEX"

    def label(self) -> str:
        return "Plex"

    def features(self) -> Mapping[str, bool]:
        return PLEXModule.supported_features()

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "watchlist": {"writes": "discover_first", "pms_fallback": True},
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True,
                "unrate": True,
                "from_date": False,
            },
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        c = cfg or {}
        pl = c.get("plex") or {}
        au = (c.get("auth") or {}).get("plex") or {}
        account_token = (pl.get("account_token") or au.get("account_token") or "").strip()

        pms = pl.get("pms") or {}
        pms_url = (pms.get("url") or "").strip()
        pms_token = (pms.get("token") or "").strip()
        if not pms_token:
            pms_token = (pms.get("x_plex_token") or "").strip()

        return bool(account_token or (pms_url and pms_token))

    def _adapter(self, cfg: Mapping[str, Any]) -> PLEXModule:
        return PLEXModule(cfg)

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

OPS = _PlexOPS()