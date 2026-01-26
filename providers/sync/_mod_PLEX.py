# /providers/sync/_mod_PLEX.py
# CrossWatch - Plex Sync Module
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import os
import socket
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from ._log import log as cw_log

def _health(status: str, ok: bool, latency_ms: int) -> None:
    cw_log("PLEX", "health", "info", "health", latency_ms=latency_ms, ok=ok, status=status)


def _dbg(event: str, **fields: Any) -> None:
    cw_log("PLEX", "module", "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log("PLEX", "module", "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log("PLEX", "module", "warn", event, **fields)


def _error(event: str, **fields: Any) -> None:
    cw_log("PLEX", "module", "error", event, **fields)


def _log(msg: str) -> None:
    _dbg(msg)

__VERSION__ = "3.2.0"
__all__ = ["get_manifest", "PLEXModule", "PLEXClient", "PLEXError", "PLEXAuthError", "PLEXNotFound", "OPS"]

try:
    from plexapi.myplex import MyPlexAccount
    from plexapi.server import PlexServer
except Exception as e:
    raise RuntimeError("plexapi is required for _mod_PLEX") from e

try:
    import requests
except Exception as e:
    raise RuntimeError("requests is required for _mod_PLEX") from e

from .plex._common import configure_plex_context
from .plex._common import (
    normalize as plex_normalize,
    key_of as plex_key_of,
    plex_headers,
    DISCOVER,
)
from .plex._utils import resolve_user_scope
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
        _warn("feature_import_failed", feature="watchlist", error=str(e))

try:
    from .plex import _history as feat_history
except Exception as e:
    feat_history = None
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        _warn("feature_import_failed", feature="history", error=str(e))

try:
    from .plex import _ratings as feat_ratings
except Exception as e:
    feat_ratings = None
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        _warn("feature_import_failed", feature="ratings", error=str(e))

feat_playlists = None


class PLEXError(RuntimeError):
    pass


class PLEXAuthError(PLEXError):
    pass


class PLEXNotFound(PLEXError):
    pass


def _as_int(v: Any) -> int | None:
    try:
        if v is None or v is False or v is True:
            return None
        s = str(v).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        return None


def _stable_client_id() -> str:
    for k in ("CW_PLEX_CID", "PLEX_CLIENT_IDENTIFIER", "X_PLEX_CLIENT_IDENTIFIER"):
        v = os.environ.get(k)
        if v and v.strip():
            return v.strip()

    state_dir = "/config/.cw_state" if os.path.isdir("/config") else ".cw_state"
    try:
        os.makedirs(state_dir, exist_ok=True)
    except Exception:
        pass

    cid_path = os.path.join(state_dir, "plex_client_id.txt")
    try:
        if os.path.isfile(cid_path):
            with open(cid_path, "r", encoding="utf-8") as f:
                existing = (f.read() or "").strip()
            if existing:
                return existing
        new_id = f"crosswatch-{uuid.uuid4().hex[:16]}"
        with open(cid_path, "w", encoding="utf-8") as f:
            f.write(new_id)
        return new_id
    except Exception:
        host = socket.gethostname() or "host"
        return f"crosswatch-{host}"


def _plex_tv_client_id(cfg: Any) -> str:
    v = getattr(cfg, "client_id", None)
    if v:
        s = str(v).strip()
        if s:
            return s
    return _stable_client_id()


def _plex_tv_session(token: str, client_id: str) -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    s.headers.update(
        {
            "X-Plex-Token": token,
            "X-Plex-Client-Identifier": client_id,
            "X-Plex-Product": "CrossWatch",
            "X-Plex-Platform": "CrossWatch",
            "X-Plex-Version": __VERSION__,
            "Accept": "application/xml, application/json;q=0.9,*/*;q=0.8",
        }
    )
    return s


def _plex_tv_home_users(token: str, client_id: str, timeout: float = 10.0) -> list[dict[str, Any]]:
    s = _plex_tv_session(token, client_id)
    urls = ("https://plex.tv/api/v2/home/users", "https://plex.tv/api/home/users")
    last_err: Exception | None = None

    for url in urls:
        try:
            r = s.get(url, timeout=timeout)
            r.raise_for_status()
            ct = (r.headers.get("Content-Type") or "").lower()

            if "json" in ct:
                try:
                    data = r.json()
                    if isinstance(data, list):
                        return [u for u in data if isinstance(u, dict)]
                except Exception:
                    pass

            users: list[dict[str, Any]] = []
            root = ET.fromstring(r.text or "")
            for el in root.iter():
                if (el.tag or "").lower() == "user":
                    users.append(dict(el.attrib))
            if users:
                return users
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        raise last_err
    return []


def _plex_tv_switch_user(
    token: str,
    client_id: str,
    user_id: int,
    pin: str | None,
    timeout: float = 10.0,
) -> str | None:
    s = _plex_tv_session(token, client_id)
    url = f"https://plex.tv/api/home/users/{int(user_id)}/switch"
    params: dict[str, Any] = {}
    if pin:
        params["pin"] = str(pin)
    r = s.post(url, params=params, timeout=timeout)
    r.raise_for_status()
    root = ET.fromstring(r.text or "")
    return root.attrib.get("authenticationToken") or None


def _plex_tv_resource_access_token(
    token: str,
    client_id: str,
    machine_id: str,
    timeout: float = 10.0,
) -> str | None:
    if not token or not machine_id:
        return None
    s = _plex_tv_session(token, client_id)
    r = s.get(
        "https://plex.tv/api/resources",
        params={"includeHttps": 1, "includeRelay": 1, "includeIPv6": 1},
        timeout=timeout,
    )
    r.raise_for_status()
    root = ET.fromstring(r.text or "")
    mid = str(machine_id).strip().lower()
    for el in list(root):
        if (el.tag or "").lower() != "device":
            continue
        attrs = el.attrib or {}
        cid = (attrs.get("clientIdentifier") or attrs.get("machineIdentifier") or "").strip().lower()
        if cid and cid == mid:
            return attrs.get("accessToken") or None
    return None


def _pick_home_user(
    home_users: list[dict[str, Any]],
    target: str | None,
    target_id: int | None,
) -> dict[str, Any] | None:
    if target_id is not None:
        tid = int(target_id)
        for u in home_users:
            for k in ("id", "userId", "user_id", "accountId", "account_id"):
                uid = _as_int(u.get(k))
                if uid is not None and uid == tid:
                    return u

    if target:
        t = str(target).strip().lower()
        for u in home_users:
            for k in ("title", "username", "name", "email", "friendlyName"):
                v = u.get(k)
                if v and str(v).strip().lower() == t:
                    return u
    return None


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "PLEX",
        "label": "Plex",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": {"watchlist": True, "history": True, "ratings": True, "playlists": False},
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
    home_pin: str | None = None
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

        self.token_username: str | None = None
        self.token_account_id: int | None = None

        self.selected_username: str | None = None
        self.selected_account_id: int | None = None

        self._pms_token: str | None = None
        self._pms_baseurl: str | None = None

        self._home_users_cache: list[dict[str, Any]] | None = None
        self._home_users_cache_ts: float = 0.0
        self._token_stack: list[tuple[str | None, str | None, int | None]] = []

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
            self._pms_token = token

            cid = _plex_tv_client_id(self.cfg)
            self.session.headers.setdefault("X-Plex-Client-Identifier", cid)
            self.session.headers.setdefault("X-Plex-Product", "CrossWatch")
            self.session.headers.setdefault("X-Plex-Platform", "CrossWatch")
            self.session.headers.setdefault("X-Plex-Version", __VERSION__)
            self.session.headers.setdefault("Accept", "application/xml")
            self.session.headers["X-Plex-Token"] = token

            if self.cfg.baseurl:
                try:
                    self.server = PlexServer(self.cfg.baseurl, token, timeout=self.cfg.timeout)
                    self.server._session = self.session  # type: ignore[attr-defined]
                    self._pms_baseurl = str(getattr(self.server, "baseurl", None) or self.cfg.baseurl or "")
                    if self._pms_baseurl:
                        configure_plex_context(baseurl=str(self._pms_baseurl), token=token)
                except Exception as e:
                    _warn("pms_connect_failed", baseurl=str(self.cfg.baseurl or ""), error=str(e), mode="account_only")
                    self._post_connect_user_scope(token)
                    return self

                self._post_connect_user_scope(token)
                return self

            try:
                res = self._pick_resource(self._account)
                self.server = res.connect(timeout=self.cfg.timeout)  # type: ignore[assignment]
                self.server._session = self.session  # type: ignore[attr-defined]
                self._pms_baseurl = str(getattr(self.server, "baseurl", None) or "")
                if self._pms_baseurl:
                    configure_plex_context(baseurl=str(self._pms_baseurl), token=token)
            except Exception as e:
                _warn("pms_resource_connect_failed", error=str(e), mode="account_only")
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
            mid = self.cfg.machine_id.lower()
            for r in servers:
                if (r.clientIdentifier or "").lower() == mid:
                    return r
        if self.cfg.server_name:
            name = self.cfg.server_name.lower()
            for r in servers:
                if (r.name or "").lower() == name:
                    return r
        for r in servers:
            if getattr(r, "owned", False):
                return r
        if servers:
            return servers[0]
        raise PLEXNotFound("No Plex Media Server resource found")

    def _post_connect_user_scope(self, token: str) -> None:
        srv = self.server
        try:
            token_uname, token_aid = resolve_user_scope(self._account, srv, token, None, None)
        except Exception:
            token_uname, token_aid = (None, None)

        cfg_uname = (self.cfg.username or "").strip() or None
        cfg_aid = self.cfg.account_id
        try:
            if cfg_aid is not None and not cfg_uname:
                sel_uname, sel_aid = (None, int(cfg_aid))
            else:
                sel_uname, sel_aid = resolve_user_scope(self._account, srv, token, cfg_uname, cfg_aid)
        except Exception:
            sel_uname, sel_aid = (cfg_uname, cfg_aid)

        self.token_username = token_uname
        self.token_account_id = token_aid
        self.selected_username = sel_uname
        self.selected_account_id = sel_aid

        self.user_username = token_uname
        self.user_account_id = token_aid

        def _same_user(aid1: int | None, uname1: str | None, aid2: int | None, uname2: str | None) -> bool:
            try:
                if aid1 is not None and aid2 is not None and int(aid1) == int(aid2):
                    return True
            except Exception:
                pass
            if uname1 and uname2 and str(uname1).strip().lower() == str(uname2).strip().lower():
                return True
            return False

        if srv and _same_user(token_aid, token_uname, sel_aid, sel_uname):
            self.user_username = sel_uname or token_uname
            self.user_account_id = sel_aid or token_aid
        elif srv and (sel_aid is not None or sel_uname):
            _info("user_scope_selected", token_user=f"{token_uname}@{token_aid}", selected=f"{sel_uname}@{sel_aid}")

    def can_home_switch(self) -> bool:
        return bool(self.home_users())

    def home_users(self, *, force: bool = False) -> list[dict[str, Any]]:
        token = self._pms_token or self.cfg.token
        if not token:
            return []
        now = time.time()
        if not force and self._home_users_cache is not None and (now - self._home_users_cache_ts) < 30.0:
            return self._home_users_cache
        client_id = _plex_tv_client_id(self.cfg)
        try:
            users = _plex_tv_home_users(token, client_id, timeout=float(self.cfg.timeout))
        except Exception as e:
            _warn("home_users_fetch_failed", error=str(e))
            users = []
        self._home_users_cache = users
        self._home_users_cache_ts = now
        return users

    def is_home_user(self, *, target_username: str | None = None, target_account_id: int | None = None) -> bool:
        return _pick_home_user(self.home_users(), target=target_username, target_id=target_account_id) is not None

    def enter_home_user_scope(
        self,
        *,
        target_username: str | None = None,
        target_account_id: int | None = None,
        pin: str | None = None,
    ) -> bool:
        srv = self.server
        token = self._pms_token or self.cfg.token
        if not srv or not token:
            return False

        picked = _pick_home_user(self.home_users(), target=target_username, target_id=target_account_id)
        if not picked:
            return False

        user_id = _as_int(picked.get("id")) or 0
        if not user_id:
            return False

        protected = str(picked.get("protected") or "").strip().lower() in {"1", "true"}
        use_pin = (pin or "").strip() or None
        if protected and not use_pin:
            _info("home_switch_requires_pin", target=(picked.get("title") or target_username or user_id))
            return False

        client_id = _plex_tv_client_id(self.cfg)
        try:
            user_token = _plex_tv_switch_user(token, client_id, user_id=user_id, pin=use_pin, timeout=float(self.cfg.timeout))
        except Exception as e:
            hint = " (PIN?)" if use_pin else ""
            _warn("home_switch_failed", target=(picked.get("title") or target_username or user_id), hint=("PIN?" if use_pin else None), error=str(e))
            return False

        if not user_token:
            return False

        machine_id = (self.cfg.machine_id or "").strip() or None
        if not machine_id:
            try:
                machine_id = str(getattr(srv, "machineIdentifier", None) or "").strip() or None
            except Exception:
                machine_id = None
        if not machine_id:
            try:
                rr = request_with_retries(self.session, "GET", srv.url("/identity"), timeout=float(self.cfg.timeout), max_retries=1)
                if rr.ok:
                    root = ET.fromstring(rr.text or "")
                    machine_id = (root.attrib.get("machineIdentifier") or "").strip() or None
            except Exception:
                machine_id = None

        if not machine_id:
            _warn("home_scope_failed", reason="missing_machine_identifier")
            return False

        pms_user_token = user_token
        try:
            access_token = _plex_tv_resource_access_token(user_token, client_id, machine_id=machine_id, timeout=float(self.cfg.timeout))
            if access_token:
                pms_user_token = access_token
        except Exception:
            pass

        prev_token_raw = self.session.headers.get("X-Plex-Token")
        if isinstance(prev_token_raw, bytes):
            prev_token = prev_token_raw.decode("utf-8", "ignore")
        elif isinstance(prev_token_raw, str):
            prev_token = prev_token_raw
        else:
            prev_token = None

        self._token_stack.append((prev_token, self.user_username, self.user_account_id))
        self.session.headers["X-Plex-Token"] = pms_user_token

        try:
            srv._token = pms_user_token  # type: ignore[attr-defined]
        except Exception:
            pass
        try:
            sess = getattr(srv, "_session", None) or self.session
            sess.headers["X-Plex-Token"] = pms_user_token
        except Exception:
            pass

        try:
            baseurl = str(getattr(srv, "baseurl", None) or self._pms_baseurl or self.cfg.baseurl or "")
            if baseurl:
                configure_plex_context(baseurl=baseurl, token=pms_user_token)
        except Exception:
            pass

        try:
            rr = request_with_retries(self.session, "GET", srv.url("/library/sections"), timeout=float(self.cfg.timeout), max_retries=1)
            if rr.status_code in (401, 403):
                _warn("home_scope_failed", reason="token_rejected")
                self.exit_home_user_scope()
                return False
        except Exception as e:
            _warn("home_scope_verify_failed", error=str(e))
            self.exit_home_user_scope()
            return False

        self.user_username = (str(picked.get("title") or "").strip() or target_username)
        self.user_account_id = _as_int(picked.get("id")) or target_account_id
        _info("home_scope_entered", user=str(self.user_username), account_id=self.user_account_id)
        return True

    def exit_home_user_scope(self) -> None:
        if not self._token_stack:
            return
        prev_token, prev_uname, prev_aid = self._token_stack.pop()
        srv = self.server
        if prev_token:
            try:
                self.session.headers["X-Plex-Token"] = prev_token
            except Exception:
                pass
            if srv:
                try:
                    srv._token = prev_token  # type: ignore[attr-defined]
                except Exception:
                    pass
                try:
                    sess = getattr(srv, "_session", None) or self.session
                    sess.headers["X-Plex-Token"] = prev_token
                except Exception:
                    pass
            try:
                baseurl = str(getattr(srv, "baseurl", None) or self._pms_baseurl or self.cfg.baseurl or "")
                if baseurl:
                    configure_plex_context(baseurl=baseurl, token=prev_token)
            except Exception:
                pass
        self.user_username = prev_uname
        self.user_account_id = prev_aid
        _info("home_scope_exited")

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
            account_id=int(plex_cfg["account_id"]) if str(plex_cfg.get("account_id", "")).strip().isdigit() else None,
            home_pin=plex_cfg.get("home_pin"),
            password=plex_cfg.get("password"),
            timeout=float(plex_cfg.get("timeout", cfg.get("timeout", 10.0))),
            max_retries=int(plex_cfg.get("max_retries", cfg.get("max_retries", 3))),
            watchlist_allow_pms_fallback=bool(plex_cfg.get("watchlist_allow_pms_fallback", True)),
            watchlist_page_size=int(plex_cfg.get("watchlist_page_size", 100)),
        )

        configure_plex_context(baseurl=self.cfg.baseurl or "", token=self.cfg.token or "")

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
        toggles = {"watchlist": True, "ratings": True, "history": True, "playlists": False}
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
                        headers=plex_headers(token),
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
                    session = getattr(srv, "_session", None) or self.client.session
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

        disc_auth_failed = wl_needed and (disc_code in (401, 403) or discover_reason == "unauthorized")
        pms_auth_failed = lib_needed and (pms_code in (401, 403) or pms_reason == "unauthorized")

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
                "rate": disc_rate if wl_needed else {"limit": None, "remaining": None, "reset": None},
            },
            "pms": {"status": pms_code if lib_needed else None},
        }

        _health(status, ok, latency_ms)
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
            _info("feature_skipped", op="build_index", feature=feature, reason="disabled_or_missing")
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
            _info("feature_skipped", op="add", feature=feature, reason="disabled_or_missing")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _warn("feature_missing", op="add", feature=feature)
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            cnt, unresolved = mod.add(self, lst)
            attempted_keys: list[str] = []
            for it in lst:
                k = None
                if isinstance(it, dict):
                    k = it.get("key")
                if not k:
                    try:
                        k = plex_key_of(it)
                    except Exception:
                        k = None
                if k:
                    attempted_keys.append(str(k))
            unresolved_keys: set[str] = set()
            if isinstance(unresolved, list):
                for u in unresolved:
                    if isinstance(u, str):
                        unresolved_keys.add(u)
                    elif isinstance(u, dict):
                        uk = u.get("key")
                        if not uk:
                            try:
                                uk = plex_key_of(u)
                            except Exception:
                                uk = None
                        if uk:
                            unresolved_keys.add(str(uk))
            confirmed_keys = [k for k in attempted_keys if k not in unresolved_keys]
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
            _info("feature_skipped", op="remove", feature=feature, reason="disabled_or_missing")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _warn("feature_missing", op="remove", feature=feature)
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            cnt, unresolved = mod.remove(self, lst)
            attempted_keys: list[str] = []
            for it in lst:
                k = None
                if isinstance(it, dict):
                    k = it.get("key")
                if not k:
                    try:
                        k = plex_key_of(it)
                    except Exception:
                        k = None
                if k:
                    attempted_keys.append(str(k))
            unresolved_keys: set[str] = set()
            if isinstance(unresolved, list):
                for u in unresolved:
                    if isinstance(u, str):
                        unresolved_keys.add(u)
                    elif isinstance(u, dict):
                        uk = u.get("key")
                        if not uk:
                            try:
                                uk = plex_key_of(u)
                            except Exception:
                                uk = None
                        if uk:
                            unresolved_keys.add(str(uk))
            confirmed_keys = [k for k in attempted_keys if k not in unresolved_keys]
            return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": confirmed_keys}
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


OPS = _PlexOPS()
