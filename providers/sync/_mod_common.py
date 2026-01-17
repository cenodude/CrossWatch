# /providers/sync/_mod_common.py
# CrossWatch common sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Mapping
from urllib.parse import parse_qs, urlparse

import requests

from ._log import log as cw_log

__VERSION__ = "0.2.0"
__all__ = [
    "HitSession",
    "make_emitter",
    "build_session",
    "parse_rate_limit",
    "safe_json",
    "request_with_retries",
    "make_snapshot_progress",
    "SnapshotProgress",
    "label_simkl",
    "label_trakt",
    "label_plex",
    "label_jellyfin",
    "label_emby",
]

EmitFn = Callable[[str, Mapping[str, Any]], None]
FeatureLabelFn = Callable[[str, str, Mapping[str, Any]], str]


def _safe_url(url: str) -> str:
    try:
        p = urlparse(url)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}{p.path}"
        return p.path or str(url)
    except Exception:
        return str(url)


def _http_log(
    session: requests.Session,
    level: str,
    event: str,
    *,
    method: str,
    url: str,
    api_feature: str | None = None,
    status: int | None = None,
    attempt: int | None = None,
    max_retries: int | None = None,
    wait_s: float | None = None,
    dur_ms: int | None = None,
    error: str | None = None,
    **fields: Any,
) -> None:
    provider = getattr(session, "_provider", None)
    if not provider:
        return
    p = str(provider)
    cw_log(
        p,
        "http",
        level,
        event,
        method=str(method).upper(),
        url=_safe_url(url),
        api_feature=str(api_feature) if api_feature else None,
        status=int(status) if status is not None else None,
        attempt=int(attempt) if attempt is not None else None,
        max_retries=int(max_retries) if max_retries is not None else None,
        wait_s=float(wait_s) if wait_s is not None else None,
        dur_ms=int(dur_ms) if dur_ms is not None else None,
        error=str(error) if error else None,
        **fields,
    )


def make_emitter(ctx: Any) -> EmitFn:
    emit_fn: Callable[..., Any] | None = None
    try:
        if hasattr(ctx, "emit") and callable(getattr(ctx, "emit")):
            emit_fn = getattr(ctx, "emit")
        elif hasattr(ctx, "_emit") and callable(getattr(ctx, "_emit")):
            emit_fn = getattr(ctx, "_emit")
        elif callable(ctx):
            emit_fn = ctx
    except Exception:
        emit_fn = None

    def _emit(event: str, payload: Mapping[str, Any]) -> None:
        if not emit_fn:
            return
        try:
            try:
                emit_fn(event, **dict(payload))
            except TypeError:
                emit_fn(event, dict(payload))
        except Exception:
            pass

    return _emit

class SnapshotProgress:
    def __init__(
        self,
        ctx: Any,
        *,
        dst: str,
        feature: str,
        total: int | None = None,
        throttle_ms: int = 300,
    ):
        self._emit = make_emitter(ctx)
        self.dst = str(dst)
        self.feature = str(feature)
        self.total = int(total) if total is not None else None
        self._last_done = 0
        self._last_emitted = 0
        self._step = 100
        self._throttle_ms = int(throttle_ms)

    def tick(
        self,
        done: int,
        *,
        total: int | None = None,
        ok: bool | None = None,
        force: bool = False,
    ) -> None:
        t = self.total if total is None else total

        try:
            d = int(done or 0)
        except Exception:
            d = 0

        if not force:
            try:
                if int(d or 0) == 0 and int(t or 0) == 0:
                    self._last_done = 0
                    self._last_emitted = 0
                    return
            except Exception:
                pass

        self._last_done = max(self._last_done, d)

        if not force:
            if t is not None:
                try:
                    t_int = int(t)
                except Exception:
                    t_int = None
                else:
                    if d < self._last_emitted + self._step and d < t_int:
                        return
            else:
   
                if d < self._last_emitted + self._step:
                    return

        self._last_emitted = self._last_done

        payload: dict[str, Any] = {
            "dst": self.dst,
            "feature": self.feature,
            "done": int(self._last_done),
        }
        if t is not None:
            try:
                payload["total"] = int(t)
            except Exception:
                pass
        if ok is not None:
            payload["ok"] = bool(ok)
        self._emit("snapshot:progress", payload)

    def done(self, *, ok: bool | None = True, total: int | None = None) -> None:
        t = self.total if total is None else total

        payload: dict[str, Any] = {
            "dst": self.dst,
            "feature": self.feature,
            "done": int(self._last_done),
            "final": True,
        }
        if t is not None:
            try:
                payload["total"] = int(t)
            except Exception:
                pass
        if ok is not None:
            payload["ok"] = bool(ok)

        self._emit("snapshot:progress", payload)

def make_snapshot_progress(
    ctx: Any,
    *,
    dst: str,
    feature: str,
    total: int | None = None,
    throttle_ms: int = 300,
) -> SnapshotProgress:
    return SnapshotProgress(ctx, dst=dst, feature=feature, total=total, throttle_ms=throttle_ms)

def _get_query_value(url: str, params: Mapping[str, Any], name: str) -> str | None:
    qd = parse_qs(urlparse(url).query)
    v = params.get(name) if isinstance(params, Mapping) else None
    if isinstance(v, (list, tuple)):
        v = v[0] if v else None
    return (str(v) if v else None) or (qd.get(name, [None])[0])


def default_feature_label(
    provider: str,
    method: str,
    url: str,
    kw: Mapping[str, Any],
) -> str:
    p = urlparse(url)
    segs = [s for s in (p.path or "/").split("/") if s]
    head = "/".join(segs[:3]) or "unknown"
    return head.lower()


def label_emby(method: str, url: str, kw: Mapping[str, Any]) -> str:
    p = urlparse(url)
    segs = [s for s in (p.path or "/").split("/") if s]
    m = method.upper()

    if segs[:2] == ["System", "Ping"]:
        return "system:ping"
    if segs[:2] == ["System", "Info"]:
        return "system:info"

    if len(segs) >= 2 and segs[0] == "Users":
        if len(segs) >= 3 and segs[2] == "Views":
            return "library:views"
        if len(segs) >= 3 and segs[2] == "Items":
            if len(segs) >= 5 and segs[4] == "UserData":
                return "userdata"
            return "library:items"
        if "FavoriteItems" in segs:
            return "ratings:favorite"
        if "PlayedItems" in segs:
            return "history:add" if m == "POST" else ("history:remove" if m == "DELETE" else "history")

    if segs[:1] == ["Playlists"]:
        if m == "GET":
            return "playlists:index"
        if m == "POST":
            return "playlists:write"
        if m == "DELETE":
            return "playlists:delete"
        return "playlists"

    if segs[:1] == ["Collections"]:
        if m == "POST":
            return "collections:write"
        if m == "DELETE":
            return "collections:delete"
        return "collections"

    return default_feature_label("EMBY", method, url, kw)


def label_simkl(method: str, url: str, kw: Mapping[str, Any]) -> str:
    p = urlparse(url)
    segs = [s for s in (p.path or "/").split("/") if s]
    params = kw.get("params") or {}
    q_type = _get_query_value(url, params, "type")
    has_eps_watched = str(_get_query_value(url, params, "episode_watched_at") or "").lower() in ("1", "true", "yes", "y")

    if segs[:2] == ["sync", "activities"]:
        return "activities"
    if segs[:2] == ["sync", "all-items"]:
        bucket = (segs[2] if len(segs) >= 3 and segs[2] in ("movies", "shows", "anime") else None) or q_type
        feature = "history" if has_eps_watched else "watchlist"
        return f"{feature}:index:{bucket}" if bucket else f"{feature}:index"
    if segs[:2] == ["sync", "add-to-list"]:
        return "watchlist:add"
    if segs[:2] == ["sync", "history"]:
        if len(segs) >= 3 and segs[2] == "remove":
            return "history:remove"
        return "history:add" if method.upper() == "POST" else "history:index"
    if segs[:2] == ["sync", "ratings"]:
        if len(segs) >= 3 and segs[2] == "remove":
            return "ratings:remove"
        return "ratings:add" if method.upper() == "POST" else "ratings:index"

    return default_feature_label("SIMKL", method, url, kw)


def label_trakt(method: str, url: str, kw: Mapping[str, Any]) -> str:
    p = urlparse(url)
    segs = [s for s in (p.path or "/").split("/") if s]
    if segs[:2] == ["sync", "last_activities"]:
        return "activities"
    if segs[:2] == ["sync", "watchlist"]:
        if len(segs) >= 3 and segs[2] in ("movies", "shows", "seasons", "episodes"):
            return f"watchlist:index:{segs[2]}"
        return "watchlist:add" if method.upper() == "POST" and len(segs) == 2 else "watchlist:index"
    if segs[:2] == ["sync", "history"]:
        return "history:remove" if (len(segs) == 3 and segs[2] == "remove") else "history:add"
    if segs[:2] == ["sync", "ratings"]:
        return "ratings:index"
    return default_feature_label("TRAKT", method, url, kw)


def label_plex(method: str, url: str, kw: Mapping[str, Any]) -> str:
    p = urlparse(url)
    segs = [s for s in (p.path or "/").split("/") if s]
    if segs[:2] == ["status", "sessions"]:
        return "sessions"
    if segs[:2] == ["library", "sections"]:
        return "library:sections"
    if segs[:1] == [":"] and len(segs) >= 2 and segs[1] in ("scrobble", "unscrobble"):
        return "history:write"
    if segs[:1] == ["playlists"]:
        m = method.upper()
        if m == "GET":
            return "playlists:index"
        if m == "POST":
            return "playlists:create"
        if m == "DELETE":
            return "playlists:delete"
        if m == "PUT":
            return "playlists:update"
    return default_feature_label("PLEX", method, url, kw)


def label_jellyfin(method: str, url: str, kw: Mapping[str, Any]) -> str:
    p = urlparse(url)
    segs = [s for s in (p.path or "/").split("/") if s]
    if len(segs) >= 3 and segs[:2] == ["Users", segs[1]] and segs[2] == "Views":
        return "library:views"
    if len(segs) >= 3 and segs[:2] == ["Users", segs[1]] and segs[2] == "Items":
        return "library:items"
    if "FavoriteItems" in segs:
        return "ratings:favorite"
    if "PlayedItems" in segs:
        m = method.upper()
        if m == "POST":
            return "history:add"
        if m == "DELETE":
            return "history:remove"
        return "history"
    if "Episodes" in segs:
        return "shows:episodes"
    return default_feature_label("JELLYFIN", method, url, kw)


class HitSession(requests.Session):
    def __init__(
        self,
        provider: str,
        emit: EmitFn,
        feature_label: FeatureLabelFn | None = None,
        emit_hits: bool | None = None,
    ):
        super().__init__()
        self._provider = provider
        self._emit = emit
        self._label = feature_label or (lambda m, u, kw: default_feature_label(provider, m, u, kw))
        self._emit_hits = bool(os.getenv("CW_API_HITS")) if emit_hits is None else bool(emit_hits)

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        try:
            resp = super().request(method, url, **kwargs)
            return resp
        finally:
            try:
                feature = self._label(method.upper(), url, kwargs)
            except Exception:
                feature = "unknown"
            if self._emit_hits:
                try:
                    self._emit("api:hit", {"provider": self._provider, "feature": feature})
                except Exception:
                    pass


def build_session(
    provider: str,
    ctx: Any,
    *,
    feature_label: FeatureLabelFn | None = None,
    emit_hits: bool | None = None,
) -> HitSession:
    return HitSession(provider, make_emitter(ctx), feature_label, emit_hits)


def parse_rate_limit(h: Mapping[str, Any]) -> dict[str, int | None]:
    def _i(x: Any) -> int | None:
        try:
            return int(x)
        except Exception:
            return None

    return {
        "limit": _i(h.get("X-RateLimit-Limit") or h.get("RateLimit-Limit") or h.get("Ratelimit-Limit")),
        "remaining": _i(h.get("X-RateLimit-Remaining") or h.get("RateLimit-Remaining") or h.get("Ratelimit-Remaining")),
        "reset": _i(h.get("X-RateLimit-Reset") or h.get("RateLimit-Reset") or h.get("Ratelimit-Reset")),
    }


def safe_json(resp: requests.Response) -> Any:
    try:
        if not (resp.text or "").strip():
            return {}
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "json" in ctype:
            return resp.json()
        return json.loads(resp.text)
    except Exception:
        return {}


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: float = 10.0,
    max_retries: int = 3,
    retry_on: tuple[int, ...] = (429, 500, 502, 503, 504),
    backoff_base: float = 0.5,
    **kwargs: Any,
) -> requests.Response:
    api_feature: str | None = None
    try:
        label = getattr(session, "_label", None)
        if callable(label):
            api_feature = str(label(str(method).upper(), str(url), dict(kwargs)))
    except Exception:
        api_feature = None

    last: Any = None
    for i in range(max(1, int(max_retries))):
        try:
            t0 = time.monotonic()
            resp = session.request(method, url, timeout=timeout, **kwargs)
            dur_ms = int((time.monotonic() - t0) * 1000)
            if resp.status_code in retry_on and i < max_retries - 1:
                wait = backoff_base * (2**i)
                retry_after: float | None = None
                try:
                    if resp.status_code == 429:
                        ra = resp.headers.get("Retry-After")
                        if ra:
                            retry_after = float(ra)
                            wait = max(wait, retry_after)
                except Exception:
                    pass

                if resp.status_code == 429:
                    rl = parse_rate_limit(resp.headers)
                    _http_log(
                        session,
                        "warn",
                        "rate_limit_retry",
                        method=method,
                        url=url,
                        api_feature=api_feature,
                        status=resp.status_code,
                        attempt=i + 1,
                        max_retries=max_retries,
                        wait_s=wait,
                        dur_ms=dur_ms,
                        retry_after_s=retry_after,
                        rate_limit_limit=rl.get("limit"),
                        rate_limit_remaining=rl.get("remaining"),
                        rate_limit_reset=rl.get("reset"),
                    )
                else:
                    _http_log(
                        session,
                        "warn",
                        "http_retry",
                        method=method,
                        url=url,
                        api_feature=api_feature,
                        status=resp.status_code,
                        attempt=i + 1,
                        max_retries=max_retries,
                        wait_s=wait,
                        dur_ms=dur_ms,
                    )

                time.sleep(wait)
                last = resp
                continue
            return resp
        except Exception as e:
            dur_ms: int | None = None
            try:
                dur_ms = int((time.monotonic() - t0) * 1000)
            except Exception:
                dur_ms = None
            last = e
            if i < max_retries - 1:
                wait = backoff_base * (2**i)
                _http_log(
                    session,
                    "warn",
                    "http_exception_retry",
                    method=method,
                    url=url,
                    api_feature=api_feature,
                    attempt=i + 1,
                    max_retries=max_retries,
                    wait_s=wait,
                    dur_ms=dur_ms,
                    error=f"{type(e).__name__}: {e}",
                )
                time.sleep(wait)
            else:
                _http_log(
                    session,
                    "error",
                    "http_exception_giveup",
                    method=method,
                    url=url,
                    api_feature=api_feature,
                    attempt=i + 1,
                    max_retries=max_retries,
                    dur_ms=dur_ms,
                    error=f"{type(e).__name__}: {e}",
                )
                break
    if isinstance(last, requests.Response):
        return last
    msg = f"request failed after retries: {method} {url}"
    if last is not None:
        msg = f"{msg} ({type(last).__name__}: {last})"
    _http_log(session, "error", "http_request_failed", method=method, url=url, api_feature=api_feature, max_retries=max_retries, error=msg)
    raise requests.RequestException(msg)


request_with_retry = request_with_retries
