# /providers/sync/_mod_common.py
from __future__ import annotations
__VERSION__ = "0.2.0"
__all__ = [
    "HitSession",
    "make_emitter",
    "build_session",
    "parse_rate_limit",
    "safe_json",
    "request_with_retries",
    # progress
    "make_snapshot_progress",
    "SnapshotProgress",
    # Optional provider labelers
    "label_simkl",
    "label_trakt",
    "label_plex",
    "label_jellyfin",
    "label_emby",
]

from typing import Any, Callable, Mapping, Optional, Tuple
from urllib.parse import urlparse, parse_qs
import os, json, time, requests

EmitFn = Callable[[str, Mapping[str, Any]], None]
FeatureLabelFn = Callable[[str, str, Mapping[str, Any]], str]


# ---------- emit helpers ----------

def make_emitter(ctx: Any) -> EmitFn:
    """Return a emitter; prefers orchestrator style emit(event, **data), falls back to emit(event, data)."""
    emit_fn = None
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
            # Prefer kwargs style (event, **payload)
            try:
                emit_fn(event, **dict(payload))
            except TypeError:
                # Fallback: legacy (event, payload)
                emit_fn(event, dict(payload))
        except Exception:
            # Never blow up provider loops
            pass

    return _emit


# ---------- minimal snapshot progress ----------

class SnapshotProgress:
    """
    Tiny, throttled emitter for snapshot progress.
    Emits only: event='snapshot:progress' with {dst, feature, done, total?, ok?, final?}.
    """
    def __init__(self, ctx: Any, *, dst: str, feature: str, total: Optional[int] = None, throttle_ms: int = 300):
        self._emit = make_emitter(ctx)
        self.dst = str(dst)
        self.feature = str(feature)
        self.total = (int(total) if total is not None else None)
        self._last_ts = 0.0
        self._throttle = max(100, int(throttle_ms))
        self._last_done = 0

    def tick(self, done: int, *, total: Optional[int] = None, ok: Optional[bool] = None, force: bool = False) -> None:
        t = self.total if total is None else total
        if not force:
            try:
                if int(done or 0) == 0 and int(t or 0) == 0:
                    self._last_done = 0
                    return
            except Exception:
                pass
        now = time.monotonic()
        if not force and (now - self._last_ts) * 1000 < self._throttle:
            self._last_done = max(self._last_done, int(done))
            return
        self._last_ts = now
        self._last_done = max(self._last_done, int(done))
        payload = {
            "dst": self.dst,
            "feature": self.feature,
            "done": int(done),
        }
        if t is not None:
            try:
                payload["total"] = int(t)
            except Exception:
                pass
        if ok is not None:
            payload["ok"] = bool(ok)
        self._emit("snapshot:progress", payload)

    def done(self, *, ok: Optional[bool] = True, total: Optional[int] = None) -> None:
        payload = {
            "dst": self.dst,
            "feature": self.feature,
            "done": int(self._last_done),
            "final": True,
        }
        t = self.total if total is None else total
        if t is not None:
            try:
                payload["total"] = int(t)
            except Exception:
                pass
        if ok is not None:
            payload["ok"] = bool(ok)
        self._emit("snapshot:progress", payload)


def make_snapshot_progress(ctx: Any, *, dst: str, feature: str, total: Optional[int] = None, throttle_ms: int = 300) -> SnapshotProgress:
    """Convenience factory."""
    return SnapshotProgress(ctx, dst=dst, feature=feature, total=total, throttle_ms=throttle_ms)

# ---------- feature label (generic + provider-specific) ----------
def _get_query_value(url: str, params: Mapping[str, Any], name: str) -> Optional[str]:
    qd = parse_qs(urlparse(url).query)
    v = params.get(name) if isinstance(params, Mapping) else None
    if isinstance(v, (list, tuple)):
        v = v[0] if v else None
    return (str(v) if v else None) or (qd.get(name, [None])[0])

def default_feature_label(provider: str, method: str, url: str, kw: Mapping[str, Any]) -> str:
    p = urlparse(url)
    segs = [s for s in (p.path or "/").split("/") if s]
    head = "/".join(segs[:3]) or "unknown"
    return head.lower()

def label_emby(method: str, url: str, kw: Mapping[str, Any]) -> str:
    p = urlparse(url)
    segs = [s for s in (p.path or "/").split("/") if s]
    m = method.upper()

    # System
    if segs[:2] == ["System", "Ping"]:
        return "system:ping"
    if segs[:2] == ["System", "Info"]:
        return "system:info"

    # Users-scoped
    if len(segs) >= 2 and segs[0] == "Users":
        # /Users/{UserId}/Views
        if len(segs) >= 3 and segs[2] == "Views":
            return "library:views"
        # /Users/{UserId}/Items
        if len(segs) >= 3 and segs[2] == "Items":
            # /Users/{UserId}/Items/{ItemId}/UserData
            if len(segs) >= 5 and segs[4] == "UserData":
                return "userdata"
            return "library:items"
        # Favorites and Played
        if "FavoriteItems" in segs:
            return "ratings:favorite"
        if "PlayedItems" in segs:
            return "history:add" if m == "POST" else ("history:remove" if m == "DELETE" else "history")

    # Playlists
    if segs[:1] == ["Playlists"]:
        if m == "GET": return "playlists:index"
        if m == "POST": return "playlists:write"
        if m == "DELETE": return "playlists:delete"
        return "playlists"

    # Collections (BoxSets)
    if segs[:1] == ["Collections"]:
        if m == "POST": return "collections:write"
        if m == "DELETE": return "collections:delete"
        return "collections"

    return default_feature_label("EMBY", method, url, kw)

def label_simkl(method: str, url: str, kw: Mapping[str, Any]) -> str:
    p = urlparse(url)
    segs = [s for s in (p.path or "/").split("/") if s]
    params = kw.get("params") or {}
    q_type = _get_query_value(url, params, "type")
    has_eps_watched = str(_get_query_value(url, params, "episode_watched_at") or "").lower() in ("1","true","yes","y")

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
    p = urlparse(url); segs = [s for s in (p.path or "/").split("/") if s]
    if segs[:2] == ["sync", "last_activities"]:
        return "activities"
    if segs[:2] == ["sync", "watchlist"]:
        if len(segs) >= 3 and segs[2] in ("movies", "shows", "seasons", "episodes"):
            return f"watchlist:index:{segs[2]}"
        return "watchlist:add" if method == "POST" and len(segs) == 2 else "watchlist:index"
    if segs[:2] == ["sync", "history"]:
        return "history:remove" if (len(segs) == 3 and segs[2] == "remove") else "history:add"
    if segs[:2] == ["sync", "ratings"]:
        return "ratings:index"
    return default_feature_label("TRAKT", method, url, kw)

def label_plex(method: str, url: str, kw: Mapping[str, Any]) -> str:
    p = urlparse(url); segs = [s for s in (p.path or "/").split("/") if s]
    if segs[:2] == ["status", "sessions"]:
        return "sessions"
    if segs[:2] == ["library", "sections"]:
        return "library:sections"
    if segs[:1] == [":"] and len(segs) >= 2 and segs[1] in ("scrobble", "unscrobble"):
        return "history:write"
    if segs[:1] == ["playlists"]:
        if method == "GET": return "playlists:index"
        if method == "POST": return "playlists:create"
        if method == "DELETE": return "playlists:delete"
        if method == "PUT": return "playlists:update"
    return default_feature_label("PLEX", method, url, kw)

def label_jellyfin(method: str, url: str, kw: Mapping[str, Any]) -> str:
    p = urlparse(url); segs = [s for s in (p.path or "/").split("/") if s]
    if len(segs) >= 3 and segs[:2] == ["Users", segs[1]] and segs[2] == "Views":
        return "library:views"
    if len(segs) >= 3 and segs[:2] == ["Users", segs[1]] and segs[2] == "Items":
        return "library:items"
    if "FavoriteItems" in segs:
        return "ratings:favorite"
    if "PlayedItems" in segs:
        return "history:add" if method == "POST" else ("history:remove" if method == "DELETE" else "history")
    if "Episodes" in segs:
        return "shows:episodes"
    return default_feature_label("JELLYFIN", method, url, kw)

# ---------- instrumented session ----------
class HitSession(requests.Session):
    """Optional api:hit emission (off by default)."""
    def __init__(self, provider: str, emit: EmitFn, feature_label: Optional[FeatureLabelFn] = None, emit_hits: Optional[bool] = None):
        super().__init__()
        self._provider = provider
        self._emit = emit
        self._label = feature_label or (lambda m, u, kw: default_feature_label(provider, m, u, kw))
        self._emit_hits = bool(os.getenv("CW_API_HITS")) if emit_hits is None else bool(emit_hits)


    def request(self, method: str, url: str, **kwargs):
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

def build_session(provider: str, ctx: Any, *, feature_label: Optional[FeatureLabelFn] = None, emit_hits: Optional[bool] = None) -> HitSession:
    """Factory for a shared instrumented session."""
    return HitSession(provider, make_emitter(ctx), feature_label, emit_hits)

# ---------- tiny HTTP utils ----------
def parse_rate_limit(h: Mapping[str, Any]) -> Dict[str, Optional[int]]:
    """Normalize common rate headers across providers."""
    def _i(x):
        try:
            return int(x)
        except Exception:
            return None
    return {
        "limit":     _i(h.get("X-RateLimit-Limit") or h.get("RateLimit-Limit") or h.get("Ratelimit-Limit")),
        "remaining": _i(h.get("X-RateLimit-Remaining") or h.get("RateLimit-Remaining") or h.get("Ratelimit-Remaining")),
        "reset":     _i(h.get("X-RateLimit-Reset") or h.get("RateLimit-Reset") or h.get("Ratelimit-Reset")),
    }

def safe_json(resp: requests.Response) -> Any:
    """Lenient JSON parse."""
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
    retry_on: Tuple[int, ...] = (429, 500, 502, 503, 504),
    backoff_base: float = 0.5,
    **kwargs: Any,
) -> requests.Response:
    """Small resilient wrapper with 429-aware backoff."""
    last: Any = None
    for i in range(max(1, int(max_retries))):
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code in retry_on and i < max_retries - 1:
                wait = backoff_base * (2 ** i)
                try:
                    if resp.status_code == 429:
                        ra = resp.headers.get("Retry-After")
                        if ra:
                            wait = max(wait, float(ra))
                except Exception:
                    pass
                time.sleep(wait)
                last = resp
                continue
            return resp
        except Exception as e:
            last = e
            if i < max_retries - 1:
                time.sleep(backoff_base * (2 ** i))
            else:
                break
    if isinstance(last, requests.Response):
        return last
    raise requests.RequestException(f"request failed after retries: {method} {url}")

# Compatibility alias
request_with_retry = request_with_retries
