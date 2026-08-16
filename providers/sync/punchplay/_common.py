# /providers/sync/punchplay/_common.py
# Shared helpers for PunchPlay sync modules
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import requests

from .._log import log as cw_log
from providers.auth._auth_PUNCHPLAY import (
    PLATFORM_BASE,
    is_configured as auth_configured,
    request_with_auth as punchplay_request_with_auth,
)

STATE_DIR = Path("/config/.cw_state")
try:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass

URL_SYNC_CHANGES = f"{PLATFORM_BASE}/me/sync/changes"
URL_SYNC_SNAPSHOT = f"{PLATFORM_BASE}/me/sync/snapshot"
URL_BULK = f"{PLATFORM_BASE}/sync/{{resource}}"
URL_LISTS = f"{PLATFORM_BASE}/me/lists"
URL_LIST_DETAILS = f"{PLATFORM_BASE}/lists/{{list_id}}"
URL_LIST_ITEMS = f"{PLATFORM_BASE}/lists/{{list_id}}/items"
URL_HISTORY = f"{PLATFORM_BASE}/me/history"
URL_WATCH_HISTORY = f"{PLATFORM_BASE}/watch-history/{{entry_id}}"
URL_TITLE_HISTORY = f"{PLATFORM_BASE}/title/{{kind}}/{{title_id}}/history"
URL_RATINGS = f"{PLATFORM_BASE}/me/ratings"
URL_FAVOURITES = f"{PLATFORM_BASE}/me/favourites"
URL_WATCH_STATUS = f"{PLATFORM_BASE}/me/watch-status"
URL_IN_PROGRESS = f"{PLATFORM_BASE}/playback/in-progress"
URL_IN_PROGRESS_ITEM = f"{PLATFORM_BASE}/playback/in-progress/{{entry_id}}"
URL_CONTINUE_WATCHING = f"{PLATFORM_BASE}/me/continue-watching"
URL_PLAYBACK = f"{PLATFORM_BASE}/playback/{{action}}"

SNAPSHOT_RESOURCES = ("history", "interaction", "list", "list_item", "collection", "playback")
PLAYBACK_ACTIONS = ("start", "pause", "resume", "stop", "progress")

BULK_MAX_ITEMS = 100
BULK_MAX_BYTES = 512 * 1024
PAGE_MAX = 500
HISTORY_PAGE_MAX = 100
LIST_PAGE_MAX = 500
LIST_SUMMARY_PAGE_MAX = 100

DEFAULT_GET_PER_SEC = 2.0
DEFAULT_POST_PER_SEC = 0.5
DEFAULT_TIMEOUT = 20.0
DEFAULT_MAX_RETRIES = 3

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}

RATE_BUDGETS: dict[str, tuple[int, float]] = {
    "bulk": (30, 60.0),
    "playback": (120, 300.0),
    "sync_read": (120, 60.0),
    "history_read": (120, 60.0),
    "lists_read": (120, 60.0),
    "calendar": (60, 60.0),
    "token_read": (300, 60.0),
    "token_write": (120, 60.0),
}

RATE_REMAINING_FLOOR = 2
RATE_MAX_SLEEP = 60.0


def _endpoint_bucket(method: str, url: str) -> str | None:
    m = str(method or "").upper()
    u = str(url or "")
    if "/sync/history" in u or "/sync/ratings" in u or "/sync/watchlist" in u:
        return "bulk"
    if "/playback/" in u and m == "POST":
        return "playback"
    if "/me/sync/changes" in u or "/me/sync/snapshot" in u:
        return "sync_read"
    if "/me/history" in u:
        return "history_read"
    if "/me/lists" in u or "/lists/" in u:
        return "lists_read"
    if "/calendar" in u:
        return "calendar"
    return None


class RateGovernor:
    def __init__(self, budgets: Mapping[str, tuple[int, float]] | None = None) -> None:
        self._lock = threading.Lock()
        self._budgets = dict(budgets or RATE_BUDGETS)
        self._hits: dict[str, list[float]] = {}
        self._until: dict[str, float] = {}

    def _buckets_for(self, method: str, url: str) -> list[str]:
        out: list[str] = []
        endpoint = _endpoint_bucket(method, url)
        if endpoint:
            out.append(endpoint)
        out.append("token_write" if str(method or "").upper() in ("POST", "PATCH", "DELETE", "PUT") else "token_read")
        return out

    def _wait_for(self, bucket: str, now: float) -> float:
        budget = self._budgets.get(bucket)
        blocked_until = self._until.get(bucket, 0.0)
        wait = max(0.0, blocked_until - now)
        if not budget:
            return wait
        limit, window = budget
        hits = self._hits.setdefault(bucket, [])
        cutoff = now - window
        while hits and hits[0] <= cutoff:
            hits.pop(0)
        if len(hits) >= limit:
            wait = max(wait, hits[0] + window - now)
        return wait

    def try_acquire(self, method: str, url: str) -> float:
        with self._lock:
            now = time.monotonic()
            buckets = self._buckets_for(method, url)
            wait = max((self._wait_for(b, now) for b in buckets), default=0.0)
            if wait > 0.0:
                return wait
            for b in buckets:
                if b in self._budgets:
                    self._hits.setdefault(b, []).append(now)
            return 0.0

    def acquire(self, method: str, url: str) -> float:
        slept = 0.0
        for _ in range(8):
            with self._lock:
                now = time.monotonic()
                buckets = self._buckets_for(method, url)
                wait = max((self._wait_for(b, now) for b in buckets), default=0.0)
                if wait <= 0.0:
                    for b in buckets:
                        if b in self._budgets:
                            self._hits.setdefault(b, []).append(now)
                    return slept
            pause = min(RATE_MAX_SLEEP, wait)
            time.sleep(pause)
            slept += pause
        return slept

    def observe(self, method: str, url: str, headers: Mapping[str, Any] | None) -> None:
        if not headers:
            return
        raw_remaining = headers.get("X-RateLimit-Remaining")
        if raw_remaining is None:
            return
        try:
            remaining = int(raw_remaining)
        except Exception:
            return
        if remaining > RATE_REMAINING_FLOOR:
            return
        try:
            reset = float(headers.get("X-RateLimit-Reset") or 0)
        except Exception:
            reset = 0.0
        delay = max(0.0, reset - time.time()) if reset else 1.0
        delay = min(RATE_MAX_SLEEP, delay)
        if delay <= 0.0:
            return
        with self._lock:
            until = time.monotonic() + delay
            for bucket in self._buckets_for(method, url):
                self._until[bucket] = max(self._until.get(bucket, 0.0), until)


_GOVERNORS: dict[str, RateGovernor] = {}
_GOVERNORS_GUARD = threading.Lock()


def budgets_from_cfg(section: Mapping[str, Any] | None) -> dict[str, tuple[int, float]]:
    out = dict(RATE_BUDGETS)
    rl = (section or {}).get("rate_limit")
    if not isinstance(rl, Mapping):
        return out
    overrides = (
        ("bulk", "bulk_per_min", 60.0),
        ("playback", "playback_per_5min", 300.0),
        ("sync_read", "sync_read_per_min", 60.0),
        ("history_read", "history_read_per_min", 60.0),
        ("lists_read", "lists_read_per_min", 60.0),
    )
    for bucket, key, window in overrides:
        raw = rl.get(key)
        if raw is None:
            continue
        try:
            limit = int(raw)
        except Exception:
            continue
        if limit > 0:
            out[bucket] = (limit, window)
    return out


def rate_governor(instance: Any = None, section: Mapping[str, Any] | None = None) -> RateGovernor:
    key = str(instance or "default")
    with _GOVERNORS_GUARD:
        gov = _GOVERNORS.get(key)
        if gov is None:
            gov = RateGovernor(budgets_from_cfg(section))
            _GOVERNORS[key] = gov
        return gov


class PunchPlayError(RuntimeError):
    pass


def _dbg(feature: str, msg: str, **fields: Any) -> None:
    cw_log("PUNCHPLAY", feature, "debug", msg, **fields)


def _info(feature: str, msg: str, **fields: Any) -> None:
    cw_log("PUNCHPLAY", feature, "info", msg, **fields)


def _warn(feature: str, msg: str, **fields: Any) -> None:
    cw_log("PUNCHPLAY", feature, "warn", msg, **fields)


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None


def _safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"


def state_file(name: str) -> Path:
    scope = _pair_scope()
    safe = _safe_scope(scope) if scope else "unscoped"
    p = Path(name)
    if p.suffix:
        return STATE_DIR / f"{p.stem}.{safe}{p.suffix}"
    return STATE_DIR / f"{name}.{safe}"


def read_json(path: Path, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return default


def write_json(path: Path, data: Any) -> None:
    try:
        tmp = Path(str(path) + ".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        tmp.replace(path)
    except Exception:
        pass


def cfg_section(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    if isinstance(cfg, dict) and isinstance(cfg.get("punchplay"), dict):
        return cfg["punchplay"]
    try:
        runtime_cfg = getattr(getattr(adapter, "cfg", None), "config", {}) or {}
        if isinstance(runtime_cfg, dict) and isinstance(runtime_cfg.get("punchplay"), dict):
            return runtime_cfg["punchplay"]
    except Exception:
        pass
    return {}


def instance_id(adapter: Any) -> str | None:
    inst = getattr(adapter, "instance_id", None)
    return str(inst) if inst else None


def has_auth(data: Mapping[str, Any]) -> bool:
    return auth_configured(data)


def cfg_int(data: Mapping[str, Any], key: str, default: int) -> int:
    raw = data.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def cfg_float(data: Mapping[str, Any], key: str, default: float) -> float:
    raw = data.get(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def cfg_bool(data: Mapping[str, Any], key: str, default: bool) -> bool:
    raw = data.get(key)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def iso_z(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            secs = float(value)
            if secs > 1e11:
                secs = secs / 1000.0
            return datetime.fromtimestamp(secs, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        norm = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(norm)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    except Exception:
        return None


def not_future(value: str | None) -> str | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return value
    now = datetime.now(timezone.utc)
    if dt > now:
        return now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return value


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = int(str(value).strip())
    except Exception:
        return None
    return out if out > 0 else None


def _as_imdb(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if not text.startswith("tt"):
        text = f"tt{text.lstrip('t')}"
    rest = text[2:]
    return text if rest.isdigit() and rest else None


def ids_for_punchplay(item: Mapping[str, Any]) -> dict[str, Any]:
    ids = item.get("ids") if isinstance(item.get("ids"), Mapping) else item
    out: dict[str, Any] = {}
    tmdb = _as_int((ids or {}).get("tmdb"))
    if tmdb:
        out["tmdb_id"] = tmdb
    imdb = _as_imdb((ids or {}).get("imdb"))
    if imdb:
        out["imdb_id"] = imdb
    tvdb = _as_int((ids or {}).get("tvdb"))
    if tvdb:
        out["tvdb_id"] = tvdb
    mal = _as_int((ids or {}).get("mal"))
    if mal:
        out["mal_id"] = mal
    return out


def has_write_id(payload: Mapping[str, Any]) -> bool:
    return any(payload.get(k) for k in ("tmdb_id", "imdb_id", "tvdb_id", "mal_id"))


def needs_client_item_id(payload: Mapping[str, Any]) -> bool:
    return not payload.get("tmdb_id")


def title_path_id(payload: Mapping[str, Any]) -> str | None:
    if payload.get("tmdb_id"):
        return str(payload["tmdb_id"])
    if payload.get("imdb_id"):
        return str(payload["imdb_id"])
    if payload.get("tvdb_id"):
        return f"tvdb:{payload['tvdb_id']}"
    if payload.get("mal_id"):
        return f"mal:{payload['mal_id']}"
    return None


def client_item_id(*parts: Any) -> str:
    raw = ":".join(str(p) for p in parts if str(p or "").strip())
    if len(raw) <= 200:
        return raw
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"cw:{digest}"[:200]


def idempotency_key(resource: str, payload: Mapping[str, Any], *, attempt: int = 0, operation_id: str = "") -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    seed = f"{resource}|{operation_id}|{attempt}|{body}"
    return f"cw-{hashlib.sha256(seed.encode('utf-8')).hexdigest()}"


class PunchPlayRateLimited(RuntimeError):
    def __init__(self, retry_after: float, bucket: str = "") -> None:
        super().__init__(f"rate_limited retry_after={int(retry_after)}s bucket={bucket or 'token'}")
        self.retry_after = int(max(1.0, retry_after))
        self.bucket = bucket or "token"


def punchplay_request(adapter: Any, method: str, url: str, **kwargs: Any) -> requests.Response:
    cfg = getattr(adapter, "config", None) or getattr(adapter, "raw_cfg", None) or {}
    session = getattr(adapter, "session", None)
    if session is None:
        client = getattr(adapter, "client", None)
        session = getattr(client, "session", None) or requests.Session()
    section = cfg_section(adapter)
    timeout = kwargs.pop("timeout", cfg_float(section, "timeout", DEFAULT_TIMEOUT))
    retries = kwargs.pop("max_retries", cfg_int(section, "max_retries", DEFAULT_MAX_RETRIES))
    inst = instance_id(adapter)

    gov = rate_governor(inst, section)
    if bool(kwargs.pop("no_wait", False)):
        wait = gov.try_acquire(method, url)
        if wait > 0.0:
            bucket = _endpoint_bucket(method, url) or "token"
            _warn("ratelimit", "client_throttle_refused", method=str(method).upper(), bucket=bucket, retry_after_s=int(wait))
            raise PunchPlayRateLimited(wait, bucket)
        slept = 0.0
    else:
        slept = gov.acquire(method, url)
    if slept > 0.25:
        _dbg("ratelimit", "client_throttle", method=str(method).upper(), bucket=_endpoint_bucket(method, url) or "token", slept_s=round(slept, 2))

    resp = punchplay_request_with_auth(
        session,
        method,
        url,
        cfg=cfg,
        instance_id=inst,
        timeout=timeout,
        max_retries=retries,
        **kwargs,
    )
    try:
        gov.observe(method, url, getattr(resp, "headers", None))
    except Exception:
        pass
    return resp


def error_of(resp: requests.Response) -> str:
    try:
        body = resp.json() or {}
    except Exception:
        return ""
    if not isinstance(body, Mapping):
        return ""
    return str(body.get("error") or "").strip()


def request_id_of(resp: requests.Response) -> str:
    try:
        return str(resp.headers.get("X-PunchPlay-Request-Id") or "").strip()
    except Exception:
        return ""


def retry_after_of(resp: requests.Response) -> float:
    try:
        return max(0.0, float(resp.headers.get("Retry-After") or 0))
    except Exception:
        return 0.0


def safe_json(resp: requests.Response) -> Any:
    try:
        if not (resp.text or "").strip():
            return None
        return resp.json()
    except Exception:
        return None


def chunk_items(items: Sequence[Mapping[str, Any]]) -> Iterator[list[Mapping[str, Any]]]:
    batch: list[Mapping[str, Any]] = []
    size = 2
    for item in items:
        try:
            encoded = len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) + 1
        except Exception:
            encoded = 512
        if batch and (len(batch) >= BULK_MAX_ITEMS or size + encoded > BULK_MAX_BYTES):
            yield batch
            batch = []
            size = 2
        batch.append(item)
        size += encoded
    if batch:
        yield batch


def snapshot_pages(adapter: Any, resource: str, *, feature: str, limit: int = PAGE_MAX) -> Iterator[list[Mapping[str, Any]]]:
    if resource not in SNAPSHOT_RESOURCES:
        raise PunchPlayError(f"unsupported snapshot resource: {resource}")
    after = 0
    page_size = max(1, min(int(limit or PAGE_MAX), PAGE_MAX))
    guard = 0
    while True:
        guard += 1
        if guard > 10000:
            _warn(feature, "snapshot_page_guard_tripped", resource=resource)
            return
        resp = punchplay_request(
            adapter,
            "GET",
            URL_SYNC_SNAPSHOT,
            params={"resource": resource, "after": after, "limit": page_size},
        )
        if resp.status_code != 200:
            _warn(feature, "snapshot_failed", resource=resource, status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
            return
        data = safe_json(resp) or {}
        if not isinstance(data, Mapping):
            return
        rows = data.get("items")
        rows = [r for r in rows if isinstance(r, Mapping)] if isinstance(rows, list) else []
        if rows:
            yield rows
        if not data.get("hasMore"):
            return
        nxt = data.get("nextAfter")
        try:
            nxt_int = int(nxt) if nxt is not None else None
        except Exception:
            nxt_int = None
        if nxt_int is None or nxt_int <= after:
            return
        after = nxt_int


def classify_bulk_results(
    results: Any,
    sent: Sequence[Mapping[str, Any]],
    key_by_index: Mapping[int, str],
) -> dict[str, Any]:
    confirmed: list[str] = []
    skipped_keys: list[str] = []
    unresolved_keys: list[str] = []
    deferred_keys: list[str] = []
    accepted_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    resolved_tmdb: dict[str, int] = {}
    status_counts: dict[str, int] = {}

    rows = results if isinstance(results, list) else []
    seen_index: set[int] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        raw_index = row.get("index")
        if raw_index is None:
            continue
        try:
            idx = int(raw_index)
        except Exception:
            continue
        seen_index.add(idx)
        key = key_by_index.get(idx) or str(row.get("client_item_id") or "").strip()
        status = str(row.get("status") or "").strip()
        if not key:
            continue
        if status:
            status_counts[status] = status_counts.get(status, 0) + 1
        if status in ("inserted", "updated", "removed"):
            confirmed.append(key)
            accepted_keys.append(key)
            tmdb = _as_int(row.get("resolved_tmdb_id"))
            if tmdb:
                resolved_tmdb[key] = tmdb
        elif status == "skipped":
            skipped_keys.append(key)
            accepted_keys.append(key)
            tmdb = _as_int(row.get("resolved_tmdb_id"))
            if tmdb:
                resolved_tmdb[key] = tmdb
        elif status == "deferred":
            deferred_keys.append(key)
            accepted_keys.append(key)
        else:
            unresolved_keys.append(key)
            unresolved.append({
                "key": key,
                "status": status or "invalid",
                "error": str(row.get("error") or "")[:200],
            })

    for idx, key in key_by_index.items():
        if idx not in seen_index and key:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_result"})

    return {
        "confirmed_keys": confirmed,
        "skipped_keys": skipped_keys,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": deferred_keys,
        "accepted_keys": accepted_keys,
        "unresolved": unresolved,
        "resolved_tmdb": resolved_tmdb,
        "status_counts": status_counts,
    }


def bulk_write(
    adapter: Any,
    resource: str,
    items: Sequence[Mapping[str, Any]],
    key_of: Any,
    *,
    feature: str,
) -> dict[str, Any]:
    if resource not in ("history", "ratings", "watchlist"):
        raise PunchPlayError(f"unsupported bulk resource: {resource}")

    confirmed: list[str] = []
    skipped_keys: list[str] = []
    unresolved_keys: list[str] = []
    deferred_keys: list[str] = []
    accepted_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    resolved_tmdb: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    ok = True

    for batch in chunk_items(items):
        payload = {"items": [dict(entry.get("payload") or {}) for entry in batch]}
        key_by_index = {i: str(entry.get("key") or "") for i, entry in enumerate(batch)}
        operation_id = f"{time.time_ns()}-{secrets.token_hex(8)}"

        attempt = 0
        waits = 0
        while True:
            headers = {"Idempotency-Key": idempotency_key(resource, payload, attempt=attempt, operation_id=operation_id)}
            resp = punchplay_request(
                adapter,
                "POST",
                URL_BULK.format(resource=resource),
                json=payload,
                headers=headers,
            )
            code = int(resp.status_code)
            err = error_of(resp)

            if code == 409 and err == "request_in_progress" and waits < 3:
                time.sleep(min(5.0, retry_after_of(resp) or 1.5 * (waits + 1)))
                waits += 1
                continue
            if code == 409 and err in ("idempotency_conflict", "idempotency_indeterminate") and attempt < 2:
                _warn(feature, "bulk_idempotency_retry", resource=resource, error=err, request_id=request_id_of(resp))
                attempt += 1
                continue
            break

        if not (200 <= code < 300):
            ok = False
            for entry in batch:
                key = str(entry.get("key") or "")
                if key:
                    unresolved_keys.append(key)
                    unresolved.append({"key": key, "status": f"http:{code}", "error": err})
            _warn(feature, "bulk_write_failed", resource=resource, status=code, error=err, request_id=request_id_of(resp), items=len(batch))
            continue

        body = safe_json(resp) or {}
        parsed = classify_bulk_results(
            body.get("results") if isinstance(body, Mapping) else None,
            [dict(e) for e in batch],
            key_by_index,
        )
        confirmed.extend(parsed["confirmed_keys"])
        skipped_keys.extend(parsed["skipped_keys"])
        unresolved_keys.extend(parsed["unresolved_keys"])
        deferred_keys.extend(parsed["deferred_keys"])
        accepted_keys.extend(parsed["accepted_keys"])
        unresolved.extend(parsed["unresolved"])
        resolved_tmdb.update(parsed["resolved_tmdb"])
        for status, count in (parsed.get("status_counts") or {}).items():
            status_counts[str(status)] = status_counts.get(str(status), 0) + int(count or 0)

        _dbg(
            feature,
            "bulk_write_batch",
            resource=resource,
            items=len(batch),
            inserted=body.get("inserted") if isinstance(body, Mapping) else None,
            updated=body.get("updated") if isinstance(body, Mapping) else None,
            removed=body.get("removed") if isinstance(body, Mapping) else None,
            skipped=body.get("skipped") if isinstance(body, Mapping) else None,
            deferred=body.get("deferred") if isinstance(body, Mapping) else None,
            invalid=body.get("invalid") if isinstance(body, Mapping) else None,
        )

    return {
        "ok": ok,
        "confirmed_keys": confirmed,
        "skipped_keys": skipped_keys,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": deferred_keys,
        "accepted_keys": accepted_keys,
        "unresolved": unresolved,
        "resolved_tmdb": resolved_tmdb,
        "status_counts": status_counts,
    }
