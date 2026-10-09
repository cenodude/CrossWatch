# providers/sync/_mod_MYANIMELIST.py
# CrossWatch - MyAnimeList anime tracker provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import hashlib
import os
import threading
import time
from collections.abc import Mapping
from contextlib import contextmanager
from email.utils import parsedate_to_datetime
from typing import Any

from cw_platform.id_map import canonical_key, minimal
from cw_platform.provider_instances import normalize_instance_id
from providers.auth import _auth_MYANIMELIST as auth
from providers.auth.runtime import request_with_auth
from ._mod_common import build_op_result, build_session, _sleep_cancellable
from ._log import log
from .myanimelist import _history, _ratings, _watchlist

__VERSION__ = "0.1"
__all__ = ["get_manifest", "MYANIMELISTModule", "OPS"]
_FEATURES = {"watchlist": _watchlist, "ratings": _ratings, "history": _history}

API_URL = "https://api.myanimelist.net/v2"
STATUS_FIELDS = "status,score,num_episodes_watched,is_rewatching,start_date,finish_date,priority,num_times_rewatched,rewatch_value,tags,comments,updated_at"
FIELDS = "id,title,media_type,num_episodes,status,start_date"
LIST_FIELDS = FIELDS + ",list_status{" + STATUS_FIELDS + "}"
DETAIL_FIELDS = FIELDS + ",my_list_status{" + STATUS_FIELDS + "}"
_LOCK = threading.RLock()
_NEXT_REQUEST = 0.0
_ACCOUNT_LOCKS: dict[str, Any] = {}
_STATUSES = {"watching", "completed", "on_hold", "dropped", "plan_to_watch"}


def current_instance(cfg: Mapping[str, Any]) -> str:
    if cfg.get("_cw_provider_instance") is not None:
        return normalize_instance_id(cfg["_cw_provider_instance"])
    if (cfg.get("_cw_probe") or {}).get("instance"):
        return normalize_instance_id(cfg["_cw_probe"]["instance"])
    for name in ("PROBE", "PAIR_SRC", "PAIR_DST"):
        provider_key = "CW_PROBE_PROVIDER" if name == "PROBE" else f"CW_{name}"
        if str(os.getenv(provider_key) or "").upper() == "MYANIMELIST":
            return normalize_instance_id(os.getenv(f"CW_{name}_INSTANCE"))
    return "default"


def supported_features() -> dict[str, bool]:
    return {"watchlist": True, "ratings": True, "history": True, "progress": False, "playlists": False}


def capabilities() -> dict[str, Any]:
    titles = {"movies": True, "shows": True, "seasons": False, "episodes": False}
    return {"bidirectional": True, "provides_ids": True, "index_semantics": "present", "observed_deletes": False,
            "watchlist": {"types": titles, "remove": True},
            "ratings": {"types": titles, "upsert": True, "unrate": True, "from_date": False},
            "history": {"types": {"movies": True, "shows": False, "seasons": False, "episodes": True},
                        "upsert": False, "remove": True, "observed_deletes": False,
                        "rewatches": {"read": False, "write": False}}}


def get_manifest() -> dict[str, Any]:
    return {"name": "MYANIMELIST", "label": "MyAnimeList", "version": __VERSION__, "type": "sync", "experimental": True,
            "bidirectional": True, "features": supported_features(), "capabilities": capabilities(), "requires": []}


def retry_delay(value: str, attempt: int) -> float:
    try:
        return max(0.0, float(value))
    except (ValueError, TypeError):
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return float(2 ** attempt)


def transport(session: Any, method: str, url: str, *, max_retries: int = 1, **kwargs: Any) -> Any:
    global _NEXT_REQUEST
    for attempt in range(3):
        with _LOCK:
            delay = max(0.0, _NEXT_REQUEST - time.monotonic())
            if delay > 60:
                raise RuntimeError("MyAnimeList rate limit cooldown")
            _sleep_cancellable(delay)
            _NEXT_REQUEST = time.monotonic() + 1.0
            response = session.request(method, url, **kwargs)
            if response.status_code != 429:
                return response
            delay = retry_delay(response.headers.get("Retry-After", ""), attempt)
            _NEXT_REQUEST = max(_NEXT_REQUEST, time.monotonic() + delay)
        log("MYANIMELIST", "http", "warn", "rate_limit_retry", attempt=attempt + 1, wait_s=delay)
        if delay > 60 or attempt == 2:
            return response
    return response


class MyAnimeListClient:
    def __init__(self, cfg: Any, instance_id: str, ctx: Any = None):
        self.cfg = cfg
        self.instance_id = instance_id
        self.feature = "library"
        self.session = build_session("MYANIMELIST", ctx, feature_label=lambda method, url, kwargs: self.feature)
        block = auth.provider_block(cfg, instance_id)
        identity = str(block.get("user_id") or block.get("access_token") or instance_id)
        self.account_key = hashlib.sha256(identity.encode()).hexdigest()
        with _LOCK:
            self.write_lock = _ACCOUNT_LOCKS.setdefault(self.account_key, threading.RLock())
        self._snapshot: dict[str, tuple[dict[str, Any], dict[str, Any]]] | None = None
        self._media: dict[str, dict[str, Any]] = {}
        self._entries: dict[str, dict[str, Any] | None] | None = None
        self._shared: dict[str, Any] | None = None
        if ctx is not None:
            with _LOCK:
                if not hasattr(ctx, "_myanimelist_snapshots"):
                    ctx._myanimelist_snapshots = {}
                self._shared = ctx._myanimelist_snapshots

    def request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = request_with_auth("myanimelist", self.session, method, API_URL + path,
                                     cfg=self.cfg, instance_id=self.instance_id, request_func=transport, **kwargs)
        if response.status_code == 401:
            raise auth.MyAnimeListAuthError("MyAnimeList reconnect required")
        if method == "DELETE" and response.status_code in (200, 204, 404):
            return {}
        if response.status_code >= 300:
            raise RuntimeError(f"MyAnimeList HTTP {response.status_code}")
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError("MyAnimeList returned an invalid response")
        return data

    def user(self) -> dict[str, Any]:
        data = self.request("GET", "/users/@me")
        if not auth.valid_account(data):
            raise RuntimeError("MyAnimeList returned an invalid account")
        return data

    @staticmethod
    def validate_status(row: Any) -> dict[str, Any]:
        if (not isinstance(row, dict) or row.get("status") not in _STATUSES
                or type(row.get("score")) is not int or not 0 <= row["score"] <= 10
                or type(row.get("num_episodes_watched")) is not int or row["num_episodes_watched"] < 0):
            raise RuntimeError("MyAnimeList returned invalid list status")
        return row

    @staticmethod
    def validate_media(row: Any) -> dict[str, Any]:
        if (not isinstance(row, dict) or type(row.get("id")) is not int or row["id"] <= 0
                or not row.get("title") or not row.get("media_type")
                or type(row.get("num_episodes")) is not int or row["num_episodes"] < 0):
            raise RuntimeError("MyAnimeList returned invalid anime metadata")
        return row

    def entries(self) -> Any:
        with self.write_lock:
            if self._snapshot is None and self._shared is not None:
                self._snapshot = copy.deepcopy(self._shared.get(self.account_key))
            if self._snapshot is None:
                snapshot = {}
                offset = 0
                while True:
                    data = self.request("GET", "/users/@me/animelist", params={
                        "limit": 1000, "offset": offset, "fields": LIST_FIELDS, "nsfw": "true"})
                    rows = data.get("data")
                    if not isinstance(rows, list) or not isinstance(data.get("paging"), dict):
                        raise RuntimeError("MyAnimeList returned an incomplete library")
                    for row in rows:
                        if not isinstance(row, dict):
                            raise RuntimeError("MyAnimeList returned an invalid library row")
                        media = self.validate_media(row.get("node"))
                        entry = self.validate_status(row.get("list_status"))
                        ident = str(media["id"])
                        if ident in snapshot:
                            raise RuntimeError("MyAnimeList repeated a library page")
                        snapshot[ident] = (entry, media)
                    if not data["paging"].get("next"):
                        break
                    if not rows:
                        raise RuntimeError("MyAnimeList returned an empty continuation page")
                    offset += len(rows)
                self._snapshot = snapshot
                if self._shared is not None:
                    self._shared[self.account_key] = copy.deepcopy(snapshot)
            self._entries = {ident: copy.deepcopy(row[0]) for ident, row in self._snapshot.items()}
            return list(copy.deepcopy(self._snapshot).values())

    @contextmanager
    def library_batch(self, identifiers: Any):
        if list(identifiers):
            self.entries()
        yield

    def lookup(self, ident: str) -> dict[str, Any] | None:
        if self._entries is not None:
            return copy.deepcopy(self._entries.get(ident))
        return copy.deepcopy(self.media(ident).get("my_list_status"))

    def media(self, ident: str) -> dict[str, Any]:
        if self._snapshot is not None and ident in self._snapshot:
            return copy.deepcopy(self._snapshot[ident][1])
        if ident not in self._media:
            row = self.validate_media(self.request("GET", f"/anime/{ident}", params={"fields": DETAIL_FIELDS}))
            if str(row["id"]) != ident:
                raise RuntimeError("MyAnimeList returned the wrong anime")
            if row.get("my_list_status") is not None:
                self.validate_status(row["my_list_status"])
            self._media[ident] = row
        return copy.deepcopy(self._media[ident])

    def save(self, ident: str, entry: Any, attributes: dict[str, Any]) -> None:
        if entry is not None and all(entry.get(key) == value for key, value in attributes.items()):
            return
        payload = {("num_watched_episodes" if k == "num_episodes_watched" else k): v for k, v in attributes.items()}
        if self._shared is not None:
            self._shared.pop(self.account_key, None)
        try:
            saved = self.validate_status(self.request("PATCH", f"/anime/{ident}/my_list_status", data=payload))
            if any(saved.get(key) != value for key, value in attributes.items()):
                raise RuntimeError("MyAnimeList did not confirm the requested values")
        except Exception:
            self._snapshot = None
            self._entries = None
            self._media.pop(ident, None)
            raise
        if self._snapshot is not None:
            if ident in self._snapshot:
                self._snapshot[ident] = (saved, self._snapshot[ident][1])
            elif ident in self._media:
                self._snapshot[ident] = (saved, self._media[ident])
            else:
                self._snapshot = None
        if self._entries is not None:
            self._entries[ident] = saved
        if ident in self._media:
            self._media[ident]["my_list_status"] = saved

    def delete(self, ident: str) -> None:
        if self._shared is not None:
            self._shared.pop(self.account_key, None)
        try:
            self.request("DELETE", f"/anime/{ident}/my_list_status")
        except Exception:
            self._snapshot = None
            self._entries = None
            self._media.pop(ident, None)
            raise
        if self._entries is not None:
            self._entries[ident] = None
        if self._snapshot is not None:
            self._snapshot.pop(ident, None)
        self._media.pop(ident, None)


class MYANIMELISTModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.raw_cfg = cfg
        self.instance_id = current_instance(cfg)
        self.client = MyAnimeListClient(cfg, self.instance_id, globals().get("ctx"))

    supported_features = staticmethod(supported_features)
    manifest = staticmethod(get_manifest)
    normalize = staticmethod(minimal)
    key_of = staticmethod(canonical_key)

    def feature_names(self) -> tuple[str, ...]:
        return tuple(_FEATURES)

    def health(self) -> dict[str, Any]:
        start = time.monotonic()
        try:
            self.client.user()
            ok, reason = True, None
        except Exception as exc:
            ok, reason = False, "reconnect_required" if isinstance(exc, auth.MyAnimeListAuthError) else type(exc).__name__
        return {"ok": ok, "status": "ok" if ok else "down", "latency_ms": int((time.monotonic() - start) * 1000),
                "features": {k: bool(v and ok) for k, v in supported_features().items()}, "details": {"reason": reason}}

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, Any]:
        if feature not in _FEATURES:
            return {}
        self.client.feature = feature + ":index"
        result = _FEATURES[feature].build_index(self)
        log("MYANIMELIST", feature, "info", "index_done", count=len(result), source="live")
        return result

    def _write(self, feature: str, items: Any, action: str, dry_run: bool) -> dict[str, Any]:
        rows = list(items)
        if feature not in _FEATURES:
            return build_op_result(ok=False, unresolved=rows, error="unsupported_feature")
        if dry_run:
            return build_op_result(count=len(rows), dry_run=True)
        self.client.feature = feature + ":" + action
        with self.client.write_lock:
            return getattr(_FEATURES[feature], action)(self, rows)

    def add(self, feature: str, items: Any, *, dry_run: bool = False) -> dict[str, Any]:
        return self._write(feature, items, "add", dry_run)

    def remove(self, feature: str, items: Any, *, dry_run: bool = False) -> dict[str, Any]:
        return self._write(feature, items, "remove", dry_run)


class _MYANIMELISTOPS:
    def name(self) -> str:
        return "MYANIMELIST"

    def label(self) -> str:
        return "MyAnimeList"

    features = staticmethod(supported_features)
    capabilities = staticmethod(capabilities)

    def state_read_features(self) -> dict[str, bool]:
        return {feature: True for feature in _FEATURES}

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return auth.is_configured(auth.provider_block(cfg, current_instance(cfg)))

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return MYANIMELISTModule(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Any]:
        return MYANIMELISTModule(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Any, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return MYANIMELISTModule(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Any, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return MYANIMELISTModule(cfg).remove(feature, items, dry_run=dry_run)

    def order_remove_items(self, cfg: Mapping[str, Any], items: Any, *, feature: str) -> list[Any]:
        return _history.order_removals(cfg, items) if feature == "history" else list(items)


OPS = _MYANIMELISTOPS()
