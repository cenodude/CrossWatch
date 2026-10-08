# providers/sync/_mod_KITSU.py
# CrossWatch - Kitsu anime tracker provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any

from cw_platform.id_map import canonical_key, minimal
from cw_platform.provider_instances import normalize_instance_id, resolve_provider_block
from providers.auth import _auth_KITSU as auth
from providers.auth.runtime import request_with_auth
from ._mod_common import build_op_result, build_session
from ._log import log
from .kitsu import _history, _ratings, _watchlist

__VERSION__ = "0.1"
__all__ = ["get_manifest", "KITSUModule", "OPS"]
_FEATURES = {"watchlist": _watchlist, "ratings": _ratings, "history": _history}


def current_instance(cfg: Mapping[str, Any]) -> str:
    if cfg.get("_cw_provider_instance") is not None:
        return normalize_instance_id(cfg["_cw_provider_instance"])
    if (cfg.get("_cw_probe") or {}).get("instance"):
        return normalize_instance_id(cfg["_cw_probe"]["instance"])
    for name in ("PROBE", "PAIR_SRC", "PAIR_DST"):
        provider_key = "CW_PROBE_PROVIDER" if name == "PROBE" else f"CW_{name}"
        if str(os.getenv(provider_key) or "").upper() == "KITSU":
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
    return {"name": "KITSU", "label": "Kitsu", "version": __VERSION__, "type": "sync", "experimental": True,
            "bidirectional": True, "features": supported_features(), "capabilities": capabilities(), "requires": []}


class KitsuClient:
    def __init__(self, cfg: Mapping[str, Any], instance_id: str):
        self.cfg = cfg
        self.instance_id = instance_id
        self.session = build_session("KITSU", globals().get("ctx"))
        self._user: dict[str, str] | None = None
        self._entry_cache: dict[str, dict[str, Any] | None] | None = None

    def request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = request_with_auth("kitsu", self.session, method, auth.API_URL + path,
                                     cfg=self.cfg, instance_id=self.instance_id, **kwargs)
        if response.status_code == 401:
            raise auth.KitsuAuthError("Kitsu reconnect required")
        if response.status_code >= 300:
            raise RuntimeError(f"Kitsu HTTP {response.status_code}")
        if response.status_code == 204:
            return {}
        data = response.json()
        if not isinstance(data, dict) or "data" not in data:
            raise RuntimeError("Kitsu returned an invalid response")
        return data

    def user(self) -> dict[str, str]:
        if self._user is None:
            response = request_with_auth("kitsu", self.session, "GET", auth.IDENTITY_URL,
                                        cfg=self.cfg, instance_id=self.instance_id)
            self._user = auth.identity(response)
        return self._user

    def entries(self) -> Any:
        offset = 0
        seen: set[str] = set()
        while True:
            data = self.request("GET", "/library-entries", params={"filter[userId]": self.user()["id"],
                "filter[kind]": "anime", "include": "anime", "page[limit]": 500, "page[offset]": offset, "sort": "id"})
            rows = data.get("data")
            if not isinstance(rows, list):
                raise RuntimeError("Kitsu returned an invalid library")
            included = {str(row["id"]): row for row in data.get("included", []) if row.get("type") == "anime"}
            for entry in rows:
                if not isinstance(entry, dict) or entry.get("type") != "libraryEntries" or not entry.get("id"):
                    raise RuntimeError("Kitsu returned an invalid library entry")
                if str(entry["id"]) in seen:
                    raise RuntimeError("Kitsu repeated a library page")
                seen.add(str(entry["id"]))
                relation = ((entry.get("relationships") or {}).get("anime") or {}).get("data") or {}
                ident = str(relation.get("id") or "")
                media = included.get(ident)
                if not media or not isinstance(entry.get("attributes"), dict):
                    raise RuntimeError("Kitsu omitted anime library data")
                yield entry, media
            if not (data.get("links") or {}).get("next"):
                return
            if not rows:
                raise RuntimeError("Kitsu returned an empty continuation page")
            offset += len(rows)

    @contextmanager
    def library_batch(self, identifiers: Any):
        self._entry_cache = None
        ids = list(dict.fromkeys(str(ident) for ident in identifiers))
        cache: dict[str, dict[str, Any] | None] = dict.fromkeys(ids)
        try:
            for start in range(0, len(ids), 100):
                batch = ids[start:start + 100]
                offset = 0
                seen: set[str] = set()
                while True:
                    data = self.request("GET", "/library-entries", params={"filter[userId]": self.user()["id"],
                        "filter[animeId]": ",".join(batch), "filter[kind]": "anime", "include": "anime", "page[limit]": 500,
                        "page[offset]": offset, "sort": "id"})
                    rows = data.get("data")
                    if not isinstance(rows, list):
                        raise RuntimeError("Kitsu returned an invalid library")
                    for row in rows:
                        if not isinstance(row, dict) or row.get("type") != "libraryEntries" or not row.get("id") or not isinstance(row.get("attributes"), dict):
                            raise RuntimeError("Kitsu returned an invalid library entry")
                        relation = ((row.get("relationships") or {}).get("anime") or {}).get("data") or {}
                        ident = str(relation.get("id") or "")
                        if relation.get("type") != "anime" or ident not in batch or ident in seen:
                            raise RuntimeError("Kitsu returned an ambiguous library batch")
                        seen.add(ident)
                        cache[ident] = row
                    if not (data.get("links") or {}).get("next"):
                        break
                    if not rows:
                        raise RuntimeError("Kitsu returned an empty continuation page")
                    offset += len(rows)
            self._entry_cache = cache
            yield
        finally:
            self._entry_cache = None

    def lookup(self, ident: str) -> dict[str, Any] | None:
        if self._entry_cache is not None and ident in self._entry_cache:
            return self._entry_cache[ident]
        data = self.request("GET", "/library-entries", params={"filter[userId]": self.user()["id"],
                            "filter[animeId]": ident, "page[limit]": 2})
        rows = data.get("data")
        if not isinstance(rows, list) or len(rows) > 1:
            raise RuntimeError("Kitsu returned an ambiguous library entry")
        if rows and (not isinstance(rows[0], dict) or rows[0].get("type") != "libraryEntries"
                     or not rows[0].get("id") or not isinstance(rows[0].get("attributes"), dict)):
            raise RuntimeError("Kitsu returned an invalid library entry")
        entry = rows[0] if rows else None
        if self._entry_cache is not None:
            self._entry_cache[ident] = entry
        return entry

    def media(self, ident: str) -> dict[str, Any]:
        row = self.request("GET", f"/anime/{ident}")["data"]
        if not isinstance(row, dict) or row.get("type") != "anime" or str(row.get("id")) != ident or not isinstance(row.get("attributes"), dict):
            raise RuntimeError("Kitsu returned invalid anime metadata")
        return row

    def save(self, ident: str, entry: dict[str, Any] | None, attributes: dict[str, Any]) -> None:
        if self._entry_cache is not None:
            self._entry_cache.pop(ident, None)
        payload: dict[str, Any] = {"type": "libraryEntries", "attributes": attributes}
        if entry:
            payload["id"] = str(entry["id"])
            saved = self.request("PATCH", f"/library-entries/{entry['id']}", json={"data": payload})
        else:
            payload["relationships"] = {"user": {"data": {"type": "users", "id": self.user()["id"]}},
                                        "anime": {"data": {"type": "anime", "id": ident}}}
            saved = self.request("POST", "/library-entries", json={"data": payload})
        row = saved.get("data")
        if not isinstance(row, dict) or not row.get("id") or row.get("type") != "libraryEntries":
            raise RuntimeError("Kitsu did not confirm the library update")
        actual = row.get("attributes") or {}
        if any(actual.get(key) != value for key, value in attributes.items()):
            raise RuntimeError("Kitsu returned different library values")
        if self._entry_cache is not None:
            self._entry_cache[ident] = row

    def delete(self, entry: dict[str, Any]) -> None:
        cache = self._entry_cache
        cached_ids = [ident for ident, row in (cache or {}).items() if row and row.get("id") == entry["id"]]
        if cache is not None:
            for ident in cached_ids:
                cache.pop(ident, None)
        self.request("DELETE", f"/library-entries/{entry['id']}")
        if self._entry_cache is not None:
            for ident in cached_ids:
                self._entry_cache[ident] = None


class KITSUModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.raw_cfg = cfg
        self.instance_id = current_instance(cfg)
        self.client = KitsuClient(cfg, self.instance_id)

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
            ok, reason = False, "reconnect_required" if isinstance(exc, auth.KitsuAuthError) else type(exc).__name__
        return {"ok": ok, "status": "ok" if ok else "down", "latency_ms": int((time.monotonic() - start) * 1000),
                "features": {k: bool(v and ok) for k, v in supported_features().items()}, "details": {"reason": reason}}

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, Any]:
        if feature not in _FEATURES:
            return {}
        result = _FEATURES[feature].build_index(self)
        log("KITSU", feature, "info", "index_done", count=len(result), source="live")
        return result

    def _write(self, feature: str, items: Any, action: str, dry_run: bool) -> dict[str, Any]:
        rows = list(items)
        if feature not in _FEATURES:
            return build_op_result(ok=False, unresolved=rows, error="unsupported_feature")
        if dry_run:
            return build_op_result(count=len(rows), dry_run=True)
        with auth.instance_lock(self.instance_id):
            return getattr(_FEATURES[feature], action)(self, rows)

    def add(self, feature: str, items: Any, *, dry_run: bool = False) -> dict[str, Any]:
        return self._write(feature, items, "add", dry_run)

    def remove(self, feature: str, items: Any, *, dry_run: bool = False) -> dict[str, Any]:
        return self._write(feature, items, "remove", dry_run)


class _KITSUOPS:
    def name(self) -> str:
        return "KITSU"

    def label(self) -> str:
        return "Kitsu"

    features = staticmethod(supported_features)
    capabilities = staticmethod(capabilities)

    def state_read_features(self) -> dict[str, bool]:
        return {feature: True for feature in _FEATURES}

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return auth.is_configured(resolve_provider_block(cfg, "kitsu", current_instance(cfg)))

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return KITSUModule(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Any]:
        return KITSUModule(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Any, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return KITSUModule(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Any, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return KITSUModule(cfg).remove(feature, items, dry_run=dry_run)

    def order_remove_items(self, cfg: Mapping[str, Any], items: Any, *, feature: str) -> list[Any]:
        return _history.order_removals(cfg, items) if feature == "history" else list(items)


OPS = _KITSUOPS()
