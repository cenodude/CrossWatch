# providers/sync/_path_matching.py
# CrossWatch - Pair-enabled path lookup in scoped media server catalogues
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import copy
from datetime import datetime, timezone
from functools import wraps
from email.utils import parsedate_to_datetime
from hashlib import sha256
import json
from importlib import import_module
from typing import Any
import time

from cw_platform import library_presence as lp
from cw_platform.local_db import path_catalog as persisted

from cw_platform.path_matching import PathIndex, path_keys
from cw_platform.run_control import raise_if_cancelled
from ._log import log


def _identity(adapter: Any, provider: str, allowed: Any) -> str | None:
    cfg = getattr(adapter, "cfg", None)
    if provider == "PLEX":
        server = adapter.client.server
        address = getattr(server, "_baseurl", None) or getattr(server, "baseurl", None)
        token = getattr(server, "_token", None)
        user = [getattr(cfg, "account_id", None), getattr(cfg, "username", None)]
    else:
        address = getattr(cfg, "server", None)
        token = getattr(cfg, "access_token", None)
        user = getattr(cfg, "user_id", None)
    if not address or not token or not user:
        return None
    return sha256(json.dumps([1, provider, str(address).rstrip("/"), token, user, sorted(allowed)],
                             separators=(",", ":"), default=str).encode()).hexdigest()


def _revision(adapter: Any, provider: str, allowed: Any, previous: Any = None) -> str | None:
    stamps = []
    try:
        old_stamps = {row[0]: row for row in json.loads((previous or {}).get("revision") or "[]")}
    except (ValueError, TypeError, IndexError):
        old_stamps = {}
    if provider == "PLEX":
        library = adapter.client.server.library
        library.reload()
        for section in library.sections():
            if allowed and str(section.key) not in allowed:
                continue
            if str(section.type) not in {"movie", "show"}:
                continue
            updated = getattr(section, "updatedAt", None)
            attributes = getattr(getattr(section, "_data", None), "attrib", {})
            scanned = attributes.get("scannedAt") or getattr(section, "scannedAt", None)
            changed = attributes.get("contentChangedAt") or getattr(section, "contentChangedAt", None)
            if scanned is None and changed is None:
                return None
            stamps.append([str(section.key), str(getattr(section, "uuid", "")), str(updated), str(scanned), str(changed)])
    else:
        for parent in sorted(allowed) or [None]:
            params = {"Recursive": True, "IncludeItemTypes": "Movie,Episode,Series",
                      "EnableImages": False, "EnableUserData": False, "Limit": 1}
            if parent:
                params["ParentId"] = parent
            response = adapter.client.get(f"/Users/{adapter.cfg.user_id}/Items", params=params)
            if response.status_code != 200:
                raise RuntimeError(f"path_catalog_revision_http_{response.status_code}")
            body = response.json()
            if not isinstance(body, dict) or not isinstance(body.get("Items"), list):
                raise ValueError("path_catalog_invalid_revision")
            count = body.get("TotalRecordCount")
            if not isinstance(count, int) or count < 0:
                return None
            try:
                server_now = parsedate_to_datetime(response.headers.get("Date", "")).timestamp()
            except (AttributeError, TypeError, ValueError, OverflowError):
                return None
            old = old_stamps.get(parent)
            delta_known = False
            marker = 0
            watermark = server_now
            if isinstance(old, list) and len(old) == 5 and old[2] == "delta-v2":
                delta_known = True
                marker = int(old[3])
                watermark = float(old[4])
            since = watermark - 120 if delta_known else server_now + 86400
            probe = {**params, "MinDateLastSaved": datetime.fromtimestamp(since, timezone.utc).isoformat()}
            changed = adapter.client.get(f"/Users/{adapter.cfg.user_id}/Items", params=probe)
            if changed.status_code != 200:
                if not delta_known and changed.status_code == 400:
                    return None
                raise RuntimeError(f"path_catalog_revision_http_{changed.status_code}")
            changes = changed.json()
            if not isinstance(changes, dict) or not isinstance(changes.get("Items"), list):
                raise ValueError("path_catalog_invalid_revision")
            if not delta_known and changes["Items"]:
                return None
            if delta_known and (changes["Items"] or server_now < watermark or count != (old[1] if isinstance(old, list) else None)):
                marker += 1
                watermark = server_now
            stamps.append([parent, count, "delta-v2", marker, watermark])
    return json.dumps(sorted(stamps, key=str), separators=(",", ":"))


def _catalog(adapter: Any, provider: str, feature: str) -> PathIndex:
    if provider != "PLEX":
        return _scoped_catalog(adapter, provider, feature)
    common = import_module("providers.sync.plex._common")
    needed, switched, _, _ = common.home_scope_enter(adapter)
    try:
        if needed and not switched:
            raise RuntimeError("home_scope_not_applied")
        return _scoped_catalog(adapter, provider, feature)
    finally:
        common.home_scope_exit(adapter, switched)


def _scoped_catalog(adapter: Any, provider: str, feature: str) -> PathIndex:
    common = import_module(f"providers.sync.{provider.lower()}._common")
    if provider == "PLEX":
        allowed = common.plex_feature_library_ids(adapter, feature)
    else:
        select = common.jf_selected_library_ids if provider == "JELLYFIN" else common.emby_selected_library_ids
        allowed = select(adapter.cfg, feature)
    cache = getattr(adapter, "_media_path_catalogs", None)
    if cache is None:
        cache = {}
        adapter._media_path_catalogs = cache
    identity = _identity(adapter, provider, allowed)
    generation = persisted.generation()
    if getattr(adapter, "_media_path_generation", None) != generation:
        cache.clear()
        adapter._media_path_generation = generation
    scope = (identity, tuple(sorted(allowed)))
    cached = cache.get(scope)
    if cached and time.monotonic() - cached[0] < 90:
        return cached[1]
    now = time.time()
    previous = None
    revision = None
    if identity:
        try:
            previous = persisted.load(identity)
        except Exception as exc:
            raise_if_cancelled()
            log(provider, feature, "warning", "path_catalog_cache_read_failed", error_type=type(exc).__name__)
        try:
            revision = _revision(adapter, provider, allowed, previous)
        except Exception as exc:
            raise_if_cancelled()
            previous = None
            log(provider, feature, "warning", "path_catalog_revision_failed", error_type=type(exc).__name__)
    if previous is not None:
        age = now - previous["refreshed_at"]
        ttl = 86400 if revision is not None else 300
        if 0 <= age < ttl and previous["revision"] == revision:
            result = PathIndex(previous["items"].items())
            cache[scope] = (time.monotonic(), result)
            return result
    rows = []
    if provider == "PLEX":
        server = adapter.client.server
        for section in server.library.sections():
            raise_if_cancelled()
            if allowed and str(section.key) not in allowed:
                continue
            kind = str(section.type)
            if kind not in {"movie", "show"}:
                continue
            for obj in section.all(libtype="movie" if kind == "movie" else "episode"):
                rows.append((str(obj.ratingKey), common.normalize(obj)))
    else:
        raw = []
        uid = adapter.cfg.user_id
        for parent in sorted(allowed) or [None]:
            start = 0
            seen = set()
            while True:
                raise_if_cancelled()
                params = {
                    "UserId": uid, "Recursive": True, "IncludeItemTypes": "Movie,Episode,Series",
                    "Fields": "Path,MediaSources,ProviderIds,ProductionYear,IndexNumber,ParentIndexNumber,SeriesId,SeriesName",
                    "StartIndex": start, "Limit": 500,
                    "EnableImages": False, "EnableUserData": False, "CollapseBoxSetItems": False,
                }
                if parent:
                    params["ParentId"] = parent
                response = adapter.client.get(f"/Users/{uid}/Items", params=params)
                if response.status_code != 200:
                    raise RuntimeError(f"{provider.lower()}_path_catalog_http_{response.status_code}")
                body = response.json()
                if not isinstance(body, dict) or not isinstance(body.get("Items"), list):
                    raise ValueError("path_catalog_invalid_response")
                page = body["Items"]
                if any(not isinstance(row, dict) or not row.get("Id") for row in page):
                    raise ValueError("path_catalog_invalid_row")
                signature = tuple(str(row.get("Id") or "") for row in page)
                if page and signature in seen:
                    raise RuntimeError(f"{provider.lower()}_path_catalog_repeated_page")
                seen.add(signature)
                raw.extend(page)
                total = body.get("TotalRecordCount")
                if isinstance(total, int):
                    if not page and start < total:
                        raise ValueError("path_catalog_incomplete_response")
                    if len(page) < 500 and start + len(page) >= total:
                        break
                elif len(page) < 500:
                    break
                start += len(page)
        shows = {str(row.get("Id")): common.normalize(row).get("ids", {}) for row in raw if row.get("Type") == "Series"}
        for row in raw:
            item = common.normalize(row)
            if item.get("type") == "episode":
                item["show_ids"] = shows.get(str(row.get("SeriesId")), {})
            if row.get("Id"):
                rows.append((str(row["Id"]), item))
    result = PathIndex(rows)
    if identity:
        try:
            persisted.save(identity, revision, now, dict(result.items))
        except Exception as exc:
            raise_if_cancelled()
            log(provider, feature, "warning", "path_catalog_cache_write_failed", error_type=type(exc).__name__)
    cache[scope] = (time.monotonic(), result)
    return result


def resolve_path(adapter: Any, item: Any, provider: str, feature: str) -> tuple[str | None, str]:
    if not getattr(adapter, "path_matching", False) or not path_keys(item):
        return None, "path_not_found"
    native_id, reason = _catalog(adapter, provider, feature).resolve(item)
    if reason != "path_not_found":
        log(provider, feature, "info" if native_id else "warning", reason,
            resolution_method="file_path", item_id=native_id, title=item.get("title"))
    return native_id, reason


def path_fallback(provider: str, feature: str = "history"):
    def decorate(resolve):
        @wraps(resolve)
        def wrapped(adapter, item, **kwargs):
            if not getattr(adapter, "path_matching", False) or not path_keys(item):
                return resolve(adapter, item, **kwargs)
            strict_adapter = copy(adapter)
            strict_adapter.cfg = copy(adapter.cfg)
            strict_adapter.cfg.strict_id_matching = True
            if hasattr(adapter, "config"):
                strict_adapter.config = {**adapter.config, provider.lower(): {
                    **adapter.config.get(provider.lower(), {}), "strict_id_matching": True,
                }}
            strict_kwargs = dict(kwargs)
            if "strict" in strict_kwargs:
                strict_kwargs["strict"] = True
            setattr(adapter, f"_{provider.lower()}_last_resolve_hint", None)
            found = resolve(strict_adapter, item, **strict_kwargs)
            if found:
                return found
            found, reason = resolve_path(adapter, item, provider, kwargs.get("feature", feature))
            if found or reason in {"path_ambiguous", "path_id_conflict"}:
                setattr(adapter, f"_{provider.lower()}_last_resolve_hint", reason)
                return found
            return resolve(adapter, item, **kwargs)
        return wrapped
    return decorate


def enrich_snapshot(adapter: Any, index: Any, provider: str, feature: str):
    if not index:
        return index
    if not getattr(adapter, "path_matching", False):
        return {key: {field: value for field, value in item.items() if field != "media_paths"}
                for key, item in index.items()}
    missing = [item for item in index.values() if item.get("type") in {"movie", "episode"} and not path_keys(item)]
    if not missing:
        return index
    catalog = _catalog(adapter, provider, feature)
    out = {}
    for key, item in index.items():
        native = item.get(provider.lower() + "_item_id") or (item.get("ids") or {}).get(provider.lower())
        found = catalog.items.get(str(native)) if native else None
        out[key] = {**item, "media_paths": list(found.get("media_paths") or [])} if found else item
    return out


def path_presence(adapter: Any, items: Any, results: Any, provider: str, feature: str):
    if not getattr(adapter, "path_matching", False):
        return results
    out = list(results)
    for i, (item, result) in enumerate(zip(items, results)):
        if result.get("status") == lp.PRESENT:
            continue
        try:
            native_id, reason = resolve_path(adapter, item, provider, feature)
            if native_id:
                out[i] = lp.verdict(lp.PRESENT, item_id=native_id, reason="path_match")
            elif reason in {"path_ambiguous", "path_id_conflict"}:
                out[i] = lp.unknown(reason)
        except Exception as exc:
            out[i] = lp.unknown(type(exc).__name__)
    return out
