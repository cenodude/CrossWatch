"""Pair-scoped movie/series identity catalogue for targeted Jellyfin writes."""
from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime, timezone
from hashlib import sha256
from math import isfinite
from pathlib import Path
from typing import Any, Mapping

from . import _common as common

_VERSION = 1
_FULL_REFRESH_SECONDS = 86400
_OVERLAP_SECONDS = 120
_PAGE_SIZE = 500
_FIELDS = "ProviderIds,Type,Name,ProductionYear,ParentId,CollectionFolderId,AncestorIds,LibraryId"


def _cache_path(adapter: Any, feature: str) -> Path | None:
    cfg = adapter.cfg
    config = getattr(adapter, "config", None)
    scope = config.get("_cw_pair_scope") if isinstance(config, Mapping) else None
    scope = str(scope or common._pair_scope() or "")
    server = str(getattr(cfg, "server", "") or "")
    user = str(getattr(cfg, "user_id", "") or "")
    token = str(getattr(cfg, "access_token", "") or "")
    if not server or not user or not token or not scope or scope.lower() in {"none", "default", "unscoped"}:
        return None
    identity = [scope, server, user, token, feature, sorted(common.jf_selected_library_ids(cfg, feature))]
    digest = sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()
    return common.STATE_DIR / f"jellyfin_identity.{digest}.json"


def _compact(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in ("Id", *_FIELDS.split(",")) if key in row}


def _fetch(adapter: Any, feature: str, *, after: float | None = None) -> dict[str, dict[str, Any]]:
    selected = common.jf_selected_library_ids(adapter.cfg, feature)
    out: dict[str, dict[str, Any]] = {}
    for parent in sorted(selected) or [None]:
        start = 0
        seen: set[tuple[str, ...]] = set()
        while True:
            params = {
                "userId": adapter.cfg.user_id, "Recursive": True,
                "IncludeItemTypes": "Movie,Series", "Fields": _FIELDS,
                "EnableImages": False, "EnableUserData": False,
                "EnableTotalRecordCount": False, "StartIndex": start, "Limit": _PAGE_SIZE,
            }
            if parent:
                params["ParentId"] = parent
            if after is not None:
                params["MinDateLastSaved"] = datetime.fromtimestamp(after, timezone.utc).isoformat()
            response = adapter.client.get("/Items", params=params)
            if response.status_code != 200:
                raise RuntimeError(f"identity_index_http_{response.status_code}")
            body = response.json()
            if not isinstance(body, dict) or not isinstance(body.get("Items"), list):
                raise ValueError("invalid_identity_index_response")
            rows = body["Items"]
            if any(not isinstance(row, dict) or not common._valid_item_id(row.get("Id")) for row in rows):
                raise ValueError("invalid_identity_index_row")
            signature = tuple(str(row["Id"]) for row in rows)
            if rows and signature in seen:
                raise ValueError("identity_index_repeated_page")
            seen.add(signature)
            scoped = common.jf_filter_library_candidates(rows, selected, trust_query_scope=True)
            for row in scoped:
                if row.get("Type") in {"Movie", "Series"}:
                    out[str(row["Id"])] = _compact(row)
            common._dbg("identity_index_page", lookup_feature=feature, count=len(rows), start=start,
                        refresh="delta" if after is not None else "full")
            if len(rows) < _PAGE_SIZE:
                break
            start += len(rows)
    return out


def _load(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("version") != _VERSION or not isinstance(data.get("rows"), dict):
            return None
        if any(not isinstance(row, dict) or str(row.get("Id")) != iid for iid, row in data["rows"].items()):
            return None
        data["refreshed"] = float(data["refreshed"])
        data["full_refresh"] = float(data["full_refresh"])
        if not isfinite(data["refreshed"]) or not isfinite(data["full_refresh"]):
            return None
        return data
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return None


def _save(path: Path | None, data: dict[str, Any]) -> None:
    if path is None:
        return
    temporary = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=path.name, suffix=".tmp", delete=False) as fh:
            temporary = Path(fh.name)
            json.dump(data, fh, separators=(",", ":"), ensure_ascii=False)
        os.replace(temporary, path)
    except OSError as exc:
        common._dbg("identity_index_cache_write_failed", error_type=type(exc).__name__)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _catalogue(adapter: Any, feature: str) -> dict[str, Any] | None:
    cache = common._targeted_cache(adapter, feature)
    key = ("identity_catalogue",)
    if key in cache:
        return cache[key]
    # A failed refresh is not retried for every unresolved source item.
    cache[key] = None
    path = _cache_path(adapter, feature)
    previous = _load(path)
    now = time.time()
    full = (previous is None or now < previous["refreshed"]
            or now - previous["full_refresh"] >= _FULL_REFRESH_SECONDS
            or previous.get("delta_supported") is False)
    try:
        if previous is not None and not full:
            after = previous["refreshed"] - _OVERLAP_SECONDS
            rows = dict(previous["rows"])
            full_refresh = previous["full_refresh"]
        else:
            after = None
            rows = {}
            full_refresh = now
        changed = _fetch(adapter, feature, after=after)
        rows.update(changed)
        delta_supported = previous.get("delta_supported", True) if previous else True
        if previous is None:
            # Unsupported query parameters can be silently ignored by Jellyfin.
            # A future-date probe must return no rows before enabling deltas.
            delta_supported = not _fetch(adapter, feature, after=now + _FULL_REFRESH_SECONDS)
        data = {"version": _VERSION, "rows": rows, "refreshed": now,
                "full_refresh": full_refresh, "delta_supported": delta_supported}
        _save(path, data)
        index: dict[str, list[dict[str, Any]]] = {}
        for row in rows.values():
            common._index_row_provider_ids(index, row)
        result = {"index": index, "fresh": set(changed), "validated": {}}
        cache[key] = result
        common._dbg("identity_index_ready", lookup_feature=feature, refresh="full" if full else "delta",
                    items=len(rows), changed=len(changed), delta_supported=delta_supported)
        return result
    except Exception as exc:
        common._dbg("identity_index_failed", lookup_feature=feature, error_type=type(exc).__name__)
        return None


def find(adapter: Any, feature: str, kind: str, pairs: list[str]) -> list[Mapping[str, Any]]:
    if not pairs:
        return []
    catalogue = _catalogue(adapter, feature)
    if catalogue is None:
        return []
    candidates = {str(row["Id"]): row for pair in pairs for row in catalogue["index"].get(pair, [])
                  if row.get("Type") == kind}
    found = []
    selected = common.jf_selected_library_ids(adapter.cfg, feature)
    for iid, row in sorted(candidates.items()):
        if iid not in catalogue["fresh"]:
            # Validate persisted matches, including deleted items, changed IDs
            # and items moved out of the pair's selected libraries.
            if iid not in catalogue["validated"]:
                try:
                    response = adapter.client.get(f"/Items/{iid}", params={"userId": adapter.cfg.user_id, "Fields": _FIELDS})
                    current = response.json() if response.status_code == 200 else {}
                    if not isinstance(current, dict) or str(current.get("Id") or "") != iid:
                        current = {}
                    if current and selected:
                        ancestors = adapter.client.get(f"/Items/{iid}/Ancestors", params={"userId": adapter.cfg.user_id})
                        allowed = ancestors.json() if ancestors.status_code == 200 else []
                        if not isinstance(allowed, list) or not any(isinstance(a, dict) and str(a.get("Id")) in selected for a in allowed):
                            current = {}
                    catalogue["validated"][iid] = current
                except Exception:
                    catalogue["validated"][iid] = {}
            row = catalogue["validated"][iid]
        if row.get("Type") == kind and common._row_matches_pair(row, pairs):
            found.append(row)
    common._dbg("identity_index_match", lookup_feature=feature, kind=kind, candidates=len(candidates), matched=len(found))
    return found


def prepare(adapter: Any, feature: str, items: Any) -> None:
    """Validate cached targets together before a history write batch resolves them."""
    if not common._targeted_enabled(adapter):
        return
    requested = []
    priority = common._merged_guid_priority(adapter)
    for source in items:
        kind = common._lookup_type(source)
        ids = source.get("show_ids") if kind == "episode" else source.get("ids")
        pairs = common.all_ext_pairs(ids or {}, priority)
        if pairs:
            requested.append(("Movie" if kind == "movie" else "Series", pairs))
    if not requested:
        return
    catalogue = _catalogue(adapter, feature)
    if catalogue is None:
        return
    # Scoped matches also require current ancestry verification; find handles it.
    if common.jf_selected_library_ids(adapter.cfg, feature):
        return
    pending = sorted({str(row["Id"]) for kind, pairs in requested for pair in pairs
                      for row in catalogue["index"].get(pair, []) if row.get("Type") == kind
                      and str(row["Id"]) not in catalogue["fresh"]
                      and str(row["Id"]) not in catalogue["validated"]})
    for offset in range(0, len(pending), 100):
        batch = pending[offset:offset + 100]
        # Missing/deleted items and failed reads must not become assumed matches.
        catalogue["validated"].update({iid: {} for iid in batch})
        try:
            response = adapter.client.get("/Items", params={
                "userId": adapter.cfg.user_id, "Ids": ",".join(batch), "Fields": _FIELDS,
                "EnableImages": False, "EnableUserData": False, "Limit": len(batch),
            })
            if response.status_code != 200:
                continue
            body = response.json()
            rows = body.get("Items") if isinstance(body, dict) else None
            if not isinstance(rows, list):
                continue
            for row in rows:
                if isinstance(row, dict) and str(row.get("Id")) in batch:
                    catalogue["validated"][str(row["Id"])] = row
        except Exception as exc:
            common._dbg("identity_validation_failed", lookup_feature=feature, error_type=type(exc).__name__)
