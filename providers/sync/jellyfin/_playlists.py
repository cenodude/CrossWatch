# /providers/sync/jellyfin/_playlists.py
# JELLYFIN Module for playlists sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal
from ._common import normalize as jelly_normalize  # kept for future use / consistency

UNRESOLVED_PATH = "/config/.cw_state/jellyfin_playlists.unresolved.json"


def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_JELLYFIN_DEBUG"):
        print(f"[JELLYFIN:playlists] {msg}")


def _load() -> dict[str, Any]:
    try:
        with open(UNRESOLVED_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save(obj: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(UNRESOLVED_PATH), exist_ok=True)
        with open(UNRESOLVED_PATH, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass


def _freeze(item: Mapping[str, Any], *, reason: str) -> None:
    key = canonical_key(item)
    data = _load()
    ent = data.get(key) or {"feature": "playlists", "attempts": 0}
    ent.update({"hint": id_minimal(item)})
    ent["attempts"] = int(ent.get("attempts", 0)) + 1
    ent["reason"] = reason
    data[key] = ent
    _save(data)


def _thaw_if_present(keys: Iterable[str]) -> None:
    data = _load()
    changed = False
    for k in list(keys or []):
        if k in data:
            data.pop(k, None)
            changed = True
    if changed:
        _save(data)


def _resolve_item_id(adapter: Any, it: Mapping[str, Any]) -> str | None:
    try:
        from . import _watchlist as wl  # type: ignore[import]
        fn = getattr(wl, "_resolve_item_id", None)
        if callable(fn):
            res = fn(adapter, it)
            if isinstance(res, (str, int)):
                return str(res)
    except Exception:
        pass
    try:
        from ._common import resolve_item_id as _common_resolve
        res2 = _common_resolve(adapter, it)
        if isinstance(res2, (str, int)):
            return str(res2)
        return None
    except Exception:
        return None


def _ensure_playlist(adapter: Any, name: str) -> str | None:
    http = adapter.client
    uid = adapter.cfg.user_id
    norm = (name or "").strip() or "Watchlist"

    r = http.get(
        f"/Users/{uid}/Items",
        params={"includeItemTypes": "Playlist", "recursive": False},
    )
    try:
        for it in (r.json() or {}).get("Items") or []:
            if (it.get("Name") or "").strip().lower() == norm.lower():
                pid = it.get("Id")
                if pid:
                    return str(pid)
    except Exception:
        pass

    r2 = http.post("/Playlists", json={"Name": norm, "UserId": uid, "MediaType": "Video"})
    if getattr(r2, "status_code", 0) in (200, 201):
        try:
            pid = (r2.json() or {}).get("Id")
            if pid:
                return str(pid)
        except Exception:
            pass

    r3 = http.post("/Playlists", params={"name": norm, "userId": uid})
    if getattr(r3, "status_code", 0) in (200, 201, 204):
        rr = http.get(
            f"/Users/{uid}/Items",
            params={"includeItemTypes": "Playlist", "recursive": False},
        )
        try:
            for it in (rr.json() or {}).get("Items") or []:
                if (it.get("Name") or "").strip().lower() == norm.lower():
                    pid = it.get("Id")
                    if pid:
                        return str(pid)
        except Exception:
            pass

    return None


def _playlist_add(adapter: Any, playlist_id: str, item_ids: list[str]) -> bool:
    http = adapter.client
    uid = adapter.cfg.user_id
    if not item_ids:
        return True

    r = http.post(
        f"/Playlists/{playlist_id}/Items",
        params={"userId": uid, "ids": ",".join(item_ids)},
    )
    if getattr(r, "status_code", 0) in (200, 204):
        return True

    r2 = http.post(f"/Playlists/{playlist_id}/Items", json={"Ids": item_ids})
    return getattr(r2, "status_code", 0) in (200, 204)


def _playlist_remove(adapter: Any, playlist_id: str, item_ids: list[str]) -> bool:
    http = adapter.client
    uid = adapter.cfg.user_id
    if not item_ids:
        return True

    rev: dict[str, list[str]] = {}
    r = http.get(f"/Playlists/{playlist_id}/Items", params={"userId": uid})
    try:
        for it in (r.json() or {}).get("Items") or []:
            mid = str(it.get("Id") or "")
            eid = str(it.get("PlaylistItemId") or "") or mid
            if mid:
                rev.setdefault(mid, []).append(eid)
    except Exception:
        pass

    entry_ids: list[str] = []
    for mid in item_ids:
        entry_ids.extend(rev.get(mid, []))

    if entry_ids:
        re = http.delete(
            f"/Playlists/{playlist_id}/Items",
            params={"entryIds": ",".join(entry_ids)},
        )
        if getattr(re, "status_code", 0) in (200, 204):
            return True

    r2 = http.delete(
        f"/Playlists/{playlist_id}/Items",
        params={"ids": ",".join(item_ids)},
    )
    return getattr(r2, "status_code", 0) in (200, 204)


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("playlists") if callable(prog_mk) else None
    if prog:
        try:
            prog.done(ok=True, total=0)
        except Exception:
            pass
    _log("index size: 0 (playlists index is intentionally empty)")
    return {}


def add(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    ok_total = 0
    unresolved: list[dict[str, Any]] = []

    for pl in items:
        name = (pl.get("playlist") or pl.get("title") or "").strip()
        rows = list(pl.get("items") or [])
        if not name:
            unresolved.append({"item": id_minimal(pl), "hint": "missing_playlist_name"})
            _freeze(pl, reason="missing_playlist_name")
            continue

        pid = _ensure_playlist(adapter, name)
        if not pid:
            unresolved.append({"item": id_minimal(pl), "hint": "ensure_playlist_failed"})
            _freeze(pl, reason="write_failed")
            continue

        ids: list[str] = []
        for it in rows:
            iid = _resolve_item_id(adapter, it)
            if iid:
                ids.append(iid)
            else:
                unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
                _freeze(it, reason="resolve_failed")

        if ids and _playlist_add(adapter, pid, ids):
            ok_total += len(ids)
            _thaw_if_present([canonical_key(id_minimal(x)) for x in rows])
        else:
            for it in rows:
                _freeze(it, reason="write_failed")
                unresolved.append({"item": id_minimal(it), "hint": "playlist_add_failed"})

    _log(f"add done: +{ok_total} / unresolved {len(unresolved)}")
    return ok_total, unresolved


def remove(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    ok_total = 0
    unresolved: list[dict[str, Any]] = []

    for pl in items:
        name = (pl.get("playlist") or pl.get("title") or "").strip()
        rows = list(pl.get("items") or [])
        if not name:
            unresolved.append({"item": id_minimal(pl), "hint": "missing_playlist_name"})
            _freeze(pl, reason="missing_playlist_name")
            continue

        pid = _ensure_playlist(adapter, name)
        if not pid:
            unresolved.append({"item": id_minimal(pl), "hint": "missing_playlist"})
            _freeze(pl, reason="write_failed")
            continue

        ids: list[str] = []
        for it in rows:
            iid = _resolve_item_id(adapter, it)
            if iid:
                ids.append(iid)
            else:
                unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
                _freeze(it, reason="resolve_failed")

        if ids and _playlist_remove(adapter, pid, ids):
            ok_total += len(ids)
            _thaw_if_present([canonical_key(id_minimal(x)) for x in rows])
        else:
            for it in rows:
                _freeze(it, reason="write_failed")
                unresolved.append({"item": id_minimal(it), "hint": "playlist_remove_failed"})

    _log(f"remove done: -{ok_total} / unresolved {len(unresolved)}")
    return ok_total, unresolved
