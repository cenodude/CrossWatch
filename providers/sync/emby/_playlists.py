# /providers/sync/emby/_playlists.py
# EMBY Module for playlist synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

UNRESOLVED_PATH = "/config/.cw_state/emby_playlists.unresolved.json"


def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_EMBY_DEBUG"):
        print(f"[EMBY:playlists] {msg}")


# unresolved store
def _load() -> dict[str, Any]:
    try:
        with open(UNRESOLVED_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f) or {}
        return obj if isinstance(obj, dict) else {}
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


# helpers
def _resolve_item_id(adapter: Any, it: Mapping[str, Any]) -> str | None:
    from . import _watchlist as wl  # type: ignore[import]

    return wl._resolve_item_id(adapter, it)  # type: ignore[attr-defined]


def _ensure_playlist(adapter: Any, name: str) -> str | None:
    http = adapter.client
    uid = adapter.cfg.user_id
    norm = (name or "").strip() or "Watchlist"

    r = http.get(
        f"/Users/{uid}/Items",
        params={"IncludeItemTypes": "Playlist", "Recursive": False},
    )
    if not getattr(r, "ok", False):
        r = http.get(
            f"/Users/{uid}/Items",
            params={"includeItemTypes": "Playlist", "recursive": False},
        )

    try:
        body = r.json() or {}
    except Exception:
        body = {}

    items = body.get("Items") or [] if isinstance(body, Mapping) else []
    try:
        for it in items:
            nm = (it.get("Name") or "").strip()
            if nm.lower() == norm.lower():
                pid = it.get("Id")
                if pid:
                    return str(pid)
    except Exception:
        pass

    r2 = http.post(
        "/Playlists",
        json={"Name": norm, "UserId": uid, "MediaType": "Video"},
    )
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
            params={"IncludeItemTypes": "Playlist", "Recursive": False},
        )
        if not getattr(rr, "ok", False):
            rr = http.get(
                f"/Users/{uid}/Items",
                params={"includeItemTypes": "Playlist", "recursive": False},
            )
        try:
            body2 = rr.json() or {}
        except Exception:
            body2 = {}
        items2 = body2.get("Items") or [] if isinstance(body2, Mapping) else []
        try:
            for it in items2:
                nm = (it.get("Name") or "").strip()
                if nm.lower() == norm.lower():
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
        params={"UserId": uid, "Ids": ",".join(item_ids)},
    )
    if getattr(r, "status_code", 0) in (200, 204):
        return True

    r2 = http.post(
        f"/Playlists/{playlist_id}/Items",
        params={"userId": uid, "ids": ",".join(item_ids)},
    )
    if getattr(r2, "status_code", 0) in (200, 204):
        return True

    r3 = http.post(
        f"/Playlists/{playlist_id}/Items",
        json={"Ids": item_ids},
    )
    return getattr(r3, "status_code", 0) in (200, 204)


def _playlist_remove(adapter: Any, playlist_id: str, item_ids: list[str]) -> bool:
    http = adapter.client
    uid = adapter.cfg.user_id
    if not item_ids:
        return True

    rev: dict[str, list[str]] = {}
    r = http.get(f"/Playlists/{playlist_id}/Items", params={"UserId": uid})
    if not getattr(r, "ok", False):
        r = http.get(f"/Playlists/{playlist_id}/Items", params={"userId": uid})

    try:
        body = r.json() or {}
    except Exception:
        body = {}

    rows = body.get("Items") or [] if isinstance(body, Mapping) else []
    try:
        for it in rows:
            mid = str(it.get("Id") or "") or ""
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
            params={"EntryIds": ",".join(entry_ids)},
        )
        if getattr(re, "status_code", 0) in (200, 204):
            return True
        re2 = http.delete(
            f"/Playlists/{playlist_id}/Items",
            params={"entryIds": ",".join(entry_ids)},
        )
        if getattr(re2, "status_code", 0) in (200, 204):
            return True

    r2 = http.delete(
        f"/Playlists/{playlist_id}/Items",
        params={"Ids": ",".join(item_ids)},
    )
    if getattr(r2, "status_code", 0) in (200, 204):
        return True

    r3 = http.delete(
        f"/Playlists/{playlist_id}/Items",
        params={"ids": ",".join(item_ids)},
    )
    return getattr(r3, "status_code", 0) in (200, 204)


# index
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


# writes
def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    ok_total = 0
    unresolved: list[dict[str, Any]] = []

    for pl in items or []:
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
            thaw_keys = [canonical_key(id_minimal(x)) for x in rows]
            _thaw_if_present(thaw_keys)
        else:
            for it in rows:
                _freeze(it, reason="write_failed")
                unresolved.append({"item": id_minimal(it), "hint": "playlist_add_failed"})

    _log(f"add done: +{ok_total} / unresolved {len(unresolved)}")
    return ok_total, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    ok_total = 0
    unresolved: list[dict[str, Any]] = []

    for pl in items or []:
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
            thaw_keys = [canonical_key(id_minimal(x)) for x in rows]
            _thaw_if_present(thaw_keys)
        else:
            for it in rows:
                _freeze(it, reason="write_failed")
                unresolved.append({"item": id_minimal(it), "hint": "playlist_remove_failed"})

    _log(f"remove done: -{ok_total} / unresolved {len(unresolved)}")
    return ok_total, unresolved
