# /providers/sync/trakt/_playlists.py
# TRAKT Module for playlist sync functions (currently not used)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os, json, time
from pathlib import Path
from typing import Any, Mapping
from collections.abc import Iterable, Callable, Sequence

from ._common import (
    build_headers,
    normalize_watchlist_row,
    key_of,
    ids_for_trakt,
    pick_trakt_kind,
)
from .._mod_common import request_with_retries
from cw_platform.id_map import minimal as id_minimal

BASE = "https://api.trakt.tv"
URL_LISTS_ME = f"{BASE}/users/me/lists"
URL_LIST_ITEMS_FMT = f"{BASE}/users/me/lists/{{lid}}/items"
URL_LIST_ADD_FMT = f"{BASE}/users/me/lists/{{lid}}/items"
URL_LIST_REM_FMT = f"{BASE}/users/me/lists/{{lid}}/items/remove"

UNRESOLVED_PATH = "/config/.cw_state/trakt_playlists.unresolved.json"


def _log(msg: str) -> None:
    if os.getenv("CW_DEBUG") or os.getenv("CW_TRAKT_DEBUG"):
        print(f"[TRAKT:playlists] {msg}")


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_unresolved() -> dict[str, Any]:
    try:
        return json.loads(Path(UNRESOLVED_PATH).read_text("utf-8"))
    except Exception:
        return {}


def _save_unresolved(data: Mapping[str, Any]) -> None:
    try:
        p = Path(UNRESOLVED_PATH)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, p)
    except Exception as e:
        _log(f"unresolved.save failed: {e}")


def _freeze_item(item: Mapping[str, Any], *, action: str, reasons: list[str]) -> None:
    m = id_minimal(item)
    key = key_of(m)
    data = _load_unresolved()
    entry = data.get(key) or {
        "feature": "playlists",
        "action": action,
        "first_seen": _now_iso(),
        "attempts": 0,
    }
    entry.update({"item": m, "last_attempt": _now_iso()})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    entry["reasons"] = sorted(rset)
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)


def _unfreeze_keys_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved()
    changed = False
    for k in list(keys or []):
        if k in data:
            del data[k]
            changed = True
    if changed:
        _save_unresolved(data)


def _is_frozen(item: Mapping[str, Any]) -> bool:
    return key_of(id_minimal(item)) in _load_unresolved()


def _fetch_lists(sess, headers: Mapping[str, Any], *, timeout: float, max_retries: int) -> list[dict[str, Any]]:
    r = request_with_retries(sess, "GET", URL_LISTS_ME, headers=headers, timeout=timeout, max_retries=max_retries)
    if r.status_code != 200:
        raise RuntimeError(f"lists GET failed: {r.status_code}")
    return r.json() or []


def _pick_list(
    lists: Sequence[Mapping[str, Any]],
    *,
    list_id: Any | None,
    list_slug: str | None,
    list_name: str | None,
) -> Mapping[str, Any]:
    if list_id is not None:
        sid = str(list_id).strip().lower()
        for L in lists:
            ids = L.get("ids") or {}
            if str(ids.get("trakt")).lower() == sid or str(ids.get("slug")).lower() == sid:
                return L
    if list_slug:
        s = list_slug.strip().lower()
        for L in lists:
            if str((L.get("ids") or {}).get("slug") or "").lower() == s:
                return L
    if list_name:
        s = list_name.strip().lower()
        for L in lists:
            if str(L.get("name") or "").strip().lower() == s:
                return L
    raise RuntimeError("list_not_found")


def _lid_token(L: Mapping[str, Any]) -> str:
    ids = L.get("ids") or {}
    if ids.get("trakt") is not None:
        return str(ids.get("trakt"))
    if ids.get("slug"):
        return str(ids.get("slug"))
    return str(ids or "")


def _collect_items_for_list(
    sess,
    headers: Mapping[str, Any],
    L: Mapping[str, Any],
    *,
    timeout: float,
    max_retries: int,
    bump: Callable[[int], None] | None = None,
) -> list[dict[str, Any]]:
    lid = _lid_token(L)
    url = URL_LIST_ITEMS_FMT.format(lid=lid)
    r = request_with_retries(sess, "GET", url, headers=headers, timeout=timeout, max_retries=max_retries)
    if r.status_code != 200:
        _log(f"items GET list={lid} -> {r.status_code}")
        return []
    rows = r.json() or []
    out: list[dict[str, Any]] = []
    added = 0
    for row in rows:
        t = (row.get("type") or "").lower()
        payload = row.get("movie") if t == "movie" else row.get("show") if t == "show" else None
        if not isinstance(payload, dict):
            continue
        m = normalize_watchlist_row({"type": t or "movie", t or "movie": payload})
        m["_list"] = {
            "id": (L.get("ids") or {}).get("trakt"),
            "slug": (L.get("ids") or {}).get("slug"),
            "name": L.get("name"),
        }
        out.append(m)
        added += 1
    if bump and added:
        try:
            bump(added)
        except Exception:
            pass
    return out


def _tick(prog: Any, value: int) -> None:
    if prog is None:
        return
    try:
        prog.tick(value)
    except Exception:
        pass


def _done(prog: Any, ok: bool, total: int) -> None:
    if prog is None:
        return
    try:
        prog.done(ok=ok, total=total)
    except Exception:
        pass


def build_index(
    adapter: Any,
    *,
    list_id: Any | None = None,
    list_slug: str | None = None,
    list_name: str | None = None,
) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("playlists") if callable(prog_mk) else None
    done = 0

    def bump(n: int) -> None:
        nonlocal done
        done += int(n or 0)
        _tick(prog, done)

    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})

    try:
        lists = _fetch_lists(sess, headers, timeout=adapter.cfg.timeout, max_retries=adapter.cfg.max_retries)
    except Exception as e:
        _log(str(e))
        _done(prog, ok=False, total=0)
        return {}

    if list_id is not None or list_slug or list_name:
        try:
            targets = [
                _pick_list(lists, list_id=list_id, list_slug=list_slug, list_name=list_name)
            ]
        except Exception as e:
            _log(f"select list failed: {e}")
            _done(prog, ok=False, total=0)
            return {}
    else:
        targets = lists

    items: list[dict[str, Any]] = []
    for L in targets:
        items.extend(
            _collect_items_for_list(
                sess,
                headers,
                L,
                timeout=adapter.cfg.timeout,
                max_retries=adapter.cfg.max_retries,
                bump=bump,
            )
        )

    idx = {key_of(m): m for m in items}
    _unfreeze_keys_if_present(idx.keys())
    _done(prog, ok=True, total=len(idx))
    _log(f"index size: {len(idx)} from {len(targets)} list(s)")
    return idx


def _batch(
    items: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[str], list[dict[str, Any]], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    accepted_keys: list[str] = []
    accepted_minimals: list[dict[str, Any]] = []

    for it in items or []:
        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue
        ids = ids_for_trakt(it)
        if not ids:
            unresolved.append({"item": id_minimal(it), "hint": "missing ids"})
            _freeze_item(it, action="playlist", reasons=["missing-ids"])
            continue
        kind = pick_trakt_kind(it)
        obj = {"ids": ids}
        if kind == "shows":
            shows.append(obj)
            t = "show"
        else:
            movies.append(obj)
            t = "movie"
        m_min = id_minimal({"type": t, "ids": ids})
        accepted_minimals.append(m_min)
        accepted_keys.append(key_of(m_min))

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    if shows:
        body["shows"] = shows
    return body, accepted_keys, unresolved, accepted_minimals


def _resolve_target_list(
    adapter: Any,
    *,
    list_id: Any | None = None,
    list_slug: str | None = None,
    list_name: str | None = None,
) -> str:
    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})
    lists = _fetch_lists(sess, headers, timeout=adapter.cfg.timeout, max_retries=adapter.cfg.max_retries)
    L = _pick_list(lists, list_id=list_id, list_slug=list_slug, list_name=list_name)
    return _lid_token(L)


def add(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
    *,
    list_id: Any | None = None,
    list_slug: str | None = None,
    list_name: str | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    lid = _resolve_target_list(adapter, list_id=list_id, list_slug=list_slug, list_name=list_name)
    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})

    body, accepted_keys, unresolved, accepted_minimals = _batch(items)
    if not body:
        return 0, unresolved

    r = request_with_retries(
        sess,
        "POST",
        URL_LIST_ADD_FMT.format(lid=lid),
        headers=headers,
        json=body,
        timeout=adapter.cfg.timeout,
        max_retries=adapter.cfg.max_retries,
    )
    ok = 0
    if r.status_code in (200, 201):
        d = r.json() or {}
        added = d.get("added") or {}
        existing = d.get("existing") or {}
        ok = (
            int(added.get("movies") or 0)
            + int(added.get("shows") or 0)
            + int(existing.get("movies") or 0)
            + int(existing.get("shows") or 0)
        )
        nf = d.get("not_found") or {}
        for t in ("movies", "shows"):
            for obj in nf.get(t) or []:
                m = id_minimal({"type": "movie" if t == "movies" else "show", "ids": obj.get("ids") or {}})
                unresolved.append({"item": m, "hint": "not_found"})
                _freeze_item(m, action="playlist:add", reasons=["not-found"])
        if ok > 0:
            _unfreeze_keys_if_present(accepted_keys)
        elif not unresolved:
            _log("ADD returned 200 but nothing added/existing")
    else:
        _log(f"ADD failed {r.status_code}: {(r.text or '')[:180]}")
        for m in accepted_minimals:
            _freeze_item(m, action="playlist:add", reasons=[f"http:{r.status_code}"])
    return ok, unresolved


def remove(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
    *,
    list_id: Any | None = None,
    list_slug: str | None = None,
    list_name: str | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    lid = _resolve_target_list(adapter, list_id=list_id, list_slug=list_slug, list_name=list_name)
    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})

    body, accepted_keys, unresolved, accepted_minimals = _batch(items)
    if not body:
        return 0, unresolved

    r = request_with_retries(
        sess,
        "POST",
        URL_LIST_REM_FMT.format(lid=lid),
        headers=headers,
        json=body,
        timeout=adapter.cfg.timeout,
        max_retries=adapter.cfg.max_retries,
    )
    ok = 0
    if r.status_code in (200, 201):
        d = r.json() or {}
        deleted = d.get("deleted") or d.get("removed") or {}
        ok = int(deleted.get("movies") or 0) + int(deleted.get("shows") or 0)
        nf = d.get("not_found") or {}
        for t in ("movies", "shows"):
            for obj in nf.get(t) or []:
                m = id_minimal({"type": "movie" if t == "movies" else "show", "ids": obj.get("ids") or {}})
                unresolved.append({"item": m, "hint": "not_found"})
                _freeze_item(m, action="playlist:remove", reasons=["not-found"])
        if ok > 0:
            _unfreeze_keys_if_present(accepted_keys)
    else:
        _log(f"REMOVE failed {r.status_code}: {(r.text or '')[:180]}")
        for m in accepted_minimals:
            _freeze_item(m, action="playlist:remove", reasons=[f"http:{r.status_code}"])
    return ok, unresolved
