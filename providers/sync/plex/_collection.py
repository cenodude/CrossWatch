# /providers/sync/plex/_collection.py
# Plex library collection source
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key

from ._common import (
    active_pms_token,
    make_logger,
    minimal_from_history_row,
    plex_feature_library_ids,
)
from ._history import _fetch_section_guid_rows

_dbg, _info, _warn, _error, _log = make_logger("collection")


def _row_type(row: Mapping[str, Any], fallback: str) -> str:
    typ = str(row.get("type") or "").strip().lower()
    if typ:
        return typ
    return fallback


def _normalize_row(row: Mapping[str, Any], *, library_id: str, fallback_type: str, token: str | None) -> dict[str, Any] | None:
    raw = dict(row)
    raw["librarySectionID"] = str(library_id)
    raw.setdefault("type", fallback_type)
    item = minimal_from_history_row(raw, token=token, allow_discover=False)
    if not item:
        return None
    item = dict(item)
    item["type"] = "episode" if _row_type(raw, fallback_type) == "episode" else "movie"
    item["library_id"] = str(library_id)
    return item


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        _info("index_skipped", reason="no_pms")
        return {}

    allowed = plex_feature_library_ids(adapter, "collection")
    token = active_pms_token(adapter)
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("collection") if callable(prog_mk) else None

    sections: list[tuple[str, str, int]] = []
    found: set[str] = set()
    for sec in adapter.libraries(types=("movie", "show")) or []:
        sid = str(getattr(sec, "key", "") or "").strip()
        typ = str(getattr(sec, "type", "") or "").strip().lower()
        if not sid or typ not in {"movie", "show"}:
            continue
        found.add(sid)
        if allowed and sid not in allowed:
            continue
        sections.append((sid, typ, 1 if typ == "movie" else 4))

    missing = sorted(allowed - found)
    if missing:
        _warn("libraries_not_found", libraries=",".join(missing))

    out: dict[str, dict[str, Any]] = {}
    scanned = 0
    total_sections = max(1, len(sections))

    for index, (sid, lib_type, plex_type) in enumerate(sections, start=1):
        rows, requests_made = _fetch_section_guid_rows(srv, sid, plex_type)
        scanned += len(rows)
        fallback_type = "movie" if lib_type == "movie" else "episode"
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            item = _normalize_row(row, library_id=sid, fallback_type=fallback_type, token=token)
            if not item:
                continue
            key = canonical_key(item)
            if key:
                out[key] = item
        if prog:
            try:
                prog.tick(index, total=total_sections)
            except Exception:
                pass
        _dbg("section_scanned", library_id=sid, library_type=lib_type, rows=len(rows), requests=requests_made)

    _info("index_done", count=len(out), scanned=scanned, libraries=len(sections))
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    unresolved = [{"item": dict(item), "hint": "plex_collection_write_unsupported"} for item in items or []]
    _info("write_skipped", op="add", reason="unsupported", unresolved=len(unresolved))
    return 0, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    unresolved = [{"item": dict(item), "hint": "plex_collection_write_unsupported"} for item in items or []]
    _info("write_skipped", op="remove", reason="unsupported", unresolved=len(unresolved))
    return 0, unresolved
