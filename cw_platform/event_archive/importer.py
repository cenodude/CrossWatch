# cw_platform/event_archive/importer.py
# CrossWatch - Read-only JSON bridge
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .db import get_conn
from .recorder import record_events, make_event, _item_fields, _as_map, _as_list

_LOG = logging.getLogger("crosswatch.event_archive")


def _config_base() -> Path:
    try:
        from ..config_base import CONFIG_BASE
        return CONFIG_BASE()
    except Exception:
        return Path("/config")


def _read_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return None


def _parse_state_filename(name: str) -> tuple[str, str, str, str] | None:
    if not name.endswith(".json"):
        return None
    stem = name[:-5]
    head, sep, rest = stem.partition(".")
    if not sep or "_" not in head:
        return None
    provider, _, feature = head.partition("_")
    if not provider or not feature:
        return None
    for token in ("unresolved.pending", "unresolved", "blackbox", "flap"):
        if token in rest:
            scope = rest.replace(token, "").strip(".")
            return provider.upper(), feature.lower(), token, scope
    return None


def _pair_from_scope(scope: str) -> str | None:
    s = str(scope or "").strip()
    if not s or s in ("unscoped", "default"):
        return None
    return s


def import_all(
    *,
    state_dir: str | Path | None = None,
    reports_dir: str | Path | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "imported": 0, "files": 0, "error": "db_unavailable"}

    base = _config_base()
    sdir = Path(state_dir) if state_dir is not None else (base / ".cw_state")
    rdir = Path(reports_dir) if reports_dir is not None else (base / "sync_reports")

    total_rows = 0
    total_files = 0

    files: list[Path] = []
    if sdir.exists():
        try:
            files = [p for p in sdir.iterdir() if p.is_file() and p.suffix == ".json"]
        except Exception:
            files = []
    if rdir.exists():
        try:
            files += [p for p in rdir.glob("sync-*.json") if p.is_file()]
        except Exception:
            pass

    for p in files:
        try:
            st = p.stat()
        except Exception:
            continue
        if not _needs_import(c, p, st):
            continue
        rows = _rows_for_file(p)
        n = record_events(rows, conn=c) if rows else 0
        _mark_imported(c, p, st, len(rows))
        total_rows += n
        total_files += 1

    try:
        from .groups import correlate
        correlate(conn=c)
    except Exception as exc:
        _LOG.warning("event correlation after import failed: %s", exc)

    return {"ok": True, "imported": total_rows, "files": total_files}


def _needs_import(conn: sqlite3.Connection, p: Path, st) -> bool:
    try:
        row = conn.execute(
            "SELECT source_mtime, source_size FROM event_imports WHERE source_file=?",
            (str(p),),
        ).fetchone()
    except Exception:
        return True
    if not row:
        return True
    return not (int(row[0] or 0) == int(st.st_mtime) and int(row[1] or 0) == int(st.st_size))


def _mark_imported(conn: sqlite3.Connection, p: Path, st, rows: int) -> None:
    try:
        with conn:
            conn.execute(
                "INSERT INTO event_imports (source_file, source_mtime, source_size, last_imported_at, imported_rows) "
                "VALUES (?,?,?,?,?) ON CONFLICT(source_file) DO UPDATE SET source_mtime=excluded.source_mtime, "
                "source_size=excluded.source_size, last_imported_at=excluded.last_imported_at, imported_rows=excluded.imported_rows",
                (str(p), int(st.st_mtime), int(st.st_size), int(time.time()), int(rows)),
            )
    except Exception as exc:
        _LOG.warning("event archive import mark failed: %s", exc)


def _rows_for_file(p: Path) -> list[dict[str, Any]]:
    name = p.name
    if name == "tombstones.json":
        return _rows_tombstones(p)
    if name.startswith("sync-"):
        return _rows_report(p)
    parsed = _parse_state_filename(name)
    if not parsed:
        return []
    provider, feature, kind, scope = parsed
    data = _read_json(p)
    if not isinstance(data, Mapping):
        return []
    pair = _pair_from_scope(scope)
    src_file = str(p)
    if kind == "unresolved.pending":
        return _rows_unresolved_pending(data, provider, feature, pair, src_file)
    if kind == "unresolved":
        return _rows_unresolved_blocking(data, provider, feature, pair, src_file)
    if kind == "blackbox":
        return _rows_blackbox(data, provider, feature, pair, src_file)
    if kind == "flap":
        return _rows_flap(data, provider, feature, pair, src_file)
    return []


def _mk(**kw: Any) -> dict[str, Any]:
    kw.setdefault("source_kind", "json_import")
    return make_event(**kw)


def _rows_unresolved_blocking(data: Mapping[str, Any], provider, feature, pair, src_file) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ck, meta in data.items():
        m = meta if isinstance(meta, Mapping) else {}
        ts = int(m.get("ts") or 0)
        out.append(_mk(
            event_type="unresolved_recorded", operation="add", severity="warn",
            feature=feature, pair_key=pair, destination_provider=provider,
            origin_confidence="unknown", item_key=str(ck),
            reason_code=str(m.get("reason") or ""), reason=str(m.get("reason") or ""),
            created_at=ts or None, source_mtime=ts or None, source_file=src_file,
        ))
    return out


def _rows_unresolved_pending(data: Mapping[str, Any], provider, feature, pair, src_file) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    keys = _as_list(data.get("keys"))
    items = _as_map(data.get("items"))
    hints = _as_map(data.get("hints"))
    for ck in keys:
        ck = str(ck)
        hint = _as_map(hints.get(ck))
        it = _as_map(items.get(ck))
        ts = int((hint or {}).get("ts") or 0)
        out.append(_mk(
            event_type="unresolved_recorded", operation="add", severity="warn",
            feature=feature, pair_key=pair, destination_provider=provider,
            origin_confidence="unknown", item_key=ck,
            reason_code=str((hint or {}).get("reason") or ""), reason=str((hint or {}).get("reason") or ""),
            created_at=ts or None, source_mtime=ts or None, source_file=src_file,
            **_item_fields(it),
        ))
    return out


def _rows_blackbox(data: Mapping[str, Any], provider, feature, pair, src_file) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ck, meta in data.items():
        m = meta if isinstance(meta, Mapping) else {}
        since = int(m.get("since") or 0)
        out.append(_mk(
            event_type="blackbox_promoted", operation="quarantine", severity="warn",
            feature=feature, pair_key=pair, destination_provider=provider,
            origin_confidence="unknown", item_key=str(ck),
            reason_code=str(m.get("reason") or "flapper"), reason=str(m.get("reason") or ""),
            created_at=since or None, source_mtime=since or None, source_file=src_file,
        ))
    return out


def _rows_flap(data: Mapping[str, Any], provider, feature, pair, src_file) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ck, meta in data.items():
        m = meta if isinstance(meta, Mapping) else {}
        cons = int(m.get("consecutive") or 0)
        op = str(m.get("last_op") or "") or None
        if cons > 0:
            ts = int(m.get("last_attempt_ts") or 0)
            out.append(_mk(
                event_type="write_failed", operation=op, severity="warn",
                feature=feature, pair_key=pair, destination_provider=provider,
                origin_confidence="unknown", item_key=str(ck),
                reason_code=str(m.get("last_reason") or ""), reason=str(m.get("last_reason") or ""),
                created_at=ts or None, source_mtime=ts or None, source_file=src_file,
                detail={"consecutive": cons},
            ))
    return out


def _rows_tombstones(p: Path) -> list[dict[str, Any]]:
    data = _read_json(p)
    if not isinstance(data, Mapping):
        return []
    keys = _as_map(data.get("keys"))
    out: list[dict[str, Any]] = []
    src_file = str(p)
    for raw, ts in keys.items():
        k = str(raw)
        feature = ""
        pair = None
        token = k
        if "|" in k:
            prefix, _, token = k.partition("|")
            fpart, _, ppart = prefix.partition(":")
            feature = fpart.lower()
            pair = ppart or None
        try:
            tsi = int(ts)
        except Exception:
            tsi = 0
        out.append(_mk(
            event_type="tombstone_created", operation="remove", severity="info",
            feature=feature or None, pair_key=pair, item_key=token,
            origin_confidence="unknown",
            created_at=tsi or None, source_mtime=tsi or None, source_file=src_file,
        ))
    return out


def _rows_report(p: Path) -> list[dict[str, Any]]:
    return []
