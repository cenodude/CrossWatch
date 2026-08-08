# cw_platform/local_db/state.py
# CrossWatch - SQLite-backed state storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import re
import sqlite3
import threading
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .db import get_conn
from .schema import ID_KEYS

_LOCK = threading.RLock()
_EVENT_KEY_RE = re.compile(r"^(?P<base>.+)@(?P<event>(?:\d{7,}|id:.+))$")
_CACHE: dict[str, Any] = {"path": None, "fingerprint": None, "state": None}
_BASELINE_ITEM_COLUMNS = [
    "provider_state_id",
    "item_key",
    "base_key",
    "event_key",
    "media_type",
    "title",
    "year",
    "season",
    "episode",
    "series_title",
    *[f"ids_{k}" for k in ID_KEYS],
    *[f"show_ids_{k}" for k in ID_KEYS],
    "watched",
    "watched_at",
    "rating",
    "rated_at",
    "progress_ms",
    "progress_percent",
    "duration_ms",
    "progress_at",
    "progress_at_source",
    "history_id",
    "trakt_history_id",
    "simkl_bucket",
    "anime_type",
    "simkl_episode_number",
    "floppy_consumption_id",
    "floppy_list_item_id",
    "floppy_season",
    "floppy_episode",
    "trakt_number_abs",
    "cw_marked",
    "cw_instance",
    "provider_item_id",
    "provider_event_id",
    "updated_at",
]
_BASELINE_ITEM_COMPARE_COLUMNS = _BASELINE_ITEM_COLUMNS[1:-1]
_BASELINE_ITEM_INSERT_SQL = (
    f"INSERT INTO baseline_items({','.join(_BASELINE_ITEM_COLUMNS)}) "
    f"VALUES({','.join('?' for _ in _BASELINE_ITEM_COLUMNS)})"
)
_BASELINE_ITEM_UPDATE_COLUMNS = _BASELINE_ITEM_COLUMNS[2:]
_BASELINE_ITEM_UPDATE_SQL = (
    "UPDATE baseline_items SET "
    + ",".join(f"{column}=?" for column in _BASELINE_ITEM_UPDATE_COLUMNS)
    + " WHERE provider_state_id=? AND item_key=?"
)
_PFS_INSERT_SQL = (
    "INSERT INTO provider_feature_state(provider,instance,feature,mode,checkpoint_text,"
    "checkpoint_int,checkpoint_real,checkpoint_type,updated_at) VALUES(?,?,?,?,?,?,?,?,?)"
)


def _now() -> int:
    return int(time.time_ns())


def _s(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _i(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _f(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _b(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        return 0 if value.strip().lower() in {"0", "false", "no", "off", ""} else 1
    if isinstance(value, (int, float)):
        return 0 if value == 0 else 1
    return 1 if bool(value) else 0


def _event_parts(key: str) -> tuple[str, str | None]:
    m = _EVENT_KEY_RE.match(str(key or ""))
    if not m:
        return str(key or ""), None
    return m.group("base"), str(key)


def _iso_from_event_key(key: str) -> str | None:
    m = _EVENT_KEY_RE.match(str(key or ""))
    if not m:
        return None
    event = str(m.group("event") or "").split("~", 1)[0]
    if not event.isdigit():
        return None
    try:
        return datetime.fromtimestamp(int(event), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def _scalar_parts(value: Any) -> tuple[str | None, int | None, float | None, str | None]:
    if isinstance(value, bool):
        return None, 1 if value else 0, None, "bool"
    if isinstance(value, int):
        return None, int(value), None, "int"
    if isinstance(value, float):
        return None, None, float(value), "real"
    if value in (None, ""):
        return None, None, None, None
    return str(value), None, None, "text"


def _scalar_from_row(row: sqlite3.Row, prefix: str) -> Any:
    typ = row[f"{prefix}_type"]
    if typ == "bool":
        return bool(row[f"{prefix}_int"])
    if typ == "int":
        return row[f"{prefix}_int"]
    if typ == "real":
        return row[f"{prefix}_real"]
    if typ == "text":
        return row[f"{prefix}_text"]
    return None


def _set_meta(conn: sqlite3.Connection, key: str, value: Any, ts: int) -> None:
    text, iv, rv, typ = _scalar_parts(value)
    if typ is None:
        conn.execute("DELETE FROM state_meta WHERE key=?", (key,))
        return
    conn.execute(
        "INSERT INTO state_meta(key,value_text,value_int,value_real,value_type,updated_at) VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text,value_int=excluded.value_int,"
        "value_real=excluded.value_real,value_type=excluded.value_type,updated_at=excluded.updated_at",
        (key, text, iv, rv, typ, ts),
    )


def _get_meta(conn: sqlite3.Connection, key: str) -> Any:
    row = conn.execute(
        "SELECT value_text,value_int,value_real,value_type FROM state_meta WHERE key=?",
        (key,),
    ).fetchone()
    if row is None:
        return None
    return _scalar_from_row(row, "value")


def _item_to_row(provider_state_id: int, key: str, item: Mapping[str, Any], ts: int) -> tuple[Any, ...]:
    base_key, event_key = _event_parts(key)
    raw_ids = item.get("ids")
    ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
    raw_show_ids = item.get("show_ids")
    show_ids: Mapping[str, Any] = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
    watched_at = _s(item.get("watched_at")) or _iso_from_event_key(key)
    id_values = [_s(ids.get(k)) for k in ID_KEYS]
    show_id_values = [_s(show_ids.get(k)) for k in ID_KEYS]
    return (
        provider_state_id,
        str(key),
        base_key,
        event_key,
        _s(item.get("type")),
        _s(item.get("title")),
        _i(item.get("year")),
        _i(item.get("season")),
        _i(item.get("episode")),
        _s(item.get("series_title")),
        *id_values,
        *show_id_values,
        _b(item.get("watched")) if "watched" in item else None,
        watched_at,
        _f(item.get("rating")),
        _s(item.get("rated_at")),
        _i(item.get("progress_ms")),
        _f(item.get("progress_percent")),
        _i(item.get("duration_ms")),
        _s(item.get("progress_at")),
        _s(item.get("progress_at_source")),
        _s(item.get("history_id")),
        _s(item.get("_trakt_history_id")),
        _s(item.get("simkl_bucket")),
        _s(item.get("anime_type")),
        _i(item.get("_simkl_episode_number")),
        _s(item.get("_floppy_consumption_id")),
        _s(item.get("_floppy_list_item_id")),
        _i(item.get("_floppy_season")),
        _i(item.get("_floppy_episode")),
        _i(item.get("_trakt_number_abs")),
        _b(item.get("_cw_marked")) if "_cw_marked" in item else None,
        _s(item.get("_cw_instance")),
        _s(item.get("provider_item_id")),
        _s(item.get("provider_event_id") or item.get("_simkl_rewatch_id") or item.get("rewatch_id") or item.get("_publicmetadb_history_id")),
        ts,
    )


def _row_to_item(row: sqlite3.Row) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col, key in (
        ("media_type", "type"),
        ("title", "title"),
        ("year", "year"),
        ("season", "season"),
        ("episode", "episode"),
        ("series_title", "series_title"),
        ("watched_at", "watched_at"),
        ("rated_at", "rated_at"),
        ("progress_at", "progress_at"),
        ("progress_at_source", "progress_at_source"),
        ("history_id", "history_id"),
        ("trakt_history_id", "_trakt_history_id"),
        ("simkl_bucket", "simkl_bucket"),
        ("anime_type", "anime_type"),
        ("simkl_episode_number", "_simkl_episode_number"),
        ("floppy_consumption_id", "_floppy_consumption_id"),
        ("floppy_list_item_id", "_floppy_list_item_id"),
        ("floppy_season", "_floppy_season"),
        ("floppy_episode", "_floppy_episode"),
        ("trakt_number_abs", "_trakt_number_abs"),
        ("cw_instance", "_cw_instance"),
        ("provider_item_id", "provider_item_id"),
        ("provider_event_id", "provider_event_id"),
    ):
        value = row[col]
        if value not in (None, ""):
            out[key] = value
    if row["watched"] is not None:
        out["watched"] = bool(row["watched"])
    if row["rating"] is not None:
        out["rating"] = row["rating"]
    if row["progress_ms"] is not None:
        out["progress_ms"] = row["progress_ms"]
    if row["progress_percent"] is not None:
        out["progress_percent"] = row["progress_percent"]
    if row["duration_ms"] is not None:
        out["duration_ms"] = row["duration_ms"]
    if row["cw_marked"] is not None:
        out["_cw_marked"] = bool(row["cw_marked"])
    ids = {k: row[f"ids_{k}"] for k in ID_KEYS if row[f"ids_{k}"] not in (None, "")}
    show_ids = {k: row[f"show_ids_{k}"] for k in ID_KEYS if row[f"show_ids_{k}"] not in (None, "")}
    if ids:
        out["ids"] = ids
    if show_ids:
        out["show_ids"] = show_ids
    if "type" not in out:
        out["type"] = "movie"
    if "ids" not in out:
        out["ids"] = {}
    return out


def _iter_feature_blocks(state: Mapping[str, Any]) -> list[tuple[str, str, str, Mapping[str, Any]]]:
    out: list[tuple[str, str, str, Mapping[str, Any]]] = []
    providers = state.get("providers")
    if not isinstance(providers, Mapping):
        return out
    for provider_raw, provider_node in providers.items():
        provider = str(provider_raw or "").upper().strip()
        if not provider or not isinstance(provider_node, Mapping):
            continue
        _collect_node(out, provider, "default", provider_node)
        instances = provider_node.get("instances")
        if isinstance(instances, Mapping):
            for inst_raw, inst_node in instances.items():
                inst = str(inst_raw or "default").strip() or "default"
                if isinstance(inst_node, Mapping):
                    _collect_node(out, provider, inst, inst_node)
    return out


def _collect_node(out: list[tuple[str, str, str, Mapping[str, Any]]], provider: str, instance: str, node: Mapping[str, Any]) -> None:
    for feature_raw, block in node.items():
        feature = str(feature_raw or "").strip().lower()
        if feature in ("instances", "manual") or not isinstance(block, Mapping):
            continue
        if "baseline" not in block and "checkpoint" not in block:
            continue
        out.append((provider, instance, feature, block))


def _feature_key(provider: str, instance: str, feature: str) -> tuple[str, str, str]:
    return (
        str(provider or "").upper().strip(),
        str(instance or "default").strip() or "default",
        str(feature or "").lower().strip(),
    )


def _baseline_items(block: Mapping[str, Any]) -> Mapping[str, Any]:
    baseline = block.get("baseline")
    baseline = baseline if isinstance(baseline, Mapping) else {}
    raw_items = baseline.get("items")
    return raw_items if isinstance(raw_items, Mapping) else {}


def _feature_mode(items: Mapping[str, Any]) -> str:
    return "event" if any(_event_parts(str(k))[1] for k in items.keys()) else "collapsed"


def _stored_item_compare_map(conn: sqlite3.Connection, provider_state_id: int) -> dict[str, tuple[Any, ...]]:
    columns = ",".join(_BASELINE_ITEM_COMPARE_COLUMNS)
    rows = conn.execute(
        f"SELECT {columns} FROM baseline_items WHERE provider_state_id=?",
        (provider_state_id,),
    ).fetchall()
    return {str(row["item_key"]): tuple(row) for row in rows}


def _incoming_item_row_map(rows: list[tuple[Any, ...]]) -> dict[str, tuple[Any, ...]]:
    out: dict[str, tuple[Any, ...]] = {}
    for row in rows:
        key = str(row[1])
        if key:
            out[key] = row
    return out


def _replace_feature(
    conn: sqlite3.Connection,
    provider: str,
    instance: str,
    feature: str,
    block: Mapping[str, Any],
    ts: int,
) -> bool:
    provider, instance, feature = _feature_key(provider, instance, feature)
    if not provider or not feature:
        return False
    items = _baseline_items(block)
    rows = [
        _item_to_row(0, str(k), v, ts)
        for k, v in items.items()
        if isinstance(v, Mapping)
    ]
    mode = _feature_mode(items)
    checkpoint = _scalar_parts(block.get("checkpoint"))
    pfs = conn.execute(
        "SELECT * FROM provider_feature_state WHERE provider=? AND instance=? AND feature=?",
        (provider, instance, feature),
    ).fetchone()
    cp_text, cp_int, cp_real, cp_type = checkpoint
    if pfs is None:
        cur = conn.execute(_PFS_INSERT_SQL, (provider, instance, feature, mode, cp_text, cp_int, cp_real, cp_type, ts))
        pfs_id_raw = cur.lastrowid
        if pfs_id_raw is None:
            return False
        pfs_id = int(pfs_id_raw)
        if rows:
            conn.executemany(
                _BASELINE_ITEM_INSERT_SQL,
                [(pfs_id, *row[1:-1], ts) for row in rows],
            )
        return True

    pfs_id = int(pfs["id"])
    current_checkpoint = (
        pfs["checkpoint_text"],
        pfs["checkpoint_int"],
        pfs["checkpoint_real"],
        pfs["checkpoint_type"],
    )
    metadata_changed = str(pfs["mode"] or "") != mode or current_checkpoint != checkpoint
    existing = _stored_item_compare_map(conn, pfs_id)
    incoming_rows = _incoming_item_row_map(rows)
    incoming_compare = {key: tuple(row[1:-1]) for key, row in incoming_rows.items()}
    existing_keys = set(existing)
    incoming_keys = set(incoming_rows)
    delete_keys = sorted(existing_keys - incoming_keys)
    insert_rows = [incoming_rows[key] for key in sorted(incoming_keys - existing_keys)]
    update_rows = [
        incoming_rows[key]
        for key in sorted(existing_keys & incoming_keys)
        if existing[key] != incoming_compare[key]
    ]
    if not (metadata_changed or delete_keys or insert_rows or update_rows):
        return False

    conn.execute(
        "UPDATE provider_feature_state SET mode=?,checkpoint_text=?,checkpoint_int=?,"
        "checkpoint_real=?,checkpoint_type=?,updated_at=? WHERE id=?",
        (mode, cp_text, cp_int, cp_real, cp_type, ts, pfs_id),
    )
    if delete_keys:
        placeholders = ",".join("?" for _ in delete_keys)
        conn.execute(
            f"DELETE FROM baseline_items WHERE provider_state_id=? AND item_key IN ({placeholders})",
            (pfs_id, *delete_keys),
        )
    if insert_rows:
        conn.executemany(
            _BASELINE_ITEM_INSERT_SQL,
            [(pfs_id, *row[1:-1], ts) for row in insert_rows],
        )
    if update_rows:
        conn.executemany(
            _BASELINE_ITEM_UPDATE_SQL,
            [(*row[2:-1], ts, pfs_id, str(row[1])) for row in update_rows],
        )
    return True


def _fingerprint(conn: sqlite3.Connection) -> tuple[Any, ...]:
    a = conn.execute("SELECT COUNT(*), COALESCE(MAX(updated_at),0) FROM provider_feature_state").fetchone()
    b = conn.execute("SELECT COUNT(*), COALESCE(MAX(updated_at),0) FROM baseline_items").fetchone()
    c = conn.execute("SELECT value_int,value_text,updated_at FROM state_meta WHERE key='last_sync_epoch'").fetchone()
    return (a[0], a[1], b[0], b[1], tuple(c) if c else None)


def fingerprint(base_path: str | Path, features: set[str] | list[str] | tuple[str, ...] | None = None) -> tuple[Any, ...] | None:
    wanted = sorted({str(feature or "").strip().lower() for feature in features or [] if str(feature or "").strip()})
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return None
        if not wanted:
            return _fingerprint(conn)
        placeholders = ",".join("?" for _ in wanted)
        a = conn.execute(
            f"SELECT COUNT(*), COALESCE(MAX(updated_at),0) FROM provider_feature_state WHERE feature IN ({placeholders})",
            wanted,
        ).fetchone()
        b = conn.execute(
            "SELECT COUNT(*), COALESCE(MAX(b.updated_at),0) "
            "FROM baseline_items b "
            "JOIN provider_feature_state p ON p.id=b.provider_state_id "
            f"WHERE p.feature IN ({placeholders})",
            wanted,
        ).fetchone()
        c = conn.execute("SELECT value_int,value_text,updated_at FROM state_meta WHERE key='last_sync_epoch'").fetchone()
        return (tuple(wanted), a[0], a[1], b[0], b[1], tuple(c) if c else None)


def _invalidate() -> None:
    _CACHE["fingerprint"] = None
    _CACHE["state"] = None


def has_state(base_path: str | Path) -> bool:
    conn = get_conn(base_path)
    if conn is None:
        return False
    row = conn.execute("SELECT COUNT(*) FROM provider_feature_state").fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _build_state_from_feature_rows(conn: sqlite3.Connection, rows: list[sqlite3.Row]) -> dict[str, Any]:
    providers: dict[str, Any] = {}
    wall: list[dict[str, Any]] = []
    for pfs in rows:
        pnode = providers.setdefault(str(pfs["provider"]).upper(), {})
        target = pnode
        inst = str(pfs["instance"] or "default")
        if inst != "default":
            insts = pnode.setdefault("instances", {})
            target = insts.setdefault(inst, {})
        feature = str(pfs["feature"])
        feat_node: dict[str, Any] = {"baseline": {"items": {}}}
        checkpoint = _scalar_from_row(pfs, "checkpoint")
        if checkpoint is not None:
            feat_node["checkpoint"] = checkpoint
        items = conn.execute(
            "SELECT * FROM baseline_items WHERE provider_state_id=? ORDER BY item_key",
            (int(pfs["id"]),),
        ).fetchall()
        for row in items:
            item = _row_to_item(row)
            feat_node["baseline"]["items"][str(row["item_key"])] = item
            if feature == "watchlist":
                wall.append(item)
        target[feature] = feat_node
    return {
        "providers": providers,
        "wall": _dedupe_wall(wall),
        "last_sync_epoch": _get_meta(conn, "last_sync_epoch"),
    }


def load_state(base_path: str | Path) -> dict[str, Any]:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return {"providers": {}, "wall": [], "last_sync_epoch": None}
        path_key = str(Path(base_path).resolve())
        fp = _fingerprint(conn)
        if _CACHE.get("path") == path_key and _CACHE.get("fingerprint") == fp and isinstance(_CACHE.get("state"), dict):
            return copy.deepcopy(_CACHE["state"])
        rows = conn.execute(
            "SELECT * FROM provider_feature_state ORDER BY provider, instance, feature"
        ).fetchall()
        state = _build_state_from_feature_rows(conn, rows)
        _CACHE["path"] = path_key
        _CACHE["fingerprint"] = fp
        _CACHE["state"] = copy.deepcopy(state)
        return state


def load_state_features(base_path: str | Path, features: set[str] | list[str] | tuple[str, ...]) -> dict[str, Any]:
    wanted = sorted({str(feature or "").strip().lower() for feature in features or [] if str(feature or "").strip()})
    if not wanted:
        return {"providers": {}, "wall": [], "last_sync_epoch": None}
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return {"providers": {}, "wall": [], "last_sync_epoch": None}
        placeholders = ",".join("?" for _ in wanted)
        rows = conn.execute(
            f"SELECT * FROM provider_feature_state WHERE feature IN ({placeholders}) ORDER BY provider, instance, feature",
            wanted,
        ).fetchall()
        return _build_state_from_feature_rows(conn, rows)


def provider_feature_counts(base_path: str | Path, feature: str = "watchlist") -> dict[str, int]:
    feat = str(feature or "").strip().lower()
    if not feat:
        return {}
    conn = get_conn(base_path)
    if conn is None:
        return {}
    rows = conn.execute(
        "SELECT p.provider AS provider, COUNT(DISTINCT b.item_key) AS count "
        "FROM provider_feature_state p "
        "LEFT JOIN baseline_items b ON b.provider_state_id=p.id "
        "WHERE p.feature=? "
        "GROUP BY p.provider",
        (feat,),
    ).fetchall()
    return {
        str(row["provider"] or "").upper(): int(row["count"] or 0)
        for row in rows
        if str(row["provider"] or "").strip()
    }


def provider_names(base_path: str | Path, features: set[str] | list[str] | tuple[str, ...] | None = None) -> list[str]:
    conn = get_conn(base_path)
    if conn is None:
        return []
    wanted = sorted({str(feature or "").strip().lower() for feature in features or [] if str(feature or "").strip()})
    if wanted:
        placeholders = ",".join("?" for _ in wanted)
        rows = conn.execute(
            f"SELECT DISTINCT provider FROM provider_feature_state WHERE feature IN ({placeholders}) ORDER BY provider",
            wanted,
        ).fetchall()
    else:
        rows = conn.execute("SELECT DISTINCT provider FROM provider_feature_state ORDER BY provider").fetchall()
    return [str(row["provider"] or "").upper() for row in rows if str(row["provider"] or "").strip()]


def feature_inventory(base_path: str | Path) -> list[dict[str, Any]]:
    conn = get_conn(base_path)
    if conn is None:
        return []
    rows = conn.execute(
        "SELECT p.provider AS provider,p.instance AS instance,p.feature AS feature,p.mode AS mode,"
        "p.checkpoint_text AS checkpoint_text,p.checkpoint_int AS checkpoint_int,"
        "p.checkpoint_real AS checkpoint_real,p.checkpoint_type AS checkpoint_type,"
        "p.updated_at AS updated_at,COUNT(b.item_key) AS items "
        "FROM provider_feature_state p "
        "LEFT JOIN baseline_items b ON b.provider_state_id=p.id "
        "GROUP BY p.id ORDER BY p.provider,p.instance,p.feature"
    ).fetchall()
    return [
        {
            "provider": str(row["provider"] or "").upper(),
            "instance": str(row["instance"] or "default"),
            "feature": str(row["feature"] or "").lower(),
            "mode": str(row["mode"] or ""),
            "checkpoint": _scalar_from_row(row, "checkpoint"),
            "items": int(row["items"] or 0),
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def last_sync_epoch(base_path: str | Path) -> Any:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return None
        return _get_meta(conn, "last_sync_epoch")


def save_state(base_path: str | Path, state: Mapping[str, Any]) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        ts = _now()
        blocks = _iter_feature_blocks(state)
        incoming = {_feature_key(provider, instance, feature) for provider, instance, feature, _ in blocks}
        with conn:
            for provider, instance, feature, block in blocks:
                _replace_feature(conn, provider, instance, feature, block, ts)
            existing = conn.execute(
                "SELECT id,provider,instance,feature FROM provider_feature_state"
            ).fetchall()
            stale_ids = [
                int(row["id"])
                for row in existing
                if _feature_key(row["provider"], row["instance"], row["feature"]) not in incoming
            ]
            if stale_ids:
                placeholders = ",".join("?" for _ in stale_ids)
                conn.execute(
                    f"DELETE FROM provider_feature_state WHERE id IN ({placeholders})",
                    stale_ids,
                )
            if isinstance(state, Mapping):
                _set_meta(conn, "last_sync_epoch", state.get("last_sync_epoch"), ts)
        _invalidate()


def save_feature_baseline(
    base_path: str | Path,
    *,
    provider: str,
    instance: str = "default",
    feature: str,
    items: Mapping[str, Any],
    checkpoint: Any = None,
    last_sync_epoch: Any = None,
) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        ts = _now()
        block = {"baseline": {"items": items if isinstance(items, Mapping) else {}}, "checkpoint": checkpoint}
        with conn:
            _replace_feature(conn, provider, instance, feature, block, ts)
            if last_sync_epoch is not None:
                _set_meta(conn, "last_sync_epoch", last_sync_epoch, ts)
        _invalidate()


def save_feature_blocks(
    base_path: str | Path,
    blocks: Mapping[tuple[str, str, str], Mapping[str, Any]],
    *,
    last_sync_epoch: Any = None,
) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        ts = _now()
        with conn:
            for key, block in blocks.items():
                if not isinstance(key, tuple) or len(key) != 3 or not isinstance(block, Mapping):
                    continue
                provider, instance, feature = key
                _replace_feature(conn, provider, instance, feature, block, ts)
            if last_sync_epoch is not None:
                _set_meta(conn, "last_sync_epoch", last_sync_epoch, ts)
        _invalidate()


def set_last_sync_epoch(base_path: str | Path, value: Any) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        with conn:
            _set_meta(conn, "last_sync_epoch", value, _now())
        _invalidate()


def clear_state(base_path: str | Path) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        with conn:
            conn.execute("DELETE FROM baseline_items")
            conn.execute("DELETE FROM provider_feature_state")
            conn.execute("DELETE FROM state_meta")
        _invalidate()


def _dedupe_wall(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    try:
        from ..id_map import canonical_key
    except Exception:
        canonical_key = None  # type: ignore[assignment]
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in items:
        key = ""
        if canonical_key is not None:
            try:
                key = str(canonical_key(item) or "")
            except Exception:
                key = ""
        if not key:
            key = str(item.get("title") or id(item))
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(item))
    return out
