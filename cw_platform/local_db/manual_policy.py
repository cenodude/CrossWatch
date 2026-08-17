# cw_platform/local_db/manual_policy.py
# CrossWatch - SQLite-backed manual policy storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .db import crosswatch_db_path, get_conn
from .schema import ID_KEYS

_LOCK = threading.RLock()
_FEATURES = ("watchlist", "history", "ratings", "progress", "playlists")


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


def _scalar_from_values(value_text: Any, value_int: Any, value_real: Any, value_type: Any) -> Any:
    typ = str(value_type or "")
    if typ == "bool":
        return bool(value_int)
    if typ == "int":
        return value_int
    if typ == "real":
        return value_real
    if typ == "text":
        return value_text
    return None


def _get_meta(conn: Any, key: str) -> Any:
    row = conn.execute(
        "SELECT value_text,value_int,value_real,value_type FROM local_meta WHERE key=?",
        (key,),
    ).fetchone()
    if row is None:
        return None
    return _scalar_from_values(row["value_text"], row["value_int"], row["value_real"], row["value_type"])


def _set_meta(conn: Any, key: str, value: Any, ts: int) -> None:
    text, iv, rv, typ = _scalar_parts(value)
    if typ is None:
        conn.execute("DELETE FROM local_meta WHERE key=?", (key,))
        return
    conn.execute(
        "INSERT INTO local_meta(key,value_text,value_int,value_real,value_type,updated_at) VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text,value_int=excluded.value_int,"
        "value_real=excluded.value_real,value_type=excluded.value_type,updated_at=excluded.updated_at",
        (key, text, iv, rv, typ, ts),
    )


def _normalize_instance(value: Any) -> str:
    raw = str(value or "").strip()
    return "default" if not raw or raw.lower() == "default" else raw


def _normalize_blocks(value: Any) -> list[str]:
    raw = list(value.keys()) if isinstance(value, Mapping) else value
    out: list[str] = []
    seen: set[str] = set()
    if not isinstance(raw, (list, tuple, set)):
        return out
    for item in raw:
        text = str(item or "").strip()
        if not text:
            continue
        low = text.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(text)
    return out


def _feature_blocks(policy: Mapping[str, Any]) -> list[tuple[str, str, str, Mapping[str, Any]]]:
    out: list[tuple[str, str, str, Mapping[str, Any]]] = []
    providers = policy.get("providers")
    if not isinstance(providers, Mapping):
        return out

    def collect(provider: str, instance: str, node: Mapping[str, Any]) -> None:
        manual = node.get("manual")
        if isinstance(manual, Mapping):
            for feature, block in manual.items():
                feat = str(feature or "").strip().lower()
                if feat and isinstance(block, Mapping):
                    out.append((provider, instance, feat, block))
        for feature, block in node.items():
            feat = str(feature or "").strip().lower()
            if feat in ("instances", "manual") or not isinstance(block, Mapping):
                continue
            if feat in _FEATURES or "blocks" in block or "adds" in block:
                out.append((provider, instance, feat, block))

    for provider_raw, node in providers.items():
        provider = str(provider_raw or "").strip()
        if not provider or not isinstance(node, Mapping):
            continue
        collect(provider, "default", node)
        instances = node.get("instances")
        if isinstance(instances, Mapping):
            for instance_raw, instance_node in instances.items():
                if isinstance(instance_node, Mapping):
                    collect(provider, _normalize_instance(instance_raw), instance_node)
    return out


def _item_to_row(feature_id: int, ordinal: int, item_key: str, item: Mapping[str, Any], ts: int) -> tuple[Any, ...]:
    raw_ids = item.get("ids")
    ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
    raw_show_ids = item.get("show_ids")
    show_ids: Mapping[str, Any] = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
    return (
        feature_id,
        str(item_key),
        int(ordinal),
        _s(item.get("type")),
        _s(item.get("title")),
        _s(item.get("name")),
        _i(item.get("year")),
        _i(item.get("season")),
        _i(item.get("episode")),
        _s(item.get("series_title")),
        _s(item.get("show_title")),
        *[_s(ids.get(k)) for k in ID_KEYS],
        *[_s(show_ids.get(k)) for k in ID_KEYS],
        _b(item.get("watched")) if "watched" in item else None,
        _s(item.get("watched_at")),
        _s(item.get("last_watched_at")),
        _f(item.get("rating")),
        _f(item.get("user_rating")),
        _s(item.get("rated_at")),
        _s(item.get("user_rated_at")),
        _i(item.get("progress_ms")),
        _f(item.get("progress_percent")),
        _i(item.get("duration_ms")),
        _s(item.get("progress_at")),
        _s(item.get("progress_at_source")),
        _s(item.get("provider_item_id")),
        _s(item.get("provider_event_id")),
        ts,
    )


def _row_to_item(row: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col, key in (
        ("media_type", "type"),
        ("title", "title"),
        ("name", "name"),
        ("year", "year"),
        ("season", "season"),
        ("episode", "episode"),
        ("series_title", "series_title"),
        ("show_title", "show_title"),
        ("watched_at", "watched_at"),
        ("last_watched_at", "last_watched_at"),
        ("rated_at", "rated_at"),
        ("user_rated_at", "user_rated_at"),
        ("progress_at", "progress_at"),
        ("progress_at_source", "progress_at_source"),
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
    if row["user_rating"] is not None:
        out["user_rating"] = row["user_rating"]
    if row["progress_ms"] is not None:
        out["progress_ms"] = row["progress_ms"]
    if row["progress_percent"] is not None:
        out["progress_percent"] = row["progress_percent"]
    if row["duration_ms"] is not None:
        out["duration_ms"] = row["duration_ms"]
    ids = {k: row[f"ids_{k}"] for k in ID_KEYS if row[f"ids_{k}"] not in (None, "")}
    show_ids = {k: row[f"show_ids_{k}"] for k in ID_KEYS if row[f"show_ids_{k}"] not in (None, "")}
    if ids:
        out["ids"] = ids
    if show_ids:
        out["show_ids"] = show_ids
    return out


def load_policy(base_path: str | Path, policy_path: str | Path | None = None) -> dict[str, Any]:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return {"version": 1, "providers": {}}
        version = _get_meta(conn, "manual_policy_version") or 1
        out: dict[str, Any] = {"version": version, "providers": {}}
        rows = conn.execute(
            "SELECT * FROM manual_policy_features ORDER BY ordinal,id"
        ).fetchall()
        providers = out["providers"]
        for feature_row in rows:
            provider = str(feature_row["provider"] or "").strip()
            instance = _normalize_instance(feature_row["instance"])
            feature = str(feature_row["feature"] or "").strip().lower()
            if not provider or not feature:
                continue
            node = providers.setdefault(provider, {})
            if instance != "default":
                insts = node.setdefault("instances", {})
                node = insts.setdefault(instance, {})
            blocks = [
                str(row["item_key"])
                for row in conn.execute(
                    "SELECT item_key FROM manual_policy_blocks WHERE feature_id=? ORDER BY ordinal,id",
                    (int(feature_row["id"]),),
                ).fetchall()
                if row["item_key"] not in (None, "")
            ]
            items_rows = conn.execute(
                "SELECT * FROM manual_policy_add_items WHERE feature_id=? ORDER BY ordinal,id",
                (int(feature_row["id"]),),
            ).fetchall()
            items = {str(row["item_key"]): _row_to_item(row) for row in items_rows if row["item_key"] not in (None, "")}
            node[feature] = {"blocks": blocks, "adds": {"items": items}}
        return out


def update_policy(base_path: str | Path, mutator: Any, policy_path: str | Path | None = None) -> tuple[dict[str, Any], Any]:
    with _LOCK:
        raw = load_policy(base_path, policy_path)
        if not isinstance(raw, dict):
            raw = {"version": 1, "providers": {}}
        if not isinstance(raw.get("providers"), dict):
            raw["providers"] = {}
        if "version" not in raw:
            raw["version"] = 1
        result = mutator(raw)
        save_policy(base_path, raw, policy_path)
        return raw, result


def save_policy(base_path: str | Path, policy: Mapping[str, Any], policy_path: str | Path | None = None) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        ts = _now()
        version = policy.get("version") if isinstance(policy, Mapping) else 1
        item_columns = [
            "feature_id",
            "item_key",
            "ordinal",
            "media_type",
            "title",
            "name",
            "year",
            "season",
            "episode",
            "series_title",
            "show_title",
            *[f"ids_{k}" for k in ID_KEYS],
            *[f"show_ids_{k}" for k in ID_KEYS],
            "watched",
            "watched_at",
            "last_watched_at",
            "rating",
            "user_rating",
            "rated_at",
            "user_rated_at",
            "progress_ms",
            "progress_percent",
            "duration_ms",
            "progress_at",
            "progress_at_source",
            "provider_item_id",
            "provider_event_id",
            "updated_at",
        ]
        item_sql = f"INSERT INTO manual_policy_add_items({','.join(item_columns)}) VALUES({','.join('?' for _ in item_columns)})"
        with conn:
            conn.execute("DELETE FROM manual_policy_add_items")
            conn.execute("DELETE FROM manual_policy_blocks")
            conn.execute("DELETE FROM manual_policy_features")
            _set_meta(conn, "manual_policy_version", version or 1, ts)
            for ordinal, (provider, instance, feature, block) in enumerate(_feature_blocks(policy)):
                cur = conn.execute(
                    "INSERT INTO manual_policy_features(provider,instance,feature,ordinal,updated_at) VALUES(?,?,?,?,?)",
                    (provider, instance, feature, ordinal, ts),
                )
                feature_id_raw = cur.lastrowid
                if feature_id_raw is None:
                    continue
                feature_id = int(feature_id_raw)
                blocks = _normalize_blocks(block.get("blocks"))
                if blocks:
                    conn.executemany(
                        "INSERT INTO manual_policy_blocks(feature_id,item_key,ordinal,updated_at) VALUES(?,?,?,?)",
                        [(feature_id, key, idx, ts) for idx, key in enumerate(blocks)],
                    )
                raw_adds = block.get("adds")
                adds: Mapping[str, Any] = raw_adds if isinstance(raw_adds, Mapping) else {}
                raw_items = adds.get("items")
                items: Mapping[str, Any] = raw_items if isinstance(raw_items, Mapping) else {}
                rows = [
                    _item_to_row(feature_id, idx, str(key), item, ts)
                    for idx, (key, item) in enumerate(items.items())
                    if str(key or "").strip() and isinstance(item, Mapping)
                ]
                if rows:
                    conn.executemany(item_sql, rows)


def fingerprint(base_path: str | Path, features: set[str] | list[str] | tuple[str, ...] | None = None) -> tuple[Any, ...] | None:
    wanted = sorted({str(feature or "").strip().lower() for feature in features or [] if str(feature or "").strip()})
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return None
        params: list[Any] = []
        where = ""
        if wanted:
            placeholders = ",".join("?" for _ in wanted)
            where = f" WHERE feature IN ({placeholders})"
            params = list(wanted)
        features_row = conn.execute(
            f"SELECT COUNT(*), COALESCE(MAX(updated_at),0) FROM manual_policy_features{where}",
            params,
        ).fetchone()
        join_where = f" WHERE f.feature IN ({','.join('?' for _ in wanted)})" if wanted else ""
        blocks_row = conn.execute(
            "SELECT COUNT(*), COALESCE(MAX(b.updated_at),0) "
            "FROM manual_policy_blocks b "
            "JOIN manual_policy_features f ON f.id=b.feature_id"
            f"{join_where}",
            params,
        ).fetchone()
        adds_row = conn.execute(
            "SELECT COUNT(*), COALESCE(MAX(i.updated_at),0) "
            "FROM manual_policy_add_items i "
            "JOIN manual_policy_features f ON f.id=i.feature_id"
            f"{join_where}",
            params,
        ).fetchone()
        meta_row = conn.execute(
            "SELECT value_int,value_text,updated_at FROM local_meta WHERE key='manual_policy_version'"
        ).fetchone()
        return (
            tuple(wanted),
            features_row[0],
            features_row[1],
            blocks_row[0],
            blocks_row[1],
            adds_row[0],
            adds_row[1],
            tuple(meta_row) if meta_row else None,
        )


def has_policy(base_path: str | Path, policy_path: str | Path | None = None) -> bool:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return False
        row = conn.execute("SELECT COUNT(*) FROM manual_policy_features").fetchone()
        return bool(row and int(row[0] or 0) > 0)


def policy_mtime(base_path: str | Path) -> int | None:
    try:
        path = crosswatch_db_path(base_path)
        return int(path.stat().st_mtime) if path.exists() else None
    except Exception:
        return None


def clear_policy(base_path: str | Path) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        with conn:
            conn.execute("DELETE FROM manual_policy_add_items")
            conn.execute("DELETE FROM manual_policy_blocks")
            conn.execute("DELETE FROM manual_policy_features")
            _set_meta(conn, "manual_policy_version", 1, _now())
