# cw_platform/local_db/sync_reports.py
# CrossWatch - SQLite-backed sync report storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from .db import get_conn
from .schema import ID_KEYS

_SPOTLIGHT_BUCKETS = {
    "spotlight_add": "add",
    "spotlight_remove": "remove",
    "spotlight_update": "update",
}
_SPOTLIGHT_KEYS = {
    "add": "spotlight_add",
    "remove": "spotlight_remove",
    "update": "spotlight_update",
}


def _now() -> int:
    return int(time.time_ns())


def _s(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _i(value: Any) -> int | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))
    except Exception:
        return None


def _f(value: Any) -> float | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _b(value: Any, default: bool = False) -> int:
    if value is None:
        return 1 if default else 0
    if isinstance(value, str):
        return 0 if value.strip().lower() in {"", "0", "false", "no", "off"} else 1
    return 1 if bool(value) else 0


def _epoch(value: Any) -> int:
    try:
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip()
        if not text:
            return 0
        if text.replace(".", "", 1).isdigit():
            return int(float(text))
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0


def _run_id(summary: Mapping[str, Any]) -> str:
    for key in ("run_id", "finished_at", "started_at", "raw_started_ts"):
        value = str(summary.get(key) or "").strip()
        if value:
            return value
    return str(time.time_ns())


def _id_map(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _spotlight_row(run_id: str, feature: str, bucket: str, ordinal: int, item: Mapping[str, Any], ts: int) -> tuple[Any, ...]:
    ids = _id_map(item.get("ids"))
    show_ids = _id_map(item.get("show_ids"))
    return (
        run_id,
        feature,
        bucket,
        ordinal,
        _s(item.get("key")),
        _s(item.get("title")),
        _s(item.get("name")),
        _s(item.get("display_title")),
        _s(item.get("type")),
        _i(item.get("year")),
        _i(item.get("season")),
        _i(item.get("episode")),
        _s(item.get("series_title")),
        _s(item.get("show_title")),
        _s(item.get("source")),
        _i(item.get("ts")),
        *[_s(ids.get(key)) for key in ID_KEYS],
        *[_s(show_ids.get(key)) for key in ID_KEYS],
        ts,
    )


def _provider_counts(summary: Mapping[str, Any], phase: str) -> dict[str, int]:
    out: dict[str, int] = {}
    raw = summary.get(f"provider_counts_{phase}")
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            name = str(key or "").strip().lower()
            if name:
                out[name] = int(_i(value) or 0)
    suffix = f"_{phase}"
    for key, value in summary.items():
        if not isinstance(key, str) or not key.endswith(suffix):
            continue
        name = key[: -len(suffix)].strip().lower()
        if name and name not in {"provider_counts"}:
            out[name] = int(_i(value) or 0)
    return out


def base_path_from_report_dir(report_dir: str | Path | None) -> Path | None:
    if report_dir is None:
        return None
    path = Path(report_dir)
    return path.parent if path.name == "sync_reports" else path


def save_report(base_path: str | Path | None, summary: Mapping[str, Any]) -> str:
    conn = get_conn(base_path)
    if conn is None:
        return ""
    run_id = _run_id(summary)
    ts = _now()
    created_at = _epoch(summary.get("finished_at")) or _epoch(summary.get("started_at")) or _epoch(summary.get("raw_started_ts")) or int(time.time())
    features_raw = summary.get("features")
    features: Mapping[str, Any] = features_raw if isinstance(features_raw, Mapping) else {}
    enabled_raw = summary.get("features_enabled") or summary.get("enabled")
    enabled: Mapping[str, Any] = enabled_raw if isinstance(enabled_raw, Mapping) else {}
    timeline_raw = summary.get("timeline")
    timeline: Mapping[str, Any] = timeline_raw if isinstance(timeline_raw, Mapping) else {}
    provider_current_raw = summary.get("provider_counts")
    provider_current: Mapping[str, Any] = provider_current_raw if isinstance(provider_current_raw, Mapping) else {}

    spotlight_columns = [
        "run_id",
        "feature",
        "bucket",
        "ordinal",
        "item_key",
        "title",
        "name",
        "display_title",
        "media_type",
        "year",
        "season",
        "episode",
        "series_title",
        "show_title",
        "source",
        "ts",
        *[f"ids_{key}" for key in ID_KEYS],
        *[f"show_ids_{key}" for key in ID_KEYS],
        "updated_at",
    ]

    with conn:
        conn.execute(
            "INSERT INTO sync_run_reports(run_id,started_at,finished_at,raw_started_ts,duration_sec,result,exit_code,cmd,running,"
            "added_last,removed_last,updated_last,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(run_id) DO UPDATE SET started_at=excluded.started_at,finished_at=excluded.finished_at,"
            "raw_started_ts=excluded.raw_started_ts,duration_sec=excluded.duration_sec,result=excluded.result,"
            "exit_code=excluded.exit_code,cmd=excluded.cmd,running=excluded.running,added_last=excluded.added_last,"
            "removed_last=excluded.removed_last,updated_last=excluded.updated_last,created_at=excluded.created_at,updated_at=excluded.updated_at",
            (
                run_id,
                _s(summary.get("started_at")),
                _s(summary.get("finished_at")),
                _f(summary.get("raw_started_ts")),
                _f(summary.get("duration_sec")),
                _s(summary.get("result")),
                _i(summary.get("exit_code")),
                _s(summary.get("cmd")),
                _b(summary.get("running")),
                int(_i(summary.get("added_last")) or 0),
                int(_i(summary.get("removed_last")) or 0),
                int(_i(summary.get("updated_last")) or 0),
                created_at,
                ts,
            ),
        )
        for table in ("sync_run_timeline", "sync_run_provider_counts", "sync_run_feature_lanes", "sync_run_spotlight_items"):
            conn.execute(f"DELETE FROM {table} WHERE run_id=?", (run_id,))
        if timeline:
            conn.executemany(
                "INSERT INTO sync_run_timeline(run_id,flag,value,updated_at) VALUES(?,?,?,?)",
                [(run_id, str(key or ""), 1 if bool(value) else 0, ts) for key, value in timeline.items() if str(key or "").strip()],
            )
        provider_rows: list[tuple[str, str, str, int, int]] = []
        for phase in ("pre", "post"):
            for provider, value in _provider_counts(summary, phase).items():
                provider_rows.append((run_id, phase, provider, value, ts))
        for provider, value in provider_current.items():
            name = str(provider or "").strip().lower()
            if name:
                provider_rows.append((run_id, "current", name, int(_i(value) or 0), ts))
        if provider_rows:
            conn.executemany(
                "INSERT INTO sync_run_provider_counts(run_id,phase,provider,value,updated_at) VALUES(?,?,?,?,?)",
                provider_rows,
            )
        lane_rows: list[tuple[str, str, int, int, int, int, int]] = []
        spotlight_rows: list[tuple[Any, ...]] = []
        for feature, raw_lane in features.items():
            feature_name = str(feature or "").strip().lower()
            if not feature_name or not isinstance(raw_lane, Mapping):
                continue
            lane_rows.append(
                (
                    run_id,
                    feature_name,
                    _b(enabled.get(feature_name), True),
                    int(_i(raw_lane.get("added")) or 0),
                    int(_i(raw_lane.get("removed")) or 0),
                    int(_i(raw_lane.get("updated")) or 0),
                    ts,
                )
            )
            for spot_key, bucket in _SPOTLIGHT_BUCKETS.items():
                raw_items = raw_lane.get(spot_key)
                if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes, bytearray)):
                    continue
                for ordinal, item in enumerate(raw_items[:25]):
                    if isinstance(item, Mapping):
                        spotlight_rows.append(_spotlight_row(run_id, feature_name, bucket, ordinal, item, ts))
        if lane_rows:
            conn.executemany(
                "INSERT INTO sync_run_feature_lanes(run_id,feature,enabled,added,removed,updated,updated_at) VALUES(?,?,?,?,?,?,?)",
                lane_rows,
            )
        if spotlight_rows:
            conn.executemany(
                f"INSERT INTO sync_run_spotlight_items({','.join(spotlight_columns)}) VALUES({','.join('?' for _ in spotlight_columns)})",
                spotlight_rows,
            )
    return run_id


def _spotlight_item(row: Any) -> dict[str, Any]:
    item: dict[str, Any] = {}
    pairs = (
        ("key", "item_key"),
        ("title", "title"),
        ("name", "name"),
        ("display_title", "display_title"),
        ("type", "media_type"),
        ("year", "year"),
        ("season", "season"),
        ("episode", "episode"),
        ("series_title", "series_title"),
        ("show_title", "show_title"),
        ("source", "source"),
        ("ts", "ts"),
    )
    for out_key, col in pairs:
        value = row[col]
        if value not in (None, ""):
            item[out_key] = value
    ids = {key: row[f"ids_{key}"] for key in ID_KEYS if row[f"ids_{key}"] not in (None, "")}
    show_ids = {key: row[f"show_ids_{key}"] for key in ID_KEYS if row[f"show_ids_{key}"] not in (None, "")}
    if ids:
        item["ids"] = ids
    if show_ids:
        item["show_ids"] = show_ids
    return item


def list_reports(
    base_path: str | Path | None = None,
    *,
    limit: int = 60,
    feature_keys: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    conn = get_conn(base_path)
    if conn is None:
        return []
    cap = max(0, int(limit or 0))
    if cap <= 0:
        return []
    rows = conn.execute(
        "SELECT * FROM sync_run_reports ORDER BY created_at DESC, updated_at DESC LIMIT ?",
        (cap,),
    ).fetchall()
    run_ids = [str(row["run_id"] or "") for row in rows if row["run_id"]]
    if not run_ids:
        return []
    placeholders = ",".join("?" for _ in run_ids)
    lane_rows = conn.execute(
        f"SELECT * FROM sync_run_feature_lanes WHERE run_id IN ({placeholders}) ORDER BY run_id, feature",
        run_ids,
    ).fetchall()
    spot_rows = conn.execute(
        f"SELECT * FROM sync_run_spotlight_items WHERE run_id IN ({placeholders}) ORDER BY run_id, feature, bucket, ordinal",
        run_ids,
    ).fetchall()
    provider_rows = conn.execute(
        f"SELECT * FROM sync_run_provider_counts WHERE run_id IN ({placeholders}) ORDER BY run_id, phase, provider",
        run_ids,
    ).fetchall()
    timeline_rows = conn.execute(
        f"SELECT * FROM sync_run_timeline WHERE run_id IN ({placeholders}) ORDER BY run_id, flag",
        run_ids,
    ).fetchall()

    features_by_run: dict[str, dict[str, dict[str, Any]]] = {}
    enabled_by_run: dict[str, dict[str, bool]] = {}
    for row in lane_rows:
        run_id = str(row["run_id"] or "")
        feature = str(row["feature"] or "")
        if not run_id or not feature:
            continue
        features_by_run.setdefault(run_id, {})[feature] = {
            "added": int(row["added"] or 0),
            "removed": int(row["removed"] or 0),
            "updated": int(row["updated"] or 0),
            "spotlight_add": [],
            "spotlight_remove": [],
            "spotlight_update": [],
        }
        enabled_by_run.setdefault(run_id, {})[feature] = bool(row["enabled"])

    for row in spot_rows:
        run_id = str(row["run_id"] or "")
        feature = str(row["feature"] or "")
        bucket = str(row["bucket"] or "")
        spot_key = _SPOTLIGHT_KEYS.get(bucket)
        if not run_id or not feature or not spot_key:
            continue
        lane = features_by_run.setdefault(run_id, {}).setdefault(
            feature,
            {"added": 0, "removed": 0, "updated": 0, "spotlight_add": [], "spotlight_remove": [], "spotlight_update": []},
        )
        lane.setdefault(spot_key, []).append(_spotlight_item(row))

    counts_by_run: dict[str, dict[str, dict[str, int]]] = {}
    for row in provider_rows:
        run_id = str(row["run_id"] or "")
        phase = str(row["phase"] or "")
        provider = str(row["provider"] or "")
        if run_id and phase and provider:
            counts_by_run.setdefault(run_id, {}).setdefault(phase, {})[provider] = int(row["value"] or 0)

    timeline_by_run: dict[str, dict[str, bool]] = {}
    for row in timeline_rows:
        run_id = str(row["run_id"] or "")
        flag = str(row["flag"] or "")
        if run_id and flag:
            timeline_by_run.setdefault(run_id, {})[flag] = bool(row["value"])

    out: list[dict[str, Any]] = []
    keys = [str(key).strip().lower() for key in (feature_keys or []) if str(key).strip()]
    for row in rows:
        run_id = str(row["run_id"] or "")
        features = features_by_run.get(run_id) or {}
        if keys:
            for key in keys:
                features.setdefault(key, {"added": 0, "removed": 0, "updated": 0, "spotlight_add": [], "spotlight_remove": [], "spotlight_update": []})
        enabled = enabled_by_run.get(run_id) or {}
        counts = counts_by_run.get(run_id) or {}
        provider_posts = dict(counts.get("post") or counts.get("current") or counts.get("pre") or {})
        report = {
            "run_id": run_id,
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "raw_started_ts": row["raw_started_ts"],
            "duration_sec": row["duration_sec"],
            "result": row["result"] or "",
            "exit_code": row["exit_code"],
            "added": int(row["added_last"] or 0),
            "removed": int(row["removed_last"] or 0),
            "updated_total": int(row["updated_last"] or 0),
            "added_last": int(row["added_last"] or 0),
            "removed_last": int(row["removed_last"] or 0),
            "updated_last": int(row["updated_last"] or 0),
            "features": features,
            "features_enabled": enabled,
            "enabled": enabled,
            "provider_counts": dict(counts.get("current") or provider_posts),
            "provider_counts_pre": dict(counts.get("pre") or {}),
            "provider_counts_post": dict(counts.get("post") or {}),
            "provider_posts": provider_posts,
            "timeline": timeline_by_run.get(run_id) or {},
        }
        for provider, value in provider_posts.items():
            report[f"{provider}_post"] = value
        out.append(report)
    return out


def recent_feature_reports(base_path: str | Path | None = None, *, limit: int = 3) -> list[dict[str, Any]]:
    return list_reports(base_path, limit=limit)


def clear_reports(base_path: str | Path | None = None) -> int:
    conn = get_conn(base_path)
    if conn is None:
        return 0
    row = conn.execute("SELECT COUNT(*) AS c FROM sync_run_reports").fetchone()
    count = int(row["c"] or 0) if row is not None else 0
    with conn:
        conn.execute("DELETE FROM sync_run_reports")
    return count


def report_count(base_path: str | Path | None = None) -> int:
    conn = get_conn(base_path)
    if conn is None:
        return 0
    row = conn.execute("SELECT COUNT(*) AS c FROM sync_run_reports").fetchone()
    return int(row["c"] or 0) if row is not None else 0
