# cw_platform/local_db/statistics.py
# CrossWatch - SQLite-backed statistics storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .db import get_conn

_LOCK = threading.RLock()

_EVENT_COLUMNS = (
    "ts",
    "action",
    "feature",
    "item_key",
    "source",
    "title",
    "name",
    "media_type",
    "year",
    "season",
    "episode",
    "series_title",
    "show_title",
    "ids_tmdb",
    "ids_imdb",
    "ids_tvdb",
    "ids_simkl",
    "ids_slug",
    "show_ids_tmdb",
    "show_ids_imdb",
    "show_ids_tvdb",
    "show_ids_simkl",
    "show_ids_slug",
    "added_at",
    "listed_at",
    "watched_at",
    "rated_at",
    "last_watched_at",
    "user_rated_at",
    "sync_ts",
    "ingested_ts",
    "seen_ts",
    "updated_at",
)

_HTTP_COLUMNS = (
    "ts",
    "provider",
    "endpoint",
    "method",
    "status",
    "ok",
    "ms",
    "bytes_in",
    "bytes_out",
    "rate_remaining",
    "rate_reset",
    "updated_at",
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


def _set_statistics_meta(conn: Any, key: str, value: Any, ts: int) -> None:
    text, iv, rv, typ = _scalar_parts(value)
    if typ is None:
        conn.execute("DELETE FROM statistics_meta WHERE key=?", (key,))
        return
    conn.execute(
        "INSERT INTO statistics_meta(key,value_text,value_int,value_real,value_type,updated_at) VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text,value_int=excluded.value_int,"
        "value_real=excluded.value_real,value_type=excluded.value_type,updated_at=excluded.updated_at",
        (key, text, iv, rv, typ, ts),
    )


def _get_statistics_meta(conn: Any, key: str) -> Any:
    row = conn.execute(
        "SELECT value_text,value_int,value_real,value_type FROM statistics_meta WHERE key=?",
        (key,),
    ).fetchone()
    if row is None:
        return None
    return _scalar_from_values(row["value_text"], row["value_int"], row["value_real"], row["value_type"])


def default_statistics() -> dict[str, Any]:
    return {
        "events": [],
        "samples": [],
        "current": {},
        "current_by_feature": {},
        "counters": {"added": 0, "removed": 0},
        "last_run": {"added": 0, "removed": 0, "ts": 0},
        "http": {"events": [], "counters": {}, "last": {}},
        "feature_totals": [],
        "ingested_runs": [],
    }


def ensure_shape(data: Mapping[str, Any] | None) -> dict[str, Any]:
    out = dict(data or {})
    defaults = default_statistics()
    for key, value in defaults.items():
        if key not in out or not isinstance(out.get(key), type(value)):
            out[key] = value
    http = out.get("http")
    if not isinstance(http, dict):
        http = {"events": [], "counters": {}, "last": {}}
        out["http"] = http
    http.setdefault("events", [])
    http.setdefault("counters", {})
    http.setdefault("last", {})
    counters = out.get("counters")
    if not isinstance(counters, dict):
        counters = {"added": 0, "removed": 0}
        out["counters"] = counters
    counters.setdefault("added", 0)
    counters.setdefault("removed", 0)
    last_run = out.get("last_run")
    if not isinstance(last_run, dict):
        last_run = {"added": 0, "removed": 0, "ts": 0}
        out["last_run"] = last_run
    last_run.setdefault("added", 0)
    last_run.setdefault("removed", 0)
    last_run.setdefault("ts", 0)
    return out


def _event_to_row(event: Mapping[str, Any], ts: int) -> tuple[Any, ...]:
    raw_ids = event.get("ids")
    ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
    raw_show_ids = event.get("show_ids")
    show_ids: Mapping[str, Any] = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
    return (
        _i(event.get("ts")) or 0,
        _s(event.get("action")),
        _s(event.get("feature")),
        _s(event.get("key")),
        _s(event.get("source")),
        _s(event.get("title")),
        _s(event.get("name")),
        _s(event.get("type")),
        _i(event.get("year")),
        _i(event.get("season")),
        _i(event.get("episode")),
        _s(event.get("series_title")),
        _s(event.get("show_title")),
        _s(ids.get("tmdb")),
        _s(ids.get("imdb")),
        _s(ids.get("tvdb")),
        _s(ids.get("simkl")),
        _s(ids.get("slug")),
        _s(show_ids.get("tmdb")),
        _s(show_ids.get("imdb")),
        _s(show_ids.get("tvdb")),
        _s(show_ids.get("simkl")),
        _s(show_ids.get("slug")),
        _s(event.get("added_at")),
        _s(event.get("listed_at")),
        _s(event.get("watched_at")),
        _s(event.get("rated_at")),
        _s(event.get("last_watched_at")),
        _s(event.get("user_rated_at")),
        _i(event.get("sync_ts")),
        _i(event.get("ingested_ts")),
        _i(event.get("seen_ts")),
        ts,
    )


def _row_to_event(row: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"ts": int(row["ts"] or 0)}
    for col, key in (
        ("action", "action"),
        ("feature", "feature"),
        ("item_key", "key"),
        ("source", "source"),
        ("title", "title"),
        ("name", "name"),
        ("media_type", "type"),
        ("year", "year"),
        ("season", "season"),
        ("episode", "episode"),
        ("series_title", "series_title"),
        ("show_title", "show_title"),
        ("added_at", "added_at"),
        ("listed_at", "listed_at"),
        ("watched_at", "watched_at"),
        ("rated_at", "rated_at"),
        ("last_watched_at", "last_watched_at"),
        ("user_rated_at", "user_rated_at"),
        ("sync_ts", "sync_ts"),
        ("ingested_ts", "ingested_ts"),
        ("seen_ts", "seen_ts"),
    ):
        value = row[col]
        if value not in (None, ""):
            out[key] = value
    ids = {
        key: row[f"ids_{key}"]
        for key in ("tmdb", "imdb", "tvdb", "simkl", "slug")
        if row[f"ids_{key}"] not in (None, "")
    }
    show_ids = {
        key: row[f"show_ids_{key}"]
        for key in ("tmdb", "imdb", "tvdb", "simkl", "slug")
        if row[f"show_ids_{key}"] not in (None, "")
    }
    if ids:
        out["ids"] = ids
    if show_ids:
        out["show_ids"] = show_ids
    return out


def _http_to_row(event: Mapping[str, Any], ts: int) -> tuple[Any, ...]:
    return (
        _i(event.get("ts")) or 0,
        str(event.get("provider") or "UNKNOWN").upper(),
        _s(event.get("endpoint")),
        str(event.get("method") or "").upper() or None,
        _i(event.get("status")),
        _b(event.get("ok")),
        _i(event.get("ms")),
        _i(event.get("bytes_in")),
        _i(event.get("bytes_out")),
        _i(event.get("rate_remaining")),
        _s(event.get("rate_reset")),
        ts,
    )


def _row_to_http(row: Any) -> dict[str, Any]:
    out: dict[str, Any] = {
        "ts": int(row["ts"] or 0),
        "provider": str(row["provider"] or "UNKNOWN"),
        "endpoint": row["endpoint"] or "",
        "method": row["method"] or "",
        "status": int(row["status"] or 0),
        "ok": bool(row["ok"]),
        "ms": int(row["ms"] or 0),
        "bytes_in": int(row["bytes_in"] or 0),
        "bytes_out": int(row["bytes_out"] or 0),
    }
    if row["rate_remaining"] is not None:
        out["rate_remaining"] = int(row["rate_remaining"])
    if row["rate_reset"]:
        out["rate_reset"] = row["rate_reset"]
    return out


def load_statistics(base_path: str | Path) -> dict[str, Any]:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return default_statistics()
        out = default_statistics()
        generated_at = _get_statistics_meta(conn, "generated_at")
        if generated_at:
            out["generated_at"] = generated_at
        events = conn.execute("SELECT * FROM statistics_events ORDER BY id").fetchall()
        out["events"] = [_row_to_event(row) for row in events]
        samples = conn.execute(
            "SELECT ts,count FROM statistics_samples WHERE feature='watchlist' ORDER BY id"
        ).fetchall()
        out["samples"] = [{"ts": int(row["ts"] or 0), "count": int(row["count"] or 0)} for row in samples]
        counters = conn.execute("SELECT name,value FROM statistics_counters ORDER BY name").fetchall()
        if counters:
            out["counters"] = {str(row["name"]): int(row["value"] or 0) for row in counters}
        last_run = conn.execute("SELECT added,removed,updated,ts FROM statistics_last_run WHERE id=1").fetchone()
        if last_run is not None:
            out["last_run"] = {
                "added": int(last_run["added"] or 0),
                "removed": int(last_run["removed"] or 0),
                "updated": int(last_run["updated"] or 0),
                "ts": int(last_run["ts"] or 0),
            }
        current_rows = conn.execute("SELECT * FROM statistics_current_items ORDER BY feature,item_key").fetchall()
        current_by_feature: dict[str, dict[str, Any]] = {}
        for row in current_rows:
            feature = str(row["feature"] or "watchlist")
            item_key = str(row["item_key"] or "")
            if not item_key:
                continue
            providers = conn.execute(
                "SELECT provider FROM statistics_current_providers WHERE feature=? AND item_key=? ORDER BY provider",
                (feature, item_key),
            ).fetchall()
            item = {
                "src": row["src"] or "",
                "title": row["title"] or "",
                "type": row["media_type"] or "",
                "providers": [str(p["provider"]).lower() for p in providers],
            }
            current_by_feature.setdefault(feature, {})[item_key] = item
        out["current"] = dict(current_by_feature.get("watchlist") or {})
        out["current_by_feature"] = {k: v for k, v in current_by_feature.items() if k != "watchlist"}
        http_events = conn.execute("SELECT * FROM statistics_http_events ORDER BY id").fetchall()
        http_counters = conn.execute("SELECT * FROM statistics_http_counters ORDER BY provider").fetchall()
        http_last = conn.execute("SELECT * FROM statistics_http_last ORDER BY request_key").fetchall()
        out["http"] = {
            "events": [_row_to_http(row) for row in http_events],
            "counters": {
                str(row["provider"]): {
                    "calls": int(row["calls"] or 0),
                    "ok": int(row["ok"] or 0),
                    "err": int(row["err"] or 0),
                    "bytes_in": int(row["bytes_in"] or 0),
                    "bytes_out": int(row["bytes_out"] or 0),
                    "ms_sum": int(row["ms_sum"] or 0),
                    "last_status": int(row["last_status"] or 0),
                    "last_ok": bool(row["last_ok"]),
                    "last_at": int(row["last_at"] or 0),
                    "last_rate_remaining": row["last_rate_remaining"],
                }
                for row in http_counters
            },
            "last": {str(row["request_key"]): _row_to_http(row) for row in http_last},
        }
        feature_totals = conn.execute("SELECT * FROM statistics_feature_totals ORDER BY id").fetchall()
        out["feature_totals"] = [
            {
                "ts": int(row["ts"] or 0),
                "feature": row["feature"] or "",
                "added": int(row["added"] or 0),
                "removed": int(row["removed"] or 0),
                "updated": int(row["updated"] or 0),
                "src": row["src"] or "",
                "run_id": row["run_id"] or "",
                "kind": row["kind"] or "",
            }
            for row in feature_totals
        ]
        ingested = conn.execute("SELECT run_id FROM statistics_ingested_runs ORDER BY id").fetchall()
        out["ingested_runs"] = [str(row["run_id"] or "") for row in ingested if row["run_id"]]
        return ensure_shape(out)


def save_statistics(base_path: str | Path, data: Mapping[str, Any]) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        ts = _now()
        payload = ensure_shape(data)
        events = [e for e in list(payload.get("events") or [])[-5000:] if isinstance(e, Mapping)]
        samples = [s for s in list(payload.get("samples") or [])[-4000:] if isinstance(s, Mapping)]
        http = payload.get("http") if isinstance(payload.get("http"), Mapping) else {}
        http_events = [e for e in list((http or {}).get("events") or [])[-2000:] if isinstance(e, Mapping)]
        current_maps: dict[str, Mapping[str, Any]] = {}
        current = payload.get("current")
        if isinstance(current, Mapping):
            current_maps["watchlist"] = current
        current_by = payload.get("current_by_feature")
        if isinstance(current_by, Mapping):
            for feature, rows in current_by.items():
                if isinstance(rows, Mapping):
                    current_maps[str(feature or "watchlist")] = rows
        event_sql = f"INSERT INTO statistics_events({','.join(_EVENT_COLUMNS)}) VALUES({','.join('?' for _ in _EVENT_COLUMNS)})"
        http_sql = f"INSERT INTO statistics_http_events({','.join(_HTTP_COLUMNS)}) VALUES({','.join('?' for _ in _HTTP_COLUMNS)})"
        last_sql = f"INSERT INTO statistics_http_last(request_key,{','.join(_HTTP_COLUMNS)}) VALUES(?{',' + ','.join('?' for _ in _HTTP_COLUMNS)})"
        with conn:
            _set_statistics_meta(conn, "generated_at", payload.get("generated_at"), ts)
            for table in (
                "statistics_events",
                "statistics_samples",
                "statistics_current_providers",
                "statistics_current_items",
                "statistics_counters",
                "statistics_last_run",
                "statistics_http_events",
                "statistics_http_counters",
                "statistics_http_last",
                "statistics_feature_totals",
                "statistics_ingested_runs",
            ):
                conn.execute(f"DELETE FROM {table}")
            if events:
                conn.executemany(event_sql, [_event_to_row(e, ts) for e in events])
            if samples:
                conn.executemany(
                    "INSERT INTO statistics_samples(feature,ts,count,updated_at) VALUES(?,?,?,?)",
                    [
                        ("watchlist", _i(row.get("ts")) or 0, _i(row.get("count")) or 0, ts)
                        for row in samples
                    ],
                )
            for feature, rows in current_maps.items():
                for item_key, item_any in rows.items():
                    if not isinstance(item_any, Mapping):
                        continue
                    item = dict(item_any)
                    key = str(item_key or "")
                    if not key:
                        continue
                    conn.execute(
                        "INSERT INTO statistics_current_items(feature,item_key,src,title,media_type,updated_at) VALUES(?,?,?,?,?,?)",
                        (feature, key, _s(item.get("src")), _s(item.get("title")), _s(item.get("type")), ts),
                    )
                    providers = item.get("providers")
                    if isinstance(providers, (list, tuple, set)):
                        conn.executemany(
                            "INSERT OR IGNORE INTO statistics_current_providers(feature,item_key,provider) VALUES(?,?,?)",
                            [(feature, key, str(p or "").lower()) for p in providers if str(p or "").strip()],
                        )
            counters = payload.get("counters")
            if isinstance(counters, Mapping):
                conn.executemany(
                    "INSERT INTO statistics_counters(name,value,updated_at) VALUES(?,?,?)",
                    [(str(k or ""), _i(v) or 0, ts) for k, v in counters.items() if str(k or "")],
                )
            last_run = payload.get("last_run") if isinstance(payload.get("last_run"), Mapping) else {}
            conn.execute(
                "INSERT INTO statistics_last_run(id,added,removed,updated,ts,updated_at) VALUES(1,?,?,?,?,?)",
                (
                    _i((last_run or {}).get("added")) or 0,
                    _i((last_run or {}).get("removed")) or 0,
                    _i((last_run or {}).get("updated")) or 0,
                    _i((last_run or {}).get("ts")) or 0,
                    ts,
                ),
            )
            if http_events:
                conn.executemany(http_sql, [_http_to_row(e, ts) for e in http_events])
            http_counters = (http or {}).get("counters") if isinstance(http, Mapping) else {}
            if isinstance(http_counters, Mapping):
                conn.executemany(
                    "INSERT INTO statistics_http_counters(provider,calls,ok,err,bytes_in,bytes_out,ms_sum,last_status,last_ok,last_at,last_rate_remaining,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        (
                            str(provider or "UNKNOWN").upper(),
                            _i(row.get("calls")) or 0,
                            _i(row.get("ok")) or 0,
                            _i(row.get("err")) or 0,
                            _i(row.get("bytes_in")) or 0,
                            _i(row.get("bytes_out")) or 0,
                            _i(row.get("ms_sum")) or 0,
                            _i(row.get("last_status")) or 0,
                            _b(row.get("last_ok")) or 0,
                            _i(row.get("last_at")) or 0,
                            _i(row.get("last_rate_remaining")),
                            ts,
                        )
                        for provider, row in http_counters.items()
                        if isinstance(row, Mapping)
                    ],
                )
            http_last = (http or {}).get("last") if isinstance(http, Mapping) else {}
            if isinstance(http_last, Mapping):
                conn.executemany(
                    last_sql,
                    [
                        (str(key or ""), *_http_to_row(row, ts))
                        for key, row in http_last.items()
                        if str(key or "") and isinstance(row, Mapping)
                    ],
                )
            feature_totals = payload.get("feature_totals")
            if isinstance(feature_totals, list):
                conn.executemany(
                    "INSERT INTO statistics_feature_totals(ts,feature,added,removed,updated,src,run_id,kind,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    [
                        (
                            _i(row.get("ts")) or 0,
                            str(row.get("feature") or ""),
                            _i(row.get("added")) or 0,
                            _i(row.get("removed")) or 0,
                            _i(row.get("updated")) or 0,
                            _s(row.get("src")),
                            _s(row.get("run_id")),
                            _s(row.get("kind")),
                            ts,
                        )
                        for row in feature_totals[-400:]
                        if isinstance(row, Mapping)
                    ],
                )
            ingested = payload.get("ingested_runs")
            if isinstance(ingested, list):
                conn.executemany(
                    "INSERT OR IGNORE INTO statistics_ingested_runs(run_id,updated_at) VALUES(?,?)",
                    [(str(run_id or ""), ts) for run_id in ingested[-50:] if str(run_id or "")],
                )


def clear_statistics(base_path: str | Path) -> None:
    with _LOCK:
        conn = get_conn(base_path)
        if conn is None:
            return
        with conn:
            for table in (
                "statistics_events",
                "statistics_samples",
                "statistics_current_providers",
                "statistics_current_items",
                "statistics_counters",
                "statistics_last_run",
                "statistics_http_events",
                "statistics_http_counters",
                "statistics_http_last",
                "statistics_feature_totals",
                "statistics_ingested_runs",
                "statistics_meta",
            ):
                conn.execute(f"DELETE FROM {table}")
