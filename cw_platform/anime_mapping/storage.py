# /cw_platform/anime_mapping/storage.py
# CrossWatch - Anime Mapping Storage Utilities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from cw_platform.config_base import CONFIG_BASE

from .descriptors import Descriptor, parse_descriptor

SCHEMA_VERSION = 1
_RELEASE_TAG_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_PROVIDER = "anibridge"


def normalize_release_tag(value: Any = "v3") -> str:
    tag = str(value or "v3").strip() or "v3"
    return tag if _RELEASE_TAG_RE.fullmatch(tag) else "v3"


def _base_root() -> Path:
    return (CONFIG_BASE() / ".cw_cache" / "anime_mapping" / _PROVIDER).resolve()


def _safe_child(base: Path, *parts: str) -> Path:
    root = base.resolve()
    candidate = root.joinpath(*parts).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("Invalid anime mapping path") from exc
    return candidate


def _safe_existing_path(path: Path, *, base: Path | None = None) -> Path:
    root = (base or _base_root()).resolve()
    candidate = Path(path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("Invalid anime mapping path") from exc
    return candidate


def cache_root(release_tag: str = "v3") -> Path:
    tag = normalize_release_tag(release_tag)
    return _safe_child(_base_root(), tag)


def paths(release_tag: str = "v3") -> dict[str, Path]:
    root = cache_root(release_tag)
    return {
        "root": root,
        "stats": _safe_child(root, "stats.json"),
        "mappings": _safe_child(root, "mappings.min.json"),
        "db": _safe_child(root, "anime_mapping.sqlite"),
        "state": _safe_child(root, "state.json"),
    }


def read_json(path: Path) -> dict[str, Any]:
    path = _safe_existing_path(path)
    try:
        data = json.loads(path.read_text("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path = _safe_existing_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(dict(payload or {}), f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass


def read_state(release_tag: str = "v3") -> dict[str, Any]:
    return read_json(paths(release_tag)["state"])


def write_state(release_tag: str, patch: Mapping[str, Any]) -> dict[str, Any]:
    p = paths(release_tag)["state"]
    state = read_json(p)
    state.update(dict(patch or {}))
    write_json_atomic(p, state)
    return state


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path = _safe_existing_path(db_path)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con


def _init_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS mapping_edges (
          source_provider TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_scope TEXT NOT NULL DEFAULT '',
          source_kind TEXT NOT NULL DEFAULT '',
          target_provider TEXT NOT NULL,
          target_id TEXT NOT NULL,
          target_scope TEXT NOT NULL DEFAULT '',
          target_kind TEXT NOT NULL DEFAULT '',
          source_range TEXT NOT NULL DEFAULT '',
          target_range TEXT NOT NULL DEFAULT '',
          reverse INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_mapping_source
          ON mapping_edges(source_provider, source_id);
        CREATE INDEX IF NOT EXISTS idx_mapping_source_scope
          ON mapping_edges(source_provider, source_id, source_scope);
        CREATE INDEX IF NOT EXISTS idx_mapping_target
          ON mapping_edges(target_provider, target_id);
        CREATE TABLE IF NOT EXISTS meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        """
    )


def _insert_edge(
    rows: list[tuple[str, str, str, str, str, str, str, str, str, str, int]],
    src: Descriptor,
    dst: Descriptor,
    source_range: Any = "",
    target_range: Any = "",
    *,
    reverse: bool = False,
) -> None:
    rows.append(
        (
            src.provider,
            src.id,
            src.scope,
            src.media_kind,
            dst.provider,
            dst.id,
            dst.scope,
            dst.media_kind,
            str(source_range or ""),
            str(target_range or ""),
            1 if reverse else 0,
        )
    )


def rebuild_sqlite_from_mappings(
    *,
    release_tag: str = "v3",
    mappings_path: Path | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    pp = paths(release_tag)
    root = pp["root"]
    mappings_path = _safe_existing_path(mappings_path or pp["mappings"], base=root)
    db_path = _safe_existing_path(db_path or pp["db"], base=root)
    if not mappings_path.exists():
        raise FileNotFoundError("AniBridge mappings file is missing")

    data = json.loads(mappings_path.read_text("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("AniBridge mappings payload must be a JSON object")

    db_dir = db_path.parent
    db_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{db_path.name}.", suffix=".tmp", dir=str(db_dir))
    os.close(fd)
    tmp = _safe_existing_path(Path(tmp_name), base=db_dir)
    rows: list[tuple[str, str, str, str, str, str, str, str, str, str, int]] = []
    edge_count = 0
    source_count = 0

    try:
        for raw_src, targets in data.items():
            src = parse_descriptor(raw_src)
            if src is None or not isinstance(targets, Mapping):
                continue
            source_count += 1
            for raw_dst, ranges in targets.items():
                dst = parse_descriptor(raw_dst)
                if dst is None:
                    continue
                if isinstance(ranges, Mapping) and ranges:
                    for source_range, target_range in ranges.items():
                        _insert_edge(rows, src, dst, source_range, target_range)
                        _insert_edge(rows, dst, src, target_range, source_range, reverse=True)
                        edge_count += 2
                else:
                    _insert_edge(rows, src, dst)
                    _insert_edge(rows, dst, src, reverse=True)
                    edge_count += 2

        con = _connect(tmp)
        try:
            _init_schema(con)
            con.executemany(
                """
                INSERT INTO mapping_edges (
                  source_provider, source_id, source_scope, source_kind,
                  target_provider, target_id, target_scope, target_kind,
                  source_range, target_range, reverse
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("schema_version", str(SCHEMA_VERSION)))
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("built_at", str(int(time.time()))))
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("source_count", str(source_count)))
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("edge_count", str(edge_count)))
            con.commit()
        finally:
            con.close()

        os.replace(tmp, db_path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass

    write_state(
        release_tag,
        {
            "index_ready": True,
            "index_built_at": int(time.time()),
            "schema_version": SCHEMA_VERSION,
            "source_count": source_count,
            "edge_count": edge_count,
        },
    )
    return {"ok": True, "source_count": source_count, "edge_count": edge_count, "db_path": str(db_path)}


def query_edges(release_tag: str, provider: str, ident: str) -> list[dict[str, Any]]:
    db = _safe_existing_path(paths(release_tag)["db"])
    if not db.exists():
        return []
    p = str(provider or "").strip().lower()
    i = str(ident or "").strip()
    if not p or not i:
        return []
    con = _connect(db)
    try:
        rows = con.execute(
            """
            SELECT source_provider, source_id, source_scope, source_kind,
                   target_provider, target_id, target_scope, target_kind,
                   source_range, target_range, reverse
            FROM mapping_edges
            WHERE source_provider = ? AND source_id = ?
            LIMIT 500
            """,
            (p, i),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()
