# /cw_platform/anime_mapping/storage.py
# CrossWatch - Anime Mapping Storage Utilities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from cw_platform.config_base import CONFIG_BASE

from .descriptors import Descriptor, parse_descriptor

SCHEMA_VERSION = 4
_RELEASE_TAG_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_PROVIDER = "anibridge"
_EDGE_FANOUT_LIMIT = 5000
_READERS = threading.local()
_PATHS_CACHE: dict[tuple[str, str], dict[str, Path]] = {}
IDENTITY_NAMESPACES = ("anidb", "mal", "anilist")
_IDENTITY_COLUMNS = {"anidb": "anidb", "mal": "myanimelist", "anilist": "anilist"}
_IDENTITY_TARGET_COLUMNS = {"simkl": "simkl_id", "kitsu": "kitsu_id"}


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
    tag = normalize_release_tag(release_tag)
    key = (str(CONFIG_BASE()), tag)
    cached = _PATHS_CACHE.get(key)
    if cached is not None:
        return cached
    built = _build_paths(tag)
    _PATHS_CACHE[key] = built
    return built


def _build_paths(release_tag: str) -> dict[str, Path]:
    root = cache_root(release_tag)
    return {
        "root": root,
        "stats": _safe_child(root, "stats.json"),
        "mappings": _safe_child(root, "mappings.min.json"),
        "identity": _safe_child(root, "animeapi.tsv"),
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


def _reader(db_path: Path) -> sqlite3.Connection | None:
    try:
        st = db_path.stat()
    except OSError:
        return None
    key = (str(db_path), st.st_mtime_ns, st.st_size)
    cached = getattr(_READERS, "entry", None)
    if cached is not None:
        if cached[0] == key:
            return cached[1]
        try:
            cached[1].close()
        except Exception:
            pass
        _READERS.entry = None
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    _READERS.entry = (key, con)
    return con


def close_readers() -> None:
    cached = getattr(_READERS, "entry", None)
    if cached is None:
        return
    try:
        cached[1].close()
    except Exception:
        pass
    _READERS.entry = None


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
        CREATE TABLE IF NOT EXISTS show_pairs (
          tvdb_id TEXT NOT NULL,
          tmdb_id TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_show_pairs_tvdb ON show_pairs(tvdb_id);
        CREATE INDEX IF NOT EXISTS idx_show_pairs_tmdb ON show_pairs(tmdb_id);
        CREATE TABLE IF NOT EXISTS native_identity (
          namespace TEXT NOT NULL,
          native_id TEXT NOT NULL,
          simkl_id TEXT NOT NULL DEFAULT '',
          kitsu_id TEXT NOT NULL DEFAULT '',
          PRIMARY KEY (namespace, native_id)
        );
        CREATE INDEX IF NOT EXISTS idx_native_identity_simkl
          ON native_identity(simkl_id);
        CREATE INDEX IF NOT EXISTS idx_native_identity_kitsu
          ON native_identity(kitsu_id);
        CREATE TABLE IF NOT EXISTS meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        """
    )


def _collect_show_pair(
    src: Descriptor,
    dst: Descriptor,
    tv_to_tm: dict[str, set[str]],
    tm_to_tv: dict[str, set[str]],
) -> None:
    if src.media_kind != "show" or dst.media_kind != "show":
        return
    if src.provider == "tvdb" and dst.provider == "tmdb":
        tv, tm = src.id, dst.id
    elif src.provider == "tmdb" and dst.provider == "tvdb":
        tv, tm = dst.id, src.id
    else:
        return
    if not tv or not tm:
        return
    tv_to_tm.setdefault(tv, set()).add(tm)
    tm_to_tv.setdefault(tm, set()).add(tv)


def _unambiguous_pairs(
    tv_to_tm: Mapping[str, set[str]],
    tm_to_tv: Mapping[str, set[str]],
) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for tv, tms in tv_to_tm.items():
        if len(tms) != 1:
            continue
        tm = next(iter(tms))
        if len(tm_to_tv.get(tm) or ()) != 1:
            continue
        out.append((tv, tm))
    return out


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


def _tsv_value(raw: str) -> str:
    text = str(raw or "").strip()
    if len(text) >= 2 and text.startswith('"') and text.endswith('"'):
        return text[1:-1].replace('""', '"')
    return text


def parse_identity_rows(text: str) -> list[tuple[str, str, str, str]]:
    lines = str(text or "").split("\n")
    if not lines:
        return []
    header = [_tsv_value(x) for x in lines[0].split("\t")]
    needed = set(_IDENTITY_COLUMNS.values()) | {"simkl", "kitsu"}
    cols: dict[str, int] = {}
    for name in needed:
        if name not in header:
            raise ValueError(f"animeApi TSV is missing the '{name}' column")
        cols[name] = header.index(name)

    seen: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for line in lines[1:]:
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != len(header):
            continue
        simkl = _tsv_value(parts[cols["simkl"]])
        kitsu = _tsv_value(parts[cols["kitsu"]])
        if not simkl and not kitsu:
            continue
        for namespace, column in _IDENTITY_COLUMNS.items():
            native = _tsv_value(parts[cols[column]])
            if not native:
                continue
            seen.setdefault((namespace, native), set()).add((simkl, kitsu))

    out: list[tuple[str, str, str, str]] = []
    for (namespace, native), values in seen.items():
        if len(values) != 1:
            continue
        simkl, kitsu = next(iter(values))
        out.append((namespace, native, simkl, kitsu))
    return out


def _load_identity_rows(root: Path, identity_path: Path | None) -> list[tuple[str, str, str, str]]:
    try:
        path = _safe_existing_path(identity_path or (root / "animeapi.tsv"), base=root)
    except ValueError:
        return []
    if not path.exists():
        return []
    try:
        return parse_identity_rows(path.read_text("utf-8"))
    except Exception:
        return []


def rebuild_sqlite_from_mappings(
    *,
    release_tag: str = "v3",
    mappings_path: Path | None = None,
    db_path: Path | None = None,
    identity_path: Path | None = None,
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
    tv_to_tm: dict[str, set[str]] = {}
    tm_to_tv: dict[str, set[str]] = {}

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
                _collect_show_pair(src, dst, tv_to_tm, tm_to_tv)
                if isinstance(ranges, Mapping) and ranges:
                    for source_range, target_range in ranges.items():
                        _insert_edge(rows, src, dst, source_range, target_range)
                        _insert_edge(rows, dst, src, target_range, source_range, reverse=True)
                        edge_count += 2
                else:
                    _insert_edge(rows, src, dst)
                    _insert_edge(rows, dst, src, reverse=True)
                    edge_count += 2

        pairs = _unambiguous_pairs(tv_to_tm, tm_to_tv)
        identity_rows = _load_identity_rows(root, identity_path or pp["identity"])
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
            con.executemany("INSERT INTO show_pairs (tvdb_id, tmdb_id) VALUES (?, ?)", pairs)
            con.executemany(
                "INSERT OR REPLACE INTO native_identity (namespace, native_id, simkl_id, kitsu_id) VALUES (?, ?, ?, ?)",
                identity_rows,
            )
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("identity_count", str(len(identity_rows))))
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("pair_count", str(len(pairs))))
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("schema_version", str(SCHEMA_VERSION)))
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("built_at", str(int(time.time()))))
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("source_count", str(source_count)))
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", ("edge_count", str(edge_count)))
            con.commit()
        finally:
            con.close()

        close_readers()
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
            "pair_count": len(pairs),
            "identity_count": len(identity_rows),
        },
    )
    return {
        "ok": True,
        "source_count": source_count,
        "edge_count": edge_count,
        "pair_count": len(pairs),
        "identity_count": len(identity_rows),
        "db_path": str(db_path),
    }


def query_show_pair(release_tag: str, provider: str, ident: str) -> str | None:
    db = paths(release_tag)["db"]
    p = str(provider or "").strip().lower()
    i = str(ident or "").strip()
    if not i or p not in ("tvdb", "tmdb"):
        return None
    want, have = ("tmdb_id", "tvdb_id") if p == "tvdb" else ("tvdb_id", "tmdb_id")
    con = _reader(db)
    if con is None:
        return None
    try:
        row = con.execute(f"SELECT {want} FROM show_pairs WHERE {have} = ? LIMIT 1", (i,)).fetchone()
        return str(row[0]) if row and row[0] else None
    except sqlite3.Error:
        return None


def query_edges(release_tag: str, provider: str, ident: str, *, scope: str | None = None) -> list[dict[str, Any]]:
    db = paths(release_tag)["db"]
    p = str(provider or "").strip().lower()
    i = str(ident or "").strip()
    if not p or not i:
        return []
    sql = """
        SELECT source_provider, source_id, source_scope, source_kind,
               target_provider, target_id, target_scope, target_kind,
               source_range, target_range, reverse
        FROM mapping_edges
        WHERE source_provider = ? AND source_id = ?
    """
    args: list[Any] = [p, i]
    if scope is not None:
        sql += " AND source_scope = ?"
        args.append(str(scope))
    sql += f" LIMIT {_EDGE_FANOUT_LIMIT}"
    con = _reader(db)
    if con is None:
        return []
    try:
        return [dict(r) for r in con.execute(sql, args).fetchall()]
    except sqlite3.Error:
        return []


def query_native_identity(release_tag: str, namespace: str, native_id: str) -> dict[str, str]:
    db = paths(release_tag)["db"]
    ns = str(namespace or "").strip().lower()
    nid = str(native_id or "").strip()
    if ns not in IDENTITY_NAMESPACES or not nid:
        return {}
    con = _reader(db)
    if con is None:
        return {}
    try:
        row = con.execute(
            "SELECT simkl_id, kitsu_id FROM native_identity WHERE namespace = ? AND native_id = ? LIMIT 1",
            (ns, nid),
        ).fetchone()
    except sqlite3.Error:
        return {}
    if not row:
        return {}
    out: dict[str, str] = {}
    if str(row["simkl_id"] or "").strip():
        out["simkl"] = str(row["simkl_id"]).strip()
    if str(row["kitsu_id"] or "").strip():
        out["kitsu"] = str(row["kitsu_id"]).strip()
    return out


def query_identity_natives(release_tag: str, namespace: str, ident: str) -> dict[str, str]:
    db = paths(release_tag)["db"]
    ns = str(namespace or "").strip().lower()
    value = str(ident or "").strip()
    column = _IDENTITY_TARGET_COLUMNS.get(ns)
    if not column or not value:
        return {}
    con = _reader(db)
    if con is None:
        return {}
    try:
        rows = con.execute(
            f"SELECT namespace, native_id FROM native_identity WHERE {column} = ?",
            (value,),
        ).fetchall()
    except sqlite3.Error:
        return {}
    found: dict[str, str] = {}
    for row in rows:
        row_ns = str(row["namespace"] or "").strip().lower()
        native = str(row["native_id"] or "").strip()
        if row_ns not in IDENTITY_NAMESPACES or not native:
            continue
        current = found.get(row_ns)
        if current is None:
            found[row_ns] = native
        elif current != native:
            found[row_ns] = ""
    return {k: v for k, v in found.items() if v}


def index_schema_ok(release_tag: str = "v3") -> bool:
    try:
        return int(read_state(release_tag).get("schema_version") or 0) == SCHEMA_VERSION
    except (TypeError, ValueError):
        return False


def index_ready(release_tag: str = "v3") -> bool:
    if not paths(release_tag)["db"].exists():
        return False
    if not bool(read_state(release_tag).get("index_ready", False)):
        return False
    return index_schema_ok(release_tag)
