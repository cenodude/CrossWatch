# /cw_platform/anime_mapping/updater.py
# CrossWatch - Anime Mapping Updater
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import requests

from _logging import log

from .storage import (
    normalize_release_tag,
    paths,
    read_state,
    rebuild_sqlite_from_mappings,
    write_json_atomic,
    write_state,
    _safe_existing_path,
)

BASE_URL = "https://github.com/anibridge/anibridge-mappings/releases/download"
UA = "CrossWatch AnimeMapping/1.0"
_UPDATE_LOCK = threading.Lock()


def _cfg_block(cfg: Mapping[str, Any] | None) -> dict[str, Any]:
    am = (cfg or {}).get("anime_mapping") if isinstance(cfg, Mapping) else {}
    return am if isinstance(am, dict) else {}


def _asset_url(release_tag: str, name: str) -> str:
    tag = normalize_release_tag(release_tag)
    return f"{BASE_URL}/{tag}/{name}"


def _download_json(url: str, *, timeout: float = 30.0) -> tuple[dict[str, Any], dict[str, str]]:
    r = requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise ValueError(f"{url} did not return a JSON object")
    return data, {k.lower(): v for k, v in r.headers.items()}


def _download_file_atomic(url: str, dest: Path, *, timeout: float = 120.0) -> dict[str, Any]:
    dest = _safe_existing_path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{dest.name}.", suffix=".tmp", dir=str(dest.parent))
    tmp_path = _safe_existing_path(Path(tmp_name), base=dest.parent)
    size = 0
    headers: dict[str, str] = {}
    try:
        with os.fdopen(fd, "wb") as fh:
            with requests.get(url, headers={"User-Agent": UA}, timeout=timeout, stream=True) as r:
                r.raise_for_status()
                headers = {k.lower(): v for k, v in r.headers.items()}
                for chunk in r.iter_content(1024 * 1024):
                    if not chunk:
                        continue
                    fh.write(chunk)
                    size += len(chunk)

        # Validate before swapping.
        with open(tmp_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("AniBridge mappings payload must be a JSON object")
        os.replace(tmp_path, dest)
        return {"size": size, "headers": headers}
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def status(*, cfg: Mapping[str, Any] | None = None) -> dict[str, Any]:
    am = _cfg_block(cfg)
    tag = normalize_release_tag(am.get("release_tag"))
    pp = paths(tag)
    stats_path = _safe_existing_path(pp["stats"])
    mappings_path = _safe_existing_path(pp["mappings"])
    db_path = _safe_existing_path(pp["db"])
    st = read_state(tag)
    stats = {}
    if stats_path.exists():
        try:
            stats = json.loads(stats_path.read_text("utf-8"))
        except Exception:
            stats = {}
    meta = stats.get("meta") if isinstance(stats, dict) else {}
    generated_on = str(st.get("dataset_generated_on") or (meta or {}).get("generated_on") or "")
    stale_after_days = int(am.get("stale_after_days", 14) or 14)
    stale = False
    age_hours: float | None = None
    if generated_on:
        try:
            from datetime import datetime, timezone

            dt = datetime.fromisoformat(generated_on.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_hours = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
            stale = age_hours > (stale_after_days * 24)
        except Exception:
            age_hours = None

    mapping_size = mappings_path.stat().st_size if mappings_path.exists() else 0
    db_size = db_path.stat().st_size if db_path.exists() else 0
    installed = bool(mappings_path.exists() and db_path.exists())
    return {
        "ok": True,
        "enabled": bool(am.get("enabled", False)),
        "auto_update": bool(am.get("auto_update", True)),
        "provider": str(am.get("provider") or "anibridge"),
        "release_tag": tag,
        "installed": installed,
        "index_ready": bool(db_path.exists() and st.get("index_ready", False)),
        "status": "installed" if installed else "missing",
        "dataset_generated_on": generated_on,
        "age_hours": age_hours,
        "stale": stale,
        "stale_after_days": stale_after_days,
        "last_checked_at": int(st.get("last_checked_at") or 0),
        "last_updated_at": int(st.get("last_updated_at") or 0),
        "index_built_at": int(st.get("index_built_at") or 0),
        "source_count": int(st.get("source_count") or 0),
        "edge_count": int(st.get("edge_count") or 0),
        "mappings_size": int(mapping_size),
        "db_size": int(db_size),
        "root": str(pp["root"]),
        "stats": stats if isinstance(stats, dict) else {},
    }


def update(*, release_tag: str = "v3", force: bool = False) -> dict[str, Any]:
    with _UPDATE_LOCK:
        return _update_locked(release_tag=release_tag, force=force)


def _update_locked(*, release_tag: str = "v3", force: bool = False) -> dict[str, Any]:
    tag = normalize_release_tag(release_tag)
    pp = paths(tag)
    root = _safe_existing_path(pp["root"])
    mappings_path = _safe_existing_path(pp["mappings"])
    db_path = _safe_existing_path(pp["db"])
    stats_path = _safe_existing_path(pp["stats"])
    root.mkdir(parents=True, exist_ok=True)
    now = int(time.time())

    stats_url = _asset_url(tag, "stats.json")
    mappings_url = _asset_url(tag, "mappings.min.json")
    stats_data, stats_headers = _download_json(stats_url)
    meta = stats_data.get("meta") if isinstance(stats_data.get("meta"), dict) else {}
    generated_on = str((meta or {}).get("generated_on") or "")
    previous = read_state(tag)
    previous_generated = str(previous.get("dataset_generated_on") or "")
    changed = force or not mappings_path.exists() or not db_path.exists() or (generated_on and generated_on != previous_generated)

    write_json_atomic(stats_path, stats_data)
    write_state(
        tag,
        {
            "last_checked_at": now,
            "dataset_generated_on": generated_on,
            "stats_etag": stats_headers.get("etag", ""),
            "stats_last_modified": stats_headers.get("last-modified", ""),
        },
    )

    if not changed:
        log(
            "update_skipped",
            level="debug",
            module="ANIME_MAPPING",
            extra={
                "release_tag": tag,
                "reason": "dataset_current",
                "generated_on": generated_on,
                "previous_generated_on": previous_generated,
            },
        )
        return {"ok": True, "updated": False, **status(cfg={"anime_mapping": {"release_tag": tag, "enabled": True}})}

    log("download_started", level="debug", module="ANIME_MAPPING", extra={"release_tag": tag})
    dl = _download_file_atomic(mappings_url, mappings_path)
    log(
        "download_finished",
        level="debug",
        module="ANIME_MAPPING",
        extra={"release_tag": tag, "mappings_size": int(dl.get("size") or 0)},
    )
    log("index_rebuild_started", level="debug", module="ANIME_MAPPING", extra={"release_tag": tag})
    rebuild = rebuild_sqlite_from_mappings(release_tag=tag)
    log(
        "index_rebuild_finished",
        level="debug",
        module="ANIME_MAPPING",
        extra={
            "release_tag": tag,
            "source_count": int(rebuild.get("source_count") or 0),
            "edge_count": int(rebuild.get("edge_count") or 0),
        },
    )
    write_state(
        tag,
        {
            "last_updated_at": int(time.time()),
            "mappings_etag": (dl.get("headers") or {}).get("etag", ""),
            "mappings_last_modified": (dl.get("headers") or {}).get("last-modified", ""),
            "mappings_size": int(dl.get("size") or 0),
            "error": "",
        },
    )
    return {"ok": True, "updated": True, "download": dl, "rebuild": rebuild, **status(cfg={"anime_mapping": {"release_tag": tag, "enabled": True}})}
