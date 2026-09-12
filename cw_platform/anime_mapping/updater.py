# /cw_platform/anime_mapping/updater.py
# CrossWatch - Anime Mapping Updater
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from _logging import log

from .storage import (
    SCHEMA_VERSION,
    index_ready,
    index_schema_ok,
    normalize_release_tag,
    paths,
    read_state,
    rebuild_sqlite_from_mappings,
    write_json_atomic,
    write_state,
    _safe_existing_path,
)

BASE_URL = "https://github.com/anibridge/anibridge-mappings/releases/download"
IDENTITY_URL = "https://raw.githubusercontent.com/nattadasu/animeApi/v3/database/animeapi.tsv"
IDENTITY_COMMITS_URL = "https://api.github.com/repos/nattadasu/animeApi/commits"
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


def _validate_mappings_file(path: Path) -> None:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("AniBridge mappings payload must be a JSON object")


def _validate_identity_file(path: Path) -> None:
    from .storage import parse_identity_rows

    rows = parse_identity_rows(path.read_text("utf-8"))
    if not rows:
        raise ValueError("animeApi payload produced no usable identity rows")


def _file_sha256(path: Path) -> str:
    if not path.exists():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity_release() -> dict[str, str]:
    """Resolve a dated revision so the timestamp and downloaded bytes agree."""
    try:
        response = requests.get(IDENTITY_COMMITS_URL, headers={"User-Agent": UA, "Accept": "application/vnd.github+json"},
                                params={"sha": "v3", "path": "database/animeapi.tsv", "per_page": 1}, timeout=15)
        response.raise_for_status()
        commit = response.json()[0]
        revision = str(commit["sha"])
        published = datetime.fromisoformat(commit["commit"]["committer"]["date"].replace("Z", "+00:00"))
        if not re.fullmatch(r"[0-9a-f]{40}", revision) or published.tzinfo is None:
            raise ValueError("Invalid animeApi revision")
        return {"revision": revision, "generated_on": published.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")}
    except Exception as exc:
        # Release metadata is optional; rate limits must not block data updates.
        log(f"identity_version_unavailable error_type={exc.__class__.__name__}", level="debug", module="ANIME_MAPPING")
        return {}


def _download_file_atomic(
    url: str,
    dest: Path,
    *,
    timeout: float = 120.0,
    validate: Any = None,
    conditional_headers: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    dest = _safe_existing_path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{dest.name}.", suffix=".tmp", dir=str(dest.parent))
    tmp_path = _safe_existing_path(Path(tmp_name), base=dest.parent)
    size = 0
    headers: dict[str, str] = {}
    try:
        with os.fdopen(fd, "wb") as fh:
            with requests.get(url, headers={"User-Agent": UA, **(conditional_headers or {})}, timeout=timeout, stream=True) as r:
                if r.status_code == 304:
                    if not dest.exists():
                        raise ValueError("Server returned unchanged for a missing file")
                    return {"size": dest.stat().st_size, "headers": {k.lower(): v for k, v in r.headers.items()}, "changed": False}
                r.raise_for_status()
                headers = {k.lower(): v for k, v in r.headers.items()}
                for chunk in r.iter_content(1024 * 1024):
                    if not chunk:
                        continue
                    fh.write(chunk)
                    size += len(chunk)

        # Validate before swapping.
        (validate or _validate_mappings_file)(tmp_path)
        changed = _file_sha256(tmp_path) != _file_sha256(dest)
        if changed:
            os.replace(tmp_path, dest)
        return {"size": size, "headers": headers, "changed": changed}
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

    identity_path = _safe_existing_path(pp["identity"])
    mapping_size = mappings_path.stat().st_size if mappings_path.exists() else 0
    identity_size = identity_path.stat().st_size if identity_path.exists() else 0
    db_size = db_path.stat().st_size if db_path.exists() else 0
    installed = bool(mappings_path.exists() and db_path.exists())
    return {
        "ok": True,
        "enabled": bool(am.get("enabled", False)),
        "auto_update": bool(am.get("auto_update", True)),
        "provider": str(am.get("provider") or "anibridge"),
        "release_tag": tag,
        "installed": installed,
        "index_ready": index_ready(tag),
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
        "identity_count": int(st.get("identity_count") or 0),
        "identity_installed": bool(identity_path.exists()),
        "identity_error": str(st.get("identity_error") or ""),
        "identity_release_tag": "v3",
        "identity_revision": str(st.get("identity_revision") or ""),
        "identity_generated_on": str(st.get("identity_generated_on") or ""),
        "error": str(st.get("error") or ""),
        "mappings_size": int(mapping_size),
        "identity_size": int(identity_size),
        "db_size": int(db_size),
        "root": str(pp["root"]),
        "stats": stats if isinstance(stats, dict) else {},
    }


def boot_check(*, cfg: Mapping[str, Any] | None = None, auto_repair: bool = True) -> dict[str, Any]:
    am = _cfg_block(cfg)
    tag = normalize_release_tag(am.get("release_tag"))
    pp = paths(tag)
    mappings_path = _safe_existing_path(pp["mappings"])
    db_path = _safe_existing_path(pp["db"])
    enabled = bool(am.get("enabled", False))

    def _result(status: str, ok: bool, message: str) -> dict[str, Any]:
        st = read_state(tag)
        try:
            found_version = int(st.get("schema_version") or 0)
        except (TypeError, ValueError):
            found_version = 0
        return {
            "ok": ok,
            "status": status,
            "message": message,
            "enabled": enabled,
            "release_tag": tag,
            "schema_version": found_version,
            "expected_schema_version": SCHEMA_VERSION,
            "source_count": int(st.get("source_count") or 0),
            "edge_count": int(st.get("edge_count") or 0),
            "identity_count": int(st.get("identity_count") or 0),
            "size_bytes": db_path.stat().st_size if db_path.exists() else 0,
            "path": str(db_path),
        }

    if not enabled:
        return _result("disabled", True, "Disabled")
    if not mappings_path.exists():
        return _result("missing", True, "Not installed - dataset will download on first update")
    if index_ready(tag):
        return _result("ready", True, f"Ready - anime schema v{SCHEMA_VERSION}")
    if not auto_repair:
        return _result("stale", False, f"Stale - expected anime schema v{SCHEMA_VERSION}")

    reason = "schema_outdated" if not index_schema_ok(tag) else ("index_missing" if not db_path.exists() else "index_not_ready")
    log("boot_reindex_started", level="debug", module="ANIME_MAPPING", extra={"release_tag": tag, "reason": reason})
    try:
        rebuild = rebuild_sqlite_from_mappings(release_tag=tag)
    except Exception as exc:
        log(
            "boot_reindex_failed",
            level="warning",
            module="ANIME_MAPPING",
            extra={"release_tag": tag, "reason": reason, "error_type": exc.__class__.__name__, "error": str(exc)},
        )
        return _result("error", False, f"Reindex failed - {exc.__class__.__name__}")

    if not index_ready(tag):
        return _result("error", False, "Reindex did not produce a ready index")

    log(
        "boot_reindex_finished",
        level="debug",
        module="ANIME_MAPPING",
        extra={
            "release_tag": tag,
            "reason": reason,
            "edge_count": int(rebuild.get("edge_count") or 0),
            "identity_count": int(rebuild.get("identity_count") or 0),
        },
    )
    note = "schema updated" if reason == "schema_outdated" else "index rebuilt"
    return _result("reindexed", True, f"Reindexed - {note} - anime schema v{SCHEMA_VERSION}")


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
    previous = read_state(tag)
    previous_generated = str(previous.get("dataset_generated_on") or "")
    generated_on = previous_generated
    dl: dict[str, Any] = {}
    stats_headers: dict[str, str] = {}
    mapping_error = ""
    mappings_changed = False
    try:
        stats_data, stats_headers = _download_json(stats_url)
        meta = stats_data.get("meta") if isinstance(stats_data.get("meta"), dict) else {}
        generated_on = str((meta or {}).get("generated_on") or "")
        needs_download = force or not mappings_path.exists() or (generated_on and generated_on != previous_generated)
        if needs_download:
            log("download_started dataset=anibridge", level="debug", module="ANIME_MAPPING")
            dl = _download_file_atomic(mappings_url, mappings_path)
            mappings_changed = bool(dl.get("changed", True))
        write_json_atomic(stats_path, stats_data)
    except Exception as exc:
        mapping_error = exc.__class__.__name__
        log(f"download_failed dataset=anibridge error_type={mapping_error}", level="warning", module="ANIME_MAPPING")

    # animeApi has its own publication schedule. Always check it, including
    # when aniBridge is unchanged or its update check failed.
    identity_path = _safe_existing_path(pp["identity"])
    idl: dict[str, Any] = {}
    identity_error = ""
    identity_release = _identity_release()
    try:
        conditional = {}
        if identity_path.exists() and not force:
            if previous.get("identity_etag"):
                conditional["If-None-Match"] = str(previous["identity_etag"])
            elif previous.get("identity_last_modified"):
                conditional["If-Modified-Since"] = str(previous["identity_last_modified"])
        identity_url = IDENTITY_URL.replace("/v3/", f"/{identity_release['revision']}/") if identity_release else IDENTITY_URL
        idl = _download_file_atomic(
            identity_url, identity_path, validate=_validate_identity_file,
            conditional_headers=conditional,
        )
    except Exception as exc:
        identity_error = exc.__class__.__name__
        log(f"identity_download_failed error_type={identity_error}", level="warning", module="ANIME_MAPPING")

    # Compare against the indexed bytes, so a failed rebuild is retried even
    # when a later HTTP request reports that the downloaded file is unchanged.
    identity_hash = _file_sha256(identity_path)
    mapping_hash = _file_sha256(mappings_path)
    identity_changed = bool(identity_hash) and identity_hash != previous.get("indexed_identity_sha256")
    pending_mappings = bool(mapping_hash) and mapping_hash != previous.get("indexed_mappings_sha256")
    needs_rebuild = force or mappings_changed or identity_changed or pending_mappings or not db_path.exists() or not index_schema_ok(tag)
    errors = []
    if mapping_error:
        errors.append(f"aniBridge update failed ({mapping_error})")
    if identity_error:
        errors.append(f"animeApi update failed ({identity_error})")
    patch: dict[str, Any] = {"last_checked_at": now, "identity_error": identity_error}
    rebuild: dict[str, Any] = {}
    if mappings_path.exists() and needs_rebuild:
        log("index_rebuild_started", level="debug", module="ANIME_MAPPING", extra={"release_tag": tag})
        try:
            rebuild = rebuild_sqlite_from_mappings(release_tag=tag)
        except Exception as exc:
            write_state(tag, {**patch, "error": f"Anime mapping index rebuild failed ({exc.__class__.__name__})"})
            raise
        patch.update(indexed_identity_sha256=identity_hash, indexed_mappings_sha256=mapping_hash,
                     last_updated_at=int(time.time()))
        log("index_rebuild_finished", level="debug", module="ANIME_MAPPING", extra={"release_tag": tag})
    if not mapping_error:
        # Commit the publication marker only after the index is ready.
        patch.update(dataset_generated_on=generated_on,
                     stats_etag=stats_headers.get("etag", ""),
                     stats_last_modified=stats_headers.get("last-modified", ""))
    if dl:
        patch.update(mappings_etag=dl["headers"].get("etag", ""),
                     mappings_last_modified=dl["headers"].get("last-modified", ""), mappings_size=dl["size"])
    if identity_changed:
        patch.update(identity_revision="", identity_generated_on="")
    if idl:
        patch.update(identity_size=idl["size"],
                     identity_etag=idl["headers"].get("etag", previous.get("identity_etag", "") if not idl.get("changed") else ""),
                     identity_last_modified=idl["headers"].get("last-modified", previous.get("identity_last_modified", "") if not idl.get("changed") else ""))
        if identity_release:
            patch.update(identity_revision=identity_release["revision"], identity_generated_on=identity_release["generated_on"])
        elif identity_changed or idl.get("changed"):
            patch.update(identity_revision="", identity_generated_on="")
    patch["error"] = "; ".join(errors)
    write_state(tag, patch)
    updated = bool(rebuild and (mappings_changed or pending_mappings or identity_changed))
    if not errors:
        message = "update_finished" if updated else ("index_schema_rebuild_finished" if rebuild else "update_skipped reason=datasets_current")
        log(message, level="debug", module="ANIME_MAPPING")
    return {**status(cfg={"anime_mapping": {"release_tag": tag, "enabled": True}}),
            "ok": not errors, "updated": updated, "rebuilt": bool(rebuild),
            "mappings_updated": bool(rebuild and (mappings_changed or pending_mappings)),
            "identity_updated": bool(rebuild and identity_changed), "download": dl, "rebuild": rebuild}
