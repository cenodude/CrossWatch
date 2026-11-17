# services/artwork_choices.py
# CrossWatch - Saved artwork choices with pinned local copies
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import requests

from cw_platform.config_base import CONFIG_BASE

KINDS = ("poster", "backdrop", "logo", "disc")
SOURCES = ("tmdb", "fanart")
PIN_SIZES = {"poster": "w780", "backdrop": "w1280", "logo": "w500", "disc": "w500"}
MAX_PIN_BYTES = 25 * 1024 * 1024

TMDB_PATH_RE = re.compile(r"^/[A-Za-z0-9_-]{4,64}\.(jpg|jpeg|png)$", re.IGNORECASE)
FANART_URL_RE = re.compile(r"^https://assets\.fanart\.tv/fanart/[A-Za-z0-9/_.-]+\.(png|jpg|jpeg)$", re.IGNORECASE)
_PIN_NAME_RE = re.compile(r"^(movie|tv)_\d{1,10}_(poster|backdrop|logo|disc)\.(jpg|png)$")
_KEY_RE = re.compile(r"^(movie|tv):(\d{1,10})$")
_LANG_RE = re.compile(r"^[a-z]{2}$")

_LOCK = threading.RLock()
_STATE: dict[str, Any] = {"root": None, "items": None, "revs": None}


class ArtworkChoiceError(ValueError):
    pass


_PUBLIC_ERRORS = {
    text: text
    for text in (
        "Invalid TMDb id", "Invalid image reference", "Invalid pin path", "Not an image", "Image too large",
        "Empty image", "Invalid artwork kind", "Invalid artwork source",
    )
}


def public_error(error: Exception) -> str:
    return _PUBLIC_ERRORS.get(str(error), "Invalid request")


def _root() -> Path:
    return CONFIG_BASE() / ".cw_state" / "artwork"


def _entity(value: Any) -> str:
    return "movie" if str(value or "").strip().lower() == "movie" else "tv"


def _tmdb_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text.isdigit() or len(text) > 10 or int(text) <= 0:
        raise ArtworkChoiceError("Invalid TMDb id")
    return str(int(text))


def title_key(entity: Any, tmdb_id: Any) -> str:
    return f"{_entity(entity)}:{_tmdb_id(tmdb_id)}"


def _clean_choice(kind: str, raw: Any) -> dict[str, Any] | None:
    if kind not in KINDS or not isinstance(raw, dict):
        return None
    source = str(raw.get("source") or "")
    ref = str(raw.get("ref") or "")
    if source == "tmdb" and not TMDB_PATH_RE.fullmatch(ref):
        return None
    if source == "fanart" and not FANART_URL_RE.fullmatch(ref):
        return None
    if source not in SOURCES or (kind == "disc" and source != "fanart"):
        return None
    lang = str(raw.get("lang") or "").strip().lower()
    out: dict[str, Any] = {"source": source, "ref": ref, "lang": lang if _LANG_RE.fullmatch(lang) else ""}
    try:
        out["set_at"] = int(raw.get("set_at") or 0)
    except Exception:
        out["set_at"] = 0
    if raw.get("missing") is True:
        out["missing"] = True
        try:
            out["missing_at"] = int(raw.get("missing_at") or 0)
        except Exception:
            out["missing_at"] = 0
    return out


def _load() -> dict[str, dict[str, dict[str, Any]]]:
    root = _root()
    with _LOCK:
        if _STATE["items"] is not None and _STATE["root"] == root:
            return _STATE["items"]
        try:
            data = json.loads((root / "choices.json").read_text("utf-8"))
        except Exception:
            data = {}
        raw = data.get("items") if isinstance(data, dict) else None
        items: dict[str, dict[str, dict[str, Any]]] = {}
        for key, kinds in (raw if isinstance(raw, dict) else {}).items():
            if not _KEY_RE.fullmatch(str(key)) or not isinstance(kinds, dict):
                continue
            clean = {kind: row for kind in KINDS if (row := _clean_choice(kind, kinds.get(kind)))}
            if clean:
                items[str(key)] = clean
        _STATE.update(root=root, items=items, revs=None)
        return items


def _save(items: dict[str, dict[str, dict[str, Any]]]) -> None:
    root = _root()
    root.mkdir(parents=True, exist_ok=True)
    path = root / "choices.json"
    fd, tmp_name = tempfile.mkstemp(prefix=".choices.json.", suffix=".tmp", dir=str(root))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "items": items}, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass
    _STATE.update(root=root, items=items, revs=None)


def _rev(kinds: dict[str, dict[str, Any]]) -> str:
    payload = json.dumps(kinds, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:8]


def clear_memory() -> None:
    with _LOCK:
        _STATE.update(root=None, items=None, revs=None)


def revisions() -> dict[str, str]:
    with _LOCK:
        items = _load()
        if _STATE["revs"] is None:
            _STATE["revs"] = {key: _rev(kinds) for key, kinds in items.items()}
        return dict(_STATE["revs"])


def revision(entity: Any, tmdb_id: Any) -> str:
    try:
        key = title_key(entity, tmdb_id)
    except ArtworkChoiceError:
        return ""
    with _LOCK:
        items = _load()
        if key not in items:
            return ""
        if _STATE["revs"] is None:
            _STATE["revs"] = {k: _rev(v) for k, v in items.items()}
        return str(_STATE["revs"].get(key) or "")


def choices(entity: Any, tmdb_id: Any) -> dict[str, dict[str, Any]]:
    try:
        key = title_key(entity, tmdb_id)
    except ArtworkChoiceError:
        return {}
    with _LOCK:
        return copy.deepcopy(_load().get(key) or {})


def choice(entity: Any, tmdb_id: Any, kind: str) -> dict[str, Any] | None:
    try:
        key = title_key(entity, tmdb_id)
    except ArtworkChoiceError:
        return None
    with _LOCK:
        items = _load()
        if not items:
            return None
        row = (items.get(key) or {}).get(kind)
        return dict(row) if row else None


def _pin_dir() -> Path:
    return _root() / "pins"


def _pin_file(entity: str, tmdb_id: str, kind: str, extension: str) -> Path | None:
    name = f"{entity}_{tmdb_id}_{kind}.{extension}"
    if not _PIN_NAME_RE.fullmatch(name):
        return None
    root = os.path.realpath(_pin_dir())
    full = os.path.realpath(os.path.join(root, name))
    if not full.startswith(root + os.sep):
        return None
    return Path(full)


def pin_file(entity: Any, tmdb_id: Any, kind: str) -> tuple[Path, str] | None:
    if kind not in KINDS:
        return None
    try:
        ent, tid = _entity(entity), _tmdb_id(tmdb_id)
    except ArtworkChoiceError:
        return None
    for extension, mime in (("jpg", "image/jpeg"), ("png", "image/png")):
        path = _pin_file(ent, tid, kind, extension)
        if path is not None and path.is_file():
            return path, mime
    return None


def _drop_pins(entity: str, tmdb_id: str, kind: str) -> None:
    for extension in ("jpg", "png"):
        path = _pin_file(entity, tmdb_id, kind, extension)
        if path is not None:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass


def source_url(source: str, ref: str, kind: str) -> str:
    if source == "tmdb" and TMDB_PATH_RE.fullmatch(ref):
        return f"https://image.tmdb.org/t/p/{PIN_SIZES.get(kind, 'w780')}{ref}"
    if source == "fanart" and FANART_URL_RE.fullmatch(ref):
        return ref
    raise ArtworkChoiceError("Invalid image reference")


def _download_pin(entity: str, tmdb_id: str, kind: str, source: str, ref: str) -> Path:
    url = source_url(source, ref, kind)
    extension = "png" if url.lower().endswith(".png") else "jpg"
    target = _pin_file(entity, tmdb_id, kind, extension)
    if target is None:
        raise ArtworkChoiceError("Invalid pin path")
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent))
    try:
        size = 0
        with requests.get(url, stream=True, timeout=20) as response:
            response.raise_for_status()
            if not str(response.headers.get("Content-Type") or "image/").lower().startswith("image/"):
                raise ArtworkChoiceError("Not an image")
            with os.fdopen(fd, "wb") as f:
                for chunk in response.iter_content(64 * 1024):
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > MAX_PIN_BYTES:
                        raise ArtworkChoiceError("Image too large")
                    f.write(chunk)
        if size == 0:
            raise ArtworkChoiceError("Empty image")
        _drop_pins(entity, tmdb_id, kind)
        os.replace(tmp_name, target)
        return target
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass


def set_choice(entity: Any, tmdb_id: Any, kind: str, source: str, ref: str, *, lang: str = "") -> str:
    if kind not in KINDS:
        raise ArtworkChoiceError("Invalid artwork kind")
    if source not in SOURCES:
        raise ArtworkChoiceError("Invalid artwork source")
    ent, tid = _entity(entity), _tmdb_id(tmdb_id)
    if kind == "disc" and ent != "movie":
        raise ArtworkChoiceError("Invalid artwork kind")
    row = _clean_choice(kind, {"source": source, "ref": ref, "lang": lang, "set_at": int(time.time())})
    if row is None:
        raise ArtworkChoiceError("Invalid image reference")
    _download_pin(ent, tid, kind, source, ref)
    with _LOCK:
        items = copy.deepcopy(_load())
        items.setdefault(f"{ent}:{tid}", {})[kind] = row
        _save(items)
    return revision(ent, tid)


def reset(entity: Any, tmdb_id: Any, kind: str | None = None) -> str:
    ent, tid = _entity(entity), _tmdb_id(tmdb_id)
    kinds = KINDS if kind is None else ((kind,) if kind in KINDS else ())
    if not kinds:
        raise ArtworkChoiceError("Invalid artwork kind")
    key = f"{ent}:{tid}"
    with _LOCK:
        items = copy.deepcopy(_load())
        row = items.get(key) or {}
        for name in kinds:
            row.pop(name, None)
            _drop_pins(ent, tid, name)
        if row:
            items[key] = row
        else:
            items.pop(key, None)
        _save(items)
    return revision(ent, tid)


def repin(entity: Any, tmdb_id: Any, kind: str) -> tuple[Path, str] | None:
    row = choice(entity, tmdb_id, kind)
    if not row:
        return None
    try:
        _download_pin(_entity(entity), _tmdb_id(tmdb_id), kind, row["source"], row["ref"])
    except Exception:
        return None
    return pin_file(entity, tmdb_id, kind)


def mark_missing(entity: Any, tmdb_id: Any, kind: str, missing: bool) -> None:
    try:
        key = title_key(entity, tmdb_id)
    except ArtworkChoiceError:
        return
    with _LOCK:
        items = _load()
        row = (items.get(key) or {}).get(kind)
        if not row or bool(row.get("missing")) == missing:
            return
        items = copy.deepcopy(items)
        if missing:
            items[key][kind]["missing"] = True
            items[key][kind]["missing_at"] = int(time.time())
        else:
            items[key][kind].pop("missing", None)
            items[key][kind].pop("missing_at", None)
        _save(items)
