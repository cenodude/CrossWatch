# services/shelf_store.py
# CrossWatch - Personal shelf with owned, ordered and wishlist titles and their copies
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
import os
import re
import tempfile
import threading
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from cw_platform.collection_media import FORMATS, copy_media
from cw_platform.config_base import CONFIG_BASE

STATUSES = ("owned", "ordered", "wishlist")
PACKAGING = ("keep_case", "steelbook", "digipak", "mediabook", "slipcover", "box_set", "other")
CONDITIONS = ("mint", "like_new", "good", "fair", "poor")
ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "simkl", "anilist", "mal", "kitsu", "anidb")
MAX_COPIES = 50

_KEY_RE = re.compile(r"^(movie|tv):(\d{1,10})$")
_COPY_ID_RE = re.compile(r"^[a-f0-9]{12}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,40}$")
_TEXT_LIMITS = {"edition": 80, "region": 12, "volume": 40, "shelf": 60, "lent_to": 80, "store": 80, "notes": 500}
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_LOGO_RE = re.compile(r"^/[A-Za-z0-9_-]{4,64}\.(jpg|jpeg|png)$", re.IGNORECASE)
_SOURCE_RE = re.compile(r"^[a-z0-9_]{1,24}$")
_REF_RE = re.compile(r"^[A-Za-z0-9:_-]{1,40}$")
MAX_PRICE = 10_000_000

_OWNER_RE = re.compile(r"^[a-z0-9_-]{1,64}$")
ADMIN_OWNER = "administrator"

_LOCK = threading.RLock()
_STATES: dict[str, dict[str, Any]] = {}


class ShelfError(ValueError):
    pass


_PUBLIC_ERRORS = {
    text: text
    for text in (
        "Invalid TMDb id", "Invalid copy", "Choose a format for each copy", "Invalid status",
        f"A title can have up to {MAX_COPIES} copies", "Invalid currency", "Invalid shelf entry", "Invalid shelf owner",
    )
}


def public_error(error: Exception) -> str:
    return _PUBLIC_ERRORS.get(str(error), "Invalid request")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _entity(value: Any) -> str:
    return "movie" if str(value or "").strip().lower() == "movie" else "tv"


def title_key(entity: Any, tmdb_id: Any) -> str:
    text = str(tmdb_id or "").strip()
    if not text.isdigit() or len(text) > 10 or int(text) <= 0:
        raise ShelfError("Invalid TMDb id")
    return f"{_entity(entity)}:{int(text)}"


def _text(value: Any, limit: int, multiline: bool = False) -> str:
    raw = str(value or "")
    if multiline:
        return "\n".join(" ".join(line.split()) for line in raw.strip().splitlines())[:limit].strip()
    return " ".join(raw.split())[:limit]


def _valid_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _int(value: Any, low: int, high: int) -> int | None:
    try:
        number = int(str(value).strip())
    except Exception:
        return None
    return number if low <= number <= high else None


def clean_copy(raw: Any, *, strict: bool = True) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        if strict:
            raise ShelfError("Invalid copy")
        return None
    fmt = str(raw.get("format") or "").strip().lower()
    if fmt not in FORMATS:
        if strict:
            raise ShelfError("Choose a format for each copy")
        return None
    out: dict[str, Any] = {"format": fmt}
    copy_id = str(raw.get("id") or "").strip().lower()
    out["id"] = copy_id if _COPY_ID_RE.fullmatch(copy_id) else uuid.uuid4().hex[:12]
    packaging = str(raw.get("packaging") or "").strip().lower()
    if packaging in PACKAGING:
        out["packaging"] = packaging
    condition = str(raw.get("condition") or "").strip().lower()
    if condition in CONDITIONS:
        out["condition"] = condition
    for field, limit in _TEXT_LIMITS.items():
        value = _text(raw.get(field), limit, multiline=field == "notes")
        if value:
            out[field] = value
    discs = _int(raw.get("discs"), 1, 99)
    if discs is not None:
        out["discs"] = discs
    season = _int(raw.get("season"), 0, 999)
    if season is not None:
        out["season"] = season
    try:
        price = round(float(str(raw.get("price")).replace(",", ".")), 2) if raw.get("price") not in (None, "") else None
    except Exception:
        price = None
    if price is not None and 0 <= price <= MAX_PRICE:
        out["price"] = price
    service = raw.get("service")
    if fmt == "digital" and isinstance(service, dict):
        service_id = _int(service.get("id"), 1, 10_000_000)
        service_name = _text(service.get("name"), 80)
        logo = str(service.get("logo") or "").strip()
        if service_id and service_name:
            out["service"] = {"id": service_id, "name": service_name, **({"logo": logo} if _LOGO_RE.fullmatch(logo) else {})}
    purchased = str(raw.get("purchased") or "").strip()
    if _DATE_RE.fullmatch(purchased) and _valid_date(purchased):
        out["purchased"] = purchased
    out.update(copy_media(raw))
    source = str(raw.get("source") or "").strip().lower()
    ref = str(raw.get("ref") or "").strip()
    if _SOURCE_RE.fullmatch(source):
        out["source"] = source
        if _REF_RE.fullmatch(ref):
            out["ref"] = ref
    added = str(raw.get("added_at") or "").strip()
    out["added_at"] = added[:20] if added else _now()
    return out


def _clean_ids(raw: Any, key: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if isinstance(raw, dict):
        for name in ID_KEYS:
            value = str(raw.get(name) or "").strip()
            if value and _ID_RE.fullmatch(value):
                out[name] = value
    out["tmdb"] = key.split(":", 1)[1]
    return out


def _clean_entry(key: str, raw: Any, *, strict: bool) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    status = str(raw.get("status") or "owned").strip().lower()
    if status not in STATUSES:
        if strict:
            raise ShelfError("Invalid status")
        status = "owned"
    raw_copies = raw.get("copies")
    copies_raw: list[Any] = raw_copies if isinstance(raw_copies, list) else []
    if strict and len(copies_raw) > MAX_COPIES:
        raise ShelfError(f"A title can have up to {MAX_COPIES} copies")
    copies = [row for row in (clean_copy(c, strict=strict) for c in copies_raw[:MAX_COPIES]) if row]
    seen: set[str] = set()
    for row in copies:
        while row["id"] in seen:
            row["id"] = uuid.uuid4().hex[:12]
        seen.add(row["id"])
    entry: dict[str, Any] = {
        "type": "movie" if key.startswith("movie:") else "show",
        "status": status,
        "title": _text(raw.get("title"), 200) or "Untitled",
        "ids": _clean_ids(raw.get("ids"), key),
        "copies": copies,
        "added_at": str(raw.get("added_at") or "").strip()[:20] or _now(),
        "updated_at": str(raw.get("updated_at") or "").strip()[:20] or _now(),
    }
    year = _int(raw.get("year"), 1870, 2200)
    if year is not None:
        entry["year"] = year
    return entry


def _owner(value: Any) -> str:
    text = str(value or "").strip().lower()
    if _OWNER_RE.fullmatch(text):
        return text
    raise ShelfError("Invalid shelf owner")


def _path(owner: str) -> Path:
    return CONFIG_BASE() / ".cw_state" / "shelf" / f"{_owner(owner)}.json"


def _migrate_legacy(owner: str, path: Path) -> None:
    if owner != ADMIN_OWNER or path.exists():
        return
    legacy = CONFIG_BASE() / ".cw_state" / "shelf.json"
    if not legacy.is_file():
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(legacy, path)
    except Exception:
        pass


def _state(owner: str) -> dict[str, Any]:
    path = _path(owner)
    cached = _STATES.get(str(path))
    if cached is not None:
        return cached
    _migrate_legacy(_owner(owner), path)
    try:
        data = json.loads(path.read_text("utf-8"))
    except Exception:
        data = {}
    raw = data.get("items") if isinstance(data, dict) else None
    items: dict[str, dict[str, Any]] = {}
    for key, row in (raw if isinstance(raw, dict) else {}).items():
        if not _KEY_RE.fullmatch(str(key)):
            continue
        entry = _clean_entry(str(key), row, strict=False)
        if entry:
            items[str(key)] = entry
    raw_settings = data.get("settings") if isinstance(data, dict) else None
    code = str((raw_settings or {}).get("currency") or "") if isinstance(raw_settings, dict) else ""
    state = {"path": path, "items": items, "summaries": None, "settings": {"currency": code} if _CURRENCY_RE.fullmatch(code) else {}}
    if len(_STATES) > 64:
        _STATES.clear()
    _STATES[str(path)] = state
    return state


def _save(state: dict[str, Any], items: dict[str, dict[str, Any]]) -> None:
    path: Path = state["path"]
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "items": items, "settings": state.get("settings") or {}}, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass
    state["items"] = items
    state["summaries"] = None


def clear_memory() -> None:
    with _LOCK:
        _STATES.clear()


def summary(entry: dict[str, Any]) -> dict[str, Any]:
    copies = entry.get("copies") or []
    formats: list[str] = []
    for fmt in FORMATS:
        if any(row.get("format") == fmt for row in copies):
            formats.append(fmt)
    out: dict[str, Any] = {"status": entry.get("status") or "owned", "copies": len(copies), "formats": formats}
    if copies:
        out["per_format"] = {fmt: sum(1 for row in copies if row.get("format") == fmt) for fmt in formats}
    shelves: list[str] = []
    for row in copies:
        name = str(row.get("shelf") or "").strip()
        if name and name not in shelves and len(shelves) < 5:
            shelves.append(name)
    if shelves:
        out["shelves"] = shelves
    packaging = next((row.get("packaging") for row in copies if row.get("packaging") == "steelbook"), None)
    if packaging:
        out["packaging"] = packaging
    edition = next((row.get("edition") for row in copies if row.get("edition")), None)
    if edition:
        out["edition"] = edition
    if any(row.get("lent_to") for row in copies):
        out["lent"] = True
    service = next((row["service"] for row in copies if row.get("format") == "digital" and isinstance(row.get("service"), dict)), None)
    if service:
        out["service"] = {key: service[key] for key in ("name", "logo") if service.get(key)}
    purchased = max((str(row.get("purchased")) for row in copies if row.get("purchased")), default="")
    if purchased:
        out["purchased"] = purchased
    prices = [row["price"] for row in copies if isinstance(row.get("price"), (int, float))]
    if prices:
        out["spent"] = round(sum(prices), 2)
    return out


def clean_copies(raw: Any) -> list[dict[str, Any]]:
    rows = raw if isinstance(raw, list) else []
    if len(rows) > MAX_COPIES:
        raise ShelfError(f"A title can have up to {MAX_COPIES} copies")
    copies = [row for row in (clean_copy(item, strict=True) for item in rows) if row]
    seen: set[str] = set()
    for row in copies:
        while row["id"] in seen:
            row["id"] = uuid.uuid4().hex[:12]
        seen.add(row["id"])
    return copies


def currency(owner: Any) -> str:
    with _LOCK:
        return str((_state(owner).get("settings") or {}).get("currency") or "")


def set_currency(owner: Any, code: Any) -> str:
    value = str(code or "").strip().upper()
    if value and not _CURRENCY_RE.fullmatch(value):
        raise ShelfError("Invalid currency")
    with _LOCK:
        state = _state(owner)
        state["settings"] = {"currency": value} if value else {}
        _save(state, state["items"])
    return value


def entries(owner: Any) -> dict[str, dict[str, Any]]:
    with _LOCK:
        return _state(owner)["items"]


def summaries(owner: Any) -> dict[str, dict[str, Any]]:
    with _LOCK:
        state = _state(owner)
        if state["summaries"] is None:
            state["summaries"] = {key: summary(entry) for key, entry in state["items"].items()}
        return dict(state["summaries"])


def get(owner: Any, entity: Any, tmdb_id: Any) -> dict[str, Any] | None:
    key = title_key(entity, tmdb_id)
    with _LOCK:
        entry = _state(owner)["items"].get(key)
        return copy.deepcopy(entry) if entry else None


def put(owner: Any, entity: Any, tmdb_id: Any, payload: dict[str, Any]) -> dict[str, Any]:
    key = title_key(entity, tmdb_id)
    if not isinstance(payload, dict):
        raise ShelfError("Invalid shelf entry")
    with _LOCK:
        state = _state(owner)
        items = copy.deepcopy(state["items"])
        previous = items.get(key) or {}
        merged = {**payload, "added_at": previous.get("added_at") or _now(), "updated_at": _now()}
        if not str(payload.get("title") or "").strip() and previous.get("title"):
            merged["title"] = previous["title"]
        if payload.get("year") in (None, "") and previous.get("year"):
            merged["year"] = previous["year"]
        old_ids = previous.get("ids")
        new_ids = payload.get("ids")
        merged["ids"] = {**(old_ids if isinstance(old_ids, dict) else {}), **(new_ids if isinstance(new_ids, dict) else {})}
        entry = _clean_entry(key, merged, strict=True)
        if entry is None:
            raise ShelfError("Invalid shelf entry")
        items[key] = entry
        _save(state, items)
        return copy.deepcopy(entry)


def remove(owner: Any, entity: Any, tmdb_id: Any) -> bool:
    key = title_key(entity, tmdb_id)
    with _LOCK:
        state = _state(owner)
        items = copy.deepcopy(state["items"])
        if items.pop(key, None) is None:
            return False
        _save(state, items)
        return True
