# providers/sync/kodi/_addon_sync.py
# CrossWatch Kodi add-on sync transport
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import hashlib
import json
import re
import secrets
import threading
import time
import zlib
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

from cw_platform.provider_instances import _SYNC_USER_MARKER, list_sync_user_ids, normalize_instance_id, split_sync_user_id, sync_user_instance_id
from providers.auth._auth_KODI import KodiAuthError

VERSION = 1
FLAG_KEY = "kodi_addon_sync"
KODI_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
WRITE_METHODS = {"VideoLibrary.SetMovieDetails": "movieid", "VideoLibrary.SetEpisodeDetails": "episodeid"}
WRITE_KINDS = {"movieid": "movies", "episodeid": "episodes"}
KIND_METHODS = {"movies": "VideoLibrary.SetMovieDetails", "episodes": "VideoLibrary.SetEpisodeDetails"}
GROUPS = {"history": ("playcount", "lastplayed"), "ratings": ("userrating",), "progress": ("resume",)}
FIELD_GROUP = {field: group for group, fields in GROUPS.items() for field in fields}
WRITE_FIELDS = frozenset(FIELD_GROUP)
REF_KEY = "_cw_addon_ref"
MAX_ROWS = 250_000
MAX_PART_ROWS = 10_000
MAX_STORED_BYTES = 256 * 1024 * 1024
MAX_PARTS = 1000
QUEUE_LIMIT = 500
QUEUE_LIMIT_MAX = 1000
MAX_VIEWERS = 64
MAX_TEXT = 1000
RUN_RETRY_SECONDS = 30.0
RUN_RETRIES = 60
RUN_POLL_SECONDS = 5.0
RUN_WAIT_SECONDS = 6 * 3600.0
DELTA_RUN_DELAY = 60.0

_MOVIE_FIELDS = ("movieid", "uniqueid", "title", "year", "file", "playcount", "lastplayed", "userrating", "resume", "dateadded")
_EPISODE_FIELDS = ("episodeid", "tvshowid", "uniqueid", "title", "showtitle", "season", "episode", "file",
                   "playcount", "lastplayed", "userrating", "resume", "dateadded")
_SHOW_FIELDS = ("tvshowid", "uniqueid", "title", "year")
_KINDS = {"movies": ("movieid", _MOVIE_FIELDS), "episodes": ("episodeid", _EPISODE_FIELDS), "tvshows": ("tvshowid", _SHOW_FIELDS)}
_TEXT_FIELDS = ("title", "showtitle", "file", "dateadded")
_META_FIELDS = ("uniqueid", "title", "year", "showtitle", "season", "episode", "file")

_LOCK = threading.RLock()
_PENDING_RUNS: set[str] = set()
_RERUN: dict[str, list[str]] = {}
_DELAYED: dict[str, threading.Timer] = {}
_CACHE: dict[str, tuple[int, int, dict[str, Any]]] = {}
_NEXT_CHANGE: dict[str, int] = {}


class SyncRequestError(Exception):
    def __init__(self, status: int, code: str, message: str, retryable: bool = False):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.retryable = retryable


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any, limit: int = 200) -> str:
    return str(value or "").strip()[:limit]


def _int(value: Any) -> int | None:
    try:
        if isinstance(value, bool):
            return None
        return int(value)
    except Exception:
        return None


def _log(level: str, event: str, **fields: Any) -> None:
    try:
        from providers.sync.kodi._common import log

        log("addon_sync", level, event, **fields)
    except Exception:
        pass


def sync_enabled(cfg: Mapping[str, Any] | None) -> bool:
    return _dict(_dict(cfg).get("runtime")).get(FLAG_KEY) is True


def _root() -> Path:
    from cw_platform import config_base

    return Path(config_base.CONFIG) / ".cw_state" / "kodi_addon_sync"


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def _path(instance_id: Any, kind: str) -> Path:
    ident = normalize_instance_id(instance_id)
    prefix = re.sub(r"[^A-Za-z0-9._-]", "_", ident)[:40] or "default"
    return _root() / f"{prefix}-{_hash(ident)}.{kind}.json"


def _viewer_path(device: Any, viewer_id: Any, kind: str) -> Path:
    return _root() / f"viewer-{_hash(normalize_instance_id(device) + chr(10) + _text(viewer_id, 80))}.{kind}"


def _read(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _cached(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
    except Exception:
        return {}
    hit = _CACHE.get(str(path))
    if hit and hit[0] == stat.st_mtime_ns and hit[1] == stat.st_size:
        return hit[2]
    data = _read(path)
    _CACHE[str(path)] = (stat.st_mtime_ns, stat.st_size, data)
    return data


def _encode(data: Mapping[str, Any]) -> bytes:
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _write(path: Path, data: Mapping[str, Any] | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(data if isinstance(data, bytes) else _encode(data))
    tmp.replace(path)
    _CACHE.pop(str(path), None)


def _unlink(path: Path) -> None:
    try:
        path.unlink()
    except Exception:
        pass
    _CACHE.pop(str(path), None)


def device_of(instance_id: Any) -> str:
    ref = split_sync_user_id(instance_id)
    return ref[0] if ref else normalize_instance_id(instance_id)


def device_info(device: Any) -> dict[str, Any]:
    with _LOCK:
        return _cached(_path(device, "device"))


def _map(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _token_enabled(cfg: Mapping[str, Any] | None, device: str) -> bool:
    from providers.scrobble.kodi import addon as kodi_addon

    return kodi_addon.instance_enabled(cfg, device)


def _token_hash(cfg: Mapping[str, Any] | None, device: str) -> str:
    from providers.scrobble.kodi import addon as kodi_addon

    return _hash(kodi_addon.instance_token(cfg, device) or "")


def binding(cfg: Mapping[str, Any] | None, device: Any) -> dict[str, Any]:
    dev = normalize_instance_id(device)
    token = _token_hash(cfg, dev)
    data = _cached(_path(dev, "binding"))
    if data.get("token") != token:
        return {"token": token, "viewers": {}}
    return {"token": token, "device_id": data.get("device_id") or "", "bound_at": data.get("bound_at") or 0,
            "viewers": _dict(data.get("viewers"))}


def _save_binding(device: Any, data: Mapping[str, Any]) -> None:
    _write(_path(device, "binding"), {"token": data.get("token") or "", "device_id": data.get("device_id") or "",
                                      "bound_at": data.get("bound_at") or 0, "viewers": _dict(data.get("viewers"))})


def _require_device(cfg: Mapping[str, Any] | None, device: str, device_id: Any, *, bind: bool) -> None:
    did = _text(device_id, 128)
    if not did:
        raise SyncRequestError(400, "invalid_request", "Send the device id in the X-CrossWatch-Device header.")
    current = binding(cfg, device)
    bound = str(current.get("device_id") or "")
    if bound and bound != did:
        raise SyncRequestError(403, "not_sync_device", "Another Kodi device is the sync device for this connection.")
    if not bound:
        if not bind:
            raise SyncRequestError(403, "not_sync_device", "This device is not the sync device for this connection.")
        _save_binding(device, {**current, "device_id": did, "bound_at": time.time()})


def _clear_kodi_baselines(inst: str) -> None:
    try:
        from cw_platform.config_base import CONFIG_BASE
        from cw_platform.local_db.state import remove_provider_instance_state

        removed, items = remove_provider_instance_state(Path(CONFIG_BASE()), "KODI", inst)
    except Exception as exc:
        _log("warning", "kodi_baselines_clear_failed", instance=inst, error=type(exc).__name__)
        raise SyncRequestError(503, "temporarily_unavailable", "Could not prepare this instance for library sync.", True) from exc
    if removed:
        _log("info", "kodi_baselines_cleared", instance=inst, baselines=removed, items=items)


def uses_jsonrpc(cfg: Mapping[str, Any] | None, instance_id: Any) -> bool:
    from providers.sync.kodi._common import configured_block

    block = configured_block(cfg, normalize_instance_id(instance_id))
    return bool(str(block.get("server") or "").strip() and block.get("connection_verified") is True)


def _library_holder(cfg: Mapping[str, Any] | None, library_id: Any) -> str | None:
    lib = _text(library_id, 128)
    if not lib:
        return None
    from providers.scrobble.kodi import addon as kodi_addon

    best: tuple[tuple[float, str], str] | None = None
    paired = [normalize_instance_id(str(key)[len(kodi_addon.TOKEN_PREFIX):])
              for key in _dict(_dict(_dict(cfg).get("security")).get("webhook_ids"))
              if str(key).startswith(kodi_addon.TOKEN_PREFIX)]
    for inst in paired:
        info = device_info(inst)
        if info.get("library_id") != lib or info.get("enabled") is not True or not _token_enabled(cfg, inst):
            continue
        bound = binding(cfg, inst)
        if not bound.get("device_id"):
            continue
        rank = (float(bound.get("bound_at") or 0), inst)
        if best is None or rank < best[0]:
            best = (rank, inst)
    return best[1] if best else None


def library_taken(cfg: Mapping[str, Any] | None, device: Any) -> bool:
    dev = normalize_instance_id(device)
    holder = _library_holder(cfg, device_info(dev).get("library_id"))
    return bool(holder) and holder != dev


def device_active(cfg: Mapping[str, Any] | None, device: Any) -> bool:
    dev = normalize_instance_id(device)
    return (sync_enabled(cfg) and _token_enabled(cfg, dev) and device_info(dev).get("enabled") is True
            and not library_taken(cfg, dev))


def _viewer_ids(info: Mapping[str, Any]) -> list[str]:
    return [str(v.get("viewer_id")) for v in info.get("viewers") or [] if isinstance(v, Mapping) and v.get("viewer_id")]


def _parent_sync_users(cfg: Mapping[str, Any] | None, device: str) -> list[str]:
    from providers.sync.kodi._common import pick_instance_id

    users = list_sync_user_ids(dict(cfg or {}), "kodi", device)
    if users or device == "default":
        return users
    block = _dict(_dict(cfg).get("kodi"))
    if device in _dict(block.get("instances")) or normalize_instance_id(pick_instance_id(cfg)) != device:
        return []
    return [sync_user_instance_id(device, str(key).strip()) for key, entry in _dict(block.get("sync_users")).items()
            if str(key).strip() and isinstance(entry, Mapping)]


def _sync_users(cfg: Mapping[str, Any] | None, device: str) -> list[str]:
    users = _parent_sync_users(cfg, device)
    if users:
        return users
    block = _dict(_dict(cfg).get("kodi"))
    key = _text(block.get(_SYNC_USER_MARKER), 80)
    if not key:
        return []
    ref = split_sync_user_id(_dict(cfg).get("_cw_provider_instance"))
    if ref is not None and (ref[0] != device or ref[1] != key):
        return []
    return [sync_user_instance_id(device, key)]


def viewer_instance(cfg: Mapping[str, Any] | None, device: Any, viewer_id: Any, known: Iterable[str] | None = None) -> str | None:
    dev = normalize_instance_id(device)
    vid = _text(viewer_id, 80)
    if not vid:
        return None
    viewers = list(known) if known is not None else _viewer_ids(device_info(dev))
    if vid not in viewers:
        return None
    users = _sync_users(cfg, dev)
    candidate = sync_user_instance_id(dev, vid)
    if candidate in users:
        inst: str | None = candidate
    elif not users and ((viewers == [vid] and binding(cfg, dev)["viewers"].get(dev) in (None, vid))
                        or binding(cfg, dev)["viewers"].get(dev) == vid):
        inst = dev
    else:
        inst = None
    if inst is None or uses_jsonrpc(cfg, inst):
        return None
    return inst


def _generation(viewer_id: Any, info: Mapping[str, Any]) -> str:
    return f"{_text(viewer_id, 80)}|{_text(info.get('library_id'), 128)}|{_int(info.get('rev')) or 0}"


def _clean_viewers(value: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in value if isinstance(value, list) else []:
        if not isinstance(row, Mapping):
            continue
        vid = _text(row.get("viewer_id"), 80)
        if not vid or vid in seen:
            continue
        seen.add(vid)
        out.append({"viewer_id": vid, "name": _text(row.get("name"), 80)})
        if len(out) >= MAX_VIEWERS:
            break
    return out


def handle_ping(cfg: Mapping[str, Any] | None, device: Any, body: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not sync_enabled(cfg):
        return None
    try:
        dev = normalize_instance_id(device)
        payload = _dict(body)
        sent = _dict(payload.get("library_sync"))
        device_id = _text(_dict(payload.get("device")).get("id"), 128)
        with _LOCK:
            current = binding(cfg, dev)
            bound = str(current.get("device_id") or "")
            if bound and device_id != bound:
                return {"version": VERSION, "enabled": False, "reason": "not_sync_device", "viewers": [], "queue_pending": 0}
            if sent and not device_id:
                return {"version": VERSION, "enabled": False, "reason": "device_id_missing", "viewers": [], "queue_pending": 0}
            wants_sync = bool(sent) and sent.get("enabled", True) is not False
            if bound and sent and not wants_sync:
                _save_binding(dev, {"token": current["token"]})
            elif not bound and wants_sync:
                _save_binding(dev, {**current, "device_id": device_id, "bound_at": time.time()})
            path = _path(dev, "device")
            info = _read(path)
            before = dict(info)
            info["instance"] = dev
            if sent:
                info["enabled"] = sent.get("enabled", True) is not False
                info["viewers"] = _clean_viewers(sent.get("viewers"))
                for key, limit in (("library_id", 128), ("kodi_version", 40), ("jsonrpc_version", 40)):
                    if key in sent:
                        info[key] = _text(sent.get(key), limit)
            known = _viewer_ids(info)
            mapping = [[vid, viewer_instance(cfg, dev, vid, known)] for vid in known]
            mapping_key = json.dumps([info.get("library_id") or "", mapping], sort_keys=True)
            if info.get("mapping_key") != mapping_key:
                info["rev"] = (_int(info.get("rev")) or 0) + 1
                info["mapping_key"] = mapping_key
            if info != before:
                _write(path, info)
            if library_taken(cfg, dev):
                return {"version": VERSION, "enabled": False, "reason": "library_has_sync_device", "viewers": [], "queue_pending": 0}
        viewers = []
        pending = 0
        for vid, inst in mapping:
            if inst:
                viewers.append({"viewer_id": vid, "instance": inst, "upload_seq": last_upload_seq(dev, vid),
                                "next_change": _change_floor(dev, vid), "run_pending": _run_pending(inst)})
                pending += len(pending_changes(cfg, inst))
        return {"version": VERSION, "enabled": device_active(cfg, dev), "viewers": viewers, "queue_pending": pending}
    except Exception:
        return {"version": VERSION, "enabled": False, "viewers": [], "queue_pending": 0}


def _zone(name: Any) -> ZoneInfo | None:
    text = _text(name, 64)
    if not text:
        return None
    try:
        return ZoneInfo(text)
    except Exception:
        return None


def to_utc(value: Any, zone: ZoneInfo | None = None) -> str:
    text = str(value or "").strip()
    if not text or text.startswith("0000"):
        return ""
    if "T" in text:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    else:
        dt = datetime.strptime(text[:19], KODI_TIME_FORMAT)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=zone or timezone.utc)
    return dt.astimezone(timezone.utc).strftime(UTC_FORMAT)


def _identity(kind: str, row: Mapping[str, Any], shows: Mapping[str, Any] | None = None) -> str:
    from providers.sync.kodi._common import normalize_uniqueids

    ids = normalize_uniqueids(row.get("uniqueid"))
    if ids:
        return kind + "|" + "|".join(f"{k}:{v}" for k, v in sorted(ids.items()))
    if kind == "episodes":
        show = row.get("show") if isinstance(row.get("show"), Mapping) else _map(_map(shows).get(f"id:{_int(row.get('tvshowid'))}"))
        show_ids = normalize_uniqueids(_map(show).get("uniqueid"))
        if show_ids:
            owner = "show:" + "|".join(f"{k}:{v}" for k, v in sorted(show_ids.items()))
        else:
            owner = _text(row.get("showtitle") or _map(show).get("title"), 200).casefold()
        return "|".join([kind, owner, str(row.get("season")), str(row.get("episode"))])
    return "|".join([kind, _text(row.get("title"), 200).casefold(), str(row.get("year"))])


def _tokens(kind: str, row: Mapping[str, Any], shows: Mapping[str, Any] | None = None) -> list[str]:
    from providers.sync.kodi._common import normalize_uniqueids

    ids = normalize_uniqueids(row.get("uniqueid"))
    tokens = [f"{kind}|{k}:{v}" for k, v in sorted(ids.items())]
    if kind == "episodes":
        show = row.get("show") if isinstance(row.get("show"), Mapping) else _map(_map(shows).get(f"id:{_int(row.get('tvshowid'))}"))
        show_ids = normalize_uniqueids(_map(show).get("uniqueid"))
        tokens += [f"{kind}|show:{k}:{v}|{row.get('season')}|{row.get('episode')}" for k, v in sorted(show_ids.items())]
    return tokens or [_identity(kind, row, shows)]


def _aliases(view: Mapping[str, Any], *, attach: bool = False) -> dict[str, str]:
    found = view.get("aliases")
    if isinstance(found, dict):
        return found
    aliases: dict[str, str] = {}
    for key, entry in _map(view.get("entries")).items():
        kind = str(_map(entry).get("kind") or "")
        meta = _map(_map(entry).get("meta"))
        if kind in ("movies", "episodes") and meta:
            for token in _tokens(kind, meta):
                aliases.setdefault(token, str(key))
    if attach and isinstance(view, dict):
        view["aliases"] = aliases
    return aliases


def _matching_keys(aliases: Mapping[str, str], kind: str, row: Mapping[str, Any], shows: Mapping[str, Any] | None = None) -> list[str]:
    return sorted({aliases[t] for t in _tokens(kind, row, shows) if t in aliases})


def _canonical(aliases: dict[str, str], kind: str, row: Mapping[str, Any], shows: Mapping[str, Any] | None = None,
               *, register: bool = False) -> str:
    tokens = _tokens(kind, row, shows)
    keys = _matching_keys(aliases, kind, row, shows)
    key = keys[0] if keys else _identity(kind, row, shows)
    if register:
        for token in tokens:
            aliases[token] = key
    return key


def _merge_wins(group: str, theirs: Mapping[str, Any], mine: Mapping[str, Any]) -> bool:
    if group != "history":
        return (_int(theirs.get("src")) or 0) > (_int(mine.get("src")) or 0)
    if bool(theirs.get("watched")) != bool(mine.get("watched")):
        negative, positive = (mine, theirs) if theirs.get("watched") else (theirs, mine)
        unwatch_wins = negative.get("explicit") is True and _event(negative) > _event(positive)
        return (negative is theirs) == unwatch_wins
    return _event(theirs) > _event(mine)


def _merge_keys(view: dict[str, Any], changes: dict[str, Any], primary: str, others: Iterable[str]) -> None:
    entries = view["entries"]
    aliases = _aliases(view, attach=True)
    records = changes["records"]
    target = entries.setdefault(primary, {"kind": _map(entries.get(next(iter(others), ""), {})).get("kind")})
    for other in others:
        if other == primary:
            continue
        source = _map(entries.pop(other, None))
        if source:
            target["meta"] = {**_map(source.get("meta")), **_map(target.get("meta"))}
            target.setdefault("kind", source.get("kind"))
            for group in GROUPS:
                mine, theirs = _map(target.get(group)), _map(source.get(group))
                if theirs and (not mine or _merge_wins(group, theirs, mine)):
                    target[group] = dict(theirs)
        for token, key in list(aliases.items()):
            if key == other:
                aliases[token] = primary
        for group in GROUPS:
            rec = records.pop(_rec_key(other, group), None)
            if rec is None:
                continue
            current = records.get(_rec_key(primary, group))
            if current is None or (_int(rec.get("seq")) or 0) > (_int(current.get("seq")) or 0):
                records[_rec_key(primary, group)] = {**rec, "ident": primary}


def _whole(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0


def _check_state(kind: str, row: Mapping[str, Any]) -> None:
    resume = row.get("resume")
    valid = (
        _whole(row.get("playcount")) and row["playcount"] >= 0
        and _whole(row.get("userrating")) and 0 <= row["userrating"] <= 10
        and isinstance(row.get("lastplayed"), str)
        and isinstance(resume, Mapping) and _number(resume.get("position")) and _number(resume.get("total"))
        and ("unwatched" not in row or isinstance(row.get("unwatched"), bool))
    )
    if not valid:
        raise SyncRequestError(400, "invalid_request", f"A {kind} row needs a valid playcount, lastplayed, userrating and resume.")


def _clean_row(kind: str, raw: Any, zone: ZoneInfo | None) -> tuple[str, dict[str, Any]]:
    from providers.sync.kodi._common import strip_path_userinfo

    if not isinstance(raw, Mapping):
        raise SyncRequestError(400, "invalid_request", f"Every {kind} row must be an object.")
    id_key, fields = _KINDS[kind]
    row = {k: raw[k] for k in fields if k in raw}
    if "uniqueid" in row and not isinstance(row["uniqueid"], Mapping):
        raise SyncRequestError(400, "invalid_request", f"A {kind} row has an invalid uniqueid.")
    for field in _TEXT_FIELDS:
        if field in row:
            if row[field] is not None and not isinstance(row[field], str):
                raise SyncRequestError(400, "invalid_request", f"A {kind} row has an invalid {field}.")
            row[field] = str(row[field] or "")[:MAX_TEXT]
    if row.get("file"):
        row["file"] = strip_path_userinfo(row["file"])
    if kind != "tvshows":
        if "unwatched" in raw:
            row["unwatched"] = raw["unwatched"]
        _check_state(kind, row)
        try:
            row["lastplayed"] = to_utc(row["lastplayed"], zone)
        except Exception as exc:
            raise SyncRequestError(400, "invalid_request", f"A {kind} row has an invalid lastplayed.") from exc
        if row.get("dateadded"):
            try:
                row["dateadded"] = to_utc(row["dateadded"], zone)
            except Exception:
                row["dateadded"] = ""
        row["resume"] = {"position": row["resume"]["position"], "total": row["resume"]["total"]}
    if kind != "tvshows" and raw.get("in_library") is False:
        row.pop(id_key, None)
        row.pop("tvshowid", None)
        row["in_library"] = False
        if kind == "episodes":
            show = _dict(raw.get("show"))
            if show:
                row["show"] = {k: show[k] for k in ("uniqueid", "title", "year") if k in show}
        return "ghost:" + _identity(kind, row), row
    kid = _int(row.get(id_key))
    if kid is None or kid <= 0:
        raise SyncRequestError(400, "invalid_request", f"A {kind} row has no valid {id_key}.")
    row[id_key] = kid
    return f"id:{kid}", row


def _clean_rows(body: Mapping[str, Any], zone: ZoneInfo | None, *, required: bool) -> dict[str, dict[str, dict[str, Any]]]:
    out: dict[str, dict[str, dict[str, Any]]] = {}
    total = 0
    for kind in _KINDS:
        raw_rows = body.get(kind)
        if raw_rows is None and not required:
            out[kind] = {}
            continue
        if not isinstance(raw_rows, list):
            raise SyncRequestError(400, "invalid_request", f"{kind} must be a list.")
        rows: dict[str, dict[str, Any]] = {}
        for raw in raw_rows:
            key, row = _clean_row(kind, raw, zone)
            rows[key] = row
        total += len(rows)
        out[kind] = rows
    if total > MAX_ROWS:
        raise SyncRequestError(400, "invalid_request", "Too many rows.")
    return out


def _row_count(rows: Mapping[str, Any]) -> int:
    return sum(len(_dict(rows.get(kind))) for kind in _KINDS)


def _staged_rows(staging: Mapping[str, Any]) -> int:
    total = sum(_row_count(_dict(_dict(chunk).get("rows"))) for chunk in _dict(staging.get("received")).values())
    return total + sum(_row_count(_dict(_dict(d).get("rows"))) for d in staging.get("deltas") or [])


def _clean_sources(value: Any) -> list[dict[str, Any]]:
    from providers.sync.kodi._common import strip_path_userinfo

    out: list[dict[str, Any]] = []
    for row in value if isinstance(value, list) else []:
        if isinstance(row, Mapping) and isinstance(row.get("file"), str) and row.get("file"):
            out.append({"file": strip_path_userinfo(row["file"][:MAX_TEXT]), "label": _text(row.get("label"), 200)})
        if len(out) >= 500:
            break
    return out


def _check_zone(name: str) -> ZoneInfo | None:
    if not name:
        return None
    zone = _zone(name)
    if zone is None:
        raise SyncRequestError(400, "invalid_request", "timezone must be an IANA time zone.")
    return zone


def _too_large(kind: str) -> SyncRequestError:
    return SyncRequestError(413, "payload_too_large", f"The {kind} holds too many rows.")


def _stale() -> SyncRequestError:
    return SyncRequestError(409, "upload_stale", "CW already has a newer upload. Send a new one with a higher upload_seq.")


class StateUnreadable(Exception):
    pass


def _read_strict(path: Path) -> dict[str, Any] | None:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except Exception as exc:
        raise StateUnreadable(path.name) from exc
    try:
        data = json.loads(text)
    except Exception as exc:
        raise StateUnreadable(path.name) from exc
    if not isinstance(data, dict):
        raise StateUnreadable(path.name)
    return data


def _cached_strict(path: Path) -> dict[str, Any] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    except Exception as exc:
        raise StateUnreadable(path.name) from exc
    hit = _CACHE.get(str(path))
    if hit and hit[0] == stat.st_mtime_ns and hit[1] == stat.st_size:
        return hit[2]
    data = _read_strict(path)
    if data is None:
        return None
    _CACHE[str(path)] = (stat.st_mtime_ns, stat.st_size, data)
    return data


def _view_file(device: Any, viewer_id: Any) -> Path:
    return _viewer_path(device, viewer_id, "view.json")


def _load_view(device: Any, viewer_id: Any) -> dict[str, Any]:
    view = dict(_read_strict(_view_file(device, viewer_id)) or {})
    view.setdefault("entries", {})
    view.setdefault("max_seq", 0)
    return view


def _seq_file(device: Any, viewer_id: Any) -> Path:
    return _viewer_path(device, viewer_id, "seq.json")


def _load_seq(device: Any, viewer_id: Any) -> dict[str, Any]:
    try:
        return dict(_cached_strict(_seq_file(device, viewer_id)) or {})
    except StateUnreadable as exc:
        _log("error", "upload_order_unreadable", file=str(exc))
        raise SyncRequestError(503, "temporarily_unavailable", "Upload order state is unreadable on the server.", True) from exc


def _update_seq(device: Any, viewer_id: Any, **fields: Any) -> None:
    path = _seq_file(device, viewer_id)
    _write(path, {**_map(_cached_strict(path)), **fields})


def last_upload_seq(device: Any, viewer_id: Any) -> int:
    with _LOCK:
        return _int(_load_seq(device, viewer_id).get("max_seq")) or 0


def _applied_upload_seq(device: Any, viewer_id: Any) -> int:
    return _int(_load_seq(device, viewer_id).get("applied_upload_seq")) or 0


def _sync_running() -> bool:
    try:
        from api.syncAPI import _is_sync_running

        return bool(_is_sync_running())
    except Exception:
        return False


def _reset_viewer(cfg: Mapping[str, Any] | None, device: str, viewer_id: str, inst: str, reason: str) -> None:
    stamp = str(int(time.time()))
    for path in (_view_file(device, viewer_id), _changes_file(device, viewer_id)):
        try:
            if path.exists():
                path.replace(path.with_name(f"{path.name}.broken-{stamp}"))
        except Exception:
            _unlink(path)
        _CACHE.pop(str(path), None)
    for old in _root().glob(_viewer_path(device, viewer_id, "changes.*.log").name):
        _unlink(old)
    _unlink(_path(inst, "inventory"))
    _unlink(_path(inst, "staging"))
    current = binding(cfg, device)
    viewers = dict(current["viewers"])
    viewers.pop(inst, None)
    _save_binding(device, {**current, "viewers": viewers})
    _update_seq(device, viewer_id, changes_initialized=False)
    _log("error", "viewer_state_reset", instance=inst, reason=reason)


def _change_id(rec: Mapping[str, Any]) -> str:
    return _text(rec.get("change_id"), 64)


def _rec_key(ident: str, group: str) -> str:
    return f"{ident}\t{group}"


def _changes_file(device: Any, viewer_id: Any) -> Path:
    return _viewer_path(device, viewer_id, "changes.json")


def _log_file(device: Any, viewer_id: Any, number: int) -> Path:
    return _viewer_path(device, viewer_id, f"changes.{number}.log")


def _changes_snapshot(device: Any, viewer_id: Any) -> dict[str, Any]:
    return dict(_cached_strict(_changes_file(device, viewer_id)) or {})


def _load_changes(device: Any, viewer_id: Any) -> dict[str, Any]:
    snapshot = _cached_strict(_changes_file(device, viewer_id))
    if snapshot is None:
        seq_state = _map(_cached_strict(_seq_file(device, viewer_id)))
        numbered = [p for p in _root().glob(_viewer_path(device, viewer_id, "changes.*.log").name)
                    if p.name != _log_file(device, viewer_id, 0).name]
        if seq_state.get("changes_initialized") is True or numbered:
            raise StateUnreadable(_changes_file(device, viewer_id).name)
    data = dict(snapshot or {})
    number = _int(data.get("log")) or 0
    records: dict[str, Any] = {k: dict(v) for k, v in _dict(data.get("records")).items() if isinstance(v, Mapping)}
    path = _log_file(device, viewer_id, number)
    try:
        lines = path.read_text(encoding="utf-8").split("\n")
    except FileNotFoundError:
        lines = []
    except Exception as exc:
        raise StateUnreadable(path.name) from exc
    complete = lines[:-1]
    for index, line in enumerate(complete):
        try:
            op = json.loads(line)
        except Exception as exc:
            raise StateUnreadable(f"{path.name}:{index + 1}") from exc
        if not isinstance(op, Mapping) or op.get("op") != "set" or not isinstance(op.get("rec"), Mapping):
            raise StateUnreadable(f"{path.name}:{index + 1}")
        rec = dict(op["rec"])
        records[_rec_key(str(rec.get("ident")), str(rec.get("group")))] = rec
    seqs = [_int(r.get("seq")) or 0 for r in records.values()]
    return {"records": records, "log": number, "next_change": max([_int(data.get("next_change")) or 1, *[s + 1 for s in seqs]])}


def _save_changes(device: Any, viewer_id: Any, data: Mapping[str, Any]) -> None:
    number = _int(data.get("log")) or 0
    _write(_changes_file(device, viewer_id), {"records": data.get("records") or {}, "next_change": data.get("next_change") or 1, "log": number + 1})
    _unlink(_log_file(device, viewer_id, number))
    seq_state = _map(_cached_strict(_seq_file(device, viewer_id)))
    if seq_state.get("changes_initialized") is not True:
        _write(_seq_file(device, viewer_id), {**seq_state, "changes_initialized": True})


def _append_change(device: Any, viewer_id: Any, op: Mapping[str, Any]) -> None:
    number = _int(_changes_snapshot(device, viewer_id).get("log")) or 0
    path = _log_file(device, viewer_id, number)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            if size:
                handle.seek(size - 1)
            tail_ok = not size or handle.read(1) == b"\n"
    except FileNotFoundError:
        tail_ok = True
    if not tail_ok:
        raw = path.read_bytes()
        _write(path, raw[: raw.rfind(b"\n") + 1])
        _log("warning", "change_log_tail_repaired", file=path.name)
    with path.open("ab") as handle:
        handle.write(json.dumps(op, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n")


CHANGE_BLOCK = 1000


def _clock_floor() -> int:
    return int(time.time() * 1000)


def _change_floor(device: Any, viewer_id: Any) -> int:
    key = str(_viewer_path(device, viewer_id, "changes"))
    if key in _NEXT_CHANGE:
        return _NEXT_CHANGE[key]
    reserved = _int(_map(_cached_strict(_seq_file(device, viewer_id))).get("change_reserved")) or 1
    return max(int(_load_changes(device, viewer_id)["next_change"]), reserved, _clock_floor())


def _raise_change_floor(device: Any, viewer_id: Any, seen: int) -> None:
    if seen <= 0:
        return
    key = str(_viewer_path(device, viewer_id, "changes"))
    _NEXT_CHANGE[key] = max(_change_floor(device, viewer_id), seen + 1)
    seq_path = _seq_file(device, viewer_id)
    seq_state = _map(_cached_strict(seq_path))
    if (_int(seq_state.get("change_reserved")) or 1) <= seen:
        _write(seq_path, {**seq_state, "change_reserved": seen + 1})


def _next_change(device: Any, viewer_id: Any) -> int:
    key = str(_viewer_path(device, viewer_id, "changes"))
    seq_path = _seq_file(device, viewer_id)
    seq_state = _map(_cached_strict(seq_path))
    reserved = _int(seq_state.get("change_reserved")) or 1
    if key not in _NEXT_CHANGE:
        _NEXT_CHANGE[key] = _change_floor(device, viewer_id)
    seq = _NEXT_CHANGE[key]
    if seq >= reserved:
        _write(seq_path, {**seq_state, "change_reserved": seq + CHANGE_BLOCK})
    _NEXT_CHANGE[key] = seq + 1
    return seq


def _ids_conflict(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    from providers.sync.kodi._common import normalize_uniqueids

    a, b = normalize_uniqueids(left.get("uniqueid")), normalize_uniqueids(right.get("uniqueid"))
    return any(k in b and b[k] != v for k, v in a.items())


def _keep_show_ids(new_shows: dict[str, Any], old_shows: Mapping[str, Any]) -> None:
    from providers.sync.kodi._common import normalize_uniqueids

    for key, row in list(new_shows.items()):
        old = _map(old_shows.get(key))
        if normalize_uniqueids(_map(row).get("uniqueid")) or not normalize_uniqueids(old.get("uniqueid")):
            continue
        if _text(_map(row).get("title"), 200).casefold() == _text(old.get("title"), 200).casefold():
            new_shows[key] = {**dict(row), "uniqueid": dict(_map(old.get("uniqueid")))}


def _check_show_refs(inventory: Mapping[str, Any]) -> None:
    shows = _map(inventory.get("tvshows"))
    for row in _map(inventory.get("episodes")).values():
        if f"id:{_int(_map(row).get('tvshowid'))}" not in shows:
            raise SyncRequestError(400, "invalid_request", "An episode refers to a show that is not in the upload.")


def _apply_inventory(inventory: dict[str, Any], rows: Mapping[str, Mapping[str, dict[str, Any]]]) -> None:
    shows = inventory.setdefault("tvshows", {})
    incoming_shows = dict(_dict(rows.get("tvshows")))
    _keep_show_ids(incoming_shows, shows)
    shows.update(incoming_shows)
    for kind in ("movies", "episodes"):
        incoming = _dict(rows.get(kind))
        if not incoming:
            continue
        current = inventory.setdefault(kind, {})
        by_file: dict[str, set[str]] = {}
        by_token: dict[str, set[str]] = {}
        keys_info: dict[str, tuple[str, frozenset[str]]] = {}

        def index(key: str, row: Mapping[str, Any]) -> None:
            info = (str(row.get("file") or ""), frozenset(_tokens(kind, row, shows)))
            keys_info[key] = info
            if info[0]:
                by_file.setdefault(info[0], set()).add(key)
            for token in info[1]:
                by_token.setdefault(token, set()).add(key)

        def drop(key: str) -> None:
            info = keys_info.pop(key, None)
            current.pop(key, None)
            if info:
                by_file.get(info[0], set()).discard(key)
                for token in info[1]:
                    by_token.get(token, set()).discard(key)

        def same_item(row_file: str, tokens: frozenset[str]) -> set[str]:
            sharing = set().union(*(by_token.get(token, set()) for token in tokens)) if tokens else set()
            return sharing & by_file.get(row_file, set()) if row_file else sharing

        for key, row in current.items():
            index(key, row)
        for key, row in incoming.items():
            tokens = frozenset(_tokens(kind, row, shows))
            row_file = str(row.get("file") or "")
            if key.startswith("id:"):
                for other in same_item(row_file, tokens) if row_file else set():
                    if other != key:
                        drop(other)
                drop(key)
                current[key] = row
                index(key, row)
                continue
            if row_file:
                for other in list(by_file.get(row_file, set())):
                    if not _ids_conflict(row, current.get(other) or {}):
                        drop(other)
            else:
                for other in same_item("", tokens):
                    drop(other)


def _meta(kind: str, row: Mapping[str, Any], shows: Mapping[str, Any]) -> dict[str, Any]:
    meta = {k: row[k] for k in _META_FIELDS if k in row}
    if kind == "episodes":
        show = _map(row.get("show")) or _map(_map(shows).get(f"id:{_int(row.get('tvshowid'))}"))
        if show:
            meta["show"] = {k: show[k] for k in ("uniqueid", "title", "year") if k in show}
    return meta


def _event(state: Mapping[str, Any]) -> int:
    return _int(state.get("event")) or _int(state.get("src")) or 0


def _row_group(group: str, row: Mapping[str, Any], prior: Mapping[str, Any], upload_seq: int = 0) -> dict[str, Any]:
    if group == "history":
        playcount = _int(row.get("playcount")) or 0
        if playcount > 0:
            return {"watched": True, "playcount": playcount, "lastplayed": row.get("lastplayed") or "", "event": upload_seq}
        if row.get("unwatched") is True:
            return {"watched": False, "playcount": 0, "lastplayed": "", "explicit": True, "event": upload_seq}
        if not prior.get("watched"):
            return {"watched": False, "playcount": 0, "lastplayed": "", "explicit": prior.get("explicit") is True, "event": _event(prior)}
        return {**{k: prior[k] for k in ("watched", "playcount", "lastplayed") if k in prior}, "event": _event(prior)}
    if group == "ratings":
        return {"userrating": _int(row.get("userrating")) or 0}
    return {"resume": dict(_dict(row.get("resume")))}


def _positive(group: str, values: Mapping[str, Any]) -> bool:
    if group == "history":
        return bool(values.get("watched"))
    if group == "ratings":
        return (_int(values.get("userrating")) or 0) > 0
    try:
        return float(_dict(values.get("resume")).get("position") or 0) > 0
    except Exception:
        return False


def _apply_rows(view: dict[str, Any], changes: dict[str, Any], rows: Mapping[str, Mapping[str, dict[str, Any]]],
                shows: Mapping[str, Any], upload_seq: int, applied_seq: int) -> bool:
    entries = view["entries"]
    records = changes["records"]
    aliases = _aliases(view, attach=True)
    changed = False
    for kind in ("movies", "episodes"):
        for row in _dict(rows.get(kind)).values():
            keys = _matching_keys(aliases, kind, row, shows)
            if len(keys) > 1:
                _merge_keys(view, changes, keys[0], keys[1:])
            ident = _canonical(aliases, kind, row, shows, register=True)
            entry = entries.setdefault(ident, {"kind": kind})
            meta = _meta(kind, row, shows)
            if meta:
                entry["meta"] = {**_dict(entry.get("meta")), **meta}
            for group in GROUPS:
                state = _dict(entry.get(group))
                if (_int(state.get("src")) or 0) > upload_seq:
                    continue
                key = _rec_key(ident, group)
                rec = records.get(key)
                before = _state_fields(_effective(entry, records, ident))
                if rec is not None:
                    seq = _int(rec.get("seq")) or 0
                    if rec.get("status") == "failed":
                        candidate = _row_group(group, row, {}, upload_seq)
                        if upload_seq <= (_int(rec.get("after")) or 0) or not _positive(group, candidate):
                            continue
                        records.pop(key, None)
                    elif seq > applied_seq or rec.get("status") != "applied":
                        continue
                    else:
                        state = {**_dict(rec.get("values")), "src": state.get("src"), "event": upload_seq}
                        records.pop(key, None)
                new_state = _row_group(group, row, state, upload_seq)
                new_state["src"] = upload_seq
                entry[group] = new_state
                changed = changed or _state_fields(_effective(entry, records, ident)) != before
    return changed


def _effective(entry: Mapping[str, Any], records: Mapping[str, Any], ident: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for group in GROUPS:
        rec = records.get(_rec_key(ident, group))
        out[group] = _dict(rec.get("values")) if isinstance(rec, Mapping) else _dict(_dict(entry).get(group))
    return out


def _has_state(state: Mapping[str, Mapping[str, Any]]) -> bool:
    return any(_positive(group, _dict(state.get(group))) for group in GROUPS)


def _state_fields(state: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    history = _dict(state.get("history"))
    watched = bool(history.get("watched"))
    return {
        "playcount": max(1, _int(history.get("playcount")) or 1) if watched else 0,
        "lastplayed": history.get("lastplayed") or "" if watched else "",
        "userrating": _int(_dict(state.get("ratings")).get("userrating")) or 0,
        "resume": dict(_dict(_dict(state.get("progress")).get("resume"))) or {"position": 0, "total": 0},
    }


def _check_view_size(view: Mapping[str, Any]) -> bytes:
    encoded = _encode(view)
    if len(_dict(view.get("entries"))) > MAX_ROWS * 2 or len(encoded) > MAX_STORED_BYTES:
        raise _too_large("viewer state")
    return encoded


def _abort_staging(inst: str, staging_path: Path, staging: Mapping[str, Any], reason: str) -> None:
    _unlink(staging_path)
    _log("warning", "full_upload_dropped", instance=inst, upload_seq=staging.get("upload_seq"), reason=reason)


def receive_upload(cfg: Mapping[str, Any] | None, device: Any, body: Mapping[str, Any], *, device_id: Any = "") -> dict[str, Any]:
    dev = normalize_instance_id(device)
    if not device_active(cfg, dev):
        raise SyncRequestError(403, "sync_disabled", "Library sync is off for this device.")
    with _LOCK:
        _require_device(cfg, dev, device_id, bind=True)
    if any(isinstance(body.get(kind), list) and len(body[kind]) > MAX_PART_ROWS for kind in _KINDS):
        raise SyncRequestError(413, "payload_too_large", f"Send at most {MAX_PART_ROWS} rows per list in one request.")
    if _int(body.get("version")) != VERSION:
        raise SyncRequestError(400, "invalid_request", "Unsupported version.")
    viewer_id = _text(body.get("viewer_id"), 80)
    inst = viewer_instance(cfg, dev, viewer_id)
    if not inst:
        raise SyncRequestError(409, "viewer_unknown", "Unknown viewer.")
    mode = _text(body.get("mode"), 10).lower()
    if mode not in ("full", "delta"):
        raise SyncRequestError(400, "invalid_request", "mode must be full or delta.")
    zone = _check_zone(_text(body.get("timezone"), 64))
    rows = _clean_rows(body, zone, required=mode == "full")
    applied_seq = max(0, _int(body.get("applied_seq")) or 0)
    upload_seq = _int(body.get("upload_seq"))
    if upload_seq is None or upload_seq < 1:
        raise SyncRequestError(400, "invalid_request", "upload_seq must be a whole number of 1 or more.")
    with _LOCK:
        if not device_active(cfg, dev):
            raise SyncRequestError(403, "sync_disabled", "Library sync is off for this device.")
        _require_device(cfg, dev, device_id, bind=True)
        if viewer_instance(cfg, dev, viewer_id) != inst:
            raise SyncRequestError(409, "viewer_unknown", "Unknown viewer.")
        generation = _generation(viewer_id, device_info(dev))
        try:
            _raise_change_floor(dev, viewer_id, applied_seq)
        except StateUnreadable:
            pass
        inventory_path = _path(inst, "inventory")
        staging_path = _path(inst, "staging")
        view_path = _view_file(dev, viewer_id)
        inventory = _read(inventory_path)
        seq_state = _load_seq(dev, viewer_id)
        try:
            view = _load_view(dev, viewer_id)
            changes = _load_changes(dev, viewer_id)
            lost = inventory.get("complete") is True and inventory.get("viewer_id") == viewer_id and not view_path.exists()
            reason = "viewer_state_missing"
        except StateUnreadable as exc:
            lost, reason = True, f"unreadable:{exc}"
        if lost:
            _reset_viewer(cfg, dev, viewer_id, inst, reason)
            inventory, view = {}, {"entries": {}}
            changes = _load_changes(dev, viewer_id)
        staging = _read(staging_path)
        if staging.get("upload_seq") and staging.get("generation") != generation:
            _abort_staging(inst, staging_path, staging, "viewer_or_library_changed")
            staging = {}
        max_seq = _int(seq_state.get("max_seq")) or 0
        if mode == "delta":
            if inventory.get("complete") is not True or inventory.get("generation") != generation:
                raise SyncRequestError(409, "upload_incomplete", "Send a full upload first.")
            if upload_seq <= max_seq:
                if upload_seq == _int(seq_state.get("last_delta_seq")):
                    return {"ok": True, "mode": "delta", "complete": True, "instance": inst,
                            "changed": _int(seq_state.get("delta_changed")) == upload_seq}
                if not (upload_seq == max_seq and upload_seq == _int(seq_state.get("delta_pending"))):
                    raise _stale()
            _update_seq(dev, viewer_id, max_seq=upload_seq, delta_pending=upload_seq)
            _apply_inventory(inventory, rows)
            _check_show_refs(inventory)
            inventory_bytes = _encode(inventory)
            if _row_count(inventory) > MAX_ROWS or len(inventory_bytes) > MAX_STORED_BYTES:
                raise _too_large("library")
            changed = _apply_rows(view, changes, rows, _dict(inventory.get("tvshows")), upload_seq, applied_seq)
            view_bytes = _check_view_size(view)
            if changed:
                _update_seq(dev, viewer_id, delta_changed=upload_seq)
            else:
                changed = _int(seq_state.get("delta_changed")) == upload_seq
            _write(inventory_path, inventory_bytes)
            _write(view_path, view_bytes)
            _save_changes(dev, viewer_id, changes)
            _update_seq(dev, viewer_id, applied_upload_seq=max(upload_seq, _applied_upload_seq(dev, viewer_id)))
            if staging.get("upload_seq"):
                staging.setdefault("deltas", []).append({"rows": rows})
                staged = _encode(staging)
                if _staged_rows(staging) > MAX_ROWS or len(staged) > MAX_STORED_BYTES:
                    _abort_staging(inst, staging_path, staging, "too_large")
                else:
                    _write(staging_path, staged)
            _update_seq(dev, viewer_id, last_delta_seq=upload_seq, delta_pending=None)
            return {"ok": True, "mode": "delta", "complete": True, "instance": inst, "changed": changed}
        upload_id = _text(body.get("upload_id"), 80)
        part = _int(body.get("part")) or 1
        parts = _int(body.get("parts")) or 1
        if not upload_id or parts < 1 or parts > MAX_PARTS or part < 1 or part > parts:
            raise SyncRequestError(400, "invalid_request", "Invalid upload_id, part or parts.")
        if inventory.get("complete") is True and inventory.get("generation") == generation and _int(inventory.get("upload_seq")) == upload_seq:
            return {"ok": True, "mode": "full", "complete": True, "instance": inst}
        if staging.get("upload_seq") and _int(staging.get("upload_seq")) == upload_seq:
            if staging.get("upload_id") != upload_id or staging.get("parts") != parts:
                raise SyncRequestError(400, "invalid_request", "upload_id or parts changed during the upload.")
        else:
            if upload_seq <= max_seq:
                raise _stale()
            if staging.get("upload_seq"):
                _abort_staging(inst, staging_path, staging, "replaced")
            staging = {"upload_id": upload_id, "upload_seq": upload_seq, "parts": parts, "received": {}, "deltas": [],
                       "viewer_id": viewer_id, "generation": generation}
            _update_seq(dev, viewer_id, max_seq=upload_seq)
        if body.get("fresh") is True:
            staging["fresh"] = True
        staging["received"][str(part)] = {"rows": rows, "sources": _clean_sources(body.get("sources")), "applied_seq": applied_seq}
        staged = _encode(staging)
        if _staged_rows(staging) > MAX_ROWS or len(staged) > MAX_STORED_BYTES:
            _abort_staging(inst, staging_path, staging, "too_large")
            raise _too_large("full upload")
        _write(staging_path, staged)
        if len(staging["received"]) < parts:
            return {"ok": True, "mode": "full", "complete": False, "received_parts": sorted(int(p) for p in staging["received"]), "instance": inst}
        new_inventory: dict[str, Any] = {kind: {} for kind in _KINDS}
        sources: list[dict[str, Any]] = []
        chunks = [staging["received"].get(str(index)) or {} for index in range(1, parts + 1)]
        for chunk in chunks:
            chunk_rows = _dict(chunk.get("rows"))
            for kind in _KINDS:
                new_inventory[kind].update({k: v for k, v in _dict(chunk_rows.get(kind)).items() if k.startswith("id:")})
            sources.extend(chunk.get("sources") or [])
        _keep_show_ids(new_inventory["tvshows"], _map(inventory.get("tvshows")) if inventory.get("generation") == generation else {})
        part_shows = dict(_dict(new_inventory.get("tvshows")))
        for delta in staging.get("deltas") or []:
            _apply_inventory(new_inventory, _dict(delta.get("rows")))
        try:
            _check_show_refs(new_inventory)
        except SyncRequestError:
            _abort_staging(inst, staging_path, staging, "missing_show")
            raise
        new_inventory.update({
            "version": VERSION,
            "viewer_id": viewer_id,
            "generation": generation,
            "complete": True,
            "upload_id": upload_id,
            "upload_seq": upload_seq,
            "uploaded_at": int(time.time()),
            "sources": sources[:500],
        })
        inventory_bytes = _encode(new_inventory)
        if _row_count(new_inventory) > MAX_ROWS or len(inventory_bytes) > MAX_STORED_BYTES:
            _abort_staging(inst, staging_path, staging, "too_large")
            raise _too_large("library")
        shows = part_shows
        for chunk in chunks:
            _apply_rows(view, changes, _dict(chunk.get("rows")), shows, upload_seq, _int(chunk.get("applied_seq")) or 0)
        try:
            view_bytes = _check_view_size(view)
        except SyncRequestError:
            _abort_staging(inst, staging_path, staging, "too_large")
            raise
        bound = binding(cfg, dev)
        if bound["viewers"].get(inst) != viewer_id or staging.get("fresh") is True:
            if _sync_running():
                raise SyncRequestError(503, "temporarily_unavailable", "A sync is running. Send this part again shortly.", True)
            _clear_kodi_baselines(inst)
            _save_binding(dev, {**bound, "viewers": {**bound["viewers"], inst: viewer_id}})
        _write(view_path, view_bytes)
        _save_changes(dev, viewer_id, changes)
        _write(inventory_path, inventory_bytes)
        _update_seq(dev, viewer_id, applied_upload_seq=max(upload_seq, _applied_upload_seq(dev, viewer_id)))
        _unlink(staging_path)
    return {"ok": True, "mode": "full", "complete": True, "instance": inst}


def _inventory_for(cfg: Mapping[str, Any] | None, instance_id: Any) -> dict[str, Any] | None:
    inst = normalize_instance_id(instance_id)
    dev = device_of(inst)
    if not device_active(cfg, dev):
        return None
    with _LOCK:
        inventory = _cached(_path(inst, "inventory"))
        info = device_info(dev)
    if inventory.get("complete") is not True:
        return None
    if viewer_instance(cfg, dev, inventory.get("viewer_id")) != inst:
        return None
    if inventory.get("generation") != _generation(inventory.get("viewer_id"), info):
        return None
    try:
        with _LOCK:
            if _cached_strict(_view_file(dev, inventory.get("viewer_id"))) is None:
                return None
    except StateUnreadable:
        return None
    return inventory


def addon_ready(cfg: Mapping[str, Any] | None, instance_id: Any) -> bool:
    inst = normalize_instance_id(instance_id)
    try:
        if _inventory_for(cfg, inst) is not None:
            return True
        if split_sync_user_id(inst) is not None:
            return False
        return any(_inventory_for(cfg, user) is not None for user in _parent_sync_users(cfg, inst))
    except Exception:
        return False


def addon_client(cfg: Mapping[str, Any] | None, instance_id: Any) -> AddonKodiClient | None:
    try:
        inst = normalize_instance_id(instance_id)
        if uses_jsonrpc(cfg, inst) or not device_active(cfg, device_of(inst)):
            return None
        return AddonKodiClient(cfg, inst)
    except Exception:
        return None


def _live_by_identity(inventory: Mapping[str, Any], aliases: dict[str, str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    shows = _map(inventory.get("tvshows"))
    for kind in ("movies", "episodes"):
        id_key = _KINDS[kind][0]
        for row in sorted(_dict(inventory.get(kind)).values(), key=lambda r: _int(r.get(id_key)) or 0):
            out.setdefault(_canonical(aliases, kind, row, shows), row)
    return out


def _view_aliases(device: Any, viewer_id: Any) -> dict[str, str]:
    view = _cached_strict(_view_file(device, viewer_id))
    if view is None:
        raise StateUnreadable(_view_file(device, viewer_id).name)
    return _aliases(view)


def _group_params(group: str, values: Mapping[str, Any]) -> dict[str, Any]:
    if group == "history":
        watched = bool(values.get("watched"))
        return {"playcount": max(1, _int(values.get("playcount")) or 1) if watched else 0, "lastplayed": values.get("lastplayed") or "" if watched else ""}
    if group == "ratings":
        return {"userrating": _int(values.get("userrating")) or 0}
    return {"resume": dict(_dict(values.get("resume")))}


def _item_ref(kind: str, meta: Mapping[str, Any]) -> dict[str, Any]:
    ref: dict[str, Any] = {"type": "movie" if kind == "movies" else "episode", "uniqueid": _dict(meta.get("uniqueid")),
                           "title": meta.get("title") or "", "file": meta.get("file") or ""}
    if kind == "movies":
        ref["year"] = meta.get("year")
        return ref
    show = _dict(meta.get("show"))
    ref.update({"showtitle": meta.get("showtitle") or show.get("title") or "", "season": meta.get("season"),
                "episode": meta.get("episode"), "show_uniqueid": _dict(show.get("uniqueid"))})
    return ref


def pending_changes(cfg: Mapping[str, Any] | None, instance_id: Any) -> list[dict[str, Any]]:
    inst = normalize_instance_id(instance_id)
    with _LOCK:
        inventory = _inventory_for(cfg, inst)
        if inventory is None:
            return []
        records = _load_changes(device_of(inst), inventory.get("viewer_id"))["records"]
        aliases = _view_aliases(device_of(inst), inventory.get("viewer_id"))
    live = _live_by_identity(inventory, aliases)
    out: list[dict[str, Any]] = []
    for rec in sorted(records.values(), key=lambda r: _int(r.get("seq")) or 0):
        if rec.get("status") != "pending":
            continue
        ident, group, kind = str(rec.get("ident")), str(rec.get("group")), str(rec.get("kind"))
        row = live.get(ident)
        if kind not in KIND_METHODS or group not in GROUPS:
            continue
        id_key = _KINDS[kind][0]
        meta = {**_dict(rec.get("meta")), **({k: row[k] for k in _META_FIELDS if k in row} if row else {})}
        params = {id_key: row[id_key]} if row else {}
        out.append({"seq": _int(rec.get("seq")), "change_id": _change_id(rec), "method": KIND_METHODS[kind],
                    "params": {**params, **_group_params(group, _dict(rec.get("values")))},
                    "item": _item_ref(kind, meta), "after": _int(rec.get("after")) or 0,
                    "queued_at": rec.get("queued_at") or ""})
    return out


def validate_change(method: Any, params: Mapping[str, Any] | None) -> tuple[str, dict[str, Any]]:
    name = str(method or "")
    id_key = WRITE_METHODS.get(name)
    if not id_key:
        raise ValueError("method_not_allowed")
    data = _dict(params)
    kid = _int(data.get(id_key))
    if kid is None or kid <= 0:
        raise ValueError("not_in_library")
    fields = {k: v for k, v in data.items() if k != id_key}
    if not fields or not set(fields) <= WRITE_FIELDS:
        raise ValueError("field_not_allowed")
    return name, {id_key: kid, **fields}


def enqueue(cfg: Mapping[str, Any] | None, instance_id: Any, method: Any, params: Mapping[str, Any] | None,
            *, generation: Any, identity: Any) -> list[int]:
    inst = normalize_instance_id(instance_id)
    name, clean = validate_change(method, params)
    id_key = WRITE_METHODS[name]
    kind = WRITE_KINDS[id_key]
    with _LOCK:
        inventory = _inventory_for(cfg, inst)
        if inventory is None:
            raise ValueError("addon_sync_waiting")
        row = _map(inventory.get(kind)).get(f"id:{clean[id_key]}")
        if inventory.get("generation") != generation or not isinstance(row, Mapping):
            raise ValueError("stale_target")
        try:
            current = _canonical(_view_aliases(device_of(inst), inventory.get("viewer_id")), kind, row, _map(inventory.get("tvshows")))
        except StateUnreadable as exc:
            raise ValueError("addon_sync_state_unreadable") from exc
        if current != identity:
            raise ValueError("stale_target")
        dev, viewer_id = device_of(inst), inventory.get("viewer_id")
        after = _applied_upload_seq(dev, viewer_id)
        meta = _meta(kind, row, _map(inventory.get("tvshows")))
        seqs: list[int] = []
        for group in GROUPS:
            fields = {k: v for k, v in clean.items() if FIELD_GROUP.get(k) == group}
            if not fields:
                continue
            if group == "history":
                watched = (_int(fields.get("playcount")) or 0) > 0
                lastplayed = fields.get("lastplayed")
                values: dict[str, Any] = {"watched": watched, "playcount": _int(fields.get("playcount")) or 0,
                                          "lastplayed": (to_utc(lastplayed) if lastplayed else "") if watched else ""}
                if not watched:
                    values["explicit"] = True
            elif group == "ratings":
                values = {"userrating": _int(fields.get("userrating")) or 0}
            else:
                values = {"resume": dict(_dict(fields.get("resume")))}
            seq = _next_change(dev, viewer_id)
            _append_change(dev, viewer_id, {"op": "set", "rec": {"seq": seq, "ident": str(identity), "group": group, "kind": kind,
                                                                 "values": values, "meta": meta, "status": "pending", "after": after, "change_id": secrets.token_hex(8),
                                                                 "queued_at": datetime.now(timezone.utc).strftime(UTC_FORMAT)}})
            seqs.append(seq)
    return seqs


def receive_ack(cfg: Mapping[str, Any] | None, device: Any, body: Mapping[str, Any], *, device_id: Any = "") -> dict[str, Any]:
    dev = normalize_instance_id(device)
    if not device_active(cfg, dev):
        raise SyncRequestError(403, "sync_disabled", "Library sync is off for this device.")
    viewer_id = _text(body.get("viewer_id"), 80)
    inst = viewer_instance(cfg, dev, viewer_id)
    if not inst:
        raise SyncRequestError(409, "viewer_unknown", "Unknown viewer.")
    results = body.get("results")
    if not isinstance(results, list):
        raise SyncRequestError(400, "invalid_request", "results must be a list.")
    wanted: dict[int, tuple[str, str]] = {}
    for row in results:
        if not isinstance(row, Mapping):
            continue
        seq = _int(row.get("seq"))
        status = _text(row.get("status"), 20).lower()
        if seq is not None and status in ("applied", "failed"):
            wanted[seq] = (status, _text(row.get("error"), 80), _text(row.get("change_id"), 64))
    applied = failed = 0
    with _LOCK:
        if not device_active(cfg, dev):
            raise SyncRequestError(403, "sync_disabled", "Library sync is off for this device.")
        _require_device(cfg, dev, device_id, bind=False)
        if viewer_instance(cfg, dev, viewer_id) != inst:
            raise SyncRequestError(409, "viewer_unknown", "Unknown viewer.")
        changes = _load_changes(dev, viewer_id)
        for key, rec in list(changes["records"].items()):
            hit = wanted.get(_int(rec.get("seq")) or 0)
            if not hit or rec.get("status") != "pending" or not hit[2] or hit[2] != _change_id(rec):
                continue
            changes["records"][key] = {**rec, "status": hit[0]}
            if hit[0] == "applied":
                applied += 1
            else:
                failed += 1
                _log("warning", "queue_change_failed", instance=inst, seq=rec.get("seq"), group=rec.get("group"), error=hit[1] or "unknown")
        _save_changes(dev, viewer_id, changes)
    return {"ok": True, "applied": applied, "failed": failed}


def fetch_queue(cfg: Mapping[str, Any] | None, device: Any, viewer_id: Any, *, device_id: Any = "",
                after: Any = None, limit: Any = None) -> dict[str, Any]:
    dev = normalize_instance_id(device)
    if not device_active(cfg, dev):
        raise SyncRequestError(403, "sync_disabled", "Library sync is off for this device.")
    start = _int(after) if after not in (None, "") else 0
    size = _int(limit) if limit not in (None, "") else QUEUE_LIMIT
    if start is None or start < 0 or size is None or size < 1:
        raise SyncRequestError(400, "invalid_request", "after and limit must be whole numbers.")
    size = min(size, QUEUE_LIMIT_MAX)
    with _LOCK:
        if not device_active(cfg, dev):
            raise SyncRequestError(403, "sync_disabled", "Library sync is off for this device.")
        _require_device(cfg, dev, device_id, bind=False)
        inst = viewer_instance(cfg, dev, viewer_id)
        if not inst:
            raise SyncRequestError(409, "viewer_unknown", "Unknown viewer.")
        pending = pending_changes(cfg, inst)
    changes = [c for c in pending if (_int(c.get("seq")) or 0) > start]
    page = changes[:size]
    more = len(changes) > size
    return {"ok": True, "version": VERSION, "changes": page, "has_more": more, "next_after": page[-1]["seq"] if more else None}


def _ghost_id(identity: str) -> int:
    return -((zlib.crc32(identity.encode("utf-8")) & 0x7FFFFFFF) + 1)


def _rule_matches(rule: Mapping[str, Any], path: str) -> bool:
    if "and" in rule:
        return all(_rule_matches(_dict(r), path) for r in rule.get("and") or [])
    if "or" in rule:
        return any(_rule_matches(_dict(r), path) for r in rule.get("or") or [])
    value = str(rule.get("value") or "").replace("\\", "/").casefold()
    operator = str(rule.get("operator") or "")
    if operator == "startswith":
        return path.startswith(value)
    if operator == "doesnotcontain":
        return value not in path
    return True


def _path_ok(file: Any, path_filter: Mapping[str, Any] | None, *, live: bool) -> bool:
    if not path_filter:
        return True
    path = str(file or "").replace("\\", "/").casefold()
    if not path:
        return not live
    return _rule_matches(path_filter, path)


class AddonKodiClient:
    def __init__(self, cfg: Mapping[str, Any] | None, instance_id: Any):
        self.config = dict(cfg or {})
        self.instance_id = normalize_instance_id(instance_id)
        self.device = device_of(self.instance_id)
        self._aliases: dict[str, str] = {}

    def _snapshot(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        with _LOCK:
            inventory = _inventory_for(self.config, self.instance_id)
            if inventory is None:
                raise KodiAuthError("No complete Kodi add-on upload yet", reason="addon_sync_waiting")
            viewer_id = inventory.get("viewer_id")
            try:
                view = _cached_strict(_view_file(self.device, viewer_id))
                records = _load_changes(self.device, viewer_id)["records"]
            except StateUnreadable as exc:
                raise KodiAuthError(f"Kodi add-on viewer state is unreadable: {exc}", reason="addon_sync_state_unreadable") from exc
            if view is None:
                raise KodiAuthError("Kodi add-on viewer state is missing", reason="addon_sync_state_unreadable")
            entries = _map(view.get("entries"))
            self._aliases = _aliases(view)
        return inventory, dict(entries), records

    def library_rows(self, feature: str = "", path_filter: Mapping[str, Any] | None = None) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]], list[Mapping[str, Any]]]:
        inventory, entries, records = self._snapshot()
        generation = str(inventory.get("generation") or "")
        collection = str(feature or "").strip().lower() == "collection"
        inventory_shows = _map(inventory.get("tvshows"))
        shows: list[Mapping[str, Any]] = [dict(r) for r in inventory_shows.values()]
        ghost_shows: dict[str, int] = {}
        out: dict[str, list[Mapping[str, Any]]] = {"movies": [], "episodes": []}
        live_idents: set[str] = set()
        for kind in ("movies", "episodes"):
            for raw in _dict(inventory.get(kind)).values():
                ident = _canonical(self._aliases, kind, raw, inventory_shows)
                live_idents.add(ident)
                if not _path_ok(raw.get("file"), path_filter, live=True):
                    continue
                row = {k: v for k, v in raw.items() if k != "unwatched"}
                row.update(_state_fields(_effective(_dict(entries.get(ident)), records, ident)))
                row[REF_KEY] = [generation, ident]
                out[kind].append(row)
        if not collection:
            rec_by_ident: dict[str, Mapping[str, Any]] = {}
            for rec in records.values():
                rec_by_ident.setdefault(str(rec.get("ident")), rec)
            for ident in set(entries) | set(rec_by_ident):
                if ident in live_idents:
                    continue
                entry = _dict(entries.get(ident))
                rec = _dict(rec_by_ident.get(ident))
                kind = str(entry.get("kind") or rec.get("kind") or "")
                state = _effective(entry, records, ident)
                if kind not in out or not _has_state(state):
                    continue
                meta = _dict(entry.get("meta")) or _dict(rec.get("meta"))
                if not _path_ok(meta.get("file"), path_filter, live=False):
                    continue
                id_key = _KINDS[kind][0]
                row = {k: v for k, v in meta.items() if k != "show"}
                row.update(_state_fields(state))
                row[id_key] = _ghost_id(ident)
                row["in_library"] = False
                if kind == "episodes":
                    show = _dict(meta.get("show"))
                    show_key = _identity("tvshows", show) if show else ""
                    if show_key and show_key not in ghost_shows:
                        ghost_shows[show_key] = _ghost_id("show:" + show_key)
                        shows.append({"tvshowid": ghost_shows[show_key], **show})
                    if show_key:
                        row["tvshowid"] = ghost_shows[show_key]
                out[kind].append(row)
        return out["movies"], out["episodes"], shows

    def get_movies(self, properties: list[str] | None = None, path_filter: Mapping[str, Any] | None = None) -> list[Mapping[str, Any]]:
        return self.library_rows("", path_filter)[0]

    def get_episodes(self, properties: list[str] | None = None, path_filter: Mapping[str, Any] | None = None) -> list[Mapping[str, Any]]:
        return self.library_rows("", path_filter)[1]

    def get_tvshows(self, properties: list[str] | None = None, path_filter: Mapping[str, Any] | None = None) -> list[Mapping[str, Any]]:
        return self.library_rows("", path_filter)[2]

    def get_sources(self, media: str = "video") -> list[Mapping[str, Any]]:
        return list(self._snapshot()[0].get("sources") or [])

    def verify(self) -> dict[str, Any]:
        if not addon_ready(self.config, self.instance_id):
            self._snapshot()
        info = device_info(self.device)
        return {"kodi_version": info.get("kodi_version") or None, "jsonrpc_version": info.get("jsonrpc_version") or ""}

    @staticmethod
    def kodi_time(value: Any) -> str:
        try:
            return to_utc(value) or datetime.now(timezone.utc).strftime(UTC_FORMAT)
        except Exception:
            return datetime.now(timezone.utc).strftime(UTC_FORMAT)

    def rpc(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        if method in WRITE_METHODS:
            raise KodiAuthError("Kodi add-on writes need a resolved library item", reason="stale_target")
        if method == "Files.GetSources":
            return {"sources": self.get_sources()}
        raise KodiAuthError(f"{method} is not available through the Kodi add-on", reason="addon_unsupported")

    def set_target(self, media_type: str, kodi_id: int, payload: Mapping[str, Any], target: Mapping[str, Any]) -> str:
        method = {"movie": "VideoLibrary.SetMovieDetails", "episode": "VideoLibrary.SetEpisodeDetails"}.get(str(media_type or ""))
        raw = target.get("_kodi_raw") if isinstance(target, Mapping) else None
        ref = raw.get(REF_KEY) if isinstance(raw, Mapping) else None
        try:
            if not method or not isinstance(ref, list) or len(ref) != 2 or not ref[0] or not ref[1]:
                raise ValueError("stale_target")
            enqueue(self.config, self.instance_id, method, {WRITE_METHODS[method]: int(kodi_id), **dict(payload)},
                    generation=ref[0], identity=ref[1])
        except ValueError as exc:
            raise KodiAuthError(f"Kodi add-on refused the change: {exc}", reason=str(exc)) from exc
        return "OK"

    def set_movie(self, movieid: int, payload: Mapping[str, Any]) -> Any:
        raise KodiAuthError("Kodi add-on writes need a resolved library item", reason="stale_target")

    def set_episode(self, episodeid: int, payload: Mapping[str, Any]) -> Any:
        raise KodiAuthError("Kodi add-on writes need a resolved library item", reason="stale_target")


def pair_ids_for(cfg: Mapping[str, Any] | None, instance_id: Any) -> list[str]:
    inst = normalize_instance_id(instance_id)
    try:
        from cw_platform.orchestrator._pairs import expand_sync_user_pairs

        pairs = list(expand_sync_user_pairs(dict(cfg or {})))
    except Exception:
        pairs = list(_dict(cfg).get("pairs") or [])
    ids: list[str] = []
    for pair in pairs:
        if not isinstance(pair, Mapping) or pair.get("enabled", True) is False:
            continue
        for side in ("source", "target"):
            if str(pair.get(side) or "").strip().upper() == "KODI" and normalize_instance_id(pair.get(f"{side}_instance")) == inst:
                pid = str(pair.get("id") or "").strip()
                if pid and pid not in ids:
                    ids.append(pid)
    return ids


def _submit_run(ids: list[str]) -> Mapping[str, Any]:
    from api.syncAPI import api_run_sync

    started: list[Any] = []
    result = api_run_sync({"source": "scheduler", "pair_scope_ids": ids, "_cw_on_start": started.append}, cast(Any, None))
    out = dict(result) if isinstance(result, Mapping) else {}
    out["_run"] = started[0] if started else None
    return out


def _run_once(inst: str, ids: list[str]) -> bool:
    for _ in range(RUN_RETRIES):
        try:
            result = _submit_run(ids)
        except Exception as exc:
            _log("warning", "pair_run_failed", instance=inst, error=type(exc).__name__)
            return False
        if result.get("ok"):
            run = result.get("_run")
            if run is not None and hasattr(run, "join"):
                run.join(RUN_WAIT_SECONDS)
            return True
        if "already running" not in str(result.get("error") or "").lower():
            _log("warning", "pair_run_refused", instance=inst, error=_text(result.get("error"), 120))
            return False
        time.sleep(RUN_RETRY_SECONDS)
    _log("warning", "pair_run_gave_up", instance=inst)
    return False


def run_with_retry(inst: str, ids: list[str]) -> bool:
    finished = False
    try:
        while True:
            result = _run_once(inst, ids)
            with _LOCK:
                follow_up = _RERUN.pop(inst, None)
                if follow_up is None:
                    _PENDING_RUNS.discard(inst)
                    finished = True
                    return result
            ids = follow_up
    finally:
        if not finished:
            with _LOCK:
                _PENDING_RUNS.discard(inst)
                _RERUN.pop(inst, None)


def schedule_pair_runs(cfg: Mapping[str, Any] | None, instance_id: Any) -> bool:
    inst = normalize_instance_id(instance_id)
    if not pair_ids_for(cfg, inst):
        return False
    with _LOCK:
        if inst in _DELAYED:
            return True
        timer = threading.Timer(DELTA_RUN_DELAY, _delayed_run, args=(cfg, inst))
        timer.daemon = True
        _DELAYED[inst] = timer
    timer.start()
    return True


def _delayed_run(cfg: Mapping[str, Any] | None, inst: str) -> None:
    with _LOCK:
        if _DELAYED.get(inst) is not threading.current_thread():
            return
        _DELAYED.pop(inst, None)
        trigger_pair_runs(cfg, inst)


def _run_pending(inst: str) -> bool:
    with _LOCK:
        return inst in _PENDING_RUNS or inst in _DELAYED


def trigger_pair_runs(cfg: Mapping[str, Any] | None, instance_id: Any) -> list[str]:
    inst = normalize_instance_id(instance_id)
    ids = pair_ids_for(cfg, inst)
    with _LOCK:
        delayed = _DELAYED.pop(inst, None)
        if delayed is not None:
            delayed.cancel()
        if not ids:
            return []
        if inst in _PENDING_RUNS:
            _RERUN[inst] = ids
            return []
        _PENDING_RUNS.add(inst)
    threading.Thread(target=run_with_retry, args=(inst, ids), name="kodi-addon-sync-run", daemon=True).start()
    return ids


__all__ = [
    "AddonKodiClient",
    "SyncRequestError",
    "addon_client",
    "addon_ready",
    "device_active",
    "enqueue",
    "fetch_queue",
    "handle_ping",
    "last_upload_seq",
    "pair_ids_for",
    "pending_changes",
    "receive_ack",
    "receive_upload",
    "run_with_retry",
    "schedule_pair_runs",
    "sync_enabled",
    "to_utc",
    "trigger_pair_runs",
    "uses_jsonrpc",
    "validate_change",
    "binding",
    "viewer_instance",
]
