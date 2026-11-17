# providers/sync/wetrakr/_collection.py
# CrossWatch - WeTrakr My Library sync with copies and media info
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterable, Mapping
from datetime import timezone
from typing import Any

from cw_platform.collection_media import (
    combine_media,
    copy_media,
    copy_regions,
    from_trakt_metadata,
    item_copies,
    normalize_format,
    same_copy_value,
    to_trakt_metadata,
)
from cw_platform.id_map import canonical_key
from providers.sync._log import log
from providers.sync._mod_common import build_op_result

from ._common import WRITE_BATCH_SIZE, WeTrakrSyncError, _activities, _read, _stamp, body_of, cache_path, identifier, int_value, media_item, pages, request, write_lock

FEATURE = "collection"
KEY_PREFIX = "crosswatch:"
MAX_COPIES_PER_ITEM = 20
MAX_PRICE = 1_000_000
LOGGED_RECORDS = 20
_CW_COPY_KEY = re.compile(r"^crosswatch:([a-f0-9]{12})$")
_DIGITAL_SOURCES = {"PLEX": "plex", "JELLYFIN": "jellyfin", "EMBY": "emby", "KODI": "kodi", "STREMIO": "stremio", "NUVIO": "nuvio"}
_SERVICES = {
    "netflix": "netflix", "amazonprimevideo": "prime_video", "primevideo": "prime_video", "amazonvideo": "prime_video", "amazon": "prime_video",
    "disneyplus": "disney_plus", "max": "max", "hbomax": "max", "appletv": "apple_tv", "appletvplus": "apple_tv", "itunes": "apple_tv",
    "hulu": "hulu", "paramountplus": "paramount_plus", "peacock": "peacock", "peacockpremium": "peacock", "youtube": "youtube",
    "googleplaymovies": "google_play", "googleplay": "google_play", "googletv": "google_play", "crunchyroll": "crunchyroll", "mubi": "mubi",
    "rakutentv": "rakuten", "rakuten": "rakuten", "microsoftstore": "microsoft", "xbox": "microsoft", "skyshowtime": "skyshowtime",
    "filmin": "filmin", "movistarplus": "movistar_plus", "movistar": "movistar_plus",
}
_PACKAGING_OUT = {"keep_case": "standard", "steelbook": "steelbook", "digipak": "other", "mediabook": "mediabook",
                  "slipcover": "slipcover", "box_set": "box_set", "other": "other"}
_PACKAGING_IN = {"standard": "keep_case", "steelbook": "steelbook", "digibook": "other", "mediabook": "mediabook",
                 "slipcover": "slipcover", "box_set": "box_set", "other": "other"}
_CONDITION_OUT = {"mint": "sealed", "like_new": "like_new", "good": "good", "fair": "fair", "poor": "poor"}
_CONDITION_IN = {value: key for key, value in _CONDITION_OUT.items()}
_CLEARABLE = ("resolution", "hdr", "audio", "audio_channels", "edition", "packaging", "condition", "region", "discs")
_PRIVATE = ("price", "store", "shelf", "notes", "lent_to")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any, limit: int) -> str:
    return " ".join(str(value or "").split())[:limit] if not isinstance(value, bool) else ""


def _iso(value: Any) -> str:
    stamp = _stamp(value) if isinstance(value, str) else None
    return stamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if stamp else ""


def _progress(adapter: Any, count: int) -> None:
    progress = getattr(adapter, "_read_progress", None)
    if progress is not None:
        progress.tick(count)


def _owned(block: Mapping[str, Any]) -> bool:
    return str(block.get("ownership") or "owned") == "owned"


def _copy_id(copy: Mapping[str, Any]) -> str:
    match = _CW_COPY_KEY.match(str(copy.get("copy_key") or ""))
    if match:
        return match.group(1)
    return hashlib.sha1(f"wetrakr:{copy.get('id')}".encode()).hexdigest()[:12]


def cw_copy(copy: Mapping[str, Any]) -> dict[str, Any] | None:
    if int_value(copy.get("id")) <= 0:
        return None
    meta = _mapping(copy.get("metadata"))
    own = bool(_CW_COPY_KEY.match(str(copy.get("copy_key") or "")))
    out: dict[str, Any] = {"id": _copy_id(copy), "format": normalize_format(meta.get("media_type")) or "digital",
                           **copy_media({**meta, "is_3d": meta.get("3d")}), "source": "crosswatch" if own else "wetrakr", "ref": str(copy["id"])}
    edition = _text(copy.get("edition"), 80)
    if edition:
        out["edition"] = edition
    packaging = _PACKAGING_IN.get(str(copy.get("packaging") or ""))
    if packaging:
        out["packaging"] = packaging
    condition = _CONDITION_IN.get(str(copy.get("condition") or ""))
    if condition:
        out["condition"] = condition
    regions = ",".join(copy_regions(copy.get("region")))
    if regions and len(regions) <= 12:
        out["region"] = regions
    discs = int_value(copy.get("discs"))
    if 1 <= discs <= 99:
        out["discs"] = discs
    purchase = _mapping(copy.get("purchase"))
    try:
        price = round(float(purchase["price"]), 2) if purchase.get("price") is not None else None
    except (TypeError, ValueError):
        price = None
    if price is not None and 0 <= price <= MAX_PRICE:
        out["price"] = price
    for field, value, limit in (("store", purchase.get("store"), 80), ("shelf", copy.get("location"), 60),
                                ("notes", copy.get("notes"), 500), ("lent_to", _mapping(copy.get("loan")).get("to"), 80)):
        text = _text(value, limit)
        if text:
            out[field] = text
    collected = _iso(copy.get("collected_at"))
    if collected and not copy.get("collected_at_unknown"):
        out["purchased"] = collected[:10]
    created = _iso(copy.get("created_at"))
    if created:
        out["added_at"] = created[:19] + "Z"
    return out


def _dated(item: dict[str, Any], block: Mapping[str, Any]) -> None:
    collected = _iso(block.get("collected_at"))
    if collected and not block.get("collected_at_unknown"):
        item["collected_at"] = collected


def _movie(row: Mapping[str, Any]) -> dict[str, Any]:
    item = media_item(row, "movie")
    block = _mapping(row.get("library"))
    _dated(item, block)
    raw = block.get("copies") or []
    if not isinstance(raw, list) or not all(isinstance(copy, Mapping) for copy in raw):
        raise WeTrakrSyncError("invalid_library_copy")
    owned = [copy for copy in raw if _owned(copy)]
    if any(int_value(copy.get("id")) <= 0 for copy in owned):
        raise WeTrakrSyncError("invalid_library_copy")
    fmt, media = from_trakt_metadata(block.get("metadata"))
    if not fmt and not media and owned:
        fmt, media = combine_media((_mapping(c.get("metadata")).get("media_type"), copy_media({**_mapping(c.get("metadata")), "is_3d": _mapping(c.get("metadata")).get("3d")})) for c in owned)
    if fmt:
        item["format"] = fmt
    if media:
        item["media_info"] = media
    copies = [row for row in (cw_copy(copy) for copy in owned) if row]
    if copies:
        item["copies"] = copies
    refs = [{"id": int_value(copy.get("id")), "key": str(copy.get("copy_key") or "")} for copy in owned if int_value(copy.get("id")) > 0]
    if refs:
        item["_wetrakr_copy_refs"] = refs
    return item


def _invalid(kind: str, exc: WeTrakrSyncError, count: int) -> int:
    if count < LOGGED_RECORDS:
        log("WETRAKR", FEATURE, "warn", "library_record_invalid", kind=kind, reason=exc.reason)
    return count + 1


def _scope_of(key: str) -> str:
    return ":".join(key.split(":")[:3])


def _episodes(rows: Iterable[Mapping[str, Any]]) -> tuple[dict[str, dict[str, Any]], int]:
    groups: dict[str, dict[str, Any]] = {}
    invalid = 0
    for row in rows:
        block = _mapping(row.get("library"))
        if block.get("status") == "removed" or not _owned(block):
            continue
        try:
            item = media_item(row, "episode")
        except WeTrakrSyncError as exc:
            invalid = _invalid("episode", exc, invalid)
            continue
        key = canonical_key(item)
        group = groups.setdefault(key, {"item": item, "media": [], "dates": [], "keys": set()})
        meta = _mapping(block.get("metadata"))
        group["media"].append((meta.get("media_type"), copy_media({**meta, "is_3d": meta.get("3d")})))
        if block.get("collected_at") and not block.get("collected_at_unknown"):
            group["dates"].append(_iso(block["collected_at"]))
        copy_key = str(block.get("copy_key") or "")
        if copy_key.startswith(KEY_PREFIX):
            group["keys"].add(copy_key)
    out: dict[str, dict[str, Any]] = {}
    for key, group in groups.items():
        item = group["item"]
        scopes = [_scope_of(k) for k in group["keys"] if k.startswith(f"{KEY_PREFIX}show:")]
        fmt, media = combine_media(group["media"]) if len(scopes) == len(set(scopes)) else ("", {})
        if fmt:
            item["format"] = fmt
        if media:
            item["media_info"] = media
        dates = sorted(d for d in group["dates"] if d)
        if dates:
            item["collected_at"] = dates[0]
        if group["keys"]:
            item["_wetrakr_copy_keys"] = sorted(group["keys"])
        out[key] = item
    return out, invalid


def _marker(activities: Mapping[str, Any]) -> dict[str, Any]:
    library = activities.get("library")
    return {"library": dict(library) if isinstance(library, Mapping) else None,
            "movies": _mapping(activities.get("movies")).get("collected_at"),
            "episodes": _mapping(activities.get("episodes")).get("collected_at")}


def _stamped(marker: Mapping[str, Any]) -> bool:
    values = list(_mapping(marker.get("library")).values()) + [marker.get("movies"), marker.get("episodes")]
    return any(_stamp(value) is not None for value in values)


def _library_rows(adapter: Any, *, force: bool) -> dict[str, list[Mapping[str, Any]]]:
    path = cache_path(adapter, FEATURE)
    cached = _read(path).get("sections", {})
    marker = _marker(_activities(adapter))
    rows = cached.get("rows")
    if (not force and _stamped(marker) and cached.get("marker") == marker and isinstance(rows, Mapping)
            and all(isinstance(rows.get(kind), list) for kind in ("movies", "episodes"))):
        log("WETRAKR", FEATURE, "debug", "snapshot_cached")
        return {kind: list(rows[kind]) for kind in ("movies", "episodes")}
    movies = pages(adapter, "/sync/library/movies", extended="metadata", on_page=lambda count: _progress(adapter, count))
    episodes = pages(adapter, "/sync/library/episodes", on_page=lambda count: _progress(adapter, len(movies) + count))
    after = _marker(_activities(adapter, fresh=True))
    if _stamped(after) and after == marker:
        adapter._pending_snapshot = (path, {"marker": marker, "rows": {"movies": movies, "episodes": episodes}})
    return {"movies": movies, "episodes": episodes}


def build_index(adapter: Any, *, force: bool = False) -> dict[str, dict[str, Any]]:
    with write_lock(adapter):
        rows = _library_rows(adapter, force=force)
    out: dict[str, dict[str, Any]] = {}
    invalid = 0
    for row in rows["movies"]:
        if _mapping(row.get("library")).get("status") == "removed":
            continue
        try:
            item = _movie(row)
        except WeTrakrSyncError as exc:
            invalid = _invalid("movie", exc, invalid)
            continue
        out[canonical_key(item)] = item
    episodes, bad = _episodes(rows["episodes"])
    invalid += bad
    if invalid:
        log("WETRAKR", FEATURE, "error", "library_records_invalid", count=invalid)
        raise WeTrakrSyncError("invalid_library_records")
    out.update(episodes)
    log("WETRAKR", FEATURE, "info", "index_done", count=len(out))
    return out


def _copy_details(adapter: Any) -> bool:
    block = _mapping(_mapping(getattr(adapter, "config", {})).get("wetrakr"))
    return _mapping(block.get("collection")).get("copy_details") is True


def _pair_other() -> tuple[str, str]:
    src = str(os.getenv("CW_PAIR_SRC") or "").upper()
    dst = str(os.getenv("CW_PAIR_DST") or "").upper()
    if dst == "WETRAKR":
        return src, str(os.getenv("CW_PAIR_SRC_INSTANCE") or "")
    if src == "WETRAKR":
        return dst, str(os.getenv("CW_PAIR_DST_INSTANCE") or "")
    return "", ""


def _pair_source() -> str:
    return _DIGITAL_SOURCES.get(_pair_other()[0], "")


def _pair_scope() -> str:
    name, instance = _pair_other()
    name = re.sub(r"[^a-z0-9_]", "", name.lower()) or "cw"
    instance = re.sub(r"[^a-z0-9_.-]", "", instance.lower())
    return name if instance in ("", "default") else f"{name}.{instance}"


def _currency() -> str:
    try:
        from services.shelf_store import ADMIN_OWNER, currency
        return currency(ADMIN_OWNER)
    except Exception:
        return ""


def _service(name: Any) -> dict[str, str]:
    text = _text(name, 60)
    if not text:
        return {}
    compact = re.sub(r"[^a-z0-9]", "", text.lower().replace("+", "plus"))
    value = _SERVICES.get(compact)
    return {"digital_source": value} if value else {"digital_source": "other", "digital_source_detail": text}


def _metadata(item: Mapping[str, Any]) -> dict[str, Any]:
    out = to_trakt_metadata(item)
    if normalize_format(item.get("format")) == "4k_bluray":
        out["media_type"] = "uhd_bluray"
    if out.pop("3d", None):
        out["3d"] = True
    return out


def _token(ids: Any) -> str:
    ids = _mapping(ids)
    for source in ("tmdb", "imdb", "tvdb", "wetrakr"):
        value = str(ids.get(source) or "").strip()
        if value:
            return f"{source}:{value}"
    raise WeTrakrSyncError("missing_supported_id")


def _changed(before: Mapping[str, Any] | None, field: str, value: Any) -> bool:
    return before is None or not same_copy_value(field, before.get(field), value)


def copy_entry(copy: Mapping[str, Any], *, details: bool, before: Mapping[str, Any] | None = None, currency: str = "") -> dict[str, Any]:
    entry: dict[str, Any] = {}
    ref = int_value(before.get("ref")) if before is not None else -1
    if ref > 0:
        entry["copy_id"] = ref
    else:
        entry["copy_key"] = f"{KEY_PREFIX}{copy['id']}"
    media = copy_media(copy)
    meta = _metadata({"format": copy.get("format"), "media_info": media})
    values: dict[str, tuple[str, Any]] = {
        "media_type": ("format", meta.get("media_type")),
        "resolution": ("resolution", meta.get("resolution")),
        "hdr": ("hdr", meta.get("hdr")),
        "audio": ("audio", meta.get("audio")),
        "audio_channels": ("audio_channels", meta.get("audio_channels")),
        "edition": ("edition", _text(copy.get("edition"), 120) or None),
        "packaging": ("packaging", _PACKAGING_OUT.get(str(copy.get("packaging") or ""))),
        "condition": ("condition", _CONDITION_OUT.get(str(copy.get("condition") or ""))),
        "region": ("region", copy_regions(copy.get("region")) or None),
        "discs": ("discs", int_value(copy.get("discs")) if 1 <= int_value(copy.get("discs")) <= 999 else None),
    }
    for field, (cw_field, value) in values.items():
        if not _changed(before, cw_field, copy.get(cw_field)):
            continue
        if value is None and (before is None or field not in _CLEARABLE):
            continue
        entry[field] = value
    is_3d = media.get("is_3d") is True
    if before is None or is_3d != (copy_media(before).get("is_3d") is True):
        entry["3d"] = is_3d
    if before is None and copy.get("format") == "digital" and copy.get("service"):
        entry.update(_service(_mapping(copy.get("service")).get("name")))
    purchased = str(copy.get("purchased") or "")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", purchased) and _changed(before, "purchased", purchased):
        entry["collected_at"] = f"{purchased}T00:00:00.000Z"
    if details:
        raw_price = copy.get("price")
        price = (round(float(raw_price), 2) if isinstance(raw_price, (int, float)) and not isinstance(raw_price, bool)
                 and 0 <= raw_price <= MAX_PRICE else None)
        if _changed(before, "price", price) or _changed(before, "store", copy.get("store")):
            purchase = {**({"price": price} if price is not None else {}),
                        **({"currency": currency} if price is not None and currency else {}),
                        **({"store": copy["store"]} if copy.get("store") else {})}
            if purchase or before is not None:
                entry["purchase"] = purchase or None
        for field, target in (("shelf", "location"), ("notes", "notes")):
            if _changed(before, field, copy.get(field)) and (copy.get(field) or before is not None):
                entry[target] = copy.get(field) or None
        if _changed(before, "lent_to", copy.get("lent_to")) and (copy.get("lent_to") or before is not None):
            entry["loan"] = {"to": copy["lent_to"]} if copy.get("lent_to") else None
    return entry


def _stale_refs(item: Mapping[str, Any], copies: list[Mapping[str, Any]]) -> list[int]:
    ids = {str(c.get("id") or "") for c in copies}
    mirror_wetrakr = any(c.get("source") == "wetrakr" for c in copies)
    stale: list[int] = []
    for ref in item.get("_wetrakr_copy_refs") or []:
        if not isinstance(ref, Mapping) or int_value(ref.get("id")) <= 0:
            continue
        own = bool(_CW_COPY_KEY.match(str(ref.get("key") or "")))
        if _copy_id({"id": ref["id"], "copy_key": ref.get("key")}) in ids:
            continue
        if own or mirror_wetrakr:
            stale.append(int_value(ref["id"]))
    return stale


def _movie_row(item: Mapping[str, Any], *, details: bool, source: str, scope: str, currency: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    row: dict[str, Any] = dict(identifier(item.get("ids")))
    update = bool(item.get("_cw_media_update"))
    collected = _iso(item.get("collected_at"))
    if collected and not update:
        row["collected_at"] = collected
    copies = item_copies(item)
    if copies and (not update or "_cw_copies_before" in item):
        before = {str(c.get("id")): c for c in item_copies({"copies": item.get("_cw_copies_before")})} if update else {}
        entries = []
        for copy in copies[:MAX_COPIES_PER_ITEM]:
            prior = before.get(str(copy.get("id"))) if update else None
            entry = copy_entry(copy, details=details, before=prior, currency=currency)
            if prior is None or len(entry) > 1:
                entries.append(entry)
        stale = [{**identifier(item.get("ids")), "copy_id": copy_id} for copy_id in _stale_refs(item, copies)] if update else []
        if not entries:
            return None, stale
        row["copies"] = entries
        return row, stale
    row.update(_metadata(item))
    fmt = normalize_format(item.get("format"))
    if source and fmt in ("", "digital"):
        row["digital_source"] = source
    prefix = f"{KEY_PREFIX}movie:{scope}:"
    known = [str(ref.get("key")) for ref in item.get("_wetrakr_copy_refs") or []
             if isinstance(ref, Mapping) and str(ref.get("key") or "").startswith(prefix)] if update else []
    row["copy_key"] = known[0] if known else f"{prefix}{_token(item.get('ids'))}"
    return row, []


def _episode_parts(item: Mapping[str, Any], *, source: str, scope: str) -> tuple[str, dict[str, Any], dict[str, Any], int, dict[str, Any]]:
    season, episode = int_value(item.get("season")), int_value(item.get("episode"))
    if season < 0 or episode < 1:
        raise WeTrakrSyncError("invalid_episode_coordinates")
    show = dict(identifier(item.get("show_ids")))
    meta = _metadata(item)
    fmt = normalize_format(item.get("format"))
    if source and fmt in ("", "digital"):
        meta["digital_source"] = source
    signature = hashlib.sha1(json.dumps(meta, sort_keys=True).encode()).hexdigest()[:10]
    copy_key = f"{KEY_PREFIX}show:{scope}:{_token(item.get('show_ids'))}:{signature}"
    ep: dict[str, Any] = {"number": episode}
    collected = _iso(item.get("collected_at"))
    if collected and not item.get("_cw_media_update"):
        ep["collected_at"] = collected
    return copy_key, show, meta, season, ep


def _shows(parts: Iterable[tuple[str, str, dict[str, Any], dict[str, Any], int, dict[str, Any]]]) -> tuple[list[dict[str, Any]], dict[int, list[str]]]:
    shows: dict[str, dict[str, Any]] = {}
    owners: dict[str, list[str]] = {}
    for key, copy_key, show, meta, season, ep in parts:
        group = shows.setdefault(copy_key, {**show, **meta, "copy_key": copy_key, "seasons": {}})
        group["seasons"].setdefault(season, []).append(ep)
        owners.setdefault(copy_key, []).append(key)
    rows: list[dict[str, Any]] = []
    index: dict[int, list[str]] = {}
    for copy_key, group in shows.items():
        seasons = group.pop("seasons")
        group["seasons"] = [{"number": number, "episodes": eps} for number, eps in sorted(seasons.items())]
        index[len(rows)] = owners[copy_key]
        rows.append(group)
    return rows, index


def _tokens(row: Any) -> set[str]:
    row = _mapping(row)
    out = {f"wetrakr:{row['id']}"} if int_value(row.get("id")) > 0 else set()
    out.update(f"{k}:{str(v).strip().lower()}" for k, v in _mapping(row.get("ids")).items() if str(v or "").strip())
    show = _mapping(row.get("show"))
    if show:
        out.update(_tokens(show))
    return out


_ADD_REJECTIONS = (("not_found", "not_found"), ("notFound", "not_found"), ("errored", "write_rejected"))
_REMOVE_REJECTIONS = (("errored", "remove_rejected"),)


def _rejected(body: Mapping[str, Any], sent: Mapping[str, list[tuple[dict[str, Any], list[str]]]],
              buckets: tuple[tuple[str, str], ...] = _ADD_REJECTIONS) -> tuple[dict[str, str], bool]:
    out: dict[str, str] = {}
    for bucket, reason in buckets:
        groups = body.get(bucket)
        if not groups:
            continue
        if not isinstance(groups, Mapping):
            return out, False
        for group, entries in groups.items():
            if not entries:
                continue
            if not isinstance(entries, list):
                return out, False
            target = "movies" if group == "movies" else "shows"
            for entry in entries:
                if not isinstance(entry, Mapping):
                    return out, False
                tokens = _tokens(entry)
                hits = [keys for row, keys in sent.get(target, []) if tokens & _tokens(row)]
                if not hits:
                    return out, False
                detail = str(entry.get("error") or reason)
                if entry.get("field"):
                    detail = f"{detail}:{entry['field']}"
                for keys in hits:
                    for key in keys:
                        out.setdefault(key, detail)
    return out, True


def _reason(exc: WeTrakrSyncError) -> str:
    if exc.status_code == 420:
        return "plan_limit_reached"
    if exc.status_code == 413:
        return "too_many_items"
    return exc.reason


def _post(adapter: Any, path: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
    body = body_of(request(adapter, "POST", path, json=payload))
    if not isinstance(body, Mapping):
        raise WeTrakrSyncError("invalid_response")
    return body


def _remove_stale(adapter: Any, stale: Mapping[str, list[dict[str, Any]]], keys: Iterable[str]) -> dict[str, str]:
    payload: dict[str, list[dict[str, Any]]] = {}
    sent: dict[str, list[tuple[dict[str, Any], list[str]]]] = {}
    for key in keys:
        for row in stale.get(key) or []:
            group = "shows" if "seasons" in row else "movies"
            payload.setdefault(group, []).append(row)
            sent.setdefault(group, []).append((row, [key]))
    owners = {key for rows in sent.values() for _row, row_keys in rows for key in row_keys}
    if not payload:
        return {}
    try:
        body = _post(adapter, "/sync/library/remove", payload)
    except WeTrakrSyncError as exc:
        log("WETRAKR", FEATURE, "warn", "stale_copies_remove_failed", reason=exc.reason, status=exc.status_code)
        return {key: "stale_copy_remove_failed" for key in owners}
    rejected, mapped = _rejected(body, sent, _REMOVE_REJECTIONS) if isinstance(body.get("deleted"), Mapping) else ({}, False)
    if not mapped:
        log("WETRAKR", FEATURE, "warn", "stale_copies_remove_not_verified", count=len(owners))
        return {key: "stale_copy_remove_not_verified" for key in owners}
    log("WETRAKR", FEATURE, "info", "stale_copies_removed", deleted=body.get("deleted"), rejected=len(rejected))
    return rejected


def _old_episode_rows(item: Mapping[str, Any], copy_key: str, season: int, number: int) -> list[dict[str, Any]]:
    if not item.get("_cw_media_update"):
        return []
    rows = []
    for old in item.get("_wetrakr_copy_keys") or []:
        if isinstance(old, str) and _scope_of(old) == _scope_of(copy_key) and old != copy_key:
            rows.append({**identifier(item.get("show_ids")), "copy_key": old, "seasons": [{"number": season, "episodes": [{"number": number}]}]})
    return rows


def _finish(adapter: Any, stale: Mapping[str, list[dict[str, Any]]], keys: list[str], rejected: Mapping[str, str],
            confirmed: list[str], unresolved: list[dict[str, str]]) -> None:
    failed = {**_remove_stale(adapter, stale, [key for key in keys if key not in rejected]), **rejected}
    for key in keys:
        if key in failed:
            unresolved.append({"key": key, "reason": failed[key]})
        else:
            confirmed.append(key)


def _removal_rows(raw: Mapping[str, Any], kind: str) -> list[dict[str, Any]]:
    name = _pair_other()[0]
    two_way = str(os.getenv("CW_PAIR_MODE") or "").lower() == "two-way"
    scope = _pair_scope() if name and not two_way else ""
    if kind == "movie":
        ids = dict(identifier(raw.get("ids")))
        if not scope:
            return [ids]
        rows = [{**ids, "copy_key_prefix": f"{KEY_PREFIX}movie:{scope}:"}]
        if name == "CROSSWATCH":
            rows += [{**ids, "copy_id": int_value(copy.get("ref"))} for copy in item_copies(raw)
                     if copy.get("source") == "crosswatch" and int_value(copy.get("ref")) > 0]
        return rows
    season, episode = int_value(raw.get("season")), int_value(raw.get("episode"))
    if season < 0 or episode < 1:
        raise WeTrakrSyncError("invalid_episode_coordinates")
    row = {**identifier(raw.get("show_ids")), "seasons": [{"number": season, "episodes": [{"number": episode}]}]}
    return [{**row, "copy_key_prefix": f"{KEY_PREFIX}show:{scope}:"} if scope else row]


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    details = _copy_details(adapter)
    source = _pair_source()
    scope = _pair_scope()
    currency = _currency() if details else ""
    prepared: list[tuple[str, str, Any]] = []
    unresolved: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in items or []:
        if not isinstance(raw, Mapping):
            continue
        key = canonical_key(raw)
        if not key or key in seen:
            continue
        seen.add(key)
        try:
            kind = str(raw.get("type") or "")
            if kind == "movie":
                prepared.append((key, "movie", _movie_row(raw, details=details, source=source, scope=scope, currency=currency)))
            elif kind == "episode":
                copy_key, show, meta, season, ep = _episode_parts(raw, source=source, scope=scope)
                prepared.append((key, "episode", (copy_key, show, meta, season, ep, _old_episode_rows(raw, copy_key, season, ep["number"]))))
            else:
                raise WeTrakrSyncError("unsupported_media_type")
        except WeTrakrSyncError as exc:
            unresolved.append({"key": key, "reason": exc.reason})
            log("WETRAKR", FEATURE, "warn", "item_unresolved_before_write", key=key, reason=exc.reason)
    if dry_run or not prepared:
        return build_op_result(ok=not unresolved, count=len(prepared), unresolved=unresolved,
                               unresolved_keys=[r["key"] for r in unresolved], dry_run=dry_run)
    confirmed: list[str] = []
    with write_lock(adapter):
        for start in range(0, len(prepared), WRITE_BATCH_SIZE):
            batch = prepared[start:start + WRITE_BATCH_SIZE]
            movies = [(value[0], [key]) for key, kind, value in batch if kind == "movie" and value[0] is not None]
            stale = {key: value[1] if kind == "movie" else value[5] for key, kind, value in batch}
            show_rows, owners = _shows((key, *value[:5]) for key, kind, value in batch if kind == "episode")
            payload: dict[str, Any] = {}
            if movies:
                payload["movies"] = [row for row, _keys in movies]
            if show_rows:
                payload["shows"] = show_rows
            sent = {"movies": movies, "shows": [(row, owners[i]) for i, row in enumerate(show_rows)]}
            keys = [key for key, _kind, _value in batch]
            if not payload:
                _finish(adapter, stale, keys, {}, confirmed, unresolved)
                continue
            try:
                body = _post(adapter, "/sync/library", payload)
            except WeTrakrSyncError as exc:
                reason = _reason(exc)
                log("WETRAKR", FEATURE, "warn", "write_failed", reason=reason, status=exc.status_code, count=len(keys))
                unresolved.extend({"key": key, "reason": reason} for key, _kind, _value in prepared[start:])
                break
            if not isinstance(body.get("added"), Mapping):
                unresolved.extend({"key": key, "reason": "invalid_response"} for key in keys)
                continue
            rejected, mapped = _rejected(body, sent)
            if not mapped:
                unresolved.extend({"key": key, "reason": "write_not_verified"} for key in keys)
                log("WETRAKR", FEATURE, "warn", "write_unmapped_rejections", count=len(keys))
                continue
            _finish(adapter, stale, keys, rejected, confirmed, unresolved)
        log("WETRAKR", FEATURE, "info", "write_done", op="add", applied=len(confirmed), unresolved=len(unresolved))
    return build_op_result(ok=not unresolved, count=len(confirmed), confirmed_keys=confirmed,
                           unresolved=unresolved, unresolved_keys=[r["key"] for r in unresolved])


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    prepared: list[tuple[str, str, list[dict[str, Any]]]] = []
    unresolved: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in items or []:
        if not isinstance(raw, Mapping):
            continue
        key = canonical_key(raw)
        if not key or key in seen:
            continue
        seen.add(key)
        try:
            kind = str(raw.get("type") or "")
            if kind not in ("movie", "episode"):
                raise WeTrakrSyncError("unsupported_media_type")
            prepared.append((key, "movies" if kind == "movie" else "shows", _removal_rows(raw, kind)))
        except WeTrakrSyncError as exc:
            unresolved.append({"key": key, "reason": exc.reason})
    if dry_run or not prepared:
        return build_op_result(ok=not unresolved, count=len(prepared), unresolved=unresolved,
                               unresolved_keys=[r["key"] for r in unresolved], dry_run=dry_run)
    confirmed: list[str] = []
    with write_lock(adapter):
        for start in range(0, len(prepared), WRITE_BATCH_SIZE):
            batch = prepared[start:start + WRITE_BATCH_SIZE]
            payload: dict[str, list[dict[str, Any]]] = {}
            for _key, group, rows in batch:
                payload.setdefault(group, []).extend(rows)
            try:
                body = _post(adapter, "/sync/library/remove", payload)
            except WeTrakrSyncError as exc:
                reason = _reason(exc)
                unresolved.extend({"key": key, "reason": reason} for key, _group, _row in prepared[start:])
                log("WETRAKR", FEATURE, "warn", "remove_failed", reason=reason, status=exc.status_code)
                break
            if not isinstance(body.get("deleted"), Mapping):
                unresolved.extend({"key": key, "reason": "invalid_response"} for key, _group, _row in batch)
                continue
            sent = {group: [(row, [key]) for key, kind, rows in batch if kind == group for row in rows] for group in ("movies", "shows")}
            rejected, mapped = _rejected(body, sent, _REMOVE_REJECTIONS)
            if not mapped:
                unresolved.extend({"key": key, "reason": "remove_not_verified"} for key, _group, _row in batch)
                log("WETRAKR", FEATURE, "warn", "remove_unmapped_rejections", count=len(batch))
                continue
            for key, _group, _row in batch:
                if key in rejected:
                    unresolved.append({"key": key, "reason": rejected[key]})
                else:
                    confirmed.append(key)
        log("WETRAKR", FEATURE, "info", "write_done", op="remove", applied=len(confirmed), unresolved=len(unresolved))
    return build_op_result(ok=not unresolved, count=len(confirmed), confirmed_keys=confirmed,
                           unresolved=unresolved, unresolved_keys=[r["key"] for r in unresolved])
