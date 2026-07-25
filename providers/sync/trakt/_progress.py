# /providers/sync/trakt/_progress.py
# CrossWatch Trakt progress functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
import os
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.sync._log import log as cw_log
from providers.sync._progress_policy import decide_progress_write, progress_materially_equal, select_progress_record

from ._common import _fix_imdb


_PROVIDER = "TRAKT"
_FEATURE = "progress"


def _dbg(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except Exception:
        return None


def _int(value: Any) -> int | None:
    try:
        if value is None or value == "" or isinstance(value, bool):
            return None
        return int(float(value))
    except Exception:
        return None


def _positive_int(value: Any) -> int | None:
    number = _int(value)
    return number if number is not None and number > 0 else None


def _ids(source: Any, allowed: Iterable[str]) -> dict[str, Any]:
    raw = dict(source or {}) if isinstance(source, Mapping) else {}
    _fix_imdb(raw)
    out: dict[str, Any] = {}
    for key in allowed:
        value = raw.get(key)
        if value in (None, ""):
            continue
        if key in {"trakt", "tmdb", "tvdb"}:
            number = _positive_int(value)
            if number is not None:
                out[key] = number
            continue
        text = str(value).strip()
        if text:
            out[key] = text
    return out


def _duration_ms(*sources: Mapping[str, Any]) -> int | None:
    for source in sources:
        runtime = _positive_int(source.get("runtime"))
        if runtime is not None:
            return runtime * 60_000
        for key in ("duration_ms", "durationMs", "runtime_ms", "runtimeMs"):
            value = _positive_int(source.get(key))
            if value is not None:
                return value
    return None


def _progress_ms(progress_percent: Any, duration_ms: int | None) -> int | None:
    progress = _float(progress_percent)
    if progress is None or progress <= 0 or duration_ms is None or duration_ms <= 0:
        return None
    return int(round(float(duration_ms) * max(0.0, min(progress, 100.0)) / 100.0))


def _row_payload(row: Mapping[str, Any]) -> tuple[str, Mapping[str, Any], Mapping[str, Any]]:
    row_type = str(row.get("type") or "").strip().lower()
    movie = row.get("movie") if isinstance(row.get("movie"), Mapping) else None
    episode_raw = row.get("episode")
    episode = episode_raw if isinstance(episode_raw, Mapping) else None
    show_raw = row.get("show")
    show: Mapping[str, Any] = show_raw if isinstance(show_raw, Mapping) else {}
    if movie is not None or row_type == "movie":
        return "movie", movie or row, {}
    return "episode", episode or row, show


def _item_from_row(row: Mapping[str, Any]) -> tuple[str | None, dict[str, Any] | None, str | None]:
    playback_id = _positive_int(row.get("id"))
    progress = _float(row.get("progress"))
    if playback_id is None or progress is None or progress <= 0:
        return None, None, "trakt_progress_invalid"

    media_type, payload, show = _row_payload(row)
    duration = _duration_ms(payload, show, row)
    position = _progress_ms(progress, duration)
    if duration is None or position is None:
        return None, None, "trakt_duration_missing"

    progress_at = str(row.get("paused_at") or "").strip()
    if media_type == "movie":
        item = id_minimal(
            {
                "type": "movie",
                "title": payload.get("title"),
                "year": payload.get("year"),
                "ids": _ids(payload.get("ids"), ("trakt", "slug", "imdb", "tmdb")),
            }
        )
    else:
        item = id_minimal(
            {
                "type": "episode",
                "title": payload.get("title") or show.get("title"),
                "series_title": show.get("title"),
                "year": show.get("year"),
                "season": payload.get("season"),
                "episode": payload.get("number") or payload.get("episode"),
                "ids": _ids(payload.get("ids"), ("trakt", "tvdb", "imdb", "tmdb")),
                "show_ids": _ids(show.get("ids"), ("trakt", "slug", "imdb", "tmdb", "tvdb")),
            }
        )

    key = canonical_key(item)
    if not key:
        return None, None, "trakt_id_missing"

    item["progress_ms"] = int(position)
    item["duration_ms"] = int(duration)
    item["progress_percent"] = round(progress, 3)
    if progress_at:
        item["progress_at"] = progress_at
    item["_trakt_playback_id"] = playback_id
    return key, item, None


def _progress_page(adapter: Any, media_type: str, page: int, limit: int) -> list[Mapping[str, Any]] | None:
    response = adapter.client.get(
        f"{adapter.client.BASE}/sync/playback/{media_type}",
        params={"extended": "full", "page": int(page), "limit": int(limit)},
    )
    if not (200 <= int(getattr(response, "status_code", 0) or 0) < 300):
        _warn("index_http_failed", media_type=media_type, page=page, status=getattr(response, "status_code", None))
        return None
    data = response.json() if (getattr(response, "text", "") or "").strip() else []
    return data if isinstance(data, list) else []


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    limit = 100
    try:
        limit = max(1, min(100, int(getattr(adapter.cfg, "progress_per_page", 100) or 100)))
    except Exception:
        limit = 100
    try:
        max_pages = max(1, int(getattr(adapter.cfg, "progress_max_pages", 100) or 100))
    except Exception:
        max_pages = 100

    out: dict[str, dict[str, Any]] = {}
    skipped: dict[str, int] = {}
    rows_seen = 0
    for media_type in ("movies", "episodes"):
        page = 1
        while page <= max_pages:
            rows = _progress_page(adapter, media_type, page, limit)
            if rows is None:
                break
            rows_seen += len(rows)
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                key, item, reason = _item_from_row(row)
                if not key or item is None:
                    if reason:
                        skipped[reason] = skipped.get(reason, 0) + 1
                    continue
                selected, action = select_progress_record(out.get(key), item)
                out[key] = selected
                _dbg("item", canonical_key=key, media_type=item.get("type"), progress_ms=item.get("progress_ms"), duration_ms=item.get("duration_ms"), action=action)
            if len(rows) < limit:
                break
            page += 1
    _info("index_done", count=len(out), rows=rows_seen, skipped=sum(skipped.values()), skipped_reasons=skipped)
    return out


def _progress_values(item: Mapping[str, Any]) -> tuple[int | None, int | None, float | None]:
    progress = None
    for key in ("progress_ms", "progressMs", "viewOffset", "progress"):
        progress = _positive_int(item.get(key))
        if progress is not None:
            break
    duration = None
    for key in ("duration_ms", "durationMs", "duration"):
        duration = _positive_int(item.get(key))
        if duration is not None:
            break
    if progress is None or duration is None or duration <= 0:
        percent = None
        for key in ("progress_percent", "progressPercent", "percent", "position_percent", "resume_percent"):
            percent = _float(item.get(key))
            if percent is not None:
                break
        if percent is not None:
            return progress, duration, round(max(0.0, min(percent, 100.0)), 3)
        return progress, duration, None
    percent = round((float(progress) / float(duration)) * 100.0, 3)
    return progress, duration, percent


def _movie_body(item: Mapping[str, Any], progress_percent: float) -> tuple[dict[str, Any] | None, str | None]:
    title = str(item.get("title") or "").strip()
    if not title:
        return None, "trakt_title_missing"
    ids = _ids(item.get("ids"), ("trakt", "slug", "imdb", "tmdb"))
    if not ids:
        return None, "trakt_id_missing"
    return {"progress": progress_percent, "movie": {"title": title, "year": _int(item.get("year")), "ids": ids}}, None


def _episode_body(item: Mapping[str, Any], progress_percent: float) -> tuple[dict[str, Any] | None, str | None]:
    show_ids = _ids(item.get("show_ids"), ("trakt", "slug", "imdb", "tmdb", "tvdb"))
    season = _int(item.get("season") if item.get("season") is not None else item.get("season_number"))
    number = _positive_int(item.get("episode") if item.get("episode") is not None else item.get("number"))

    if show_ids and season is not None and season >= 0 and number is not None:
        show: dict[str, Any] = {"ids": show_ids}
        title = str(item.get("series_title") or "").strip()
        if title:
            show["title"] = title
        year = _int(item.get("series_year") or item.get("year"))
        if year is not None:
            show["year"] = year
        return {"progress": progress_percent, "show": show, "episode": {"season": season, "number": number}}, None

    ids = _ids(item.get("ids"), ("trakt", "tvdb", "imdb", "tmdb"))
    if ids:
        return {"progress": progress_percent, "episode": {"ids": ids}}, None

    if not show_ids or season is None or season < 0 or number is None:
        return None, "trakt_episode_id_missing"
    return None, "trakt_episode_id_missing"


def _scrobble_body(item: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    progress_ms, duration_ms, percent = _progress_values(item)
    if percent is None:
        return None, "trakt_progress_missing" if progress_ms is None else "trakt_duration_missing"
    if percent <= 0 or percent >= 100:
        return None, "trakt_progress_invalid"
    media_type = str(item.get("type") or "").strip().lower()
    if media_type == "movie":
        return _movie_body(item, percent)
    if media_type == "episode":
        return _episode_body(item, percent)
    return None, "trakt_media_type_unsupported"


def _unresolved(item: Mapping[str, Any], reason: str, status: str = "unresolved", **extra: Any) -> dict[str, Any]:
    out = {"status": status, "reason": reason, "item": id_minimal(item)}
    out.update(extra)
    key = canonical_key(item)
    if key:
        out["canonical_key"] = key
    return out


def _result(ok: bool, count: int, attempted: int, confirmed: list[str], unresolved: list[dict[str, Any]], results: list[dict[str, Any]], skipped: int, **extra: Any) -> dict[str, Any]:
    unresolved_keys = []
    for row in unresolved:
        key = str(row.get("canonical_key") or "")
        if key:
            unresolved_keys.append(key)
            continue
        item = row.get("item")
        if isinstance(item, Mapping):
            key = canonical_key(item)
            if key:
                unresolved_keys.append(key)
    out = {
        "ok": bool(ok),
        "count": int(count),
        "attempted": int(attempted),
        "confirmed_keys": list(dict.fromkeys(k for k in confirmed if k)),
        "unresolved": unresolved,
        "unresolved_keys": list(dict.fromkeys(unresolved_keys)),
        "results": results,
        "skipped": int(skipped),
        "errors": sum(1 for row in unresolved if str(row.get("status") or "") == "failed"),
    }
    out.update(extra)
    return out


def _same_trakt_endpoint() -> bool:
    src = str(os.getenv("CW_PAIR_SRC") or "").upper().strip()
    dst = str(os.getenv("CW_PAIR_DST") or "").upper().strip()
    src_instance = str(os.getenv("CW_PAIR_SRC_INSTANCE") or "default").strip().lower() or "default"
    dst_instance = str(os.getenv("CW_PAIR_DST_INSTANCE") or "default").strip().lower() or "default"
    return src == dst == "TRAKT" and src_instance == dst_instance


def _percent_equal(source: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
    _sp, _sd, source_percent = _progress_values(source)
    target_percent = _float(target.get("progress_percent"))
    if source_percent is None or target_percent is None:
        return False
    return abs(float(source_percent) - float(target_percent)) <= 0.1


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    src = [dict(item or {}) for item in items or [] if isinstance(item, Mapping)]
    current = build_index(adapter)
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    pending: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    skipped = 0

    for item in src:
        key = canonical_key(item) or ""
        body, reason = _scrobble_body(item)
        if body is None:
            row = _unresolved(item, reason or "trakt_progress_invalid")
            unresolved.append(row)
            results.append(row)
            continue
        target = current.get(key)
        progress_ms, duration_ms, _percent = _progress_values(item)
        if isinstance(target, Mapping) and _percent_equal(item, target):
            skipped += 1
            results.append({"status": "skipped", "reason": "progress_unchanged", "canonical_key": key, "item": id_minimal(item)})
            continue
        decision = decide_progress_write(
            active_session=False,
            source_timestamp=item.get("progress_at") or item.get("progressAt") or item.get("last_played") or item.get("lastViewedAt"),
            target_timestamp=(target or {}).get("progress_at") if isinstance(target, Mapping) else None,
            source_progress_ms=progress_ms,
            source_duration_ms=duration_ms,
            target_progress_ms=(target or {}).get("progress_ms") if isinstance(target, Mapping) else None,
            target_duration_ms=(target or {}).get("duration_ms") if isinstance(target, Mapping) else None,
            target_watched=False,
            same_origin=_same_trakt_endpoint(),
            replay_enabled=True,
        )
        if not decision.apply:
            skipped += 1
            results.append({"status": "skipped", "reason": decision.reason, "canonical_key": key, "item": id_minimal(item)})
            continue
        pending.append((key, item, body))
        results.append({"status": "pending" if not dry_run else "dry_run", "canonical_key": key, "item": id_minimal(item)})

    if dry_run:
        return _result(len(unresolved) == 0, len(pending), len(pending), [], unresolved, results, skipped, dry_run=True)

    failed = False
    for key, item, body in pending:
        response = adapter.client.post(f"{adapter.client.BASE}/scrobble/pause", json=body)
        status = int(getattr(response, "status_code", 0) or 0)
        if status != 201:
            failed = True
            row = _unresolved(item, f"http:{status}", status="failed", remote_status=status)
            unresolved.append(row)
            results.append(row)
            _warn("write_failed", op="add", canonical_key=key, status=status)

    after = build_index(adapter) if pending and not failed else current
    confirmed: list[str] = []
    for key, item, _body in ([] if failed else pending):
        target = after.get(key)
        if isinstance(target, Mapping) and (
            progress_materially_equal(item.get("progress_ms"), item.get("duration_ms"), target.get("progress_ms"), target.get("duration_ms"))
            or _percent_equal(item, target)
        ):
            confirmed.append(key)
        else:
            row = _unresolved(item, "trakt_progress_verification_failed", status="failed")
            unresolved.append(row)
            results.append(row)

    ok = len(unresolved) == 0
    _info("write_done", op="add", ok=ok, attempted=len(pending), confirmed=len(confirmed), skipped=skipped, unresolved=len(unresolved))
    return _result(ok, len(confirmed), len(pending), confirmed, unresolved, results, skipped)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    src = [dict(item or {}) for item in items or [] if isinstance(item, Mapping)]
    current = build_index(adapter)
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    pending: list[tuple[str, int, dict[str, Any]]] = []
    skipped = 0

    for item in src:
        key = canonical_key(item) or ""
        target = current.get(key)
        if not isinstance(target, Mapping):
            skipped += 1
            results.append({"status": "skipped", "reason": "already_absent", "canonical_key": key, "item": id_minimal(item)})
            continue
        playback_id = _positive_int(target.get("_trakt_playback_id") or item.get("_trakt_playback_id"))
        if playback_id is None:
            row = _unresolved(item, "trakt_playback_id_missing")
            unresolved.append(row)
            results.append(row)
            continue
        pending.append((key, playback_id, item))
        results.append({"status": "pending" if not dry_run else "dry_run", "canonical_key": key, "remote_id": playback_id, "item": id_minimal(item)})

    if dry_run:
        return _result(len(unresolved) == 0, len(pending), len(pending), [], unresolved, results, skipped, dry_run=True)

    failed = False
    confirmed: list[str] = []
    for key, playback_id, item in pending:
        response = adapter.client.delete(f"{adapter.client.BASE}/sync/playback/{playback_id}")
        status = int(getattr(response, "status_code", 0) or 0)
        if status == 204:
            confirmed.append(key)
        else:
            failed = True
            row = _unresolved(item, f"http:{status}", status="failed", remote_status=status)
            unresolved.append(row)
            results.append(row)
            _warn("write_failed", op="remove", canonical_key=key, playback_id=playback_id, status=status)

    after = build_index(adapter) if pending and not failed else current
    verified: list[str] = []
    for key, _playback_id, item in ([] if failed else pending):
        if key not in after:
            verified.append(key)
        else:
            row = _unresolved(item, "trakt_progress_delete_unconfirmed", status="failed")
            unresolved.append(row)
            results.append(row)

    ok = len(unresolved) == 0
    _info("write_done", op="remove", ok=ok, attempted=len(pending), confirmed=len(verified), skipped=skipped, unresolved=len(unresolved))
    return _result(ok, len(verified), len(pending), verified, unresolved, results, skipped)
