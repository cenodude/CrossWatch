# services/export_trakt.py
# CrossWatch - Trakt CSV validation and ordered rewatch exports
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import csv
import io
import re
import zipfile
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from fastapi import Response

from providers.sync.trakt._common import ids_for_trakt

ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt")
HEADER = [f"{key}_id" for key in ID_KEYS] + ["type", "watched_at", "watchlisted_at", "rating", "rated_at"]
MEDIA_IDS = {
    "movie": {"tmdb", "imdb", "trakt"},
    "show": set(ID_KEYS),
    "season": {"tmdb", "tvdb", "trakt"},
    "episode": set(ID_KEYS),
}
IMPORT_URL = "https://app.trakt.tv/settings/data?source=trakt-csv"
GUIDANCE = (
    "Trakt CSV includes available TMDB, IMDb, TVDB and Trakt IDs for movies, shows, seasons and episodes. "
    "Import history files one at a time in numbered order, waiting for each to finish. "
    "Import your watchlist last: importing history can remove watchlist entries. "
    "Trakt uses import time for watchlist dates, even when the CSV contains the original dates."
)


def _timestamp(value: Any, *, unknown: bool = False) -> str:
    raw = str(value or "").strip()
    if unknown and raw in {"", "unknown"}:
        return "unknown"
    if not raw:
        return ""
    if not re.match(r"^\d{4}-\d{2}-\d{2}(?:$|[T ])", raw):
        raise ValueError("Invalid timestamp")
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_ids(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    ids = {}
    for key in ID_KEYS:
        value = str(raw.get(key) or "").strip()
        if key == "imdb":
            if re.fullmatch(r"(?:tt)?[0-9]{7,}", value):
                ids[key] = value if value.startswith("tt") else f"tt{value}"
        elif re.fullmatch(r"[0-9]+", value) and int(value) > 0:
            ids[key] = str(int(value))
    return ids


def _row(feature: str, item: dict[str, Any], media_type: str, ids: dict[str, str]) -> dict[str, str]:
    watched = listed = rating = rated = ""
    if feature == "history":
        watched = _timestamp(item.get("watched_at") or item.get("watchedAt") or item.get("viewed_at"), unknown=True)
    elif feature == "combined" and item.get("_cw_export_history"):
        watched = _timestamp(item.get("_cw_export_watched_at"), unknown=True)
    if feature == "watchlist":
        listed = _timestamp(item.get("watchlisted_at") or item.get("listed_at") or item.get("added_at"))
    if feature in {"ratings", "combined"}:
        raw_rating = item.get("rating")
        if raw_rating is None or raw_rating == "":
            raw_rating = item.get("user_rating")
        if raw_rating not in (None, ""):
            score = float(raw_rating)
            if isinstance(raw_rating, bool) or not score.is_integer() or not 1 <= score <= 10:
                raise ValueError("Invalid rating")
            rating = str(int(score))
            rated = _timestamp(item.get("rated_at"))
        elif not watched:
            raise ValueError("Missing rating")
    return {
        **{f"{key}_id": ids.get(key, "") for key in ID_KEYS},
        "type": media_type, "watched_at": watched, "watchlisted_at": listed, "rating": rating, "rated_at": rated,
    }


def _grouped_rows(items: list[tuple[str, dict[str, Any]]]) -> list[tuple[tuple[str, int, str], dict[str, str]]]:
    parents: dict[tuple[str, int, str], tuple[str, int, str]] = {}

    def root(token: tuple[str, int, str]) -> tuple[str, int, str]:
        parents.setdefault(token, token)
        while parents[token] != token:
            parents[token] = parents[parents[token]]
            token = parents[token]
        return token

    rows = []
    for _, item in items:
        row = dict(item["_cw_trakt_row"])
        tokens = [(row["type"], index, row[f"{key}_id"]) for index, key in enumerate(ID_KEYS) if row[f"{key}_id"]]
        first = root(tokens[0])
        for token in tokens[1:]:
            other = root(token)
            parents[max(first, other)] = min(first, other)
            first = min(first, other)
        rows.append((tokens[0], row))
    return sorted(
        ((root(token), row) for token, row in rows),
        key=lambda entry: (entry[0], entry[1]["watched_at"], entry[1]["rated_at"], entry[1]["rating"]),
    )


def validate_items(
    feature: str, items: list[tuple[str, dict[str, Any]]],
) -> tuple[list[tuple[str, dict[str, Any]]], list[str], dict[str, int]]:
    stats = {
        "matched_total": len(items),
        "unsupported_media_total": 0,
        "missing_identity_total": 0,
        "invalid_watched_date_total": 0,
        "invalid_data_total": 0,
    }
    exportable: list[tuple[str, dict[str, Any]]] = []
    warnings = [GUIDANCE]
    unknown_watches = missing_listed = missing_rated = 0
    for key, item in items:
        media_type = str(item.get("type") or "").strip().lower()
        media_type = {"movies": "movie", "film": "movie", "films": "movie", "tv": "show", "series": "show", "tv series": "show", "shows": "show", "seasons": "season", "episodes": "episode"}.get(media_type, media_type)
        if media_type not in MEDIA_IDS:
            stats["unsupported_media_total"] += 1
            continue
        raw_ids = item.get("ids")
        if not isinstance(raw_ids, dict):
            raw_ids = {}
        ids = _normalize_ids(ids_for_trakt({
            **item, "type": media_type,
            "ids": {**{key: raw_ids[key] for key in ("plex", "jellyfin", "emby") if key in raw_ids}, **_normalize_ids(raw_ids)},
            "show_ids": _normalize_ids(item.get("show_ids")),
        }))
        if not MEDIA_IDS[media_type].intersection(ids):
            stats["missing_identity_total"] += 1
            continue
        try:
            row = _row(feature, item, media_type, ids)
        except (ValueError, TypeError, OverflowError):
            stats["invalid_data_total"] += 1
            continue
        unknown_watches += row["watched_at"] == "unknown"
        missing_listed += feature == "watchlist" and not row["watchlisted_at"]
        missing_rated += bool(row["rating"]) and not row["rated_at"]
        exportable.append((key, {**item, "_cw_trakt_row": row}))
    for field, reason in (
        ("unsupported_media_total", "unsupported media types"),
        ("missing_identity_total", "no usable item-level TMDB, IMDb, TVDB or Trakt ID (Trakt CSV cannot use TVDB alone for movies or IMDb alone for seasons, or resolve show IDs into episodes)"),
        ("invalid_data_total", "invalid dates or missing/invalid integer ratings (1-10)"),
    ):
        if stats[field]:
            warnings.append(f"Trakt skipped {stats[field]} row(s) with {reason}.")
    if unknown_watches:
        warnings.append(f"{unknown_watches} watch(es) will use Trakt's unknown watch date.")
    if missing_listed:
        warnings.append(f"{missing_listed} watchlist row(s) have no saved date and will use export time in the CSV.")
    if missing_rated:
        warnings.append(f"{missing_rated} rating(s) have no saved rating date; Trakt will assign a date on import.")
    counts = Counter(identity for identity, _ in _grouped_rows(exportable))
    if counts and max(counts.values()) > 1:
        warnings.append(f"Download contains a ZIP with {max(counts.values())} CSV files. Extract it and import each CSV separately; combining them can lose rewatches.")
    return exportable, warnings, stats


def _csv_bytes(rows: list[dict[str, str]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=HEADER, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def build_export(provider: str, feature: str, items: list[tuple[str, dict[str, Any]]]) -> Response:
    now = datetime.now(timezone.utc)
    export_time = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    batches: list[list[dict[str, str]]] = [[]]
    counts: Counter[tuple[str, int, str]] = Counter()
    rated_ids: set[tuple[str, int, str]] = set()
    for identity, row in _grouped_rows(items):
        index = counts[identity]
        counts[identity] += 1
        if index == len(batches):
            batches.append([])
        if feature == "watchlist" and not row["watchlisted_at"]:
            row["watchlisted_at"] = export_time
        if row["rating"]:
            if identity in rated_ids and row["watched_at"]:
                row["rating"] = row["rated_at"] = ""
            rated_ids.add(identity)
        batches[index].append(row)
    name = f"trakt_{feature}_{provider.lower()}_{now:%Y%m%d}"
    if len(batches) == 1:
        data = _csv_bytes(batches[0])
        extension, media_type = "csv", "text/csv; charset=utf-8"
    else:
        buffer = io.BytesIO()
        filenames = []
        width = max(2, len(str(len(batches))))
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for index, batch in enumerate(batches, 1):
                filename = f"{index:0{width}d}_trakt_{feature}.csv"
                filenames.append(filename)
                archive.writestr(zipfile.ZipInfo(filename), _csv_bytes(batch), compress_type=zipfile.ZIP_DEFLATED)
            instructions = (
                f"Trakt CSV import\n\nOpen {IMPORT_URL}\n\n{GUIDANCE}\n\n"
                "Extract this ZIP. Upload the following CSV files one at a time, in this order. "
                "Do not combine them into one file.\n\n" + "\n".join(filenames) + "\n"
            )
            archive.writestr(zipfile.ZipInfo("README.txt"), instructions.encode("utf-8"), compress_type=zipfile.ZIP_DEFLATED)
        data = buffer.getvalue()
        extension, media_type = "zip", "application/zip"
    return Response(content=data, media_type=media_type, headers={
        "Content-Disposition": f'attachment; filename="{name}.{extension}"',
        "Cache-Control": "no-store",
    })
