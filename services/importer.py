# services/importer.py
# CrossWatch - Import external tracker exports into the local CrossWatch tracker
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import csv
import hashlib
import io
import json
import time
import zipfile
from collections.abc import Iterable, Mapping
from datetime import date as dt_date, datetime, timezone
from typing import Any, Literal, cast

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from cw_platform.access_policy import request_user, user_can_access_instance
from cw_platform.config_base import load_config
from cw_platform.history_events import history_sync_key
from cw_platform.id_map import canonical_key, coalesce_ids, minimal as id_minimal
from cw_platform.modules_registry import load_sync_ops
from cw_platform.provider_instances import (
    build_provider_config_view,
    list_instance_ids,
    normalize_instance_id,
    sanitize_instance_label,
)

router = APIRouter(prefix="/api/import", tags=["import"])

MAX_UPLOAD_BYTES = 25 * 1024 * 1024
MAX_TEXT_BYTES = 8 * 1024 * 1024
MAX_ZIP_FILES = 1000
MAX_ZIP_MEMBER_BYTES = 8 * 1024 * 1024
MAX_ZIP_TOTAL_BYTES = 60 * 1024 * 1024
MAX_ROWS = 50_000
MAX_PREVIEW_ROWS = 500
PREVIEW_TTL_SECONDS = 30 * 60
ALLOWED_EXTENSIONS = {".zip", ".json", ".csv", ".txt"}
IMPORT_SOURCES = {"auto", "trakt", "letterboxd", "simkl", "imdb", "tvtime", "yamtrack", "generic"}
FEATURES = {"history", "ratings", "watchlist"}
MEDIA_TYPES = {"movie", "show", "season", "episode"}
DEFAULT_MEDIA_TYPES = ["movie", "show", "season", "episode"]

STATUS_READY = "ready"
STATUS_EXISTS = "exists"
STATUS_DUPLICATE = "duplicate"
STATUS_MISSING_IDENTITY = "missing_identity"
STATUS_INVALID = "invalid"
STATUS_UNSUPPORTED = "unsupported"
STATUSES = (STATUS_READY, STATUS_EXISTS, STATUS_DUPLICATE, STATUS_MISSING_IDENTITY, STATUS_INVALID, STATUS_UNSUPPORTED)

SIMKL_WATCHLIST_STATUSES = {"plantowatch"}

YAMTRACK_MEDIA_TYPES = {"tv": "show", "season": "season", "episode": "episode", "movie": "movie", "anime": "show"}
YAMTRACK_WATCHLIST_STATUSES = {"planning", "plan_to_watch", "plantowatch"}

TVTIME_TRACKING_PREFIX = "tracking-prod-records"
TVTIME_FOLLOW_FILE = "followed_tv_show.csv"
TVTIME_EPISODE_RATING_FILES = {
    "ratings-3-prod-episode_votes.csv",
    "ratings-prod-episode_votes.csv",
    "ratings-v2-prod-votes.csv",
}
TVTIME_MOVIE_RATING_FILES = {"ratings-live-votes.csv"}
TVTIME_GDPR_FILES = {
    TVTIME_FOLLOW_FILE,
    "seen_episode_latest.csv",
    "show_seen_episode_latest.csv",
    "seen_episode_source.csv",
    "user_tv_show_data.csv",
    *TVTIME_EPISODE_RATING_FILES,
    *TVTIME_MOVIE_RATING_FILES,
}
TVTIME_RATINGS = {"3": 10.0, "27": 7.0, "29": 4.0}

ERROR_FILE_TOO_LARGE = "import_file_too_large"
ERROR_UNSUPPORTED_FILE_TYPE = "import_unsupported_file_type"
ERROR_ZIP_TOO_LARGE = "import_zip_too_large"
ERROR_ZIP_TOO_MANY_FILES = "import_zip_too_many_files"
ERROR_ZIP_ENCRYPTED = "import_zip_encrypted"
ERROR_ZIP_UNSUPPORTED_MEMBER = "import_zip_unsupported_member"
ERROR_PARSE_FAILED = "import_parse_failed"
ERROR_PREVIEW_NOT_FOUND = "import_preview_not_found"
ERROR_NO_ROWS = "import_no_rows"
ERROR_TARGET_UNAVAILABLE = "import_target_unavailable"
ERROR_SOURCE_MISMATCH = "import_source_mismatch"

_PREVIEWS: dict[str, dict[str, Any]] = {}

SOURCE_EXPECTATIONS: dict[str, dict[str, Any]] = {
    "trakt": {
        "label": "Trakt data export",
        "files": [
            "watched-history-*.json",
            "ratings*.json",
            "watchlist*.json",
            "lists*.json",
            "comments*.json",
        ],
    },
    "letterboxd": {
        "label": "Letterboxd data export",
        "files": ["diary.csv", "watched.csv", "ratings.csv", "watchlist.csv", "reviews.csv", "lists/*.csv"],
    },
    "simkl": {
        "label": "Simkl backup",
        "files": ["SimklBackup.json", "SimklBackup.zip"],
    },
    "imdb": {
        "label": "IMDb CSV export",
        "files": ["watchlist.csv", "ratings.csv", "*.csv"],
    },
    "tvtime": {
        "label": "TV Time export",
        "files": ["tracking-prod-records*.csv", "followed_tv_show.csv", "ratings-*-votes.csv", "tvtime-*.json"],
    },
    "yamtrack": {
        "label": "Yamtrack CSV export",
        "files": ["yamtrack*.csv", "*.csv"],
    },
    "generic": {
        "label": "Generic CSV/JSON",
        "files": ["*.csv", "*.json", "*.zip"],
    },
}

SOURCE_GUIDES: dict[str, dict[str, Any]] = {
    "trakt": {
        "title": "How to export from Trakt",
        "steps": [
            "Go to trakt.tv/settings > Data.",
            "Click Export Data and wait for the download or email.",
            "Upload the ZIP here. No need to unzip it.",
        ],
    },
    "letterboxd": {
        "title": "How to export from Letterboxd",
        "steps": [
            "Go to letterboxd.com/settings/data.",
            "Click Export Your Data and download the ZIP.",
            "Upload the ZIP here. No need to unzip it.",
        ],
    },
    "simkl": {
        "title": "How to export from Simkl",
        "steps": [
            "Go to simkl.com/apps/backup.",
            "Create a full backup.",
            "Upload SimklBackup.json or the ZIP that contains it.",
        ],
    },
    "imdb": {
        "title": "How to export from IMDb",
        "steps": [
            "Open your IMDb Watchlist, Ratings, or custom list on desktop.",
            "Use the Export icon or Export this list option.",
            "Upload the CSV here.",
        ],
    },
    "tvtime": {
        "title": "How to export from TV Time",
        "steps": [
            "Upload the GDPR export ZIP you requested from gdpr.tvtime.com.",
            "TV Time exports carry no TMDb or IMDb IDs, so rows are matched on title.",
            "Ratings are the Wow/Good/Meh reactions, imported as 10/7/4.",
        ],
    },
    "yamtrack": {
        "title": "How to export from Yamtrack",
        "steps": [
            "Go to Settings > Import Data or Export Data.",
            "Export all tracked media as CSV.",
            "Upload the CSV here.",
        ],
    },
    "generic": {
        "title": "Generic CSV or JSON",
        "steps": [
            "Upload a CSV or JSON with title plus IDs when possible.",
            "Supported ID columns include tmdb, imdb, tvdb, trakt, simkl, mal, anilist, and kitsu.",
            "Use watched_at or rating columns to import history or ratings.",
        ],
    },
}


class ImportCommitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    import_id: str = Field(min_length=8, max_length=80)
    target_instance: str = "default"
    mode: Literal["ready", "selected"] = "ready"
    row_ids: list[str] = Field(default_factory=list, max_length=MAX_ROWS)
    features: list[str] = Field(default_factory=lambda: ["history", "ratings", "watchlist"])
    media_types: list[str] = Field(default_factory=lambda: list(DEFAULT_MEDIA_TYPES))
    include_existing: bool = False


def _clean_cache() -> None:
    now = time.time()
    stale = [key for key, value in _PREVIEWS.items() if now - float(value.get("created_at") or 0) > PREVIEW_TTL_SECONDS]
    for key in stale:
        _PREVIEWS.pop(key, None)


def _api_error(code: str, status_code: int = 400, detail: str | None = None, **extra: Any) -> HTTPException:
    payload = {"code": code, "detail": detail or code}
    payload.update(extra)
    return HTTPException(status_code=status_code, detail=payload)


def _suffix(name: str) -> str:
    dot = str(name or "").lower().rfind(".")
    return "" if dot < 0 else str(name or "").lower()[dot:]


def _base_name(name: str) -> str:
    return str(name or "").replace("\\", "/").rsplit("/", 1)[-1].lower()


def _expected_files(source: str) -> list[str]:
    spec = SOURCE_EXPECTATIONS.get(str(source or "").lower()) or {}
    files = spec.get("files")
    return [str(x) for x in files] if isinstance(files, list) else []


def _matches_expected_file(source: str, name: str) -> bool:
    src = str(source or "").lower()
    base = _base_name(name)
    full = str(name or "").replace("\\", "/").lower()
    if src == "trakt":
        return _suffix(base) == ".json" and (
            base.startswith("watched-history-")
            or base.startswith("history")
            or "watchlist" in base
            or "rating" in base
            or "list" in full
            or "comment" in base
        )
    if src == "letterboxd":
        return _suffix(base) == ".csv" and (
            base in {"diary.csv", "watched.csv", "ratings.csv", "watchlist.csv", "reviews.csv"}
            or full.startswith("lists/")
            or "/lists/" in full
        )
    if src == "simkl":
        return base == "simklbackup.json" or "simkl" in base
    if src == "imdb":
        return _suffix(base) == ".csv" and (
            "imdb" in base
            or "watchlist" in base
            or "rating" in base
            or base in {"export.csv", "list.csv"}
        )
    if src == "tvtime":
        return _suffix(base) in {".json", ".csv"} and (
            base.startswith(TVTIME_TRACKING_PREFIX)
            or base in TVTIME_GDPR_FILES
            or "tvtime" in base
            or "tv-time" in base
            or base in {"shows.json", "movies.json", "episodes.json", "series.json", "lists.json"}
            or "gdpr" in full
        )
    if src == "yamtrack":
        return _suffix(base) == ".csv" and ("yamtrack" in base or base in {"export.csv", "media.csv", "tracked_media.csv"})
    if src == "generic":
        return _suffix(base) in {".csv", ".json", ".txt"}
    return True


def _validate_source_files(source: str, files: list[tuple[str, bytes]], *, requested: str) -> dict[str, Any]:
    src = str(source or "").lower()
    names = [str(name or "") for name, _data in files]
    expected = _expected_files(src)
    matched = [name for name in names if _matches_expected_file(src, name)]
    warnings: list[str] = []
    if src == "letterboxd" and names and not any(_base_name(name) in {"diary.csv", "watched.csv", "ratings.csv", "watchlist.csv"} for name in names):
        warnings.append("letterboxd_no_core_csv")
    if src == "simkl" and names and not any(_base_name(name) == "simklbackup.json" for name in names):
        warnings.append("simkl_missing_backup_json")
    if src == "tvtime" and names and not matched:
        warnings.append("tvtime_unknown_export_shape")

    if requested != "auto" and expected and not matched:
        raise _api_error(
            ERROR_SOURCE_MISMATCH,
            detail=f"The upload does not look like a {SOURCE_EXPECTATIONS[src]['label']}.",
            expected_files=expected,
        )
    return {"source": src, "expected_files": expected, "matched_files": matched, "warnings": warnings}


async def _read_upload(file: UploadFile) -> bytes:
    name = str(file.filename or "").strip()
    ext = _suffix(name)
    if ext not in ALLOWED_EXTENSIONS:
        raise _api_error(ERROR_UNSUPPORTED_FILE_TYPE, detail="Upload a ZIP, JSON or CSV export.")

    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_UPLOAD_BYTES:
            raise _api_error(ERROR_FILE_TOO_LARGE, detail="The import file is too large.")
        chunks.append(chunk)
    return b"".join(chunks)


def _decode_text(data: bytes) -> str:
    if len(data) > MAX_TEXT_BYTES:
        raise _api_error(ERROR_FILE_TOO_LARGE, detail="Text exports must be 8 MB or smaller.")
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    raise _api_error(ERROR_PARSE_FAILED, detail="Could not decode the export file.")


def _safe_zip_members(data: bytes) -> list[tuple[str, bytes]]:
    if not zipfile.is_zipfile(io.BytesIO(data)):
        raise _api_error(ERROR_PARSE_FAILED, detail="The ZIP file could not be opened.")

    out: list[tuple[str, bytes]] = []
    total_size = 0
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            infos = [info for info in zf.infolist() if not info.is_dir()]
            if len(infos) > MAX_ZIP_FILES:
                raise _api_error(ERROR_ZIP_TOO_MANY_FILES, detail="The ZIP contains too many files.")
            for info in infos:
                if info.flag_bits & 0x1:
                    raise _api_error(ERROR_ZIP_ENCRYPTED, detail="Encrypted ZIP files are not supported.")
                name = str(info.filename or "")
                ext = _suffix(name)
                if ext not in {".csv", ".json", ".txt"}:
                    continue
                if info.file_size > MAX_ZIP_MEMBER_BYTES:
                    raise _api_error(ERROR_ZIP_TOO_LARGE, detail="A file inside the ZIP is too large.")
                total_size += int(info.file_size or 0)
                if total_size > MAX_ZIP_TOTAL_BYTES:
                    raise _api_error(ERROR_ZIP_TOO_LARGE, detail="The ZIP expands to too much data.")
                out.append((name, zf.read(info)))
    except HTTPException:
        raise
    except Exception as exc:
        raise _api_error(ERROR_PARSE_FAILED, detail="The ZIP file could not be parsed.") from exc

    if not out:
        raise _api_error(ERROR_ZIP_UNSUPPORTED_MEMBER, detail="The ZIP did not contain supported CSV or JSON files.")
    return out


def _csv_rows(text: str) -> list[dict[str, Any]]:
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample)
    except Exception:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    rows: list[dict[str, Any]] = []
    for row in reader:
        if len(rows) >= MAX_ROWS:
            break
        if not isinstance(row, Mapping):
            continue
        rows.append({str(k or "").strip(): v for k, v in row.items() if str(k or "").strip()})
    return rows


def _json_rows(data: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(data, Mapping):
        for key in ("items", "data", "history", "ratings", "watchlist", "movies", "shows", "episodes"):
            val = data.get(key)
            if isinstance(val, list):
                for row in val:
                    if isinstance(row, Mapping):
                        yield row
        yield data
        return
    if isinstance(data, list):
        for row in data:
            if isinstance(row, Mapping):
                yield row


def _norm_key(key: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in str(key or "")).strip("_")


def _lower_map(row: Mapping[str, Any]) -> dict[str, Any]:
    return {_norm_key(str(k)): v for k, v in (row or {}).items()}


def _first(row: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        norm = _norm_key(key)
        if norm in row and row.get(norm) not in (None, ""):
            return row.get(norm)
    return None


def _as_int(value: Any) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "watched", "completed", "complete", "done"}


def _first_scalar(row: Mapping[str, Any], keys: Iterable[str]) -> Any:
    value = _first(row, keys)
    if isinstance(value, Mapping):
        return None
    return value


def _type(value: Any, path: str = "") -> str:
    raw = str(value or path or "").strip().lower()
    if "episode" in raw:
        return "episode"
    if raw in {"tv", "show", "shows", "series", "anime"} or "show" in raw or "series" in raw:
        return "show"
    if "season" in raw:
        return "season"
    return "movie"


def _iso_date(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if raw.isdigit():
            dt = datetime.fromtimestamp(int(raw[:10]), tz=timezone.utc)
            return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if len(raw) == 10:
            d = dt_date.fromisoformat(raw)
            return datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00").replace(" ", "T"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _rating(value: Any, *, source: str = "") -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(str(value).strip())
    except Exception:
        return None
    if source == "letterboxd" and 0 < number <= 5:
        number *= 2
    if not 0 < number <= 10:
        return None
    return round(number, 1)


def _ids(row: Mapping[str, Any], nested: Mapping[str, Any] | None = None) -> dict[str, str]:
    src = dict(nested or {})
    aliases = {
        "tmdb": "tmdb",
        "tmdb_id": "tmdb",
        "tmdbid": "tmdb",
        "imdb": "imdb",
        "imdb_id": "imdb",
        "imdbid": "imdb",
        "const": "imdb",
        "tvdb": "tvdb",
        "tvdb_id": "tvdb",
        "trakt": "trakt",
        "trakt_id": "trakt",
        "simkl": "simkl",
        "simkl_id": "simkl",
        "mal": "mal",
        "mal_id": "mal",
        "myanimelist": "mal",
        "anilist": "anilist",
        "anilist_id": "anilist",
        "kitsu": "kitsu",
        "kitsu_id": "kitsu",
        "guid": "guid",
        "slug": "slug",
    }
    for key, dest in aliases.items():
        val = _first(row, [key])
        if val not in (None, ""):
            src[dest] = val
    return coalesce_ids(src)


def _nested_ids(raw: Mapping[str, Any]) -> dict[str, str]:
    ids = raw.get("ids")
    if isinstance(ids, Mapping):
        return coalesce_ids(ids)
    ids = raw.get("id")
    if isinstance(ids, Mapping):
        return coalesce_ids(ids)
    return {}


_TITLE_KEYS = ["title", "name", "full_title", "fulltitle", "movie_title", "show_title", "series_title", "original_title"]
_EPISODE_TITLE_KEYS = ["title", "name", "episode_title", "full_title", "fulltitle", "original_title"]


def _row_title(row: Mapping[str, Any], *, media_type: str = "") -> str:
    value = _first(row, _EPISODE_TITLE_KEYS if media_type == "episode" else _TITLE_KEYS)
    if isinstance(value, Mapping | list | tuple | set):
        return ""
    return str(value or "").strip()


def _row_year(row: Mapping[str, Any]) -> int | None:
    return _as_int(_first(row, ["year", "release_year", "movie_year", "show_year"]))


def _feature_from_path(path: str, row: Mapping[str, Any], source: str) -> str | None:
    lower = path.lower()
    raw_type = str(_first(row, ["feature", "section", "action"]) or "").lower()
    if source == "generic":
        if "watchlist" in lower or "list" in lower or "planned" in str(_first(row, ["status", "state"]) or "").lower():
            return "watchlist"
        if _first(row, ["watched_at", "watched_date", "watcheddate", "viewed_at", "viewdate", "last_viewed_at", "lastviewedat", "last_watched_at", "end_date", "ended_on", "progressed_at"]):
            return "history"
        if _first(row, ["rating10", "your_rating", "yourrating", "rating", "user_rating", "userrating", "rate", "score"]):
            return "ratings"
        return "watchlist"
    if source == "yamtrack":
        status = "_".join(str(_first(row, ["status", "state"]) or "").strip().lower().split())
        if status in YAMTRACK_WATCHLIST_STATUSES:
            return "watchlist"
        if status in {"completed", "in_progress", "paused", "dropped"} or _first(row, ["end_date", "progressed_at", "watched_at"]):
            return "history"
        if _first(row, ["score", "rating"]) not in (None, ""):
            return "ratings"
        return "watchlist"
    if source == "imdb":
        if "watchlist" in lower:
            return "watchlist"
        if "rating" in lower or _first(row, ["your_rating", "rating", "date_rated"]):
            return "ratings"
        return "watchlist"
    if source == "tvtime":
        if "list" in lower or "favorite" in lower:
            return "watchlist"
        if _as_bool(_first(row, ["is_watched", "watched"])) or _first(row, ["watched_at", "watched_date", "watched_date_utc"]):
            return "history"
        if "movie" in lower or "series" in lower or "show" in lower:
            return "watchlist"
    if "watchlist" in lower or "watchlist" in raw_type:
        return "watchlist"
    if "history" in lower or "watched" in lower or "diary" in lower or _first(row, ["watched_at", "watched_date", "watcheddate", "viewed_at"]):
        return "history"
    if source == "simkl" and _first(row, ["last_watched_at", "watched_at"]):
        return "history"
    if "rating" in lower or "ratings" in raw_type or _first(row, ["rating", "rated_at"]):
        return "ratings"
    return None


def _minimal_item(
    path: str,
    raw_row: Mapping[str, Any],
    source: str,
    feature_hint: str | None = None,
) -> tuple[str | None, dict[str, Any]]:
    row = _lower_map(raw_row)
    nested_name = ""
    nested_obj: Mapping[str, Any] = {}
    for candidate in ("episode", "season", "movie", "show"):
        value = raw_row.get(candidate)
        if isinstance(value, Mapping):
            nested_name = candidate
            nested_obj = value
            break
    nested_row = _lower_map(nested_obj)
    feature = feature_hint or _feature_from_path(path, row, source)
    raw_type = _first(row, ["type", "media_type", "kind", "title_type", "item_type"])
    if source == "yamtrack":
        yam_type = str(raw_type or "").strip().lower()
        media_type = YAMTRACK_MEDIA_TYPES.get(yam_type, yam_type or "movie")
    else:
        media_type = _type(raw_type or nested_name, path)
    title = _row_title(row, media_type=media_type) or _row_title(nested_row, media_type=media_type)
    year = _row_year(row) or _row_year(nested_row)
    nested = _nested_ids(raw_row)
    ids = _ids(row, coalesce_ids(nested, _nested_ids(nested_obj), _ids(nested_row, None)))
    if source in {"yamtrack", "generic"}:
        src_name = str(_first(row, ["source"]) or "").strip().lower()
        media_id = _first(row, ["media_id", "identifier", "source_id"])
        if media_id not in (None, ""):
            key = {"myanimelist": "mal"}.get(src_name, src_name)
            if key in {"tmdb", "imdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu"}:
                ids = coalesce_ids(ids, {key: media_id})
    item: dict[str, Any] = {"type": media_type, "title": title or None, "year": year, "ids": ids}
    if media_type in {"episode", "season"}:
        season_raw = _first_scalar(row, ["season", "season_number"])
        if season_raw in (None, ""):
            season_keys = ["season", "season_number"] if media_type == "episode" else ["season", "season_number", "number"]
            season_raw = _first_scalar(nested_row, season_keys)
        season = _as_int(season_raw)
        episode = None
        if media_type == "episode":
            episode_raw = _first_scalar(row, ["episode", "episode_number", "number"])
            if episode_raw in (None, ""):
                episode_raw = _first_scalar(nested_row, ["episode", "episode_number", "number"])
            episode = _as_int(episode_raw)
        show_raw = raw_row.get("show")
        show_obj: Mapping[str, Any] = show_raw if isinstance(show_raw, Mapping) else {}
        row_show_ids = raw_row.get("show_ids")
        nested_show_ids = show_obj.get("ids")
        show_ids = coalesce_ids(
            row_show_ids if isinstance(row_show_ids, Mapping) else {},
            nested_show_ids if isinstance(nested_show_ids, Mapping) else {},
        )
        if not show_ids:
            show_ids = coalesce_ids(
                {
                    "tvdb": _first(row, ["series_tvdb_id", "show_tvdb_id"]),
                    "imdb": _first(row, ["series_imdb_id", "show_imdb_id"]),
                    "slug": _first(row, ["series_uuid", "show_uuid"]),
                }
            )
        if not show_ids:
            show_ids = _ids(row, None)
        item["show_ids"] = show_ids
        item["season"] = season
        if media_type == "episode":
            item["episode"] = episode
        show_row = _lower_map(show_obj)
        series_title = str(_first_scalar(row, ["series_title", "show_title", "show_name"]) or _row_title(show_row) or "").strip()
        if series_title:
            item["series_title"] = series_title

    watched = _iso_date(_first(row, ["watched_at", "watched_date", "watcheddate", "viewed_at", "viewdate", "last_viewed_at", "lastviewedat", "last_watched_at", "end_date", "ended_on", "progressed_at", "date"]))
    rated = _iso_date(_first(row, ["rated_at", "user_rated_at", "date_rated", "daterated", "reviewed_at", "date"]))
    rate = _rating(_first(row, ["rating10", "your_rating", "yourrating", "rating", "user_rating", "userrating", "rate", "score"]), source=source)
    if watched:
        item["watched_at"] = watched
        item["watched"] = True
    if rated:
        item["rated_at"] = rated
    if rate is not None:
        item["rating"] = rate
    if source == "trakt":
        event_id = _first(row, ["id", "history_id"])
        if event_id not in (None, ""):
            item["_trakt_history_id"] = str(event_id).strip()
    return feature, item


def _simkl_expand(data: Any) -> list[tuple[str, Mapping[str, Any], str | None]]:
    rows: list[tuple[str, Mapping[str, Any], str | None]] = []
    if not isinstance(data, Mapping):
        return rows

    for section in ("movies", "shows", "anime"):
        entries = data.get(section)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue

            nested = entry.get("movie") if section == "movies" else entry.get("show")
            media_obj = nested if isinstance(nested, Mapping) else entry
            media_type = "movie" if section == "movies" or str(entry.get("anime_type") or "").lower() == "movie" else "show"
            ids = media_obj.get("ids") if isinstance(media_obj.get("ids"), Mapping) else {}
            title = media_obj.get("title") or media_obj.get("name")
            base: dict[str, Any] = {"type": media_type, "title": title, "year": media_obj.get("year"), "ids": ids}
            status = str(entry.get("status") or "").strip().lower()

            if status in SIMKL_WATCHLIST_STATUSES:
                rows.append((f"{section}_watchlist.json", {**base, "listed_at": entry.get("added_to_watchlist_at")}, "watchlist"))

            seasons = entry.get("seasons")
            if isinstance(seasons, list):
                for season in seasons:
                    if not isinstance(season, Mapping):
                        continue
                    sn = season.get("number")
                    if sn in (None, ""):
                        sn = season.get("season")
                    episodes = season.get("episodes")
                    if not isinstance(episodes, list):
                        continue
                    for ep in episodes:
                        if not isinstance(ep, Mapping):
                            continue
                        merged = {**dict(ep), "type": "episode", "season": sn, "show_ids": ids, "series_title": title}
                        rows.append((f"{section}_episodes.json", merged, "history"))
            elif entry.get("last_watched_at"):
                rows.append((f"{section}_history.json", {**base, "watched_at": entry.get("last_watched_at")}, "history"))

            if entry.get("user_rating") not in (None, ""):
                rated = {**base, "user_rating": entry.get("user_rating"), "rated_at": entry.get("user_rated_at")}
                rows.append((f"{section}_ratings.json", rated, "ratings"))
    return rows


def _tvtime_expand(data: Any, path: str) -> list[tuple[str, Mapping[str, Any], str | None]]:
    rows: list[tuple[str, Mapping[str, Any], str | None]] = []
    lower = path.lower()
    payload = data
    if isinstance(data, Mapping):
        for key in ("shows", "series", "movies", "episodes", "items", "data"):
            val = data.get(key)
            if isinstance(val, list):
                payload = val
                lower = f"{lower}/{key}"
                break
    if not isinstance(payload, list):
        return [(path, data, None)] if isinstance(data, Mapping) else rows

    def ids_from(value: Mapping[str, Any]) -> dict[str, Any]:
        ids = value.get("ids") if isinstance(value.get("ids"), Mapping) else value.get("id")
        return dict(ids) if isinstance(ids, Mapping) else {}

    if "movie" in lower:
        for movie in payload:
            if not isinstance(movie, Mapping):
                continue
            row = dict(movie)
            row["type"] = "movie"
            row["ids"] = ids_from(movie)
            rows.append((path, row, None))
        return rows

    if "episode" in lower:
        for ep in payload:
            if isinstance(ep, Mapping):
                row = dict(ep)
                row["type"] = "episode"
                rows.append((path, row, None))
        return rows

    for show in payload:
        if not isinstance(show, Mapping):
            continue
        show_ids = ids_from(show)
        show_title = show.get("title") or show.get("name")
        rows.append((path, {**dict(show), "type": "show", "ids": show_ids}, None))
        seasons = show.get("seasons")
        if not isinstance(seasons, list):
            continue
        for season in seasons:
            if not isinstance(season, Mapping):
                continue
            season_number = season.get("number") or season.get("season")
            episodes = season.get("episodes")
            if not isinstance(episodes, list):
                continue
            for ep in episodes:
                if not isinstance(ep, Mapping):
                    continue
                rows.append(
                    (
                        f"{path}/episodes",
                        {
                            **dict(ep),
                            "type": "episode",
                            "season": season_number,
                            "episode": ep.get("number") or ep.get("episode"),
                            "series_title": show_title,
                            "show_ids": show_ids,
                            "ids": ids_from(ep),
                        },
                        None,
                    )
                )
    return rows


def _tvtime_slug(title: str) -> str:
    parts = "".join(ch.lower() if ch.isalnum() else " " for ch in str(title or "")).split()
    if parts and parts[0] == "the":
        parts = parts[1:]
    return f"tvtime-{'-'.join(parts)}" if parts else ""


def _tvtime_show_ids(title: str) -> dict[str, str]:
    slug = _tvtime_slug(title)
    return {"slug": slug} if slug else {}


def _tvtime_track_row(raw: Mapping[str, Any]) -> tuple[dict[str, Any], str] | None:
    row = _lower_map(raw)
    kind = str(_first(row, ["type"]) or "").strip().lower()
    if kind.startswith("count-"):
        return None
    when = _first(row, ["created_at", "watch_date", "updated_at"])

    movie_title = str(_first(row, ["movie_name"]) or "").strip()
    if movie_title:
        item: dict[str, Any] = {"type": "movie", "title": movie_title, "year": _as_int(str(_first(row, ["release_date"]) or "")[:4])}
        if kind in {"follow", "towatch"}:
            return {**item, "listed_at": when}, "watchlist"
        if kind and kind not in {"watch", "rewatch"}:
            return None
        return {**item, "watched_at": when}, "history"

    series_title = str(_first(row, ["series_name", "tv_show_name"]) or "").strip()
    if not series_title:
        return None
    season = _first(row, ["season_number", "s_no"])
    episode = _first(row, ["episode_number", "ep_no"])
    has_episode = episode not in (None, "") or _first(row, ["episode_id", "ep_id"]) not in (None, "")
    if kind in {"follow", "towatch"} or not has_episode:
        if kind in {"follow", "towatch"} or _as_bool(_first(row, ["is_followed", "is_for_later"])):
            return {"type": "show", "title": series_title, "ids": _tvtime_show_ids(series_title), "listed_at": when}, "watchlist"
        return None
    if kind and kind not in {"watch", "rewatch"}:
        return None
    return (
        {
            "type": "episode",
            "series_title": series_title,
            "season": season,
            "episode": episode,
            "show_ids": _tvtime_show_ids(series_title),
            "watched_at": when,
        },
        "history",
    )


def _tvtime_gdpr_expand(path: str, rows: list[dict[str, Any]]) -> list[tuple[str, Mapping[str, Any], str | None]] | None:
    base = _base_name(path)
    if not (base.startswith(TVTIME_TRACKING_PREFIX) or base in TVTIME_GDPR_FILES):
        return None
    out: list[tuple[str, Mapping[str, Any], str | None]] = []

    if base.startswith(TVTIME_TRACKING_PREFIX):
        for raw in rows:
            built = _tvtime_track_row(raw)
            if built:
                out.append((path, built[0], built[1]))
        return out

    if base == TVTIME_FOLLOW_FILE:
        for raw in rows:
            row = _lower_map(raw)
            title = str(_first(row, ["tv_show_name", "series_name"]) or "").strip()
            active = _first(row, ["active"])
            if not title or (active is not None and not _as_bool(active)):
                continue
            out.append((path, {"type": "show", "title": title, "ids": _tvtime_show_ids(title), "listed_at": _first(row, ["created_at"])}, "watchlist"))
        return out

    is_movie_ratings = base in TVTIME_MOVIE_RATING_FILES
    if not is_movie_ratings and base not in TVTIME_EPISODE_RATING_FILES:
        return out

    for raw in rows:
        row = _lower_map(raw)
        vote = str(_first(row, ["vote_key"]) or "").strip().rsplit("-", 1)[-1]
        rating = TVTIME_RATINGS.get(vote)
        if rating is None:
            continue
        when = _first(row, ["created_at", "updated_at"])
        if is_movie_ratings:
            title = str(_first(row, ["movie_name"]) or "").strip()
            if title:
                out.append((path, {"type": "movie", "title": title, "rating": rating, "rated_at": when}, "ratings"))
            continue
        series_title = str(_first(row, ["series_name", "tv_show_name"]) or "").strip()
        season = _first(row, ["season_number"])
        episode = _first(row, ["episode_number"])
        if not series_title or season in (None, "") or episode in (None, ""):
            continue
        out.append(
            (
                path,
                {
                    "type": "episode",
                    "series_title": series_title,
                    "season": season,
                    "episode": episode,
                    "show_ids": _tvtime_show_ids(series_title),
                    "rating": rating,
                    "rated_at": when,
                },
                "ratings",
            )
        )
    return out


def _parse_files(source: str, files: list[tuple[str, bytes]]) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    trakt_has_history = source == "trakt" and any(_base_name(path).startswith("watched-history-") for path, _data in files)

    letterboxd_has_diary = source == "letterboxd" and any(_base_name(path) == "diary.csv" for path, _data in files)
    letterboxd_seen: set[str] = set()
    if letterboxd_has_diary:
        priority = {"diary.csv": 0, "ratings.csv": 1, "watchlist.csv": 2, "watched.csv": 3}
        files = sorted(files, key=lambda item: priority.get(_base_name(item[0]), 4))

    for path, data in files:
        base_name = _base_name(path)
        if letterboxd_has_diary and base_name == "reviews.csv":
            continue
        if source == "trakt":
            if base_name.startswith(
                (
                    "hidden-",
                    "user-",
                    "network-",
                    "likes-",
                    "notes-",
                    "collection-",
                    "comments-",
                    "lists-list-",
                    "lists-lists",
                    "lists-favorites",
                    "lists-collaborations",
                )
            ):
                continue
            if base_name == "watched-playback.json":
                continue
            if trakt_has_history and base_name.startswith(("watched-movies-", "watched-shows")):
                continue
        rows: list[tuple[str, Mapping[str, Any], str | None]] = []
        ext = _suffix(path)
        if ext == ".csv":
            csv_rows = _csv_rows(_decode_text(data))
            expanded = _tvtime_gdpr_expand(path, csv_rows) if source == "tvtime" else None
            rows = expanded if expanded is not None else [(path, row, None) for row in csv_rows]
        elif ext in {".json", ".txt"}:
            text = _decode_text(data)
            try:
                loaded = json.loads(text)
            except Exception:
                if ext == ".txt":
                    continue
                raise
            if source == "simkl":
                rows = _simkl_expand(loaded)
            elif source == "tvtime":
                rows = _tvtime_expand(loaded, path)
            else:
                rows = [(path, row, None) for row in _json_rows(loaded)]
        for row_path, row, hint in rows:
            if len(parsed) >= MAX_ROWS:
                break
            feature, item = _minimal_item(row_path, row, source, hint)
            if not feature:
                continue
            if letterboxd_has_diary and feature == "history":
                base = canonical_key(id_minimal(item))
                if base_name == "watched.csv":
                    if base in letterboxd_seen:
                        continue
                else:
                    letterboxd_seen.add(base)
            parsed.append({"source_path": row_path, "feature": feature, "item": item})
            if feature in {"history", "watchlist"} and item.get("rating") not in (None, ""):
                parsed.append({"source_path": row_path, "feature": "ratings", "item": item})
    return parsed


def _detect_source(requested: str, filename: str, files: list[tuple[str, bytes]]) -> str:
    wanted = str(requested or "auto").strip().lower()
    if wanted in IMPORT_SOURCES and wanted != "auto":
        return wanted
    hay = " ".join([filename, *(name for name, _ in files)]).lower()
    sample = " ".join((data[:2048].decode("utf-8", "ignore") for _name, data in files[:5])).lower()
    if "letterboxd" in hay or any(name.lower().endswith(("watched.csv", "diary.csv")) for name, _ in files):
        return "letterboxd"
    if "simkl" in hay:
        return "simkl"
    if "yamtrack" in hay or ("media_id" in sample and "media_type" in sample and "progressed_at" in sample):
        return "yamtrack"
    if (
        "tvtime" in hay
        or "tv-time" in hay
        or any(_base_name(name).startswith(TVTIME_TRACKING_PREFIX) or _base_name(name) in TVTIME_GDPR_FILES for name, _ in files)
        or "series_tvdb_id" in sample
        or "is_watched" in sample
    ):
        return "tvtime"
    if "imdb" in hay or ("const" in sample and "title type" in sample) or "your rating" in sample:
        return "imdb"
    if "trakt" in hay:
        return "trakt"
    return "generic"


def _target_connected(cfg: Mapping[str, Any], instance: str) -> bool:
    ops = load_sync_ops("CROSSWATCH")
    if not ops:
        return False
    if not hasattr(ops, "is_configured"):
        return True
    try:
        return bool(ops.is_configured(build_provider_config_view(cfg, "CROSSWATCH", instance)))
    except Exception:
        return False


def _target_instances(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    root = cfg.get("crosswatch") if isinstance(cfg, Mapping) else None
    root = root if isinstance(root, Mapping) else {}
    out: list[dict[str, Any]] = []
    for inst in list_instance_ids(cfg, "crosswatch"):
        iid = normalize_instance_id(inst)
        block = root if iid == "default" else ((root.get("instances") or {}).get(iid) if isinstance(root.get("instances"), Mapping) else {})
        if iid != "default" and not isinstance(block, Mapping):
            continue
        label = sanitize_instance_label((block or {}).get("label") if isinstance(block, Mapping) else "", max_len=24)
        out.append({"id": iid, "label": label or ("Default" if iid == "default" else iid), "connected": _target_connected(cfg, iid)})
    return out or [{"id": "default", "label": "Default", "connected": _target_connected(cfg, "default")}]


def _key_for(feature: str, item: Mapping[str, Any]) -> str:
    if feature == "history":
        return history_sync_key(item, event_mode=True)
    return canonical_key(id_minimal(item))


def _existing_keys(cfg: Mapping[str, Any], instance: str) -> dict[str, Any]:
    ops = load_sync_ops("CROSSWATCH")
    if not ops:
        return {f: set() for f in FEATURES}
    cfg_view = build_provider_config_view(cfg, "CROSSWATCH", instance)
    if hasattr(ops, "is_configured"):
        try:
            if not bool(ops.is_configured(cfg_view)):
                return {f: {} for f in FEATURES}
        except Exception:
            return {f: {} for f in FEATURES}
    out: dict[str, Any] = {f: {} for f in FEATURES}
    for feature in FEATURES:
        try:
            view = dict(cfg_view)
            view["_cw_readonly"] = True
            view["_cw_current_state_only"] = True
            if feature == "history":
                view["_cw_history_rewatches"] = True
            idx = ops.build_index(view, feature=feature) or {}
            out[feature] = {str(k): dict(v or {}) for k, v in idx.items()}
        except Exception:
            out[feature] = set()
    return out


def _existing_item(existing: Mapping[str, Any], feature: str, key: str) -> Mapping[str, Any] | None:
    bucket = existing.get(feature)
    if isinstance(bucket, Mapping):
        if key not in bucket:
            return None
        item = bucket.get(key)
        return item if isinstance(item, Mapping) else {}
    if key in (bucket or set()):
        return {}
    return None


def _same_rating(a: Any, b: Any) -> bool:
    ra = _rating(a)
    rb = _rating(b)
    return ra is not None and rb is not None and ra == rb


def _display_title(item: Mapping[str, Any], key: str = "") -> str:
    media_type = str(item.get("type") or "").lower()
    title = str(item.get("title") or "").strip()
    series_title = str(item.get("series_title") or item.get("show_title") or "").strip()
    if media_type == "episode":
        season = _as_int(item.get("season"))
        episode = _as_int(item.get("episode"))
        if series_title and season is not None and episode is not None:
            suffix = f"S{season:02d}E{episode:02d}"
            return f"{series_title} - {suffix}" + (f" - {title}" if title and title not in {suffix, series_title} else "")
        if series_title:
            return series_title
    if media_type == "season":
        season = _as_int(item.get("season"))
        if series_title and season is not None:
            return f"{series_title} - S{season:02d}"
        if series_title:
            return series_title
    return str(series_title or title or key or "").strip()


def _shape_rows(raw_rows: list[dict[str, Any]], cfg: Mapping[str, Any], target_instance: str, source: str) -> list[dict[str, Any]]:
    existing = _existing_keys(cfg, target_instance)
    seen: set[tuple[str, str]] = set()
    rows: list[dict[str, Any]] = []
    for i, row in enumerate(raw_rows):
        feature = str(row.get("feature") or "").lower()
        item = dict(row.get("item") or {})
        media_type = str(item.get("type") or "").lower()
        key = _key_for(feature, item) if feature in FEATURES else ""
        status = STATUS_READY
        reason = ""
        if feature not in FEATURES:
            status, reason = STATUS_UNSUPPORTED, "unsupported_feature"
        elif media_type not in MEDIA_TYPES:
            status, reason = STATUS_UNSUPPORTED, "unsupported_media_type"
        elif not key or key == "unknown:":
            status, reason = STATUS_MISSING_IDENTITY, "missing_identity"
        elif feature == "history" and not item.get("watched_at"):
            status, reason = STATUS_INVALID, "missing_watched_at"
        elif feature == "ratings" and item.get("rating") in (None, ""):
            status, reason = STATUS_INVALID, "missing_rating"
        elif (feature, key) in seen:
            status, reason = STATUS_DUPLICATE, "duplicate_in_file"
        else:
            existing_item = _existing_item(existing, feature, key)
            if existing_item is not None:
                if feature == "ratings" and not _same_rating(item.get("rating"), existing_item.get("rating")):
                    reason = "rating_update"
                else:
                    status, reason = STATUS_EXISTS, "already_exists"
        seen.add((feature, key))
        title = _display_title(item, key)
        row_id = hashlib.sha256(f"{source}|{feature}|{key}|{i}".encode("utf-8")).hexdigest()[:16]
        rows.append(
            {
                "id": row_id,
                "feature": feature,
                "media_type": media_type,
                "title": title,
                "year": item.get("year"),
                "key": key,
                "status": status,
                "reason": reason,
                "default_included": status == STATUS_READY,
                "importable": status in (STATUS_READY, STATUS_EXISTS),
                "source_path": row.get("source_path"),
                "item": item,
            }
        )
    return rows


def _summary(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "total": 0,
        "ready": 0,
        "exists": 0,
        "by_feature": {f: 0 for f in sorted(FEATURES)},
        "by_feature_ready": {f: 0 for f in sorted(FEATURES)},
        "by_feature_exists": {f: 0 for f in sorted(FEATURES)},
        "by_status": {s: 0 for s in STATUSES},
        "by_reason": {},
    }
    for row in rows:
        out["total"] += 1
        status = str(row.get("status") or "unknown")
        reason = str(row.get("reason") or "")
        feature = str(row.get("feature") or "")
        if status == STATUS_READY:
            out["ready"] += 1
        elif status == STATUS_EXISTS:
            out["exists"] += 1
        if feature in out["by_feature"]:
            out["by_feature"][feature] += 1
            if status == STATUS_READY:
                out["by_feature_ready"][feature] += 1
            elif status == STATUS_EXISTS:
                out["by_feature_exists"][feature] += 1
        out["by_status"][status] = out["by_status"].get(status, 0) + 1
        if reason:
            out["by_reason"][reason] = out["by_reason"].get(reason, 0) + 1
    return out


def _filtered_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    features: set[str],
    media_types: set[str],
    status: str,
    q: str,
) -> list[dict[str, Any]]:
    q = str(q or "").strip().lower()
    out: list[dict[str, Any]] = []
    for row in rows:
        if features and str(row.get("feature") or "") not in features:
            continue
        if media_types and str(row.get("media_type") or "") not in media_types:
            continue
        if status and status != "all" and str(row.get("status") or "") != status:
            continue
        if q:
            hay = " ".join(
                str(row.get(k) or "")
                for k in ("title", "year", "key", "feature", "media_type", "status", "reason")
            ).lower()
            if not all(tok in hay for tok in q.split()):
                continue
        out.append(dict(row))
    if status == "all":
        priority = {
            STATUS_READY: 0,
            STATUS_EXISTS: 1,
            STATUS_MISSING_IDENTITY: 2,
            STATUS_INVALID: 3,
            STATUS_UNSUPPORTED: 4,
            STATUS_DUPLICATE: 5,
        }
        out.sort(key=lambda row: (priority.get(str(row.get("status") or ""), 9), str(row.get("title") or "").lower()))
    return out


def _public_rows(rows: Iterable[Mapping[str, Any]], limit: int, offset: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in list(rows)[max(0, offset) : max(0, offset) + max(1, min(limit, MAX_PREVIEW_ROWS))]:
        raw_item = row.get("item")
        item: Mapping[str, Any] = raw_item if isinstance(raw_item, Mapping) else {}
        show_ids = item.get("show_ids")
        ids = item.get("ids")
        if str(row.get("media_type")) == "episode" and not ids:
            ids = show_ids
        out.append(
            {
                "id": row.get("id"),
                "feature": row.get("feature"),
                "media_type": row.get("media_type"),
                "title": row.get("title"),
                "year": row.get("year"),
                "key": row.get("key"),
                "status": row.get("status"),
                "reason": row.get("reason"),
                "default_included": row.get("default_included"),
                "importable": row.get("importable"),
                "watched_at": item.get("watched_at"),
                "rating": item.get("rating"),
                "season": item.get("season"),
                "episode": item.get("episode"),
                "show_ids": show_ids if isinstance(show_ids, Mapping) else {},
                "ids": ids if isinstance(ids, Mapping) else {},
            }
        )
    return out


@router.get("/options", response_class=JSONResponse)
def api_import_options(request: Request = cast(Request, None)) -> dict[str, Any]:
    cfg = load_config() or {}
    targets = [
        target for target in _target_instances(cfg)
        if user_can_access_instance(cfg, request_user(request), "CROSSWATCH", target.get("id") or "default")
    ]
    return {
        "sources": [
            {"id": "auto", "label": "Auto detect"},
            {"id": "trakt", "label": "Trakt export ZIP"},
            {"id": "letterboxd", "label": "Letterboxd ZIP"},
            {"id": "simkl", "label": "Simkl backup JSON"},
            {"id": "imdb", "label": "IMDb CSV"},
            {"id": "tvtime", "label": "TV Time export"},
            {"id": "yamtrack", "label": "Yamtrack CSV"},
            {"id": "generic", "label": "Generic CSV/JSON"},
        ],
        "targets": targets,
        "features": sorted(FEATURES),
        "media_types": list(DEFAULT_MEDIA_TYPES),
        "statuses": list(STATUSES),
        "source_expectations": {
            source: {
                "label": str(spec.get("label") or source),
                "files": _expected_files(source),
            }
            for source, spec in SOURCE_EXPECTATIONS.items()
        },
        "source_guides": SOURCE_GUIDES,
        "limits": {
            "upload_bytes": MAX_UPLOAD_BYTES,
            "rows": MAX_ROWS,
        },
    }


@router.post("/preview", response_class=JSONResponse)
async def api_import_preview(
    file: UploadFile = File(...),
    source: str = Form("auto"),
    target_instance: str = Form("default"),
    request: Request = cast(Request, None),
) -> dict[str, Any]:
    _clean_cache()
    requested = str(source or "auto").strip().lower()
    if requested not in IMPORT_SOURCES:
        raise _api_error(ERROR_UNSUPPORTED_FILE_TYPE, detail="Unsupported import source.")
    instance = normalize_instance_id(target_instance)
    cfg = load_config() or {}
    if not user_can_access_instance(cfg, request_user(request), "CROSSWATCH", instance):
        raise _api_error(ERROR_TARGET_UNAVAILABLE, status_code=403, detail="profile_scope_denied")
    data = await _read_upload(file)
    filename = str(file.filename or "upload").strip()
    files = _safe_zip_members(data) if zipfile.is_zipfile(io.BytesIO(data)) else [(filename, data)]
    detected = _detect_source(requested, filename, files)
    validation = _validate_source_files(detected, files, requested=requested)
    try:
        raw_rows = _parse_files(detected, files)
    except HTTPException:
        raise
    except Exception as exc:
        raise _api_error(ERROR_PARSE_FAILED, detail="Could not parse this export.") from exc
    if not raw_rows:
        raise _api_error(ERROR_NO_ROWS, detail="No importable rows were found.")

    rows = _shape_rows(raw_rows, cfg, instance, detected)
    import_id = hashlib.sha256(f"{time.time_ns()}|{filename}|{len(data)}".encode("utf-8")).hexdigest()[:32]
    _PREVIEWS[import_id] = {
        "created_at": time.time(),
        "source": detected,
        "filename": filename,
        "target_instance": instance,
        "validation": validation,
        "rows": rows,
    }
    filtered = _filtered_rows(rows, features=set(), media_types=set(), status="ready", q="")
    return {
        "ok": True,
        "import_id": import_id,
        "source": detected,
        "filename": filename,
        "target_instance": instance,
        "validation": validation,
        "summary": _summary(rows),
        "rows": _public_rows(filtered, 200, 0),
        "total": len(rows),
        "filtered_total": len(filtered),
    }


@router.get("/preview/{import_id}", response_class=JSONResponse)
def api_import_preview_rows(
    import_id: str,
    features: str = Query("history,ratings,watchlist"),
    media_types: str = Query("movie,show,season,episode"),
    status: str = Query("all", pattern="^(all|ready|exists|duplicate|missing_identity|unsupported|invalid)$"),
    q: str = Query(""),
    limit: int = Query(100, ge=1, le=MAX_PREVIEW_ROWS),
    offset: int = Query(0, ge=0),
    request: Request = cast(Request, None),
) -> dict[str, Any]:
    _clean_cache()
    preview = _PREVIEWS.get(import_id)
    if not preview:
        raise _api_error(ERROR_PREVIEW_NOT_FOUND, status_code=404, detail="The import preview expired.")
    cfg = load_config() or {}
    if not user_can_access_instance(cfg, request_user(request), "CROSSWATCH", preview.get("target_instance") or "default"):
        raise _api_error(ERROR_TARGET_UNAVAILABLE, status_code=403, detail="profile_scope_denied")
    feature_set = {f.strip() for f in features.split(",") if f.strip() in FEATURES}
    media_set = {m.strip() for m in media_types.split(",") if m.strip() in MEDIA_TYPES}
    base_rows = _filtered_rows(preview.get("rows") or [], features=feature_set, media_types=media_set, status="all", q=q)
    rows = base_rows if status == "all" else [row for row in base_rows if str(row.get("status") or "") == status]
    return {
        "ok": True,
        "import_id": import_id,
        "summary": _summary(base_rows),
        "rows": _public_rows(rows, limit, offset),
        "total": len(base_rows),
        "filtered_total": len(rows),
        "offset": offset,
        "limit": limit,
    }


@router.post("/commit", response_class=JSONResponse)
def api_import_commit(payload: ImportCommitRequest = Body(...), request: Request = cast(Request, None)) -> dict[str, Any]:
    _clean_cache()
    preview = _PREVIEWS.get(payload.import_id)
    if not preview:
        raise _api_error(ERROR_PREVIEW_NOT_FOUND, status_code=404, detail="The import preview expired.")

    ops = load_sync_ops("CROSSWATCH")
    if not ops:
        raise _api_error(ERROR_TARGET_UNAVAILABLE, status_code=503, detail="CrossWatch tracker is unavailable.")

    cfg = load_config() or {}
    instance = normalize_instance_id(payload.target_instance or preview.get("target_instance") or "default")
    if not user_can_access_instance(cfg, request_user(request), "CROSSWATCH", instance):
        raise _api_error(ERROR_TARGET_UNAVAILABLE, status_code=403, detail="profile_scope_denied")
    if not _target_connected(cfg, instance):
        raise _api_error(ERROR_TARGET_UNAVAILABLE, status_code=503, detail="Connect the CrossWatch tracker before importing.")
    cfg_view = build_provider_config_view(cfg, "CROSSWATCH", instance)
    wanted_features = {str(f).strip() for f in payload.features if str(f).strip() in FEATURES}
    wanted_media = {str(m).strip() for m in payload.media_types if str(m).strip() in MEDIA_TYPES}
    selected = {str(x).strip() for x in payload.row_ids if str(x).strip()}

    rows: list[Mapping[str, Any]] = []
    for row in preview.get("rows") or []:
        if str(row.get("feature") or "") not in wanted_features:
            continue
        if str(row.get("media_type") or "") not in wanted_media:
            continue
        if payload.mode == "selected" and str(row.get("id") or "") not in selected:
            continue
        status = str(row.get("status") or "")
        if status != STATUS_READY and not (status == STATUS_EXISTS and payload.include_existing):
            continue
        rows.append(row)

    grouped: dict[str, list[Mapping[str, Any]]] = {f: [] for f in FEATURES}
    skipped = 0
    for row in rows:
        item = row.get("item")
        feature = str(row.get("feature") or "")
        if not isinstance(item, Mapping) or feature not in FEATURES:
            skipped += 1
            continue
        grouped[feature].append(item)

    results: dict[str, Any] = {}
    applied = 0
    unresolved_total = 0
    for feature in ("watchlist", "history", "ratings"):
        items = grouped.get(feature) or []
        if not items:
            continue
        view = dict(cfg_view)
        if feature == "history":
            view["_cw_history_rewatches"] = True
        res = ops.add(view, items, feature=feature)
        result = dict(res) if isinstance(res, Mapping) else {"ok": bool(res)}
        unresolved = result.get("unresolved")
        applied += int(result.get("count") or 0)
        unresolved_total += len(unresolved) if isinstance(unresolved, (list, tuple)) else 0
        results[feature] = result

    return {
        "ok": unresolved_total == 0,
        "import_id": payload.import_id,
        "target_instance": instance,
        "applied": applied,
        "skipped": skipped,
        "results": results,
    }
