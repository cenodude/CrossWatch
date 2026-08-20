# providers/scrobble/media_filters.py
# CrossWatch - Scrobble media-level filters
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None


DEFAULT_AGREGARR_FILENAME_PATTERNS = ("{edition-trailer}",)
DEFAULT_AGREGARR_EDITIONS = ("trailer",)
DEFAULT_AGREGARR_MARKER_FILES = (".comingsoon",)

_PATH_KEYS = {
    "path",
    "file",
    "filename",
    "filepath",
    "file_path",
    "mediafile",
    "media_file",
    "_cw_file_path",
    "_cw_file_paths",
}
_EDITION_KEYS = {
    "edition",
    "editiontitle",
    "edition_title",
    "version",
    "versiontitle",
    "version_title",
    "_cw_edition_title",
}


def _log(msg: str, lvl: str = "DEBUG") -> None:
    if BASE_LOG:
        try:
            BASE_LOG(str(msg), level=lvl, module="SCROBBLE")
            return
        except Exception:
            pass


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    items = value if isinstance(value, (list, tuple, set)) else [value]
    out: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _norm_path(value: Any) -> str:
    text = str(value or "").strip().replace("\\", "/")
    while "//" in text:
        text = text.replace("//", "/")
    return text.rstrip("/").lower()


def _basename(value: str) -> str:
    text = str(value or "").strip().replace("\\", "/").rstrip("/")
    return text.rsplit("/", 1)[-1].lower()


def _walk_media_values(raw: Any) -> tuple[list[str], list[str]]:
    paths: list[str] = []
    editions: list[str] = []

    def add_path(value: Any) -> None:
        text = str(value or "").strip()
        if text:
            paths.append(text)

    def add_edition(value: Any) -> None:
        text = str(value or "").strip()
        if text:
            editions.append(text)

    def add_many(value: Any, add_fn: Any) -> None:
        if isinstance(value, Mapping):
            walk(value)
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                add_many(item, add_fn)
        else:
            add_fn(value)

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                key_lc = str(key or "").strip().lower()
                if key_lc in _PATH_KEYS:
                    add_many(item, add_path)
                    continue
                if key_lc in _EDITION_KEYS:
                    add_many(item, add_edition)
                    continue
                walk(item)
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                walk(item)

    walk(raw)
    return paths, editions


def _path_matches_prefix(paths: list[str], prefixes: list[str]) -> str | None:
    norm_prefixes = [_norm_path(prefix) for prefix in prefixes if _norm_path(prefix)]
    if not norm_prefixes:
        return None
    for path in paths:
        got = _norm_path(path)
        if not got:
            continue
        for prefix in norm_prefixes:
            if got == prefix or got.startswith(prefix + "/"):
                return f"path_prefix:{prefix}"
    return None


def _path_matches_pattern(paths: list[str], patterns: list[str]) -> str | None:
    needles = [str(pattern or "").strip().lower() for pattern in patterns if str(pattern or "").strip()]
    if not needles:
        return None
    for path in paths:
        haystacks = [_norm_path(path), _basename(path)]
        for needle in needles:
            if any(needle in hay for hay in haystacks):
                return f"filename_pattern:{needle}"
    return None


def _edition_matches(editions: list[str], blocked: list[str]) -> str | None:
    blocked_lc = {str(item or "").strip().lower() for item in blocked if str(item or "").strip()}
    if not blocked_lc:
        return None
    for edition in editions:
        got = str(edition or "").strip().lower()
        if got and got in blocked_lc:
            return f"edition:{got}"
    return None


def _marker_file_exists(paths: list[str], markers: list[str]) -> str | None:
    marker_names = [str(marker or "").strip() for marker in markers if str(marker or "").strip()]
    if not marker_names:
        return None
    for raw_path in paths:
        text = str(raw_path or "").strip()
        if not text or "://" in text:
            continue
        try:
            parent = Path(text).parent
            if not str(parent) or str(parent) == ".":
                continue
            for marker in marker_names:
                if (parent / marker).exists():
                    return f"marker_file:{marker}"
        except Exception:
            continue
    return None


def media_filter_ignore_reason(
    filters: Mapping[str, Any] | None,
    raw: Mapping[str, Any] | None,
    *,
    title: Any = None,
) -> str | None:
    filt = filters if isinstance(filters, Mapping) else {}
    raw_map = raw if isinstance(raw, Mapping) else {}
    paths, editions = _walk_media_values(raw_map)

    custom_prefixes = _as_list(filt.get("ignored_path_prefixes"))
    custom_patterns = _as_list(filt.get("ignored_filename_patterns"))
    custom_editions = _as_list(filt.get("ignored_editions"))
    custom_markers = _as_list(filt.get("ignored_marker_files"))

    agregarr_on = bool(filt.get("ignore_agregarr_trailers"))
    if agregarr_on:
        custom_patterns = [*DEFAULT_AGREGARR_FILENAME_PATTERNS, *custom_patterns]
        custom_editions = [*DEFAULT_AGREGARR_EDITIONS, *custom_editions]
        custom_markers = [*DEFAULT_AGREGARR_MARKER_FILES, *custom_markers]

    reason = _path_matches_prefix(paths, custom_prefixes)
    if reason:
        return reason
    reason = _path_matches_pattern(paths, custom_patterns)
    if reason:
        return reason
    reason = _edition_matches(editions, custom_editions)
    if reason:
        return reason
    reason = _marker_file_exists(paths, custom_markers)
    if reason:
        return reason

    return None


def event_ignore_reason(event: Any, cfg: Mapping[str, Any] | None) -> str | None:
    cfg_map = cfg if isinstance(cfg, Mapping) else {}
    filt = (((cfg_map.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
    if not isinstance(filt, Mapping):
        return None
    return media_filter_ignore_reason(filt, getattr(event, "raw", None), title=getattr(event, "title", None))


def log_media_filter_drop(event: Any, reason: str) -> None:
    title = str(getattr(event, "title", "") or "?")
    session = str(getattr(event, "session_key", "") or "?")
    _log(f"event filtered by media filter: reason={reason} title={title!r} sess={session}", "DEBUG")
