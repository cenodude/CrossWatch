# /providers/sync/plex/_recovery_db.py
# Plex Module for removed-item history recovery from the library database
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable, Iterator, Mapping
import zipfile

from cw_platform.id_map import ids_from_guid

from . import _common as common, _history as history, _recovery as recovery

SQLITE_HEADER = b"SQLite format 3\x00"
MAX_DATABASE_BYTES = 16 * 1024 * 1024 * 1024
CHUNK = 1024 * 1024
REQUIRED_TABLES = {"metadata_item_views", "metadata_items", "accounts"}
REQUIRED_COLUMNS = {"id", "account_id", "guid", "metadata_type", "title", "viewed_at"}
NOT_A_DATABASE = "No Plex library database was found in this file."
TOO_LARGE = "The Plex database is too large to recover."
TOO_SLOW = "The Plex database took too long to read."
TOO_MANY_ITEMS = "The Plex library in this database is too large to check."
CHECK_SECONDS = 120
SCAN_SECONDS = 30 * 60
BATCH_ROWS = 500
MAX_TEXT = 1024
MAX_ACCOUNTS = 500
MAX_LIBRARY_TOKENS = 4_000_000

_EXTERNAL = ("tmdb", "imdb", "tvdb")
_KNOWN = "Known identity"
_GUID = "Matched by Plex GUID"
_MISSING = "Missing identity — enter a match"
_HAMA_TVDB = re.compile(r"^com\.plexapp\.agents\.hama://tvdb-(\d+)(?:/(\d+)/(\d+))?$", re.IGNORECASE)
_AGENT_PATH = re.compile(r"^com\.plexapp\.agents\.[a-z]+://[^/]+/(\d+)/(\d+)$", re.IGNORECASE)


class RecoveryError(Exception):
    pass


def _int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _year(value: Any) -> int | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(int(value) + 43200, tz=timezone.utc).year
        except (OverflowError, OSError, ValueError):
            return None
    match = re.match(r"^(\d{4})-", str(value or "").strip())
    return int(match.group(1)) if match else None


def _clean(guid: Any) -> str:
    return str(guid or "").strip().split("?", 1)[0].split("#", 1)[0]


def _external(ids: Mapping[str, Any] | None) -> dict[str, str]:
    return {key: str(ids[key]) for key in _EXTERNAL if ids and ids.get(key)}


def _local_ids(guid: str) -> tuple[dict[str, str], int | None, int | None]:
    hama = _HAMA_TVDB.match(guid)
    if hama:
        return {"tvdb": hama.group(1)}, _int(hama.group(2)), _int(hama.group(3))
    if guid.lower().startswith(("plex://", "com.plexapp.agents.hama://")):
        return {}, None, None
    path = _AGENT_PATH.match(guid)
    return _external(ids_from_guid(guid)), _int(path.group(1)) if path else None, _int(path.group(2)) if path else None


def _connect(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"{Path(os.path.abspath(path)).as_uri()}?mode=ro&immutable=1", uri=True, check_same_thread=False)
    con.setconfig(sqlite3.SQLITE_DBCONFIG_DEFENSIVE, True)
    con.setconfig(sqlite3.SQLITE_DBCONFIG_TRUSTED_SCHEMA, False)
    con.execute("PRAGMA cell_size_check=ON")
    con.execute("PRAGMA mmap_size=0")
    return con


class _Clock:
    def __init__(self, seconds: float) -> None:
        self.left, self.since = seconds, None

    def __enter__(self) -> _Clock:
        self.since = time.monotonic()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.left -= time.monotonic() - self.since
        self.since = None

    def expired(self) -> bool:
        return self.left - (time.monotonic() - self.since if self.since is not None else 0) <= 0


@contextmanager
def _reading(path: Path, seconds: float, check_cancel: Callable[[], None] = lambda: None) -> Iterator[tuple[sqlite3.Connection, _Clock]]:
    clock = _Clock(seconds)

    def stop() -> int:
        if clock.expired():
            return 1
        try:
            check_cancel()
        except InterruptedError:
            return 1
        return 0

    with closing(_connect(path)) as con:
        con.set_progress_handler(stop, 100_000)
        try:
            yield con, clock
        except sqlite3.OperationalError:
            check_cancel()
            if clock.expired():
                raise RecoveryError(TOO_SLOW)
            raise


def _tables(con: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _is_library(path: Path) -> bool:
    try:
        with _reading(path, CHECK_SECONDS) as (con, clock), clock:
            if not REQUIRED_TABLES <= _tables(con):
                return False
            return REQUIRED_COLUMNS <= {str(row[1]) for row in con.execute("PRAGMA table_info(metadata_item_views)")}
    except sqlite3.DatabaseError:
        return False


def _extract(archive: zipfile.ZipFile, info: zipfile.ZipInfo, target: Path, check_cancel: Callable[[], None]) -> None:
    total = 0
    with archive.open(info) as source, open(target, "wb") as out:
        while chunk := source.read(CHUNK):
            check_cancel()
            total += len(chunk)
            if total > MAX_DATABASE_BYTES:
                raise RecoveryError(TOO_LARGE)
            out.write(chunk)


def prepare(source: Path, folder: Path, *, check_cancel: Callable[[], None] = lambda: None) -> Path:
    with open(source, "rb") as handle:
        head = handle.read(len(SQLITE_HEADER))
    if head == SQLITE_HEADER:
        if _is_library(source):
            return source
        raise RecoveryError(NOT_A_DATABASE)
    if not zipfile.is_zipfile(source):
        raise RecoveryError(NOT_A_DATABASE)
    with zipfile.ZipFile(source) as archive:
        for index, info in enumerate(archive.infolist()):
            if info.is_dir() or info.flag_bits & 0x1 or info.file_size < len(SQLITE_HEADER):
                continue
            with archive.open(info) as member:
                if member.read(len(SQLITE_HEADER)) != SQLITE_HEADER:
                    continue
            target = Path(folder) / f"database-{index}.db"
            _extract(archive, info, target, check_cancel)
            if _is_library(target):
                return target
            target.unlink(missing_ok=True)
    raise RecoveryError(NOT_A_DATABASE)


def download(adapter: Any, folder: Path, *, progress: Callable[..., None], check_cancel: Callable[[], None]) -> Path:
    server = adapter.client.server
    if server is None:
        raise RuntimeError("Connect a Plex server before recovering history.")
    base, token = common._as_base_url(server), common.active_pms_token(server)
    session = getattr(server, "_session", None)
    if not (base and token and session):
        raise RuntimeError("Connect a Plex server before recovering history.")
    target = Path(folder) / "server-database.zip"
    progress("download", "Downloading the Plex database (MB)", 0, 0)
    with session.get(f"{base}/diagnostics/databases", headers=common.plex_headers(token), stream=True, timeout=(15, 600)) as response:
        if response.status_code in (401, 403):
            raise RecoveryError("Plex server owner access is required to download the database.")
        response.raise_for_status()
        size = _int(response.headers.get("Content-Length")) or 0
        done = 0
        with open(target, "wb") as out:
            for chunk in response.iter_content(CHUNK):
                check_cancel()
                done += len(chunk)
                if done > MAX_DATABASE_BYTES:
                    raise RecoveryError(TOO_LARGE)
                out.write(chunk)
                progress("download", "Downloading the Plex database (MB)", done >> 20, size >> 20)
    progress("download", "Unpacking the Plex database", 0, 0)
    path = prepare(target, Path(folder), check_cancel=check_cancel)
    if path != target:
        target.unlink(missing_ok=True)
    return path


def accounts(path: Path) -> list[dict[str, Any]]:
    with _reading(path, CHECK_SECONDS) as (con, clock), clock:
        rows = con.execute(
            f"SELECT v.account_id, substr(a.name, 1, {MAX_TEXT}), v.plays FROM (SELECT CAST(account_id AS INTEGER) AS account_id, "
            "COUNT(*) AS plays FROM metadata_item_views WHERE metadata_type IN (1, 4) AND account_id IS NOT NULL GROUP BY 1) v "
            "LEFT JOIN (SELECT id, MIN(name) AS name FROM accounts GROUP BY id) a ON a.id = v.account_id "
            f"ORDER BY v.plays DESC, v.account_id LIMIT {MAX_ACCOUNTS}"
        ).fetchall()
    return [dict(id=int(aid), name=str(name or "").strip() or f"Account {aid}", plays=int(count)) for aid, name, count in rows]


def _current(con: sqlite3.Connection, tables: set[str], check_cancel: Callable[[], None]) -> set[str]:
    tokens: set[str] = set()

    def add(kind: str, ids: Mapping[str, str], suffix: str = "") -> None:
        check_cancel()
        tokens.update(f"{kind}:{key}:{value}{suffix}" for key, value in ids.items())
        if len(tokens) > MAX_LIBRARY_TOKENS:
            raise RecoveryError(TOO_MANY_ITEMS)

    tagged = {"taggings", "tags"} <= tables
    if tagged:
        for (tag,) in con.execute(
            f"SELECT substr(t.tag, 1, {MAX_TEXT}) FROM metadata_items m JOIN taggings g ON g.metadata_item_id = m.id "
            "JOIN tags t ON t.id = g.tag_id WHERE m.metadata_type = 1 AND t.tag_type = 314"
        ):
            add("movie", _external(ids_from_guid(str(tag or ""))))
    for (guid,) in con.execute(f"SELECT substr(guid, 1, {MAX_TEXT}) FROM metadata_items WHERE metadata_type = 1"):
        add("movie", _local_ids(_clean(guid))[0])
    if tagged:
        for tag, season, episode in con.execute(
            f'SELECT substr(t.tag, 1, {MAX_TEXT}), CAST(s."index" AS INTEGER), CAST(e."index" AS INTEGER) FROM metadata_items e '
            "JOIN metadata_items s ON s.id = e.parent_id JOIN taggings g ON g.metadata_item_id = s.parent_id "
            "JOIN tags t ON t.id = g.tag_id WHERE e.metadata_type = 4 AND t.tag_type = 314"
        ):
            add("episode", _external(ids_from_guid(str(tag or ""))), f"#{season}:{episode}")
    for guid, season, episode in con.execute(
        f'SELECT substr(sh.guid, 1, {MAX_TEXT}), CAST(s."index" AS INTEGER), CAST(e."index" AS INTEGER) FROM metadata_items e '
        "JOIN metadata_items s ON s.id = e.parent_id JOIN metadata_items sh ON sh.id = s.parent_id WHERE e.metadata_type = 4"
    ):
        add("episode", _local_ids(_clean(guid))[0], f"#{season}:{episode}")
    return tokens


def _label(kind: str, title: str, show: str, season: int | None, episode: int | None, year: int | None) -> str:
    if kind == "episode":
        return f"{show or title} · S{season if season is not None else '?'} E{episode if episode is not None else '?'} · {title}"
    return f"{title} ({year})" if year else title


def _scan(con: sqlite3.Connection, clock: _Clock, *, account: int, libraries: set[str],
          resolve: Callable[[str, str], Mapping[str, Any]], progress: Callable[..., None],
          check_cancel: Callable[[], None], max_rows: int) -> list[dict[str, Any]]:
    progress("libraries", "Checking the Plex library in the database", 0, 0)
    with clock:
        tables = _tables(con)
        current = _current(con, tables, check_cancel)
        sections = {str(row[0]) for row in con.execute(f"SELECT substr(id, 1, {MAX_TEXT}) FROM library_sections")} if "library_sections" in tables else set()
        columns = {str(row[1]) for row in con.execute("PRAGMA table_info(metadata_item_views)")}

    def text(name: str) -> str:
        return f'substr(v."{name}", 1, {MAX_TEXT})' if name in columns else "NULL"

    def number(name: str) -> str:
        return f'CAST(v."{name}" AS INTEGER)' if name in columns else "NULL"

    source = ("FROM metadata_item_views v WHERE v.account_id = ? AND v.metadata_type IN (1, 4) "
              "AND NOT EXISTS (SELECT 1 FROM metadata_items m WHERE m.guid = v.guid) GROUP BY v.guid, v.viewed_at")
    progress("history", "Reading Plex watch history from the database", 0, 0)
    with clock:
        total = int(con.execute(f"SELECT COUNT(*) FROM (SELECT 1 {source})", (int(account),)).fetchone()[0])
        cursor = con.execute(
            f"SELECT {text('guid')}, CAST(v.metadata_type AS INTEGER), {text('title')}, {text('grandparent_title')}, "
            f"{text('grandparent_guid')}, {number('parent_index')}, {number('index')}, CAST(v.viewed_at AS INTEGER), "
            f"{number('library_section_id')}, {number('originally_available_at')}, CAST(MIN(v.id) AS INTEGER) {source} "
            "ORDER BY v.viewed_at, MIN(v.id)",
            (int(account),),
        )

    def rows() -> Iterator[tuple[Any, ...]]:
        while True:
            with clock:
                batch = cursor.fetchmany(BATCH_ROWS)
            if not batch:
                return
            yield from batch

    cloud: dict[tuple[str, str], dict[str, str]] = {}
    shows: dict[str, tuple[dict[str, str], str]] = {}
    pending: list[tuple[dict[str, Any], str]] = []
    recovered: list[dict[str, Any]] = []
    skipped = 0

    def report(checked: int, title: str = "", action: str = "", result: str = "") -> None:
        activity: dict[str, Any] = dict(checked=checked, found=len(recovered), skipped=skipped,
                                        current_item=title, current_action=action)
        if result and title:
            activity["event"] = dict(title=title, result=result)
        progress("matching", "Matching removed-item history", checked, total, activity)

    def lookup(kind: str, guid: str) -> dict[str, str]:
        if not guid.lower().startswith("plex://"):
            return {}
        if (kind, guid) not in cloud:
            cloud[(kind, guid)] = _external(resolve(kind, guid))
        return cloud[(kind, guid)]

    report(0)
    for index, (raw_guid, media_type, title, show_title, raw_show, season, episode, viewed_at, section, aired, _) in enumerate(rows()):
        check_cancel()
        guid, show_guid = _clean(raw_guid), _clean(raw_show)
        kind = "movie" if media_type == 1 else "episode"
        title, show_title = str(title or "").strip(), str(show_title or "").strip()
        out_of_scope = bool(libraries) and section is not None and str(section) in sections and str(section) not in libraries
        if not guid or "/edition/trailer" in guid.lower() or out_of_scope:
            skipped += 1
            report(index + 1)
            continue
        method = _KNOWN
        if kind == "movie":
            year = _year(aired)
            label = _label(kind, title, "", None, None, year)
            group = guid.split("/edition/", 1)[0]
            ids = _local_ids(guid)[0]
            if not ids:
                report(index, label, "Looking up identity at Plex")
                ids, method = lookup("movie", group), _GUID
            original = title
            item: dict[str, Any] = {"type": "movie", "title": title, "year": year, "ids": dict(ids)}
            identity = bool(ids)
        else:
            year = None
            local, guid_season, guid_episode = _local_ids(guid)
            season = _int(season) if _int(season) is not None else guid_season
            episode = _int(episode) if _int(episode) is not None else guid_episode
            label = _label(kind, title, show_title, season, episode, None)
            group = show_guid
            ids = local or _local_ids(show_guid)[0]
            if not ids and group in shows:
                ids, method = shows[group]
            if not ids:
                report(index, label, "Looking up identity at Plex")
                ids, method = lookup("show", show_guid) or lookup("episode", guid), _GUID
            if ids and group:
                shows.setdefault(group, (ids, method))
            original = show_title or title
            code = f"S{season:02d}E{episode:02d}" if season is not None and episode is not None else original
            item = {"type": "episode", "title": code, "series_title": original, "year": None,
                    "season": season, "episode": episode, "ids": {}, "show_ids": dict(ids)}
            identity = bool(ids) and season is not None and episode is not None
        if ids and recovery._tokens(item) & current:
            skipped += 1
            report(index + 1, label, result="Skipped: still in Plex")
            continue
        item["watched_at"] = history._iso(int(viewed_at)) if _int(viewed_at) is not None else None
        item["watched"] = True
        entry = {"feature": "history", "item": item, "source_path": "Plex database",
                 "recovery_match": method if identity else _MISSING, "recovery_valid": identity,
                 "recovery_requires_review": False,
                 "original_group": json.dumps(["show" if kind == "episode" else "movie", group, [],
                                               original.casefold(), str(year or "")], ensure_ascii=True),
                 "original_year": year, "original_title": original}
        recovered.append(entry)
        if len(recovered) > max_rows:
            raise RuntimeError(f"More than {max_rows:,} removed watch events. Narrow the configured history library scope.")
        if not ids and kind == "episode" and group:
            pending.append((entry, group))
        result = "Found for review"
        if not identity:
            result = "Found: needs a match"
        elif not item["watched_at"]:
            result = "Found: missing watch date"
        report(index + 1, label, result=result)
    present: set[int] = set()
    for entry, group in pending:
        if group not in shows:
            continue
        ids, method = shows[group]
        item = entry["item"]
        item["show_ids"] = dict(ids)
        if recovery._tokens(item) & current:
            present.add(id(entry))
        elif item.get("season") is not None and item.get("episode") is not None:
            entry.update(recovery_valid=True, recovery_match=method)
    return [entry for entry in recovered if id(entry) not in present]


def scan(path: Path, *, account: int, libraries: set[str], resolve: Callable[[str, str], Mapping[str, Any]],
         progress: Callable[..., None], check_cancel: Callable[[], None], max_rows: int = 50_000) -> list[dict[str, Any]]:
    check_cancel()
    with _reading(path, SCAN_SECONDS, check_cancel) as (con, clock):
        return _scan(con, clock, account=account, libraries={str(x) for x in libraries}, resolve=resolve,
                     progress=progress, check_cancel=check_cancel, max_rows=max_rows)
