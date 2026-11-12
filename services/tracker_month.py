# services/tracker_month.py
# CrossWatch - Persistent monthly and yearly watch reviews with idle background generation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from cw_platform.tracker_store import TrackerStore, tracker_roots
from cw_platform.tracker_automation import root_options, instance_options

_VERSION = 3
_LOG = logging.getLogger("crosswatch.tracker_month")
_LOCK = threading.Lock()
_PUBLICATION = threading.Lock()
_PENDING: set[tuple[str, str, str]] = set()
_FAILURES: dict[tuple[str, str, str], float] = {}
_RECENT: OrderedDict[tuple[str, str, str], Any] = OrderedDict()
_RECENT_SIZE = 0
_POOL: ThreadPoolExecutor | None = None
_LIFECYCLE = threading.Lock()
_STOP = threading.Event()
_WORK = threading.local()
_LOAD_CONFIG = None
_THREAD: threading.Thread | None = None


def review_timezone(cfg: Mapping[str, Any]) -> Any:
    from services.scheduling import _display_tz_from_cfg

    return _display_tz_from_cfg(dict(cfg.get("scheduling") or {})) or datetime.now().astimezone().tzinfo or timezone.utc


def month_bounds(month: str, tz: Any) -> tuple[datetime, datetime]:
    start = datetime.strptime(month, "%Y-%m").replace(tzinfo=tz)
    if start.strftime("%Y-%m") != month or start.year < 1900:
        raise ValueError("invalid_month")
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return start, end


def previous_month(now: datetime) -> str:
    return (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")


def period_bounds(period: str, tz: Any) -> tuple[datetime, datetime]:
    if len(period) != 4:
        return month_bounds(period, tz)
    if not period.isascii() or not period.isdecimal() or not 1900 <= int(period) < 9999:
        raise ValueError("invalid_year")
    return datetime(int(period), 1, 1, tzinfo=tz), datetime(int(period) + 1, 1, 1, tzinfo=tz)


def _option(period: str) -> str:
    return "wrapped" if len(period) == 4 else "month_review"


def _current(period: str, now: datetime) -> str:
    return now.strftime("%Y" if len(period) == 4 else "%Y-%m")


def _recent(period: str, now: datetime) -> bool:
    return period >= (str(now.year) if len(period) == 4 else previous_month(now))


def _scheduled_at(now: datetime) -> datetime:
    scheduled = now.replace(hour=5, minute=0, second=0, microsecond=0)
    return scheduled if now >= scheduled else scheduled - timedelta(days=1)


def _cache(store: TrackerStore) -> sqlite3.Connection:
    store.root.mkdir(parents=True, exist_ok=True)
    path = store.root / "month-reviews.db"
    conn = sqlite3.connect(path, timeout=.05)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE IF NOT EXISTS reviews(month TEXT, zone TEXT, version INTEGER, revision TEXT, generated REAL, data TEXT, PRIMARY KEY(month, zone))")
        conn.execute("CREATE TABLE IF NOT EXISTS review_state(month TEXT, zone TEXT, revision TEXT, PRIMARY KEY(month, zone))")
        return conn
    except sqlite3.DatabaseError as exc:
        conn.close()
        if getattr(exc, "sqlite_errorcode", None) not in (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB):
            raise
        suffix = f".corrupt-{time.time_ns()}"
        for target in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
            if target.exists():
                target.rename(target.with_name(target.name + suffix))
        return _cache(store)


def _readonly(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0)


def _saved(store: TrackerStore, month: str, tz: Any) -> Any:
    if not (store.root / "month-reviews.db").is_file():
        _remember(store, month, tz, None)
        return None
    with closing(_readonly(store.root / "month-reviews.db")) as conn:
        saved = _saved_row(conn, month, tz)
    _remember(store, month, tz, saved)
    return saved


def _remember(store: TrackerStore, month: str, tz: Any, saved: Any) -> None:
    global _RECENT_SIZE
    key = (str(store.root.resolve()), month, str(tz))
    with _LOCK:
        previous = _RECENT.pop(key, None)
        if previous:
            _RECENT_SIZE -= len(previous[3]) * 4
        if saved and isinstance(saved[3], str):
            _RECENT[key] = saved
            _RECENT_SIZE += len(saved[3]) * 4
        while len(_RECENT) > 16 or _RECENT_SIZE > 16 * 1024 * 1024:
            _, removed = _RECENT.popitem(last=False)
            _RECENT_SIZE -= len(removed[3]) * 4


def _saved_row(conn: sqlite3.Connection, month: str, tz: Any) -> Any:
    return conn.execute("SELECT r.version,coalesce(s.revision,r.revision),r.generated,r.data FROM reviews r LEFT JOIN review_state s ON s.month=r.month AND s.zone=r.zone WHERE r.month=? AND r.zone=?", (month, str(tz))).fetchone()


def _revision(store: TrackerStore, month: str, tz: Any) -> str:
    if not store.db_path.is_file():
        return ""
    with closing(_readonly(store.db_path)) as conn:
        start, end = period_bounds(month, tz)
        first = (start.astimezone(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        last = (end.astimezone(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
        rows = conn.execute("SELECT day,revision FROM history_revisions WHERE day='' OR (day>=? AND day<?) ORDER BY day", (first, last)).fetchall()
        return json.dumps(rows)


def _checkpoint() -> None:
    if _STOP.is_set() or _busy():
        raise InterruptedError("monthly_review_paused")
    root = getattr(_WORK, "root", None)
    if root is not None and _LOAD_CONFIG is not None and time.monotonic() >= getattr(_WORK, "checked", 0):
        _WORK.checked = time.monotonic() + 1
        if not root_options(_LOAD_CONFIG() or {}, root)[getattr(_WORK, "option", "month_review")]:
            raise InterruptedError("monthly_review_disabled")


def _rows(store: TrackerStore, month: str, tz: Any):
    start, end = period_bounds(month, tz)
    min_date = (start.astimezone(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    max_date = (end.astimezone(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    if not store.db_path.is_file():
        return
    with closing(_readonly(store.db_path)) as conn:
        conn.set_progress_handler(lambda: int(_STOP.is_set() or _busy()), 2000)
        cursor = conn.execute(
            "SELECT i.item_key, i.data, i.when_at, i.kind, i.show_key, c.runtime FROM items i"
            " LEFT JOIN checks c ON c.scope=i.scope AND c.feature=i.feature AND c.item_key=i.item_key"
            " WHERE i.scope='' AND i.feature='history' AND i.when_at>=? AND i.when_at<?"
            " AND i.kind IN ('movie','episode') ORDER BY i.when_at,i.item_key", (min_date, max_date),
        )
        for row in cursor:
            _checkpoint()
            yield row


def _dependencies(store: TrackerStore, month: str, tz: Any, locale: str) -> str:
    from api.metaAPI import _meta_cache_path

    digest = hashlib.sha256(locale.encode())
    identifiers = set()
    start, end = period_bounds(month, tz)
    for key, raw, stamp, kind, _, runtime in _rows(store, month, tz):
        try:
            moment = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
            item = json.loads(raw)
            if not start <= moment < end or not isinstance(item, dict):
                continue
            ids = item.get("ids" if kind == "movie" else "show_ids") or {}
            identifier = str(ids.get("tmdb") or "")
        except (ValueError, TypeError):
            continue
        digest.update(f"{key}\0{raw}\0{runtime}".encode())
        if identifier.isascii() and identifier.isdecimal():
            identifiers.add(("movie" if kind == "movie" else "show", identifier))
    for kind, identifier in sorted(identifiers):
        _checkpoint()
        try:
            stat = _meta_cache_path(kind, identifier, locale).stat()
            token = (stat.st_mtime_ns, stat.st_size)
        except FileNotFoundError:
            token = None
        digest.update(f"{kind}:{identifier}:{token}".encode())
    return digest.hexdigest()


def _metadata(kind: str, identifier: str, locale: str | None = None) -> dict[str, Any]:
    from api.metaAPI import _cfg_ui_locale, _read_meta_cache

    if not identifier.isascii() or not identifier.isdecimal():
        return {}
    return _read_meta_cache(kind, identifier, locale or _cfg_ui_locale() or "en-US") or {}


def _image(meta: Mapping[str, Any], kind: str) -> str:
    rows = (meta.get("images") or {}).get(kind) or []
    for row in rows:
        url = str(row.get("url") or "") if isinstance(row, dict) else ""
        if url.startswith("https://image.tmdb.org/t/p/"):
            return url
    return ""


def build_report(store: TrackerStore, month: str, tz: Any, *, metadata: Callable | None = None, now: datetime | None = None, locale: str = "en-US") -> dict[str, Any]:
    metadata = metadata or (lambda kind, identifier: _metadata(kind, identifier, locale))
    start, end = period_bounds(month, tz)
    now = (now or datetime.now(timezone.utc)).astimezone(tz)
    yearly = len(month) == 4
    days = [{"day": day + 1, "date": (start + timedelta(days=day)).strftime("%Y-%m-%d"), "movies": 0, "episodes": 0, "minutes": 0, "movie_minutes": 0, "show_minutes": 0, "titles": []} for day in range((end.date() - start.date()).days)]
    months = [{"month": f"{start.year}-{number:02d}", "plays": 0, "minutes": 0} for number in range(1, 13)]
    hours, weekdays = [0] * 24, [0] * 7
    titles: dict[str, dict[str, Any]] = {}
    genres: dict[str, set[str]] = {}
    first = last = None
    plays = timed = 0
    digest = hashlib.sha256()
    for key, raw, stamp, kind, show_key, runtime in _rows(store, month, tz):
        try:
            moment = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
            moment = moment.astimezone(tz)
            item = json.loads(raw)
        except (ValueError, TypeError):
            continue
        if not start <= moment < end or moment > now or not isinstance(item, dict):
            continue
        digest.update(f"{key}\0{raw}\0{runtime}".encode())
        movie = kind == "movie"
        ids = (item.get("ids") if movie else item.get("show_ids")) or {}
        identifier = str(ids.get("tmdb") or "")
        identity = ("movie:" if movie else "show:") + (f"tmdb:{identifier}" if identifier else str(ids.get("imdb") or show_key or key.split("@")[0]))
        if identity not in titles:
            meta = metadata("movie" if movie else "show", identifier) if identifier else {}
            name = item.get("title") if movie else item.get("series_title")
            labels = [str(g.get("name") if isinstance(g, dict) else g) for g in meta.get("genres", []) if g]
            titles[identity] = {
                "key": identity, "type": "movie" if movie else "show", "title": name or meta.get("title") or "Untitled",
                "year": item.get("year") if movie else item.get("series_year"), "ids": ids,
                "ref": {"feature": "history", "key": key}, "plays": 0, "minutes": 0, "timed": 0,
                "poster": _image(meta, "poster"), "backdrop": _image(meta, "backdrop"), "logo": _image(meta, "logo"), "replays": 0,
                "genres": labels, "runtime": meta.get("runtime_minutes"), "days": [], "episodes": set(),
            }
            for genre in labels:
                genres.setdefault(genre, set()).add(identity)
        title = titles[identity]
        try:
            duration = int(runtime or item.get("runtime_minutes") or title["runtime"] or 0)
        except (TypeError, ValueError):
            duration = 0
        duration = duration if 0 < duration < 1440 else 0
        day_index = (moment.date() - start.date()).days
        day = days[day_index]
        months[moment.month - 1]["plays"] += 1
        months[moment.month - 1]["minutes"] += duration
        hours[moment.hour] += 1
        weekdays[moment.weekday()] += 1
        day["movies" if movie else "episodes"] += 1
        day["movie_minutes" if movie else "show_minutes"] += duration
        day["minutes"] += duration
        if identity not in day["titles"]:
            day["titles"].append(identity)
        title["plays"] += 1
        title["minutes"] += duration
        title["timed"] += bool(duration)
        if day_index + 1 not in title["days"]:
            title["days"].append(day_index + 1)
        if not movie:
            episode = (item.get("season"), item.get("episode"))
            title["replays"] += None not in episode and episode in title["episodes"]
            if not yearly or None not in episode:
                title["episodes"].add(episode)
        event = {"title_key": identity, "date": moment.isoformat(), "season": item.get("season"), "episode": item.get("episode")}
        if first is None or moment < datetime.fromisoformat(first["date"]):
            first = event
        if last is None or moment > datetime.fromisoformat(last["date"]):
            last = event
        plays += 1
        timed += bool(duration)
    ordered = sorted(titles.values(), key=lambda row: (-row["plays"], -row["minutes"], str(row["title"]).casefold()))
    for row in ordered:
        if row["type"] == "movie":
            row["replays"] = max(0, row["plays"] - 1)
        row["episodes"] = len(row["episodes"])
        row.pop("runtime", None)
    streak = longest = 0
    streak_end = None
    for day in days:
        streak = streak + 1 if day["movies"] + day["episodes"] else 0
        if streak > longest:
            longest, streak_end = streak, day["date"]
    best = max(days, key=lambda d: d["movies"] + d["episodes"])
    return {
        "month": month, "timezone": str(tz), "current": start <= now < end, "fingerprint": digest.hexdigest(),
        "plays": plays, "timed": timed, "minutes": sum(d["minutes"] for d in days),
        "movies": sum(t["type"] == "movie" for t in ordered), "shows": sum(t["type"] == "show" for t in ordered),
        "active_days": sum(bool(d["movies"] + d["episodes"]) for d in days), "streak": longest,
        "elapsed_days": (now.date() - start.date()).days + 1 if start <= now < end else len(days),
        "busiest": best["day"] if plays else None, "first": first, "last": last, "days": days, "titles": ordered,
        "genres": [{"name": name, "titles": sorted(keys), "count": len(keys)} for name, keys in sorted(genres.items(), key=lambda p: (-len(p[1]), p[0]))],
        **({"year": month, "months": months, "hours": hours, "weekdays": weekdays, "streak_end": streak_end,
            "streak_start": (datetime.fromisoformat(streak_end) - timedelta(days=longest - 1)).date().isoformat() if streak_end else None,
            "replays": sum(t["replays"] for t in ordered)} if yearly else {}),
    }


def _busy() -> bool:
    try:
        from api.syncAPI import _is_sync_running

        return bool(_is_sync_running())
    except Exception:
        return False


def _publish(conn: sqlite3.Connection, month: str, tz: Any, state: str, generated: float, data: str | None) -> None:
    with _PUBLICATION:
        _checkpoint()
        with conn:
            if data is not None:
                conn.execute("INSERT OR REPLACE INTO reviews VALUES(?,?,?,?,?,?)", (month, str(tz), _VERSION, state, generated, data))
            conn.execute("INSERT OR REPLACE INTO review_state VALUES(?,?,?)", (month, str(tz), state))
            _checkpoint()


def _generate(store: TrackerStore, month: str, tz: Any, locale: str, key: tuple[str, str, str]) -> None:
    _WORK.root, _WORK.checked = store.root, 0
    _WORK.option = _option(month)
    conn = None
    try:
        while _busy():
            if _STOP.wait(5):
                return
        _checkpoint()
        with store._db():
            pass
        _checkpoint()
        conn = _cache(store)
        revision = _revision(store, month, tz)
        saved = _saved_row(conn, month, tz)
        report, state = _decode(saved)
        now = datetime.now(tz)
        if not _needs_validation(saved, report, state, revision, locale, month, now):
            return
        dependencies = _dependencies(store, month, tz, locale)
        daily = _recent(month, now) and (not saved or saved[2] < _scheduled_at(now).timestamp())
        changed = not report or state.get("dependencies") != dependencies or daily or bool(report.get("current") and month != _current(month, now))
        if changed:
            report = build_report(store, month, tz, locale=locale)
        _checkpoint()
        if (changed or _revision(store, month, tz) != revision) and _dependencies(store, month, tz, locale) != dependencies:
            return
        generated = time.time() if changed else saved[2]
        state = json.dumps({"history": revision, "locale": locale, "checked": time.time(), "dependencies": dependencies})
        data = json.dumps(report, ensure_ascii=False) if changed else None
        _publish(conn, month, tz, state, generated, data)
        with _LOCK:
            _FAILURES.pop(key, None)
    except InterruptedError:
        pass
    except Exception as exc:
        if isinstance(exc, OSError) and getattr(exc, "winerror", None) in (32, 33):
            return
        if isinstance(exc, sqlite3.Error) and (getattr(exc, "sqlite_errorcode", 0) & 255) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_INTERRUPT):
            return
        if not _STOP.is_set() and not _busy():
            _LOG.exception("Could not prepare monthly review for %s", month)
            with _LOCK:
                _FAILURES[key] = time.time()
                if len(_FAILURES) > 64:
                    _FAILURES.pop(next(iter(_FAILURES)))
    finally:
        _WORK.root = None
        if conn is not None:
            conn.close()
        with _LOCK:
            _PENDING.discard(key)


def _decode(saved: Any) -> tuple[Any, dict[str, Any]]:
    if saved and saved[0] == _VERSION:
        try:
            report, state = json.loads(saved[3]), json.loads(saved[1])
            if isinstance(report, dict) and isinstance(state, dict):
                return report, state
        except (ValueError, TypeError):
            pass
    return None, {}


def _needs_validation(saved: Any, report: Any, state: dict[str, Any], revision: str | None, locale: str, month: str, now: datetime) -> bool:
    scheduled = _scheduled_at(now).timestamp()
    daily = bool(saved and _recent(month, now) and saved[2] < scheduled)
    return not report or state.get("history") != revision or state.get("locale") != locale or daily or state.get("checked", 0) < scheduled or bool(report.get("current") and month != _current(month, now))


def get_report(store: TrackerStore, cfg: Mapping[str, Any], month: str = "", *, yearly: bool = False) -> dict[str, Any]:
    global _POOL
    if not root_options(cfg, store.root)["wrapped" if yearly else "month_review"]:
        return {"ok": True, "disabled": True, "report": None, "pending": False, "stale": False, "error": None}
    tz = review_timezone(cfg)
    now = datetime.now(tz)
    month = month or (str(now.year - 1) if yearly else previous_month(now))
    if yearly and len(month) != 4 or not yearly and len(month) != 7:
        raise ValueError("invalid_period")
    if month > _current(month, now):
        raise ValueError("future_month")
    period_bounds(month, tz)
    locale = str((cfg.get("metadata") or {}).get("locale") or (cfg.get("ui") or {}).get("locale") or "en-US").strip() or "en-US"
    key = (str(store.root.resolve()), month, str(tz))
    saved, revision, unavailable, recovering = None, None, False, False
    try:
        saved = _saved(store, month, tz)
        revision = _revision(store, month, tz)
    except (sqlite3.Error, OSError) as exc:
        unavailable = True
        recovering = isinstance(exc, sqlite3.Error) and (getattr(exc, "sqlite_errorcode", 0) & 255) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
        if recovering and saved is None:
            with _LOCK:
                saved = _RECENT.get(key)
    report, state = _decode(saved)
    stale = unavailable or _needs_validation(saved, report, state, revision, locale, month, now)
    with _LOCK:
        failed = _FAILURES.get(key, 0) > time.time() - 60
        if stale and not recovering and key not in _PENDING and not failed and not _STOP.is_set() and len(_PENDING) < 64:
            if _POOL is None:
                _POOL = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tracker-month")
            _PENDING.add(key)
            try:
                _POOL.submit(_generate, TrackerStore(store.root), month, tz, locale, key)
            except RuntimeError:
                _PENDING.discard(key)
        pending = key in _PENDING or recovering
    return {"ok": True, "month": month, "current_month": now.strftime("%Y-%m"), "timezone": str(tz), "report": report,
            "pending": pending, "stale": bool(stale), "error": "review_failed" if failed else "review_busy" if unavailable and not pending else None,
            "generated": saved[2] if report and saved is not None else None,
            **({"year": month, "current_year": str(now.year)} if yearly else {})}


def available_month(store: TrackerStore, cfg: Mapping[str, Any]) -> str:
    if not root_options(cfg, store.root)["month_review"]:
        return ""
    if not (store.root / "month-reviews.db").is_file():
        return ""
    tz = review_timezone(cfg)
    try:
        with closing(_readonly(store.root / "month-reviews.db")) as conn:
            row = conn.execute("SELECT month FROM reviews WHERE length(month)=7 AND zone=? AND version=? AND month<? AND json_extract(data,'$.current')=0 AND json_extract(data,'$.plays')>0 ORDER BY month DESC LIMIT 1",
                               (str(tz), _VERSION, datetime.now(tz).strftime("%Y-%m"))).fetchone()
        return row[0] if row else ""
    except (sqlite3.Error, OSError):
        return ""


def available_wrapped(store: TrackerStore, cfg: Mapping[str, Any]) -> dict[str, Any]:
    if not root_options(cfg, store.root)["wrapped"] or not (store.root / "month-reviews.db").is_file():
        return {}
    tz = review_timezone(cfg)
    now = datetime.now(tz)
    if now.month != 1:
        return {}
    year = str(now.year - 1)
    try:
        with closing(_readonly(store.root / "month-reviews.db")) as conn:
            row = conn.execute("SELECT json_extract(data,'$.titles[0].poster'), json_extract(data,'$.titles[1].poster'), json_extract(data,'$.titles[2].poster') FROM reviews WHERE month=? AND zone=? AND version=? AND json_extract(data,'$.current')=0 AND json_extract(data,'$.plays')>0",
                               (year, str(tz), _VERSION)).fetchone()
        return {"year": year, "posters": [value for value in row if value]} if row else {}
    except (sqlite3.Error, OSError):
        return {}


def run_daily(cfg: Mapping[str, Any], config_dir: Path | str) -> bool:
    from services import tracker_migration

    tz = review_timezone(cfg)
    now = datetime.now(tz)
    seen: set[str] = set()
    complete = True
    for instance, root in tracker_roots(cfg, config_dir):
        options = instance_options(cfg, instance)
        if not options["month_review"] and not options["wrapped"]:
            continue
        if _STOP.is_set():
            return False
        store = TrackerStore(root)
        identity = str(root.resolve())
        if identity in seen or not store.db_path.is_file():
            continue
        seen.add(identity)
        if tracker_migration.migrating(root):
            complete = False
            continue
        months = {now.strftime("%Y-%m"), previous_month(now)} if options["month_review"] else set()
        if options["wrapped"]:
            months.add(str(now.year - 1))
        try:
            if (store.root / "month-reviews.db").is_file():
                with closing(_readonly(store.root / "month-reviews.db")) as conn:
                    months.update(row[0] for row in conn.execute("SELECT month FROM reviews WHERE zone=?", (str(tz),)))
        except (sqlite3.Error, OSError):
            complete = False
            _LOG.warning("Could not read monthly review cache for %s", identity)
        for month in sorted(months, reverse=True):
            if not options[_option(month)]:
                continue
            if _STOP.is_set():
                return False
            try:
                result = get_report(store, cfg, month, yearly=True) if len(month) == 4 else get_report(store, cfg, month)
                if result["pending"] or result["stale"] or result["error"]:
                    complete = False
            except Exception:
                complete = False
                _LOG.exception("Monthly review scheduling failed for %s: %s", identity, month)
    return complete


def _maintenance_tick(cfg: Mapping[str, Any], config_dir: Path | str, *, now: datetime | None = None) -> None:
    tz = review_timezone(cfg)
    scheduled = _scheduled_at((now or datetime.now(tz)).astimezone(tz))
    marker = Path(config_dir) / "month-review-maintenance.json"
    configuration = hashlib.sha256(json.dumps([(instance, str(root.resolve()), instance_options(cfg, instance)) for instance, root in tracker_roots(cfg, config_dir)], sort_keys=True).encode()).hexdigest()
    try:
        saved = json.loads(marker.read_text(encoding="utf-8"))
        if isinstance(saved, dict) and saved.get("zone") == str(tz) and saved.get("configuration") == configuration and float(saved.get("scheduled", 0)) >= scheduled.timestamp():
            return
    except (OSError, ValueError, TypeError):
        pass
    if _STOP.is_set() or _busy() or not run_daily(cfg, config_dir) or _STOP.is_set():
        return
    marker.parent.mkdir(parents=True, exist_ok=True)
    temporary = marker.with_suffix(".tmp")
    temporary.write_text(json.dumps({"zone": str(tz), "scheduled": scheduled.timestamp(), "configuration": configuration}), encoding="utf-8")
    temporary.replace(marker)


def start(load_config: Callable, config_dir: Path | str) -> None:
    global _LOAD_CONFIG
    _LOAD_CONFIG = load_config
    global _THREAD

    def run() -> None:
        while not _STOP.wait(60):
            try:
                _maintenance_tick(load_config() or {}, config_dir)
            except Exception:
                _LOG.exception("Monthly review maintenance failed")

    with _LIFECYCLE:
        if _THREAD and _THREAD.is_alive():
            return
        _STOP.clear()
        _THREAD = threading.Thread(target=run, name="tracker-month-daily", daemon=True)
        _THREAD.start()


def stop() -> None:
    global _POOL, _THREAD
    with _LIFECYCLE:
        with _LOCK:
            _STOP.set()
            pool, _POOL = _POOL, None
        with _PUBLICATION:
            pass
        if _THREAD:
            _THREAD.join()
            _THREAD = None
        if pool:
            pool.shutdown(wait=True, cancel_futures=True)
        with _LOCK:
            _PENDING.clear()
