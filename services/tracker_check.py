# services/tracker_check.py
# CrossWatch - Background TMDB metadata check for the local tracker
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import re
import threading
import time
import unicodedata
import uuid
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any, NamedTuple

import requests

from cw_platform.anime_mapping.history_coords import tmdb_coordinate
from cw_platform.tracker_store import ATTENTION_CODES, TrackerStore

AliasContext = tuple[str, Any]
NO_ALIASES: AliasContext = ("", None)

_LOG = logging.getLogger("crosswatch.tracker_check")
_LOCK = threading.RLock()
_ACTIVE: dict[str, dict[str, Any]] = {}
_CANCEL: set[str] = set()
JOB = "metadata_check"
MAX_AGE = 30 * 86_400
BATCH = 200
MAX_FAILURES = 25
NETWORK_PAUSE = 0.03
CACHE_LIMIT = 5000
_ANIME_FIELDS = ("_anime_absolute", "_cw_anime_map", "_simkl_episode_number", "_trakt_number_abs", "anime_type")
_KEEP = ("title", "name", "original_title", "original_name", "release_date", "first_air_date", "runtime", "episode_run_time")
_FAILED = object()
_FIND_BUCKETS = ("movie_results", "tv_results", "tv_season_results", "tv_episode_results")


class Abort(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _norm(text: Any) -> str:
    raw = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode().lower()
    raw = re.sub(r"[^a-z0-9]+", " ", raw).strip()
    return re.sub(r"^(the|a|an) ", "", raw)


def _year(value: Any) -> int | None:
    match = re.match(r"^(\d{4})", str(value or ""))
    return int(match.group(1)) if match else None


def _int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") and not isinstance(value, bool) else None
    except (TypeError, ValueError):
        return None


def _same_title(stored: Any, *candidates: Any) -> bool:
    mine = _norm(stored)
    if not mine:
        return True
    for candidate in candidates:
        other = _norm(candidate)
        if other and (mine == other or (min(len(mine), len(other)) >= 4 and (mine in other or other in mine))):
            return True
    return False


class Found(NamedTuple):
    data: dict[str, Any]
    fetched_at: float
    source: str


def _trim(data: Any) -> dict[str, Any]:
    if not isinstance(data, Mapping):
        return {}
    out = {field: data.get(field) for field in _KEEP if field in data}
    last = data.get("last_episode_to_air")
    if isinstance(last, Mapping) and last.get("runtime"):
        out["last_episode_to_air"] = {"runtime": last.get("runtime")}
    if isinstance(data.get("episodes"), list):
        out["episodes"] = [{"episode_number": row.get("episode_number")} for row in data["episodes"] if isinstance(row, Mapping)]
    return out


class Lookups:
    def __init__(self, provider: Any, *, pause: float = NETWORK_PAUSE):
        self.provider = provider
        self.pause = pause
        self.cache: OrderedDict[tuple[str, str, int | None], Any] = OrderedDict()
        self.failures = 0

    def get(self, kind: str, tmdb: str, season: int | None = None) -> Any:
        key = (kind, tmdb, season)
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        try:
            external = kind in ("imdb", "tvdb")
            if external:
                value = self.provider.get_response(
                    f"https://api.themoviedb.org/3/find/{tmdb}", {"external_source": f"{kind}_id"},
                    quiet_404=True, max_age=MAX_AGE, timeout=10, max_retries=2,
                )
                data = value.data
                if not isinstance(data, Mapping) or not any(bucket in data for bucket in _FIND_BUCKETS):
                    raise ValueError("TMDB returned incomplete external ID results")
                trimmed = {}
                for bucket in _FIND_BUCKETS:
                    rows = data.get(bucket, [])
                    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
                        raise ValueError("TMDB returned invalid external ID results")
                    trimmed[bucket] = [{field: row[field] for field in ("id", "show_id", "season_number", "episode_number") if field in row} for row in rows]
                value = Found(trimmed, value.fetched_at, value.source)
            else:
                value = self.provider.lookup(kind, tmdb, season=season, max_age=MAX_AGE, timeout=10, max_retries=2)
                if value is not None:
                    value = Found(_trim(value.data), value.fetched_at, value.source)
        except requests.HTTPError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status == 401:
                raise Abort("tmdb_key_invalid")
            if status != 404:
                return self._failed(key)
            value = None
        except Abort:
            raise
        except RuntimeError:
            raise Abort("tmdb_key_missing")
        except (requests.RequestException, ValueError):
            return self._failed(key)
        self.failures = 0
        self._remember(key, value)
        if value is not None and value.source == "network" and self.pause:
            time.sleep(self.pause)
        return value

    def _failed(self, key: tuple[str, str, int | None]) -> Any:
        self.failures += 1
        if self.failures >= MAX_FAILURES:
            raise Abort("tmdb_unreachable")
        self._remember(key, _FAILED)
        return _FAILED

    def _remember(self, key: tuple[str, str, int | None], value: Any) -> None:
        self.cache[key] = value
        self.cache.move_to_end(key)
        while len(self.cache) > CACHE_LIMIT:
            self.cache.popitem(last=False)


def _runtime(data: Mapping[str, Any], movie: bool) -> int | None:
    if movie:
        return _int(data.get("runtime")) or None
    per_episode = data.get("episode_run_time")
    if isinstance(per_episode, list) and per_episode:
        return _int(per_episode[0]) or None
    last = data.get("last_episode_to_air")
    return (_int(last.get("runtime")) or None) if isinstance(last, Mapping) else None


def _external_tmdb(item: Mapping[str, Any], kind: str, ids: Mapping[str, Any], lookups: Lookups) -> Any:
    targets = [(ids, "movie_results" if kind == "movie" else "tv_results")]
    typ = str(item.get("type") or "").lower()
    if typ in ("episode", "season") and isinstance(item.get("ids"), Mapping):
        targets.append((item["ids"], f"tv_{typ}_results"))
    for external_ids, bucket in targets:
        for source in ("imdb", "tvdb"):
            if (source == "tvdb" and kind == "movie") or (source == "imdb" and bucket == "tv_season_results"):
                continue
            identifier = str(external_ids.get(source) or "").strip()
            if not re.fullmatch(r"tt[0-9]+" if source == "imdb" else r"[0-9]+", identifier):
                continue
            if source == "tvdb" and int(identifier) <= 0:
                continue
            found = lookups.get(source, identifier)
            if found is _FAILED:
                return _FAILED
            if found is None:
                continue
            candidates = set()
            for row in found.data[bucket]:
                child = bucket in ("tv_episode_results", "tv_season_results")
                if child and any(
                    item.get(field) is not None and _int(item[field]) != row.get(f"{field}_number")
                    for field in (("season", "episode") if typ == "episode" else ("season",))
                ):
                    continue
                tmdb = row.get("show_id" if child else "id")
                if type(tmdb) is int and tmdb > 0:
                    candidates.add(str(tmdb))
            if len(candidates) == 1:
                return candidates.pop()
    return ""


def classify(item: Mapping[str, Any], lookups: Lookups, aliases: AliasContext = NO_ALIASES) -> tuple[str, dict[str, Any]] | None:
    typ = str(item.get("type") or "").lower()
    child = typ in ("season", "episode")
    ids = item.get("show_ids" if child else "ids")
    ids = ids if isinstance(ids, Mapping) else {}
    if typ == "movie":
        kind = "movie"
    elif typ in ("show", "anime") or child:
        kind = "show"
    else:
        return "skipped", {}
    tmdb = str(ids.get("tmdb") or "").strip()
    coordinate = tmdb_coordinate(item, aliases) if typ == "episode" else None
    if coordinate and tmdb and tmdb != coordinate[0]:
        coordinate = None
    tmdb = tmdb or (coordinate[0] if coordinate else "")
    if not tmdb:
        tmdb = _external_tmdb(item, kind, ids, lookups)
        if tmdb is _FAILED:
            return None
        if not tmdb:
            return "no_tmdb", {"kind": kind}
    if not tmdb.isascii() or not tmdb.isdecimal() or int(tmdb) <= 0:
        return "not_found", {"kind": kind, "tmdb": tmdb}
    found = lookups.get(kind, tmdb)
    if found is _FAILED:
        return None
    if found is None:
        return "not_found", {"kind": kind, "tmdb": tmdb}
    data = found.data if isinstance(found.data, Mapping) else {}
    movie = kind == "movie"
    detail: dict[str, Any] = {
        "kind": kind,
        "tmdb": tmdb,
        "title": data.get("title" if movie else "name"),
        "year": _year(data.get("release_date" if movie else "first_air_date")),
        "fetched_at": found.fetched_at,
        "source": found.source,
    }
    runtime = _runtime(data, movie)
    if runtime:
        detail["runtime"] = runtime
    stored_title = item.get("series_title" if child else "title")
    stored_year = _int(item.get("series_year" if child else "year"))
    if (
        not _same_title(stored_title, detail["title"], data.get("original_title" if movie else "original_name"))
        and stored_year is not None and detail["year"] is not None and abs(stored_year - detail["year"]) > 1
    ):
        return "mismatch", detail
    if child and (coordinate or not any(item.get(field) not in (None, "", {}, []) for field in _ANIME_FIELDS)):
        season = coordinate[1] if coordinate else _int(item.get("season"))
        episode = coordinate[2] if coordinate else _int(item.get("episode"))
        if season is not None and season >= 0:
            found_season = lookups.get(kind, tmdb, season)
            if found_season is _FAILED:
                return None
            numbers = set()
            if found_season is not None and isinstance(found_season.data, Mapping):
                numbers = {row.get("episode_number") for row in found_season.data.get("episodes") or [] if isinstance(row, Mapping)}
            if found_season is None or (typ == "episode" and episode and episode not in numbers):
                return "episode_missing", {**detail, "season": season, "episode": episode if typ == "episode" else None}
    if not str(stored_title or "").strip() or (not child and stored_year is None and detail["year"] is not None):
        return "incomplete", detail
    return "ok", detail


def status(store: TrackerStore) -> dict[str, Any]:
    key = str(store.db_path)
    with _LOCK:
        if key in _ACTIVE:
            return dict(_ACTIVE[key])
    job = store.job(JOB)
    if not job:
        return {"state": "idle"}
    if job.get("state") in ("queued", "running"):
        return {**job, "state": "interrupted"}
    return job


def cancel(store: TrackerStore) -> bool:
    key = str(store.db_path)
    with _LOCK:
        if key not in _ACTIVE:
            return False
        _CANCEL.add(key)
        return True


def start(store: TrackerStore, provider: Any, *, instance: str, background: bool = True,
          aliases: AliasContext = NO_ALIASES) -> dict[str, Any]:
    key = str(store.db_path)
    with _LOCK:
        if key in _ACTIVE:
            return dict(_ACTIVE[key])
        if _ACTIVE:
            return {"ok": False, "error": "check_running"}
        job: dict[str, Any] = {
            "id": uuid.uuid4().hex, "instance": instance, "state": "running", "started_at": time.time(),
            "done": 0, "total": 0, "attention": 0, "failed": 0,
        }
        _ACTIVE[key] = job
        _CANCEL.discard(key)
    try:
        store.save_job(JOB, job)
        if background:
            threading.Thread(target=run, args=(store, provider, job, aliases), name="tracker-metadata-check", daemon=True).start()
    except BaseException:
        with _LOCK:
            _ACTIVE.pop(key, None)
        raise
    if not background:
        run(store, provider, job, aliases)
    return dict(job)


def run(store: TrackerStore, provider: Any, job: dict[str, Any], aliases: AliasContext = NO_ALIASES) -> None:
    key = str(store.db_path)
    lookups = Lookups(provider)
    pending: list[tuple[str, str, str, dict[str, Any], str]] = []
    state, error = "completed", None
    try:
        total = store.record_count()
        with _LOCK:
            job["total"] = total
        store.save_job(JOB, job)
        saved = time.monotonic()
        for feature, item_key, data, text in store.iter_records():
            if key in _CANCEL:
                state = "cancelled"
                break
            result = classify(data if isinstance(data, Mapping) else {}, lookups, aliases)
            with _LOCK:
                if result is None:
                    job["failed"] += 1
                else:
                    pending.append((feature, item_key, result[0], result[1], text))
                    if result[0] in ATTENTION_CODES:
                        job["attention"] += 1
                job["done"] += 1
                job["total"] = max(job["total"], job["done"])
            if len(pending) >= BATCH or time.monotonic() - saved > 2:
                store.save_checks(pending)
                pending = []
                store.save_job(JOB, job)
                saved = time.monotonic()
        store.save_checks(pending)
        if state == "completed":
            store.prune_checks()
    except Abort as exc:
        state, error = "failed", exc.code
    except Exception:
        _LOG.exception("Tracker metadata check failed")
        state, error = "failed", "internal_error"
    finally:
        with _LOCK:
            job.update(state=state, finished_at=time.time())
            if error:
                job["error"] = error
        try:
            job["attention"] = sum(store.attention_counts().values())
        except Exception:
            pass
        try:
            store.save_job(JOB, job)
        except Exception:
            _LOG.exception("Could not save the tracker metadata check result")
        with _LOCK:
            _ACTIVE.pop(key, None)
            _CANCEL.discard(key)
        _LOG.info("Tracker metadata check %s: %s of %s records, %s need attention", state, job["done"], job["total"], job["attention"])
