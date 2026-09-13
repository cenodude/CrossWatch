# services/plex_history_recovery.py
# CrossWatch - Plex history recovery into the CrossWatch tracker
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field, replace
import hashlib
import json
import logging
import re
import threading
import time
from typing import Any, Literal
import uuid
from email.utils import parsedate_to_datetime

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from cw_platform.access_policy import request_user, user_can_access_instance
from cw_platform.config_base import load_config
from cw_platform.history_events import history_epoch_from_value
from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.provider_instances import build_provider_config_view, list_instance_ids, normalize_instance_id
from . import importer

router = APIRouter(prefix="/api/import/plex-recovery", tags=["import"])
LOCK = threading.RLock()
JOBS: dict[str, Job] = {}
LOG = logging.getLogger(__name__)
AUTO_MATCH_INTERVAL = 0.5
RECOVERY_TTL_SECONDS = 24 * 60 * 60


class Start(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_instance: str = Field(default="default", max_length=128)
    target_instance: str = Field(default="default", max_length=128)


class Commit(importer.ImportCommitFields):
    revision: int = Field(ge=0)
    status: Literal["all", "ready", "needs_review", "exists", "duplicate", "missing_identity", "unsupported", "invalid"] = "all"


class Match(BaseModel):
    model_config = ConfigDict(extra="forbid")
    revision: int = Field(ge=0)
    row_id: str = Field(max_length=80)
    title: str = Field(min_length=1, max_length=512)
    tmdb: str = Field(default="", max_length=20)
    tvdb: str = Field(default="", max_length=20)
    imdb: str = Field(default="", max_length=20)
    season: int | None = Field(default=None, ge=0, le=10000)
    episode: int | None = Field(default=None, ge=0, le=100000)


class AcceptTitleMatches(BaseModel):
    model_config = ConfigDict(extra="forbid")
    revision: int = Field(ge=0)
    q: str = Field(default="", max_length=1000)


class GroupEdit(BaseModel):
    model_config = ConfigDict(extra="forbid")
    group_id: str = Field(min_length=1, max_length=64)
    title: str = Field(min_length=1, max_length=512)
    ids: dict[str, str] = Field(max_length=12)


class GroupMatches(BaseModel):
    model_config = ConfigDict(extra="forbid")
    revision: int = Field(ge=0)
    edits: list[GroupEdit] = Field(min_length=1, max_length=50_000)


class AutoMatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    revision: int = Field(ge=0)
    catalog: str = Field(min_length=1, max_length=512)
    language: str = Field(default="en-US", pattern=r"^[a-z]{2}-[A-Z]{2}$")
    group_ids: list[str] = Field(min_length=1, max_length=50_000)


@dataclass
class Job:
    owner: str
    source: str
    target: str
    source_hash: str
    target_hash: str = ""
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: str = "reading"
    message: str = "Connecting to Plex"
    stage: str = "connecting"
    done: int = 0
    total: int = 0
    revision: int = 0
    touched: float = field(default_factory=time.time)
    cancel: threading.Event = field(default_factory=threading.Event)
    raw: list[dict[str, Any]] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    receipt: list[str] = field(default_factory=list)
    activity: dict[str, Any] = field(default_factory=dict)
    recent: list[dict[str, str]] = field(default_factory=list)
    match_cache: dict[str, Any] = field(default_factory=dict)
    auto: dict[str, Any] = field(default_factory=dict)
    suggestions: dict[str, Any] = field(default_factory=dict)
    auto_attempts: dict[str, Any] = field(default_factory=dict)
    auto_cooldowns: dict[str, float] = field(default_factory=dict)
    auto_stop: threading.Event = field(default_factory=threading.Event)
    auto_thread: threading.Thread | None = None

    def public(self):
        return dict(id=self.id, import_id=self.id, status=self.status, message=self.message, stage=self.stage,
                    done=self.done, total=self.total, revision=self.revision, source_instance=self.source,
                    target_instance=self.target, imported=len(self.receipt),
                    activity=dict(self.activity), recent=[dict(item) for item in self.recent], auto_match=dict(self.auto))


def _owner(request):
    user = request_user(request)
    if user and not user.get("is_admin") and not (user.get("permissions") or {}).get("write"):
        raise HTTPException(403, "Write permission required")
    return str((user or {}).get("id") or (user or {}).get("username") or "local")


def _access(cfg, request, provider, instance):
    if instance not in list_instance_ids(cfg, provider.lower()) or not user_can_access_instance(cfg, request_user(request), provider, instance):
        raise HTTPException(403, "Profile unavailable")


def _source_hash(cfg, instance):
    block = build_provider_config_view(cfg, "PLEX", instance).get("plex") or {}
    return hashlib.sha256(json.dumps(block, sort_keys=True, default=str).encode()).hexdigest()


def _target_hash(cfg, instance):
    block = build_provider_config_view(cfg, "CROSSWATCH", instance).get("crosswatch") or {}
    return hashlib.sha256(json.dumps(block, sort_keys=True, default=str).encode()).hexdigest()


def _search_fingerprint(cfg, job: Job, choice):
    from .interactive_sync_catalogs import provider_block
    route = choice["route"]
    catalog = {"api_key": (cfg.get("tmdb") or {}).get("api_key")} if choice["catalog"] == "tmdb" else provider_block(cfg, route["provider"], route["instance"])
    dependencies = [_source_hash(cfg, job.source), _target_hash(cfg, job.target), route, catalog]
    return hashlib.sha256(json.dumps(dependencies, sort_keys=True, default=str).encode()).hexdigest()


def _recovery_candidates(result):
    for candidate in result["results"]:
        ids = {key: str(value) for key, value in candidate.get("ids", {}).items() if key in {"tmdb", "tvdb", "imdb"}
               and re.fullmatch(r"tt\d+" if key == "imdb" else r"[1-9]\d*", str(value))}
        if not ids:
            candidate["mapping_unavailable"] = "No valid TMDb, TVDB or IMDb identity. Try another search provider."
    return result


def _search_language(choice, language):
    return language if choice["catalog"] == "tmdb" or choice["route"]["provider"] == "TMDB" else ""


def _prune():
    for key, job in list(JOBS.items()):
        if job.status != "reading" and not _auto_running(job) and time.time() - job.touched > RECOVERY_TTL_SECONDS:
            del JOBS[key]


def _job(sid, request, cfg):
    _prune()
    job = JOBS.get(sid)
    if job is None or job.owner != _owner(request):
        raise HTTPException(404, "Recovery expired or not found. Start another scan.")
    _access(cfg, request, "PLEX", job.source)
    _access(cfg, request, "CROSSWATCH", job.target)
    job.touched = time.time()
    return job


def _shape(job, cfg):
    grouped: dict[str, list[int]] = {}
    for index, raw in enumerate(job.raw):
        item = raw["item"]
        if raw["recovery_valid"] and not raw.get("recovery_requires_review"):
            identity = canonical_key(id_minimal(item))
        else:
            source = raw.get("original_group") or (item.get("ids") or {}).get("plex") or raw.get("original_title")
            identity = json.dumps([item.get("type"), source, raw.get("original_year"), item.get("season"), item.get("episode")])
        if item.get("type") == "episode" and (item.get("season") is None or item.get("episode") is None):
            identity = f"unresolved:{index}"
        grouped.setdefault(identity, []).append(index)
    selected = [(max(indices, key=lambda i: history_epoch_from_value(job.raw[i]["item"].get("watched_at")) or 0), indices)
                for indices in grouped.values()]
    rows = importer._shape_rows([job.raw[index] for index, _ in selected], cfg, job.target, "plex_recovery")
    existing = {canonical_key(id_minimal(item)) for item in importer._existing_keys(cfg, job.target)["history"].values()}
    for row, (index, indices) in zip(rows, selected):
        raw = job.raw[index]
        row["id"] = str(index)
        row["recovery_row_ids"] = [str(i) for i in indices]
        row.update(recovery_match=raw["recovery_match"], original_title=raw["original_title"])
        if not raw["recovery_valid"]:
            row.update(status="missing_identity", reason="missing_identity", importable=False, default_included=False)
        elif row["status"] == "ready" and raw.get("recovery_requires_review", False):
            row.update(status="needs_review", reason="title_guess", default_included=False)
        if raw["recovery_valid"] and canonical_key(id_minimal(raw["item"])) in existing:
            row.update(status="exists", reason="already_exists", importable=False, default_included=False)
    job.rows = rows


def _scan(job, cfg):
    from api import syncAPI
    from providers.sync._mod_PLEX import PLEXModule
    from providers.sync.plex._common import isolated_plex_context
    from providers.sync.plex._recovery import scan

    def check_cancel():
        if job.cancel.is_set():
            raise InterruptedError("Recovery cancelled. Nothing was imported.")

    def progress(stage, message, done, total, activity=None):
        with LOCK:
            job.stage, job.message, job.done, job.total = stage, message, done, total
            if activity is not None:
                job.activity = {key:value for key,value in activity.items() if key != "event"}
                if activity.get("event"):
                    job.recent = [*job.recent, dict(activity["event"])][-8:]

    adapter = None
    try:
        with isolated_plex_context():
            check_cancel()
            adapter = PLEXModule(build_provider_config_view(cfg, "PLEX", job.source))
            raw = scan(adapter, progress=progress, check_cancel=check_cancel, max_rows=importer.MAX_ROWS)
            check_cancel()
            progress("review", "Checking recovered events against the CW tracker", 0, 0)
            with LOCK:
                job.raw = raw
                _shape(job, cfg)
                job.status = "review"
                job.message = "Review the recovered watch events. Nothing has been imported."
                job.revision += 1
    except InterruptedError:
        with LOCK:
            job.status, job.message = "cancelled", "Recovery cancelled. Nothing was imported."
    except Exception:
        LOG.exception("plex_recovery_failed job=%s", job.id)
        with LOCK:
            job.status = "error"
            job.message = "Recovery could not finish. Check that the Plex connection has server-owner access to play history, and check the connection and library access, then retry. Nothing was imported."
    finally:
        if adapter is not None:
            for session in (getattr(adapter.client, "session", None), getattr(getattr(adapter.client, "server", None), "_session", None)):
                if session is not None:
                    try:
                        session.close()
                    except Exception:
                        LOG.debug("plex_recovery_session_close_failed", exc_info=True)
        with LOCK:
            job.touched = time.time()
        rt = syncAPI._rt()
        with rt[2]:
            if rt[1].get("SYNC") is threading.current_thread():
                rt[1].pop("SYNC", None)


@router.get("/options")
def options(request: Request):
    _owner(request)
    cfg = load_config() or {}
    sources = []
    for instance in list_instance_ids(cfg, "plex"):
        if not user_can_access_instance(cfg, request_user(request), "PLEX", instance):
            continue
        block = build_provider_config_view(cfg, "PLEX", instance).get("plex") or {}
        pms = block.get("pms") or {}
        if not (block.get("baseurl") or block.get("server_url") or pms.get("url") or pms.get("baseurl")):
            continue
        sources.append(dict(id=instance, label=block.get("label") or instance,
                            user=block.get("username") or "Server account",
                            libraries=(block.get("history") or {}).get("libraries") or []))
    targets = [t for t in importer._target_instances(cfg) if user_can_access_instance(cfg, request_user(request), "CROSSWATCH", t["id"])]
    return dict(sources=sources, targets=targets)


@router.post("")
def start(payload: Start, request: Request):
    from api import syncAPI
    cfg = load_config() or {}
    owner = _owner(request)
    source, target = normalize_instance_id(payload.source_instance), normalize_instance_id(payload.target_instance)
    _access(cfg, request, "PLEX", source)
    _access(cfg, request, "CROSSWATCH", target)
    if not importer._target_connected(cfg, target):
        raise HTTPException(409, "Connect the CrossWatch tracker before recovering history.")
    rt = syncAPI._rt()
    with LOCK, rt[2]:
        _prune()
        for key, previous in list(JOBS.items()):
            if previous.cancel.is_set() and previous.status != "reading" and not _auto_running(previous):
                JOBS.pop(key, None)
        if any(previous.cancel.is_set() for previous in JOBS.values()):
            raise HTTPException(409, "The previous recovery is still stopping. Try again shortly.")
        if JOBS:
            raise HTTPException(409, "A Plex recovery is already open. Return to it or close it before starting another.")
        if syncAPI._is_sync_running():
            raise HTTPException(409, "Another sync or recovery is running. Try again when it finishes.")
        job = Job(owner, source, target, _source_hash(cfg, source), target_hash=_target_hash(cfg, target))
        JOBS[job.id] = job
        thread = threading.Thread(target=_scan, args=(job, deepcopy(cfg)), daemon=True)
        rt[1]["SYNC"] = thread
        thread.start()
        return job.public()


@router.get("/active")
def active(request: Request):
    owner = _owner(request)
    cfg = load_config() or {}
    with LOCK:
        _prune()
        for job in JOBS.values():
            if job.owner != owner or job.cancel.is_set():
                continue
            try:
                _access(cfg, request, "PLEX", job.source)
                _access(cfg, request, "CROSSWATCH", job.target)
            except HTTPException:
                continue
            return dict(job=job.public(), busy=True)
        stopping = any(job.cancel.is_set() and (job.status == "reading" or _auto_running(job)) for job in JOBS.values())
        return dict(job=None, busy=stopping or any(not job.cancel.is_set() for job in JOBS.values()), **({"stopping": True} if stopping else {}))


@router.get("/{sid}")
def status(sid: str, request: Request):
    with LOCK:
        return _job(sid, request, load_config() or {}).public()


@router.delete("/{sid}")
def close(sid: str, request: Request):
    with LOCK:
        job = _job(sid, request, load_config() or {})
        job.cancel.set()
        job.auto_stop.set()
        if job.status != "reading" and not _auto_running(job):
            JOBS.pop(sid, None)
        return dict(ok=True)


@router.get("/{sid}/rows")
def rows(sid: str, request: Request, q: str = Query("", max_length=1000), status: str = "all",
         limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
    with LOCK:
        job = _job(sid, request, load_config() or {})
        found = importer._filtered_rows(job.rows, features={"history"}, media_types=set(), status="all", q=q)
        filtered = [r for r in found if status == "all" or r["status"] == status]
        public = importer._public_rows(filtered, limit, offset)
        for item, row in zip(public, filtered[offset:offset + limit]):
            item.update(recovery_match=row["recovery_match"], original_title=row["original_title"])
        return dict(**job.public(), rows=public, filtered_total=len(filtered), summary=importer._summary(found))


def _matching_job(sid, revision, request, cfg):
    job = _job(sid, request, cfg)
    if job.cancel.is_set() or job.status != "review" or job.revision != revision:
        raise HTTPException(409, "The recovery changed. Reopen matching.")
    if job.source_hash != _source_hash(cfg, job.source) or (job.target_hash and job.target_hash != _target_hash(cfg, job.target)):
        raise HTTPException(409, "Connection settings changed. Start a new recovery scan.")
    return job


def _auto_running(job: Job) -> bool:
    return job.auto_thread is not None and job.auto_thread.is_alive()


def _require_idle_matching(job: Job):
    if _auto_running(job):
        raise HTTPException(409, "Auto match is running. Stop it or wait before changing or importing matches.")


def _auto_view(job: Job):
    return dict(auto_match=dict(job.auto), suggestions={} if job.auto.get("status") == "running" else deepcopy(job.suggestions))


def _retry_delay(error: HTTPException) -> float:
    value = (error.headers or {}).get("Retry-After", "60")
    try:
        return max(1, float(value))
    except (ValueError, TypeError):
        try:
            return max(1, parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return 60


def _auto_match(job: Job, payload: AutoMatch, groups: list[dict[str, Any]], request: Request, fingerprint: str):
    from .interactive_sync_mapping import search_candidates

    def check():
        if job.auto_stop.is_set() or job.cancel.is_set():
            raise InterruptedError()
        cfg = load_config() or {}
        _matching_job(job.id, payload.revision, request, cfg)
        choice = _match_catalogs(cfg, request).get(payload.catalog)
        if choice is None:
            raise HTTPException(403, "Search provider is no longer accessible.")
        if _search_fingerprint(cfg, job, choice) != fingerprint:
            raise HTTPException(409, "Search provider settings changed. Reopen matching before continuing.")
        return cfg, choice

    try:
        for group in groups:
            with LOCK:
                cfg, choice = check()
                title, year = group["item"]["title"], group["item"].get("year")
                language = _search_language(choice, payload.language)
                key = json.dumps([fingerprint, payload.catalog, group["item"]["type"], title.strip().casefold(), language])
                attempt = json.dumps([key, year])
                job.auto.update(current_title=title, message="Finding title matches")
                cached = attempt in job.auto_attempts
                result = deepcopy(job.match_cache.get(key))
            if not cached:
                if result is None:
                    for retry in range(3):
                        delay = max(0, job.auto_cooldowns.get(payload.catalog, 0) - time.time())
                        if delay > 300:
                            raise HTTPException(429, f"Provider rate limit reached. Retry in {int(delay)} seconds.")
                        if delay:
                            with LOCK:
                                job.auto["message"] = f"Provider rate limit reached. Waiting {int(delay)} seconds before retrying."
                            if job.auto_stop.wait(delay):
                                raise InterruptedError()
                        with LOCK:
                            cfg, choice = check()
                            job.auto["searches"] += 1
                        try:
                            result = _recovery_candidates(search_candidates(cfg, dict(choice["route"], item=deepcopy(group["item"]), metadata_language=language), title, catalog=choice["catalog"]))
                            break
                        except HTTPException as error:
                            if error.status_code != 429:
                                raise
                            delay = _retry_delay(error)
                            with LOCK:
                                job.auto_cooldowns[payload.catalog] = time.time() + delay
                            if delay > 300 or retry == 2:
                                raise
                    with LOCK:
                        check()
                        if len(job.match_cache) >= 512:
                            job.match_cache.pop(next(iter(job.match_cache)))
                        job.match_cache[key] = deepcopy(result)
                    if job.auto_stop.wait(AUTO_MATCH_INTERVAL):
                        raise InterruptedError()
                matches = [m for m in (result or {}).get("results", []) if m.get("exact_title")
                           and (not year or not m.get("year") or str(year) == str(m["year"]))]
                suggestion = None
                if len(matches) == 1 and not matches[0].get("mapping_unavailable"):
                    candidate = matches[0]
                    ids = {k: str(v) for k, v in candidate.get("ids", {}).items() if k in {"tmdb", "tvdb", "imdb"}
                           and re.fullmatch(r"tt\d+" if k == "imdb" else r"[1-9]\d*", str(v))}
                    if ids:
                        suggestion = dict(title=candidate["title"], ids=ids, year=candidate.get("year"))
                with LOCK:
                    check()
                    job.auto_attempts[attempt] = suggestion
            with LOCK:
                check()
                suggestion = job.auto_attempts[attempt]
                job.suggestions[group["id"]] = dict(match=deepcopy(suggestion), catalog=payload.catalog,
                                                   reason="" if suggestion else "Choose a title manually; no single reliable match was found.")
                job.auto["done"] += 1
                job.auto["matched"] += int(suggestion is not None)
                job.auto["cached"] += int(cached)
        with LOCK:
            job.auto.update(status="complete", message="Auto match finished. Review the suggestions, then save checked matches. Nothing was imported.")
    except InterruptedError:
        with LOCK:
            job.auto.update(status="stopped", message="Auto match stopped. Suggestions are kept. Run Auto match again to continue.")
    except Exception as error:
        with LOCK:
            detail = str(error.detail) if isinstance(error, HTTPException) else "The search provider could not finish the request."
            job.auto.update(status="paused", message=f"Auto match paused. {detail} Suggestions are kept; run Auto match again to continue.")
    finally:
        with LOCK:
            job.touched = time.time()
            if job.cancel.is_set():
                JOBS.pop(job.id, None)


@router.post("/{sid}/auto-match")
def start_auto_match(sid: str, payload: AutoMatch, request: Request):
    cfg = load_config() or {}
    with LOCK:
        job = _matching_job(sid, payload.revision, request, cfg)
        _require_idle_matching(job)
        choice = _match_catalogs(cfg, request).get(payload.catalog)
        if choice is None:
            raise HTTPException(403, "Search provider is unavailable or not accessible.")
        groups = {group["id"]: group for group in _match_groups(job)}
        if any(key not in groups for key in payload.group_ids):
            raise HTTPException(409, "The unresolved titles changed. Reopen matching.")
        selected = [groups[key] for key in dict.fromkeys(payload.group_ids) if not job.suggestions.get(key, {}).get("match")]
        if not selected:
            return _auto_view(job)
        job.auto_stop.clear()
        job.auto = dict(id=uuid.uuid4().hex, status="running", catalog=payload.catalog, language=payload.language, total=len(selected), done=0,
                        matched=0, searches=0, cached=0, current_title="", message="Starting Auto match")
        fingerprint = _search_fingerprint(cfg, job, choice)
        thread = threading.Thread(target=_auto_match, args=(job, payload, deepcopy(selected), request, fingerprint), daemon=True)
        job.auto_thread = thread
        thread.start()
        return _auto_view(job)


@router.get("/{sid}/auto-match")
def auto_match_status(sid: str, request: Request):
    with LOCK:
        return _auto_view(_job(sid, request, load_config() or {}))


@router.delete("/{sid}/auto-match")
def stop_auto_match(sid: str, request: Request):
    with LOCK:
        job = _job(sid, request, load_config() or {})
        job.auto_stop.set()
        return _auto_view(job)


def _match_groups(job: Job, q: str = "") -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    visible = {row["id"] for row in importer._filtered_rows(job.rows, features={"history"}, media_types=set(), status="all", q=q)}
    for row in job.rows:
        raw = job.raw[int(row["id"])]
        if row["status"] == "exists" or (raw["recovery_valid"] and not raw.get("recovery_requires_review")):
            continue
        item = raw["item"]
        kind = "show" if item.get("type") == "episode" else "movie"
        title = raw.get("original_title") or item.get("series_title") or item.get("title") or "Untitled item"
        identity = raw.setdefault("original_group", json.dumps([kind, title, raw.get("original_year"), (item.get("ids") or {}).get("plex")]))
        key = hashlib.sha256(identity.encode()).hexdigest()
        group = groups.setdefault(key, dict(id=key, provider="PLEX", instance=job.source, feature="history",
                                           item=dict(type=kind, title=title, year=raw.get("original_year"), ids={}), row_ids=[], recovery_count=0))
        group["row_ids"].extend(row["recovery_row_ids"])
        group["recovery_count"] += 1
    return [group for group in groups.values() if visible.intersection(group["row_ids"])]


def _match_catalogs(cfg, request):
    from .interactive_sync_catalogs import NATIVE, search_catalogs
    choices = {}
    for provider in sorted(NATIVE):
        for instance in list_instance_ids(cfg, provider.lower()):
            if not user_can_access_instance(cfg, request_user(request), provider, instance):
                continue
            route = dict(provider=provider, instance=instance)
            catalogs = search_catalogs(cfg, route)["catalogs"]
            if not isinstance(catalogs, list):
                continue
            for catalog in catalogs:
                if catalog["id"] != "destination":
                    continue
                key = json.dumps([provider, instance, "destination"])
                label = catalog["label"] + (f" · {instance}" if instance != "default" else "")
                choices[key] = dict(id=key, label=label, route=route, catalog="destination")
    if (cfg.get("tmdb") or {}).get("api_key"):
        choices["tmdb"] = dict(id="tmdb", label="TMDb metadata", route=dict(provider="TMDB", instance="default", metadata_only=True), catalog="tmdb")
    priority = {"TMDB": 0, "TRAKT": 1, "SIMKL": 2, "MDBLIST": 3, "PLEX": 4}
    return dict(sorted(choices.items(), key=lambda entry: priority.get(entry[1]["route"]["provider"], 5)))


@router.get("/{sid}/match-groups")
def match_groups(sid: str, revision: int, request: Request, q: str = Query("", max_length=1000)):
    cfg = load_config() or {}
    with LOCK:
        job = _matching_job(sid, revision, request, cfg)
        groups = _match_groups(job, q)
        catalogs = [dict(id=c["id"], label=c["label"], supports_language=bool(_search_language(c, "en-US"))) for c in _match_catalogs(cfg, request).values()]
        return dict(revision=job.revision, rows=groups, catalogs=catalogs, total=len(groups),
                    auto_match=dict(job.auto), suggestions=deepcopy(job.suggestions))


@router.get("/{sid}/match-search")
def match_search(sid: str, revision: int, group_id: str, catalog: str, request: Request,
                 q: str = Query(min_length=1, max_length=200), language: str = Query("en-US", pattern=r"^[a-z]{2}-[A-Z]{2}$")):
    from .interactive_sync_mapping import search_candidates
    cfg = load_config() or {}
    with LOCK:
        job = _matching_job(sid, revision, request, cfg)
        group = next((g for g in _match_groups(job) if g["id"] == group_id), None)
        if group is None:
            raise HTTPException(404, "Unresolved movie or series not found")
        choice = _match_catalogs(cfg, request).get(catalog)
        if choice is None:
            raise HTTPException(403, "Search provider is unavailable or not accessible.")
        fingerprint = _search_fingerprint(cfg, job, choice)
        language = _search_language(choice, language)
        cache_key = json.dumps([fingerprint, catalog, group["item"]["type"], q.strip().casefold(), language])
        if cache_key in job.match_cache:
            return deepcopy(job.match_cache[cache_key])
        route = dict(choice["route"], item=deepcopy(group["item"]), metadata_language=language)
    result = _recovery_candidates(search_candidates(cfg, route, q.strip(), catalog=choice["catalog"]))
    with LOCK:
        current_cfg = load_config() or {}
        job = _matching_job(sid, revision, request, current_cfg)
        current_choice = _match_catalogs(current_cfg, request).get(catalog)
        if current_choice is None or _search_fingerprint(current_cfg, job, current_choice) != fingerprint:
            raise HTTPException(409, "Search provider settings changed. Reopen matching.")
        if len(job.match_cache) >= 512:
            job.match_cache.pop(next(iter(job.match_cache)))
        job.match_cache[cache_key] = deepcopy(result)
        return result


@router.post("/{sid}/match-groups")
def save_group_matches(sid: str, payload: GroupMatches, request: Request):
    cfg = load_config() or {}
    with LOCK:
        job = _matching_job(sid, payload.revision, request, cfg)
        _require_idle_matching(job)
        groups = {g["id"]: g for g in _match_groups(job)}
        updated = deepcopy(job.raw)
        seen = set()
        changed = 0
        for edit in payload.edits:
            if edit.group_id in seen or edit.group_id not in groups:
                raise HTTPException(409, "The unresolved selection changed. Reopen matching.")
            seen.add(edit.group_id)
            ids = {key: value.strip() for key, value in edit.ids.items() if key in {"tmdb", "tvdb", "imdb"} and value.strip()}
            if not edit.title.strip() or not ids or any(not re.fullmatch(r"tt\d+" if key == "imdb" else r"[1-9]\d*", value) for key, value in ids.items()):
                raise HTTPException(400, "Enter a title and valid TMDb, TVDB or IMDb IDs.")
            for row_id in groups[edit.group_id]["row_ids"]:
                raw = updated[int(row_id)]
                item = raw["item"]
                valid = True
                if item["type"] == "episode":
                    item.update(ids={}, show_ids=dict(ids), series_title=edit.title.strip())
                    valid = item.get("season") is not None and item.get("episode") is not None
                else:
                    item["ids"] = dict(ids)
                item["title"] = edit.title.strip()
                raw.update(recovery_valid=valid, recovery_requires_review=False, recovery_match="Confirmed bulk match")
            changed += groups[edit.group_id]["recovery_count"]
        candidate = replace(job, raw=updated, rows=[])
        _shape(candidate, cfg)
        job.raw, job.rows = candidate.raw, candidate.rows
        for edit in payload.edits:
            job.suggestions.pop(edit.group_id, None)
        job.revision += 1
        return dict(**job.public(), matched_events=changed)


@router.post("/{sid}/match")
def match(sid: str, payload: Match, request: Request):
    cfg = load_config() or {}
    with LOCK:
        job = _job(sid, request, cfg)
        _require_idle_matching(job)
        if job.status != "review" or payload.revision != job.revision:
            raise HTTPException(409, "The recovery changed. Reload the preview.")
        row = next((r for r in job.rows if r["id"] == payload.row_id), None)
        if row is None:
            raise HTTPException(404, "Watch event not found")
        ids = {k: getattr(payload, k).strip() for k in ("tmdb", "tvdb", "imdb") if getattr(payload, k).strip()}
        if not ids or any(not re.fullmatch(r"tt\d+" if k == "imdb" else r"[1-9]\d*", value) for k, value in ids.items()):
            raise HTTPException(400, "Enter a valid TMDb, TVDB or IMDb ID.")
        if row["media_type"] == "episode" and (payload.season is None or payload.episode is None):
            raise HTTPException(400, "Enter season and episode numbers.")
        for row_id in row["recovery_row_ids"]:
            raw = job.raw[int(row_id)]
            item = raw["item"]
            if item["type"] == "episode":
                item.update(show_ids=dict(ids), ids={}, series_title=payload.title.strip(), season=payload.season, episode=payload.episode)
            else:
                item["ids"] = dict(ids)
            item["title"] = payload.title.strip()
            raw.update(recovery_valid=True, recovery_requires_review=False, recovery_match="Manually matched")
        _shape(job, cfg)
        job.revision += 1
        return job.public()


@router.post("/{sid}/accept-title-matches")
def accept_title_matches(sid: str, payload: AcceptTitleMatches, request: Request):
    cfg = load_config() or {}
    with LOCK:
        job = _matching_job(sid, payload.revision, request, cfg)
        _require_idle_matching(job)
        candidate = replace(job, raw=deepcopy(job.raw), rows=[])
        _shape(candidate, cfg)
        matches = importer._filtered_rows(candidate.rows, features={"history"}, media_types={"movie", "episode"},
                                          status="needs_review", q=payload.q)
        for row in matches:
            for row_id in row["recovery_row_ids"]:
                candidate.raw[int(row_id)].update(recovery_requires_review=False, recovery_match="Confirmed title match")
        _shape(candidate, cfg)
        job.raw, job.rows = candidate.raw, candidate.rows
        job.revision += 1
        return dict(**job.public(), accepted=len(matches))


@router.post("/{sid}/commit")
def commit(sid: str, payload: Commit, request: Request):
    from api import syncAPI
    cfg = load_config() or {}
    rt = syncAPI._rt()
    with LOCK, rt[2]:
        job = _job(sid, request, cfg)
        _require_idle_matching(job)
        if job.status != "review" or payload.revision != job.revision:
            raise HTTPException(409, "The recovery changed. Reload the preview.")
        if syncAPI._is_sync_running():
            raise HTTPException(409, "Another sync is running. Wait before importing.")
        if job.target_hash and job.target_hash != _target_hash(cfg, job.target):
            raise HTTPException(409, "Tracker settings changed. Start a new recovery scan.")
        if job.source_hash != _source_hash(cfg, job.source):
            raise HTTPException(409, "Plex settings changed. Start a new recovery scan.")
        if normalize_instance_id(payload.target_instance) != job.target or payload.import_id != sid:
            raise HTTPException(400, "The import destination changed. Start a new preview.")
        _shape(job, cfg)
        args = payload.model_dump(exclude={"revision"})
        args.update(features=["history"], media_types=["movie", "episode"], include_existing=False)
        explicit = set(payload.row_ids)
        excluded = set(payload.excluded_row_ids)
        selected = [r for r in importer._filtered_rows(job.rows, features={"history"}, media_types={"movie", "episode"}, status=payload.status, q=payload.q)
                    if r["id"] not in excluded
                    and ((r["status"] == "ready" and (payload.mode != "selected" or r["id"] in explicit))
                         or (r["status"] == "needs_review" and r["id"] in explicit))]
        titles: dict[str, dict[str, Any]] = {}
        for row in selected:
            identity = canonical_key(id_minimal(row["item"]))
            previous = titles.get(identity)
            if previous is None or (history_epoch_from_value(row["item"].get("watched_at")) or 0) > (history_epoch_from_value(previous["item"].get("watched_at")) or 0):
                titles[identity] = row
        selected = list(titles.values())
        approved = [{**r, "status": "ready"} for r in selected]
        importer._PREVIEWS[job.id] = dict(created_at=time.time(), target_instance=job.target, rows=approved)
        args.update(mode="selected", row_ids=[r["id"] for r in selected], status="all", q="")
        try:
            result = importer.api_import_commit(importer.ImportCommitRequest(**args), request=request)
        finally:
            importer._PREVIEWS.pop(job.id, None)
        existing = importer._existing_keys(cfg, job.target)["history"]
        job.receipt = list(dict.fromkeys([*job.receipt, *(r["key"] for r in selected if r["key"] in existing)]))
        _shape(job, cfg)
        job.revision += 1
        return dict(**result, revision=job.revision, receipt_id=job.id, imported_keys=job.receipt)


@router.get("/{sid}/receipt")
def receipt(sid: str, request: Request):
    cfg = load_config() or {}
    with LOCK:
        job = _job(sid, request, cfg)
        return dict(provider="CROSSWATCH", instance=job.target, feature="history", keys=list(job.receipt))
