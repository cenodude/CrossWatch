# providers/metadata/_meta_TMDB.py
# CrossWatch - TMDb Metadata Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import os
import random
import threading
import time
import weakref
from collections.abc import Mapping
from concurrent.futures import Future
from copy import deepcopy
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Literal

import requests

from cw_platform.app_version import user_agent as http_user_agent
from cw_platform.metadata_cache import normalize_title

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None

def log(msg: str, level: str = "INFO", module: str = "META", **_: Any) -> None:
    try:
        if _real_log is not None:
            _real_log(msg, level=level, module=module, **_)
        else:
            print(f"[{module}] {level}: {msg}")
    except Exception:
        pass

IMG_BASE = "https://image.tmdb.org/t/p"

_PROVIDERS: weakref.WeakSet[Any] = weakref.WeakSet()
_PROVIDERS_LOCK = threading.RLock()
_REQUEST_LOCKS = tuple(threading.RLock() for _ in range(64))


@dataclass(frozen=True)
class TmdbResponse:
    data: Any
    fetched_at: float
    source: Literal["cache", "network", "shared"]


_INFLIGHT: dict[tuple[Any, ...], Future[TmdbResponse]] = {}


@lru_cache(maxsize=64)
def _scope_of(root: str) -> str:
    return os.path.normcase(os.path.realpath(root))


def clear_all_caches() -> None:
    with _PROVIDERS_LOCK:
        for provider in list(_PROVIDERS):
            provider.clear_cache()


class TmdbProvider:
    name = "TMDB"
    UA = http_user_agent(override_env="CW_TMDB_UA")
    CACHE_MAX_ENTRIES = 4096

    @staticmethod
    def manifest() -> dict[str, Any]:
        return {
            "id": "tmdb",
            "name": "TMDB",
            "enabled": True,
            "ready": None,
            "ok": None,
            "version": "1.0",
        }

    def __init__(
        self,
        load_cfg: Callable[[], dict[str, Any]],
        save_cfg: Callable[[dict[str, Any]], None],
    ) -> None:
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self._cache: dict[str, tuple[float, Any]] = {}
        self._disk_dir: Path | None = None
        with _PROVIDERS_LOCK:
            _PROVIDERS.add(self)

    def _apikey(self) -> str:
        cfg = self.load_cfg() or {}
        tmdb = cfg.get("tmdb") or {}
        md = cfg.get("metadata") or {}
        api_key = (tmdb.get("api_key") or md.get("tmdb_api_key") or "").strip()
        if not api_key:
            raise RuntimeError("TMDb API key is missing")
        return api_key

    def _disk_cache_dir(self) -> Path | None:
        if self._disk_dir is not None:
            return self._disk_dir
        try:
            import crosswatch as CW  # type: ignore

            base = Path(getattr(CW, "CACHE_DIR", None) or "")
        except Exception:
            base = Path("")
        if not str(base):
            try:
                from cw_platform.config_base import CONFIG

                base = Path(CONFIG) / "cache"
            except Exception:
                return None
        target = base / "meta" / "tmdb"
        try:
            target.mkdir(parents=True, exist_ok=True)
        except Exception:
            return None
        self._disk_dir = target
        return target

    def _disk_read(self, key: str) -> tuple[float, Any] | None:
        target = self._disk_cache_dir()
        if target is None:
            return None
        try:
            record = json.loads((target / f"{key}.json").read_text("utf-8"))
            if not isinstance(record, dict) or record.get("data") is None:
                return None
            return float(record.get("ts") or 0), record["data"]
        except Exception:
            return None

    def _disk_write(self, key: str, data: Any, fetched_at: float) -> None:
        target = self._disk_cache_dir()
        if target is None:
            return
        path = target / f"{key}.json"
        tmp = target / f"{key}.{os.getpid()}.{threading.get_ident()}.tmp"
        try:
            tmp.write_text(json.dumps({"ts": fetched_at, "data": data}, ensure_ascii=False), encoding="utf-8")
            tmp.replace(path)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    def _prune_cache(self) -> None:
        while len(self._cache) > self.CACHE_MAX_ENTRIES:
            self._cache.pop(next(iter(self._cache)), None)

    def clear_cache(self) -> None:
        with _PROVIDERS_LOCK:
            self._cache.clear()

    def _cache_scope(self) -> str:
        root = self._disk_cache_dir()
        return _scope_of(str(root)) if root is not None else f"memory:{id(self)}"

    def _publish(self, key: str, record: tuple[float, Any], scope: str) -> None:
        with _PROVIDERS_LOCK:
            for provider in list(_PROVIDERS):
                if provider is not self and (key not in provider._cache or provider._disk_dir is None or provider._cache_scope() != scope):
                    continue
                current = provider._cache.pop(key, None)
                newer = current is not None and current[0] > record[0]
                provider._cache[key] = current if newer else (record[0], deepcopy(record[1]))
                provider._prune_cache()

    @staticmethod
    def _retryable(exc: BaseException) -> bool:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return status is None or status == 429 or 500 <= int(status) < 600

    def _backoff_params(self) -> tuple[int, float, float]:
        cfg = self.load_cfg() or {}
        md = cfg.get("metadata") or {}
        max_retries = int(md.get("backoff_max_retries", 4))
        base_ms = int(md.get("backoff_base_ms", 500))
        max_ms = int(md.get("backoff_max_ms", 4000))
        return max(0, max_retries), max(0.05, base_ms / 1000.0), max(0.1, max_ms / 1000.0)

    def _retry_delay(self, attempt: int, base_s: float, max_s: float) -> float:
        delay = min(max_s, base_s * (2**attempt))
        return delay + random.uniform(0.0, 0.25)

    def _seconds_from_retry_after(self, header: str | None) -> float | None:
        if not header:
            return None
        header = header.strip()
        if header.isdigit():
            return float(header)
        try:
            dt = parsedate_to_datetime(header)
            return max(0.0, dt.timestamp() - time.time())
        except Exception:
            return None

    def _log_exc(self, msg: str, exc: Exception) -> None:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        lvl = "INFO" if int(status or 0) == 404 else "WARNING"
        log(f"{msg}: {exc}", level=lvl, module="META")

    def _debug_enabled(self) -> bool:
        try:
            cfg = self.load_cfg() or {}
            return bool((cfg.get("runtime") or {}).get("debug"))
        except Exception:
            return False

    def _debug_log(self, event: str, **fields: Any) -> None:
        if not self._debug_enabled():
            return
        clean = {
            str(k): v
            for k, v in fields.items()
            if v not in (None, "", [], {})
        }
        suffix = " ".join(f"{k}={v}" for k, v in clean.items())
        msg = event if not suffix else f"{event} {suffix}"
        log(msg, level="debug", module="META", extra=clean or None)

    def get_latest(self, url: str, params: dict[str, Any] | None = None, *, quiet_404: bool = False) -> Any:
        try:
            return self._get(url, params, quiet_404=quiet_404, fresh=True)
        except Exception:
            return self._get(url, params, quiet_404=quiet_404)

    def _get(self, url: str, params: dict[str, Any] | None = None, *, quiet_404: bool = False, fresh: bool = False) -> Any:
        return self.get_response(url, params, quiet_404=quiet_404, fresh=fresh, copy=False).data

    def get_response(
        self, url: str, params: dict[str, Any] | None = None, *, quiet_404: bool = False,
        fresh: bool = False, max_age: float | None = None, timeout: float = 15,
        request: Callable[..., Any] | None = None, max_retries: int | None = None, copy: bool = True,
    ) -> TmdbResponse:
        q = dict(params or {})
        q["api_key"] = self._apikey()
        ck = url + "?" + "&".join(sorted(f"{k}={v}" for k, v in q.items()))
        h = hashlib.sha1(ck.encode("utf-8")).hexdigest()
        scope = self._cache_scope()
        retries = self._backoff_params()[0] if max_retries is None else max(0, max_retries)
        identity = (scope, h, retries, timeout, request, quiet_404)
        lock = _REQUEST_LOCKS[hash((scope, h)) % len(_REQUEST_LOCKS)]
        with lock:
            with _PROVIDERS_LOCK:
                shared = _INFLIGHT.get(identity)
                hit = self._cache.pop(h, None) if not fresh else None
                if hit is not None:
                    self._cache[h] = hit
            if hit is None and not fresh:
                hit = self._disk_read(h)
                if hit is not None:
                    self._publish(h, hit, scope)
            if hit is not None:
                timestamp, data = hit
                recent = max_age is None or (timestamp > 0 and time.time() - timestamp <= max(0, max_age))
                if recent:
                    return TmdbResponse(deepcopy(data) if copy else data, timestamp, "cache")
            pending: Future[TmdbResponse] = shared if shared is not None else Future()
            if shared is None:
                with _PROVIDERS_LOCK:
                    _INFLIGHT[identity] = pending
        if shared is not None:
            result = shared.result()
            return TmdbResponse(deepcopy(result.data), result.fetched_at, "shared")
        try:
            result = self._fetch(url, q, h, scope, lock, quiet_404=quiet_404, timeout=timeout, request=request, retries=retries)
            pending.set_result(TmdbResponse(deepcopy(result.data), result.fetched_at, "network"))
            return result
        except BaseException as exc:
            pending.set_exception(exc if isinstance(exc, Exception) else requests.ConnectionError("TMDb shared lookup was interrupted"))
            raise
        finally:
            with lock, _PROVIDERS_LOCK:
                _INFLIGHT.pop(identity, None)

    def _fetch(
        self, url: str, q: dict[str, Any], key: str, scope: str, lock: Any, *, quiet_404: bool,
        timeout: float, request: Callable[..., Any] | None, retries: int,
    ) -> TmdbResponse:
        data = self._request_json(url, q, quiet_404=quiet_404, timeout=timeout, request=request, max_retries=retries)
        fetched_at = time.time()
        show = url.removeprefix("https://api.themoviedb.org/3/tv/")
        notify = show.isascii() and show.isdecimal() and set(q) == {"api_key", "language"} and q.get("language") == "en-US" and isinstance(data, dict) and data.get("id") == int(show) and isinstance(data.get("seasons"), list)
        with lock:
            previous = (self._cache.get(key) or self._disk_read(key)) if notify else None
            self._disk_write(key, data, fetched_at)
            self._publish(key, (fetched_at, data), scope)
        if notify:
            fields = ("status", "number_of_episodes", "number_of_seasons", "next_episode_to_air")
            def signature(value):
                seasons = value.get("seasons")
                counts = [(season.get("season_number"), season.get("episode_count")) for season in seasons if isinstance(season, dict)] if isinstance(seasons, list) else []
                return tuple(value.get(field) for field in fields), counts
            if not previous or not isinstance(previous[1], dict) or signature(previous[1]) != signature(data):
                try:
                    from services.tracker_automation import metadata_changed
                    metadata_changed(show)
                except Exception as exc:
                    self._log_exc("Tracker metadata notification failed", exc)
        return TmdbResponse(data, fetched_at, "network")

    def peek_show(self, tmdb_id: str, *, max_age: float = 7 * 86400) -> TmdbResponse | None:
        identifier = str(tmdb_id)
        if not identifier.isascii() or not identifier.isdecimal() or int(identifier) <= 0:
            return None
        params = {"api_key": self._apikey(), "language": "en-US"}
        key = f"https://api.themoviedb.org/3/tv/{int(identifier)}?" + "&".join(sorted(f"{k}={v}" for k, v in params.items()))
        digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
        with _PROVIDERS_LOCK:
            hit = self._cache.get(digest)
        hit = hit or self._disk_read(digest)
        if not hit or time.time() - hit[0] > max_age:
            return None
        data = hit[1]
        if not isinstance(data, dict) or data.get("id") != int(identifier) or not isinstance(data.get("seasons"), list):
            return None
        return TmdbResponse(deepcopy(data), hit[0], "cache")

    def lookup(
        self, entity: str, tmdb_id: str | int, *, season: int | None = None,
        episode: int | None = None, locale: str | None = "en-US", fresh: bool = False,
        max_age: float | None = None, timeout: float = 15, max_retries: int | None = None,
    ) -> TmdbResponse:
        kind = {"movie": "movie", "show": "tv", "tv": "tv"}.get(entity)
        identifier = str(tmdb_id)
        if kind is None or not identifier.isascii() or not identifier.isdecimal() or int(identifier) <= 0:
            raise ValueError("A movie or show and a positive TMDb ID are required")
        if season is not None and (kind != "tv" or type(season) is not int or season < 0):
            raise ValueError("A show season must be a nonnegative integer")
        if episode is not None and (season is None or type(episode) is not int or episode <= 0):
            raise ValueError("An episode requires a season and a positive episode number")
        path = f"{kind}/{int(identifier)}"
        if season is not None:
            path += f"/season/{season}"
        if episode is not None:
            path += f"/episode/{episode}"
        result = self.get_response(f"https://api.themoviedb.org/3/{path}", {"language": locale} if locale else None,
                                   quiet_404=True, fresh=fresh, max_age=max_age, timeout=timeout, max_retries=max_retries)
        data = result.data
        if not isinstance(data, dict) or not isinstance(data.get("id"), int) or isinstance(data["id"], bool) or data["id"] <= 0:
            raise ValueError("TMDb returned incomplete metadata")
        if season is None and data["id"] != int(identifier):
            raise ValueError("TMDb returned a different title")
        if season is None and not isinstance(data.get("title" if kind == "movie" else "name"), str):
            raise ValueError("TMDb returned incomplete title metadata")
        if season is not None and (type(data.get("season_number")) is not int or data["season_number"] != season):
            raise ValueError("TMDb returned a different season")
        if episode is not None and (type(data.get("episode_number")) is not int or data["episode_number"] != episode):
            raise ValueError("TMDb returned a different episode")
        if season is not None and episode is None and not isinstance(data.get("episodes"), list):
            raise ValueError("TMDb returned an incomplete season")
        if season is not None and episode is None and any(
            not isinstance(row, dict) or type(row.get("episode_number")) is not int or row["episode_number"] <= 0
            or row.get("season_number", season) != season for row in data["episodes"]
        ):
            raise ValueError("TMDb returned incomplete episode metadata")
        return result

    def _request_json(
        self, url: str, q: dict[str, Any], *, quiet_404: bool, timeout: float,
        request: Callable[..., Any] | None, max_retries: int | None,
    ) -> Any:
        retries, base_s, max_s = self._backoff_params()
        max_retries = retries if max_retries is None else max(0, max_retries)
        attempt = 0
        while True:
            try:
                r = (request or requests.get)(
                    url,
                    params=q,
                    headers={"User-Agent": self.UA, "Accept": "application/json"},
                    timeout=timeout,
                )
                status = r.status_code

                if status == 429:
                    if attempt >= max_retries:
                        r.raise_for_status()
                    retry_after = self._seconds_from_retry_after(r.headers.get("Retry-After", ""))
                    delay = retry_after if retry_after is not None else self._retry_delay(attempt, base_s, max_s)
                    time.sleep(delay)
                    attempt += 1
                    continue

                if 500 <= status < 600:
                    if attempt >= max_retries:
                        r.raise_for_status()
                    time.sleep(self._retry_delay(attempt, base_s, max_s))
                    attempt += 1
                    continue

                r.raise_for_status()
                data = r.json()
                return data

            except requests.exceptions.RequestException as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                if (not self._retryable(e)) or (attempt >= max_retries):
                    if not (quiet_404 and int(status or 0) == 404):
                        lvl = "INFO" if int(status or 0) == 404 else "WARNING"
                        log(
                            f"TMDb request failed ({status or 'n/a'}) at {url}",
                            level=lvl,
                            module="META",
                            extra={"status": status, "url": url},
                        )
                    raise
                time.sleep(self._retry_delay(attempt, base_s, max_s))
                attempt += 1

    @staticmethod
    def _safe_int_year(s: str | None) -> int | None:
        if not s:
            return None
        if len(s) >= 4 and s[:4].isdigit():
            return int(s[:4])
        return None

    @staticmethod
    def _locale_cc(locale: str | None) -> str | None:
        if not locale:
            return None
        parts = str(locale).replace("_", "-").split("-")
        if len(parts) >= 2 and len(parts[1]) == 2:
            return parts[1].upper()
        return None

    @staticmethod
    def _pick_first(arr: list[Any] | None) -> Any | None:
        return arr[0] if isinstance(arr, list) and arr else None

    @staticmethod
    def _entity_kind(value: Any) -> str:
        text = str(value or "").strip().lower()
        return "movie" if text in {"movie", "movies", "film", "films"} else "tv"

    def find_imdb(self, imdb_id: str, entity: str) -> dict[str, str]:
        base = "https://api.themoviedb.org/3"
        kind = self._entity_kind(entity)

        def lookup(**options: bool) -> str:
            try:
                found = self._get(f"{base}/find/{imdb_id}", {"external_source": "imdb_id"}, quiet_404=True, **options)
            except requests.exceptions.RequestException as e:
                if int(getattr(getattr(e, "response", None), "status_code", 0) or 0) == 404:
                    return ""
                raise
            hit = self._pick_first(found.get(f"{kind}_results")) if isinstance(found, dict) else None
            return str(hit.get("id") or "").strip() if isinstance(hit, dict) else ""

        tmdb_id = lookup() or lookup(fresh=True)
        if not tmdb_id:
            return {}
        out = {"tmdb": tmdb_id}
        if kind == "tv":
            external = self._get(f"{base}/tv/{tmdb_id}/external_ids")
            tvdb_id = str(external.get("tvdb_id") or "").strip() if isinstance(external, dict) else ""
            if tvdb_id:
                out["tvdb"] = tvdb_id
        return out

    @staticmethod
    def _detail_titles(det: Mapping[str, Any], kind: str) -> list[str]:
        keys = ("title", "original_title") if kind == "movie" else ("name", "original_name")
        return [str(det.get(k) or "") for k in keys if det.get(k)]

    def _detail_year(self, det: Mapping[str, Any], kind: str) -> int | None:
        field = "release_date" if kind == "movie" else "first_air_date"
        return self._safe_int_year(det.get(field))

    @staticmethod
    def _expected_titles(title: str | None, year: int | None) -> set[str]:
        want = normalize_title(title)
        if not want:
            return set()
        wants = {want}
        if year is not None:
            suffix = f" {int(year)}"
            if want.endswith(suffix):
                trimmed = want[: -len(suffix)].strip()
                if trimmed:
                    wants.add(trimmed)
        return wants

    def _matches_expected(
        self,
        det: Mapping[str, Any],
        kind: str,
        title: str | None,
        year: int | None,
    ) -> bool:
        wants = self._expected_titles(title, year)
        if not wants:
            return False
        if not any(normalize_title(t) in wants for t in self._detail_titles(det, kind)):
            return False
        if year is None:
            return True
        got = self._detail_year(det, kind)
        if got is None:
            return True
        return abs(int(got) - int(year)) <= 1

    def _try_details(self, kind: str, tmdb_id: str, lang: str) -> tuple[dict[str, Any] | None, bool]:
        base = "https://api.themoviedb.org/3"
        try:
            data = self._get(f"{base}/{kind}/{tmdb_id}", {"language": lang}, quiet_404=True)
            return (data if isinstance(data, dict) else None), True
        except requests.exceptions.RequestException as e:
            status = int(getattr(getattr(e, "response", None), "status_code", 0) or 0)
            if status == 404:
                return None, True
            return None, False
        except Exception:
            return None, False

    def _find_by_external(
        self,
        external_id: str,
        source: str,
        tmdb_id: str,
    ) -> tuple[str | None, bool]:
        base = "https://api.themoviedb.org/3"
        try:
            data = self._get(f"{base}/find/{external_id}", {"external_source": source}, quiet_404=True)
        except requests.exceptions.RequestException as e:
            status = int(getattr(getattr(e, "response", None), "status_code", 0) or 0)
            if status != 404:
                return None, False
            return None, True
        except Exception:
            return None, False
        if not isinstance(data, dict):
            return None, True
        for bucket, kind in (("movie_results", "movie"), ("tv_results", "tv")):
            for row in data.get(bucket) or []:
                if isinstance(row, dict) and str(row.get("id") or "") == str(tmdb_id):
                    return kind, True
        return None, True

    def _search_confirms(
        self,
        kind: str,
        tmdb_id: str,
        title: str,
        year: int | None,
        lang: str,
    ) -> tuple[bool, bool]:
        base = "https://api.themoviedb.org/3"
        params: dict[str, Any] = {"query": title, "language": lang, "include_adult": False}
        if year is not None:
            params["primary_release_year" if kind == "movie" else "first_air_date_year"] = year
        try:
            data = self._get(f"{base}/search/{kind}", params, quiet_404=True)
        except requests.exceptions.RequestException as e:
            status = int(getattr(getattr(e, "response", None), "status_code", 0) or 0)
            if status != 404:
                return False, False
            return False, True
        except Exception:
            return False, False
        if not isinstance(data, dict):
            return False, True
        for row in data.get("results") or []:
            if not isinstance(row, dict) or str(row.get("id") or "") != str(tmdb_id):
                continue
            if self._matches_expected(row, kind, title, year):
                return True, True
        return False, True

    def resolve_namespace(
        self,
        *,
        tmdb_id: str | int,
        requested_type: str,
        title: str | None = None,
        year: int | str | None = None,
        imdb_id: str | None = None,
        tvdb_id: str | int | None = None,
        locale: str | None = None,
    ) -> dict[str, Any] | None:
        tid = str(tmdb_id or "").strip()
        if not tid.isdecimal() or int(tid) <= 0:
            return None

        requested = self._entity_kind(requested_type)
        alternate = "tv" if requested == "movie" else "movie"
        lang = locale or "en-US"

        want_title = str(title or "").strip()
        try:
            want_year: int | None = int(str(year)[:4]) if str(year or "").strip() else None
        except Exception:
            want_year = None

        if not want_title:
            return None

        req_det, req_ok = self._try_details(requested, tid, lang)
        if not req_ok:
            return None
        if req_det and self._matches_expected(req_det, requested, want_title, want_year):
            return {"resolved_type": requested, "status": "resolved", "reason": "requested_title_year_match"}

        alt_det, alt_ok = self._try_details(alternate, tid, lang)
        if not alt_ok:
            return None

        alt_match = bool(alt_det) and self._matches_expected(alt_det or {}, alternate, want_title, want_year)

        if alt_match:
            return {"resolved_type": alternate, "status": "resolved", "reason": "alternate_title_year_match"}
        if req_det and not alt_det:
            return {"resolved_type": requested, "status": "resolved", "reason": "only_requested_namespace_exists"}
        if alt_det and not req_det:
            return {"resolved_type": alternate, "status": "resolved", "reason": "only_alternate_namespace_exists"}

        external = str(imdb_id or "").strip() if requested == "movie" else str(tvdb_id or "").strip()
        source = "imdb_id" if requested == "movie" else "tvdb_id"
        if not external:
            external = str(tvdb_id or "").strip() if requested == "movie" else str(imdb_id or "").strip()
            source = "tvdb_id" if requested == "movie" else "imdb_id"
        if external:
            found, ok = self._find_by_external(external, source, tid)
            if not ok:
                return None
            if found:
                return {"resolved_type": found, "status": "resolved", "reason": f"external_id_{source}"}

        for kind in (requested, alternate):
            confirmed, ok = self._search_confirms(kind, tid, want_title, want_year, lang)
            if not ok:
                return None
            if confirmed:
                return {"resolved_type": kind, "status": "resolved", "reason": "search_title_year_match"}

        return {"resolved_type": requested, "status": "unresolved", "reason": "inconclusive"}

    def _images(self, tmdb_id: str, kind: str, lang: str, need: Mapping[str, bool]) -> dict[str, list[dict[str, Any]]]:
        want_poster = bool(need.get("poster"))
        want_back = bool(need.get("backdrop"))
        want_logo = bool(need.get("logo"))
        if not (want_poster or want_back or want_logo):
            return {}

        include = f"{lang[:2]},{lang},null" if lang else "en,null"
        if want_logo:
            include = ",".join(dict.fromkeys([*include.split(","), "en"]))
        base = "https://api.themoviedb.org/3"
        if kind == "movie":
            imgs = self._get(f"{base}/movie/{tmdb_id}/images", {"include_image_language": include})
        else:
            imgs = self._get(f"{base}/tv/{tmdb_id}/images", {"include_image_language": include})

        posters = imgs.get("posters") or []
        backs = imgs.get("backdrops") or []
        logos = imgs.get("logos") or []

        out: dict[str, list[dict[str, Any]]] = {}
        if want_poster:
            out["poster"] = [
                {
                    "url": f"{IMG_BASE}/w780{p['file_path']}",
                    "w": p.get("width"),
                    "h": p.get("height"),
                    "lang": p.get("iso_639_1"),
                }
                for p in posters
                if p.get("file_path")
            ]
        if want_back:
            out["backdrop"] = [
                {
                    "url": f"{IMG_BASE}/w1280{p['file_path']}",
                    "w": p.get("width"),
                    "h": p.get("height"),
                    "lang": p.get("iso_639_1"),
                    "vote_average": p.get("vote_average"),
                    "vote_count": p.get("vote_count"),
                }
                for p in backs
                if p.get("file_path")
            ]
        if want_logo:
            out["logo"] = [
                {
                    "url": f"{IMG_BASE}/w500{p['file_path']}",
                    "w": p.get("width"),
                    "h": p.get("height"),
                    "lang": p.get("iso_639_1"),
                }
                for p in logos
                if p.get("file_path")
            ]
        return out

    def _videos(self, tmdb_id: str, kind: str, lang: str, need: Mapping[str, bool]) -> list[dict[str, Any]]:
        if not need.get("videos"):
            return []
        include = f"{lang[:2]},{lang},null" if lang else "en,null"
        base = "https://api.themoviedb.org/3"
        if kind == "movie":
            data = self._get(f"{base}/movie/{tmdb_id}/videos", {"include_video_language": include})
        else:
            data = self._get(f"{base}/tv/{tmdb_id}/videos", {"include_video_language": include})
        out: list[dict[str, Any]] = []
        for v in data.get("results", []) or []:
            site = (v.get("site") or "").strip()
            key = (v.get("key") or "").strip()
            if not site or not key:
                continue
            out.append(
                {
                    "site": site,
                    "key": key,
                    "type": v.get("type"),
                    "official": bool(v.get("official")),
                    "name": v.get("name"),
                    "published_at": v.get("published_at"),
                }
            )
        return out

    def _movie_cert_and_release(
        self,
        tmdb_id: str,
        lang: str,
        locale: str | None,
    ) -> tuple[str | None, str | None, str | None]:
        base = "https://api.themoviedb.org/3"
        try:
            data = self._get(f"{base}/movie/{tmdb_id}/release_dates")
        except Exception as e:
            self._log_exc("TMDb release_dates failed", e)
            return None, None, None

        def pick(results: list[dict[str, Any]], cc: str | None) -> tuple[str | None, str | None]:
            rows = [r for r in results if r.get("iso_3166_1") == cc]
            if not rows:
                return None, None
            rels = rows[0].get("release_dates") or []
            theatrical = [r for r in rels if r.get("type") == 3]
            candidates = theatrical or rels
            for r in candidates:
                cert = (r.get("certification") or "").strip() or None
                date = (r.get("release_date") or "").strip() or None
                if cert or date:
                    return cert, date
            return None, None

        cc_pref = self._locale_cc(locale)
        cert: str | None = None
        date: str | None = None
        used_cc: str | None = None

        for cc in [cc_pref, "US", None]:
            if cc is None:
                all_results = data.get("results") or []
                for row in all_results:
                    c, d = pick([row], row.get("iso_3166_1"))
                    if c or d:
                        cert, date, used_cc = c, d, row.get("iso_3166_1")
                        break
                if cert or date:
                    break
            else:
                c, d = pick(data.get("results") or [], cc)
                if c or d:
                    cert, date, used_cc = c, d, cc
                    break

        return cert, date, used_cc

    def _tv_cert(self, tmdb_id: str, locale: str | None) -> tuple[str | None, str | None]:
        base = "https://api.themoviedb.org/3"
        try:
            data = self._get(f"{base}/tv/{tmdb_id}/content_ratings")
        except Exception as e:
            self._log_exc("TMDb content_ratings failed", e)
            return None, None

        cc_pref = self._locale_cc(locale)
        results = data.get("results") or []

        def pick(cc: str | None) -> tuple[str | None, str | None]:
            for r in results:
                if r.get("iso_3166_1") == cc and r.get("rating"):
                    return r.get("rating"), cc
            return None, None

        for cc in [cc_pref, "US"]:
            if not cc:
                continue
            cert, used = pick(cc)
            if cert:
                return cert, used

        for r in results:
            if r.get("rating"):
                return r.get("rating"), r.get("iso_3166_1")

        return None, None

    CREW_PRIORITY = ("Creator", "Director", "Screenplay", "Writer", "Novel", "Producer", "Original Music Composer", "Director of Photography")

    @staticmethod
    def _seasons(det: Mapping[str, Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in det.get("seasons") or []:
            if not isinstance(row, Mapping) or row.get("season_number") is None:
                continue
            out.append(
                {
                    "season": row.get("season_number"),
                    "name": row.get("name") or "",
                    "episode_count": row.get("episode_count"),
                    "air_date": row.get("air_date") or "",
                    "poster_path": row.get("poster_path") or "",
                }
            )
        return out

    @classmethod
    def _credits(cls, det: Mapping[str, Any], kind: str) -> dict[str, list[dict[str, Any]]]:
        raw_credits = det.get("credits")
        credits: Mapping[str, Any] = raw_credits if isinstance(raw_credits, Mapping) else {}
        cast: list[dict[str, Any]] = []
        for person in credits.get("cast") or []:
            if not isinstance(person, Mapping) or not person.get("name"):
                continue
            cast.append(
                {
                    "id": person.get("id"),
                    "name": person.get("name"),
                    "character": person.get("character") or "",
                    "profile_path": person.get("profile_path") or "",
                }
            )
            if len(cast) >= 18:
                break
        crew: list[dict[str, Any]] = []
        seen: set[tuple[Any, str]] = set()

        def add(person: Any, job: str) -> None:
            if not isinstance(person, Mapping) or not person.get("name") or job not in cls.CREW_PRIORITY:
                return
            key = (person.get("id"), job)
            if key in seen:
                return
            seen.add(key)
            crew.append({"id": person.get("id"), "name": person.get("name"), "job": job, "profile_path": person.get("profile_path") or ""})

        if kind == "tv":
            for person in det.get("created_by") or []:
                add(person, "Creator")
        for person in credits.get("crew") or []:
            if isinstance(person, Mapping):
                add(person, str(person.get("job") or ""))
        crew.sort(key=lambda row: cls.CREW_PRIORITY.index(row["job"]))
        return {"cast": cast, "crew": crew[:12]}

    @staticmethod
    def _recommendations(det: Mapping[str, Any], kind: str) -> list[dict[str, Any]]:
        raw = det.get("recommendations")
        block: Mapping[str, Any] = raw if isinstance(raw, Mapping) else {}
        out: list[dict[str, Any]] = []
        for row in block.get("results") or []:
            if not isinstance(row, Mapping) or not row.get("id"):
                continue
            media = str(row.get("media_type") or kind)
            if media not in {"movie", "tv"}:
                continue
            out.append(
                {
                    "id": row.get("id"),
                    "type": media,
                    "title": row.get("title") or row.get("name") or "",
                    "year": TmdbProvider._safe_int_year(row.get("release_date") or row.get("first_air_date")),
                    "poster_path": row.get("poster_path") or "",
                    "backdrop_path": row.get("backdrop_path") or "",
                    "vote_average": row.get("vote_average"),
                }
            )
            if len(out) >= 18:
                break
        return out

    def fetch(
        self,
        *,
        entity: str,
        ids: dict[str, str],
        locale: str | None = None,
        need: dict[str, bool] | None = None,
        fresh: bool = False,
        strict: bool = False,
    ) -> dict[str, Any]:
        need = need or {"poster": True, "backdrop": True}
        ent_in = (entity or "").lower().strip()
        if ent_in in {"show", "shows"}:
            ent_in = "tv"
        if ent_in not in {"movie", "tv"}:
            return {}

        tmdb_id = str(ids.get("tmdb") or ids.get("id") or "").strip()
        imdb_id = str(ids.get("imdb") or "").strip()
        tvdb_id = str(ids.get("tvdb") or ids.get("tvdb_id") or "").strip()
        title = (ids.get("title") or "").strip()
        year_in = (ids.get("year") or "").strip()

        lang = locale or "en-US"
        base = "https://api.themoviedb.org/3"

        if not tmdb_id and imdb_id:
            try:
                found = self._get(f"{base}/find/{imdb_id}", {"external_source": "imdb_id"})
                if ent_in == "movie":
                    hit = self._pick_first(found.get("movie_results") or [])
                    tmdb_id = str(hit.get("id")) if hit else ""
                else:
                    hit = self._pick_first(found.get("tv_results") or [])
                    tmdb_id = str(hit.get("id")) if hit else ""
                if not tmdb_id:
                    m = self._pick_first(found.get("movie_results") or [])
                    t = self._pick_first(found.get("tv_results") or [])
                    tmdb_id = str((m or t or {}).get("id") or "") if (m or t) else ""
                    if m and not t:
                        ent_in = "movie"
                    if t and not m:
                        ent_in = "tv"
            except Exception as e:
                self._log_exc("TMDb find by IMDb failed", e)

        if not tmdb_id and tvdb_id:
            try:
                found = self._get(f"{base}/find/{tvdb_id}", {"external_source": "tvdb_id"})
                if ent_in == "movie":
                    hit = self._pick_first(found.get("movie_results") or [])
                else:
                    hit = self._pick_first(found.get("tv_results") or [])
                tmdb_id = str(hit.get("id")) if hit else ""
                if not tmdb_id:
                    m = self._pick_first(found.get("movie_results") or [])
                    t = self._pick_first(found.get("tv_results") or [])
                    tmdb_id = str((m or t or {}).get("id") or "") if (m or t) else ""
                    if m and not t:
                        ent_in = "movie"
                    if t and not m:
                        ent_in = "tv"
            except Exception as e:
                self._log_exc("TMDb find by TVDB failed", e)

        title_guess = not tmdb_id and bool(imdb_id or tvdb_id)
        if not tmdb_id and title:
            try:
                if ent_in == "movie":
                    q: dict[str, Any] = {"query": title, "language": lang}
                    if year_in:
                        q["year"] = year_in
                    res = self._get(f"{base}/search/movie", q)
                    hit = self._pick_first(res.get("results") or [])
                    tmdb_id = str(hit.get("id")) if hit else ""
                else:
                    q = {"query": title, "language": lang}
                    if year_in:
                        q["first_air_date_year"] = year_in
                    res = self._get(f"{base}/search/tv", q)
                    hit = self._pick_first(res.get("results") or [])
                    tmdb_id = str(hit.get("id")) if hit else ""
            except Exception as e:
                self._log_exc("TMDb search failed", e)

        if not tmdb_id:
            return {}

        det: dict[str, Any] | None = None
        kind = "movie" if ent_in == "movie" else "tv"

        appends = [
            part
            for flag, part in (("credits", "credits"), ("recommendations", "recommendations"))
            if need.get(flag)
        ]

        def _get_details(k: str) -> dict[str, Any]:
            params: dict[str, Any] = {"language": lang}
            if appends:
                params["append_to_response"] = ",".join(appends)
            if fresh and strict:
                return self._get(f"{base}/{k}/{tmdb_id}", params, fresh=True)
            return self.get_latest(f"{base}/{k}/{tmdb_id}", params) if fresh else self._get(f"{base}/{k}/{tmdb_id}", params)

        try:
            det = _get_details(kind)
        except requests.exceptions.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if int(status or 0) == 404:
                alt = "tv" if kind == "movie" else "movie"
                self._debug_log(
                    "tmdb_detail_fallback",
                    tmdb_id=tmdb_id,
                    requested=kind,
                    fallback=alt,
                    entity=entity,
                )
                try:
                    det = _get_details(alt)
                    kind = alt
                    self._debug_log(
                        "tmdb_detail_fallback_hit",
                        tmdb_id=tmdb_id,
                        resolved=kind,
                    )
                except Exception as e2:
                    self._log_exc("TMDb detail fetch failed", e2)
                    if strict and int(getattr(getattr(e2, "response", None), "status_code", 0) or 0) != 404:
                        raise
                    return {}
            else:
                self._log_exc("TMDb detail fetch failed", e)
                if strict:
                    raise
                return {}
        except Exception as e:
            self._log_exc("TMDb detail fetch failed", e)
            if strict:
                raise
            return {}

        vote_count_raw = det.get("vote_count")
        vote_count = (
            int(vote_count_raw)
            if isinstance(vote_count_raw, (int, float))
            and not isinstance(vote_count_raw, bool)
            and vote_count_raw >= 0
            else None
        )

        if kind == "movie":
            title_out = det.get("title") or det.get("original_title")
            year = self._safe_int_year(det.get("release_date"))
            runtime = det.get("runtime")
            runtime_minutes = runtime if isinstance(runtime, int) and runtime > 0 else None
            overview = det.get("overview") if need.get("overview") else None
            tagline = det.get("tagline") if need.get("tagline") else None
            genres = (
                [g["name"] for g in (det.get("genres") or []) if isinstance(g, dict) and g.get("name")]
                if need.get("genres")
                else None
            )
            vote_avg = det.get("vote_average")
            score = round(float(vote_avg) * 10) if isinstance(vote_avg, (int, float)) else None
            detail: dict[str, Any] = {"release_date": det.get("release_date"), "vote_average": det.get("vote_average"), "belongs_to_collection": det.get("belongs_to_collection")}
        else:
            title_out = det.get("name") or det.get("original_name")
            year = self._safe_int_year(det.get("first_air_date"))
            run_list = det.get("episode_run_time") or []
            ep_runtime = next((x for x in run_list if isinstance(x, int) and x > 0), None)
            runtime_minutes = ep_runtime
            overview = det.get("overview") if need.get("overview") else None
            tagline = det.get("tagline") if need.get("tagline") else None
            genres = (
                [g["name"] for g in (det.get("genres") or []) if isinstance(g, dict) and g.get("name")]
                if need.get("genres")
                else None
            )
            vote_avg = det.get("vote_average")
            score = round(float(vote_avg) * 10) if isinstance(vote_avg, (int, float)) else None
            detail = {
                "first_air_date": det.get("first_air_date"),
                "episode_run_time": run_list,
                "status": det.get("status"),
                "number_of_seasons": det.get("number_of_seasons"),
                "number_of_episodes": det.get("number_of_episodes"),
                "next_episode_to_air": det.get("next_episode_to_air"),
                "last_episode_to_air": det.get("last_episode_to_air"),
                "vote_average": det.get("vote_average"),
                "seasons": self._seasons(det),
            }

        images: dict[str, list[dict[str, Any]]] = {}
        try:
            images = self._images(tmdb_id, kind, lang, need)
        except Exception as e:
            self._log_exc("TMDb images fetch failed", e)

        videos: list[dict[str, Any]] = []
        try:
            videos = self._videos(tmdb_id, kind, lang, need)
        except Exception as e:
            self._log_exc("TMDb videos fetch failed", e)

        extra_ids: dict[str, Any] = {}
        if need.get("ids") and not title_guess:
            try:
                if kind == "movie":
                    data = self._get(f"{base}/movie/{tmdb_id}/external_ids")
                    imdb_out = data.get("imdb_id")
                    if imdb_out:
                        extra_ids["imdb"] = imdb_out
                else:
                    data = self._get(f"{base}/tv/{tmdb_id}/external_ids")
                    imdb_out = data.get("imdb_id")
                    tvdb_out = data.get("tvdb_id")
                    if imdb_out:
                        extra_ids["imdb"] = imdb_out
                    if tvdb_out:
                        extra_ids["tvdb"] = tvdb_out
            except Exception as e:
                self._log_exc("TMDb external IDs fetch failed", e)

        certification: str | None = None
        release_iso: str | None = None
        release_cc: str | None = None
        try:
            if kind == "movie" and (need.get("certification") or need.get("release")):
                certification, release_iso, release_cc = self._movie_cert_and_release(tmdb_id, lang, locale)
            elif kind == "tv" and need.get("certification"):
                certification, release_cc = self._tv_cert(tmdb_id, locale)
        except Exception as e:
            self._log_exc("TMDb certification/release failed", e)

        out: dict[str, Any] = {
            "type": "movie" if kind == "movie" else "tv",
            "ids": {} if title_guess else {"tmdb": str(tmdb_id), **extra_ids},
            "title": title_out,
            "year": year,
            "runtime_minutes": runtime_minutes,
            "vote_count": vote_count,
            "images": images or {},
            "detail": detail,
        }
        if overview:
            out["overview"] = overview
        if tagline:
            out["tagline"] = tagline
        if genres is not None:
            out["genres"] = genres
        if score is not None:
            out["score"] = score
        if need.get("videos"):
            out["videos"] = videos
        if certification:
            out["certification"] = certification
        if release_iso:
            out["release"] = {"date": release_iso, "country": release_cc}
        if need.get("credits"):
            out["credits"] = self._credits(det, kind)
        if need.get("recommendations"):
            out["recommendations"] = self._recommendations(det, kind)

        return out


def build(
    load_cfg: Callable[[], dict[str, Any]],
    save_cfg: Callable[[dict[str, Any]], None],
) -> TmdbProvider:
    return TmdbProvider(load_cfg, save_cfg)


PROVIDER = TmdbProvider


def html() -> str:
    return r'''<div class="section" id="sec-tmdb">
  <style>
    #sec-tmdb details.advanced { border: 1px dashed var(--border); border-radius: 12px; background: #0b0d12; padding: 8px 10px; margin-top: 4px; }
    #sec-tmdb details.advanced summary { cursor: pointer; font-weight: 700; opacity: .9; list-style: none; }
    #sec-tmdb details.advanced .adv-wrap { margin-top: 10px; display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  </style>

  <div class="head" data-toggle-section="sec-tmdb">
    <span class="chev"></span><strong>The Movie Database (TMDb)</strong>
  </div>

  <div class="body">
    <div class="grid2">
      <div style="grid-column:1 / -1">
        <label for="tmdb_api_key">TMDb API key</label>
        <input id="tmdb_api_key" name="tmdb_api_key" placeholder="Your TMDb API key" oninput="this.dataset.dirty='1'; updateTmdbHint()">
        <div id="tmdb_hint" class="msg warn hidden">
          TMDb is optional but highly recommended.
          Get an API key at <a href="https://www.themoviedb.org/settings/api" target="_blank" rel="noopener">TMDb API settings</a>
        </div>
        <div class="sub">This product uses the TMDb API but is not endorsed by TMDb.</div>
        <div class="sep"></div>
      </div>

      <details class="advanced" id="tmdb_advanced" style="grid-column:1 / -1">
        <summary>Advanced</summary>
        <div class="adv-wrap">

          <div>
            <label for="metadata_locale">Default locale (metadata.locale)</label>
            <input id="metadata_locale" name="metadata_locale" placeholder="e.g. nl-NL or en-US" oninput="this.dataset.dirty='1'">
            <div class="sub">Used when a request doesn't pass ?locale=. Falls back to UI locale if empty.</div>
          </div>

        </div>
      </details>
    </div>
    <div class="sep"></div>
  </div>
</div>'''
