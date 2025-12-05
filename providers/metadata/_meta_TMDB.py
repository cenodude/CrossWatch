# providers/metadata/_meta_TMDB.py
# CrossWatch - TMDb Metadata Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import random
import time
from collections.abc import Mapping
from email.utils import parsedate_to_datetime
from typing import Any, Callable

import requests

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

class TmdbProvider:
    name = "TMDB"
    UA = "CrossWatch/1.0"

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

    def _apikey(self) -> str:
        cfg = self.load_cfg() or {}
        tmdb = cfg.get("tmdb") or {}
        md = cfg.get("metadata") or {}
        api_key = (tmdb.get("api_key") or md.get("tmdb_api_key") or "").strip()
        if not api_key:
            raise RuntimeError("TMDb API key is missing")
        return api_key

    def _ttl_seconds(self) -> int:
        cfg = self.load_cfg() or {}
        md = cfg.get("metadata") or {}
        hours = md.get("ttl_hours", 6)
        try:
            hours = int(hours)
        except Exception:
            hours = 6
        return max(1, hours) * 3600

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

    def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        q = dict(params or {})
        q["api_key"] = self._apikey()
        ck = url + "?" + "&".join(sorted(f"{k}={v}" for k, v in q.items()))
        h = hashlib.sha1(ck.encode("utf-8")).hexdigest()

        now = time.time()
        hit = self._cache.get(h)
        if hit and (now - hit[0]) < self._ttl_seconds():
            return hit[1]

        max_retries, base_s, max_s = self._backoff_params()
        attempt = 0
        while True:
            try:
                r = requests.get(
                    url,
                    params=q,
                    headers={"User-Agent": self.UA, "Accept": "application/json"},
                    timeout=15,
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
                self._cache[h] = (time.time(), data)
                return data

            except requests.exceptions.RequestException as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                retryable = (status == 429) or (status is None) or (500 <= int(status or 0) < 600)
                if (not retryable) or (attempt >= max_retries):
                    lvl = "INFO" if int(status or 0) == 404 else "WARNING"
                    log(f"TMDb request failed ({status or 'n/a'}) at {url}", level=lvl, module="META")
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

    def _images(self, tmdb_id: str, kind: str, lang: str, need: Mapping[str, bool]) -> dict[str, list[dict[str, Any]]]:
        want_poster = bool(need.get("poster"))
        want_back = bool(need.get("backdrop"))
        want_logo = bool(need.get("logo"))
        if not (want_poster or want_back or want_logo):
            return {}

        include = f"{lang[:2]},{lang},null" if lang else "en,null"
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

    def fetch(
        self,
        *,
        entity: str,
        ids: dict[str, str],
        locale: str | None = None,
        need: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
        need = need or {"poster": True, "backdrop": True}
        ent_in = (entity or "").lower().strip()
        if ent_in in {"show", "shows"}:
            ent_in = "tv"
        if ent_in not in {"movie", "tv"}:
            return {}

        tmdb_id = str(ids.get("tmdb") or ids.get("id") or "").strip()
        imdb_id = (ids.get("imdb") or "").strip()
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

        def _get_details(k: str) -> dict[str, Any]:
            return self._get(f"{base}/{k}/{tmdb_id}", {"language": lang})

        try:
            det = _get_details(kind)
        except requests.exceptions.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if int(status or 0) == 404:
                alt = "tv" if kind == "movie" else "movie"
                try:
                    det = _get_details(alt)
                    kind = alt
                except Exception as e2:
                    self._log_exc("TMDb detail fetch failed", e2)
                    return {}
            else:
                self._log_exc("TMDb detail fetch failed", e)
                return {}
        except Exception as e:
            self._log_exc("TMDb detail fetch failed", e)
            return {}

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
            detail: dict[str, Any] = {"release_date": det.get("release_date")}
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
                "number_of_seasons": det.get("number_of_seasons"),
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
        if need.get("ids"):
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
            "ids": {"tmdb": str(tmdb_id), **extra_ids} if extra_ids else {"tmdb": str(tmdb_id)},
            "title": title_out,
            "year": year,
            "runtime_minutes": runtime_minutes,
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
        if videos:
            out["videos"] = videos
        if certification:
            out["certification"] = certification
        if release_iso:
            out["release"] = {"date": release_iso, "country": release_cc}

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

  <div class="head" onclick="toggleSection('sec-tmdb')">
    <span class="chev"></span><strong>The Movie Database (TMDb)</strong>
  </div>

  <div class="body">
    <div class="grid2">
      <div style="grid-column:1 / -1">
        <label>TMDb API key</label>
        <input id="tmdb_api_key" placeholder="Your TMDb API key" oninput="this.dataset.dirty='1'; updateTmdbHint()">
        <div id="tmdb_hint" class="msg warn hidden">
          TMDb is optional but recommended to enrich posters &amp; metadata in the preview.
          Get an API key at
          <a href="https://www.themoviedb.org/settings/api" target="_blank" rel="noopener">TMDb API settings</a>.
        </div>
        <div class="sub">This product uses the TMDb API but is not endorsed by TMDb.</div>
        <div class="sep"></div>
      </div>

      <details class="advanced" id="tmdb_advanced" style="grid-column:1 / -1">
        <summary>Advanced</summary>
        <div class="adv-wrap">

          <div>
            <label>Default locale (metadata.locale)</label>
            <input id="metadata_locale" placeholder="e.g. nl-NL or en-US" oninput="this.dataset.dirty='1'">
            <div class="sub">Used when a request doesn't pass ?locale=. Falls back to UI locale if empty.</div>
          </div>

          <div>
            <label>TTL hours (metadata.ttl_hours)</label>
            <input id="metadata_ttl_hours" type="number" min="1" step="1" placeholder="6" oninput="this.dataset.dirty='1'">
            <div class="sub">Resolver cache freshness (coarse). Default 6h.</div>
          </div>

        </div>
      </details>
    </div>
    <div class="sep"></div>
  </div>
</div>'''
