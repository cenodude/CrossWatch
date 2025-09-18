# providers/metadata/_meta_TMDB.py
from __future__ import annotations

from typing import Any, Dict, Optional, Mapping, List, Tuple
import hashlib
import time
import random
import requests
from email.utils import parsedate_to_datetime

from _logging import log

IMG_BASE = "https://image.tmdb.org/t/p"


class TmdbProvider:
    """TMDb metadata provider with TTL cache, backoff, and need-aware fetching."""

    name = "TMDB"
    UA = "Crosswatch/1.0"

    def __init__(self, load_cfg, save_cfg) -> None:
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self._cache: dict[str, tuple[float, Any]] = {}  # sha1 -> (ts, payload)

    # --------------------------- config helpers ---------------------------

    def _apikey(self) -> str:
        """Read API key from either tmdb.api_key or metadata.tmdb_api_key."""
        cfg = self.load_cfg() or {}
        tmdb = cfg.get("tmdb") or {}
        md = cfg.get("metadata") or {}
        api_key = (tmdb.get("api_key") or md.get("tmdb_api_key") or "").strip()
        if not api_key:
            raise RuntimeError("TMDb API key is missing")
        return api_key

    def _ttl_seconds(self) -> int:
        """Metadata TTL; defaults to 6h if not configured."""
        cfg = self.load_cfg() or {}
        md = cfg.get("metadata") or {}
        hours = md.get("ttl_hours", 6)
        try:
            hours = int(hours)
        except Exception:
            hours = 6
        return max(1, hours) * 3600

    def _backoff_params(self) -> tuple[int, float, float]:
        """Return (max_retries, base_sec, max_sec) for HTTP backoff."""
        cfg = self.load_cfg() or {}
        md = cfg.get("metadata") or {}
        max_retries = int(md.get("backoff_max_retries", 4))
        base_ms = int(md.get("backoff_base_ms", 500))
        max_ms = int(md.get("backoff_max_ms", 4000))
        return max(0, max_retries), max(0.05, base_ms / 1000.0), max(0.1, max_ms / 1000.0)

    # --------------------------- HTTP + caching ---------------------------

    def _retry_delay(self, attempt: int, base_s: float, max_s: float) -> float:
        """Exponential backoff with jitter."""
        delay = min(max_s, base_s * (2 ** attempt))
        return delay + random.uniform(0.0, 0.25)

    def _seconds_from_retry_after(self, header: str) -> Optional[float]:
        """Parse Retry-After header as seconds or HTTP-date."""
        if not header:
            return None
        header = header.strip()
        if header.isdigit():
            return float(header)
        try:
            dt = parsedate_to_datetime(header)
            return max(0.0, (dt.timestamp() - time.time()))
        except Exception:
            return None

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        GET with TTL cache + respectful retry/backoff.
        Retries: 429 (honors Retry-After), 5xx, timeouts. No retry for other 4xx.
        """
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
                    delay = retry_after if (retry_after is not None) else self._retry_delay(attempt, base_s, max_s)
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
                    log(f"TMDb request failed ({status or 'n/a'}) at {url}", level="WARNING", module="META")
                    raise
                time.sleep(self._retry_delay(attempt, base_s, max_s))
                attempt += 1

    # --------------------------- normalize helpers ---------------------------

    @staticmethod
    def _safe_int_year(s: Optional[str]) -> Optional[int]:
        if not s:
            return None
        if len(s) >= 4 and s[:4].isdigit():
            return int(s[:4])
        return None

    @staticmethod
    def _locale_cc(locale: Optional[str]) -> Optional[str]:
        """Return ISO-3166 country from locale like 'nl-NL' -> 'NL'."""
        if not locale:
            return None
        parts = str(locale).replace("_", "-").split("-")
        if len(parts) >= 2 and len(parts[1]) == 2:
            return parts[1].upper()
        return None

    def _images(self, tmdb_id: str, kind: str, lang: str, need: Mapping[str, bool]) -> Dict[str, list]:
        """Fetch posters/backdrops/logos only if requested."""
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

        out: Dict[str, list] = {}
        if want_poster:
            out["poster"] = [
                {"url": f"{IMG_BASE}/w780{p['file_path']}", "w": p.get("width"), "h": p.get("height"), "lang": p.get("iso_639_1")}
                for p in posters if p.get("file_path")
            ]
        if want_back:
            out["backdrop"] = [
                {"url": f"{IMG_BASE}/w1280{p['file_path']}", "w": p.get("width"), "h": p.get("height"), "lang": p.get("iso_639_1")}
                for p in backs if p.get("file_path")
            ]
        if want_logo:
            out["logo"] = [
                {"url": f"{IMG_BASE}/w500{p['file_path']}", "w": p.get("width"), "h": p.get("height"), "lang": p.get("iso_639_1")}
                for p in logos if p.get("file_path")
            ]
        return out

    def _videos(self, tmdb_id: str, kind: str, lang: str, need: Mapping[str, bool]) -> List[Dict[str, Any]]:
        """Fetch trailers/teasers if requested; normalized shape."""
        if not need.get("videos"):
            return []
        include = f"{lang[:2]},{lang},null" if lang else "en,null"
        base = "https://api.themoviedb.org/3"
        if kind == "movie":
            data = self._get(f"{base}/movie/{tmdb_id}/videos", {"include_video_language": include})
        else:
            data = self._get(f"{base}/tv/{tmdb_id}/videos", {"include_video_language": include})
        out = []
        for v in data.get("results", []) or []:
            site = (v.get("site") or "").strip()
            key = (v.get("key") or "").strip()
            if not site or not key:
                continue
            out.append({
                "site": site,                # YouTube | Vimeo
                "key": key,                  # video key for embed
                "type": v.get("type"),       # Trailer | Teaser | ...
                "official": bool(v.get("official")),
                "name": v.get("name"),
                "published_at": v.get("published_at"),
            })
        return out

    def _movie_cert_and_release(self, tmdb_id: str, lang: str, locale: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Return (certification, best_release_iso, country_used) for movies.
        Prefers country from locale; fallbacks to US; then any.
        """
        base = "https://api.themoviedb.org/3"
        try:
            data = self._get(f"{base}/movie/{tmdb_id}/release_dates")
        except Exception as e:
            log(f"TMDb release_dates failed: {e}", level="WARNING", module="META")
            return None, None, None

        def pick(results, cc) -> Tuple[Optional[str], Optional[str]]:
            # Prefer type=3 (Theatrical), else first with certification/date.
            rows = [r for r in results if (r.get("iso_3166_1") == cc)]
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
        cert, date = (None, None)
        used_cc = None

        for cc in [cc_pref, "US", None]:
            if cc is None:
                # fallback: any country
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

    def _tv_cert(self, tmdb_id: str, locale: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        """Return (certification, country_used) for TV."""
        base = "https://api.themoviedb.org/3"
        try:
            data = self._get(f"{base}/tv/{tmdb_id}/content_ratings")
        except Exception as e:
            log(f"TMDb content_ratings failed: {e}", level="WARNING", module="META")
            return None, None

        cc_pref = self._locale_cc(locale)
        results = data.get("results") or []

        def pick(cc):
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

        # any fallback
        for r in results:
            if r.get("rating"):
                return r.get("rating"), r.get("iso_3166_1")

        return None, None

    # --------------------------- provider API ---------------------------

    def fetch(
        self,
        *,
        entity: str,
        ids: Dict[str, str],
        locale: Optional[str] = None,
        need: Optional[Dict[str, bool]] = None,
    ) -> dict:
        """Fetch TMDb metadata for a movie or TV show."""
        need = need or {"poster": True, "backdrop": True}
        entity = (entity or "").lower().strip()
        if entity not in ("movie", "show", "tv"):
            return {}

        tmdb_id = (ids.get("tmdb") or ids.get("id") or "").strip()
        if not tmdb_id:
            return {}

        lang = (locale or "en-US")
        base = "https://api.themoviedb.org/3"

        # ---- 1) Details ----
        try:
            if entity == "movie":
                det = self._get(f"{base}/movie/{tmdb_id}", {"language": lang})
                title = det.get("title") or det.get("original_title")
                year = self._safe_int_year(det.get("release_date"))
                runtime = det.get("runtime")
                runtime_minutes = runtime if isinstance(runtime, int) and runtime > 0 else None
                overview = det.get("overview") if need.get("overview") else None
                tagline = det.get("tagline") if need.get("tagline") else None
                genres = [g["name"] for g in (det.get("genres") or []) if isinstance(g, dict) and g.get("name")] if need.get("genres") else None
                vote_avg = det.get("vote_average")
                score = round(float(vote_avg) * 10) if (isinstance(vote_avg, (int, float))) else None
                detail = {"release_date": det.get("release_date")}
                kind = "movie"
            else:
                det = self._get(f"{base}/tv/{tmdb_id}", {"language": lang})
                title = det.get("name") or det.get("original_name")
                year = self._safe_int_year(det.get("first_air_date"))
                run_list = det.get("episode_run_time") or []
                ep_runtime = next((x for x in run_list if isinstance(x, int) and x > 0), None)
                runtime_minutes = ep_runtime
                overview = det.get("overview") if need.get("overview") else None
                tagline = det.get("tagline") if need.get("tagline") else None
                genres = [g["name"] for g in (det.get("genres") or []) if isinstance(g, dict) and g.get("name")] if need.get("genres") else None
                vote_avg = det.get("vote_average")
                score = round(float(vote_avg) * 10) if (isinstance(vote_avg, (int, float))) else None
                detail = {
                    "first_air_date": det.get("first_air_date"),
                    "episode_run_time": run_list,
                    "number_of_seasons": det.get("number_of_seasons"),
                }
                kind = "tv"
        except Exception as e:
            log(f"TMDb detail fetch failed: {e}", level="WARNING", module="META")
            return {}

        # ---- 2) Images ----
        images = {}
        try:
            images = self._images(tmdb_id, kind, lang, need)
        except Exception as e:
            log(f"TMDb images fetch failed: {e}", level="WARNING", module="META")

        # ---- 3) Videos (trailers) ----
        videos = []
        try:
            videos = self._videos(tmdb_id, kind, lang, need)
        except Exception as e:
            log(f"TMDb videos fetch failed: {e}", level="WARNING", module="META")

        # ---- 4) External IDs ----
        extra_ids = {}
        if need.get("ids"):
            try:
                if kind == "movie":
                    data = self._get(f"{base}/movie/{tmdb_id}/external_ids")
                    imdb_id = data.get("imdb_id")
                    if imdb_id: extra_ids["imdb"] = imdb_id
                else:
                    data = self._get(f"{base}/tv/{tmdb_id}/external_ids")
                    imdb_id = data.get("imdb_id")
                    tvdb_id = data.get("tvdb_id")
                    if imdb_id: extra_ids["imdb"] = imdb_id
                    if tvdb_id: extra_ids["tvdb"] = tvdb_id
            except Exception as e:
                log(f"TMDb external IDs fetch failed: {e}", level="WARNING", module="META")

        # ---- 5) Certifications & release date ----
        certification = None
        release_iso = None
        release_cc = None
        try:
            if kind == "movie" and (need.get("certification") or need.get("release")):
                certification, release_iso, release_cc = self._movie_cert_and_release(tmdb_id, lang, locale)
            elif kind == "tv" and need.get("certification"):
                certification, release_cc = self._tv_cert(tmdb_id, locale)
                # TV release date stays first_air_date (already in detail)
        except Exception as e:
            log(f"TMDb certification/release failed: {e}", level="WARNING", module="META")

        # ---- Assemble payload ----
        out: Dict[str, Any] = {
            "type": "movie" if kind == "movie" else "show",
            "ids": {"tmdb": str(tmdb_id), **extra_ids} if extra_ids else {"tmdb": str(tmdb_id)},
            "title": title,
            "year": year,
            "runtime_minutes": runtime_minutes,
            "images": images or {},
            "detail": detail,
        }
        if overview: out["overview"] = overview
        if tagline: out["tagline"] = tagline
        if genres is not None: out["genres"] = genres
        if score is not None: out["score"] = score  # 0..100
        if videos: out["videos"] = videos
        if certification: out["certification"] = certification
        if release_iso: out["release"] = {"date": release_iso, "country": release_cc}

        return out


# Discovery hook
def build(load_cfg, save_cfg):
    return TmdbProvider(load_cfg, save_cfg)


# Optional singleton
PROVIDER = TmdbProvider


def html() -> str:
    # Settings UI for TMDb + Metadata
    return r'''<div class="section" id="sec-tmdb">
  <div class="head" onclick="toggleSection('sec-tmdb')">
    <span class="chev"></span><strong>TMDb & Metadata</strong>
  </div>
  <div class="body">
    <div class="grid2">
      <div style="grid-column:1 / -1">
        <label>TMDb API key</label>
        <input id="tmdb_api_key" placeholder="Your TMDb API key" oninput="this.dataset.dirty='1'; updateTmdbHint()">
        <div id="tmdb_hint" class="msg warn hidden">
          TMDb is optional but recommended to enrich posters & metadata in the preview.
          Get an API key at
          <a href="https://www.themoviedb.org/settings/api" target="_blank" rel="noopener">TMDb API settings</a>.
        </div>
        <div class="sub">This product uses the TMDb API but is not endorsed by TMDb.</div>
        <div class="sep"></div>
      </div>

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

      <div>
        <label>Bulk max (metadata.bulk_max)</label>
        <input id="metadata_bulk_max" type="number" min="1" step="1" placeholder="300" oninput="this.dataset.dirty='1'">
        <div class="sub">Safety cap for <code>/api/metadata/bulk</code> items. Default 300.</div>
      </div>

      <div>
        <label>Backoff max retries (metadata.backoff_max_retries)</label>
        <input id="metadata_backoff_max_retries" type="number" min="0" step="1" placeholder="4" oninput="this.dataset.dirty='1'">
        <div class="sub">429/5xx retries before failing.</div>
      </div>

      <div>
        <label>Backoff base (ms) (metadata.backoff_base_ms)</label>
        <input id="metadata_backoff_base_ms" type="number" min="100" step="50" placeholder="500" oninput="this.dataset.dirty='1'">
        <div class="sub">Base delay for exponential backoff.</div>
      </div>

      <div>
        <label>Backoff max (ms) (metadata.backoff_max_ms)</label>
        <input id="metadata_backoff_max_ms" type="number" min="200" step="100" placeholder="4000" oninput="this.dataset.dirty='1'">
        <div class="sub">Upper bound for backoff delay.</div>
      </div>

      <div style="grid-column:1 / -1">
        <label>Provider priority (metadata.priority)</label>
        <input id="metadata_priority" placeholder="CSV, e.g. TMDB,TRAKT" oninput="this.dataset.dirty='1'">
        <div class="sub">Order of providers for resolve(); first non-empty wins (merge mode still aggregates images).</div>
      </div>
    </div>
    <div class="sep"></div>
  </div>
</div>'''