# providers/metadata/_meta_TMDB.py
from __future__ import annotations

from typing import Any, Dict, Optional, MutableMapping, Mapping
import hashlib
import time
import requests

from _logging import log


IMG_BASE = "https://image.tmdb.org/t/p"


class TmdbProvider:
    name = "TMDB"
    UA = "Crosswatch/1.0"

    def __init__(self, load_cfg, save_cfg) -> None:
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self._cache: dict[str, Any] = {}

    # ---- config helpers ----
    def _apikey(self) -> str:
        cfg = self.load_cfg() or {}
        md = cfg.get("tmdb") or cfg.get("metadata") or {}
        # support either {"tmdb":{"api_key":...}} or {"metadata":{"tmdb_api_key":...}}
        api_key = (md.get("api_key") or md.get("tmdb_api_key") or "").strip()
        if not api_key:
            raise RuntimeError("TMDb API key is missing")
        return api_key

    # ---- tiny GET with cache ----
    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        q = dict(params or {})
        q["api_key"] = self._apikey()
        ck = url + "?" + "&".join(sorted(f"{k}={v}" for k, v in q.items()))
        h = hashlib.sha1(ck.encode("utf-8")).hexdigest()
        if h in self._cache:
            return self._cache[h]
        r = requests.get(url, params=q, headers={"User-Agent": self.UA, "Accept": "application/json"}, timeout=12)
        r.raise_for_status()
        data = r.json()
        self._cache[h] = data
        return data

    # ---- normalize helpers ----
    @staticmethod
    def _safe_int_year(s: Optional[str]) -> Optional[int]:
        if not s: return None
        if len(s) >= 4 and s[:4].isdigit():
            return int(s[:4])
        return None

    def _images(self, tmdb_id: str, kind: str, lang: str, need: Mapping[str, bool]) -> Dict[str, list]:
        out: Dict[str, list] = {}
        include = f"{lang[:2]},{lang},null" if lang else "en,null"
        if kind == "movie":
            imgs = self._get(f"https://api.themoviedb.org/3/movie/{tmdb_id}/images", {"include_image_language": include})
        else:
            imgs = self._get(f"https://api.themoviedb.org/3/tv/{tmdb_id}/images", {"include_image_language": include})
        posters = imgs.get("posters") or []
        backs = imgs.get("backdrops") or []
        logos = imgs.get("logos") or []

        if need.get("poster"):
            out["poster"] = [
                {"url": f"{IMG_BASE}/w780{p['file_path']}", "w": p.get("width"), "h": p.get("height"), "lang": p.get("iso_639_1")}
                for p in posters if p.get("file_path")
            ]
        if need.get("backdrop"):
            out["backdrop"] = [
                {"url": f"{IMG_BASE}/w1280{p['file_path']}", "w": p.get("width"), "h": p.get("height"), "lang": p.get("iso_639_1")}
                for p in backs if p.get("file_path")
            ]
        if need.get("logo"):
            out["logo"] = [
                {"url": f"{IMG_BASE}/w500{p['file_path']}", "w": p.get("width"), "h": p.get("height"), "lang": p.get("iso_639_1")}
                for p in logos if p.get("file_path")
            ]
        return out

    # ---- provider API ----
    def fetch(self, *, entity: str, ids: Dict[str, str], locale: Optional[str] = None,
            need: Optional[Dict[str, bool]] = None) -> dict:
        need = need or {"poster": True, "backdrop": True}
        entity = (entity or "").lower().strip()
        # accept tv as alias for show
        if entity not in ("movie", "show", "tv"):
            return {}
        tmdb_id = ids.get("tmdb") or ids.get("id") or ""
        if not tmdb_id:
            return {}

        lang = (locale or "en-US")
        base = "https://api.themoviedb.org/3"
        try:
            if entity == "movie":
                det = self._get(f"{base}/movie/{tmdb_id}", {"language": lang})
                title = det.get("title") or det.get("original_title")
                year = self._safe_int_year(det.get("release_date"))
                runtime = det.get("runtime")
                runtime_minutes = runtime if isinstance(runtime, int) and runtime > 0 else None
                detail = {"release_date": det.get("release_date"), "runtime": runtime}
            else:
                det = self._get(f"{base}/tv/{tmdb_id}", {"language": lang})
                title = det.get("name") or det.get("original_name")
                year = self._safe_int_year(det.get("first_air_date"))
                run_list = det.get("episode_run_time") or []
                ep_runtime = next((x for x in run_list if isinstance(x, int) and x > 0), None)
                runtime_minutes = ep_runtime
                detail = {
                    "first_air_date": det.get("first_air_date"),
                    "episode_run_time": run_list,
                    "number_of_seasons": det.get("number_of_seasons"),
                }

        except Exception as e:
            log(f"TMDb detail fetch failed: {e}", level="WARNING", module="META")
            return {}

        out: Dict[str, Any] = {
            "type": "movie" if entity == "movie" else "show",
            "ids": {"tmdb": str(tmdb_id)},
            "title": title,
            "year": year,
            "runtime_minutes": runtime_minutes,
            "images": {},
            "detail": detail,
        }

        try:
            imgs = self._images(str(tmdb_id), "movie" if entity == "movie" else "tv", lang, need)
            if imgs: out["images"] = imgs
        except Exception as e:
            log(f"TMDb images fetch failed: {e}", level="WARNING", module="META")

        return out


# Discovery hook
def build(load_cfg, save_cfg):
    return TmdbProvider(load_cfg, save_cfg)

# Optional singleton export for direct imports
PROVIDER = TmdbProvider  # allow manager to call build, or importers to instantiate


def html() -> str:
    # UI for TMDb settings (Settings → Metadata)
    return r'''<div class="section" id="sec-tmdb">
  <div class="head" onclick="toggleSection('sec-tmdb')">
    <span class="chev"></span><strong>TMDb</strong>
  </div>
  <div class="body">
    <div class="grid2">
      <div style="grid-column:1 / -1">
        <label>API key</label>
        <input id="tmdb_api_key" placeholder="Your TMDb API key" oninput="this.dataset.dirty='1'; updateTmdbHint()">
        <div id="tmdb_hint" class="msg warn hidden">
          TMDb is optional but recommended to enrich posters & metadata in the preview.
          Get an API key at
          <a href="https://www.themoviedb.org/settings/api" target="_blank" rel="noopener">TMDb API settings</a>.
        </div>
        <div class="sub">This product uses the TMDb API but is not endorsed by TMDb.</div>
      </div>
    </div>
    <div class="sep"></div>
  </div>
</div>'''
