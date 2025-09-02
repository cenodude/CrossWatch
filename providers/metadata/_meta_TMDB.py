# providers/metadata/_meta_TMDB.py
from __future__ import annotations

import hashlib
import os
from typing import Any, Dict, Optional

import requests

from _logging import log


class TmdbProvider:
    """
    TMDb metadata provider.
    Minimal, fast, and returns normalized fields the app expects.
    """

    name = "TMDB"
    UA = "CrossWatch/1.0"

    def __init__(self, load_cfg, save_cfg) -> None:
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self._cache: Dict[str, dict] = {}  # tiny in-memory cache for dev

    # ---- manifest / capabilities -------------------------------------------------

    def manifest(self) -> dict:
        # Used by Settings → Metadata (UI hints only)
        return {
            "name": self.name,
            "label": "TMDb",
            "supports": {
                "entities": ["movie", "show", "season", "episode"],
                "assets": ["poster", "backdrop", "logo"],
                "locales": True,
            },
            "auth": {"api_key": True},  # show API key field in UI
        }

    def capabilities(self) -> dict:
        # Informational; the manager doesn’t strictly depend on it
        return {
            "images": {"poster": True, "backdrop": True, "logo": True},
            "text": {"title": True, "overview": True},
            "ids": ["tmdb", "imdb"],
        }

    # ---- HTTP / key --------------------------------------------------------------

    def _apikey(self) -> str:
        cfg = self.load_cfg() or {}
        # prefers config; falls back to env
        return (cfg.get("tmdb", {}).get("api_key") or os.environ.get("TMDB_API_KEY") or "").strip()

    def _get(self, url: str, params: dict) -> dict:
        key = self._apikey()
        if not key:
            raise RuntimeError("TMDb API key is missing")
        q = dict(params or {})
        q["api_key"] = key

        ck = url + "?" + "&".join(sorted(f"{k}={v}" for k, v in q.items()))
        h = hashlib.sha1(ck.encode()).hexdigest()
        if h in self._cache:
            return self._cache[h]

        r = requests.get(
            url,
            params=q,
            headers={"User-Agent": self.UA, "Accept": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        self._cache[h] = data
        return data

    # ---- fetch -------------------------------------------------------------------

    def fetch(
        self,
        *,
        entity: str,
        ids: Dict[str, str],
        locale: Optional[str] = None,
        need: Optional[Dict[str, bool]] = None,
    ) -> dict:
        """
        Return normalized metadata:

        {
          "ids": {"tmdb":"...", "imdb":"..."},
          "title": "...",
          "overview": "...",
          "year": 2024,
          "images": {
            "poster": [{"url": "...", "w": 1000, "h": 1500, "lang":"en"}],
            "backdrop": [...],
            "logo": [...]
          }
        }
        """
        need = need or {"poster": True, "backdrop": True}
        lang = (locale or "en-US")
        out: Dict[str, Any] = {"ids": {}, "images": {}}

        base = "https://api.themoviedb.org/3"
        img_base = "https://image.tmdb.org/t/p"
        kind = entity.lower()

        # Resolve a tmdb id if only imdb was provided
        tmdb_id = ids.get("tmdb")
        if not tmdb_id and ids.get("imdb"):
            try:
                data = self._get(f"{base}/find/{ids['imdb']}", {"external_source": "imdb_id", "language": lang})
                blk = None
                if kind == "movie":
                    blk = (data.get("movie_results") or [None])[0]
                elif kind in ("show", "tv"):
                    blk = (data.get("tv_results") or [None])[0]
                if blk and blk.get("id"):
                    tmdb_id = str(blk["id"])
            except Exception as e:
                log(f"TMDb external-id resolve failed: {e}", level="WARNING", module="META")

        if not tmdb_id:
            # nothing we can do
            return {}

        out["ids"]["tmdb"] = tmdb_id
        if ids.get("imdb"):
            out["ids"]["imdb"] = ids["imdb"]

        # Details
        try:
            if kind == "movie":
                det = self._get(f"{base}/movie/{tmdb_id}", {"language": lang})
            elif kind in ("show", "tv"):
                det = self._get(f"{base}/tv/{tmdb_id}", {"language": lang})
            else:
                det = {}
            out["title"] = det.get("title") or det.get("name")
            out["overview"] = det.get("overview")
            date = det.get("release_date") or det.get("first_air_date") or ""
            out["year"] = int(date[:4]) if len(date) >= 4 and date[:4].isdigit() else None
        except Exception as e:
            log(f"TMDb detail fetch failed: {e}", level="WARNING", module="META")

        # Images
        try:
            if kind == "movie":
                imgs = self._get(
                    f"{base}/movie/{tmdb_id}/images",
                    {"include_image_language": f"{lang[:2]},{lang},null"},
                )
            else:
                imgs = self._get(
                    f"{base}/tv/{tmdb_id}/images",
                    {"include_image_language": f"{lang[:2]},{lang},null"},
                )

            posters = imgs.get("posters") or []
            backs = imgs.get("backdrops") or []
            logos = imgs.get("logos") or []

            if need.get("poster"):
                out["images"]["poster"] = [
                    {"url": f"{img_base}/w780{p['file_path']}", "w": p.get("width"), "h": p.get("height"), "lang": p.get("iso_639_1")}
                    for p in posters if p.get("file_path")
                ]
            if need.get("backdrop"):
                out["images"]["backdrop"] = [
                    {"url": f"{img_base}/w1280{p['file_path']}", "w": p.get("width"), "h": p.get("height"), "lang": p.get("iso_639_1")}
                    for p in backs if p.get("file_path")
                ]
            if need.get("logo"):
                out["images"]["logo"] = [
                    {"url": f"{img_base}/w500{p['file_path']}", "w": p.get("width"), "h": p.get("height"), "lang": p.get("iso_639_1")}
                    for p in logos if p.get("file_path")
                ]
        except Exception as e:
            log(f"TMDb images fetch failed: {e}", level="WARNING", module="META")

        return out


# Discovery hook
def build(load_cfg, save_cfg):
    return TmdbProvider(load_cfg, save_cfg)
