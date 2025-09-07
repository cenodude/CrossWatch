# providers/scrobble/trakt/sink.py
from __future__ import annotations
import json, time, random
from typing import Any, Dict, Optional

import requests

from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink

TRAKT_API = "https://api.trakt.tv"


def _load_config() -> Dict[str, Any]:
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)


def _save_config(cfg: Dict[str, Any]) -> None:
    try:
        from crosswatch import save_config as _save
        _save(cfg)
    except Exception:
        with open("config.json", "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)


def _headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    tr = cfg.get("trakt") or {}
    au = (cfg.get("auth") or {}).get("trakt") or {}
    h = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": (tr.get("client_id") or "").strip(),
        "User-Agent": "CrossWatch/Scrobble",
    }
    tok = (au.get("access_token") or tr.get("access_token") or "").strip()
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h


def _post(path: str, body: Dict[str, Any], cfg: Dict[str, Any]) -> requests.Response:
    url = f"{TRAKT_API}{path}"
    r = requests.post(url, json=body, headers=_headers(cfg), timeout=15)
    if r.status_code == 401:
        try:
            from providers.auth._auth_TRAKT import PROVIDER as TRAKT_AUTH
            TRAKT_AUTH.refresh(cfg)
            _save_config(cfg)
        except Exception:
            pass
        r = requests.post(url, json=body, headers=_headers(cfg), timeout=15)
    return r


def _search_ids(media_type: str, title: Optional[str], year: Optional[int], cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not title:
        return {}
    try:
        params = {"query": title, "limit": 1}
        if isinstance(year, int):
            params["years"] = year
        url = f"{TRAKT_API}/search/{'movie' if media_type == 'movie' else 'show'}"
        r = requests.get(url, params=params, headers=_headers(cfg), timeout=12)
        if r.status_code != 200:
            return {}
        arr = r.json() or []
        if not arr:
            return {}
        obj = arr[0].get("movie" if media_type == "movie" else "show") or {}
        ids = obj.get("ids") or {}
        return {k: ids[k] for k in ("imdb", "tmdb", "tvdb") if k in ids}
    except Exception:
        return {}


class TraktSink(ScrobbleSink):
    def __init__(self, logger=None):
        self._logger = logger

    def _log(self, msg: str, level: str = "INFO"):
        try:
            if self._logger:
                self._logger(msg, level=level, module="TRAKT")
        except Exception:
            pass

    # ---------- body builders ----------

    def _build_body(self, event: ScrobbleEvent, ids: Dict[str, Any]) -> Dict[str, Any]:
        body: Dict[str, Any] = {"progress": float(event.progress)}

        # Quick "watching"
        if event.action == "start" and body["progress"] < 1.0:
            body["progress"] = 1.0
        if event.action == "pause" and body["progress"] < 0.1:
            body["progress"] = 0.1

        if event.media_type == "movie":
            body["movie"] = {"ids": ids} if ids else {"title": event.title, "year": event.year}
            return body

        # episode
        has_sn = (event.season is not None and event.number is not None)
        ep_ids = {k: ids[k] for k in ("imdb", "tmdb", "tvdb") if k in ids}

        if has_sn:
            if ids:
                body["show"] = {"ids": ids}
            else:
                body["show"] = {"title": event.title, "year": event.year}
            body["episode"] = {"season": event.season, "number": event.number}
        elif ep_ids:
            body["episode"] = {"ids": ep_ids}
        else:
            body["show"] = {"title": event.title, "year": event.year}
            if has_sn:
                body["episode"] = {"season": event.season, "number": event.number}

        return body

    # ---------- send ----------

    def send(self, event: ScrobbleEvent) -> Dict[str, Any]:
        cfg = _load_config()
        if not ((cfg.get("trakt") or {}).get("client_id")):
            self._log("missing trakt.client_id", "ERROR")
            return {"ok": False, "error": "no_trakt_client"}

        path = f"/scrobble/{event.action}"
        ids = dict(event.ids or {})
        body = self._build_body(event, ids)

        # Robust 5xx + network backoff (0, ~0.8s, ~2.4s)
        r, rj = None, {}
        for attempt in range(3):
            try:
                r = _post(path, body, cfg)
                try:
                    rj = r.json()
                except Exception:
                    rj = {"raw": (r.text or "")[:200]}
                self._log(f"{path} {r.status_code} ids={(ids or {})}", level="DEBUG")
                if r.status_code not in (502, 503, 504):
                    break
            except requests.exceptions.RequestException as e:
                self._log(f"{path} network error: {e}", level="WARN")
            # backoff
            sleep_s = 0.8 if attempt == 0 else 2.4
            time.sleep(sleep_s)

        # If total network failure
        if r is None:
            return {"ok": False, "status": 0, "resp": {"error": "network"}}

        # 404 handling for episodes: swap body shape
        def _is_ep_ids_shape(b: Dict[str, Any]) -> bool:
            return isinstance(b.get("episode"), dict) and isinstance(b["episode"].get("ids"), dict)

        if r.status_code == 404 and event.media_type == "episode":
            if _is_ep_ids_shape(body) and (event.season is not None and event.number is not None):
                self._log("retry with show.ids + season/number", level="WARN")
                alt = {"progress": body["progress"], "show": {}, "episode": {"season": event.season, "number": event.number}}
                if ids:
                    alt["show"] = {"ids": ids}
                else:
                    alt["show"] = {"title": event.title, "year": event.year}
                r = _post(path, alt, cfg)
                try:
                    rj = r.json()
                except Exception:
                    rj = {"raw": (r.text or "")[:200]}
                self._log(f"{path} retry(show+sn) {r.status_code}", level="DEBUG")
            else:
                self._log("retry with episode.ids", level="WARN")
                ep_ids = {k: (event.ids or {}).get(k) for k in ("imdb", "tmdb", "tvdb")}
                ep_ids = {k: v for k, v in ep_ids.items() if v}
                if ep_ids:
                    alt = {"progress": body["progress"], "episode": {"ids": ep_ids}}
                    r = _post(path, alt, cfg)
                    try:
                        rj = r.json()
                    except Exception:
                        rj = {"raw": (r.text or "")[:200]}
                    self._log(f"{path} retry(ep.ids) {r.status_code}", level="DEBUG")

        # Search fallback (404/422)
        if r.status_code in (404, 422):
            self._log(f"fallback search for {event.media_type} '{event.title}' ({event.year})", level="WARN")
            found = _search_ids(event.media_type, event.title, event.year, cfg)
            if event.media_type == "movie":
                body2 = {"progress": body["progress"], "movie": {"ids": found} if found else {"title": event.title, "year": event.year}}
            else:
                if event.season is not None and event.number is not None:
                    if found:
                        body2 = {"progress": body["progress"], "show": {"ids": found}, "episode": {"season": event.season, "number": event.number}}
                    else:
                        body2 = {"progress": body["progress"], "show": {"title": event.title, "year": event.year}, "episode": {"season": event.season, "number": event.number}}
                else:
                    ep_ids = {k: (event.ids or {}).get(k) for k in ("imdb", "tmdb", "tvdb")}
                    ep_ids = {k: v for k, v in ep_ids.items() if v}
                    if ep_ids:
                        body2 = {"progress": body["progress"], "episode": {"ids": ep_ids}}
                    else:
                        body2 = {"progress": body["progress"], "show": {"title": event.title, "year": event.year}}

            r = _post(path, body2, cfg)
            try:
                rj = r.json()
            except Exception:
                rj = {"raw": (r.text or "")[:200]}
            self._log(f"{path} retry(search) {r.status_code}", level="DEBUG")

        if r.status_code >= 400:
            self._log(f"err {path} {r.status_code} {str(r.text)[:180]}", level="ERROR")
            return {"ok": False, "status": r.status_code, "resp": rj}

        return {"ok": True, "status": r.status_code, "resp": rj}
