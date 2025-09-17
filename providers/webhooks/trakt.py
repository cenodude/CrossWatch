from __future__ import annotations

"""Trakt webhook integration

Validates Plex webhooks, resolves IDs from Plex metadata (with a search
fallback), maps events to Trakt scrobble endpoints, and forwards them with
simple de-duplication and logging.
"""

import base64
import hashlib
import hmac
import json
import re
import time
from typing import Any, Dict, Mapping, Optional, Callable, Iterable

import requests


TRAKT_API = "https://api.trakt.tv"
_SCROBBLE_STATE: Dict[str, Dict[str, Any]] = {}

_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|thetvdb|tvdb)://(\d+)", re.I)


def _emit(logger: Optional[Callable[..., None]], msg: str, level: str = "INFO"):
    """Emit a log line via provided logger if available, else stdout."""
    try:
        if logger:
            logger(msg, level=level, module="SCROBBLE")
            return
    except Exception:
        pass
    print(f"[SCROBBLE] {level} {msg}")


def _load_config() -> Dict[str, Any]:
    """Load configuration via app helper or fall back to config.json file."""
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)


def _save_config(cfg: Dict[str, Any]) -> None:
    """Persist configuration via app helper or to config.json as fallback."""
    try:
        from crosswatch import save_config as _save
        _save(cfg)
    except Exception:
        with open("config.json", "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)


def _tokens(cfg: Dict[str, Any]) -> Dict[str, str]:
    tr = cfg.get("trakt") or {}
    au = ((cfg.get("auth") or {}).get("trakt") or {})
    return {
        "client_id": (tr.get("client_id") or "").strip(),
        "client_secret": (tr.get("client_secret") or "").strip(),
        "access_token": (au.get("access_token") or tr.get("access_token") or "").strip(),
        "refresh_token": (au.get("refresh_token") or tr.get("refresh_token") or "").strip(),
    }


def _headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    """Standard Trakt headers with optional Authorization when token present."""
    t = _tokens(cfg)
    h = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": t["client_id"],
        "User-Agent": "CrossWatch/Scrobble",
    }
    if t["access_token"]:
        h["Authorization"] = f"Bearer {t['access_token']}"
    return h


def _post_trakt(path: str, body: Dict[str, Any], cfg: Dict[str, Any]) -> requests.Response:
    """POST to Trakt, refresh token once on 401, then retry once."""
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


def _ids_from_candidates(candidates: Iterable[Any]) -> Dict[str, Any]:
    """Scan a list of GUID-like strings and return a single best-match IDs dict."""
    for c in candidates:
        if not c:
            continue
        s = str(c)
        m = _PAT_IMDB.search(s)
        if m:
            return {"imdb": m.group(1)}
        m = _PAT_TMDB.search(s)
        if m:
            return {"tmdb": int(m.group(1))}
        m = _PAT_TVDB.search(s)
        if m:
            return {"tvdb": int(m.group(1))}
    return {}


def _gather_guid_candidates(md: Dict[str, Any]) -> list[str]:
    """Collect plausible Plex GUID fields and their array-form variants."""
    cand: list[str] = []
    for k in ("guid", "grandparentGuid", "parentGuid"):
        v = md.get(k)
        if v:
            cand.append(v)
    gi = md.get("Guid") or []
    for g in gi:
        if isinstance(g, dict):
            v = g.get("id")
            if v:
                cand.append(v)
        elif isinstance(g, str):
            cand.append(g)
    seen, out = set(), []
    for v in cand:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _ids_from_metadata(md: Dict[str, Any], media_type: str) -> Dict[str, Any]:
    """Prefer show GUIDs for episodes, else scan all GUID candidates."""
    if media_type == "episode":
        pref = [md.get("grandparentGuid"), md.get("parentGuid")]
        ids = _ids_from_candidates(pref)
        if ids:
            return ids
    return _ids_from_candidates(_gather_guid_candidates(md))


def _describe_ids(ids: Dict[str, Any]) -> str:
    """Compact string form of IDs for logs."""
    if "imdb" in ids:
        return f"imdb:{ids['imdb']}"
    if "tmdb" in ids:
        return f"tmdb:{ids['tmdb']}"
    if "tvdb" in ids:
        return f"tvdb:{ids['tvdb']}"
    return "none"


def _progress(payload: Dict[str, Any]) -> float:
    """Compute percent progress from viewOffset and duration with clamping."""
    md = payload.get("Metadata") or {}
    vo = payload.get("viewOffset") or md.get("viewOffset") or 0
    dur = md.get("duration") or 0
    if not dur:
        return 0.0
    p = max(0.0, min(99.9, float(vo) * 100.0 / float(dur)))
    return round(p, 2)


def _map_event(event: str) -> Optional[str]:
    """Translate Plex event names to Trakt scrobble endpoints."""
    e = (event or "").lower()
    if e in ("media.play", "media.resume"):
        return "/scrobble/start"
    if e == "media.pause":
        return "/scrobble/pause"
    if e in ("media.stop", "media.scrobble"):
        return "/scrobble/stop"
    return None


def _verify_signature(raw: Optional[bytes], headers: Mapping[str, str], secret: str) -> bool:
    """Validate X-Plex-Signature if a shared secret is configured."""
    if not secret:
        return True
    if not raw:
        return False
    sig = headers.get("X-Plex-Signature") or headers.get("x-plex-signature")
    if not sig:
        return False
    digest = hmac.new(secret.encode("utf-8"), raw, hashlib.sha1).digest()
    expected = base64.b64encode(digest).decode("ascii")
    return hmac.compare_digest(sig.strip(), expected.strip())


# ---- Trakt fallback search to avoid wrong matches ("Matrix" problem) ----------
def _lookup_trakt_ids(media_type: str, md: Dict[str, Any], cfg: Dict[str, Any], logger=None) -> Dict[str, Any]:
    """Search Trakt by title/year to avoid mismatches when GUIDs are missing."""
    try:
        title = (md.get("title") if media_type == "movie" else md.get("grandparentTitle")) or ""
        year = md.get("year")
        if not title:
            return {}
        params = {"query": title, "limit": 1}
        if isinstance(year, int) or (isinstance(year, str) and year.isdigit()):
            params["years"] = int(year)
        if media_type == "movie":
            url = f"{TRAKT_API}/search/movie"
        else:
            url = f"{TRAKT_API}/search/show"
        r = requests.get(url, headers=_headers(cfg), params=params, timeout=12)
        if r.status_code != 200:
            _emit(logger, f"trakt search {r.status_code} {r.text[:120]}", "DEBUG")
            return {}
        arr = r.json() or []
        if not arr:
            return {}
        obj = arr[0].get("movie" if media_type == "movie" else "show") or {}
        ids = (obj.get("ids") or {})
        # keep only imdb/tmdb/tvdb keys if present
        clean = {k: ids[k] for k in ("imdb", "tmdb", "tvdb") if k in ids}
        return clean
    except Exception as e:
        _emit(logger, f"trakt search error: {e}", "DEBUG")
        return {}


def process_webhook(
    payload: Dict[str, Any],
    headers: Mapping[str, str],
    raw: Optional[bytes] = None,
    logger: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    """Process a Plex webhook and forward a matching scrobble request to Trakt."""
    cfg = _load_config()
    secret = ((cfg.get("plex") or {}).get("webhook_secret") or "").strip()

    if not _verify_signature(raw, headers, secret):
        _emit(logger, "invalid X-Plex-Signature", "WARN")
        return {"ok": False, "error": "invalid_signature"}

    if not payload:
        _emit(logger, "empty payload", "WARN")
        return {"ok": True, "ignored": True}

    if ((cfg.get("trakt") or {}).get("client_id") or "") == "":
        _emit(logger, "missing trakt.client_id", "ERROR")
        return {"ok": False}

    allow_users = (((cfg.get("trakt") or {}).get("scrobble") or {}).get("username_whitelist") or [])
    acc_title = ((payload.get("Account") or {}).get("title") or "").strip()
    srv_uuid_cfg = ((cfg.get("plex") or {}).get("server_uuid") or "").strip()
    srv_uuid_evt = ((payload.get("Server") or {}).get("uuid") or "").strip()

    md_dbg = payload.get("Metadata") or {}
    media_name_dbg = md_dbg.get("title") or md_dbg.get("grandparentTitle") or "?"
    _emit(logger, f"incoming event from '{acc_title}' server='{srv_uuid_evt}' media='{media_name_dbg}'", "DEBUG")

    if allow_users and acc_title not in set(allow_users):
        _emit(logger, f"ignored user '{acc_title}'", "DEBUG")
        return {"ok": True, "ignored": True}

    if srv_uuid_cfg and srv_uuid_evt and srv_uuid_evt != srv_uuid_cfg:
        _emit(logger, f"ignored server '{srv_uuid_evt}' (expect '{srv_uuid_cfg}')", "DEBUG")
        return {"ok": True, "ignored": True}

    event = (payload.get("event") or "").lower()
    md = payload.get("Metadata") or {}
    if not md:
        return {"ok": True, "ignored": True}

    media_type = (md.get("type") or "").lower()
    ids = _ids_from_metadata(md, media_type)
    if not ids:
        ids = _lookup_trakt_ids(media_type, md, cfg, logger=logger)
        if ids:
            _emit(logger, f"ids via search: {media_name_dbg} -> {_describe_ids(ids)}", "DEBUG")
    _emit(logger, f"ids resolved: {media_name_dbg} -> {_describe_ids(ids)}", "DEBUG")

    prog = _progress(payload)
    sess = str(payload.get("sessionKey") or md.get("sessionKey") or md.get("ratingKey") or "n/a")

    st = _SCROBBLE_STATE.get(sess) or {}
    if st.get("last") == event and (time.time() - float(st.get("ts", 0))) < 1.0:
        return {"ok": True, "dedup": True}
    _SCROBBLE_STATE[sess] = {"last": event, "ts": time.time()}

    path = _map_event(event)
    if not path:
        return {"ok": True, "ignored": True}

    body: Dict[str, Any] = {"progress": prog}
    if media_type == "movie":
        body["movie"] = {"ids": ids} if ids else {"title": md.get("title"), "year": md.get("year")}
    elif media_type == "episode":
        body["show"] = {"ids": ids} if ids else {"title": md.get("grandparentTitle"), "year": md.get("year")}
        body["episode"] = {"season": md.get("parentIndex"), "number": md.get("index")}
    else:
        return {"ok": True, "ignored": True}

    if path == "/scrobble/start" and (body.get("progress") or 0) < 1.0:
        body["progress"] = 1.0
    if path == "/scrobble/pause" and (body.get("progress") or 0) < 0.1:
        body["progress"] = 0.1

    try:
        r = _post_trakt(path, body, cfg)
        try:
            rj = r.json()
        except Exception:
            rj = {"raw": (r.text or "")[:200]}
        what = rj.get("action") if isinstance(rj, dict) else None
        # Only access .get on dict-like; guard if Trakt returned text
        ident_src = {} if not isinstance(rj, dict) else (rj.get("movie") or rj.get("show") or {})
        ident = ident_src.get("ids") if isinstance(ident_src, dict) else {}
        _emit(logger, f"trakt resp {r.status_code} action={what} ids={ident}", "DEBUG")
        if r.status_code >= 400:
            _emit(logger, f"{path} {r.status_code} {r.text[:180]}", "ERROR")
            return {"ok": False, "status": r.status_code, "trakt": rj}
        _emit(logger, f"{path} ok progress={body['progress']} type={media_type}", "INFO")
        return {"ok": True, "status": 200, "action": path, "trakt": rj}
    except Exception as e:
        _emit(logger, f"scrobble error: {e}", "ERROR")
        return {"ok": False}
