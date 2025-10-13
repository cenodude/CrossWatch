# providers/webhooks/plextrakt.py
from __future__ import annotations
import base64, hashlib, hmac, json, re, time, requests
from typing import Any, Dict, Mapping, Optional, Callable, Iterable

TRAKT_API = "https://api.trakt.tv"
_SCROBBLE_STATE: Dict[str, Dict[str, Any]] = {}

# GUID patterns
_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|thetvdb|tvdb)://(\d+)", re.I)

# Defaults
_DEF_WEBHOOK = {
    "pause_debounce_seconds": 5,
    "suppress_start_at": 99,
    "filters_plex": {"username_whitelist": [], "server_uuid": ""},
}
_DEF_TRAKT = {"stop_pause_threshold": 80, "force_stop_at": 95, "regress_tolerance_percent": 5}

# logging
def _emit(logger: Optional[Callable[..., None]], msg: str, level: str = "INFO"):
    try:
        if logger:
            logger(msg, level=level, module="SCROBBLE"); return
    except Exception:
        pass
    print(f"[SCROBBLE] {level} {msg}")

# config i/o
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

def _ensure_scrobble(cfg: Dict[str, Any]) -> Dict[str, Any]:
    changed = False
    sc = cfg.setdefault("scrobble", {})
    wh = sc.setdefault("webhook", {})
    trk = sc.setdefault("trakt", {})
    if "pause_debounce_seconds" not in wh:
        wh["pause_debounce_seconds"] = _DEF_WEBHOOK["pause_debounce_seconds"]; changed = True
    if "suppress_start_at" not in wh:
        wh["suppress_start_at"] = _DEF_WEBHOOK["suppress_start_at"]; changed = True
    flt = wh.setdefault("filters_plex", {})
    if "username_whitelist" not in flt:
        flt["username_whitelist"] = []; changed = True
    if "server_uuid" not in flt:
        flt["server_uuid"] = ""; changed = True
    for k, dv in _DEF_TRAKT.items():
        if k not in trk: trk[k] = dv; changed = True
    if changed: _save_config(cfg)
    return cfg

# Trakt HTTP
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
    url = f"{TRAKT_API}{path}"
    r = requests.post(url, json=body, headers=_headers(cfg), timeout=15)
    if r.status_code == 401:
        try:
            from providers.auth._auth_TRAKT import PROVIDER as TRAKT_AUTH
            TRAKT_AUTH.refresh(cfg); _save_config(cfg)
        except Exception:
            pass
        r = requests.post(url, json=body, headers=_headers(cfg), timeout=15)
    if r.status_code in (429, 500, 502, 503, 504):
        try: ra = int(r.headers.get("Retry-After") or "1")
        except Exception: ra = 1
        time.sleep(min(max(ra, 1), 3))
        r = requests.post(url, json=body, headers=_headers(cfg), timeout=15)
    return r

# id utils
def _ids_from_candidates(candidates: Iterable[Any]) -> Dict[str, Any]:
    for c in candidates:
        if not c: continue
        s = str(c)
        m = _PAT_TMDB.search(s)
        if m: return {"tmdb": int(m.group(1))}
        m = _PAT_IMDB.search(s)
        if m: return {"imdb": m.group(1)}
        m = _PAT_TVDB.search(s)
        if m: return {"tvdb": int(m.group(1))}
    return {}

def _ids_from_candidates_show_first(candidates: Iterable[Any]) -> Dict[str, Any]:
    for c in candidates:
        if not c: continue
        s = str(c)
        m = _PAT_TVDB.search(s)
        if m: return {"tvdb": int(m.group(1))}
        m = _PAT_TMDB.search(s)
        if m: return {"tmdb": int(m.group(1))}
        m = _PAT_IMDB.search(s)
        if m: return {"imdb": m.group(1)}
    return {}

def _gather_guid_candidates(md: Dict[str, Any]) -> list[str]:
    cand: list[str] = []
    for k in ("guid", "grandparentGuid", "parentGuid"):
        v = md.get(k)
        if v: cand.append(v)
    gi = md.get("Guid") or []
    for g in gi:
        if isinstance(g, dict):
            v = g.get("id")
            if v: cand.append(v)
        elif isinstance(g, str):
            cand.append(g)
    seen, out = set(), []
    for v in cand:
        if v not in seen:
            seen.add(v); out.append(v)
    return out

def _all_ids_from_metadata(md: Dict[str, Any]) -> Dict[str, Any]:
    ids: Dict[str, Any] = {}
    for s in _gather_guid_candidates(md):
        if not s: continue
        m = _PAT_IMDB.search(s)
        if m: ids.setdefault("imdb", m.group(1))
        m = _PAT_TMDB.search(s)
        if m: ids.setdefault("tmdb", int(m.group(1)))
        m = _PAT_TVDB.search(s)
        if m: ids.setdefault("tvdb", int(m.group(1)))
    return ids

def _episode_ids_from_md(md: Dict[str, Any]) -> Dict[str, Any]:
    ids: Dict[str, Any] = {}
    s = str(md.get("guid") or "")
    if not s: return ids
    m = _PAT_TVDB.search(s)
    if m: ids["tvdb"] = int(m.group(1))
    m = _PAT_TMDB.search(s)
    if m: ids["tmdb"] = int(m.group(1))
    m = _PAT_IMDB.search(s)
    if m: ids["imdb"] = m.group(1)
    return ids

def _ids_from_metadata(md: Dict[str, Any], media_type: str) -> Dict[str, Any]:
    if media_type == "episode":
        pref = [md.get("grandparentGuid"), md.get("parentGuid")]
        ids = _ids_from_candidates_show_first(pref)
        if ids: return ids
        return _ids_from_candidates_show_first(_gather_guid_candidates(md))
    return _ids_from_candidates(_gather_guid_candidates(md))

def _describe_ids(ids: Dict[str, Any] | str) -> str:
    if isinstance(ids, dict):
        if "imdb" in ids: return f"imdb:{ids['imdb']}"
        if "tmdb" in ids: return f"tmdb:{ids['tmdb']}"
        if "tvdb" in ids: return f"tvdb:{ids['tvdb']}"
        return "none"
    return str(ids)

# progress + event map
def _progress(payload: Dict[str, Any]) -> float:
    md = payload.get("Metadata") or {}
    vo = payload.get("viewOffset") or md.get("viewOffset") or 0
    dur = md.get("duration") or 0
    if not dur: return 0.0
    p = max(0.0, min(100.0, (float(vo) * 100.0) / float(dur)))
    return round(p, 2)

def _map_event(event: str) -> Optional[str]:
    e = (event or "").lower()
    if e in ("media.play", "media.resume"): return "/scrobble/start"
    if e == "media.pause": return "/scrobble/pause"
    if e in ("media.stop", "media.scrobble"): return "/scrobble/stop"
    return None

# signature (Plex)
def _verify_signature(raw: Optional[bytes], headers: Mapping[str, str], secret: str) -> bool:
    if not secret: return True
    if not raw: return False
    sig = headers.get("X-Plex-Signature") or headers.get("x-plex-signature")
    if not sig: return False
    digest = hmac.new(secret.encode("utf-8"), raw, hashlib.sha1).digest()
    expected = base64.b64encode(digest).decode("ascii")
    return hmac.compare_digest(sig.strip(), expected.strip())

# search helpers
def _lookup_trakt_ids(media_type: str, md: Dict[str, Any], cfg: Dict[str, Any], logger=None) -> Dict[str, Any]:
    try:
        title = (md.get("title") if media_type == "movie" else md.get("grandparentTitle")) or ""
        year = md.get("year")
        if not title: return {}
        params = {"query": title, "limit": 1}
        if isinstance(year, int) or (isinstance(year, str) and year.isdigit()):
            params["years"] = int(year)
        url = f"{TRAKT_API}/search/movie" if media_type == "movie" else f"{TRAKT_API}/search/show"
        r = requests.get(url, headers=_headers(cfg), params=params, timeout=12)
        if r.status_code != 200:
            _emit(logger, f"trakt search {r.status_code} {r.text[:120]}", "DEBUG"); return {}
        arr = r.json() or []
        if not arr: return {}
        obj = arr[0].get("movie" if media_type == "movie" else "show") or {}
        ids = (obj.get("ids") or {})
        return {k: ids[k] for k in ("imdb", "tmdb", "tvdb") if k in ids}
    except Exception as e:
        _emit(logger, f"trakt search error: {e}", "DEBUG"); return {}

def _guid_search_episode(ids_hint: Dict[str, Any], cfg: Dict[str, Any], logger=None) -> Dict[str, Any] | None:
    for key in ("imdb", "tvdb", "tmdb"):
        val = ids_hint.get(key)
        if not val: continue
        try:
            r = requests.get(f"{TRAKT_API}/search/{key}/{val}",
                             params={"type": "episode", "limit": 1},
                             headers=_headers(cfg), timeout=10)
        except Exception:
            continue
        if r.status_code != 200: continue
        try: arr = r.json() or []
        except Exception: arr = []
        for hit in arr:
            epi_ids = ((hit.get("episode") or {}).get("ids") or {})
            out = {k: epi_ids[k] for k in ("trakt", "imdb", "tmdb", "tvdb") if epi_ids.get(k)}
            if out:
                _emit(logger, f"guid search resolved episode ids: {out}", "DEBUG")
                return out
    return None

# payload builders
def _build_bodies(media_type: str, md: Dict[str, Any], ids: Dict[str, Any], ids_all: Dict[str, Any], prog: float) -> list[Dict[str, Any]]:
    p = float(round(prog, 2))
    bodies: list[Dict[str, Any]] = []
    if media_type == "movie":
        if ids: bodies.append({"progress": p, "movie": {"ids": ids}})
        bodies.append({"progress": p, "movie": {"title": md.get("title"), **({"year": md.get("year")} if md.get("year") else {})}})
        return bodies
    epi_ids = _episode_ids_from_md(md)
    show_title = md.get("grandparentTitle")
    show_year = md.get("year")
    season = md.get("parentIndex"); number = md.get("index")
    if epi_ids: bodies.append({"progress": p, "episode": {"ids": epi_ids}})
    for key in ("tvdb", "tmdb", "imdb"):
        if key in ids_all:
            bodies.append({"progress": p, "show": {"ids": {key: ids_all[key]}}, "episode": {"season": season, "number": number}})
    bodies.append({"progress": p, "show": {"title": show_title, **({"year": show_year} if show_year else {})},
                   "episode": {"season": season, "number": number}})
    return bodies

def _body_ids_desc(b: Dict[str, Any]) -> str:
    ids = ((b.get("movie") or {}).get("ids")) or ((b.get("show") or {}).get("ids")) or ((b.get("episode") or {}).get("ids"))
    return _describe_ids(ids if ids else "title/year")

# main
def process_webhook(
    payload: Dict[str, Any],
    headers: Mapping[str, str],
    raw: Optional[bytes] = None,
    logger: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    cfg = _ensure_scrobble(_load_config())

    sc = cfg.get("scrobble") or {}
    if not sc.get("enabled", True) or str(sc.get("mode", "webhook")).lower() != "webhook":
        _emit(logger, "scrobble webhook disabled by config", "DEBUG"); return {"ok": True, "ignored": True}

    secret = ((cfg.get("plex") or {}).get("webhook_secret") or "").strip()
    if not _verify_signature(raw, headers, secret):
        _emit(logger, "invalid X-Plex-Signature", "WARN"); return {"ok": False, "error": "invalid_signature"}

    if not payload:
        _emit(logger, "empty payload", "WARN"); return {"ok": True, "ignored": True}

    if ((cfg.get("trakt") or {}).get("client_id") or "") == "":
        _emit(logger, "missing trakt.client_id", "ERROR"); return {"ok": False}

    wh = (sc.get("webhook") or {})
    pause_debounce = int(wh.get("pause_debounce_seconds", _DEF_WEBHOOK["pause_debounce_seconds"]) or 0)
    suppress_start_at = float(wh.get("suppress_start_at", _DEF_WEBHOOK["suppress_start_at"]) or 99)
    flt = (wh.get("filters_plex") or {})
    allow_users = set((flt.get("username_whitelist") or []))
    srv_uuid_cfg = (flt.get("server_uuid") or "").strip()

    tset = (sc.get("trakt") or {})
    stop_pause_threshold = float(tset.get("stop_pause_threshold", _DEF_TRAKT["stop_pause_threshold"]))
    force_stop_at = float(tset.get("force_stop_at", _DEF_TRAKT["force_stop_at"]))
    regress_tol = float(tset.get("regress_tolerance_percent", _DEF_TRAKT["regress_tolerance_percent"]))

    acc_title = ((payload.get("Account") or {}).get("title") or "").strip()
    srv_uuid_evt = ((payload.get("Server") or {}).get("uuid") or "").strip()
    event = (payload.get("event") or "").lower()
    md = payload.get("Metadata") or {}
    media_name_dbg = md.get("title") or md.get("grandparentTitle") or "?"
    media_type = (md.get("type") or "").lower()

    _emit(logger, f"incoming '{event}' user='{acc_title}' server='{srv_uuid_evt}' media='{media_name_dbg}'", "DEBUG")

    if allow_users and acc_title not in allow_users:
        _emit(logger, f"ignored user '{acc_title}'", "DEBUG"); return {"ok": True, "ignored": True}
    if srv_uuid_cfg and srv_uuid_evt and srv_uuid_evt != srv_uuid_cfg:
        _emit(logger, f"ignored server '{srv_uuid_evt}' (expect '{srv_uuid_cfg}')", "DEBUG"); return {"ok": True, "ignored": True}
    if not md or media_type not in ("movie", "episode"):
        return {"ok": True, "ignored": True}

    ids = _ids_from_metadata(md, media_type)
    ids_all = _all_ids_from_metadata(md)
    if not ids:
        ids = _lookup_trakt_ids(media_type, md, cfg, logger=logger)
        if ids: _emit(logger, f"ids via search: {media_name_dbg} -> {_describe_ids(ids)}", "DEBUG")
    _emit(logger, f"ids resolved: {media_name_dbg} -> {_describe_ids(ids)}", "DEBUG")

    prog_raw = _progress(payload)
    sess = str(payload.get("sessionKey") or md.get("sessionKey") or md.get("ratingKey") or "n/a")
    now = time.time()
    st = _SCROBBLE_STATE.get(sess) or {}

    if st.get("last_event") == event and (now - float(st.get("ts", 0))) < 1.0:
        return {"ok": True, "dedup": True}
    if event == "media.pause" and (now - float(st.get("last_pause_ts", 0))) < pause_debounce:
        _emit(logger, f"debounce pause ({pause_debounce}s)", "DEBUG")
        _SCROBBLE_STATE[sess] = {**st, "ts": now, "last_event": event}; return {"ok": True, "debounced": True}

    last_prog = float(st.get("prog", 0.0))
    tol_pts = max(0.0, regress_tol)
    prog = prog_raw
    if prog + tol_pts < last_prog:
        _emit(logger, f"regression clamp {prog_raw:.2f}% -> {last_prog:.2f}% (tol={tol_pts}%)", "DEBUG")
        prog = last_prog
    if event == "media.pause" and prog >= 99.9 and last_prog > 0.0:
        newp = max(last_prog, 95.0); _emit(logger, f"pause@100 clamp {prog:.2f}% -> {newp:.2f}%", "DEBUG"); prog = newp

    path = _map_event(event)
    if not path:
        _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "prog": prog}; return {"ok": True, "ignored": True}

    if path == "/scrobble/start" and prog >= suppress_start_at:
        _emit(logger, f"suppress start at {prog:.1f}% (>= {suppress_start_at}%)", "DEBUG")
        _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "prog": prog}; return {"ok": True, "suppressed": True}

    intended = path
    if path == "/scrobble/stop":
        if prog < stop_pause_threshold: intended = "/scrobble/pause"
        elif prog < force_stop_at: intended = "/scrobble/pause"
        elif last_prog >= 0 and (prog - last_prog) >= 30 and last_prog < stop_pause_threshold and prog >= 98:
            _emit(logger, f"Demote STOP→PAUSE jump {last_prog:.0f}%→{prog:.0f}% (thr={stop_pause_threshold})", "DEBUG")
            intended = "/scrobble/pause"; prog = last_prog

    if intended == "/scrobble/start" and prog < 1.0: prog = 1.0
    if intended == "/scrobble/pause" and prog < 0.1: prog = 0.1

    if event == "media.stop" and st.get("last_event") == "media.stop" and abs((st.get("prog", 0.0)) - prog) <= 1.0:
        _emit(logger, "suppress duplicate stop", "DEBUG")
        _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "prog": prog}; return {"ok": True, "suppressed": True}

    bodies = _build_bodies(media_type, md, ids, ids_all, prog)

    last_resp = None
    for b in bodies:
        _emit(logger, f"trakt intent {intended} using {_body_ids_desc(b)}, prog={b.get('progress')}", "DEBUG")
        r = _post_trakt(intended, b, cfg)
        try: rj = r.json()
        except Exception: rj = {"raw": (r.text or "")[:200]}
        _emit(logger, f"trakt {intended} -> {r.status_code} action={rj.get('action') or intended.rsplit('/',1)[-1]}", "DEBUG")
        if r.status_code < 400:
            _SCROBBLE_STATE[sess] = {
                "ts": now, "last_event": event,
                "last_pause_ts": (now if intended == "/scrobble/pause" else st.get("last_pause_ts", 0)),
                "prog": prog,
            }
            return {"ok": True, "status": 200, "action": intended, "trakt": rj}
        last_resp = (r.status_code, rj)
        if r.status_code != 404: break

    if media_type == "episode" and (not last_resp or last_resp[0] == 404):
        epi_hint = {**_episode_ids_from_md(md), **ids_all}
        found = _guid_search_episode(epi_hint, cfg, logger=logger)
        if found:
            body = {"progress": float(round(prog, 2)), "episode": {"ids": found}}
            _emit(logger, f"trakt intent {intended} using {_describe_ids(found)}, prog={body['progress']}", "DEBUG")
            r = _post_trakt(intended, body, cfg)
            try: rj = r.json()
            except Exception: rj = {"raw": (r.text or "")[:200]}
            _emit(logger, f"trakt {intended} (guid search) -> {r.status_code}", "DEBUG")
            if r.status_code < 400:
                _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "last_pause_ts": st.get("last_pause_ts", 0), "prog": prog}
                return {"ok": True, "status": 200, "action": intended, "trakt": rj}
            last_resp = (r.status_code, rj)

    code, rj = last_resp if last_resp else (500, {"error": "unknown"})
    _emit(logger, f"{intended} {code} {(str(rj)[:180])}", "ERROR")
    _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "last_pause_ts": st.get("last_pause_ts", 0), "prog": prog}
    return {"ok": False, "status": code, "trakt": rj}
