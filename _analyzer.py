# _analyzer.py
# CrossWatch - Data analyzer for state
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections import defaultdict
from typing import Any, DefaultDict, Dict, Iterable, List, Tuple

import json
import re
import threading

import requests
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from cw_platform.config_base import CONFIG as CONFIG_DIR

router = APIRouter(prefix="/api", tags=["analyzer"])
STATE_PATH = CONFIG_DIR / "state.json"
CWS_DIR = CONFIG_DIR / ".cw_state"
CFG_PATH = CONFIG_DIR / "config.json"
_LOCK = threading.Lock()

# ── Config helpers
def _cfg() -> Dict[str, Any]:
    try:
        return json.loads(CFG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _tmdb_key() -> str:
    return ((_cfg().get("tmdb") or {}).get("api_key") or "").strip()

def _trakt_headers() -> Dict[str, str]:
    t = (_cfg().get("trakt") or {})
    h = {"trakt-api-version": "2", "trakt-api-key": (t.get("client_id") or "").strip()}
    tok = (t.get("access_token") or "").strip()
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h

# ── State I/O
def _load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        raise HTTPException(404, "state.json not found")
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        raise HTTPException(500, "Failed to parse state.json")

def _save_state(s: Dict[str, Any]) -> None:
    with _LOCK:
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(s, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE_PATH)

# ── Item iteration / access
def _iter_items(s: Dict[str, Any]) -> Iterable[Tuple[str, str, str, Dict[str, Any]]]:
    for prov, pv in (s.get("providers") or {}).items():
        for feat in ("history", "watchlist", "ratings"):
            items = (((pv or {}).get(feat) or {}).get("baseline") or {}).get("items") or {}
            for k, it in items.items():
                yield prov, feat, k, (it or {})

def _bucket(s: Dict[str, Any], prov: str, feat: str) -> Dict[str, Any] | None:
    try:
        return s["providers"][prov][feat]["baseline"]["items"]  # type: ignore[index]
    except KeyError:
        return None

def _find_item(s: Dict[str, Any], prov: str, feat: str, key: str):
    b = _bucket(s, prov, feat)
    if not b or key not in b:
        return None, None
    return b, b[key]

# ── Counts
def _counts(s: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for prov, pv in (s.get("providers") or {}).items():
        cur = out.setdefault(prov, {"history": 0, "watchlist": 0, "ratings": 0, "total": 0})
        total = 0
        for feat in ("history", "watchlist", "ratings"):
            items = (((pv or {}).get(feat) or {}).get("baseline") or {}).get("items") or {}
            n = len(items)
            cur[feat] = n
            total += n
        cur["total"] = total
    return out

def _collect_items(s: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for prov, feat, k, it in _iter_items(s):
        out.append(
            {
                "provider": prov,
                "feature": feat,
                "key": k,
                "title": it.get("title"),
                "year": it.get("year"),
                "type": it.get("type"),
                "ids": it.get("ids") or {},
            }
        )
    return out

_ID_RX: Dict[str, re.Pattern[str]] = {
    "imdb": re.compile(r"^tt\d{5,}$"),
    "tmdb": re.compile(r"^\d+$"),
    "tvdb": re.compile(r"^\d+$"),
    "plex": re.compile(r"^\d+$"),
    "trakt": re.compile(r"^\d+$"),
    "simkl": re.compile(r"^\d+$"),
    "emby": re.compile(r"^[A-Za-z0-9-]{4,}$"),
    "mdblist": re.compile(r"^\d+$"),
}

def _read_cw_state() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not (CWS_DIR.exists() and CWS_DIR.is_dir()):
        return out
    for p in sorted(CWS_DIR.glob("*.json")):
        try:
            out[p.name] = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            out[p.name] = {"_error": "parse_error"}
    return out

def _alias_keys(obj: Dict[str, Any]) -> List[str]:
    t = (obj.get("type") or "").lower()
    ids = dict(obj.get("ids") or {})
    out: List[str] = []
    seen: set[str] = set()

    if obj.get("_key"):
        out.append(obj["_key"])

    for ns in ("imdb", "tmdb", "tvdb", "trakt", "simkl", "plex", "emby", "guid", "mdblist"):
        v = ids.get(ns)
        if v:
            vs = str(v)
            out.append(f"{ns}:{vs}")
            if t in ("movie", "show", "season", "episode"):
                out.append(f"{t}:{ns}:{vs}")

    title = (obj.get("title") or "").strip().lower()
    year = obj.get("year")
    if title and year:
        out.append(f"t:{title}|y:{year}|ty:{t}")

    res: List[str] = []
    for k in out:
        if k not in seen:
            seen.add(k)
            res.append(k)
    return res

def _alias_index(items: Dict[str, Any]) -> Dict[str, str]:
    idx: Dict[str, str] = {}
    for k, v in items.items():
        vv = dict(v)
        vv["_key"] = k
        for ak in _alias_keys(vv):
            idx.setdefault(ak, k)
    return idx

def _class_key(it: Dict[str, Any]) -> Tuple[str, str, int | None]:
    return ((it.get("type") or "").lower(), (it.get("title") or "").strip().lower(), it.get("year"))

# ── Pair awareness
def _pair_map(cfg: Dict[str, Any], state: Dict[str, Any]) -> DefaultDict[Tuple[str, str], List[str]]:
    mp: DefaultDict[Tuple[str, str], List[str]] = defaultdict(list)
    pairs = cfg.get("pairs") or []

    def add(src: str, feat: str, dst: str) -> None:
        k = (src, feat)
        if dst not in mp[k]:
            mp[k].append(dst)

    for pr in pairs:
        src = str(pr.get("src") or pr.get("source") or "").upper().strip()
        dst = str(pr.get("dst") or pr.get("target") or "").upper().strip()
        if not (src and dst):
            continue
        if pr.get("enabled") is False:
            continue

        mode = str(pr.get("mode") or "one-way").lower()
        feats = pr.get("features")
        feats_list: List[str] = []
        if isinstance(feats, (list, tuple)):
            feats_list = [str(f).lower() for f in feats]
        elif isinstance(feats, dict):
            for name in ("history", "watchlist", "ratings"):
                f = feats.get(name)
                if isinstance(f, dict) and (f.get("enable") or f.get("enabled")):
                    feats_list.append(name)
        else:
            feats_list = ["history"]

        for f in feats_list:
            add(src, f, dst)
            if mode in ("two-way", "bi", "both", "mirror", "two", "two_way", "two way"):
                add(dst, f, src)
    return mp

# ── Pair-level library whitelisting (Analyzer-side)
def _supports_pair_libs(prov: str) -> bool:
    return str(prov or "").upper() in ("PLEX", "EMBY", "JELLYFIN")

def _item_library_id(it: Dict[str, Any]) -> str | None:
    if not isinstance(it, dict):
        return None

    for k in (
        "library_id",
        "libraryId",
        "library",
        "section_id",
        "sectionId",
        "section",
        "lib_id",
        "libraryid",
    ):
        v = it.get(k)
        if v not in (None, "", []):
            return str(v).strip()

    for nest_key in ("meta", "server", "userData", "userdata", "extra"):
        nest = it.get(nest_key) or {}
        if isinstance(nest, dict):
            for k in ("library_id", "libraryId", "library", "section_id", "sectionId", "section"):
                v = nest.get(k)
                if v not in (None, "", []):
                    return str(v).strip()

    return None

def _pair_lib_filters(cfg: Dict[str, Any]) -> Dict[Tuple[str, str, str], set[str]]:
    out: Dict[Tuple[str, str, str], set[str]] = {}
    for pr in (cfg.get("pairs") or []):
        src = str(pr.get("src") or pr.get("source") or "").upper().strip()
        dst = str(pr.get("dst") or pr.get("target") or "").upper().strip()
        if not (src and dst):
            continue
        if pr.get("enabled") is False:
            continue

        mode = str(pr.get("mode") or "one-way").lower()
        feats = pr.get("features") or {}
        if not isinstance(feats, dict):
            continue

        for feat in ("history", "watchlist", "ratings"):
            fcfg = feats.get(feat) or {}
            if not (isinstance(fcfg, dict) and (fcfg.get("enable") or fcfg.get("enabled"))):
                continue

            libs_dict = fcfg.get("libraries") or {}
            if not isinstance(libs_dict, dict):
                libs_dict = {}

            def add_dir(a: str, b: str) -> None:
                if not _supports_pair_libs(a):
                    return
                raw = libs_dict.get(a) or libs_dict.get(a.lower()) or libs_dict.get(a.upper())
                if isinstance(raw, (list, tuple)) and raw:
                    allowed = set(str(x).strip() for x in raw if str(x).strip())
                    if allowed:
                        out[(a, feat, b)] = allowed

            add_dir(src, dst)
            if mode in ("two-way", "bi", "both", "mirror", "two", "two_way", "two way"):
                add_dir(dst, src)

    return out

def _passes_pair_lib_filter(
    pair_libs: Dict[Tuple[str, str, str], set[str]] | None,
    prov: str,
    feat: str,
    dst: str,
    item: Dict[str, Any],
) -> bool:
    if not pair_libs:
        return True
    p = str(prov or "").upper()
    f = str(feat or "").lower()
    d = str(dst or "").upper()
    allowed = pair_libs.get((p, f, d))
    if not allowed:
        return True
    lid = _item_library_id(item)
    if lid is None:
        return True
    return lid in allowed

def _indices_for(s: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, str]]:
    out: Dict[Tuple[str, str], Dict[str, str]] = {}
    for p, f, _, _ in _iter_items(s):
        key = (p, f)
        if key not in out:
            out[key] = _alias_index(_bucket(s, p, f) or {})
    return out

def _has_peer_by_pairs(
    s: Dict[str, Any],
    pairs: DefaultDict[Tuple[str, str], List[str]],
    prov: str,
    feat: str,
    item_key: str,
    item: Dict[str, Any],
    idx_cache: Dict[Tuple[str, str], Dict[str, str]],
    pair_libs: Dict[Tuple[str, str, str], set[str]] | None = None,
) -> bool:
    if feat not in ("history", "watchlist", "ratings"):
        return True
    targets = pairs.get((prov, feat.lower()), [])
    if not targets:
        return True

    filtered_targets: List[str] = []
    for dst in targets:
        if _passes_pair_lib_filter(pair_libs, prov, feat, dst, item):
            filtered_targets.append(dst)
    if not filtered_targets:
        return True  # out of scope for all pair dirs

    vv = dict(item)
    vv["_key"] = item_key
    keys = set(_alias_keys(vv))
    for dst in filtered_targets:
        idx = idx_cache.get((dst, feat)) or {}
        if any(k in idx for k in keys):
            return True
    return False

def _pair_stats(s: Dict[str, Any]) -> List[Dict[str, Any]]:
    stats: List[Dict[str, Any]] = []
    pairs = _pair_map(_cfg(), s)
    idx_cache = _indices_for(s)
    pair_libs = _pair_lib_filters(_cfg())

    for (prov, feat), targets in pairs.items():
        src_items = _bucket(s, prov, feat) or {}
        for dst in targets:
            total = 0
            synced = 0
            idx = idx_cache.get((dst, feat)) or {}

            for k, v in src_items.items():
                if v.get("_ignore_missing_peer"):
                    continue
                if not _passes_pair_lib_filter(pair_libs, prov, feat, dst, v):
                    continue

                total += 1
                vv = dict(v)
                vv["_key"] = k
                alias_keys = _alias_keys(vv)
                if any(a in idx for a in alias_keys):
                    synced += 1

            stats.append(
                {
                    "source": prov,
                    "target": dst,
                    "feature": feat,
                    "total": total,
                    "synced": synced,
                    "unsynced": max(total - synced, 0),
                }
            )
    return stats

# ── Problems
def _problems(s: Dict[str, Any]) -> List[Dict[str, Any]]:
    probs: List[Dict[str, Any]] = []
    CORE = ("imdb", "tmdb", "tvdb")

    pairs = _pair_map(_cfg(), s)
    idx_cache = _indices_for(s)
    pair_libs = _pair_lib_filters(_cfg())
    cw_state = _read_cw_state()
    unresolved_index: Dict[Tuple[str, str], Dict[str, List[Dict[str, Any]]]] = {}
    for name, body in (cw_state or {}).items():
        if not isinstance(body, dict):
            continue
        if not name.endswith(".json"):
            continue
        base = name[:-5]
        if "_" not in base:
            continue
        prov_raw, rest = base.split("_", 1)
        if "." in rest:
            feat_raw, suffix = rest.split(".", 1)
        else:
            feat_raw, suffix = rest, ""
        if suffix not in ("unresolved", "shadow"):
            continue
        prov_key = prov_raw.upper()
        feat_key = feat_raw.lower()
        key = (prov_key, feat_key)
        idx = unresolved_index.setdefault(key, {})
        for uk, rec in body.items():
            if not isinstance(rec, dict):
                continue
            item = rec.get("item") or {}
            if not isinstance(item, dict):
                continue
            vv = dict(item)
            alias_key = uk
            if "@" in alias_key:
                alias_key = alias_key.split("@", 1)[0]
            vv["_key"] = alias_key
            aks = _alias_keys(vv)
            if not aks:
                continue
            meta: Dict[str, Any] = {"file": name, "kind": suffix}
            reasons = rec.get("reasons")
            if isinstance(reasons, list):
                meta["reasons"] = reasons
            for ak in aks:
                lst = idx.setdefault(ak, [])
                lst.append(meta)

    for (prov, feat), targets in pairs.items():
        src_items = _bucket(s, prov, feat) or {}
        if not targets:
            continue

        for k, v in src_items.items():
            if v.get("_ignore_missing_peer"):
                continue

            filtered_targets: List[str] = []
            union_targets: List[Dict[str, str]] = []
            for t in targets:
                if _passes_pair_lib_filter(pair_libs, prov, feat, t, v):
                    filtered_targets.append(t)
                    union_targets.append(idx_cache.get((t, feat)) or {})

            if not union_targets:
                continue  # out of scope

            merged_keys = set().union(*[set(d.keys()) for d in union_targets]) if union_targets else set()
            vv = dict(v)
            vv["_key"] = k
            alias_keys = _alias_keys(vv)

            if not any(ak in merged_keys for ak in alias_keys):
                prob: Dict[str, Any] = {
                    "severity": "warn",
                    "type": "missing_peer",
                    "provider": prov,
                    "feature": feat,
                    "key": k,
                    "title": v.get("title"),
                    "year": v.get("year"),
                    "targets": filtered_targets,
                }

                hints: List[Dict[str, Any]] = []
                for dst in filtered_targets:
                    idx_key = (str(dst).upper(), feat.lower())
                    uidx = unresolved_index.get(idx_key) or {}
                    for ak in alias_keys:
                        for meta in uidx.get(ak, []):
                            h: Dict[str, Any] = {"provider": dst, "feature": feat}
                            if "reasons" in meta:
                                h["reasons"] = meta["reasons"]
                            if "file" in meta:
                                h["source"] = meta["file"]
                            if "kind" in meta:
                                h["kind"] = meta["kind"]
                            hints.append(h)
                if hints:
                    prob["hints"] = hints
                probs.append(prob)

    for p, f, k, it in _iter_items(s):
        ids = it.get("ids") or {}
        for ns in CORE:
            v = ids.get(ns)
            rx = _ID_RX.get(ns)
            if v and rx and not rx.match(str(v)):
                probs.append(
                    {
                        "severity": "warn",
                        "type": "invalid_id_format",
                        "provider": p,
                        "feature": f,
                        "key": k,
                        "id_name": ns,
                        "id_value": v,
                    }
                )
        if ":" in k:
            ns, kid = k.split(":", 1)
            base = kid.split("#", 1)[0].strip()
            val = str((ids.get(ns) or "")).strip()
            if base and val and base != val:
                probs.append(
                    {
                        "severity": "info",
                        "type": "key_ids_mismatch",
                        "provider": p,
                        "feature": f,
                        "key": k,
                        "id_name": ns,
                        "id_value": val,
                        "key_base": base,
                    }
                )
        missing = [ns for ns in CORE if not ids.get(ns)]
        if missing and ids:
            probs.append(
                {
                    "severity": "info",
                    "type": "missing_ids",
                    "provider": p,
                    "feature": f,
                    "key": k,
                    "missing": missing,
                }
            )
        if ids and not any(ids.get(ns) for ns in CORE):
            probs.append(
                {
                    "severity": "info",
                    "type": "key_missing_ids",
                    "provider": p,
                    "feature": f,
                    "key": k,
                    "ids": ids,
                }
            )

    return probs

def _peer_ids(s: Dict[str, Any], cur: Dict[str, Any]) -> Dict[str, str]:
    t = (cur.get("title") or "").strip().lower()
    y = cur.get("year")
    ty = (cur.get("type") or "").lower()
    out: Dict[str, str] = {}
    for _, _, _, it in _iter_items(s):
        if (it.get("title") or "").strip().lower() != t:
            continue
        if it.get("year") != y:
            continue
        if (it.get("type") or "").lower() != ty:
            continue
        for k, v in (it.get("ids") or {}).items():
            if v and k not in out:
                out[k] = str(v)
    return out

def _norm(ns: str, v: str) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if ns == "imdb":
        m = re.search(r"(\d+)", s)
        return f"tt{m.group(1)}" if m else None
    if ns in ("tmdb", "tvdb", "trakt", "plex", "simkl"):
        m = re.search(r"(\d+)", s)
        return m.group(1) if m else None
    return s or None

def _rekey(b: Dict[str, Any], old_key: str, it: Dict[str, Any]) -> str:
    ids = it.get("ids") or {}
    parts = old_key.split(":", 1)
    ns = parts[0]
    base = ids.get(ns) or ""
    if not base:
        for cand in ("imdb", "tmdb", "tvdb"):
            if ids.get(cand):
                ns = cand
                base = ids[cand]
                break
    base = str(base).strip()
    if not base:
        return old_key
    suffix = ""
    if "#" in old_key:
        suffix = old_key.split("#", 1)[1]
    new_key = f"{ns}:{base}"
    if suffix:
        new_key += f"#{suffix}"
    if new_key == old_key:
        return old_key
    if new_key in b:
        return old_key
    b[new_key] = it
    b.pop(old_key, None)
    return new_key

def _tmdb(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    k = _tmdb_key()
    if not k:
        raise HTTPException(400, "tmdb.api_key missing in config.json")
    r = requests.get(f"https://api.themoviedb.org/3{path}", params={**(params or {}), "api_key": k}, timeout=8)
    r.raise_for_status()
    return r.json()

def _trakt(path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    h = _trakt_headers()
    if not h.get("trakt-api-key"):
        raise HTTPException(400, "trakt.client_id missing in config.json")
    r = requests.get(f"https://api.trakt.tv{path}", params=params, headers=h, timeout=8)
    r.raise_for_status()
    return r.json()

def _tmdb_bulk(ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not ids:
        return {}
    key = _tmdb_key()
    if not key:
        return {}
    out: Dict[int, Dict[str, Any]] = {}
    for chunk_start in range(0, len(ids), 20):
        chunk = ids[chunk_start : ids[chunk_start + 20]]
        url = "https://api.themoviedb.org/3/movie"
        params = {
            "api_key": key,
            "language": "en-US",
            "append_to_response": "release_dates",
        }
        for mid in chunk:
            try:
                r = requests.get(f"{url}/{mid}", params=params, timeout=10)
                if r.ok:
                    out[mid] = r.json()
            except Exception:
                continue
    return out

def _tmdb_region_dates(meta: Dict[int, Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for mid, data in (meta or {}).items():
        rels = (data.get("release_dates") or {}).get("results") or []
        best: Dict[str, Any] | None = None
        for entry in rels:
            region = (entry.get("iso_3166_1") or "").upper()
            if region not in ("US", "GB", "NL", "DE", "FR", "CA", "AU", "NZ", "IE", "ES", "IT"):
                continue
            for rel in entry.get("release_dates") or []:
                if rel.get("type") not in (3, 4):
                    continue
                date = rel.get("release_date")
                if not date:
                    continue
                cand = {"region": region, "date": date}
                if not best or cand["date"] < best["date"]:
                    best = cand
        if best:
            out[mid] = best
    return out

def _ratings_audit(s: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    tmdb_ids: List[int] = []
    tmdb_map: Dict[int, Dict[str, Any]] = {}
    for prov, feat, k, it in _iter_items(s):
        if feat != "ratings":
            continue
        if (it.get("type") or "").lower() != "movie":
            continue
        ids = it.get("ids") or {}
        tmdb = ids.get("tmdb")
        if not tmdb:
            continue
        try:
            mid = int(str(tmdb).strip())
        except ValueError:
            continue
        tmdb_ids.append(mid)
    tmdb_ids = sorted(set(tmdb_ids))
    tmdb_map = _tmdb_region_dates(_tmdb_bulk(tmdb_ids))

    for prov, feat, k, it in _iter_items(s):
        if feat != "ratings":
            continue
        if (it.get("type") or "").lower() != "movie":
            continue
        ids = it.get("ids") or {}
        tmdb = ids.get("tmdb")
        if not tmdb:
            continue
        try:
            mid = int(str(tmdb).strip())
        except ValueError:
            continue
        rel = tmdb_map.get(mid) or {}
        out.setdefault(prov, {}).setdefault(feat, {})[k] = {
            "ids": ids,
            "tmdb_release": rel,
        }
    return out

def _apply_fix(s: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
    t, prov, feat, key = body.get("type"), body.get("provider"), body.get("feature"), body.get("key")
    b, it = _find_item(s, prov, feat, key)
    if not it:
        raise HTTPException(404, "Item not found")

    ids = it.setdefault("ids", {})
    ch: List[str] = []

    if t in ("key_missing_ids", "key_ids_mismatch"):
        ns = body.get("id_name")
        exp = body.get("expected")
        if not ns or not exp:
            raise HTTPException(400, "Missing id_name/expected")
        ids[ns] = exp
        ch.append(f"ids.{ns}={exp}")
        new = _rekey(b, key, it)
    elif t == "invalid_id_format":
        ns = body.get("id_name")
        val = body.get("id_value")
        nv = _norm(ns, val)
        if not nv:
            raise HTTPException(400, "Cannot normalize")
        ids[ns] = nv
        ch.append(f"ids.{ns}={nv}")
        new = _rekey(b, key, it)
    elif t in ("missing_ids", "missing_peer"):
        if ":" in key:
            nsb, kid = key.split(":", 1)
            base = kid.split("#", 1)[0].strip()
            if base:
                ids.setdefault(nsb, base)
        peer = _peer_ids(s, it)
        for ns, v in (peer or {}).items():
            if not ids.get(ns):
                ids[ns] = v
        new = _rekey(b, key, it)
    else:
        raise HTTPException(400, "Unsupported fix")

    pairs = _pair_map(_cfg(), s)
    idx = _indices_for(s)
    pair_libs = _pair_lib_filters(_cfg())
    it["_ignore_missing_peer"] = not _has_peer_by_pairs(s, pairs, prov, feat, new, it, idx, pair_libs)
    return {"ok": True, "changes": ch or ["ids merged from peers"], "new_key": new}

# ── Suggest
def _anchor(key: str):
    if "#" not in key:
        return None
    m = re.search(r"#s(\d+)[eE](\d+)", key)
    return (int(m.group(1)), int(m.group(2))) if m else None

def _sig(ids: Dict[str, Any]) -> str:
    return "|".join([str(ids.get(x, "")) for x in ("imdb", "tmdb", "tvdb", "trakt", "plex", "emby")])

def _suggest(s: Dict[str, Any], prov: str, feat: str, key: str) -> Dict[str, Any]:
    b, it = _find_item(s, prov, feat, key)
    if not it:
        raise HTTPException(404, "Item not found")

    title = (it.get("title") or "").strip()
    year = it.get("year")
    typ = (it.get("type") or "").lower()
    ids = it.get("ids") or {}
    se = _anchor(key)

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def push(new_ids: Dict[str, Any], why: str, conf: float, src: str) -> None:
        merged = {
            **ids,
            **{k: v for k, v in (new_ids or {}).items() if v},
        }
        sig = _sig(merged)
        dedup_key = f"{src}|{why}|{sig}"
        if dedup_key in seen:
            return
        seen.add(dedup_key)
        out.append(
            {
                "ids": merged,
                "reason": why,
                "source": src,
                "confidence": round(conf, 3),
            }
        )

    # from key
    try:
        if ":" in key:
            ns, rest = key.split(":", 1)
            base = rest.split("#", 1)[0].strip()
            if ns in ("imdb", "tmdb", "tvdb", "trakt") and not ids.get(ns):
                push({ns: base}, f"from key:{ns}", 0.92, "key")
    except Exception:
        pass

    # from peers (same title/year/type in state)
    try:
        peer = _peer_ids(s, it)
        if peer:
            push(peer, "from peers (title/year/type)", 0.87, "peers")
    except Exception:
        pass

    # TMDB from imdb
    try:
        if ids.get("imdb"):
            f = _tmdb(f"/find/{ids['imdb']}", {"external_source": "imdb_id"})
            if typ == "movie" and f.get("movie_results"):
                tid = f["movie_results"][0]["id"]
                ext = _tmdb(f"/movie/{tid}/external_ids", {})
                push(
                    {"tmdb": tid, "imdb": ext.get("imdb_id")},
                    "TMDB ext (movie)",
                    0.98,
                    "tmdb",
                )
            elif typ in ("show", "episode") and (f.get("tv_results") or f.get("tv_episode_results")):
                base = (f.get("tv_results") or [{"id": None}])[0]["id"]
                if typ == "show" and base:
                    ext = _tmdb(f"/tv/{base}/external_ids", {})
                    push(
                        {
                            "tmdb": base,
                            "imdb": ext.get("imdb_id"),
                            "tvdb": ext.get("tvdb_id"),
                        },
                        "TMDB ext (tv)",
                        0.98,
                        "tmdb",
                    )
                if typ == "episode" and base and se:
                    sN, eN = se
                    ext = _tmdb(
                        f"/tv/{base}/season/{sN}/episode/{eN}/external_ids", {}
                    )
                    push(
                        {
                            "tmdb": base,
                            "imdb": ext.get("imdb_id"),
                            "tvdb": ext.get("tvdb_id"),
                        },
                        "TMDB ext (ep)",
                        0.98,
                        "tmdb",
                    )
    except Exception:
        pass

    # TMDB from tmdb id
    try:
        if ids.get("tmdb"):
            if typ == "movie":
                ext = _tmdb(f"/movie/{ids['tmdb']}/external_ids", {})
                push(
                    {"imdb": ext.get("imdb_id")},
                    "TMDB ext (movie)",
                    0.98,
                    "tmdb",
                )
            elif typ == "show":
                ext = _tmdb(f"/tv/{ids['tmdb']}/external_ids", {})
                push(
                    {
                        "imdb": ext.get("imdb_id"),
                        "tvdb": ext.get("tvdb_id"),
                    },
                    "TMDB ext (tv)",
                    0.98,
                    "tmdb",
                )
            elif typ == "episode" and se:
                sN, eN = se
                ext = _tmdb(
                    f"/tv/{ids['tmdb']}/season/{sN}/episode/{eN}/external_ids", {}
                )
                push(
                    {
                        "imdb": ext.get("imdb_id"),
                        "tvdb": ext.get("tvdb_id"),
                    },
                    "TMDB ext (ep)",
                    0.98,
                    "tmdb",
                )
    except Exception:
        pass

    # TMDB search by title/year
    try:
        if title:
            if typ == "movie":
                r = _tmdb("/search/movie", {"query": title, "year": year or ""})
                if r.get("results"):
                    cand = r["results"][0]
                    tid = cand["id"]
                    yr = int((cand.get("release_date") or "0000")[:4] or 0)
                    conf = 0.9 - min(abs((year or 0) - yr), 2) * 0.05
                    ext = _tmdb(f"/movie/{tid}/external_ids", {})
                    push(
                        {"tmdb": tid, "imdb": ext.get("imdb_id")},
                        "TMDB search(movie)",
                        conf,
                        "tmdb",
                    )
            elif typ in ("show", "episode"):
                r = _tmdb(
                    "/search/tv",
                    {"query": title, "first_air_date_year": year or ""}, 
                )
                if r.get("results"):
                    base = r["results"][0]["id"]
                    ext = _tmdb(f"/tv/{base}/external_ids", {})
                    push(
                        {
                            "tmdb": base,
                            "imdb": ext.get("imdb_id"),
                            "tvdb": ext.get("tvdb_id"),
                        },
                        "TMDB search(tv)",
                        0.88,
                        "tmdb",
                    )
    except Exception:
        pass

    # Alt/language title hints
    try:
        core_ids = {
            ns: str(ids.get(ns)).strip()
            for ns in ("imdb", "tmdb", "tvdb")
            if ids.get(ns)
        }
        if core_ids and title:
            base_title = title.strip().lower()
            prov_up = (prov or "").upper()
            lang_prov_targets = {
                "PLEX": {"JELLYFIN", "EMBY"},
                "JELLYFIN": {"PLEX", "EMBY"},
                "EMBY": {"PLEX", "JELLYFIN"},
            }
            allowed_targets = lang_prov_targets.get(prov_up, set())
            if allowed_targets:
                hint_with: List[Tuple[str, str]] = []
                for p2, f2, k2, it2 in _iter_items(s):
                    if f2 != feat:
                        continue
                    p2_up = (p2 or "").upper()
                    if p2_up not in allowed_targets:
                        continue
                    ids2 = it2.get("ids") or {}
                    if not any(
                        str(ids2.get(ns) or "").strip() == val
                        for ns, val in core_ids.items()
                    ):
                        continue
                    t2 = (it2.get("title") or "").strip()
                    if not t2:
                        continue
                    if t2.strip().lower() == base_title:
                        continue
                    hint_with.append((p2_up, t2))
                    if len(hint_with) >= 1:
                        break
                if hint_with:
                    p2_up, t2 = hint_with[0]
                    why = f"Possible language/alt-title mismatch with {p2_up}"
                    push({}, why, 0.84, "titles")
    except Exception:
        pass

    # Trakt search by title/year
    try:
        core_have = any(ids.get(ns) for ns in ("imdb", "tmdb", "tvdb"))
        if title and not core_have:
            tmap = {"movie": "movie", "show": "show", "episode": "episode"}
            t = tmap.get(typ, "movie,show")
            r = _trakt("/search/" + t, {"query": title, "year": year or ""})
            for e in (r or [])[:3]:
                ids2 = (
                    e.get("movie")
                    or e.get("show")
                    or e.get("episode")
                    or {}
                ).get("ids") or {}
                push(
                    {
                        "trakt": ids2.get("trakt"),
                        "imdb": ids2.get("imdb"),
                        "tmdb": ids2.get("tmdb"),
                        "tvdb": ids2.get("tvdb"),
                    },
                    "Trakt search",
                    0.80,
                    "trakt",
                )
    except Exception:
        pass

    miss: List[str] = []
    if not _tmdb_key():
        miss.append("tmdb.api_key")
    if not _trakt_headers().get("trakt-api-key"):
        miss.append("trakt.client_id")
    return {"suggestions": out, "needs": miss}

# ── API
@router.get("/analyzer/state", response_class=JSONResponse)
def api_state():
    try:
        s = _load_state()
    except HTTPException as e:
        if e.status_code == 404:
            s = {}
        else:
            raise
    return {"counts": _counts(s), "items": _collect_items(s)}

@router.get("/analyzer/problems", response_class=JSONResponse)
def api_problems():
    s = _load_state()
    return {"problems": _problems(s), "pair_stats": _pair_stats(s)}

@router.get("/analyzer/ratings-audit", response_class=JSONResponse)
def api_ratings_audit():
    s = _load_state()
    return _ratings_audit(s)

@router.get("/analyzer/cw-state", response_class=JSONResponse)
def api_cw_state():
    return _read_cw_state()

@router.post("/analyzer/patch", response_class=JSONResponse)
def api_patch(payload: Dict[str, Any]):
    for f in ("provider", "feature", "key", "ids"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")
    s = _load_state()
    b, it = _find_item(s, payload["provider"], payload["feature"], payload["key"])
    if not b or not it:
        raise HTTPException(404, "Item not found")
    ids = dict(it.get("ids") or {})
    for k, v in (payload.get("ids") or {}).items():
        nv = _norm(k, v)
        if nv is None:
            ids.pop(k, None)
        else:
            ids[k] = nv
    it["ids"] = ids
    new = payload["key"]
    if payload.get("rekey"):
        new = _rekey(b, payload["key"], it)
    if payload.get("merge_peer_ids"):
        peer_ids = _peer_ids(s, it)
        for k, v in peer_ids.items():
            if k not in ids and v:
                ids[k] = v
    it["ids"] = ids
    new = _rekey(b, payload["key"], it)
    pairs = _pair_map(_cfg(), s)
    idx = _indices_for(s)
    pair_libs = _pair_lib_filters(_cfg())
    it["_ignore_missing_peer"] = not _has_peer_by_pairs(
        s, pairs, payload["provider"], payload["feature"], new, it, idx, pair_libs
    )
    _save_state(s)
    return {"ok": True, "new_key": new}

@router.post("/analyzer/suggest", response_class=JSONResponse)
def api_suggest(payload: Dict[str, Any]):
    s = _load_state()
    p = payload or {}
    return _suggest(s, p.get("provider"), p.get("feature"), p.get("key"))

@router.post("/analyzer/fix", response_class=JSONResponse)
def api_fix(payload: Dict[str, Any]):
    s = _load_state()
    r = _apply_fix(s, payload)
    _save_state(s)
    return r

@router.patch("/analyzer/item", response_class=JSONResponse)
def api_edit(payload: Dict[str, Any]):
    for f in ("provider", "feature", "key", "updates"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")
    s = _load_state()
    b = _bucket(s, payload["provider"], payload["feature"])
    if not b or payload["key"] not in b:
        raise HTTPException(404, "Item not found")

    it = b[payload["key"]]
    up = payload["updates"]

    if "title" in up:
        it["title"] = up["title"]
    if "year" in up:
        it["year"] = up["year"]
    if "ids" in up and isinstance(up["ids"], dict):
        ids = it.setdefault("ids", {})
        for k, v in up["ids"].items():
            if v is None:
                ids.pop(k, None)
            elif v != "":
                ids[k] = v

    new = _rekey(b, payload["key"], it)
    pairs = _pair_map(_cfg(), s)
    idx = _indices_for(s)
    pair_libs = _pair_lib_filters(_cfg())
    it["_ignore_missing_peer"] = not _has_peer_by_pairs(
        s, pairs, payload["provider"], payload["feature"], new, it, idx, pair_libs
    )
    _save_state(s)
    return {"ok": True, "new_key": new}

@router.delete("/analyzer/item", response_class=JSONResponse)
def api_delete(payload: Dict[str, Any]):
    for f in ("provider", "feature", "key"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")
    s = _load_state()
    b = _bucket(s, payload["provider"], payload["feature"])
    if not b or payload["key"] not in b:
        raise HTTPException(404, "Item not found")
    b.pop(payload["key"], None)
    _save_state(s)
    return {"ok": True}
