# services/analyzer.py
# CrossWatch - Data analyzer for state
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable
from pathlib import Path
import json
import re
import threading

import requests
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from cw_platform.config_base import CONFIG as CONFIG_DIR, load_config
from cw_platform.provider_instances import normalize_instance_id

router = APIRouter(prefix="/api", tags=["analyzer"])
STATE_PATH = CONFIG_DIR / "state.json"
MANUAL_STATE_PATH = CONFIG_DIR / "state.manual.json"
CWS_DIR = CONFIG_DIR / ".cw_state"
_LOCK = threading.Lock()

_DEFAULT_INSTANCE = "default"
_PROV_TOKEN_SEPS = ("@", "#", ":")

def _split_prov_token(v: Any) -> tuple[str, str]:
    raw = str(v or "").strip()
    if not raw:
        return "", _DEFAULT_INSTANCE
    for sep in _PROV_TOKEN_SEPS:
        if sep in raw:
            a, b = raw.split(sep, 1)
            return str(a or "").upper().strip(), normalize_instance_id(b)
    return raw.upper(), _DEFAULT_INSTANCE

def _split_prov_token_ex(v: Any) -> tuple[str, str, bool]:
    raw = str(v or "").strip()
    if not raw:
        return "", _DEFAULT_INSTANCE, False
    for sep in _PROV_TOKEN_SEPS:
        if sep in raw:
            a, b = raw.split(sep, 1)
            return str(a or "").upper().strip(), normalize_instance_id(b), True
    return raw.upper(), _DEFAULT_INSTANCE, False


def _prov_token(prov: str, inst: Any = None) -> str:
    p = str(prov or "").upper().strip()
    i = normalize_instance_id(inst)
    return p if i == _DEFAULT_INSTANCE else f"{p}@{i}"

def _norm_prov_token(v: Any) -> str:
    base, inst = _split_prov_token(v)
    return _prov_token(base, inst)




def _cfg() -> dict[str, Any]:
    try:
        cfg = load_config()
    except Exception:
        return {}
    return cfg or {}


def _tmdb_key() -> str:
    cfg = _cfg()
    for root_key in ("tmdb", "tmdb_sync"):
        blk = cfg.get(root_key)
        if isinstance(blk, dict):
            k = str(blk.get("api_key") or "").strip()
            if k:
                return k
            insts = blk.get("instances")
            if isinstance(insts, dict):
                for _, ib in insts.items():
                    if isinstance(ib, dict):
                        k2 = str(ib.get("api_key") or "").strip()
                        if k2:
                            return k2
    return ""

def _trakt_headers() -> dict[str, str]:
    cfg = _cfg()
    base = cfg.get("trakt")
    blocks: list[dict[str, Any]] = []
    if isinstance(base, dict):
        blocks.append(base)
        insts = base.get("instances")
        if isinstance(insts, dict):
            for _, ib in insts.items():
                if isinstance(ib, dict):
                    blocks.append(ib)

    client_id = ""
    token = ""
    for b in blocks:
        if not client_id:
            client_id = str(b.get("client_id") or "").strip()
        if not token:
            token = str(b.get("access_token") or "").strip()
        if client_id and token:
            break

    h: dict[str, str] = {
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

def _safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"


def _parse_pairs_raw(pairs_raw: str | None) -> list[str]:
    if not pairs_raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for part in str(pairs_raw).split(","):
        v = str(part or "").strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _state_candidates(token: str) -> list[Path]:
    return [
        CONFIG_DIR / f"state.{token}.json",
        CWS_DIR / f"state.{token}.json",
    ]


def _pick_existing(paths: list[Path]) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None


def _load_state_at(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise HTTPException(404, f"{path.name} not found")
    except Exception:
        raise HTTPException(500, f"Failed to parse {path.name}")


def _load_state_handles(pairs_raw: str | None) -> list[dict[str, Any]]:
    pairs = _parse_pairs_raw(pairs_raw)
    handles: list[dict[str, Any]] = []
    if pairs:
        for pid in pairs:
            safe = _safe_scope(pid)
            cand = _state_candidates(safe)
            if safe != str(pid):
                cand += _state_candidates(str(pid))
            path = _pick_existing(cand)
            if path is None:
                continue
            handles.append({"pair": pid, "safe": safe, "path": path, "state": _load_state_at(path)})
        if handles:
            return handles

    if STATE_PATH.exists():
        return [{"pair": None, "safe": None, "path": STATE_PATH, "state": _load_state_at(STATE_PATH)}]
    raise HTTPException(404, "No analyzer state found")


def _merge_states(handles: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {"providers": {}}

    def merge_feat(dst_blk: dict[str, Any], src_blk: dict[str, Any], feat: str) -> None:
        items = (((src_blk.get(feat) or {}).get("baseline") or {}).get("items") or {})
        if not isinstance(items, dict):
            return
        mb = dst_blk.setdefault(feat, {}).setdefault("baseline", {}).setdefault("items", {})
        if not isinstance(mb, dict):
            return

        for k, it in items.items():
            if k not in mb:
                mb[k] = dict(it or {})
                continue

            a = mb.get(k)
            if not isinstance(a, dict) or not isinstance(it, dict):
                continue

            ida = dict(a.get("ids") or {})
            idb = dict(it.get("ids") or {})
            for ns, vv in idb.items():
                if ns not in ida and vv:
                    ida[ns] = vv
            if ida:
                a["ids"] = ida

            for fld in ("title", "year", "type", "series_title", "season", "episode"):
                if fld not in a and fld in it:
                    a[fld] = it.get(fld)

    for h in handles:
        s = h.get("state") or {}
        provs = s.get("providers") if isinstance(s, dict) else None
        if not isinstance(provs, dict):
            continue

        for prov, pv in provs.items():
            if not isinstance(pv, dict):
                continue
            mpv = merged["providers"].setdefault(prov, {})  # type: ignore[index]
            if not isinstance(mpv, dict):
                continue

            for feat in ("history", "watchlist", "ratings"):
                merge_feat(mpv, pv, feat)

            insts = pv.get("instances")
            if not isinstance(insts, dict) or not insts:
                continue

            minst = mpv.setdefault("instances", {})
            if not isinstance(minst, dict):
                minst = {}
                mpv["instances"] = minst

            for inst_id, blk in insts.items():
                if not isinstance(blk, dict):
                    continue
                dib = minst.setdefault(str(inst_id), {})
                if not isinstance(dib, dict):
                    dib = {}
                    minst[str(inst_id)] = dib
                for feat in ("history", "watchlist", "ratings"):
                    merge_feat(dib, blk, feat)

    return merged

def _load_state(pairs_raw: str | None = None) -> dict[str, Any]:
    handles = _load_state_handles(pairs_raw)
    return _merge_states(handles)


def _save_state_at(path: Path, s: dict[str, Any]) -> None:
    with _LOCK:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(s, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)


def _save_state(s: dict[str, Any]) -> None:
    _save_state_at(STATE_PATH, s)


def _load_manual_state() -> dict[str, Any]:
    try:
        return json.loads(MANUAL_STATE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def _manual_add_blocks(manual: dict[str, Any]) -> dict[tuple[str, str], set[str]]:
    out: dict[tuple[str, str], set[str]] = {}
    providers = manual.get("providers") if isinstance(manual, dict) else None
    if not isinstance(providers, dict):
        return out
    for prov, prov_data in providers.items():
        if not isinstance(prov_data, dict):
            continue
        for feat, feat_data in prov_data.items():
            if not isinstance(feat_data, dict):
                continue
            adds = feat_data.get("adds")
            if not isinstance(adds, dict):
                continue
            blocks = adds.get("blocks")
            if not isinstance(blocks, list) or not blocks:
                continue
            out[(str(prov).upper(), str(feat).lower())] = set(str(x) for x in blocks if x)
    return out

def _iter_items(s: dict[str, Any]) -> Iterable[tuple[str, str, str, dict[str, Any]]]:
    provs = s.get("providers") if isinstance(s, dict) else None
    if not isinstance(provs, dict):
        return
    for prov, pv in provs.items():
        if not isinstance(pv, dict):
            continue

        for feat in ("history", "watchlist", "ratings"):
            items = (((pv.get(feat) or {}).get("baseline") or {}).get("items") or {})
            if isinstance(items, dict):
                for k, it in items.items():
                    yield _prov_token(str(prov)), feat, str(k), (it or {})

        insts = pv.get("instances")
        if not isinstance(insts, dict) or not insts:
            continue
        for inst_id, blk in insts.items():
            if not isinstance(blk, dict):
                continue
            tok = _prov_token(str(prov), inst_id)
            for feat in ("history", "watchlist", "ratings"):
                items = (((blk.get(feat) or {}).get("baseline") or {}).get("items") or {})
                if not isinstance(items, dict):
                    continue
                for k, it in items.items():
                    yield tok, feat, str(k), (it or {})

def _bucket(s: dict[str, Any], prov: str, feat: str) -> dict[str, Any] | None:
    provs = s.get("providers") if isinstance(s, dict) else None
    if not isinstance(provs, dict):
        return None

    p, inst, _ = _split_prov_token_ex(prov)
    if not p:
        return None

    pv = provs.get(p)
    if not isinstance(pv, dict):
        return None

    blk: dict[str, Any] = pv
    if inst != _DEFAULT_INSTANCE:
        insts = pv.get("instances")
        if isinstance(insts, dict) and isinstance(insts.get(inst), dict):
            blk = insts.get(inst) or {}
        else:
            return None

    try:
        items = blk[feat]["baseline"]["items"]  # type: ignore[index]
        return items if isinstance(items, dict) else None
    except Exception:
        return None

def _iter_buckets_for_selector(
    s: dict[str, Any],
    prov_selector: str,
    feat: str,
) -> Iterable[tuple[str, dict[str, Any]]]:
    provs = s.get("providers") if isinstance(s, dict) else None
    if not isinstance(provs, dict):
        return

    p, inst, explicit = _split_prov_token_ex(prov_selector)
    if not p:
        return
    pv = provs.get(p)
    if not isinstance(pv, dict):
        return

    if explicit:
        tok = _prov_token(p, inst)
        b = _bucket(s, tok, feat)
        if b is not None:
            yield tok, b
        return

    b0 = _bucket(s, p, feat)
    if b0 is not None:
        yield _prov_token(p, _DEFAULT_INSTANCE), b0

    insts = pv.get("instances")
    if not isinstance(insts, dict):
        return
    for inst_id, blk in insts.items():
        if not isinstance(blk, dict):
            continue
        tok = _prov_token(p, inst_id)
        b = _bucket(s, tok, feat)
        if b is not None:
            yield tok, b


def _find_items(
    s: dict[str, Any],
    prov_selector: str,
    feat: str,
    key: str,
) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    hits: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for tok, b in _iter_buckets_for_selector(s, prov_selector, feat):
        if key in b and isinstance(b.get(key), dict):
            hits.append((tok, b, b[key]))
    return hits

def _find_item(
    s: dict[str, Any],
    prov: str,
    feat: str,
    key: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    hits = _find_items(s, prov, feat, key)
    if not hits:
        return None, None
    _, b, it = hits[0]
    return b, it

def _counts(s: dict[str, Any]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for prov, feat, _, _ in _iter_items(s):
        cur = out.setdefault(prov, {"history": 0, "watchlist": 0, "ratings": 0, "total": 0})
        if feat in ("history", "watchlist", "ratings"):
            cur[feat] = int(cur.get(feat, 0)) + 1
            cur["total"] = int(cur.get("total", 0)) + 1
    return out

def _collect_items(s: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for prov_tok, feat, k, it in _iter_items(s):
        base, inst = _split_prov_token(prov_tok)
        out.append(
            {
                "provider": prov_tok,
                "provider_base": base,
                "instance": None if inst == _DEFAULT_INSTANCE else inst,
                "feature": feat,
                "key": k,
                "title": it.get("title"),
                "year": it.get("year"),
                "type": it.get("type"),
                "series_title": it.get("series_title"),
                "season": it.get("season"),
                "episode": it.get("episode"),
                "ids": it.get("ids") or {},
            }
        )
    return out


_ID_RX: dict[str, re.Pattern[str]] = {
    "imdb": re.compile(r"^tt\d{5,}$"),
    "tmdb": re.compile(r"^\d+$"),
    "tvdb": re.compile(r"^\d+$"),
    "plex": re.compile(r"^\d+$"),
    "trakt": re.compile(r"^\d+$"),
    "simkl": re.compile(r"^\d+$"),
    "emby": re.compile(r"^[A-Za-z0-9-]{4,}$"),
    "mdblist": re.compile(r"^\d+$"),
}


def _read_cw_state(allowed_scopes: set[str] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not (CWS_DIR.exists() and CWS_DIR.is_dir()):
        return out

    scopes = set(allowed_scopes or [])
    for p in sorted(CWS_DIR.glob("*.json")):
        if scopes:
            if not any(p.name.endswith(f".{safe}.json") for safe in scopes):
                continue
        try:
            out[p.name] = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            out[p.name] = {"_error": "parse_error"}
    return out


def _alias_keys(obj: dict[str, Any]) -> list[str]:
    t = (obj.get("type") or "").lower()
    ids = dict(obj.get("ids") or {})
    out: list[str] = []
    seen: set[str] = set()

    if obj.get("_key"):
        out.append(obj["_key"])

    for ns in ("imdb", "tmdb", "tvdb", "mal", "anilist", "trakt", "simkl", "plex", "emby", "guid", "mdblist"):
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

    res: list[str] = []
    for k in out:
        if k not in seen:
            seen.add(k)
            res.append(k)
    return res


def _alias_index(items: dict[str, Any]) -> dict[str, str]:
    idx: dict[str, str] = {}
    for k, v in items.items():
        vv = dict(v)
        vv["_key"] = k
        for ak in _alias_keys(vv):
            idx.setdefault(ak, k)
    return idx

def _class_key(it: dict[str, Any]) -> tuple[str, str, int | None]:
    return ((it.get("type") or "").lower(), (it.get("title") or "").strip().lower(), it.get("year"))

def _pair_map(cfg: dict[str, Any], _state: dict[str, Any]) -> dict[tuple[str, str], list[str]]:
    mp: dict[tuple[str, str], list[str]] = defaultdict(list)
    pairs = cfg.get("pairs") or []

    def add(src: str, feat: str, dst: str) -> None:
        k = (src, feat)
        if dst not in mp[k]:
            mp[k].append(dst)

    for pr in pairs:
        if not isinstance(pr, dict):
            continue

        src = str(pr.get("src") or pr.get("source") or "").upper().strip()
        dst = str(pr.get("dst") or pr.get("target") or "").upper().strip()
        if not (src and dst):
            continue
        if pr.get("enabled") is False:
            continue

        si = normalize_instance_id(pr.get("src_instance") or pr.get("source_instance"))
        ti = normalize_instance_id(pr.get("dst_instance") or pr.get("target_instance"))
        src_tok = _prov_token(src, si)
        dst_tok = _prov_token(dst, ti)

        mode = str(pr.get("mode") or "one-way").lower()
        feats = pr.get("features")
        feats_list: list[str] = []
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
            add(src_tok, f, dst_tok)
            if mode in ("two-way", "bi", "both", "mirror", "two", "two_way", "two way"):
                add(dst_tok, f, src_tok)

    return mp

def _supports_pair_libs(prov: str) -> bool:
    base, _ = _split_prov_token(prov)
    return base in ("PLEX", "EMBY", "JELLYFIN")


_TYPE_TOKEN_MAP: dict[str, str] = {
    "movie": "movie",
    "movies": "movie",
    "show": "show",
    "shows": "show",
    "tv": "show",
    "episode": "episode",
    "episodes": "episode",
    "season": "season",
    "seasons": "season",
    "anime": "anime",
    "animes": "anime",
}

_PROVIDER_ALLOWED_TYPES: dict[str, set[str]] = {
    "ANILIST": {"anime"},
}

def _provider_allowed_types(prov: str, feat: str) -> set[str] | None:
    _ = feat
    base, _ = _split_prov_token(prov)
    return _PROVIDER_ALLOWED_TYPES.get(base)

def _item_type(it: dict[str, Any]) -> str:
    t = str((it or {}).get("type") or "").strip().lower()
    return _TYPE_TOKEN_MAP.get(t, t)

def _pair_type_filters(cfg: dict[str, Any]) -> dict[tuple[str, str, str], set[str]]:
    out: dict[tuple[str, str, str], set[str]] = {}

    def is_on(feat: Any) -> bool:
        if isinstance(feat, dict) and "enable" in feat:
            return bool(feat.get("enable"))
        return bool(feat)

    def norm_types(raw: Any) -> set[str]:
        if not isinstance(raw, (list, tuple)):
            return set()
        out0: set[str] = set()
        for x in raw:
            s = str(x or "").strip().lower()
            if not s:
                continue
            out0.add(_TYPE_TOKEN_MAP.get(s, s))
        return out0

    def merge_dir(a: str, b: str, feat: str, types0: set[str]) -> None:
        if not types0:
            return
        key = (a, feat, b)
        if key in out:
            out[key] = out[key].intersection(types0)
        else:
            out[key] = set(types0)

    two_way = ("two-way", "bi", "both", "mirror", "two", "two_way", "two way")

    for pr in cfg.get("pairs") or []:
        if not isinstance(pr, dict):
            continue
        src = str(pr.get("src") or pr.get("source") or "").upper().strip()
        dst = str(pr.get("dst") or pr.get("target") or "").upper().strip()
        if not src or not dst:
            continue
        if pr.get("enabled") is False:
            continue

        si = normalize_instance_id(pr.get("src_instance") or pr.get("source_instance"))
        ti = normalize_instance_id(pr.get("dst_instance") or pr.get("target_instance"))
        src_tok = _prov_token(src, si)
        dst_tok = _prov_token(dst, ti)

        mode = str(pr.get("mode") or "one-way").lower()
        feats = pr.get("features") or {}
        if not isinstance(feats, dict):
            continue

        for feat in ("history", "watchlist", "ratings"):
            fcfg = feats.get(feat)
            if not is_on(fcfg):
                continue

            raw_types = fcfg.get("types") if isinstance(fcfg, dict) else None
            if raw_types is not None:
                if isinstance(raw_types, dict):
                    merge_dir(
                        src_tok,
                        dst_tok,
                        feat,
                        norm_types(
                            raw_types.get(src_tok)
                            or raw_types.get(src)
                            or raw_types.get(src.lower())
                            or raw_types.get(src.upper())
                        ),
                    )
                    if mode in two_way:
                        merge_dir(
                            dst_tok,
                            src_tok,
                            feat,
                            norm_types(
                                raw_types.get(dst_tok)
                                or raw_types.get(dst)
                                or raw_types.get(dst.lower())
                                or raw_types.get(dst.upper())
                            ),
                        )
                else:
                    merge_dir(src_tok, dst_tok, feat, norm_types(raw_types))
                    if mode in two_way:
                        merge_dir(dst_tok, src_tok, feat, norm_types(raw_types))

            prov_types = _provider_allowed_types(dst_tok, feat)
            if prov_types:
                merge_dir(src_tok, dst_tok, feat, prov_types)
            if mode in two_way:
                prov_types_rev = _provider_allowed_types(src_tok, feat)
                if prov_types_rev:
                    merge_dir(dst_tok, src_tok, feat, prov_types_rev)

    return out

def _passes_pair_type_filter(
    pair_types: dict[tuple[str, str, str], set[str]] | None,
    prov: str,
    feat: str,
    dst: str,
    item: dict[str, Any],
) -> bool:
    if not pair_types:
        return True
    p = _norm_prov_token(prov)
    f = str(feat or "").lower()
    d = _norm_prov_token(dst)
    allowed = pair_types.get((p, f, d))
    if not allowed:
        return True
    t = _item_type(item)
    if not t:
        return True
    return t in allowed

def _item_library_id(it: dict[str, Any]) -> str | None:
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

def _pair_lib_filters(cfg: dict[str, Any]) -> dict[tuple[str, str, str], set[str]]:
    out: dict[tuple[str, str, str], set[str]] = {}
    for pr in cfg.get("pairs") or []:
        src = str(pr.get("src") or pr.get("source") or "").upper().strip()
        dst = str(pr.get("dst") or pr.get("target") or "").upper().strip()
        if not (src and dst):
            continue
        if pr.get("enabled") is False:
            continue

        si = normalize_instance_id(pr.get("src_instance") or pr.get("source_instance"))
        ti = normalize_instance_id(pr.get("dst_instance") or pr.get("target_instance"))
        src_tok = _prov_token(src, si)
        dst_tok = _prov_token(dst, ti)

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

            def add_dir(a_tok: str, a_base: str, b_tok: str) -> None:
                if not _supports_pair_libs(a_tok):
                    return
                raw = (
                    libs_dict.get(a_tok)
                    or libs_dict.get(a_base)
                    or libs_dict.get(a_base.lower())
                    or libs_dict.get(a_base.upper())
                )
                if isinstance(raw, (list, tuple)) and raw:
                    allowed = {str(x).strip() for x in raw if str(x).strip()}
                    if allowed:
                        out[(a_tok, feat, b_tok)] = allowed

            add_dir(src_tok, src, dst_tok)
            if mode in ("two-way", "bi", "both", "mirror", "two", "two_way", "two way"):
                add_dir(dst_tok, dst, src_tok)

    return out

def _passes_pair_lib_filter(
    pair_libs: dict[tuple[str, str, str], set[str]] | None,
    prov: str,
    feat: str,
    dst: str,
    item: dict[str, Any],
) -> bool:
    if not pair_libs:
        return True
    p = _norm_prov_token(prov)
    f = str(feat or "").lower()
    d = _norm_prov_token(dst)
    allowed = pair_libs.get((p, f, d))
    if not allowed:
        return True
    lid = _item_library_id(item)
    if lid is None:
        return True
    return lid in allowed

def _indices_for(s: dict[str, Any]) -> dict[tuple[str, str], dict[str, str]]:
    out: dict[tuple[str, str], dict[str, str]] = {}
    for p, f, _, _ in _iter_items(s):
        key = (p, f)
        if key not in out:
            out[key] = _alias_index(_bucket(s, p, f) or {})
    return out

def _has_peer_by_pairs(
    s: dict[str, Any],
    pairs: dict[tuple[str, str], list[str]],
    prov: str,
    feat: str,
    item_key: str,
    item: dict[str, Any],
    idx_cache: dict[tuple[str, str], dict[str, str]],
    pair_libs: dict[tuple[str, str, str], set[str]] | None = None,
    pair_types: dict[tuple[str, str, str], set[str]] | None = None,
) -> bool:
    if feat not in ("history", "watchlist", "ratings"):
        return True

    prov_key = _norm_prov_token(prov)
    feat_key = str(feat or "").lower()
    targets = pairs.get((prov_key, feat_key), [])
    if not targets:
        return True

    filtered_targets: list[str] = []
    for dst in targets:
        if _passes_pair_lib_filter(pair_libs, prov_key, feat_key, dst, item) and _passes_pair_type_filter(pair_types, prov_key, feat_key, dst, item):
            filtered_targets.append(dst)
    if not filtered_targets:
        return True

    vv = dict(item)
    vv["_key"] = item_key
    keys = set(_alias_keys(vv))
    for dst in filtered_targets:
        dst_key = _norm_prov_token(dst)
        idx = idx_cache.get((dst_key, feat_key)) or {}
        if any(k in idx for k in keys):
            return True
    return False

def _pair_stats(s: dict[str, Any]) -> list[dict[str, Any]]:
    stats: list[dict[str, Any]] = []
    cfg = _cfg()
    pairs = _pair_map(cfg, s)
    idx_cache = _indices_for(s)
    pair_libs = _pair_lib_filters(cfg)
    pair_types = _pair_type_filters(cfg)
    for (prov, feat), targets in pairs.items():
        src_items = _bucket(s, prov, feat) or {}
        for dst in targets:
            total = 0
            synced = 0
            idx = idx_cache.get((dst, feat)) or {}

            for k, v in src_items.items():
                if v.get("_ignore_missing_peer"):
                    continue
                if not _passes_pair_lib_filter(pair_libs, prov, feat, dst, v) or not _passes_pair_type_filter(pair_types, prov, feat, dst, v):
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


def _pair_exclusions(s: dict[str, Any]) -> list[dict[str, Any]]:
    cfg = _cfg()
    pairs = _pair_map(cfg, s)
    pair_libs = _pair_lib_filters(cfg)
    pair_types = _pair_type_filters(cfg)
    out: list[dict[str, Any]] = []

    for (prov, feat), targets in pairs.items():
        if not targets:
            continue
        src_items = _bucket(s, prov, feat) or {}
        if not src_items:
            continue

        for dst in targets:
            excluded_types: dict[str, int] = {}
            excluded_libs: dict[str, int] = {}

            scanned_total = 0
            accepted_total = 0

            for v in src_items.values():
                if not isinstance(v, dict):
                    continue
                if v.get("_ignore_missing_peer"):
                    continue

                scanned_total += 1

                if not _passes_pair_type_filter(pair_types, prov, feat, dst, v):
                    t = _item_type(v)
                    if t:
                        excluded_types[t] = excluded_types.get(t, 0) + 1
                    continue

                if not _passes_pair_lib_filter(pair_libs, prov, feat, dst, v):
                    lid = _item_library_id(v) or "unknown"
                    excluded_libs[lid] = excluded_libs.get(lid, 0) + 1
                    continue

                accepted_total += 1
            total = sum(excluded_types.values()) + sum(excluded_libs.values())
            if not total:
                continue

            rec: dict[str, Any] = {
                "source": prov,
                "target": dst,
                "feature": feat,
                "excluded_total": total,
                "scanned_total": scanned_total,
                "accepted_total": accepted_total,
            }
            if excluded_types:
                rec["excluded_types"] = excluded_types
            if excluded_libs:
                rec["excluded_libraries"] = excluded_libs

            allowed_types = pair_types.get((prov, feat, dst)) if pair_types else None
            allowed_libs = pair_libs.get((prov, feat, dst)) if pair_libs else None
            if allowed_types:
                rec["allowed_types"] = sorted(allowed_types)
            if allowed_libs:
                rec["allowed_libraries"] = sorted(allowed_libs)

            out.append(rec)

    return out

def _history_show_sets(s: dict[str, Any]) -> tuple[dict[str, set[str]], dict[str, str]]:
    show_sets: dict[str, set[str]] = {}
    labels: dict[str, str] = {}

    def pick_sig(obj: Any) -> str | None:
        if not isinstance(obj, dict):
            return None
        for idk in ("imdb", "tmdb", "tvdb", "slug"):
            v = obj.get(idk)
            if v:
                return f"{idk}:{str(v).lower()}"
        return None

    def title_key(rec: dict[str, Any]) -> tuple[str, int | None] | None:
        title = rec.get("series_title") or rec.get("show_title") or rec.get("title") or rec.get("name")
        if not title:
            return None
        t = str(title).strip().lower()
        if not t:
            return None
        y = rec.get("series_year") or rec.get("year")
        yi: int | None = None
        if y not in (None, ""):
            try:
                yi = int(y)
            except Exception:
                yi = None
        return (t, yi)

    def best_sig(sigs: set[str]) -> str | None:
        if not sigs:
            return None
        by_ns: dict[str, set[str]] = {}
        for s0 in sigs:
            ns = s0.split(":", 1)[0] if ":" in s0 else ""
            by_ns.setdefault(ns, set()).add(s0)
        order = {"imdb": 0, "tmdb": 1, "tvdb": 2, "slug": 3}
        best = None
        best_p = 999
        for s0 in sigs:
            ns = s0.split(":", 1)[0] if ":" in s0 else ""
            p = order.get(ns, 999)
            if p < best_p:
                best_p = p
                best = s0
        return best

    def sig_prio(sig: str | None) -> int:
        if not sig or ":" not in sig:
            return 999
        order = {"imdb": 0, "tmdb": 1, "tvdb": 2, "slug": 3}
        return order.get(sig.split(":", 1)[0], 999)

    def show_id_sig(rec: dict[str, Any]) -> str | None:
        typ = str(rec.get("type") or "").strip().lower()
        if typ == "episode":
            return pick_sig(rec.get("show_ids") or {})
        if typ == "show":
            return pick_sig(rec.get("ids") or {})
        if rec.get("show_ids") or rec.get("series_title") or rec.get("show_title"):
            return pick_sig(rec.get("show_ids") or {})
        return None

    def ensure_label(sig: str) -> None:
        if sig in labels:
            return
        if sig.startswith("imdb:"):
            labels[sig] = sig.split(":", 1)[1].upper()
        elif sig.startswith("tmdb:"):
            labels[sig] = sig
        elif sig.startswith("tvdb:"):
            labels[sig] = sig
        else:
            labels[sig] = sig

    prov_block = (s.get("providers") or {}) if isinstance(s, dict) else {}

    title_ids: dict[str, set[str]] = {}
    title_year_ids: dict[tuple[str, int | None], set[str]] = {}

    def iter_hist_items(blk: dict[str, Any]) -> Iterable[dict[str, Any]]:
        hist = (blk or {}).get("history") or {}
        node = hist.get("baseline") if isinstance(hist, dict) else None
        node = node or hist
        items = node.get("items") if isinstance(node, dict) else None
        if not isinstance(items, dict):
            return []
        return [v for v in items.values() if isinstance(v, dict)]

    for prov_data in prov_block.values():
        if not isinstance(prov_data, dict):
            continue
        blocks = [prov_data]
        insts = prov_data.get("instances")
        if isinstance(insts, dict):
            for blk in insts.values():
                if isinstance(blk, dict):
                    blocks.append(blk)

        for blk in blocks:
            for rec in iter_hist_items(blk):
                show_sig = show_id_sig(rec)
                if not show_sig:
                    continue
                ensure_label(show_sig)
                tk = title_key(rec)
                if tk:
                    title_ids.setdefault(tk[0], set()).add(show_sig)
                    title_year_ids.setdefault(tk, set()).add(show_sig)

    title_best: dict[str, str] = {}
    for t, sigs in title_ids.items():
        b = best_sig(sigs)
        if b:
            title_best[t] = b

    title_year_best: dict[tuple[str, int | None], str] = {}
    for k, sigs in title_year_ids.items():
        b = best_sig(sigs)
        if b:
            title_year_best[k] = b

    for prov_name, prov_data in prov_block.items():
        base = str(prov_name or "").upper().strip()
        if not base or not isinstance(prov_data, dict):
            continue

        # default instance
        tok0 = _prov_token(base, _DEFAULT_INSTANCE)
        p_shows0: set[str] = set()
        for rec in iter_hist_items(prov_data):
            show_sig = show_id_sig(rec)
            if not show_sig:
                continue
            tk = title_key(rec)
            mapped = title_year_best.get(tk) if tk else None
            if not mapped and tk:
                mapped = title_best.get(tk[0])
            if mapped and sig_prio(mapped) < sig_prio(show_sig):
                show_sig = mapped
            if show_sig is None and tk:
                show_sig = f"{tk[0]}|year:{tk[1]}"
            if show_sig:
                p_shows0.add(show_sig)
                ensure_label(show_sig)
        show_sets[tok0] = p_shows0

        insts = prov_data.get("instances")
        if not isinstance(insts, dict):
            continue
        for inst_id, blk in insts.items():
            if not isinstance(blk, dict):
                continue
            tok = _prov_token(base, inst_id)
            p_shows: set[str] = set()
            for rec in iter_hist_items(blk):
                show_sig = show_id_sig(rec)
                if not show_sig:
                    continue
                tk = title_key(rec)
                mapped = title_year_best.get(tk) if tk else None
                if mapped and sig_prio(mapped) < sig_prio(show_sig):
                    show_sig = mapped
                if show_sig is None and tk:
                    show_sig = f"{tk[0]}|year:{tk[1]}"
                if show_sig:
                    p_shows.add(show_sig)
                    ensure_label(show_sig)
            show_sets[tok] = p_shows

    return show_sets, labels

def _history_normalization_issues(s: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []

    cfg = _cfg()
    pairs = _pair_map(cfg, s)
    show_sets, labels = _history_show_sets(s)
    tmdb_enabled = bool(_tmdb_key())

    seen: set[tuple[str, str]] = set()

    for (src, feat), targets in pairs.items():
        if feat != "history":
            continue
        a = _norm_prov_token(src)
        if not a:
            continue

        for dst in targets:
            b = _norm_prov_token(dst)
            if not b or a == b:
                continue

            key = (a, b) if a <= b else (b, a)
            if key in seen:
                continue
            seen.add(key)

            sa = show_sets.get(a) or set()
            sb = show_sets.get(b) or set()
            if not sa and not sb:
                continue

            only_a = sorted(sa - sb)
            only_b = sorted(sb - sa)
            if not only_a and not only_b:
                continue

            issue: dict[str, Any] = {
                "severity": "info",
                "type": "history_show_normalization",
                "feature": "history",
                "source": a,
                "target": b,
                "show_delta": {
                    "source": len(sa),
                    "target": len(sb),
                },
                "extra_source": only_a,
                "extra_target": only_b,
                "tmdb_enabled": tmdb_enabled,
            }

            if labels:
                issue["extra_source_titles"] = [labels.get(sig, sig) for sig in only_a]
                issue["extra_target_titles"] = [labels.get(sig, sig) for sig in only_b]

            issues.append(issue)

    return issues

def _history_show_signature(rec: dict[str, Any]) -> str | None:
    typ = str(rec.get("type") or "").strip().lower()
    ids = (rec.get("ids") or {}) or {}
    show_ids = (rec.get("show_ids") or {}) or {}

    def pick(obj: dict[str, Any]) -> str | None:
        for idk in ("imdb", "tmdb", "tvdb", "slug"):
            v = obj.get(idk)
            if v:
                return f"{idk}:{str(v).lower()}"
        return None

    sig: str | None = None
    if typ == "episode":
        sig = pick(show_ids)
    elif typ == "show":
        sig = pick(ids)
    else:
        if show_ids or rec.get("series_title") or rec.get("show_title"):
            sig = pick(show_ids)

    if sig is None:
        title = (
            rec.get("series_title")
            or rec.get("show_title")
            or rec.get("title")
            or rec.get("name")
        )
        if title:
            y = rec.get("series_year") or rec.get("year")
            sig = f"{str(title).strip().lower()}|year:{y}"
    return sig


def _missing_peer_show_hints(
    s: dict[str, Any],
    feat: str,
    item: dict[str, Any],
    targets: list[str],
) -> list[dict[str, Any]]:
    if feat != "history":
        return []

    sig = _history_show_signature(item)
    if not sig:
        return []

    season = item.get("season")
    episode = item.get("episode")
    out: list[dict[str, Any]] = []

    for dst in targets:
        bucket = _bucket(s, dst, feat) or {}
        show_episodes = 0
        has_episode = False

        for rec in bucket.values():
            if not isinstance(rec, dict):
                continue
            if _history_show_signature(rec) != sig:
                continue

            rtyp = str(rec.get("type") or "").strip().lower()
            if rtyp == "episode":
                show_episodes += 1
                if (
                    season is not None
                    and episode is not None
                    and rec.get("season") == season
                    and rec.get("episode") == episode
                ):
                    has_episode = True

        dst_name = str(dst or "").upper()
        if show_episodes == 0:
            msg = f"{dst_name} history snapshot has no entries for this item."
        elif has_episode:
            msg = (
                f"{dst_name} history snapshot already has this episode, "
                "but it did not match by IDs."
            )
        else:
            if season is not None and episode is not None:
                msg = (
                    f"{dst_name} has this show and {show_episodes} other episodes, "
                    f"but S{int(season):02d}E{int(episode):02d} is not in the "
                    f"{dst_name} history snapshot."
                )
            else:
                msg = (
                    f"{dst_name} has this show and {show_episodes} other episodes, "
                    f"but this entry is not in the {dst_name} history snapshot."
                )

        out.append(
            {
                "target": dst_name,
                "feature": feat,
                "show_episodes": show_episodes,
                "has_episode": has_episode,
                "message": msg,
            }
        )

    return out

def _problems(s: dict[str, Any], allowed_scopes: set[str] | None = None) -> list[dict[str, Any]]:
    probs: list[dict[str, Any]] = []
    core = ("imdb", "tmdb", "tvdb")

    cfg = _cfg()
    pairs = _pair_map(cfg, s)
    idx_cache = _indices_for(s)
    pair_libs = _pair_lib_filters(cfg)
    pair_types = _pair_type_filters(cfg)
    cw_state = _read_cw_state(allowed_scopes)
    manual = _load_manual_state()
    manual_blocks = _manual_add_blocks(manual)
    unresolved_index: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}

    for name, body in (cw_state or {}).items():
        if not isinstance(body, dict):
            continue
        if not name.endswith(".json"):
            continue

        stem = name[:-5]
        if allowed_scopes:
            for safe in sorted(allowed_scopes, key=len, reverse=True):
                suf = f".{safe}"
                if stem.endswith(suf):
                    stem = stem[: -len(suf)]
                    break

        kind: str | None = None
        for knd in ("unresolved", "shadow"):
            if stem.endswith(f".{knd}"):
                kind = knd
                stem = stem[: -len(knd) - 1]
                break
            if stem.endswith(f"_{knd}"):
                kind = knd
                stem = stem[: -len(knd) - 1]
                break
        if kind is None:
            continue

        if "_" not in stem:
            continue
        prov_raw, feat_raw = stem.split("_", 1)

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
            meta: dict[str, Any] = {"file": name, "kind": kind}
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

            filtered_targets: list[str] = []
            union_targets: list[dict[str, str]] = []
            for t in targets:
                if _passes_pair_lib_filter(pair_libs, prov, feat, t, v) and _passes_pair_type_filter(pair_types, prov, feat, t, v):
                    filtered_targets.append(t)
                    union_targets.append(idx_cache.get((t, feat)) or {})

            if not union_targets:
                continue

            merged_keys = set().union(*[set(d.keys()) for d in union_targets]) if union_targets else set()
            vv = dict(v)
            vv["_key"] = k
            alias_keys = _alias_keys(vv)

            if not any(ak in merged_keys for ak in alias_keys):
                blocks = manual_blocks.get((prov, feat))
                blocked = False
                if blocks:
                    for kk in [k, *alias_keys]:
                        if kk in blocks:
                            blocked = True
                            break
                ptype = "blocked_manual" if blocked else "missing_peer"
                sev = "info" if blocked else "warn"
                prob: dict[str, Any] = {
                    "severity": sev,
                    "type": ptype,
                    "provider": prov,
                    "feature": feat,
                    "key": k,
                    "title": v.get("title"),
                    "year": v.get("year"),
                    "targets": filtered_targets,
                    **({"manual_ref": str(MANUAL_STATE_PATH)} if blocked else {}),
                }
                hints: list[dict[str, Any]] = []
                if blocked:
                    hints.append({"kind": "blocked_manual", "message": f"Blocked by manual list ({MANUAL_STATE_PATH}).", "source": str(MANUAL_STATE_PATH)})
                for dst in filtered_targets:
                    idx_key = (str(dst).upper(), feat.lower())
                    uidx = unresolved_index.get(idx_key) or {}
                    for ak in alias_keys:
                        for meta in uidx.get(ak, []):
                            h: dict[str, Any] = {"provider": dst, "feature": feat}
                            if "reasons" in meta:
                                h["reasons"] = meta["reasons"]
                            if "file" in meta:
                                h["source"] = meta["file"]
                            if "kind" in meta:
                                h["kind"] = meta["kind"]
                            hints.append(h)
                if hints:
                    prob["hints"] = hints
                details = _missing_peer_show_hints(s, feat, v, filtered_targets)
                if blocked:
                    details = ([{"target": "ALL", "feature": feat, "message": f"Blocked by manual list ({MANUAL_STATE_PATH})."}] + (details or []))
                if details:
                    prob["target_show_info"] = details
                probs.append(prob)

    for p, f, k, it in _iter_items(s):
        ids = it.get("ids") or {}
        for ns in core:
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
        missing = [ns for ns in core if not ids.get(ns)]
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
        if ids and not any(ids.get(ns) for ns in core):
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

    try:
        probs.extend(_history_normalization_issues(s))
    except Exception:
        pass

    return probs

def _peer_ids(s: dict[str, Any], cur: dict[str, Any]) -> dict[str, str]:
    t = (cur.get("title") or "").strip().lower()
    y = cur.get("year")
    ty = (cur.get("type") or "").lower()
    out: dict[str, str] = {}
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

def _norm(ns: str, v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if ns == "imdb":
        m = re.search(r"(\d+)", s)
        return f"tt{m.group(1)}" if m else None
    if ns in ("tmdb", "tvdb", "trakt", "plex", "simkl", "mal", "anilist"):
        m = re.search(r"(\d+)", s)
        return m.group(1) if m else None
    return s or None

def _rekey(b: dict[str, Any], old_key: str, it: dict[str, Any]) -> str:
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

def _tmdb(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    k = _tmdb_key()
    if not k:
        raise HTTPException(400, "tmdb.api_key missing in config.json (tmdb or tmdb_sync)")

    query: dict[str, Any] = {}
    if params:
        query.update(params)
    query["api_key"] = k
    r = requests.get(
        f"https://api.themoviedb.org/3{path}",
        params=query,
        timeout=8,
    )
    r.raise_for_status()
    return r.json()

def _trakt(path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    h = _trakt_headers()
    if not h.get("trakt-api-key"):
        raise HTTPException(400, "trakt.client_id missing in config.json")
    r = requests.get(
        f"https://api.trakt.tv{path}",
        params=params,
        headers=h,
        timeout=8,
    )
    r.raise_for_status()
    return r.json()

def _tmdb_bulk(ids: list[int]) -> dict[int, dict[str, Any]]:
    if not ids:
        return {}
    key = _tmdb_key()
    if not key:
        return {}
    out: dict[int, dict[str, Any]] = {}
    for chunk_start in range(0, len(ids), 20):
        chunk = ids[chunk_start : chunk_start + 20]
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

def _tmdb_region_dates(meta: dict[int, dict[str, Any]]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for mid, data in (meta or {}).items():
        rels = (data.get("release_dates") or {}).get("results") or []
        best: dict[str, Any] | None = None
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

def _ratings_audit(s: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    tmdb_ids: list[int] = []
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

def _apply_fix(s: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
    t = body.get("type")

    prov_raw = body.get("provider")
    feat_raw = body.get("feature")
    key_raw = body.get("key")

    if not isinstance(prov_raw, str) or not isinstance(feat_raw, str) or not isinstance(key_raw, str):
        raise HTTPException(400, "provider/feature/key must be strings")

    prov = prov_raw
    feat = feat_raw
    key = key_raw

    b, it = _find_item(s, prov, feat, key)
    if b is None or it is None:
        raise HTTPException(404, "Item not found")

    ids = it.setdefault("ids", {})
    ch: list[str] = []

    if t in ("key_missing_ids", "key_ids_mismatch"):
        ns_raw = body.get("id_name")
        exp = body.get("expected")

        if not isinstance(ns_raw, str) or not isinstance(exp, str):
            raise HTTPException(400, "Missing id_name/expected")

        ns = ns_raw
        ids[ns] = exp
        ch.append(f"ids.{ns}={exp}")
        new = _rekey(b, key, it)

    elif t == "invalid_id_format":
        ns_raw = body.get("id_name")
        val = body.get("id_value")

        if not isinstance(ns_raw, str):
            raise HTTPException(400, "Missing id_name")

        ns = ns_raw
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

    cfg = _cfg()
    pairs = _pair_map(cfg, s)
    idx = _indices_for(s)
    pair_libs = _pair_lib_filters(cfg)
    pair_types = _pair_type_filters(cfg)
    it["_ignore_missing_peer"] = not _has_peer_by_pairs(
        s,
        pairs,
        prov,
        feat,
        new,
        it,
        idx,
        pair_libs,
        pair_types,
    )
    return {"ok": True, "changes": ch or ["ids merged from peers"], "new_key": new}

def _suggest(s: dict[str, Any], prov: str, feat: str, key: str) -> dict[str, Any]:
    _, it = _find_item(s, prov, feat, key)
    if it is None:
        raise HTTPException(404, "Item not found")
    return {"suggestions": [], "needs": []}

@router.get("/analyzer/state", response_class=JSONResponse)
def api_state(pairs: str | None = None) -> dict[str, Any]:
    try:
        handles = _load_state_handles(pairs)
        s = _merge_states(handles)
    except HTTPException as e:
        if e.status_code == 404:
            s = {}
        else:
            raise
    return {"counts": _counts(s), "items": _collect_items(s)}


@router.get("/analyzer/problems", response_class=JSONResponse)
def api_problems(pairs: str | None = None) -> dict[str, Any]:
    handles = _load_state_handles(pairs)
    s = _merge_states(handles)
    scopes = {h.get("safe") for h in handles if h.get("safe")}
    allowed = set(x for x in scopes if isinstance(x, str) and x) or None
    return {
        "problems": _problems(s, allowed),
        "pair_stats": _pair_stats(s),
        "pair_exclusions": _pair_exclusions(s),
    }


@router.get("/analyzer/ratings-audit", response_class=JSONResponse)
def api_ratings_audit(pairs: str | None = None) -> dict[str, Any]:
    s = _load_state(pairs)
    return _ratings_audit(s)

@router.get("/analyzer/cw-state", response_class=JSONResponse)
def api_cw_state(pairs: str | None = None) -> dict[str, Any]:
    try:
        handles = _load_state_handles(pairs)
        scopes = {h.get("safe") for h in handles if h.get("safe")}
        allowed = set(x for x in scopes if isinstance(x, str) and x) or None
    except HTTPException:
        allowed = None
    return _read_cw_state(allowed)

@router.post("/analyzer/patch", response_class=JSONResponse)
def api_patch(payload: dict[str, Any], pairs: str | None = None) -> dict[str, Any]:
    for f in ("provider", "feature", "key", "ids"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")

    handles = _load_state_handles(pairs)
    new_key = str(payload["key"])
    touched = 0

    for h in handles:
        s = h["state"]
        b, it = _find_item(s, payload["provider"], payload["feature"], payload["key"])
        if b is None or it is None:
            continue

        ids = dict(it.get("ids") or {})
        for k_any, v in (payload.get("ids") or {}).items():
            k = str(k_any)
            nv = _norm(k, v)
            if nv is None:
                ids.pop(k, None)
            else:
                ids[k] = nv

        it["ids"] = ids

        if payload.get("merge_peer_ids"):
            peer_ids = _peer_ids(s, it)
            for k, v in peer_ids.items():
                if k not in ids and v:
                    ids[k] = v
            it["ids"] = ids

        old_key = str(payload["key"])
        if payload.get("rekey"):
            new_key = _rekey(b, old_key, it)

        cfg = _cfg()
        pairs_map = _pair_map(cfg, s)
        idx = _indices_for(s)
        pair_libs = _pair_lib_filters(cfg)
        pair_types = _pair_type_filters(cfg)
        it["_ignore_missing_peer"] = not _has_peer_by_pairs(
            s,
            pairs_map,
            payload["provider"],
            payload["feature"],
            new_key,
            it,
            idx,
            pair_libs,
            pair_types,
        )

        _save_state_at(h["path"], s)
        touched += 1

    if touched == 0:
        raise HTTPException(404, "Item not found")
    return {"ok": True, "new_key": new_key}

@router.post("/analyzer/suggest", response_class=JSONResponse)
def api_suggest(payload: dict[str, Any], pairs: str | None = None) -> dict[str, Any]:
    for f in ("provider", "feature", "key"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")
    s = _load_state(pairs)
    return _suggest(s, payload["provider"], payload["feature"], payload["key"])


@router.post("/analyzer/fix", response_class=JSONResponse)
def api_fix(payload: dict[str, Any], pairs: str | None = None) -> dict[str, Any]:
    for f in ("type", "provider", "feature", "key"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")

    handles = _load_state_handles(pairs)
    touched = 0
    out: dict[str, Any] | None = None

    for h in handles:
        s = h["state"]
        try:
            r = _apply_fix(s, payload)
        except HTTPException:
            continue
        _save_state_at(h["path"], s)
        touched += 1
        if out is None:
            out = r

    if touched == 0:
        raise HTTPException(404, "Item not found")
    return out or {"ok": True}

@router.patch("/analyzer/item", response_class=JSONResponse)
def api_edit(payload: dict[str, Any], pairs: str | None = None) -> dict[str, Any]:
    for f in ("provider", "feature", "key", "updates"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")

    handles = _load_state_handles(pairs)
    new_key = str(payload["key"])
    touched = 0

    for h in handles:
        s = h["state"]
        b = _bucket(s, payload["provider"], payload["feature"])
        if not b or payload["key"] not in b:
            continue

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

        new_key = _rekey(b, payload["key"], it)
        cfg = _cfg()
        pairs_map = _pair_map(cfg, s)
        idx = _indices_for(s)
        pair_libs = _pair_lib_filters(cfg)
        pair_types = _pair_type_filters(cfg)
        it["_ignore_missing_peer"] = not _has_peer_by_pairs(
            s,
            pairs_map,
            payload["provider"],
            payload["feature"],
            new_key,
            it,
            idx,
            pair_libs,
            pair_types,
        )
        _save_state_at(h["path"], s)
        touched += 1

    if touched == 0:
        raise HTTPException(404, "Item not found")
    return {"ok": True, "new_key": new_key}

@router.delete("/analyzer/item", response_class=JSONResponse)
def api_delete(payload: dict[str, Any], pairs: str | None = None) -> dict[str, Any]:
    for f in ("provider", "feature", "key"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")

    handles = _load_state_handles(pairs)
    touched = 0

    for h in handles:
        s = h["state"]
        b = _bucket(s, payload["provider"], payload["feature"])
        if not b or payload["key"] not in b:
            continue
        b.pop(payload["key"], None)
        _save_state_at(h["path"], s)
        touched += 1

    if touched == 0:
        raise HTTPException(404, "Item not found")
    return {"ok": True}
