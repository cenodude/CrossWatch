# services/analyzer.py
# CrossWatch - Data analyzer for state
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable
import json
import re
import threading

import requests
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from cw_platform.config_base import CONFIG as CONFIG_DIR, load_config

router = APIRouter(prefix="/api", tags=["analyzer"])
STATE_PATH = CONFIG_DIR / "state.json"
MANUAL_STATE_PATH = CONFIG_DIR / "state.manual.json"
CWS_DIR = CONFIG_DIR / ".cw_state"
_LOCK = threading.Lock()


def _cfg() -> dict[str, Any]:
    try:
        cfg = load_config()
    except Exception:
        return {}
    return cfg or {}


def _tmdb_key() -> str:
    return ((_cfg().get("tmdb") or {}).get("api_key") or "").strip()


def _trakt_headers() -> dict[str, str]:
    t = _cfg().get("trakt") or {}
    h: dict[str, str] = {
        "trakt-api-version": "2",
        "trakt-api-key": (t.get("client_id") or "").strip(),
    }
    tok = (t.get("access_token") or "").strip()
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        raise HTTPException(404, "state.json not found")
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        raise HTTPException(500, "Failed to parse state.json")



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

def _save_state(s: dict[str, Any]) -> None:
    with _LOCK:
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(s, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE_PATH)


def _iter_items(s: dict[str, Any]) -> Iterable[tuple[str, str, str, dict[str, Any]]]:
    for prov, pv in (s.get("providers") or {}).items():
        for feat in ("history", "watchlist", "ratings"):
            items = (((pv or {}).get(feat) or {}).get("baseline") or {}).get("items") or {}
            for k, it in items.items():
                yield prov, feat, k, (it or {})


def _bucket(s: dict[str, Any], prov: str, feat: str) -> dict[str, Any] | None:
    try:
        return s["providers"][prov][feat]["baseline"]["items"]  # type: ignore[index]
    except KeyError:
        return None


def _find_item(
    s: dict[str, Any],
    prov: str,
    feat: str,
    key: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    b = _bucket(s, prov, feat)
    if b is None or key not in b:
        return None, None
    return b, b[key]


def _counts(s: dict[str, Any]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
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


def _collect_items(s: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for prov, feat, k, it in _iter_items(s):
        out.append(
            {
                "provider": prov,
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


def _read_cw_state() -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not (CWS_DIR.exists() and CWS_DIR.is_dir()):
        return out
    for p in sorted(CWS_DIR.glob("*.json")):
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
        src = str(pr.get("src") or pr.get("source") or "").upper().strip()
        dst = str(pr.get("dst") or pr.get("target") or "").upper().strip()
        if not (src and dst):
            continue
        if pr.get("enabled") is False:
            continue

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
            add(src, f, dst)
            if mode in ("two-way", "bi", "both", "mirror", "two", "two_way", "two way"):
                add(dst, f, src)
    return mp


def _supports_pair_libs(prov: str) -> bool:
    return str(prov or "").upper() in ("PLEX", "EMBY", "JELLYFIN")


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
                    allowed = {str(x).strip() for x in raw if str(x).strip()}
                    if allowed:
                        out[(a, feat, b)] = allowed

            add_dir(src, dst)
            if mode in ("two-way", "bi", "both", "mirror", "two", "two_way", "two way"):
                add_dir(dst, src)

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
) -> bool:
    if feat not in ("history", "watchlist", "ratings"):
        return True

    prov_key = str(prov or "").upper()
    feat_key = str(feat or "").lower()
    targets = pairs.get((prov_key, feat_key), [])
    if not targets:
        return True

    filtered_targets: list[str] = []
    for dst in targets:
        if _passes_pair_lib_filter(pair_libs, prov, feat, dst, item):
            filtered_targets.append(dst)
    if not filtered_targets:
        return True

    vv = dict(item)
    vv["_key"] = item_key
    keys = set(_alias_keys(vv))
    for dst in filtered_targets:
        idx = idx_cache.get((dst, feat)) or {}
        if any(k in idx for k in keys):
            return True
    return False

def _pair_stats(s: dict[str, Any]) -> list[dict[str, Any]]:
    stats: list[dict[str, Any]] = []
    cfg = _cfg()
    pairs = _pair_map(cfg, s)
    idx_cache = _indices_for(s)
    pair_libs = _pair_lib_filters(cfg)

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
        title = (
            rec.get("series_title")
            or rec.get("show_title")
            or rec.get("title")
            or rec.get("name")
        )
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
            if ":" not in s0:
                continue
            ns, v = s0.split(":", 1)
            by_ns.setdefault(ns, set()).add(v)
        for vals in by_ns.values():
            if len(vals) > 1:
                return None
        order = {"imdb": 0, "tmdb": 1, "tvdb": 2, "slug": 3}
        best: str | None = None
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

    prov_block = (s.get("providers") or {}) if isinstance(s, dict) else {}

    title_ids: dict[str, set[str]] = {}
    title_year_ids: dict[tuple[str, int | None], set[str]] = {}

    for prov_data in prov_block.values():
        if not isinstance(prov_data, dict):
            continue
        hist = (prov_data or {}).get("history") or {}
        node = hist.get("baseline") or hist
        items = node.get("items") or {}
        recs = items.values() if isinstance(items, dict) else (items if isinstance(items, list) else [])
        for rec in recs:
            if not isinstance(rec, dict):
                continue
            tk = title_key(rec)
            if not tk:
                continue
            sig = show_id_sig(rec)
            if not sig:
                continue
            t, y = tk
            title_ids.setdefault(t, set()).add(sig)
            title_year_ids.setdefault((t, y), set()).add(sig)

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
        prov = str(prov_name or "").upper().strip()
        if not prov or not isinstance(prov_data, dict):
            continue

        hist = (prov_data or {}).get("history") or {}
        node = hist.get("baseline") or hist
        items = node.get("items") or {}

        if isinstance(items, dict):
            recs = items.values()
        elif isinstance(items, list):
            recs = items
        else:
            continue

        p_shows: set[str] = set()

        for rec in recs:
            if not isinstance(rec, dict):
                continue

            def ensure_label(sig: str) -> None:
                if sig in labels:
                    return
                title = (
                    rec.get("series_title")
                    or rec.get("show_title")
                    or rec.get("title")
                    or rec.get("name")
                )
                year = rec.get("series_year") or rec.get("year")
                if title:
                    base = str(title).strip()
                    if year not in (None, ""):
                        lbl = f"{base} ({year}) [{sig}]"
                    else:
                        lbl = f"{base} [{sig}]"
                else:
                    lbl = sig
                labels[sig] = lbl

            typ = str(rec.get("type") or "").strip().lower()
            if typ not in ("episode", "show") and not (
                rec.get("show_ids") or rec.get("series_title") or rec.get("show_title")
            ):
                continue

            tk = title_key(rec)
            show_sig = show_id_sig(rec)

            mapped: str | None = None
            if tk:
                mapped = title_year_best.get(tk)
                if mapped is None:
                    mapped = title_best.get(tk[0])

            if mapped and sig_prio(mapped) < sig_prio(show_sig):
                show_sig = mapped

            if show_sig is None and tk:
                show_sig = f"{tk[0]}|year:{tk[1]}"

            if show_sig:
                p_shows.add(show_sig)
                ensure_label(show_sig)

        show_sets[prov] = p_shows

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
        a = str(src or "").upper().strip()
        if not a:
            continue

        for dst in targets:
            b = str(dst or "").upper().strip()
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

def _problems(s: dict[str, Any]) -> list[dict[str, Any]]:
    probs: list[dict[str, Any]] = []
    core = ("imdb", "tmdb", "tvdb")

    cfg = _cfg()
    pairs = _pair_map(cfg, s)
    idx_cache = _indices_for(s)
    pair_libs = _pair_lib_filters(cfg)
    cw_state = _read_cw_state()
    manual = _load_manual_state()
    manual_blocks = _manual_add_blocks(manual)
    unresolved_index: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}

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
            meta: dict[str, Any] = {"file": name, "kind": suffix}
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
                if _passes_pair_lib_filter(pair_libs, prov, feat, t, v):
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
    if ns in ("tmdb", "tvdb", "trakt", "plex", "simkl"):
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
        raise HTTPException(400, "tmdb.api_key missing in config.json")

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
    it["_ignore_missing_peer"] = not _has_peer_by_pairs(
        s,
        pairs,
        prov,
        feat,
        new,
        it,
        idx,
        pair_libs,
    )
    return {"ok": True, "changes": ch or ["ids merged from peers"], "new_key": new}

def _suggest(s: dict[str, Any], prov: str, feat: str, key: str) -> dict[str, Any]:
    _, it = _find_item(s, prov, feat, key)
    if it is None:
        raise HTTPException(404, "Item not found")
    return {"suggestions": [], "needs": []}

@router.get("/analyzer/state", response_class=JSONResponse)
def api_state() -> dict[str, Any]:
    try:
        s = _load_state()
    except HTTPException as e:
        if e.status_code == 404:
            s = {}
        else:
            raise
    return {"counts": _counts(s), "items": _collect_items(s)}


@router.get("/analyzer/problems", response_class=JSONResponse)
def api_problems() -> dict[str, Any]:
    s = _load_state()
    return {"problems": _problems(s), "pair_stats": _pair_stats(s)}


@router.get("/analyzer/ratings-audit", response_class=JSONResponse)
def api_ratings_audit() -> dict[str, Any]:
    s = _load_state()
    return _ratings_audit(s)

@router.get("/analyzer/cw-state", response_class=JSONResponse)
def api_cw_state() -> dict[str, Any]:
    return _read_cw_state()

@router.post("/analyzer/patch", response_class=JSONResponse)
def api_patch(payload: dict[str, Any]) -> dict[str, Any]:
    for f in ("provider", "feature", "key", "ids"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")
    s = _load_state()
    b, it = _find_item(s, payload["provider"], payload["feature"], payload["key"])
    if b is None or it is None:
        raise HTTPException(404, "Item not found")

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

    old_key = payload["key"]
    new_key = old_key
    if payload.get("rekey"):
        new_key = _rekey(b, old_key, it)

    cfg = _cfg()
    pairs = _pair_map(cfg, s)
    idx = _indices_for(s)
    pair_libs = _pair_lib_filters(cfg)
    it["_ignore_missing_peer"] = not _has_peer_by_pairs(
        s,
        pairs,
        payload["provider"],
        payload["feature"],
        new_key,
        it,
        idx,
        pair_libs,
    )
    _save_state(s)
    return {"ok": True, "new_key": new_key}

@router.post("/analyzer/suggest", response_class=JSONResponse)
def api_suggest(payload: dict[str, Any]) -> dict[str, Any]:
    for f in ("provider", "feature", "key"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")
    s = _load_state()
    return _suggest(s, payload["provider"], payload["feature"], payload["key"])


@router.post("/analyzer/fix", response_class=JSONResponse)
def api_fix(payload: dict[str, Any]) -> dict[str, Any]:
    for f in ("type", "provider", "feature", "key"):
        if f not in payload:
            raise HTTPException(400, f"Missing {f}")
    s = _load_state()
    r = _apply_fix(s, payload)
    _save_state(s)
    return r


@router.patch("/analyzer/item", response_class=JSONResponse)
def api_edit(payload: dict[str, Any]) -> dict[str, Any]:
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
    cfg = _cfg()
    pairs = _pair_map(cfg, s)
    idx = _indices_for(s)
    pair_libs = _pair_lib_filters(cfg)
    it["_ignore_missing_peer"] = not _has_peer_by_pairs(
        s,
        pairs,
        payload["provider"],
        payload["feature"],
        new,
        it,
        idx,
        pair_libs,
    )
    _save_state(s)
    return {"ok": True, "new_key": new}


@router.delete("/analyzer/item", response_class=JSONResponse)
def api_delete(payload: dict[str, Any]) -> dict[str, Any]:
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
