# cw_platform/orchestrator/_snapshots.py
# snapshot management for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable

import json
import os
from pathlib import Path

from ._scope import scoped_file
import time
import datetime as _dt

from ..id_map import canonical_key, KEY_PRIORITY
from ._types import InventoryOps
from ..modules_registry import load_sync_ops

SnapIndex = dict[str, dict[str, Any]]
SnapCache = dict[tuple[str, str], tuple[float, SnapIndex]]

def _key_rank(k: str) -> int:
    s = str(k or "").strip().lower()
    if not s or ":" not in s:
        return 999
    prefix = s.split(":", 1)[0].strip().lower()
    try:
        return KEY_PRIORITY.index(prefix)
    except ValueError:
        return 999

def _pick_key(provider_key: str, computed_key: str) -> str:
    pk = str(provider_key or "").strip().lower()
    ck = str(computed_key or "").strip().lower()
    if not ck:
        return pk
    if not pk:
        return ck
    return pk if _key_rank(pk) < _key_rank(ck) else ck

_ID_COALESCE_KEYS = ("imdb", "tmdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb")

def _coalesce_by_shared_ids(idx: SnapIndex, *, feature: str) -> SnapIndex:
    if str(feature or "").lower() != "watchlist" or not idx:
        return dict(idx)

    parent: dict[str, str] = {}
    rank: dict[str, int] = {}
    seen: dict[str, str] = {}

    def _find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def _union(a: str, b: str) -> None:
        ra = _find(a)
        rb = _find(b)
        if ra == rb:
            return
        if rank.get(ra, 0) < rank.get(rb, 0):
            ra, rb = rb, ra
        parent[rb] = ra
        if rank.get(ra, 0) == rank.get(rb, 0):
            rank[ra] = rank.get(ra, 0) + 1

    def _tok(idk: str, idv: Any) -> str | None:
        if idv is None:
            return None
        s = str(idv).strip().lower()
        if not s:
            return None
        return f"{str(idk).strip().lower()}:{s}"

    for ck in idx.keys():
        parent[ck] = ck
        rank[ck] = 0

    for ck, it in idx.items():
        if not isinstance(it, Mapping):
            continue
        ids = it.get("ids")
        if not isinstance(ids, Mapping):
            continue
        for idk in _ID_COALESCE_KEYS:
            if idk not in ids:
                continue
            t = _tok(idk, ids.get(idk))
            if not t:
                continue
            other = seen.get(t)
            if other and other != ck:
                _union(ck, other)
            else:
                seen[t] = ck

    groups: dict[str, list[str]] = {}
    for ck in idx.keys():
        root = _find(ck)
        groups.setdefault(root, []).append(ck)

    def _ids_count(v: Mapping[str, Any]) -> int:
        ids = v.get("ids")
        return len(ids) if isinstance(ids, Mapping) else 0

    def _best_key(keys: list[str]) -> str:
        best = keys[0]
        best_r = _key_rank(best)
        best_ids = _ids_count(idx.get(best, {}) if isinstance(idx.get(best), Mapping) else {})
        for k in keys[1:]:
            r = _key_rank(k)
            if r < best_r:
                best, best_r = k, r
                best_ids = _ids_count(idx.get(k, {}) if isinstance(idx.get(k), Mapping) else {})
                continue
            if r == best_r:
                ids_n = _ids_count(idx.get(k, {}) if isinstance(idx.get(k), Mapping) else {})
                if ids_n > best_ids:
                    best, best_ids = k, ids_n
        return best

    def _merge_dict(dst: dict[str, Any], src: Mapping[str, Any]) -> None:
        for k, v in src.items():
            if k not in dst or dst.get(k) in (None, "", 0, False):
                dst[k] = v
                continue
            if isinstance(dst.get(k), Mapping) and isinstance(v, Mapping):
                dd = dict(dst.get(k) or {})
                for kk, vv in v.items():
                    if kk not in dd or dd.get(kk) in (None, "", 0, False):
                        dd[kk] = vv
                dst[k] = dd

    def _merge_ids(dst_ids: dict[str, str], src_ids: Mapping[str, Any]) -> None:
        for k, v in src_ids.items():
            if v is None or str(v).strip() == "":
                continue
            sk = str(k).strip()
            sv = str(v).strip()
            if sk not in dst_ids or not str(dst_ids.get(sk) or "").strip():
                dst_ids[sk] = sv

    out: SnapIndex = {}
    for root, keys in groups.items():
        if len(keys) == 1:
            k = keys[0]
            v = idx.get(k)
            if isinstance(v, Mapping):
                out[k] = dict(v)
            continue

        chosen = _best_key(keys)
        chosen_item = idx.get(chosen)
        if not isinstance(chosen_item, Mapping):
            chosen = next((k for k in keys if isinstance(idx.get(k), Mapping)), chosen)
            chosen_item = idx.get(chosen) or {}

        base: dict[str, Any] = dict(chosen_item) if isinstance(chosen_item, Mapping) else {}
        base_ids: dict[str, str] = {}
        if isinstance(base.get("ids"), Mapping):
            base_ids = {str(k).strip(): str(v).strip() for k, v in base.get("ids", {}).items() if v is not None and str(v).strip()}
        base["ids"] = base_ids

        for k in keys:
            if k == chosen:
                continue
            other = idx.get(k)
            if not isinstance(other, Mapping):
                continue
            oids = other.get("ids")
            if isinstance(oids, Mapping):
                _merge_ids(base_ids, oids)
            _merge_dict(base, other)

        base["ids"] = base_ids
        out[str(chosen)] = base

    return out

def allowed_providers_for_feature(config: Mapping[str, Any], feature: str) -> set[str]:
    allowed: set[str] = set()
    feat = str(feature or "").strip().lower()

    try:
        pairs = list((config.get("pairs") or []) or [])
    except Exception:
        pairs = []

    def _pair_runs_feature(pair: Mapping[str, Any]) -> bool:
        selector = str(pair.get("feature") or "").strip().lower()
        if selector and selector != "multi":
            return selector == feat

        fmap = pair.get("features")
        if isinstance(fmap, Mapping) and fmap:
            if feat not in fmap:
                return False
            v = fmap.get(feat)
            if isinstance(v, bool):
                return bool(v)
            if isinstance(v, Mapping):
                return bool(v.get("enable", v.get("enabled", True)))
            return True

        return True

    for p in pairs:
        try:
            if not p.get("enabled", True):
                continue
            if not _pair_runs_feature(p):
                continue
            s = str(p.get("source") or p.get("src") or "").strip().upper()
            t = str(p.get("target") or p.get("dst") or "").strip().upper()
            if s:
                allowed.add(s)
            if t:
                allowed.add(t)
        except Exception:
            continue

    return allowed

def provider_configured(config: Mapping[str, Any], name: str) -> bool:
    nm = (name or "").upper()
    ops = load_sync_ops(nm)
    if ops and hasattr(ops, "is_configured"):
        try:
            return bool(ops.is_configured(config))
        except Exception:
            return False
    return False

def _coerce_checkpoint_value(v: Any) -> str | None:
    if v is None:
        return None
    return str(v)

def module_checkpoint(ops: InventoryOps, config: Mapping[str, Any], feature: str) -> str | None:
    acts_fn = getattr(ops, "activities", None)
    if not callable(acts_fn):
        return None

    try:
        raw = acts_fn(config)
    except Exception:
        return None

    acts: Mapping[str, Any]
    if isinstance(raw, Mapping):
        acts = raw
    else:
        return None

    try:
        if feature == "watchlist":
            return (
                _coerce_checkpoint_value(acts.get("watchlist"))
                or _coerce_checkpoint_value(acts.get("ptw"))
                or _coerce_checkpoint_value(acts.get("updated_at"))
            )
        if feature == "ratings":
            return (
                _coerce_checkpoint_value(acts.get("ratings"))
                or _coerce_checkpoint_value(acts.get("updated_at"))
            )
        if feature == "history":
            return (
                _coerce_checkpoint_value(acts.get("history"))
                or _coerce_checkpoint_value(acts.get("updated_at"))
            )
        return _coerce_checkpoint_value(acts.get("updated_at"))
    except Exception:
        return None

def prev_checkpoint(state: Mapping[str, Any], prov: str, feature: str) -> str | None:
    try:
        providers_block = state.get("providers")
        if not isinstance(providers_block, Mapping):
            return None
        prov_block = providers_block.get(prov)
        if not isinstance(prov_block, Mapping):
            return None
        feat_block = prov_block.get(feature)
        if not isinstance(feat_block, Mapping):
            return None
        cp = feat_block.get("checkpoint")
        return _coerce_checkpoint_value(cp)
    except Exception:
        return None

def _parse_ts(v: Any) -> int | None:
    if v in (None, "", 0):
        return None
    try:
        if isinstance(v, (int, float)):
            return int(v)
        return int(
            _dt.datetime.fromisoformat(
                str(v).replace("Z", "+00:00").replace(" ", "T")
            ).timestamp()
        )
    except Exception:
        return None

def _eventish_count(feature: str, idx: Mapping[str, Any]) -> int:
    if feature == "history":
        return sum(
            1
            for v in idx.values()
            if isinstance(v, Mapping) and (v.get("watched_at") or v.get("last_watched_at"))
        )
    if feature == "ratings":
        return sum(
            1
            for v in idx.values()
            if isinstance(v, Mapping)
            and (v.get("rated_at") or v.get("user_rated_at") or v.get("rating") or v.get("user_rating"))
        )
    return len(idx)

_STATE_DIR = Path("/config/.cw_state")

def _anilist_shadow_path() -> Path:
    return scoped_file(_STATE_DIR, "anilist_watchlist_shadow.json")

def _tokens_for_item(ck: str, it: Mapping[str, Any]) -> set[str]:
    toks: set[str] = set()
    if ck:
        toks.add(str(ck).lower())
    ids = it.get("ids") if isinstance(it, Mapping) else None
    if isinstance(ids, Mapping):
        try:
            for k, v in ids.items():
                if v is None or str(v).strip() == "":
                    continue
                toks.add(f"{str(k).lower()}:{str(v).lower()}")
        except Exception:
            pass
    return toks

def _load_json_dict(path: str | Path) -> dict[str, Any]:
    try:
        p = Path(path)
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
            return dict(raw) if isinstance(raw, dict) else {}
    except Exception:
        return {}

def _save_json_dict(path: str | Path, obj: Mapping[str, Any]) -> None:
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, sort_keys=True)
    except Exception:
        pass

def _maybe_backfill_anilist_shadow(
    snaps: dict[str, SnapIndex],
    *,
    feature: str,
    dbg: Callable[..., Any],
) -> None:
    if str(feature or "").lower() != "watchlist":
        return
    an_idx = snaps.get("ANILIST")
    if not isinstance(an_idx, dict) or not an_idx:
        return

    tok_best: dict[str, str] = {}
    key2item: dict[str, dict[str, Any]] = {}
    for pname, idx in snaps.items():
        if pname == "ANILIST" or not isinstance(idx, Mapping):
            continue
        for ck, it in (idx or {}).items():
            if not ck or not isinstance(it, Mapping):
                continue
            key2item.setdefault(str(ck), dict(it))
            for tok in _tokens_for_item(str(ck), it):
                cur = tok_best.get(tok)
                if not cur or _key_rank(str(ck)) < _key_rank(str(cur)):
                    tok_best[tok] = str(ck)

    if not tok_best:
        return

    shadow = _load_json_dict(_anilist_shadow_path())
    changed_shadow = False
    rekeyed = 0
    enriched = 0
    collisions = 0

    for ck, it in list(an_idx.items()):
        if not ck or not isinstance(it, Mapping):
            continue

        best_key = None
        best_rank = 999
        for tok in _tokens_for_item(str(ck), it):
            cand = tok_best.get(tok)
            if cand is None:
                continue
            r = _key_rank(str(cand))
            if r < best_rank:
                best_rank = r
                best_key = str(cand)

        if not best_key:
            continue

        if _key_rank(best_key) >= _key_rank(str(ck)):
            continue

        ids = it.get("ids")
        ids = dict(ids) if isinstance(ids, Mapping) else {}
        src = key2item.get(best_key) or {}
        src_ids = src.get("ids")
        if isinstance(src_ids, Mapping):
            for k, v in src_ids.items():
                if v is None or str(v).strip() == "":
                    continue
                if str(k) not in ids:
                    ids[str(k)] = str(v).strip()
                    enriched += 1
        if ids:
            try:
                it["ids"] = ids  # type: ignore[index]
            except Exception:
                pass

        aid = None
        try:
            aid = ids.get("anilist")
            if aid is not None:
                aid = int(str(aid).strip())
        except Exception:
            aid = None

        if aid:
            ent: dict[str, Any] = dict(shadow.get(best_key) or {})
            ent.pop("ignored", None)
            ent.pop("ignore_reason", None)
            ent["anilist_id"] = int(aid)

            try:
                mal = ids.get("mal")
                if mal is not None:
                    ent["mal"] = int(str(mal).strip())
            except Exception:
                pass

            aobj = it.get("anilist")
            if isinstance(aobj, Mapping):
                try:
                    le = aobj.get("list_entry_id")
                    if le is not None:
                        ent["list_entry_id"] = int(str(le).strip())
                except Exception:
                    pass

            ent["type"] = str(it.get("type") or ent.get("type") or "")
            ent["title"] = str(it.get("title") or ent.get("title") or "")
            try:
                ent["year"] = int(it.get("year") or ent.get("year") or 0)
            except Exception:
                pass

            if isinstance(src_ids, Mapping) and src_ids:
                ent["source_ids"] = {
                    str(k): str(v).strip()
                    for k, v in src_ids.items()
                    if k and v is not None and str(v).strip()
                }

            ent["updated_at"] = int(time.time())
            shadow[best_key] = ent
            changed_shadow = True

        if best_key in an_idx and best_key != ck:
            other = an_idx.get(best_key)
            if isinstance(other, Mapping):
                oids = other.get("ids")
                oids = dict(oids) if isinstance(oids, Mapping) else {}
                for k, v in (ids or {}).items():
                    if v is None or str(v).strip() == "":
                        continue
                    oids.setdefault(str(k), str(v).strip())
                try:
                    other["ids"] = oids  # type: ignore[index]
                except Exception:
                    pass
                try:
                    del an_idx[ck]
                except Exception:
                    pass
                collisions += 1
                continue

        if best_key != ck:
            try:
                an_idx[best_key] = dict(it)
                del an_idx[ck]
                rekeyed += 1
            except Exception:
                pass

    if changed_shadow:
        _save_json_dict(_anilist_shadow_path(), shadow)

    if rekeyed or enriched:
        dbg(
            "snapshot.anilist_key_backfill",
            feature=feature,
            rekeyed=int(rekeyed),
            ids_added=int(enriched),
            collisions=int(collisions),
        )

def build_snapshots_for_feature(
    *,
    feature: str,
    config: Mapping[str, Any],
    providers: Mapping[str, InventoryOps],
    snap_cache: SnapCache,
    snap_ttl_sec: int,
    dbg: Callable[..., Any],
    emit_info: Callable[[str], Any],
) -> dict[str, SnapIndex]:
    snaps: dict[str, SnapIndex] = {}
    now = time.time()
    allowed = allowed_providers_for_feature(config, feature)

    for name, ops in providers.items():
        try:
            feats_raw = ops.features()  # type: ignore[call-arg]
        except Exception:
            feats_raw = {}

        feats: Mapping[str, Any]
        if isinstance(feats_raw, Mapping):
            feats = feats_raw
        else:
            feats = {}

        if not bool(feats.get(feature, False)):
            continue

        if allowed and name.upper() not in allowed:
            continue

        if not provider_configured(config, name):
            continue

        memo_key = (name, feature)
        if snap_ttl_sec > 0:
            ent = snap_cache.get(memo_key)
            if ent is not None:
                ts, cached_idx = ent
                if (now - ts) < snap_ttl_sec:
                    snaps[name] = cached_idx
                    dbg("snapshot.memo", provider=name, feature=feature, count=_eventish_count(feature, cached_idx), raw_count=len(cached_idx))
                    continue

        degraded = False
        try:
            idx_raw = ops.build_index(config, feature=feature)  # type: ignore[call-arg]
        except Exception as e:
            emit_info(
                f"[!] snapshot.failed provider={name} feature={feature} error={e}"
            )
            dbg("provider.degraded", provider=name, feature=feature)
            degraded = True
            idx_raw = None

        canon: SnapIndex = {}

        if isinstance(idx_raw, list):
            for raw in idx_raw:
                if not isinstance(raw, Mapping):
                    continue
                item = dict(raw)
                key = canonical_key(item)
                if key:
                    canon[key] = item

        elif isinstance(idx_raw, Mapping):
            for k, raw in idx_raw.items():
                if not isinstance(raw, Mapping):
                    continue
                item = dict(raw)
                computed = canonical_key(item) or ""
                provider_key = k.split("@", 1)[0] if isinstance(k, str) and k else ""
                key = _pick_key(provider_key, computed)
                if key:
                    canon[key] = item

        else:
            canon = {}

        canon = _coalesce_by_shared_ids(canon, feature=feature)
        snaps[name] = canon

        if snap_ttl_sec > 0:
            if degraded or not canon:
                dbg(
                    "snapshot.no_cache_empty",
                    provider=name,
                    feature=feature,
                    degraded=bool(degraded),
                )
            else:
                snap_cache[memo_key] = (now, canon)

        dbg("snapshot", provider=name, feature=feature, count=_eventish_count(feature, canon), raw_count=len(canon))

    _maybe_backfill_anilist_shadow(snaps, feature=feature, dbg=dbg)
    return snaps

def coerce_suspect_snapshot(
    *,
    provider: str,
    ops: InventoryOps,
    prev_idx: Mapping[str, Any],
    cur_idx: Mapping[str, Any],
    feature: str,
    suspect_min_prev: int,
    suspect_shrink_ratio: float,
    suspect_debug: bool,
    emit: Callable[..., Any],
    emit_info: Callable[[str], Any],
    prev_cp: str | None,
    now_cp: str | None,
) -> tuple[dict[str, Any], bool, str]:
    try:
        caps_raw = ops.capabilities()  # type: ignore[call-arg]
    except Exception:
        caps_raw = {}

    if isinstance(caps_raw, Mapping):
        caps: Mapping[str, Any] = caps_raw
    else:
        caps = {}

    sem = caps.get("index_semantics", "present")

    if str(sem).lower() != "present":
        return dict(cur_idx), False, "semantics:delta"

    prev_count = len(prev_idx or {})
    cur_count = len(cur_idx or {})

    if prev_count < suspect_min_prev:
        return dict(cur_idx), False, "baseline:tiny"

    shrink_limit = max(1, int(prev_count * suspect_shrink_ratio))
    shrunk = (cur_count == 0) or (cur_count <= shrink_limit)
    if not shrunk:
        return dict(cur_idx), False, "ok"

    prev_ts = _parse_ts(prev_cp)
    now_ts = _parse_ts(now_cp)

    no_progress = (
        (prev_ts is not None and now_ts is not None and now_ts <= prev_ts)
        or (prev_ts is not None and now_ts is None)
        or (prev_cp and now_cp and str(now_cp) == str(prev_cp))
    )

    if no_progress:
        reason = "suspect:no-progress+shrunk"
        if suspect_debug:
            emit(
                "snapshot:suspect",
                provider=provider,
                feature=feature,
                prev_count=prev_count,
                cur_count=cur_count,
                shrink_limit=shrink_limit,
                prev_checkpoint=prev_cp,
                now_checkpoint=now_cp,
                reason=reason,
            )
        return dict(prev_idx), True, reason

    return dict(cur_idx), False, "progressed"
