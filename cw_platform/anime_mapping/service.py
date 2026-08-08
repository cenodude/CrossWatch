# /cw_platform/anime_mapping/service.py
# CrossWatch - Anime Mapping Service
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cw_platform.id_map import canonical_key, ids_from, merge_ids, minimal

from .descriptors import descriptor_candidates_for_id, parse_descriptor
from .overrides import find_identity_overrides
from .storage import index_ready, query_edges, query_identity_natives

ANIME_NATIVE_PROVIDERS = {"anilist", "simkl"}
DEFAULT_FEATURES = {"watchlist", "ratings"}
OPT_IN_FEATURES = {"history"}
ANY_PAIR = "*"
OUTPUT_KEYS = ("anilist", "mal", "anidb", "tmdb", "tvdb", "imdb")
SEED_KEYS = frozenset(OUTPUT_KEYS)
IDENTITY_SEED_KEYS = ("simkl", "kitsu")
IDENTITY_SEED_EXCLUDED_TYPES = frozenset({"episode", "season"})
PAIR_FEATURE_OPTIONS_KEY = "_cw_pair_feature_options"
_MEDIA_KIND_KEYS: dict[str, str] = {
    "tmdb_movie": "movie",
    "tvdb_movie": "movie",
    "imdb_movie": "movie",
    "tmdb_show": "show",
    "tvdb_show": "show",
    "imdb_show": "show",
}


def _norm_provider(value: Any) -> str:
    out = str(value or "").strip().lower()
    for sep in ("#", ".", ":"):
        if sep in out:
            out = out.split(sep, 1)[0].strip()
    return out


def mapping_enabled_for_pair(cfg: Mapping[str, Any], *providers: Any) -> bool:
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
    block = block if isinstance(block, Mapping) else {}
    if not bool(block.get("enabled", False)):
        return False
    use_for = block.get("use_for_pairs")
    if isinstance(use_for, (list, tuple, set)):
        allowed = {str(x or "").strip().lower() for x in use_for if str(x or "").strip()}
    else:
        allowed = set(ANIME_NATIVE_PROVIDERS)
    if ANY_PAIR in allowed:
        return True
    names = {_norm_provider(p) for p in providers if _norm_provider(p)}
    return bool(names & {_norm_provider(x) for x in allowed if _norm_provider(x)})


def mapping_enabled_for_feature(cfg: Mapping[str, Any], feature: Any) -> bool:
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
    block = block if isinstance(block, Mapping) else {}
    if not bool(block.get("enabled", False)):
        return False
    raw = block.get("features")
    if isinstance(raw, str):
        enabled = {x.strip().lower() for x in raw.replace(",", " ").split() if x.strip()}
    elif isinstance(raw, (list, tuple, set)):
        enabled = {str(x or "").strip().lower() for x in raw if str(x or "").strip()}
    else:
        enabled = set(DEFAULT_FEATURES)
    return str(feature or "").strip().lower() in enabled


def _pair_feature_options(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    out = dict(raw or {}) if isinstance(raw, Mapping) else {}
    use_map = bool(out.get("use_anime_mapping", False))
    out["use_anime_mapping"] = use_map
    out["anime_only_sync"] = bool(out.get("anime_only_sync", False)) if use_map else False
    return out


def anime_mapping_pair_feature_options(
    cfg: Mapping[str, Any],
    feature_cfg: Mapping[str, Any] | None,
    feature: Any,
    *providers: Any,
    anime_only_default: bool = False,
) -> dict[str, Any]:
    feature_name = str(feature or "").strip().lower()
    pair_enabled = mapping_enabled_for_pair(cfg, *providers)
    base_enabled = mapping_enabled_for_feature(cfg, feature_name) and pair_enabled
    opts = _pair_feature_options(feature_cfg)

    if "use_anime_mapping" not in dict(feature_cfg or {}):
        opts["use_anime_mapping"] = bool(base_enabled) and feature_name not in OPT_IN_FEATURES
    else:
        opts["use_anime_mapping"] = bool(pair_enabled and opts.get("use_anime_mapping"))

    if not opts["use_anime_mapping"]:
        opts["anime_only_sync"] = False
    elif "anime_only_sync" not in dict(feature_cfg or {}):
        opts["anime_only_sync"] = bool(anime_only_default)

    opts["feature"] = feature_name
    return opts


def config_with_pair_feature_options(
    cfg: Mapping[str, Any],
    options: Mapping[str, Any] | None,
) -> dict[str, Any]:
    out = dict(cfg or {})
    out[PAIR_FEATURE_OPTIONS_KEY] = _pair_feature_options(options)
    return out


def runtime_pair_feature_options(cfg: Mapping[str, Any], feature: Any = "watchlist") -> dict[str, Any]:
    raw = cfg.get(PAIR_FEATURE_OPTIONS_KEY) if isinstance(cfg, Mapping) else {}
    opts = _pair_feature_options(raw if isinstance(raw, Mapping) else {})
    if opts.get("feature") and str(opts.get("feature") or "").strip().lower() != str(feature or "").strip().lower():
        return {"use_anime_mapping": False, "anime_only_sync": False, "feature": str(feature or "").strip().lower()}
    opts["feature"] = str(feature or "").strip().lower()
    return opts


def mapped_media_type(item: Mapping[str, Any]) -> str | None:
    detail = item.get("detail") if isinstance(item.get("detail"), Mapping) else {}
    amap = detail.get("anime_mapping") if isinstance(detail, Mapping) else {}
    if not isinstance(amap, Mapping):
        return None

    has_movie = False
    has_show = False
    for key, value in amap.items():
        kind = _MEDIA_KIND_KEYS.get(str(key or "").strip().lower())
        if not kind:
            continue
        present = False
        if isinstance(value, list):
            present = bool(value)
        elif isinstance(value, Mapping):
            present = bool(value)
        elif value not in (None, "", False):
            present = True
        if not present:
            continue
        if kind == "movie":
            has_movie = True
        elif kind == "show":
            has_show = True

    if has_movie and not has_show:
        return "movie"
    if has_show:
        return "show"
    return None


def mapped_or_default_media_type(item: Mapping[str, Any]) -> str:
    current = str(item.get("type") or item.get("entity") or "").strip().lower()
    if current in {"movie", "movies", "film"}:
        return "movie"
    if current in {"show", "shows", "series", "tv", "tv_shows", "tvshows"}:
        return "show"
    if current == "anime":
        return mapped_media_type(item) or "show"
    return current or "movie"


_SIDE_ENTRY_SCOPES = {"s0", "s", "o"}


def _is_side_entry(row: Mapping[str, Any]) -> bool:
    scope = str(row.get("source_scope") or "").strip().lower()
    return scope in _SIDE_ENTRY_SCOPES


def _unambiguous_target(found: list[tuple[int, int, str]]) -> str:
    if not found:
        return ""
    best_depth = min(depth for depth, _side, _tid in found)
    tier = [(side, tid) for depth, side, tid in found if depth == best_depth]
    main = {tid for side, tid in tier if not side}
    chosen = main or {tid for _side, tid in tier}
    return next(iter(chosen)) if len(chosen) == 1 else ""


class AnimeMappingService:
    def __init__(self, cfg: Mapping[str, Any] | None = None):
        self.cfg = cfg if isinstance(cfg, Mapping) else {}
        block = self.cfg.get("anime_mapping") if isinstance(self.cfg, Mapping) else {}
        block = block if isinstance(block, Mapping) else {}
        self.release_tag = str(block.get("release_tag") or "v3").strip() or "v3"
        self._ready: bool | None = None

    def ready(self) -> bool:
        if self._ready is None:
            self._ready = index_ready(self.release_tag)
        return self._ready

    def _identity_seed_ids(self, ids: Mapping[str, Any], media_type: str | None = None) -> dict[str, str]:
        if str(media_type or "").strip().lower() in IDENTITY_SEED_EXCLUDED_TYPES:
            return {}
        if any(key in SEED_KEYS for key in ids):
            return {}
        for key in IDENTITY_SEED_KEYS:
            value = str(ids.get(key) or "").strip()
            if not value:
                continue
            try:
                found = query_identity_natives(self.release_tag, key, value)
            except Exception:
                continue
            natives = {k: str(v) for k, v in (found or {}).items() if str(v or "").strip()}
            if natives:
                return natives
        return {}

    def enrich_ids(self, ids: Mapping[str, Any] | None, *, media_type: str | None = None) -> dict[str, Any]:
        ids0 = {str(k).lower(): v for k, v in dict(ids or {}).items() if v not in (None, "")}
        if not ids0:
            return {"ids": dict(ids0), "detail": {}, "changed": False}

        try:
            ruled = find_identity_overrides({k: str(v) for k, v in ids0.items()}, media_type=media_type)
        except Exception:
            ruled = {}

        if not self.ready():
            if not ruled:
                return {"ids": dict(ids0), "detail": {}, "changed": False}
            merged = merge_ids(ruled, ids0)
            return {
                "ids": merged,
                "detail": {"anime_mapping": {"provider": "user_override", "overrides": dict(ruled)}},
                "changed": merged != {k: str(v) for k, v in ids0.items() if v not in (None, "")},
            }

        identity_seeds = self._identity_seed_ids(ids0, media_type)
        seed_ids: dict[str, Any] = {**identity_seeds, **ids0, **ruled}

        seen_sources: set[tuple[str, str]] = set()
        rows: list[dict[str, Any]] = []
        source_descriptors: list[str] = []
        queue: list[tuple[int, str]] = []

        for key, value in seed_ids.items():
            for raw_desc in descriptor_candidates_for_id(key, value, media_type=media_type):
                queue.append((0, raw_desc))

        max_depth = 2
        max_queries = 40
        while queue and len(seen_sources) < max_queries:
            depth, raw_desc = queue.pop(0)
            desc = parse_descriptor(raw_desc)
            if desc is None:
                continue
            source_descriptors.append(raw_desc)
            skey = (desc.provider, desc.id)
            if skey in seen_sources:
                continue
            seen_sources.add(skey)
            try:
                next_rows = query_edges(self.release_tag, desc.provider, desc.id)
            except Exception:
                continue
            for row in next_rows:
                row["_depth"] = depth
            rows.extend(next_rows)
            if depth >= max_depth:
                continue
            for row in next_rows:
                tp = str(row.get("target_provider") or "").strip().lower()
                tid = str(row.get("target_id") or "").strip()
                if tp not in OUTPUT_KEYS or not tid:
                    continue
                kind = str(row.get("target_kind") or "").strip().lower()
                scope = str(row.get("target_scope") or "").strip()
                prefix = f"{tp}_{kind}" if kind else tp
                next_desc = f"{prefix}:{tid}:{scope}" if scope else f"{prefix}:{tid}"
                queue.append((depth + 1, next_desc))

        if not rows and not ruled and not identity_seeds:
            return {"ids": dict(ids0), "detail": {}, "changed": False}

        out_ids: dict[str, Any] = dict(seed_ids)
        details: dict[str, list[dict[str, Any]]] = {}
        seen_details: set[tuple[str, str, str, str, str]] = set()

        def add_detail(row: Mapping[str, Any]) -> None:
            tp = str(row.get("target_provider") or "").strip()
            tid = str(row.get("target_id") or "").strip()
            if not tp or not tid:
                return
            kind = str(row.get("target_kind") or "").strip()
            scope = str(row.get("target_scope") or "").strip()
            key = f"{tp}_{kind}" if kind else tp
            source_range = str(row.get("source_range") or "")
            target_range = str(row.get("target_range") or "")
            marker = (key, tid, scope, source_range, target_range)
            if marker in seen_details:
                return
            seen_details.add(marker)
            details.setdefault(key, []).append(
                {"id": tid, "scope": scope, "source_range": source_range, "target_range": target_range}
            )

        candidates: dict[str, list[tuple[int, int, str]]] = {}
        for row in rows:
            add_detail(row)
            tp = str(row.get("target_provider") or "").strip().lower()
            tid = str(row.get("target_id") or "").strip()
            if tp not in OUTPUT_KEYS or not tid:
                continue
            if out_ids.get(tp) not in (None, ""):
                continue
            candidates.setdefault(tp, []).append(
                (int(row.get("_depth") or 0), 1 if _is_side_entry(row) else 0, tid)
            )

        ambiguous: list[str] = []
        for tp, found in candidates.items():
            picked = _unambiguous_target(found)
            if picked:
                out_ids[tp] = picked
            else:
                ambiguous.append(tp)

        merged = merge_ids(ruled, merge_ids(ids0, out_ids)) if ruled else merge_ids(ids0, out_ids)
        changed = merged != {k: str(v) for k, v in ids0.items() if v not in (None, "")}
        detail = {
            "anime_mapping": {
                "provider": "anibridge",
                "release_tag": self.release_tag,
                "source_descriptors": sorted(set(source_descriptors)),
                **({"overrides": dict(ruled)} if ruled else {}),
                **({"identity_seeds": dict(identity_seeds)} if identity_seeds else {}),
                **({"ambiguous": sorted(ambiguous)} if ambiguous else {}),
                **details,
            }
        } if (details or ruled or identity_seeds) else {}
        return {"ids": merged or dict(ids0), "detail": detail, "changed": bool(changed)}

    def enrich_item(self, item: Mapping[str, Any]) -> dict[str, Any]:
        out = dict(item or {})
        media_type = str(out.get("type") or out.get("entity") or "").strip().lower()
        ids = ids_from(out)
        res = self.enrich_ids(ids, media_type=media_type)
        if not res.get("changed") and not res.get("detail"):
            return out
        if res.get("ids"):
            out["ids"] = res["ids"]
        detail = out.get("detail")
        if not isinstance(detail, dict):
            detail = {}
        for k, v in (res.get("detail") or {}).items():
            detail[k] = v
        if detail:
            out["detail"] = detail
        mapped_type = mapped_or_default_media_type(out)
        current_type = str(out.get("type") or "").strip().lower()
        if current_type == "anime" and mapped_type in {"movie", "show"}:
            out["_cw_original_type"] = out.get("type")
            out["type"] = mapped_type
        return out


def enrich_item(item: Mapping[str, Any], cfg: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return AnimeMappingService(cfg).enrich_item(item)


def new_enrich_stats() -> dict[str, int]:
    return {"items": 0, "seeded": 0, "enriched": 0, "dead_end": 0, "rekeyed": 0, "merged": 0, "failed": 0}


def _seed_status(item: Mapping[str, Any], enriched: Mapping[str, Any]) -> tuple[bool, bool]:
    detail = enriched.get("detail") if isinstance(enriched.get("detail"), Mapping) else {}
    amap = detail.get("anime_mapping") if isinstance(detail, Mapping) else {}
    amap = amap if isinstance(amap, Mapping) else {}
    seeded = bool(amap.get("identity_seeds"))
    if seeded:
        return True, False
    media_type = str(item.get("type") or item.get("entity") or "").strip().lower()
    if media_type in IDENTITY_SEED_EXCLUDED_TYPES:
        return False, False
    return False, not any(key in SEED_KEYS for key in ids_from(item))


def enrich_index_for_pair(
    index: Mapping[str, Any],
    cfg: Mapping[str, Any],
    *providers: Any,
    stats: dict[str, int] | None = None,
) -> dict[str, Any]:
    if not index or not mapping_enabled_for_pair(cfg, *providers):
        return dict(index or {})
    svc = AnimeMappingService(cfg)
    if not svc.ready():
        return dict(index or {})
    counts = stats if isinstance(stats, dict) else None
    if counts is not None:
        counts.update(new_enrich_stats())
    out: dict[str, Any] = {}
    for key, value in (index or {}).items():
        if not isinstance(value, Mapping):
            out[str(key)] = value
            continue
        if counts is not None:
            counts["items"] += 1
        try:
            enriched = svc.enrich_item(value)
            mini = minimal(enriched)
            new_key = canonical_key(mini) or str(key)
            if counts is not None:
                seeded, dead_end = _seed_status(value, enriched)
                if seeded:
                    counts["seeded"] += 1
                if dead_end:
                    counts["dead_end"] += 1
                if ids_from(enriched) != ids_from(value):
                    counts["enriched"] += 1
                if new_key != str(key):
                    counts["rekeyed"] += 1
            existing = out.get(new_key)
            if isinstance(existing, Mapping):
                if counts is not None:
                    counts["merged"] += 1
                merged = dict(existing)
                merged["ids"] = merge_ids(existing.get("ids") if isinstance(existing.get("ids"), Mapping) else {}, mini.get("ids") if isinstance(mini.get("ids"), Mapping) else {})
                out[new_key] = merged
            else:
                out[new_key] = mini
        except Exception:
            if counts is not None:
                counts["failed"] += 1
            out[str(key)] = value
    return out
