# /cw_platform/anime_mapping/service.py
# CrossWatch - Anime Mapping Service
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cw_platform.id_map import canonical_key, ids_from, merge_ids, minimal

from .descriptors import descriptor_candidates_for_id, parse_descriptor
from .storage import paths, query_edges, read_state

ANIME_NATIVE_PROVIDERS = {"anilist"}
DEFAULT_FEATURES = {"watchlist", "ratings"}
OUTPUT_KEYS = ("anilist", "mal", "anidb", "tmdb", "tvdb", "imdb")
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
        allowed = {_norm_provider(x) for x in use_for if _norm_provider(x)}
    else:
        allowed = set(ANIME_NATIVE_PROVIDERS)
    names = {_norm_provider(p) for p in providers if _norm_provider(p)}
    return bool(names & allowed)


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
        opts["use_anime_mapping"] = bool(base_enabled)
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


class AnimeMappingService:
    def __init__(self, cfg: Mapping[str, Any] | None = None):
        self.cfg = cfg if isinstance(cfg, Mapping) else {}
        block = self.cfg.get("anime_mapping") if isinstance(self.cfg, Mapping) else {}
        block = block if isinstance(block, Mapping) else {}
        self.release_tag = str(block.get("release_tag") or "v3").strip() or "v3"

    def ready(self) -> bool:
        pp = paths(self.release_tag)
        return bool(pp["db"].exists() and read_state(self.release_tag).get("index_ready", False))

    def enrich_ids(self, ids: Mapping[str, Any] | None, *, media_type: str | None = None) -> dict[str, Any]:
        ids0 = {str(k).lower(): v for k, v in dict(ids or {}).items() if v not in (None, "")}
        if not ids0 or not self.ready():
            return {"ids": dict(ids0), "detail": {}, "changed": False}

        seen_sources: set[tuple[str, str]] = set()
        rows: list[dict[str, Any]] = []
        source_descriptors: list[str] = []
        queue: list[tuple[int, str]] = []

        for key, value in ids0.items():
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

        if not rows:
            return {"ids": dict(ids0), "detail": {}, "changed": False}

        out_ids: dict[str, Any] = dict(ids0)
        details: dict[str, list[dict[str, Any]]] = {}

        def add_detail(row: Mapping[str, Any]) -> None:
            tp = str(row.get("target_provider") or "").strip()
            tid = str(row.get("target_id") or "").strip()
            if not tp or not tid:
                return
            kind = str(row.get("target_kind") or "").strip()
            scope = str(row.get("target_scope") or "").strip()
            key = f"{tp}_{kind}" if kind else tp
            ent = {
                "id": tid,
                "scope": scope,
                "source_range": str(row.get("source_range") or ""),
                "target_range": str(row.get("target_range") or ""),
            }
            bucket = details.setdefault(key, [])
            marker = (ent["id"], ent["scope"], ent["source_range"], ent["target_range"])
            for old in bucket:
                if (
                    old.get("id"),
                    old.get("scope"),
                    old.get("source_range"),
                    old.get("target_range"),
                ) == marker:
                    return
            bucket.append(ent)

        for row in rows:
            add_detail(row)
            tp = str(row.get("target_provider") or "").strip().lower()
            tid = str(row.get("target_id") or "").strip()
            if tp not in OUTPUT_KEYS or not tid:
                continue
            if out_ids.get(tp) in (None, ""):
                out_ids[tp] = tid

        merged = merge_ids(ids0, out_ids)
        changed = merged != {k: str(v) for k, v in ids0.items() if v not in (None, "")}
        detail = {
            "anime_mapping": {
                "provider": "anibridge",
                "release_tag": self.release_tag,
                "source_descriptors": sorted(set(source_descriptors)),
                **details,
            }
        } if details else {}
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


def enrich_index_for_pair(
    index: Mapping[str, Any],
    cfg: Mapping[str, Any],
    *providers: Any,
) -> dict[str, Any]:
    if not index or not mapping_enabled_for_pair(cfg, *providers):
        return dict(index or {})
    svc = AnimeMappingService(cfg)
    if not svc.ready():
        return dict(index or {})
    out: dict[str, Any] = {}
    for key, value in (index or {}).items():
        if not isinstance(value, Mapping):
            out[str(key)] = value
            continue
        try:
            enriched = svc.enrich_item(value)
            mini = minimal(enriched)
            new_key = canonical_key(mini) or str(key)
            existing = out.get(new_key)
            if isinstance(existing, Mapping):
                merged = dict(existing)
                merged["ids"] = merge_ids(existing.get("ids") if isinstance(existing.get("ids"), Mapping) else {}, mini.get("ids") if isinstance(mini.get("ids"), Mapping) else {})
                out[new_key] = merged
            else:
                out[new_key] = mini
        except Exception:
            out[str(key)] = value
    return out
