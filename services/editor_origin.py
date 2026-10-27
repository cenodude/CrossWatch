# services/editor_origin.py
# CrossWatch - Editor item origin through IDs, saved fixes, anime mapping and custom mappings
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field

from cw_platform.access_policy import request_user, user_can_access_instance, user_can_access_pair
from cw_platform.anime_mapping.coordinates import parse_target
from cw_platform.anime_mapping.overrides import find_episode_overrides, find_identity_overrides, load_overrides
from cw_platform.anime_mapping.service import (
    PAIR_FEATURE_OPTIONS_KEY,
    AnimeMappingService,
    anime_mapping_pair_feature_options,
    config_with_pair_feature_options,
    mapping_enabled_for_feature,
    runtime_pair_feature_options,
)
from cw_platform.anime_mapping.storage import query_edges, query_native_identity
from cw_platform.id_map import ids_from, typed_keys_for_item
from cw_platform.mapping_policy import effective_policy
from cw_platform.orchestrator._pairs_utils import filter_manual_block, manual_policy, merge_manual_adds
from cw_platform.provider_instances import normalize_instance_id
from services.saved_mappings import mapping_details, saved_corrections

NATIVE = ("anilist", "mal", "anidb")
DIRECT = ("anilist", "mal", "anidb", "kitsu")
CATALOG = ("tmdb", "tvdb", "imdb")
IDENTITY = ("kitsu", "simkl")
EPISODIC = frozenset({"episode", "season"})
ANIME_TARGETS = {"ANILIST": ("anilist", "mal"), "MYANIMELIST": ("mal",), "KITSU": ("kitsu",)}
KEY_ORDER = ("tmdb", "imdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb")
GROUP_LIMIT = 50
LINK_LIMIT = 20
REACH_KEYS = frozenset({"anilist", "mal", "anidb", "tmdb", "tvdb", "imdb"})
REACH_HOPS = 3
REACH_LIMIT = 400


class OriginRequest(BaseModel):
    provider: str = Field(min_length=1, max_length=128)
    instance: str = Field(default="default", max_length=128)
    feature: Literal["history", "watchlist", "ratings", "progress", "collection"]
    key: str = Field(default="", max_length=1024)
    pair_id: str = Field(default="", max_length=256)
    original: dict[str, Any]


def _int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _type(item: Mapping[str, Any]) -> str:
    return str(item.get("type") or "").strip().lower()


def _kind(item: Mapping[str, Any]) -> str:
    return "movie" if _type(item) == "movie" else "show"


def _ids(item: Mapping[str, Any]) -> dict[str, str]:
    raw = item.get("show_ids") if _type(item) in EPISODIC else item.get("ids")
    if not isinstance(raw, Mapping):
        return {}
    return {str(k).strip().lower(): str(v).strip() for k, v in raw.items() if str(v or "").strip()}


def _title(item: Mapping[str, Any]) -> str:
    if _type(item) in EPISODIC:
        for key in ("series_title", "show_title", "grandparent_title", "title"):
            if str(item.get(key) or "").strip():
                return str(item[key]).strip()
    return str(item.get("title") or item.get("name") or "").strip()


def _season(scope: Any) -> int | None:
    text = str(scope or "").strip().lower()
    return _int(text[1:]) if text.startswith("s") else None


def _map_episode(source_range: Any, target_range: Any, episode: int) -> int | None:
    spans = parse_target(source_range)
    if not spans:
        return 0 if not str(source_range or "").strip() else None
    offset, passed = None, 0
    for lo, hi in spans:
        if episode >= lo and (hi is None or episode <= hi):
            offset = passed + episode - lo
            break
        if hi is None:
            return None
        passed += hi - lo + 1
    if offset is None:
        return None
    targets = parse_target(target_range)
    if not targets:
        return 0
    for lo, hi in targets:
        if hi is None or offset < hi - lo + 1:
            return lo + offset
        offset -= hi - lo + 1
    return None


def _hit(namespace: str, ident: str, episode: int | None, via: str, link: Mapping[str, Any] | None = None,
         rule_id: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {"namespace": namespace, "id": str(ident), "episode": episode or None, "via": via}
    if link:
        out["link"] = dict(link)
    if rule_id:
        out["rule_id"] = rule_id
    return out


class OriginMapper:
    def __init__(self, cfg: Mapping[str, Any] | None):
        self.cfg: Mapping[str, Any] = cfg if isinstance(cfg, Mapping) else {}
        self.svc = AnimeMappingService(cfg)
        self.ready = self.svc.ready()
        self.tag = self.svc.release_tag
        try:
            self.rules = [dict(r) for r in load_overrides() if r.get("enabled", True)]
        except Exception:
            self.rules = []
        self.rule_by_id = {str(r.get("id") or ""): r for r in self.rules}
        self._rule_index: dict[tuple[str, str], list[int]] = {}
        for position, rule in enumerate(self.rules):
            key = (str(rule.get("match_provider") or ""), str(rule.get("match_id") or ""))
            self._rule_index.setdefault(key, []).append(position)
        self._edges: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._targets: dict[tuple[Any, ...], tuple[str, str] | None] = {}
        self.truncated = False

    def edges(self, namespace: str, ident: str) -> list[dict[str, Any]]:
        key = (namespace, ident)
        if key not in self._edges:
            try:
                self._edges[key] = query_edges(self.tag, namespace, ident) if self.ready else []
            except Exception:
                self._edges[key] = []
        return self._edges[key]

    def identity_ids(self, namespace: str, ident: str) -> dict[str, str]:
        if not self.ready:
            return {}
        try:
            return dict(query_native_identity(self.tag, namespace, ident))
        except Exception:
            return {}

    def _rules_for(self, ids: Mapping[str, str]) -> list[dict[str, Any]]:
        positions = {pos for provider, ident in ids.items() for pos in self._rule_index.get((provider, ident), ())}
        return [self.rules[pos] for pos in sorted(positions)]

    def _rule_link(self, rule_id: str) -> dict[str, Any] | None:
        rule = self.rule_by_id.get(rule_id)
        if not rule:
            return None
        return {"namespace": rule.get("match_provider"), "id": rule.get("match_id"),
                "kind": rule.get("media_type"), "season": rule.get("match_season")}

    def _identity_rules(self, ids: Mapping[str, str], media_type: str,
                        rows: list[dict[str, Any]] | None = None) -> dict[str, tuple[str, str]]:
        rows = self._rules_for(ids) if rows is None else rows
        found = find_identity_overrides(ids, media_type=media_type, rows=rows)
        out: dict[str, tuple[str, str]] = {}
        for namespace, target in found.items():
            if namespace not in DIRECT:
                continue
            rule = next((r for r in rows if r.get("target_namespace") == namespace
                         and str(r.get("target_id") or "") == target
                         and (media_type not in ("show", "movie") or r.get("media_type") == media_type)), None)
            out[namespace] = (target, str((rule or {}).get("id") or ""))
        return out

    def _whole(self, item: Mapping[str, Any], ids: Mapping[str, str]) -> list[dict[str, Any]]:
        typ = _type(item)
        ruled = self._identity_rules(ids, typ)
        try:
            enriched = self.svc.enrich_ids(ids, media_type=typ)
        except Exception:
            enriched = {"ids": dict(ids), "detail": {}}
        out_ids = {str(k).lower(): str(v) for k, v in (enriched.get("ids") or {}).items() if str(v or "").strip()}
        detail = (enriched.get("detail") or {}).get("anime_mapping") or {}
        seeds = detail.get("identity_seeds") or {}
        seed_key = next((key for key in IDENTITY if ids.get(key)), "")
        hits: list[dict[str, Any]] = []
        for namespace in DIRECT:
            value = out_ids.get(namespace)
            if not value:
                continue
            if namespace in ruled and ruled[namespace][0] == value:
                rule_id = ruled[namespace][1]
                hits.append(_hit(namespace, value, None, "custom", self._rule_link(rule_id), rule_id))
            elif ids.get(namespace) == value:
                hits.append(_hit(namespace, value, None, "direct"))
            elif seed_key and seeds.get(namespace) == value:
                hits.append(_hit(namespace, value, None, "direct", {"namespace": seed_key, "id": ids[seed_key]}))
            else:
                link, mismatch = self._mapped_link(ids, _kind(item), namespace, value)
                hit = _hit(namespace, value, None, "mapping", link)
                if mismatch:
                    hit["kind_mismatch"] = True
                hits.append(hit)
        return hits

    def reach(self, start: tuple[str, str], hops: int = REACH_HOPS) -> dict[tuple[str, str], int]:
        seen = {start: 0}
        frontier = [start]
        for depth in range(1, hops + 1):
            nxt: list[tuple[str, str]] = []
            for namespace, ident in frontier:
                for edge in self.edges(namespace, ident):
                    node = (str(edge.get("target_provider") or "").strip().lower(), str(edge.get("target_id") or "").strip())
                    if node[0] not in REACH_KEYS or not node[1] or node in seen:
                        continue
                    if len(seen) >= REACH_LIMIT:
                        self.truncated = True
                        continue
                    seen[node] = depth
                    nxt.append(node)
            frontier = nxt
        return seen

    def _mapped_link(self, ids: Mapping[str, str], kind: str, namespace: str,
                     value: str) -> tuple[dict[str, Any] | None, bool]:
        starts = [(ns, ids[ns]) for ns in (*CATALOG, *NATIVE) if ids.get(ns)]
        mismatched: dict[str, Any] | None = None
        for ns, ident in starts:
            kinds = {str(e.get("source_kind") or "").strip().lower() for e in self.edges(ns, ident)
                     if str(e.get("target_provider") or "").strip().lower() == namespace
                     and str(e.get("target_id") or "").strip() == value}
            if not kinds:
                continue
            link = {"namespace": ns, "id": ident, "season": None, "kind": kind}
            if "" in kinds or kind in kinds or ns in NATIVE:
                return link, False
            if mismatched is None:
                mismatched = {**link, "kind": sorted(kinds)[0]}
        if mismatched is not None:
            return mismatched, True
        for ns, ident in starts:
            if (namespace, value) in self.reach((ns, ident)):
                return {"namespace": ns, "id": ident, "season": None, "kind": kind}, False
        return None, False

    def _mapped_episode(self, item: Mapping[str, Any], ids: Mapping[str, str]) -> list[dict[str, Any]]:
        typ = _type(item)
        season, episode = _int(item.get("season")), _int(item.get("episode"))
        found: dict[str, dict[str, dict[str, Any]]] = {}
        for namespace in CATALOG:
            ident = ids.get(namespace)
            if not ident:
                continue
            for edge in self.edges(namespace, ident):
                target = str(edge.get("target_provider") or "").strip().lower()
                target_id = str(edge.get("target_id") or "").strip()
                if target not in NATIVE or not target_id:
                    continue
                if target == "anidb" and str(edge.get("target_scope") or "").strip().upper() not in ("", "R"):
                    continue
                edge_kind = str(edge.get("source_kind") or "").strip().lower()
                if edge_kind and edge_kind != "show":
                    continue
                edge_season = _season(edge.get("source_scope"))
                if season is not None and edge_season is not None and edge_season != season:
                    continue
                mapped: int | None = None
                if typ == "episode":
                    if not episode:
                        continue
                    mapped = _map_episode(edge.get("source_range"), edge.get("target_range"), episode)
                    if not mapped:
                        continue
                link = {"namespace": namespace, "id": ident, "kind": "show", "season": edge_season}
                found.setdefault(target, {}).setdefault(target_id, _hit(target, target_id, mapped, "mapping", link))
        return [next(iter(hits.values())) for hits in found.values() if len(hits) == 1]

    def _episodic(self, item: Mapping[str, Any], ids: Mapping[str, str]) -> list[dict[str, Any]]:
        season, episode = _int(item.get("season")), _int(item.get("episode"))
        if _type(item) == "episode" and season is not None and episode:
            ruled = find_episode_overrides(ids, season, episode, rows=self._rules_for(ids))
            hits = [_hit(r.namespace, r.target_id, r.absolute, "custom", self._rule_link(r.rule_id), r.rule_id)
                    for r in ruled if r.namespace in DIRECT]
            if hits:
                return hits
        direct = [_hit(ns, ids[ns], None, "direct") for ns in DIRECT if ids.get(ns)]
        if direct:
            return direct
        mapped = self._mapped_episode(item, ids)
        if mapped:
            return mapped
        rows = [r for r in self._rules_for(ids) if r.get("episode_from") is None
                and (r.get("match_season") is None or season is None or _int(r.get("match_season")) == season)]
        return [_hit(ns, target, None, "custom", self._rule_link(rule_id), rule_id)
                for ns, (target, rule_id) in self._identity_rules(ids, "show", rows).items()]

    def writer_target(self, item: Mapping[str, Any], provider: str, cfg: Mapping[str, Any] | None = None,
                      scope: str = "", feature: str = "watchlist") -> tuple[str, str] | None:
        typ = _type(item)
        key = (provider, scope, feature, typ, tuple(sorted(_ids(item).items())),
               _int(item.get("season")), _int(item.get("episode")))
        if key not in self._targets:
            self._targets[key] = self._writer_target(item, provider, typ, cfg or self.cfg, feature)
        return self._targets[key]

    def mapping_on(self, cfg: Mapping[str, Any], feature: str) -> bool:
        if feature == "ratings":
            if PAIR_FEATURE_OPTIONS_KEY in cfg:
                return bool(runtime_pair_feature_options(cfg, "ratings").get("use_anime_mapping")) and self.ready
            return mapping_enabled_for_feature(cfg, "ratings") and self.ready
        if PAIR_FEATURE_OPTIONS_KEY in cfg and runtime_pair_feature_options(cfg, feature).get("use_anime_mapping") is False:
            return False
        block = cfg.get("anime_mapping")
        return bool(isinstance(block, Mapping) and block.get("enabled")) and self.ready

    def anilist_whole(self, item: Mapping[str, Any], cfg: Mapping[str, Any], feature: str) -> list[dict[str, Any]]:
        ids = _ids(item)
        if self.mapping_on(cfg, feature):
            return self._whole(item, ids)
        return [_hit(ns, ids[ns], None, "direct") for ns in ("anilist", "mal") if ids.get(ns)]

    def _writer_target(self, item: Mapping[str, Any], provider: str, typ: str, cfg: Mapping[str, Any],
                       feature: str) -> tuple[str, str] | None:
        try:
            if provider == "ANILIST":
                if typ == "episode":
                    from providers.sync.anilist._progress import resolve_target as anilist_target
                    found = anilist_target(cfg, item)
                    return ("anilist", str(found[0])) if found else None
                if typ in EPISODIC:
                    return None
                hits = self.anilist_whole(item, cfg, feature)
                for namespace in ("anilist", "mal"):
                    hit = next((h for h in hits if h["namespace"] == namespace), None)
                    if hit:
                        return (namespace, str(hit["id"]))
                return None
            if provider == "MYANIMELIST":
                from providers.sync.myanimelist._common import resolve_target as native_resolve
                namespace = "mal"
            elif provider == "KITSU":
                from providers.sync.kitsu._common import resolve_target as native_resolve
                namespace = "kitsu"
            else:
                return None
            probe = self.svc.enrich_item(item) if typ == "anime" else item
            found = native_resolve(cfg, probe)
            return (namespace, str(found[0])) if found else None
        except Exception:
            return None

    def converts(self, hit: Mapping[str, Any], target: tuple[str, str], provider: str = "") -> bool:
        source = (str(hit["namespace"]), str(hit["id"]))
        if source == target:
            return True
        namespace, ident = target
        episode = _int(hit.get("episode"))
        if episode and provider in ANIME_TARGETS:
            try:
                if provider == "ANILIST":
                    from providers.sync.anilist._progress import native_to_anilist as convert
                elif provider == "MYANIMELIST":
                    from providers.sync.myanimelist._common import native_target as convert
                else:
                    from providers.sync.kitsu._common import native_target as convert
                found = convert(self.tag, source[0], source[1], episode)
            except Exception:
                found = None
            return bool(found) and str(found[0]) == ident
        if namespace == "kitsu" and self.identity_ids(*source).get("kitsu") == ident:
            return True
        for edge in self.edges(*source):
            node = (str(edge.get("target_provider") or "").strip().lower(), str(edge.get("target_id") or "").strip())
            if node == target:
                return True
            if namespace == "kitsu" and node[0] in NATIVE and self.identity_ids(*node).get("kitsu") == ident:
                return True
        return False

    def resolve(self, item: Mapping[str, Any]) -> list[dict[str, Any]]:
        ids = _ids(item)
        if not ids:
            return []
        hits = self._episodic(item, ids) if _type(item) in EPISODIC else self._whole(item, ids)
        seen: set[tuple[str, str, int | None]] = set()
        out: list[dict[str, Any]] = []
        for hit in hits:
            marker = (hit["namespace"], hit["id"], hit["episode"])
            if marker not in seen:
                seen.add(marker)
                out.append(hit)
        return out

    def links(self, natives: set[tuple[str, str]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for namespace, ident in sorted(natives):
            for edge in self.edges(namespace, ident):
                target = str(edge.get("target_provider") or "").strip().lower()
                if target not in CATALOG:
                    continue
                link = {"via": "mapping", "namespace": target, "id": str(edge.get("target_id") or ""),
                        "kind": str(edge.get("target_kind") or "").strip().lower() or None,
                        "season": _season(edge.get("target_scope")),
                        "episodes": str(edge.get("target_range") or "") or None,
                        "anime": {"namespace": namespace, "id": ident,
                                  "episodes": str(edge.get("source_range") or "") or None}}
                marker = (link["namespace"], link["id"], link["season"], link["episodes"], namespace)
                if marker not in seen:
                    seen.add(marker)
                    out.append(link)
        for rule in self.rules:
            namespace, target = str(rule.get("target_namespace") or ""), str(rule.get("target_id") or "")
            if (namespace, target) not in natives:
                continue
            first, last = rule.get("episode_from"), rule.get("episode_to")
            episodes = None if first is None else f"{first}-{last if last is not None else ''}"
            out.append({"via": "custom", "namespace": rule.get("match_provider"), "id": rule.get("match_id"),
                        "kind": rule.get("media_type"), "season": rule.get("match_season"), "episodes": episodes,
                        "anime": {"namespace": namespace, "id": target}, "rule_id": rule.get("id"),
                        "title": rule.get("title") or None})
        return out


def _anime_list(hits: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for hit in hits:
        key = (str(hit["namespace"]), str(hit["id"]))
        entry = out.setdefault(key, {"namespace": key[0], "id": key[1], "via": hit["via"], "episodes": []})
        if hit.get("episode") and hit["episode"] not in entry["episodes"]:
            entry["episodes"].append(hit["episode"])
    return sorted(out.values(), key=lambda e: (DIRECT.index(e["namespace"]), e["id"]))


def _pick(hits: list[dict[str, Any]], natives: set[tuple[str, str]]) -> dict[str, Any] | None:
    matched = [h for h in hits if (h["namespace"], h["id"]) in natives]
    if not matched:
        return None
    return sorted(matched, key=lambda h: DIRECT.index(h["namespace"]))[0]


def _writer_pick(mapper: OriginMapper, item: Mapping[str, Any], provider: str, row_targets: set[tuple[str, str]],
                 shown: tuple[str, str], meta: Mapping[str, Any], feature: str) -> dict[str, Any] | None:
    cfg = meta.get("cfg") if isinstance(meta.get("cfg"), Mapping) else None
    target = mapper.writer_target(item, provider, cfg, str(meta.get("pair_id") or ""), feature)
    if target is None or target not in row_targets:
        return None
    if provider == "ANILIST" and _type(item) not in EPISODIC:
        hits = mapper.anilist_whole(item, cfg or mapper.cfg, feature)
    else:
        hits = mapper.resolve(item)
    basis = (next((h for h in hits if (h["namespace"], h["id"]) == target), None)
             or next((h for h in hits if h["via"] == "custom" and mapper.converts(h, target, provider)), None)
             or next((h for h in hits if mapper.converts(h, target, provider)), None)
             or (hits[0] if hits else None))
    hit = _hit(shown[0], shown[1], None, (basis or {}).get("via") or "mapping",
               (basis or {}).get("link"), (basis or {}).get("rule_id") or "")
    if (basis or {}).get("kind_mismatch"):
        hit["kind_mismatch"] = True
    return hit


def _pair_targets(mapper: OriginMapper, item: Mapping[str, Any], provider: str, meta: Mapping[str, Any],
                  feature: str, fallback: set[tuple[str, str]], cache: dict[str, set[tuple[str, str]]]) -> set[tuple[str, str]]:
    scope = str(meta.get("pair_id") or "")
    if scope in cache:
        return cache[scope]
    cfg = meta.get("cfg") if isinstance(meta.get("cfg"), Mapping) else None
    if provider not in ANIME_TARGETS or cfg is None:
        cache[scope] = fallback
        return fallback
    target = mapper.writer_target(item, provider, cfg, scope, feature)
    if target is None:
        cache[scope] = fallback
        return fallback
    out = {target}
    if provider == "ANILIST":
        hits = mapper.anilist_whole(item, cfg, feature) if _type(item) not in EPISODIC else mapper.resolve(item)
        out.update((h["namespace"], h["id"]) for h in hits if h["namespace"] == "mal")
        if _ids(item).get("mal"):
            out.add(("mal", _ids(item)["mal"]))
    cache[scope] = out
    return out


def _group_kind(item: Mapping[str, Any]) -> str:
    typ = _type(item)
    if typ in ("movie", "episode", "season"):
        return typ
    return "show"


def _plain_keys(item: Mapping[str, Any]) -> set[str]:
    keys = {str(k).lower() for k in typed_keys_for_item(item) if "|title:" not in str(k)}
    if _type(item) in EPISODIC:
        keys = {k for k in keys if "#s" in k}
    return keys


def _id_values(item: Mapping[str, Any]) -> set[str]:
    if _type(item) in EPISODIC:
        show_ids = item.get("show_ids")
        if not isinstance(show_ids, Mapping):
            return set()
        item = {"ids": show_ids}
    return {str(v).strip().lower() for v in ids_from(item).values() if str(v or "").strip()}


def _key_link(key: str, item: Mapping[str, Any]) -> dict[str, Any]:
    namespace, _, rest = key.partition(":")
    ident, _, suffix = rest.partition("#")
    season = _int(suffix[1:].split("e")[0]) if suffix.startswith("s") and suffix != "show" else None
    return {"namespace": namespace, "id": ident, "kind": _kind(item), "season": season}


def _plain_match(row_keys: set[str], row_kind: str, item: Mapping[str, Any]) -> dict[str, Any] | None:
    if _group_kind(item) != row_kind:
        return None
    shared = row_keys & _plain_keys(item)
    if not shared:
        return None
    key = sorted(shared, key=lambda k: (KEY_ORDER.index(k.split(":")[0]) if k.split(":")[0] in KEY_ORDER else 99, k))[0]
    return {"via": "ids", "link": _key_link(key, item)}


def _fix_info(rule: Mapping[str, Any]) -> dict[str, Any]:
    original = rule.get("original")
    return {"scope": rule.get("scope") or "shared", "key": rule.get("key"),
            "original_key": rule.get("original_key") or None,
            "original": mapping_details(original) if isinstance(original, Mapping) else None}


def trace(cfg: Mapping[str, Any] | None, item: Mapping[str, Any],
          sources: Iterable[tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]],
          *, provider: str = "", feature: str = "watchlist") -> dict[str, Any]:
    provider = str(provider or "").upper()
    anime_only = provider in ANIME_TARGETS
    mapper = OriginMapper(cfg)
    shown_hits = mapper.resolve(item)
    row_hits = shown_hits if anime_only else [h for h in shown_hits if h["via"] == "direct" and not h.get("link")]
    natives = {(h["namespace"], h["id"]) for h in row_hits}
    if anime_only:
        natives.update((ns, value) for ns, value in _ids(item).items() if ns in DIRECT)
    row_target = mapper.writer_target(item, provider, None, "", feature) if anime_only else None
    if anime_only and row_target is None:
        own = ANIME_TARGETS[provider]
        row_target = next(((h["namespace"], h["id"]) for ns in own for h in row_hits if h["namespace"] == ns), None)
    row_targets: set[tuple[str, str]] = set()
    if row_target is not None:
        natives.add(row_target)
        row_targets.add(row_target)
        if provider == "ANILIST":
            row_targets.update((h["namespace"], h["id"]) for h in shown_hits if h["namespace"] == "mal")
            if _ids(item).get("mal"):
                row_targets.add(("mal", _ids(item)["mal"]))
        natives.update(row_targets)
        if not any((h["namespace"], h["id"]) == row_target for h in shown_hits):
            shown_hits = [*shown_hits, _hit(row_target[0], row_target[1], None, "mapping")]
    links = mapper.links(natives) if natives else []
    wanted: dict[str, set[str]] = {}
    reached: set[tuple[str, str]] = set()
    for namespace, ident in natives:
        for node in mapper.reach((namespace, ident)):
            reached.add(node)
            wanted.setdefault(node[0], set()).add(node[1])
            if node[0] in NATIVE:
                for key, value in mapper.identity_ids(*node).items():
                    wanted.setdefault(key, set()).add(value)
    for rule in mapper.rules:
        if (str(rule.get("target_namespace") or ""), str(rule.get("target_id") or "")) in reached:
            wanted.setdefault(str(rule.get("match_provider") or ""), set()).add(str(rule.get("match_id") or ""))
    for link in links:
        wanted.setdefault(str(link["namespace"]), set()).add(str(link["id"]))
    row_keys = set() if anime_only else _plain_keys(item)
    row_kind = _group_kind(item)
    row_values = _id_values(item) if row_keys else set()

    groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    pair_cache: dict[str, set[tuple[str, str]]] = {}
    for meta, items, fixes in sources:
        for source_key, value in (items or {}).items():
            if not isinstance(value, Mapping):
                continue
            match: dict[str, Any] | None = None
            if row_values and row_values & _id_values(value):
                match = _plain_match(row_keys, row_kind, value)
            if match is None and natives:
                ids = _ids(value)
                if any(ids.get(ns) in values for ns, values in wanted.items()):
                    pair_targets = _pair_targets(mapper, item, provider, meta, feature, row_targets, pair_cache)
                    hit = (_writer_pick(mapper, value, provider, pair_targets, row_target, meta, feature)
                           if anime_only and row_target is not None else _pick(mapper.resolve(value), natives))
                    if hit is not None:
                        match = {"via": hit["via"], "link": hit.get("link") or {},
                                 "anime": {"namespace": hit["namespace"], "id": hit["id"]},
                                 "rule_id": hit.get("rule_id"), "kind_mismatch": bool(hit.get("kind_mismatch"))}
            if match is None:
                continue
            link = match.get("link") or {}
            fix = fixes.get(source_key) if isinstance(fixes, Mapping) else None
            season = _int(value.get("season")) if _type(value) in EPISODIC else None
            entry = match.get("anime")
            key = (meta.get("pair_id"), meta.get("provider"), meta.get("instance"), _title(value), match["via"],
                   link.get("namespace"), link.get("id"), season, (entry or {}).get("namespace"),
                   (entry or {}).get("id"), (fix or {}).get("original_key"), bool(fix))
            group = groups.get(key)
            if group is None:
                group = groups[key] = {
                    **{k: meta.get(k) for k in ("pair_id", "pair_label", "provider", "instance")},
                    "title": _title(value), "year": _int(value.get("year")) or None, "kind": _kind(value),
                    "via": match["via"], "link": link or None, "season": season, "anime": entry,
                    "rule_id": match.get("rule_id") or None, "kind_mismatch": bool(match.get("kind_mismatch")),
                    "fix": fix, "count": 0, "episodes": [],
                }
            group["count"] += 1
            episode = _int(value.get("episode"))
            if _type(value) == "episode" and episode is not None:
                group["episodes"].append(episode)

    rows = sorted(groups.values(), key=lambda g: (str(g.get("pair_label") or ""), str(g["title"]).casefold(),
                                                   g["season"] if g["season"] is not None else -1))
    for group in rows:
        eps = sorted(set(group.pop("episodes")))
        group["first_episode"], group["last_episode"] = (eps[0], eps[-1]) if eps else (None, None)
    block = (cfg or {}).get("anime_mapping") if isinstance(cfg, Mapping) else {}
    return {
        "ok": True,
        "title": _title(item),
        "kind": _kind(item),
        "mode": "anime" if anime_only else "ids",
        "mapping_enabled": bool(isinstance(block, Mapping) and block.get("enabled")),
        "mapping_ready": bool(mapper.ready),
        "anime": _anime_list(shown_hits),
        "sources": rows[:GROUP_LIMIT],
        "sources_total": len(rows),
        "links": links[:LINK_LIMIT],
        "links_total": len(links),
        "search_truncated": bool(mapper.truncated),
    }


def _feature_on(pair: Mapping[str, Any], feature: str) -> bool:
    value = (pair.get("features") or {}).get(feature) if isinstance(pair.get("features"), Mapping) else None
    return value is True or (isinstance(value, Mapping) and bool(value.get("enable") or value.get("enabled")))


def feeding_pairs(cfg: Mapping[str, Any], user: Any, provider: str, instance: str,
                  feature: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for pair in cfg.get("pairs") or []:
        if not isinstance(pair, Mapping) or not _feature_on(pair, feature):
            continue
        if not user_can_access_pair(cfg, user, pair):
            continue
        src = (str(pair.get("source") or "").upper(), normalize_instance_id(pair.get("source_instance")))
        dst = (str(pair.get("target") or "").upper(), normalize_instance_id(pair.get("target_instance")))
        two_way = "two" in str(pair.get("mode") or "").lower()
        if dst == (provider, instance):
            other = src
        elif two_way and src == (provider, instance):
            other = dst
        else:
            continue
        if not other[0] or other == (provider, instance):
            continue
        label = str(pair.get("name") or pair.get("label") or f"{src[0]} → {dst[0]}")
        raw_features = pair.get("features")
        features: Mapping[str, Any] = raw_features if isinstance(raw_features, Mapping) else {}
        raw_feature_cfg = features.get(feature)
        feature_cfg: Mapping[str, Any] = raw_feature_cfg if isinstance(raw_feature_cfg, Mapping) else {}
        options = anime_mapping_pair_feature_options(cfg, feature_cfg, feature, other[0], provider,
                                                     anime_only_default=provider == "ANILIST")
        out.append({"pair_id": str(pair.get("id") or ""), "pair_label": label,
                    "provider": other[0], "instance": other[1],
                    "cfg": config_with_pair_feature_options(cfg, options)})
    return out


def fix_index(policy: Mapping[str, Any]) -> dict[tuple[str, str, str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for rule in saved_corrections(policy):
        key = (str(rule["pair_id"] or ""), str(rule["provider"]).upper(), normalize_instance_id(rule["instance"]),
               str(rule["feature"]).lower(), str(rule["key"]).lower())
        out[key] = dict(rule)
    return out


def find_fix(fixes: Mapping[tuple[str, str, str, str, str], Mapping[str, Any]], pair_id: str, provider: str,
             instance: str, feature: str, item_key: str) -> dict[str, Any] | None:
    for scope in ([pair_id, ""] if pair_id else [""]):
        rule = fixes.get((scope, provider.upper(), instance, feature.lower(), str(item_key).lower()))
        if rule:
            return _fix_info(rule)
    return None


def corrected_source(items: Mapping[str, Any], policy: Mapping[str, Any], fixes: Mapping[Any, Any], pair_id: str,
                     provider: str, instance: str, feature: str) -> tuple[dict[str, Any], dict[str, Any]]:
    adds, blocks = manual_policy(effective_policy(policy, pair_id), provider, feature, instance)
    merged = merge_manual_adds(dict(items or {}), adds) if adds else dict(items or {})
    out: dict[str, Any] = {}
    tags: dict[str, Any] = {}
    for key, value in merged.items():
        if not isinstance(value, Mapping):
            continue
        if blocks and not filter_manual_block([dict(value)], blocks):
            continue
        out[key] = value
        if key in adds:
            fix = find_fix(fixes, pair_id, provider, instance, feature, key)
            tags[key] = fix or {"scope": "shared", "key": key, "original_key": None, "original": None}
    return out, tags


def item_origin(payload: OriginRequest, request: Any) -> dict[str, Any]:
    from api.editorAPI import (_load_current_state_features, _load_policy, _load_state_items,
                               _require_instance_scope, load_config)
    from services.editor_mapping import require_mapping_pair

    cfg = load_config() or {}
    user = request_user(request)
    provider, instance = payload.provider.strip().upper(), normalize_instance_id(payload.instance)
    _require_instance_scope(cfg, request, provider, instance)
    if not isinstance(payload.original, Mapping):
        raise HTTPException(400, "Missing item")
    pair_id = payload.pair_id.strip()
    if pair_id:
        require_mapping_pair(cfg, request, pair_id, provider, instance, payload.feature)
    pairs = [p for p in feeding_pairs(cfg, user, provider, instance, payload.feature)
             if user_can_access_instance(cfg, user, p["provider"], p["instance"])]
    policy = _load_policy()
    fixes = fix_index(policy)
    raw = _load_current_state_features({payload.feature}) if pairs else {}
    sources = []
    for p in pairs:
        items = _load_state_items(payload.feature, p["provider"], p["instance"], raw_state=raw)
        corrected, tags = corrected_source(items, policy, fixes, p["pair_id"], p["provider"], p["instance"],
                                           payload.feature)
        sources.append((p, corrected, tags))
    result = trace(cfg, payload.original, sources, provider=provider, feature=payload.feature)
    key = payload.key.strip()
    result["row_fix"] = find_fix(fixes, pair_id, provider, instance, payload.feature, key) if key else None
    result["pairs"] = [{k: p[k] for k in ("pair_id", "pair_label", "provider", "instance")} for p in pairs]
    return result
