# /cw_platform/anime_mapping/simkl_catalog.py
# CrossWatch - SIMKL catalog lookups for the custom anime mapping editor
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from cw_platform.config_base import load_config
from cw_platform.provider_instances import get_provider_block, list_instance_ids, normalize_instance_id

from .overrides import MATCH_PROVIDERS

BASE = "https://api.simkl.com"
SEARCH_PATH = "/search/anime"
DETAIL_PATH = "/anime"
SEARCH_PARAM = "q"
SEASON_TYPES = ("tv",)
CHAIN_RELATION = "sequel"
MAX_RESULTS = 25
MAX_CHAIN = 40
TIMEOUT = 20.0


class SimklCatalogError(RuntimeError):
    pass


class SimklNotConfigured(SimklCatalogError):
    pass


@dataclass(frozen=True)
class Entry:
    simkl: str
    title: str
    label: str
    year: int | None
    kind: str
    tmdb: str
    total_episodes: int | None
    poster: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "simkl": self.simkl,
            "title": self.title,
            "label": self.label,
            "year": self.year,
            "kind": self.kind,
            "tmdb": self.tmdb,
            "total_episodes": self.total_episodes,
            "poster": self.poster,
        }


@dataclass
class Chain:
    entries: list[Entry] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)


def _text(value: Any, limit: int = 300) -> str:
    return str(value or "").strip()[:limit]


def _int_or_none(value: Any) -> int | None:
    try:
        out = int(str(value).strip())
    except Exception:
        return None
    return out


def _key_of(cfg: Mapping[str, Any], instance_id: Any) -> str:
    block = get_provider_block(cfg, "simkl", instance_id) or {}
    return _text(block.get("api_key") or block.get("client_id"), 200)


def resolve_instance(instance_id: Any = None) -> tuple[str, str]:
    cfg = load_config() or {}
    wanted = normalize_instance_id(instance_id)
    key = _key_of(cfg, wanted)
    if key:
        return wanted, key
    for candidate in list_instance_ids(cfg, "simkl"):
        if normalize_instance_id(candidate) == wanted:
            continue
        key = _key_of(cfg, candidate)
        if key:
            return normalize_instance_id(candidate), key
    return wanted, ""


def instances() -> list[str]:
    cfg = load_config() or {}
    return [inst for inst in list_instance_ids(cfg, "simkl") if _key_of(cfg, inst)]


def client_id(instance_id: Any = None) -> str:
    return resolve_instance(instance_id)[1]


def configured(instance_id: Any = None) -> bool:
    return bool(client_id(instance_id))


def _app_version() -> str:
    try:
        from providers.sync.simkl._common import simkl_app_version

        return str(simkl_app_version())
    except Exception:
        return "1.0"


def _request(path: str, *, instance_id: Any = None, **params: Any) -> Any:
    key = client_id(instance_id)
    if not key:
        raise SimklNotConfigured("SIMKL is not connected")
    try:
        import requests
    except Exception as exc:
        raise SimklCatalogError("HTTP client unavailable") from exc

    version = _app_version()
    query: dict[str, Any] = {"client_id": key, "app-name": "crosswatch", "app-version": version}
    query.update({k: v for k, v in params.items() if v is not None})
    try:
        resp = requests.get(
            f"{BASE}{path}",
            headers={"Accept": "application/json", "User-Agent": f"crosswatch/{version}"},
            params=query,
            timeout=TIMEOUT,
        )
    except Exception as exc:
        raise SimklCatalogError("SIMKL request failed") from exc
    if resp.status_code == 401 or resp.status_code == 403:
        raise SimklNotConfigured("SIMKL rejected the client id")
    if resp.status_code >= 400:
        raise SimklCatalogError(f"SIMKL returned {resp.status_code}")
    try:
        return resp.json()
    except Exception as exc:
        raise SimklCatalogError("SIMKL returned an unreadable response") from exc


def _ids_of(row: Mapping[str, Any]) -> dict[str, str]:
    ids = row.get("ids") if isinstance(row.get("ids"), Mapping) else {}
    out: dict[str, str] = {}
    for k, v in (ids or {}).items():
        text = _text(v, 64)
        if text:
            out[str(k).strip().lower()] = text
    return out


def _simkl_id(ids: Mapping[str, str]) -> str:
    return _text(ids.get("simkl") or ids.get("simkl_id"), 64)


def _kind_of(row: Mapping[str, Any]) -> str:
    return _text(row.get("anime_type") or row.get("type"), 32).lower()


def _entry_from_row(row: Mapping[str, Any], *, total_episodes: int | None = None) -> Entry | None:
    ids = _ids_of(row)
    simkl = _simkl_id(ids)
    if not simkl:
        return None
    title = _text(row.get("title")) or _text(row.get("title_romaji")) or f"SIMKL {simkl}"
    label = _text(row.get("title_en")) or _text(row.get("en_title")) or title
    return Entry(
        simkl=simkl,
        title=title,
        label=label,
        year=_int_or_none(row.get("year")),
        kind=_kind_of(row),
        tmdb=_text(ids.get("tmdb"), 64),
        total_episodes=total_episodes if total_episodes is not None else _int_or_none(row.get("total_episodes")),
        poster=_text(row.get("poster"), 200),
    )


def search(query: str, *, limit: int = MAX_RESULTS, instance_id: Any = None) -> list[Entry]:
    term = _text(query, 200)
    if not term:
        return []
    body = _request(
        SEARCH_PATH,
        instance_id=instance_id,
        **{SEARCH_PARAM: term},
        limit=max(1, min(int(limit or MAX_RESULTS), MAX_RESULTS)),
        page=1,
    )
    rows = body if isinstance(body, list) else []
    out: list[Entry] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        entry = _entry_from_row(row)
        if entry is None or entry.kind not in SEASON_TYPES:
            continue
        out.append(entry)
    return out


def detail(simkl_id: Any, *, instance_id: Any = None) -> dict[str, Any]:
    ident = _text(simkl_id, 64)
    if not ident or not ident.isdigit():
        raise SimklCatalogError("Invalid SIMKL id")
    body = _request(f"{DETAIL_PATH}/{ident}", instance_id=instance_id)
    return dict(body) if isinstance(body, Mapping) else {}


def _sequel_ids(body: Mapping[str, Any]) -> list[str]:
    raw = body.get("relations")
    relations: list[Any] = raw if isinstance(raw, list) else []
    out: list[str] = []
    for rel in relations:
        if not isinstance(rel, Mapping):
            continue
        if _text(rel.get("relation_type"), 32).lower() != CHAIN_RELATION:
            continue
        if _kind_of(rel) not in SEASON_TYPES:
            continue
        ident = _simkl_id(_ids_of(rel))
        if ident and ident not in out:
            out.append(ident)
    return out


def season_chain(simkl_id: Any, *, instance_id: Any = None) -> Chain:
    root = detail(simkl_id, instance_id=instance_id)
    chain = Chain()
    seen: set[str] = set()
    skipped: list[dict[str, Any]] = []

    def consider(body: Mapping[str, Any]) -> None:
        entry = _entry_from_row(body)
        if entry is None or entry.simkl in seen:
            return
        seen.add(entry.simkl)
        if entry.year is None or entry.total_episodes is None or entry.total_episodes <= 0:
            skipped.append({"simkl": entry.simkl, "label": entry.label, "reason": "not aired yet"})
            return
        chain.entries.append(entry)

    consider(root)
    for ident in _sequel_ids(root)[:MAX_CHAIN]:
        try:
            consider(detail(ident, instance_id=instance_id))
        except SimklCatalogError:
            skipped.append({"simkl": ident, "label": "", "reason": "lookup failed"})

    chain.entries.sort(key=lambda e: (e.year or 0, e.simkl))
    chain.skipped = skipped
    return chain


def plan_rules(
    simkl_id: Any,
    *,
    match_provider: str,
    match_id: str,
    match_season: Any = 1,
    title: str = "",
    instance_id: Any = None,
) -> dict[str, Any]:
    provider = _text(match_provider, 32).lower()
    if provider not in MATCH_PROVIDERS:
        raise SimklCatalogError("Unsupported match provider")
    ident = _text(match_id, 64)
    if not ident:
        raise SimklCatalogError("A source id is required")
    season = _int_or_none(match_season)
    if season is None or season < 0:
        season = 1

    chain = season_chain(simkl_id, instance_id=instance_id)
    if not chain.entries:
        raise SimklCatalogError("No aired seasons found for that SIMKL entry")

    base_title = _text(title) or chain.entries[0].title
    rules: list[dict[str, Any]] = []
    cursor = 1
    for entry in chain.entries:
        count = int(entry.total_episodes or 0)
        rules.append(
            {
                "media_type": "show",
                "title": base_title,
                "note": entry.label,
                "match_provider": provider,
                "match_id": ident,
                "match_season": season,
                "target_namespace": "simkl",
                "target_id": entry.simkl,
                "episode_from": cursor,
                "episode_to": cursor + count - 1,
                "episode_start_at": 1,
                "enabled": True,
            }
        )
        cursor += count

    return {
        "rules": rules,
        "chain": [e.as_dict() for e in chain.entries],
        "skipped": chain.skipped,
        "total_episodes": cursor - 1,
    }
