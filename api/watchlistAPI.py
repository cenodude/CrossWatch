# /api/watchlistAPI.py
# CrossWatch - Unified watchlist manager for multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import urllib.parse
from typing import Any, Literal, cast

from fastapi import APIRouter, Body, Path as FPath, Query, Request
from fastapi.responses import JSONResponse

from cw_platform.provider_instances import instances_for_user_profile, normalize_instance_id, provider_display_key
from services.watchlist import (
    _feat_enabled,
    _find_item_in_state,
    _find_item_in_state_for_provider,
    build_watchlist,
    delete_watchlist_batch,
    delete_watchlist_item,
    detect_available_watchlist_providers,
)

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])
_WATCHLIST_PAYLOAD_SCHEMA = "row-meta-v1"


def _watchlist_version(
    *,
    requested_profile: str = "",
    overview: str = "none",
    locale: str | None = None,
    limit: int = 0,
    max_meta: int = 250,
) -> str:
    try:
        from cw_platform.config_base import CONFIG, config_path
        from cw_platform.local_db import manual_policy as sqlite_manual_policy
        from cw_platform.local_db import state as sqlite_state
        from cw_platform.local_db import watchlist_hide as sqlite_watchlist_hide
    except Exception:
        source = {
            "schema": _WATCHLIST_PAYLOAD_SCHEMA,
            "profile": requested_profile,
            "overview": overview,
            "locale": locale,
            "limit": limit,
            "max_meta": max_meta,
        }
    else:
        try:
            cfg_path = config_path()
            cfg_stat = cfg_path.stat()
            cfg_stamp = (str(cfg_path), int(getattr(cfg_stat, "st_mtime_ns", int(cfg_stat.st_mtime * 1e9))), int(cfg_stat.st_size))
        except Exception:
            cfg_stamp = ("", 0, 0)
        source = {
            "schema": _WATCHLIST_PAYLOAD_SCHEMA,
            "state": sqlite_state.fingerprint(CONFIG, {"watchlist"}),
            "policy": sqlite_manual_policy.fingerprint(CONFIG, {"watchlist"}),
            "hidden": sqlite_watchlist_hide.fingerprint(CONFIG),
            "config": cfg_stamp,
            "profile": requested_profile,
            "overview": overview,
            "locale": locale,
            "limit": int(limit or 0),
            "max_meta": int(max_meta or 0),
        }
    blob = json.dumps(source, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _load_watchlist_state() -> dict[str, Any]:
    try:
        from cw_platform.config_base import CONFIG
        from cw_platform.orchestrator._state_store import StateStore

        state = StateStore(CONFIG).load_state_features({"watchlist"})
        return state if isinstance(state, dict) else {}
    except Exception:
        return {}


def _int_param(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _str_param(value: Any, default: str = "") -> str:
    return value if isinstance(value, str) else default


def _public_error(message: str = "operation_failed") -> str:
    return str(message or "operation_failed")


def _sanitize_response_errors(value: Any, default_error: str = "operation_failed") -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if str(k) == "error" and v:
                out[str(k)] = _public_error(default_error)
            else:
                out[str(k)] = _sanitize_response_errors(v, default_error)
        return out
    if isinstance(value, list):
        return [_sanitize_response_errors(v, default_error) for v in value]
    return value


def _norm_key(x: Any) -> str:
    s = str((x.get("key") if isinstance(x, dict) else x) or "").strip()
    return urllib.parse.unquote(s) if "%" in s else s


def _norm_key_spec(x: Any) -> dict[str, Any]:
    key = _norm_key(x)
    aliases_raw = x.get("aliases") if isinstance(x, dict) else []
    alias_values = aliases_raw if isinstance(aliases_raw, list) else []
    aliases = [
        alias
        for alias in dict.fromkeys([key, *(_norm_key(v) for v in alias_values)])
        if alias
    ]
    return {"key": key, "aliases": aliases}


def _resolve_key_spec_for_provider(
    state: dict[str, Any],
    provider: str,
    spec: dict[str, Any],
    *,
    instance_id: str | None = None,
) -> str | None:
    for alias in spec.get("aliases") or []:
        if _find_item_in_state_for_provider(state, str(alias), provider, instance_id=instance_id):
            return str(alias)
    return None


def _active_providers(cfg: dict[str, Any]) -> list[str]:
    try:
        manifest = detect_available_watchlist_providers(cfg) or []
    except Exception:
        manifest = []

    out: list[str] = []
    for it in manifest:
        if not isinstance(it, dict):
            continue
        pid = str(it.get("id") or "").strip().upper()
        if pid and pid != "ALL" and bool(it.get("configured")) and pid not in out:
            out.append(pid)
    return out


def _active_pair_watchlist_providers(cfg: dict[str, Any]) -> list[str]:
    active = set(_active_providers(cfg))
    pairs = cfg.get("pairs") or []
    if not isinstance(pairs, list):
        return []

    out: list[str] = []
    for pair in pairs:
        if not isinstance(pair, dict) or pair.get("enabled") is False:
            continue
        feats = pair.get("features")
        if feats and not _feat_enabled(feats if isinstance(feats, dict) else {}, "watchlist"):
            continue
        for raw in (
            pair.get("src") or pair.get("source"),
            pair.get("dst") or pair.get("target"),
        ):
            prov = str(raw or "").strip().upper()
            if prov and prov in active and prov not in out:
                out.append(prov)
    return out


def _item_matches_user_filter(item: dict[str, Any], user_filter: dict[str, list[str]]) -> bool:
    if not user_filter:
        return True
    wanted = {
        provider_display_key(provider): {normalize_instance_id(inst) for inst in (instances or [])}
        for provider, instances in user_filter.items()
    }
    sources = item.get("sources_by_provider") or item.get("sourcesByProvider")
    if isinstance(sources, dict):
        for provider, raw_instances in sources.items():
            prov = provider_display_key(provider)
            values = raw_instances if isinstance(raw_instances, list) else [raw_instances]
            for inst in values:
                if normalize_instance_id(inst) in wanted.get(prov, set()):
                    return True
    provider = item.get("added_src") or item.get("source") or item.get("provider")
    instance = item.get("added_instance") or item.get("source_instance") or item.get("instance")
    prov = provider_display_key(provider)
    return bool(prov) and normalize_instance_id(instance) in wanted.get(prov, set())


def _item_for_user_filter(item: dict[str, Any], user_filter: dict[str, list[str]]) -> dict[str, Any] | None:
    if not user_filter:
        return item
    wanted = {
        provider_display_key(provider): {normalize_instance_id(inst) for inst in (instances or [])}
        for provider, instances in user_filter.items()
    }
    sources = item.get("sources_by_provider") or item.get("sourcesByProvider")
    if not isinstance(sources, dict):
        return item if _item_matches_user_filter(item, user_filter) else None
    scoped_sources: dict[str, list[str]] = {}
    for provider, raw_instances in sources.items():
        prov = provider_display_key(provider)
        values = raw_instances if isinstance(raw_instances, list) else [raw_instances]
        keep = [
            normalize_instance_id(inst)
            for inst in values
            if normalize_instance_id(inst) in wanted.get(prov, set())
        ]
        if keep:
            scoped_sources[str(provider).lower()] = keep
    if not scoped_sources:
        return None
    out = dict(item)
    providers = sorted(scoped_sources.keys())
    out["sources_by_provider"] = scoped_sources
    out["sourcesByProvider"] = scoped_sources
    out["sources"] = providers
    out["status"] = f"{providers[0]}_only" if len(providers) == 1 else "both"
    out["sync_count"] = len(providers)
    return out


def _effective_user_filter(cfg: dict[str, Any], request: Request | None, requested_profile: str) -> tuple[str, dict[str, list[str]]]:
    try:
        from api.appAuthAPI import COOKIE_NAME, effective_user_profile_id

        token = request.cookies.get(COOKIE_NAME) if request is not None else None
        profile = effective_user_profile_id(cfg, token, requested_profile)
    except Exception:
        profile = "__none__"
    scoped = bool(str(profile or "").strip())
    user_filter = instances_for_user_profile(cfg, profile) if scoped else {}
    if scoped and not user_filter:
        user_filter = {"__NONE__": ["__NONE__"]}
    return str(profile or "").strip(), user_filter


def _delete_scope(cfg: dict[str, Any], request: Request | None) -> dict[str, list[str]] | None:
    profile, user_filter = _effective_user_filter(cfg, request, "")
    if not profile:
        return None
    allowed: dict[str, list[str]] = {}
    for provider, raw in (user_filter or {}).items():
        prov = provider_display_key(provider)
        values = raw if isinstance(raw, list) else [raw]
        keep = [normalize_instance_id(value) for value in values if normalize_instance_id(value)]
        if prov and keep:
            allowed[prov] = keep
    return allowed


def _scope_permits(allowed: dict[str, list[str]] | None, provider: str, instance: Any) -> bool:
    if allowed is None:
        return True
    if not allowed:
        return False
    prov = provider_display_key(provider)
    if not prov or prov == "ALL":
        return True
    owned = allowed.get(prov) or []
    if not owned:
        return False
    inst = normalize_instance_id(instance) if str(instance or "").strip() else ""
    return inst in owned if inst else True


def _scope_denied() -> JSONResponse:
    return JSONResponse({"ok": False, "error": "profile_scope_denied"}, status_code=403)


def _tmdb_api_key(cfg: dict[str, Any]) -> str:
    def _pick_from_block(blk: Any) -> str:
        if not isinstance(blk, dict):
            return ""
        k = str(blk.get("api_key") or "").strip()
        if k:
            return k
        insts = blk.get("instances")
        if isinstance(insts, dict):
            for v in insts.values():
                kk = str((v or {}).get("api_key") or "").strip() if isinstance(v, dict) else ""
                if kk:
                    return kk
        return ""

    for key in ("tmdb", "tmdb_sync"):
        found = _pick_from_block(cfg.get(key))
        if found:
            return found
    return ""

def _type_from_item_or_guess(item: dict[str, Any], key: str = "") -> str:
    t = str(item.get("type") or item.get("media_type") or item.get("entity") or "").lower().strip()
    if t in ("tv", "show", "shows", "series", "episode", "season", "anime"):
        return "tv"
    ids = item.get("ids") or {}
    if isinstance(ids, dict) and (ids.get("anilist") or ids.get("mal")):
        return "tv"
    pref = (key or "").split(":", 1)[0].lower().strip()
    if pref in ("anilist", "mal"):
        return "tv"
    return "movie"

def _item_label(state: dict[str, Any], key: str, prov: str) -> tuple[str, str]:
    it = (
        _find_item_in_state_for_provider(state, key, prov)
        or _find_item_in_state(state, key)
        or {}
    )
    kind = _type_from_item_or_guess(it, key)
    title = it.get("title") or it.get("name") or key
    year = it.get("year") or it.get("release_year")
    return "show" if kind == "tv" else "movie", f"{title} ({year})" if year else str(title)


def _candidate_keys_from_ids(ids: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for k in ("tmdb", "imdb", "tvdb", "trakt", "simkl", "anilist", "mal"):
        v = ids.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if k == "imdb":
            if s.isdigit():
                s = f"tt{s}"
            elif not s.startswith("tt"):
                s = f"tt{s}"
        keys.append(f"{k}:{s}")

    seen: set[str] = set()
    out: list[str] = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out

def _bulk_delete(provider: str, keys_raw: list[Any], provider_instance: str | None = None, allowed_instances: dict[str, list[str]] | None = None) -> dict[str, Any]:
    from cw_platform.config_base import load_config
    from crosswatch import STATS, _append_log

    if not isinstance(keys_raw, list) or not keys_raw:
        return {"ok": False, "error": "keys must be a non-empty array"}

    specs: list[dict[str, Any]] = []
    seen_specs: set[str] = set()
    for spec in (_norm_key_spec(k) for k in keys_raw):
        key = str(spec.get("key") or "")
        if not key or key in seen_specs:
            continue
        seen_specs.add(key)
        specs.append(spec)

    cfg = load_config()
    state = _load_watchlist_state()
    active = _active_providers(cfg)
    prov = (provider or "ALL").upper().strip()
    inst_p = provider_instance if prov != "ALL" else None

    if prov == "ALL":
        targets = active[:]
        if not targets:
            return {"ok": False, "error": "no connected providers"}
    else:
        if prov not in active:
            return {"ok": False, "error": f"provider '{prov}' not connected"}
        targets = [prov]

    if allowed_instances is not None:
        targets = [p for p in targets if allowed_instances.get(provider_display_key(p))]
        if not targets:
            return {"ok": False, "error": "profile_scope_denied"}

    results: list[dict[str, Any]] = []
    deleted_sum = 0

    for p in targets:
        try:
            per_key: list[dict[str, Any]] = []
            deleted = 0
            for spec in specs:
                primary_key = str(spec.get("key") or "")
                if allowed_instances is None:
                    resolved_key = _resolve_key_spec_for_provider(state, p, spec, instance_id=inst_p)
                else:
                    resolved_key = None
                    for owned in allowed_instances.get(provider_display_key(p)) or []:
                        if inst_p and normalize_instance_id(inst_p) != owned:
                            continue
                        resolved_key = _resolve_key_spec_for_provider(state, p, spec, instance_id=owned)
                        if resolved_key:
                            break
                if not resolved_key:
                    per_key.append({"key": primary_key, "deleted": 0, "attempted": False, "reason": "not_in_state"})
                    continue
                kind, label = _item_label(state, resolved_key, p)
                safe_label = (label or "").replace("'", "’")
                r = delete_watchlist_batch([resolved_key], p, state, cfg, provider_instance=inst_p, allowed_instances=allowed_instances) or {}
                d = int(r.get("deleted", 0)) if isinstance(r, dict) else 0
                per_key.append({"key": primary_key, "resolved_key": resolved_key, "deleted": d, "attempted": True})
                deleted += d
                _append_log(
                    "SYNC",
                    f"[WL] delete 1 {kind} '{safe_label}' on {p}: {'OK' if d else 'NOOP'}",
                )
            results.append(
                {
                    "provider": p,
                    "ok": deleted > 0,
                    "deleted": deleted,
                    "per_key": per_key,
                }
            )
            deleted_sum += deleted
        except Exception as e:
            results.append({"provider": p, "ok": False, "error": _public_error("delete_failed")})
            _append_log("SYNC", f"[WL] delete on {p} failed: {e}")

    try:
        fresh = _load_watchlist_state()
        if fresh:
            STATS.refresh_from_state(fresh)
    except Exception:
        pass

    any_ok = any(r.get("ok") for r in results)
    all_ok = all(r.get("ok") for r in results)
    return {
        "ok": any_ok,
        "partial": any_ok and not all_ok,
        "provider": prov,
        "targets": targets,
        "deleted_ok": deleted_sum,
        "deleted_total": len(specs),
        "results": results,
    }

# Auto-remove code
def _origin_allowed_instances(cfg: dict[str, Any], origin: Any) -> dict[str, list[str]] | None:
    if not origin:
        return None
    if isinstance(origin, (tuple, list)):
        parts = list(origin) + ["", ""]
        prov_raw, inst_raw = parts[0], parts[1]
    else:
        prov_raw, _, inst_raw = str(origin).partition(":")
    if not str(prov_raw or "").strip():
        return None
    from cw_platform.access_policy import origin_owner_instances

    return origin_owner_instances(cfg, prov_raw, inst_raw)


def _origin_instances_for(allowed: dict[str, list[str]] | None, provider: str) -> list[str | None]:
    if allowed is None:
        return [None]
    return list(allowed.get(provider_display_key(provider)) or [])


def remove_across_providers_by_ids(
    ids: dict[str, Any],
    media_type: str | None = None,
    origin: Any = None,
) -> dict[str, Any]:
    from cw_platform.config_base import load_config
    from crosswatch import _append_log

    cfg = load_config()
    state = _load_watchlist_state()
    if not ids or not isinstance(ids, dict):
        return {"ok": False, "error": "missing ids"}

    keys = _candidate_keys_from_ids(ids)
    if not keys:
        return {"ok": False, "error": "no candidate keys from ids"}

    allowed = _origin_allowed_instances(cfg, origin)
    providers = _active_pair_watchlist_providers(cfg) or _active_providers(cfg)
    if allowed is not None:
        providers = [p for p in providers if allowed.get(provider_display_key(p))]
    if not providers:
        return {"ok": False, "error": "no connected providers"}

    results: list[dict[str, Any]] = []
    total_deleted = 0

    for prov in providers:
        found_key = None
        for inst in _origin_instances_for(allowed, prov):
            for k in keys:
                if _find_item_in_state_for_provider(state, k, prov, instance_id=inst):
                    found_key = k
                    break
            if found_key:
                break

        if not found_key:
            results.append(
                {
                    "provider": prov,
                    "ok": False,
                    "reason": "not_in_state",
                    "attempted": False,
                }
            )
            continue

        try:
            r = delete_watchlist_batch([found_key], prov, state, cfg, allowed_instances=allowed) or {}
            deleted = int(r.get("deleted", 0)) if isinstance(r, dict) else 0
            total_deleted += deleted
            ok = deleted > 0
            results.append(
                {
                    "provider": prov,
                    "ok": ok,
                    "deleted": deleted,
                    "key": found_key,
                }
            )
            kind, label = _item_label(state, found_key, prov)
            safe_label = (label or "").replace("'", "’")
            _append_log(
                "SYNC",
                f"[WL] auto-remove by ids: {kind} '{safe_label}' on {prov}: {'OK' if ok else 'NOOP'}",
            )
        except Exception as e:
            results.append({"provider": prov, "ok": False, "error": _public_error("delete_failed")})
            _append_log("SYNC", f"[WL] auto-remove on {prov} failed: {e}")

    any_ok = any(r.get("ok") for r in results)
    return {
        "ok": any_ok,
        "deleted_ok": sum(int(r.get("deleted", 0)) for r in results if r.get("ok")),
        "results": results,
    }


def remove_from_provider_by_ids(
    provider: str,
    ids: dict[str, Any],
    media_type: str | None = None,
    origin: Any = None,
) -> dict[str, Any]:
    from cw_platform.config_base import load_config
    from crosswatch import _append_log

    cfg = load_config()
    state = _load_watchlist_state()
    prov = (provider or "").strip().upper()
    if not prov:
        return {"ok": False, "error": "missing provider"}
    if prov not in _active_providers(cfg):
        return {"ok": False, "error": f"provider '{prov}' not connected"}

    allowed = _origin_allowed_instances(cfg, origin)
    if allowed is not None and not allowed.get(provider_display_key(prov)):
        return {"ok": False, "reason": "profile_scope_denied", "provider": prov}

    keys = _candidate_keys_from_ids(ids)
    if not keys:
        return {"ok": False, "error": "no candidate keys from ids"}

    found_key = None
    for inst in _origin_instances_for(allowed, prov):
        for k in keys:
            if _find_item_in_state_for_provider(state, k, prov, instance_id=inst):
                found_key = k
                break
        if found_key:
            break

    if not found_key:
        return {"ok": False, "reason": "not_in_state"}

    try:
        r = delete_watchlist_batch([found_key], prov, state, cfg, allowed_instances=allowed) or {}
        deleted = int(r.get("deleted", 0)) if isinstance(r, dict) else 0
        ok = deleted > 0
        kind, label = _item_label(state, found_key, prov)
        _append_log(
            "SYNC",
            f"[WL] remove_by_ids on {prov}: {kind} '{label}' → {'OK' if ok else 'NOOP'}",
        )
        return {"ok": ok, "deleted": deleted, "provider": prov, "key": found_key}
    except Exception as e:
        _append_log("SYNC", f"[WL] remove_by_ids on {prov} failed: {e}")
        return {"ok": False, "error": _public_error("delete_failed"), "provider": prov}


def remove_from_plex_by_ids(
    ids: dict[str, Any],
    media_type: str | None = None,
    origin: Any = None,
) -> dict[str, Any]:
    return remove_from_provider_by_ids("PLEX", ids, media_type, origin=origin)



@router.get("", include_in_schema=False)
@router.get("/")
def api_watchlist(
    request: Request = cast(Request, None),
    overview: Literal["none", "short", "full"] = Query(
        "none",
        description="Attach overview from TMDb",
    ),
    locale: str | None = Query(
        None,
        description="Override metadata locale",
    ),
    limit: int = Query(
        0,
        ge=0,
        le=5000,
        description="Slice the list",
    ),
    max_meta: int = Query(
        250,
        ge=0,
        le=2000,
        description="Cap enriched items",
    ),
    user_profile: str = Query("", description="Optional user profile id to scope provider instances"),
    known_version: str = Query("", description="Client cache version to avoid rebuilding unchanged watchlist data"),
) -> JSONResponse:

    try:
        from cw_platform.config_base import load_config
        from crosswatch import CACHE_DIR
        from .metaAPI import _shorten, get_meta
    except Exception:
        return JSONResponse({"ok": False, "error": "server import failed"}, status_code=200)

    cfg = load_config()
    overview_value = _str_param(overview, "none")
    if overview_value not in {"none", "short", "full"}:
        overview_value = "none"
    locale_value = _str_param(locale, "") or None
    limit_value = _int_param(limit, 0)
    max_meta_value = _int_param(max_meta, 250)
    known_version_value = _str_param(known_version, "")
    api_key = _tmdb_api_key(cfg)
    has_key = bool(api_key)
    profile, user_filter = _effective_user_filter(cfg, request, user_profile)
    version = _watchlist_version(
        requested_profile=profile,
        overview=overview_value,
        locale=locale_value,
        limit=limit_value,
        max_meta=max_meta_value,
    )
    if known_version_value and known_version_value.strip() == version:
        return JSONResponse(
            {"ok": True, "not_modified": True, "version": version},
            status_code=200,
            headers={"Cache-Control": "no-store"},
        )
    st = _load_watchlist_state()

    if not st:
        return JSONResponse(
            {
                "ok": True,
                "items": [],
                "error": "",
                "missing_tmdb_key": not has_key,
                "last_sync_epoch": None,
                "meta_enriched": 0,
                "version": version,
            },
            status_code=200,
            headers={"Cache-Control": "no-store"},
        )

    try:
        items = build_watchlist(st, tmdb_ok=has_key) or []
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "watchlist build failed", "missing_tmdb_key": not has_key, "version": version},
            status_code=200,
        )

    if user_filter:
        items = [
            scoped
            for it in items
            if isinstance(it, dict)
            for scoped in [_item_for_user_filter(it, user_filter)]
            if scoped is not None
        ]

    if not items:
        return JSONResponse(
            {
                "ok": bool(profile),
                "items": [],
                "error": "" if profile else "No snapshot data found.",
                "missing_tmdb_key": not has_key,
                "last_sync_epoch": st.get("last_sync_epoch"),
                "meta_enriched": 0,
                "version": version,
            },
            status_code=200,
        )

    if limit_value:
        items = items[:limit_value]

    enriched = 0
    eff_overview = overview_value if (overview_value != "none" and has_key) else "none"

    if has_key and max_meta_value:
        eff_locale = (
            locale_value
            or (cfg.get("metadata") or {}).get("locale")
            or (cfg.get("ui") or {}).get("locale")
            or None
        )

        def _norm_type(x: str | None) -> str:
            t = (x or "").strip().lower()
            if t in {"anime", "tv", "show", "shows", "series", "season", "episode"}:
                return "tv"
            if t in {"movie", "movies", "film", "films"}:
                return "movie"
            return "movie"

        def _genre_names(value: Any) -> list[str]:
            raw = value if isinstance(value, list) else []
            out: list[str] = []
            for row in raw:
                if isinstance(row, str):
                    name = row
                elif isinstance(row, dict):
                    name = str(row.get("name") or row.get("title") or "").strip()
                else:
                    name = ""
                if name and name not in out:
                    out.append(name)
            return out

        def _release_date(meta: dict[str, Any], typ: str) -> str:
            raw_detail = meta.get("detail")
            raw_release = meta.get("release")
            detail: dict[str, Any] = raw_detail if isinstance(raw_detail, dict) else {}
            release: dict[str, Any] = raw_release if isinstance(raw_release, dict) else {}
            if typ == "movie":
                raw = detail.get("release_date") or release.get("date")
            else:
                raw = detail.get("first_air_date") or release.get("date")
            return str(raw or "").strip()

        for it in items:
            if enriched >= max_meta_value:
                break
            tmdb_id = it.get("tmdb")
            if not tmdb_id:
                continue

            meta_type = _norm_type(it.get("type") or it.get("entity") or it.get("media_type"))
            raw_item_ids = it.get("ids")
            item_ids: dict[str, Any] = raw_item_ids if isinstance(raw_item_ids, dict) else {}
            need: dict[str, bool] = {"genres": True, "release": True, "title": True, "year": True}
            if eff_overview != "none":
                need["overview"] = True
                need["tagline"] = True
            try:
                meta = get_meta(
                    api_key,
                    meta_type,
                    tmdb_id,
                    CACHE_DIR,
                    need=need,
                    locale=eff_locale,
                    title=it.get("title"),
                    year=it.get("year"),
                    imdb_id=item_ids.get("imdb"),
                    tvdb_id=item_ids.get("tvdb"),
                ) or {}
                if meta.get("resolved_type"):
                    it["resolved_type"] = meta["resolved_type"]
                genres = _genre_names(meta.get("genres") or (meta.get("detail") or {}).get("genres"))
                if genres:
                    it["genres"] = genres
                release_date = _release_date(meta, meta_type)
                if release_date:
                    if meta_type == "movie":
                        it["release_date"] = release_date
                    else:
                        it["first_air_date"] = release_date
                    if not it.get("year"):
                        try:
                            it["year"] = int(release_date[:4])
                        except Exception:
                            pass
                if eff_overview != "none":
                    desc = meta.get("overview") or ""
                    if desc:
                        if eff_overview == "short":
                            desc = _shorten(desc, 280)
                        it["overview"] = desc
                    if eff_overview == "short" and meta.get("tagline"):
                        it["tagline"] = meta["tagline"]
                enriched += 1
            except Exception:
                continue

    return JSONResponse(
        {
            "ok": True,
            "items": items,
            "missing_tmdb_key": not has_key,
            "last_sync_epoch": st.get("last_sync_epoch"),
            "meta_enriched": enriched,
            "version": version,
        },
        status_code=200,
        headers={"Cache-Control": "no-store"},
    )

@router.delete("/{key}")
def api_watchlist_delete(
    key: str = FPath(...),
    request: Request = cast(Request, None),
    provider: str | None = Query("ALL", description="Provider id or ALL"),
    provider_instance: str | None = Query(None, description="Provider instance id (optional)"),
) -> JSONResponse:
    from cw_platform.config_base import load_config
    from crosswatch import STATS, _append_log

    if "%" in (key or ""):
        key = urllib.parse.unquote(key)

    prov = (provider or "ALL").upper().strip()
    cfg = load_config()
    allowed = _delete_scope(cfg, request)
    if not _scope_permits(allowed, prov, provider_instance):
        return _scope_denied()
    raw_res = delete_watchlist_item(
        key=key,
        state_path=None,
        cfg=cfg,
        provider=prov,
        provider_instance=provider_instance,
        log=_append_log,
        allowed_instances=allowed,
    )

    res: dict[str, Any]
    if isinstance(raw_res, dict):
        res = raw_res
    else:
        res = {"ok": bool(raw_res)}

    if res.get("ok"):
        try:
            state = _load_watchlist_state()
            if state:
                STATS.refresh_from_state(state)
        except Exception:
            pass

    res.setdefault("provider", prov)
    
    res = _sanitize_response_errors(res, "delete_failed")
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))

def _scoped_bulk_delete(request: Request | None, payload: dict[str, Any]) -> Any:
    from cw_platform.config_base import load_config

    provider = str(payload.get("provider") or "ALL").strip().upper()
    provider_instance = payload.get("provider_instance")
    keys = payload.get("keys") or []
    allowed = _delete_scope(load_config(), request)
    if not _scope_permits(allowed, provider, provider_instance):
        return _scope_denied()
    return _bulk_delete(provider, keys, provider_instance=provider_instance, allowed_instances=allowed)


@router.post("/delete")
def api_watchlist_delete_multi(request: Request = cast(Request, None), payload: dict[str, Any] = Body(...)) -> Any:
    return _scoped_bulk_delete(request, payload)


@router.post("/delete_batch")
def api_watchlist_delete_batch(request: Request = cast(Request, None), payload: dict[str, Any] = Body(...)) -> Any:
    return _scoped_bulk_delete(request, payload)
