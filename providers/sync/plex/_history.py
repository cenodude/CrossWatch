# /providers/sync/plex/_history.py
# Plex Module for history synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from

from ._common import (
    normalize as plex_normalize,
    minimal_from_history_row,
    server_find_rating_key_by_guid,
    candidate_guids_from_ids,
    sort_guid_candidates,
)

UNRESOLVED_PATH = "/config/.cw_state/plex_history.unresolved.json"
SHADOW_PATH = "/config/.cw_state/plex_history.shadow.json"


def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX:history] {msg}")


def _emit(evt: dict[str, Any]) -> None:
    try:
        feature = str(evt.get("feature") or "?")
        head: list[str] = []
        if "event" in evt:
            head.append(f"event={evt['event']}")
        if "action" in evt:
            head.append(f"action={evt['action']}")
        tail = [f"{k}={v}" for k, v in evt.items() if k not in {"feature", "event", "action"}]
        line = " ".join(head + tail)
        print(f"[PLEX:{feature}] {line}", flush=True)
    except Exception:
        pass


def _as_epoch(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp())
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            try:
                n = int(s)
                return n // 1000 if len(s) >= 13 else n
            except Exception:
                return None
        try:
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            return None
    return None


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _plex_cfg(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    return cfg.get("plex", {}) if isinstance(cfg, dict) else {}


def _plex_cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    cfg = _plex_cfg(adapter)
    val = cfg.get(key, default) if isinstance(cfg, dict) else default
    return default if val is None else val


def _history_cfg(adapter: Any) -> Mapping[str, Any]:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        hist = plex.get("history") or {}
        return hist if isinstance(hist, dict) else {}
    except Exception:
        return {}


def _history_cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    cfg = _history_cfg(adapter)
    val = cfg.get(key, default) if isinstance(cfg, dict) else default
    return default if val is None else val


def _get_workers(adapter: Any, cfg_key: str, env_key: str, default: int) -> int:
    try:
        n = int(_plex_cfg_get(adapter, cfg_key, 0) or 0)
    except Exception:
        n = 0
    if n <= 0:
        try:
            n = int(os.environ.get(env_key, str(default)))
        except Exception:
            n = default
    return max(1, min(n, 64))


def _allowed_history_sec_ids(adapter: Any) -> set[str]:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        arr = (plex.get("history") or {}).get("libraries") or []
        return {str(int(x)) for x in arr if str(x).strip()}
    except Exception:
        return set()


def _row_section_id(h: Any) -> str | None:
    for attr in ("librarySectionID", "sectionID", "librarySectionId", "sectionId"):
        v = getattr(h, attr, None)
        if v is not None:
            try:
                return str(int(v))
            except Exception:
                pass
    sk = getattr(h, "sectionKey", None) or getattr(h, "librarySectionKey", None)
    if sk:
        m = re.search(r"/library/sections/(\d+)", str(sk))
        if m:
            return m.group(1)
    return None


def _load_unresolved() -> dict[str, Any]:
    try:
        with open(UNRESOLVED_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save_unresolved(data: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(UNRESOLVED_PATH), exist_ok=True)
        tmp = UNRESOLVED_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, UNRESOLVED_PATH)
    except Exception as e:
        _log(f"unresolved.save failed: {e}")


def _event_key(item: Mapping[str, Any]) -> str:
    minimal = id_minimal(item)
    key = canonical_key(minimal) or canonical_key(item) or ""
    ts = _as_epoch(item.get("watched_at"))
    return f"{key}@{ts}" if ts else key


def _freeze_item(item: Mapping[str, Any], *, action: str, reasons: Iterable[str]) -> None:
    now_iso = _iso(int(datetime.now(timezone.utc).timestamp()))
    key = _event_key(item)
    data = _load_unresolved()
    entry = data.get(key) or {"feature": "history", "action": action, "first_seen": now_iso, "attempts": 0}
    entry["item"] = id_minimal(item)
    entry["watched_at"] = item.get("watched_at")
    entry["last_attempt"] = now_iso
    existing_reasons = set(entry.get("reasons", []))
    entry["reasons"] = sorted(existing_reasons | set(reasons))
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)


def _unfreeze_keys_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved()
    changed = False
    for key in list(keys or []):
        if key in data:
            del data[key]
            changed = True
    if changed:
        _save_unresolved(data)


def _is_frozen(item: Mapping[str, Any]) -> bool:
    return _event_key(item) in _load_unresolved()


def _load_shadow() -> dict[str, Any]:
    try:
        with open(SHADOW_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save_shadow(data: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(SHADOW_PATH), exist_ok=True)
        tmp = SHADOW_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, SHADOW_PATH)
    except Exception:
        pass


def _shadow_add(item: Mapping[str, Any]) -> None:
    try:
        key = _event_key(item)
        if not key:
            return
        data = _load_shadow()
        existing = data.get(key)
        entry: dict[str, Any] = dict(existing) if isinstance(existing, Mapping) else {}
        entry["item"] = id_minimal(item)
        entry["watched_at"] = item.get("watched_at")
        entry["last_seen"] = _iso(int(datetime.now(timezone.utc).timestamp()))
        if "first_seen" not in entry:
            entry["first_seen"] = entry["last_seen"]
        data[key] = entry
        _save_shadow(data)
    except Exception:
        pass


def _has_external_ids(minimal: Mapping[str, Any]) -> bool:
    ids = minimal.get("ids") or {}
    show_ids = minimal.get("show_ids") or {}
    return bool(
        ids.get("imdb")
        or ids.get("tmdb")
        or ids.get("tvdb")
        or ids.get("trakt")
        or show_ids.get("imdb")
        or show_ids.get("tmdb")
        or show_ids.get("tvdb")
        or show_ids.get("trakt")
    )


def _guid_from_minimal(minimal: Mapping[str, Any]) -> str:
    ids = minimal.get("ids") or {}
    guid = minimal.get("guid") or ids.get("guid") or ids.get("plex_guid")
    return str(guid).lower() if guid else ""


def _keep_in_snapshot(adapter: Any, minimal: Mapping[str, Any]) -> bool:
    ignore_local = bool(_plex_cfg_get(adapter, "history_ignore_local_guid", False))
    prefixes = _plex_cfg_get(adapter, "history_ignore_guid_prefixes", ["local://"]) or []
    require_ext = bool(_plex_cfg_get(adapter, "history_require_external_ids", False))
    if require_ext and not _has_external_ids(minimal):
        return False
    if ignore_local:
        guid = _guid_from_minimal(minimal)
        if guid and any(guid.startswith(p.lower()) for p in prefixes):
            return False
    return True


_FETCH_CACHE: dict[str, Any] = {}


def _fetch_one(srv: Any, rating_key: str) -> dict[str, Any] | None:
    try:
        obj = srv.fetchItem(int(rating_key))
        if not obj:
            return None
        meta = plex_normalize(obj) or {}
        return meta if meta else None
    except Exception:
        return None


def _is_marked_watched(obj: Any) -> bool:
    try:
        if getattr(obj, "isWatched", None):
            return True
    except Exception:
        pass
    try:
        view_count = getattr(obj, "viewCount", None)
        if view_count is not None and int(view_count) > 0:
            return True
    except Exception:
        pass
    return False


def _last_view_ts(obj: Any) -> int | None:
    for attr in ("lastViewedAt", "viewedAt"):
        try:
            value = getattr(obj, attr, None)
        except Exception:
            value = None
        ts = _as_epoch(value)
        if ts:
            return ts
    return None


def _iter_marked_watched_from_library(
    adapter: Any,
    allow: set[str],
    since: int | None,
) -> list[tuple[dict[str, Any], int]]:
    results: list[tuple[dict[str, Any], int]] = []
    try:
        sections = list(adapter.libraries(types=("movie", "show")) or [])
    except Exception:
        sections = []
    for sec in sections:
        try:
            section_id = str(getattr(sec, "key", "")).strip()
        except Exception:
            section_id = ""
        if allow and section_id and section_id not in allow:
            continue
        section_type = (getattr(sec, "type", "") or "").lower()
        if section_type == "movie":
            try:
                items = sec.all() or []
            except Exception:
                items = []
            for obj in items:
                try:
                    if not _is_marked_watched(obj):
                        continue
                    ts = _last_view_ts(obj)
                    if ts is None:
                        continue
                    if since is not None and ts < int(since):
                        continue
                    meta = plex_normalize(obj) or {}
                    if not meta:
                        continue
                    results.append((meta, int(ts)))
                except Exception:
                    continue
        elif section_type == "show":
            try:
                shows = sec.all() or []
            except Exception:
                shows = []
            for show in shows:
                try:
                    episodes = show.episodes() or []
                except Exception:
                    episodes = []
                for ep in episodes:
                    try:
                        if not _is_marked_watched(ep):
                            continue
                        ts = _last_view_ts(ep)
                        if ts is None:
                            continue
                        if since is not None and ts < int(since):
                            continue
                        meta = plex_normalize(ep) or {}
                        if not meta:
                            continue
                        results.append((meta, int(ts)))
                    except Exception:
                        continue
    return results


def build_index(adapter: Any, since: int | None = None, limit: int | None = None) -> dict[str, dict[str, Any]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        _log("no PMS bound (account-only) → empty history index")
        return {}
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any | None = prog_mk("history") if callable(prog_mk) else None
    fallback_guid = bool(_plex_cfg_get(adapter, "fallback_GUID", False) or _plex_cfg_get(adapter, "fallback_guid", False))
    if fallback_guid:
        _emit({"event": "debug", "msg": "fallback_guid.enabled", "provider": "PLEX", "feature": "history"})

    def _int_or_zero(v: Any) -> int:
        try:
            return int(v or 0)
        except Exception:
            return 0

    cfg_acct_id = _int_or_zero(_plex_cfg_get(adapter, "account_id", 0))
    cli_acct_id = _int_or_zero(getattr(getattr(adapter, "client", None), "user_account_id", None))
    acct_id = cfg_acct_id or cli_acct_id

    cfg_uname = str(_plex_cfg_get(adapter, "username", "") or "").strip().lower()
    cli_uname = str(getattr(getattr(adapter, "client", None), "user_username", "") or "").strip().lower()
    uname = cfg_uname or cli_uname

    allow = _allowed_history_sec_ids(adapter)
    rows: list[Any] = []
    try:
        explicit_user = bool(cfg_acct_id or cfg_uname)
        kwargs: dict[str, Any] = {}
        if cfg_acct_id and (not cli_acct_id or int(cfg_acct_id) != int(cli_acct_id)):
            kwargs["accountID"] = int(cfg_acct_id)
        elif not explicit_user and cli_acct_id:
            kwargs["accountID"] = int(cli_acct_id)
        if since is not None:
            kwargs["mindate"] = datetime.fromtimestamp(int(since), tz=timezone.utc).replace(tzinfo=None)
        rows = list(srv.history(**kwargs) or [])
        if not rows and "accountID" in kwargs and not explicit_user:
            _log("no rows with accountID → retry without account scope")
            kwargs.pop("accountID", None)
            rows = list(srv.history(**kwargs) or [])
    except Exception as e:
        _log(f"history fetch failed: {e}")
        return {}

    def _username_match(entry: Any, target_uname: str) -> bool:
        if not target_uname:
            return True
        try:
            fields = [
                getattr(getattr(entry, "Account", None), "title", None),
                getattr(getattr(entry, "Account", None), "name", None),
                getattr(entry, "account", None),
                getattr(entry, "username", None),
            ]
            target_lower = target_uname.lower()
            return any(str(v).strip().lower() == target_lower for v in fields if v)
        except Exception:
            return False

    raw_by_rk: dict[str, Any] = {}
    orphans: list[tuple[Any, int]] = []
    work: list[tuple[str, int]] = []
    for entry in rows:
        if allow:
            section_id = _row_section_id(entry)
            if section_id and section_id not in allow:
                continue

        entry_acct = getattr(entry, "accountID", None)

        if cfg_acct_id:
            try:
                if not entry_acct or int(entry_acct) != int(cfg_acct_id):
                    continue
            except Exception:
                continue
        elif cfg_uname:
            if not _username_match(entry, cfg_uname):
                continue
        elif cli_acct_id:
            try:
                if not entry_acct or int(entry_acct) != int(cli_acct_id):
                    continue
            except Exception:
                continue
        else:
            if cli_uname and not _username_match(entry, cli_uname):
                continue

        ts = (
            _as_epoch(getattr(entry, "viewedAt", None))
            or _as_epoch(getattr(entry, "viewed_at", None))
            or _as_epoch(getattr(entry, "lastViewedAt", None))
        )
        if not ts or (since is not None and ts < int(since)):
            continue
        rk = getattr(entry, "ratingKey", None) or getattr(entry, "key", None)
        if rk is None:
            if fallback_guid:
                try:
                    orphans.append((entry, int(ts)))
                except Exception:
                    pass
            continue
        try:
            rk_str = str(int(rk))
            work.append((rk_str, int(ts)))
            raw_by_rk[rk_str] = entry
        except Exception:
            if fallback_guid:
                try:
                    orphans.append((entry, int(ts)))
                except Exception:
                    pass
            continue
    if not work and not (fallback_guid and orphans):
        if prog:
            try:
                prog.done(ok=True, total=0)
            except Exception:
                pass
        _log("index size: 0 (since/user/library filter or empty history)")
        return {}

    work.sort(key=lambda x: x[1], reverse=True)
    if isinstance(limit, int) and limit > 0:
        work = work[: int(limit)]
    total = len(work) + (len(orphans) if fallback_guid else 0)
    if prog:
        try:
            prog.tick(0, total=total, force=True)
        except Exception:
            pass

    unique_rks = sorted({rk for rk, _ in work})
    workers = _get_workers(adapter, "history_workers", "CW_PLEX_HISTORY_WORKERS", 10)
    to_fetch = [rk for rk in unique_rks if rk not in _FETCH_CACHE]
    if to_fetch:
        try:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(_fetch_one, srv, rk): rk for rk in to_fetch}
                for fut in as_completed(futures):
                    rk = futures[fut]
                    meta = fut.result()
                    if rk and meta:
                        _FETCH_CACHE[rk] = meta
        except Exception as e:
            _log(f"parallel fetch error: {e}")

    if fallback_guid:
        misses = [rk for rk in to_fetch if rk not in _FETCH_CACHE]
        for rk in misses:
            _emit({"event": "fallback_guid", "provider": "PLEX", "feature": "history", "action": "try", "rk": rk})
            fb = minimal_from_history_row(raw_by_rk.get(rk), allow_discover=True)
            _emit(
                {
                    "event": "fallback_guid",
                    "provider": "PLEX",
                    "feature": "history",
                    "action": "ok" if fb else "miss",
                    "rk": rk,
                }
            )
            if fb:
                _FETCH_CACHE[rk] = fb

    extras: list[tuple[dict[str, Any], int]] = []
    if fallback_guid and orphans:
        for row_obj, ts in orphans:
            fb = minimal_from_history_row(row_obj, allow_discover=True)
            if fb:
                extras.append((fb, ts))

    out: dict[str, dict[str, Any]] = {}
    done = 0
    ignored = 0
    for rk_str, ts in work:
        meta = _FETCH_CACHE.get(rk_str)
        if not meta or not _keep_in_snapshot(adapter, meta):
            if meta:
                ignored += 1
            done += 1
            if prog:
                try:
                    prog.tick(done, total=total)
                except Exception:
                    pass
            continue
        if allow:
            lid = meta.get("library_id")
            if lid is not None and str(lid) not in allow:
                done += 1
                if prog:
                    try:
                        prog.tick(done, total=total)
                    except Exception:
                        pass
                continue
        row = dict(meta)
        row["watched"] = True
        row["watched_at"] = _iso(ts)
        out[f"{canonical_key(row)}@{ts}"] = row
        done += 1
        if prog:
            try:
                prog.tick(done, total=total)
            except Exception:
                pass

    if extras:
        for meta, ts in extras:
            if isinstance(limit, int) and limit > 0 and len(out) >= int(limit):
                _log(f"index truncated at {limit} (including extras)")
                break
            if not _keep_in_snapshot(adapter, meta):
                done += 1
                if prog:
                    try:
                        prog.tick(done, total=total)
                    except Exception:
                        pass
                continue
            if allow:
                lid = meta.get("library_id")
                if lid is not None and str(lid) not in allow:
                    done += 1
                    if prog:
                        try:
                            prog.tick(done, total=total)
                        except Exception:
                            pass
                    continue
            row = dict(meta)
            row["watched"] = True
            row["watched_at"] = _iso(ts)
            out[f"{canonical_key(row)}@{ts}"] = row
            done += 1
            if prog:
                try:
                    prog.tick(done, total=total)
                except Exception:
                    pass

    try:
        shadow = _load_shadow()
        if shadow:
            for _, entry in list(shadow.items()):
                meta = entry.get("item") or {}
                ts = _as_epoch(entry.get("watched_at"))
                if not ts:
                    continue
                row = dict(meta)
                row["watched"] = True
                row["watched_at"] = _iso(ts)
                key = f"{canonical_key(row)}@{ts}"
                if key not in out:
                    out[key] = row
    except Exception:
        pass

    include_marked_cfg = bool(_history_cfg_get(adapter, "include_marked_watched", False))

    cli = getattr(adapter, "client", None)

    def _same_user(aid1: int | None, uname1: str | None, aid2: int | None, uname2: str | None) -> bool:
        try:
            if aid1 is not None and aid2 is not None and int(aid1) == int(aid2):
                return True
        except Exception:
            pass
        if uname1 and uname2 and str(uname1).strip().lower() == str(uname2).strip().lower():
            return True
        return False

    desired_acct_id: int | None = cfg_acct_id or None
    desired_uname: str | None = cfg_uname or None
    if cli:
        try:
            sel_aid = int(getattr(cli, "selected_account_id", 0) or 0)
            if sel_aid > 0:
                desired_acct_id = sel_aid
        except Exception:
            pass
        try:
            sel_uname = str(getattr(cli, "selected_username", "") or "").strip().lower() or None
            if sel_uname:
                desired_uname = sel_uname
        except Exception:
            pass

    active_acct_id: int | None = cli_acct_id or None
    active_uname: str | None = cli_uname or None

    need_switch = bool(desired_acct_id or desired_uname) and not _same_user(
        desired_acct_id, desired_uname, active_acct_id, active_uname
    )

    include_marked = False
    did_switch = False
    if include_marked_cfg:
        try:
            token_is_owner = int(getattr(cli, "token_account_id", 0) or 0) == 1 if cli else False
        except Exception:
            token_is_owner = False

        can_home = False
        try:
            can_home = bool(getattr(cli, "can_home_switch")()) if cli else False
        except Exception:
            can_home = False

        if not token_is_owner and not can_home:
            _log("include_marked_watched disabled: token is not PMS owner and not a Plex Home user")
        elif need_switch:
            if not cli:
                _log("include_marked_watched disabled: no Plex client bound for home switch")
            else:
                pin = (getattr(getattr(cli, "cfg", None), "home_pin", None) or "").strip() or None
                try:
                    did_switch = bool(
                        getattr(cli, "enter_home_user_scope")(
                            target_username=desired_uname or None,
                            target_account_id=desired_acct_id or None,
                            pin=pin,
                        )
                    )
                except Exception:
                    did_switch = False
                if did_switch:
                    need_switch = False
                else:
                    _log(
                        f"include_marked_watched disabled: switch failed or not a home user (selected={desired_acct_id or desired_uname})"
                    )

        include_marked = bool(include_marked_cfg and not need_switch)

    if include_marked:
        try:
            base_keys: set[str] = set()
            for row in out.values():
                try:
                    base_key = canonical_key(row)
                    if base_key:
                        base_keys.add(base_key)
                except Exception:
                    continue

            marked = _iter_marked_watched_from_library(adapter, allow, since)
            _log(f"marked-watched scan: found={len(marked)} allow={sorted(allow) if allow else 'ALL'}")
            added_marked = 0

            for meta, ts in marked:
                if isinstance(limit, int) and limit > 0 and len(out) >= int(limit):
                    _log(f"index truncated at {limit} (including marked-watched)")
                    break
                if ts is None:
                    continue
                if not _keep_in_snapshot(adapter, meta):
                    continue
                if allow:
                    lid = meta.get("library_id")
                    if lid is not None and str(lid) not in allow:
                        continue
                row = dict(meta)
                row["watched"] = True
                ts_int = int(ts)
                row["watched_at"] = _iso(ts_int)
                base_key = canonical_key(row)
                if base_key and base_key in base_keys:
                    continue
                out[f"{base_key}@{ts_int}"] = row
                if base_key:
                    base_keys.add(base_key)
                added_marked += 1

            _log(f"marked-watched hydrate: added={added_marked} (skipped duplicates via base_key)")
        except Exception as e:
            _log(f"marked-watched hydrate failed: {e}")
        finally:
            if did_switch:
                try:
                    getattr(getattr(adapter, "client", None), "exit_home_user_scope")()
                except Exception:
                    pass

    if prog:
        try:
            prog.done(ok=True, total=total)
        except Exception:
            pass

    _log(
        f"index size: {len(out)} (ignored={ignored}, since={since}, scanned={total}, "
        f"workers={workers}, unique={len(unique_rks)}, selected={acct_id or uname}, token_acct_id={_int_or_zero(getattr(getattr(adapter, 'client', None), 'user_account_id', None))}, "
        f"include_marked={include_marked})"
    )
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        unresolved: list[dict[str, Any]] = []
        for item in items or []:
            _freeze_item(item, action="add", reasons=["no_plex_server"])
            unresolved.append({"item": id_minimal(item), "hint": "no_plex_server"})
        _log("add skipped: no PMS bound")
        return 0, unresolved

    ok = 0
    unresolved: list[dict[str, Any]] = []
    for item in items or []:
        if _is_frozen(item):
            _log(f"skip frozen: {id_minimal(item).get('title')}")
            continue
        ts = _as_epoch(item.get("watched_at"))
        if not ts:
            _freeze_item(item, action="add", reasons=["missing_watched_at"])
            unresolved.append({"item": id_minimal(item), "hint": "missing_watched_at"})
            continue
        rating_key = _resolve_rating_key(adapter, item)
        if not rating_key:
            _freeze_item(item, action="add", reasons=["not_in_library"])
            unresolved.append({"item": id_minimal(item), "hint": "not_in_library"})
            continue
        if _scrobble_with_date(srv, rating_key, ts):
            ok += 1
            _unfreeze_keys_if_present([_event_key(item)])
            _shadow_add(item)
        else:
            _freeze_item(item, action="add", reasons=["scrobble_failed"])
            unresolved.append({"item": id_minimal(item), "hint": "scrobble_failed"})
    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        unresolved: list[dict[str, Any]] = []
        for item in items or []:
            _freeze_item(item, action="remove", reasons=["no_plex_server"])
            unresolved.append({"item": id_minimal(item), "hint": "no_plex_server"})
        _log("remove skipped: no PMS bound")
        return 0, unresolved

    ok = 0
    unresolved: list[dict[str, Any]] = []
    for item in items or []:
        if _is_frozen(item):
            _log(f"skip frozen: {id_minimal(item).get('title')}")
            continue
        rating_key = _resolve_rating_key(adapter, item)
        if not rating_key:
            _freeze_item(item, action="remove", reasons=["not_in_library"])
            unresolved.append({"item": id_minimal(item), "hint": "not_in_library"})
            continue
        if _unscrobble(srv, rating_key):
            ok += 1
            _unfreeze_keys_if_present([_event_key(item)])
        else:
            _freeze_item(item, action="remove", reasons=["unscrobble_failed"])
            unresolved.append({"item": id_minimal(item), "hint": "unscrobble_failed"})
    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved


def _episode_rk_from_show(show_obj: Any, season: Any, episode: Any) -> str | None:
    try:
        try:
            episodes = show_obj.episodes() or []
        except Exception:
            episodes = []
        for ep in episodes:
            season_ok = season is None or getattr(ep, "parentIndex", None) == season or getattr(ep, "seasonNumber", None) == season
            episode_ok = episode is None or getattr(ep, "index", None) == episode
            if season_ok and episode_ok:
                rk = getattr(ep, "ratingKey", None)
                if rk:
                    return str(rk)
    except Exception:
        pass
    try:
        srv = getattr(show_obj, "_server", None) or getattr(show_obj, "server", None)
        show_id = getattr(show_obj, "ratingKey", None)
        if srv and show_id and hasattr(srv, "_session"):
            resp = srv._session.get(
                srv.url(f"/library/metadata/{show_id}/children"),
                params={"X-Plex-Container-Start": 0, "X-Plex-Container-Size": 500},
                timeout=8,
            )
            if resp.ok:
                import xml.etree.ElementTree as ET

                root = ET.fromstring(resp.text or "")
                for ep in root.findall(".//Video"):
                    season_ok = season is None or int(ep.attrib.get("parentIndex", "0") or "0") == int(season)
                    episode_ok = episode is None or int(ep.attrib.get("index", "0") or "0") == int(episode)
                    if season_ok and episode_ok:
                        rk = ep.attrib.get("ratingKey")
                        if rk:
                            return str(rk)
    except Exception:
        pass
    return None


def _resolve_rating_key(adapter: Any, item: Mapping[str, Any]) -> str | None:
    ids = ids_from(item)
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return None

    rk = ids.get("plex") or None
    if rk:
        try:
            if srv.fetchItem(int(rk)):
                return str(rk)
        except Exception:
            pass

    kind = (item.get("type") or "movie").lower()
    is_episode = kind == "episode"
    title = (item.get("title") or "").strip()
    series_title = (item.get("series_title") or "").strip()
    query_title = series_title if is_episode and series_title else title
    if not query_title:
        return None

    year = item.get("year")
    season = item.get("season") or item.get("season_number")
    episode = item.get("episode") or item.get("episode_number")

    sec_types = ("show",) if is_episode else ("movie",)
    allow = _allowed_history_sec_ids(adapter)
    hits: list[Any] = []

    if ids:
        try:
            guids = sort_guid_candidates(candidate_guids_from_ids({"ids": ids}))
            rk_any = server_find_rating_key_by_guid(srv, guids)
        except Exception:
            rk_any = None

        if rk_any:
            try:
                obj = srv.fetchItem(int(rk_any))
                if obj:
                    section_id = str(getattr(obj, "librarySectionID", "") or getattr(obj, "sectionID", "") or "")
                    if not allow or not section_id or section_id in allow:
                        hits.append(obj)
            except Exception:
                pass

    if not hits:
        for sec in adapter.libraries(types=sec_types) or []:
            section_id = str(getattr(sec, "key", "")).strip()
            if allow and section_id not in allow:
                continue
            try:
                search_hits = sec.search(title=query_title) or []
                if len(search_hits) == 1:
                    hits.extend(search_hits)
                    break
                hits.extend(search_hits)
            except Exception:
                continue

    if not hits:
        try:
            mediatype = "episode" if is_episode else "movie"
            search_hits = srv.search(query_title, mediatype=mediatype) or []
            for obj in search_hits:
                section_id = str(getattr(obj, "librarySectionID", "") or getattr(obj, "sectionID", "") or "")
                if allow and section_id and section_id not in allow:
                    continue
                hits.append(obj)
        except Exception:
            pass

    def _score(obj: Any) -> int:
        score = 0
        try:
            obj_title = (getattr(obj, "grandparentTitle", None) if is_episode else getattr(obj, "title", None)) or ""
            if obj_title.strip().lower() == query_title.lower():
                score += 3
            if not is_episode and year is not None and getattr(obj, "year", None) == year:
                score += 2
            if is_episode:
                s_ok = season is None or getattr(obj, "seasonNumber", None) == season or getattr(obj, "parentIndex", None) == season
                e_ok = episode is None or getattr(obj, "index", None) == episode
                if s_ok and e_ok:
                    score += 2
            meta_ids = (plex_normalize(obj).get("ids") or {})
            for key in ("imdb", "tmdb", "tvdb"):
                if key in meta_ids and key in ids and meta_ids[key] == ids[key]:
                    score += 4
        except Exception:
            pass
        return score

    if not hits:
        return None

    if is_episode:
        ep_hits = [o for o in hits if (getattr(o, "type", "") or "").lower() == "episode"]
        if ep_hits:
            best_ep = max(ep_hits, key=_score)
            rk_val = getattr(best_ep, "ratingKey", None)
            return str(rk_val) if rk_val else None
        show_hits = [o for o in hits if (getattr(o, "type", "") or "").lower() == "show"]
        for show in show_hits:
            rk_val = _episode_rk_from_show(show, season, episode)
            if rk_val:
                return rk_val
        return None

    best = max(hits, key=_score)
    rk_val = getattr(best, "ratingKey", None)
    return str(rk_val) if rk_val else None


def _scrobble_with_date(srv: Any, rating_key: Any, epoch: int) -> bool:
    try:
        try:
            obj = srv.fetchItem(int(rating_key))
            if obj:
                obj_type = (getattr(obj, "type", "") or "").lower()
                if obj_type not in ("episode", "movie"):
                    return False
                try:
                    obj.markWatched()
                    return True
                except Exception:
                    pass
        except Exception:
            pass

        url = srv.url("/:/scrobble")
        token = getattr(srv, "token", None) or getattr(srv, "_token", None)
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library", "viewedAt": int(epoch)}
        if token:
            params["X-Plex-Token"] = token
        resp = srv._session.get(url, params=params, headers=getattr(srv._session, "headers", None), timeout=10)
        if resp.status_code == 401 and token:
            params2 = {
                "ratingKey": int(rating_key),
                "identifier": "com.plexapp.plugins.library",
                "viewedAt": int(epoch),
                "X-Plex-Token": token,
            }
            resp = srv._session.get(url, params=params2, headers=getattr(srv._session, "headers", None), timeout=10)
        if not resp.ok:
            print(f"[PLEX:history] scrobble {rating_key} -> {resp.status_code}")
        return resp.ok
    except Exception as e:
        print(f"[PLEX:history] scrobble exception key={rating_key}: {e}")
        return False


def _unscrobble(srv: Any, rating_key: Any) -> bool:
    try:
        url = srv.url("/:/unscrobble")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library"}
        resp = srv._session.get(url, params=params, timeout=10)
        return resp.ok
    except Exception:
        return False
