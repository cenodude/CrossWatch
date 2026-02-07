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
from pathlib import Path

from .._log import log as cw_log

from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from

from ._common import (
    read_json,
    state_file,
    write_json,
    plex_headers,
    _as_base_url,
    _xml_to_container,
    normalize as plex_normalize,
    normalize_discover_row,
    minimal_from_history_row,
    server_find_rating_key_by_guid,
    candidate_guids_from_ids,
    sort_guid_candidates,
)

def _unresolved_path() -> Path:
    return state_file("plex_history.unresolved.json")

def _shadow_path() -> Path:
    return state_file("plex_history.shadow.json")

def _marked_state_path() -> Path:
    return state_file("plex_history.marked_watched.json")


def _load_marked_state() -> dict[str, Any]:
    return read_json(_marked_state_path())


def _save_marked_state(data: Mapping[str, Any]) -> None:
    try:
        write_json(_marked_state_path(), data, indent=0, sort_keys=False, separators=(",", ":"))
    except Exception:
        pass





def _dbg(event: str, **fields: Any) -> None:
    cw_log("PLEX", "history", "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log("PLEX", "history", "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log("PLEX", "history", "warn", event, **fields)


def _error(event: str, **fields: Any) -> None:
    cw_log("PLEX", "history", "error", event, **fields)


def _log(msg: str) -> None:
    _dbg(msg)


def _emit(evt: dict[str, Any]) -> None:
    try:
        feat = str(evt.get("feature") or "history")
        event = str(evt.get("event") or "event")
        action = evt.get("action")
        fields = {k: v for k, v in evt.items() if k not in {"feature", "event", "action"}}
        if action is not None:
            fields["action"] = action
        cw_log("PLEX", feat, "info", event, **fields)
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


def _episode_code(season: Any, episode: Any) -> str | None:
    try:
        s = int(season or 0)
        e = int(episode or 0)
    except Exception:
        return None
    if s <= 0 or e <= 0:
        return None
    return f"S{s:02d}E{e:02d}"


def _force_episode_title(row: dict[str, Any]) -> None:
    if (row.get("type") or "").lower() != "episode":
        return
    code = _episode_code(row.get("season"), row.get("episode"))
    if code:
        row["title"] = code


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
    return read_json(_unresolved_path())


def _save_unresolved(data: Mapping[str, Any]) -> None:
    try:
        write_json(_unresolved_path(), data)
    except Exception as e:
        _warn("unresolved_save_failed", path=str(_unresolved_path()), error=str(e))


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
    return read_json(_shadow_path())


def _save_shadow(data: Mapping[str, Any]) -> None:
    write_json(_shadow_path(), data)


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
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return []
    base = _as_base_url(srv)
    ses = getattr(srv, "_session", None)
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    if not (base and ses and token):
        return []

    state = _load_marked_state()
    try:
        last_ts = int((state.get("last_ts") if isinstance(state, dict) else 0) or 0)
    except Exception:
        last_ts = 0
    cutoff = max(int(since or 0), last_ts) if (since is not None or last_ts) else 0

    headers = dict(getattr(ses, "headers", {}) or {})
    headers.update(plex_headers(token))
    headers["Accept"] = "application/json"

    def _rows_from(r: Any) -> tuple[list[Mapping[str, Any]], int | None]:
        try:
            ctype = (r.headers.get("content-type") or "").lower()
            data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
            mc = data.get("MediaContainer") or {}
            rows = mc.get("Metadata") or []
            total = mc.get("totalSize")
            total_i = int(total) if total is not None else None
            return [x for x in rows if isinstance(x, Mapping)], total_i
        except Exception:
            return [], None

    page_size = 200
    results: list[tuple[dict[str, Any], int]] = []
    newest = last_ts

    try:
        sections = list(adapter.libraries(types=("movie", "show")) or [])
    except Exception:
        sections = []

    for sec in sections:
        try:
            section_id = str(getattr(sec, "key", "") or "").strip()
        except Exception:
            section_id = ""
        if allow and section_id and section_id not in allow:
            continue

        section_type = (getattr(sec, "type", "") or "").lower()
        plex_type = 1 if section_type == "movie" else 4 if section_type == "show" else None
        if plex_type is None:
            continue

        start = 0
        while True:
            params = {
                "type": plex_type,
                "unwatched": 0,
                "sort": "lastViewedAt:desc",
                "includeGuids": 1,
                "X-Plex-Container-Start": start,
                "X-Plex-Container-Size": page_size,
            }
            try:
                r = ses.get(f"{base}/library/sections/{section_id}/all", params=params, headers=headers, timeout=15)
            except Exception:
                break
            if not getattr(r, "ok", False):
                break

            rows, total = _rows_from(r)
            if not rows:
                break

            stop = False
            for row in rows:
                ts = _as_epoch(row.get("lastViewedAt") or row.get("viewedAt"))
                if not ts:
                    continue
                ts_i = int(ts)
                if cutoff and ts_i < cutoff:
                    stop = True
                    break
                if ts_i > newest:
                    newest = ts_i
                meta = normalize_discover_row(row, token=token) or {}
                if meta:
                    results.append((meta, ts_i))

            if stop:
                break
            start += len(rows)
            if total is not None and start >= total:
                break
            if len(rows) < page_size:
                break

    if newest and newest != last_ts:
        try:
            st = dict(state) if isinstance(state, dict) else {}
            st["last_ts"] = newest
            _save_marked_state(st)
        except Exception:
            pass

    return results


def build_index(adapter: Any, since: int | None = None, limit: int | None = None) -> dict[str, dict[str, Any]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        _info("no_server", reason="account_only")
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
        base_kwargs: dict[str, Any] = {}
        if cfg_acct_id and (not cli_acct_id or int(cfg_acct_id) != int(cli_acct_id)):
            base_kwargs["accountID"] = int(cfg_acct_id)
        elif not explicit_user and cli_acct_id:
            base_kwargs["accountID"] = int(cli_acct_id)
        if since is not None:
            base_kwargs["mindate"] = datetime.fromtimestamp(int(since), tz=timezone.utc).replace(tzinfo=None)

        if allow:
            _dbg("history_fetch_scoped", sections=sorted(allow))
            for sec_id in sorted(allow):
                kwargs = dict(base_kwargs)
                try:
                    kwargs["librarySectionID"] = int(sec_id)
                except Exception:
                    continue
                part = list(srv.history(**kwargs) or [])
                if not part and "accountID" in kwargs and not explicit_user:
                    _dbg("retry_without_account_scope", librarySectionID=sec_id)
                    kwargs.pop("accountID", None)
                    part = list(srv.history(**kwargs) or [])
                rows.extend(part)
        else:
            rows = list(srv.history(**base_kwargs) or [])
            if not rows and "accountID" in base_kwargs and not explicit_user:
                _dbg("retry_without_account_scope")
                base_kwargs.pop("accountID", None)
                rows = list(srv.history(**base_kwargs) or [])
    except Exception as e:
        _warn("history_fetch_failed", error=str(e))
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
        _info("index_done", count=0, reason="filters_or_empty")
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
            _warn("parallel_fetch_error", error=str(e))

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
        _force_episode_title(row)
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
                _info("index_truncated", limit=limit, reason="including_extras")
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
            _force_episode_title(row)
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
                _force_episode_title(row)
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
            _info("marked_watched_disabled", reason="not_owner_or_home_user")
        elif need_switch:
            if not cli:
                _info("marked_watched_disabled", reason="no_home_switch_client")
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
                    _info("marked_watched_disabled", reason="home_switch_failed_or_not_home_user", selected=(desired_acct_id or desired_uname))

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
            _info("marked_watched_scan", found=len(marked), allow=(sorted(allow) if allow else "ALL"))
            added_marked = 0

            for meta, ts in marked:
                if isinstance(limit, int) and limit > 0 and len(out) >= int(limit):
                    _info("index_truncated", limit=limit, reason="including_marked_watched")
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
                _force_episode_title(row)
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

            _info("marked_watched_hydrate_done", added=added_marked)
        except Exception as e:
            _warn("marked_watched_hydrate_failed", error=str(e))
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

    _info("index_done", count=len(out), ignored=ignored, since=since, scanned=total, workers=workers, unique=len(unique_rks), selected=(acct_id or uname), token_acct_id=_int_or_zero(getattr(getattr(adapter, 'client', None), 'user_account_id', None)), include_marked=include_marked)
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        unresolved: list[dict[str, Any]] = []
        for item in items or []:
            _freeze_item(item, action="add", reasons=["no_plex_server"])
            unresolved.append({"item": id_minimal(item), "hint": "no_plex_server"})
        _info("write_skipped", op="add", reason="no_server")
        return 0, unresolved

    ok = 0
    unresolved: list[dict[str, Any]] = []
    for item in items or []:
        if _is_frozen(item):
            _dbg("skip_frozen", title=id_minimal(item).get("title"))
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
    _info("write_done", op="add", ok=ok, unresolved=len(unresolved))
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        unresolved: list[dict[str, Any]] = []
        for item in items or []:
            _freeze_item(item, action="remove", reasons=["no_plex_server"])
            unresolved.append({"item": id_minimal(item), "hint": "no_plex_server"})
        _info("write_skipped", op="remove", reason="no_server")
        return 0, unresolved

    ok = 0
    unresolved: list[dict[str, Any]] = []
    for item in items or []:
        if _is_frozen(item):
            _dbg("skip_frozen", title=id_minimal(item).get("title"))
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
    _info("write_done", op="remove", ok=ok, unresolved=len(unresolved))
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
    if kind == "anime":
        kind = "episode"
    is_episode = kind == "episode"
    title = (item.get("title") or "").strip()
    series_title = (item.get("series_title") or "").strip()
    query_title = series_title if is_episode and series_title else title
    strict = bool(_plex_cfg_get(adapter, "strict_id_matching", False))
    if not query_title and not ids:
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

    if not hits and query_title and not strict:
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

    if not hits and query_title and not strict:
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
            _warn("scrobble_failed", rating_key=str(rating_key), status=resp.status_code)
        return resp.ok
    except Exception as e:
        _warn("scrobble_exception", rating_key=str(rating_key), error=str(e))
        return False


def _unscrobble(srv: Any, rating_key: Any) -> bool:
    try:
        url = srv.url("/:/unscrobble")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library"}
        resp = srv._session.get(url, params=params, timeout=10)
        return resp.ok
    except Exception:
        return False
