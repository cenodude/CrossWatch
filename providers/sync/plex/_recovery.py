# /providers/sync/plex/_recovery.py
# Plex Module for removed-item history recovery
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any, Callable, Mapping

from . import _common as common, _history as history


def _tokens(item: Mapping[str, Any]) -> set[str]:
    kind = item.get("type")
    ids = item.get("show_ids") if kind == "episode" else item.get("ids")
    suffix = f"#{item.get('season')}:{item.get('episode')}" if kind == "episode" else ""
    return {f"{kind}:{key}:{value}{suffix}" for key, value in (ids or {}).items()
            if key in {"tmdb", "tvdb", "imdb"} and value}


def _history_label(row: Any) -> str:
    title = str(common._row_get(row, "title") or "Untitled item")
    if common._row_get(row, "type") == "episode":
        series = str(common._row_get(row, "grandparentTitle") or title)
        season = common._row_get(row, "parentIndex")
        if season is None:
            season = common._row_get(row, "seasonNumber")
        episode = common._row_get(row, "index")
        return f"{series} · S{season if season is not None else '?'} E{episode if episode is not None else '?'} · {title}"
    year = common._row_get(row, "year")
    return f"{title} ({year})" if year else title


def _source_group(row: Any, original: Mapping[str, Any]) -> str:
    episodic = original.get("type") == "episode"
    title = original.get("series_title") if episodic else original.get("title")
    year = common._row_get(row, "grandparentYear") if episodic else original.get("year")
    identity = common._row_get(row, "grandparentRatingKey" if episodic else "ratingKey")
    guid = common._row_get(row, "grandparentGuid" if episodic else "guid")
    ids = original.get("show_ids" if episodic else "ids") or {}
    external = sorted((k, str(v)) for k, v in ids.items() if k in {"tmdb", "imdb", "tvdb"} and v)
    return json.dumps(["show" if episodic else "movie", str(identity or guid or ""),
                       external if not identity and not guid else [],
                       str(title or "").strip().casefold(), str(year or "")], ensure_ascii=True)


def scan(adapter: Any, *, progress: Callable[..., None], check_cancel: Callable[[], None],
         max_rows: int = 50_000) -> list[dict[str, Any]]:
    need, switched, aid, uname = common.home_scope_enter(adapter)
    try:
        if need and not switched:
            common.raise_home_scope_not_applied("history", aid, uname)
        server = adapter.client.server
        if server is None:
            raise RuntimeError("Connect a Plex server before recovering history.")
        sections = list(adapter.libraries(types=("movie", "show")))
        allow = common.plex_feature_library_ids(adapter, "history")
        available = {str(s.key) for s in sections}
        if allow and not allow <= available:
            raise RuntimeError("Some selected Plex libraries are unavailable. Check the connection and library scope.")
        current_keys: set[str] = set()
        current_tokens: set[str] = set()
        current_guids: set[str] = set()
        show_ids: dict[str, dict[str, str]] = {}
        for index, section in enumerate(sections):
            check_cancel()
            progress("libraries", f"Checking {section.title}", index, len(sections))
            types = (1,) if section.type == "movie" else (2, 4)
            for kind in types:
                rows, _ = history._fetch_section_guid_rows(server, str(section.key), kind, check_cancel=check_cancel, strict=True)
                for row in rows:
                    check_cancel()
                    if not row.get("ratingKey"):
                        raise RuntimeError("Plex returned an incomplete library. Please retry the scan.")
                    current_keys.add(str(row["ratingKey"]))
                    current_guids.update(history._row_guids(row))
                    ids = common.ids_from_discover_row(row)
                    if kind == 2:
                        show_ids[str(row["ratingKey"])] = ids
                        continue
                    item = common._build_minimal_from_row(row, ids)
                    if kind == 4:
                        item["show_ids"] = {**show_ids.get(str(row.get("grandparentRatingKey")), {}),
                                            **(item.get("show_ids") or {})}
                    current_tokens.update(_tokens(item))
        check_cancel()
        progress("history", "Reading Plex watch history", 0, 0)
        account = getattr(adapter.client, "user_account_id", None)
        configured = common.plex_cfg_get(adapter, "account_id", None)
        account = configured or account
        kwargs = {"accountID": int(account)} if account else {}
        raw_rows = []
        for sid in sorted(allow) if allow else [None]:
            check_cancel()
            scope = {**kwargs, **({"librarySectionID": int(sid)} if sid else {})}
            raw_rows.extend(server.history(**scope))
        recovered = []
        memos: dict[tuple, dict[str, Any]] = {}
        guessed: set[tuple] = set()
        skipped = 0
        def report(checked, title="", action="", result=""):
            activity: dict[str, Any] = dict(checked=checked, found=len(recovered), skipped=skipped,
                            current_item=title, current_action=action)
            if result and title:
                activity["event"] = dict(title=title, result=result)
            progress("matching", "Matching removed-item history", checked, len(raw_rows), activity)

        report(0)
        for index, raw in enumerate(raw_rows):
            check_cancel()
            if not history._allowed_history_type(raw):
                skipped += 1
                report(index + 1)
                continue
            row_account = common._row_get(raw, "accountID")
            if account and row_account is not None and str(row_account) != str(account):
                skipped += 1
                report(index + 1)
                continue
            sid = history._row_section_id(raw)
            if allow and sid and sid not in allow:
                skipped += 1
                report(index + 1)
                continue
            title = _history_label(raw)
            report(index, title, "Checking library presence")
            rk = str(common._row_get(raw, "ratingKey") or "")
            guid = str(common._row_get(raw, "guid") or "")
            if rk in current_keys or (guid and guid in current_guids):
                skipped += 1
                report(index + 1, title, result="Skipped: still in Plex")
                continue
            original = common._build_minimal_from_row(raw, common.ids_from_history_row(raw))
            if _tokens(original) & current_tokens:
                skipped += 1
                report(index + 1, title, result="Skipped: still in Plex")
                continue
            match_key = (common._fb_key_from_row(raw), original.get("season"), original.get("episode"))
            memo = memos.setdefault(match_key, {})
            report(index, title, "Looking up identity")
            item = common.minimal_from_history_row(raw, allow_discover=False, memo=memo)
            method = "Known identity"
            if not item:
                report(index, title, "Searching Plex for a title match")
                item = common.minimal_from_history_row(raw, allow_discover=True, memo=memo, check_cancel=check_cancel)
                guessed.add(match_key)
            if match_key in guessed:
                method = "Plex title match — review before importing"
            item = dict(item or original)
            if _tokens(item) & current_tokens:
                skipped += 1
                report(index + 1, title, result="Skipped: still in Plex")
                continue
            timestamp = history._epoch_from_history_entry(raw)
            item["watched_at"] = history._iso(timestamp) if timestamp is not None else None
            item["watched"] = True
            common.force_episode_title(item)
            identity = common.has_external_ids(item.get("show_ids") or {}) if item.get("type") == "episode" else common.has_external_ids(item.get("ids") or {})
            if item.get("type") == "episode" and (item.get("season") is None or item.get("episode") is None):
                identity = False
            recovered.append({"feature": "history", "item": item, "source_path": "Plex history",
                              "recovery_match": method if identity else "Missing identity — enter a match",
                              "recovery_valid": bool(identity), "recovery_requires_review": match_key in guessed,
                              "original_group": _source_group(raw, original),
                              "original_year": common._row_get(raw, "grandparentYear") if item.get("type") == "episode" else original.get("year"),
                              "original_title": original.get("series_title") or original.get("title")})
            if len(recovered) > max_rows:
                raise RuntimeError(f"More than {max_rows:,} removed watch events. Narrow the configured history library scope.")
            result = "Found for review"
            if not identity:
                result = "Found: needs a match"
            elif timestamp is None:
                result = "Found: missing watch date"
            elif match_key in guessed:
                result = "Found: title guess needs review"
            report(index + 1, title, result=result)
        return recovered
    finally:
        common.home_scope_exit(adapter, switched)
