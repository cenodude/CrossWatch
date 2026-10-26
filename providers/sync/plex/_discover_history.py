# /providers/sync/plex/_discover_history.py
# Plex account watched-state fallback with recoverable watch-date updates
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from contextlib import contextmanager
from email.utils import parsedate_to_datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from threading import RLock
import time
from typing import Any, Mapping
from urllib.parse import quote

import requests

from . import _common as common

COMMUNITY = "https://community.plex.tv/api"
DEFERRED_REASONS = frozenset({
    "plex_discover_scope_unavailable",
    "plex_discover_rate_limited", "plex_discover_date_pending", "plex_discover_request_failed",
    "plex_discover_invalid_response", "plex_discover_watch_state_unavailable", "plex_discover_history_order_unavailable",
    "plex_discover_history_unavailable", "plex_discover_history_incomplete", "plex_discover_catalog_incomplete",
})
_LOCK = RLock()
NON_STRIKE_REASONS = DEFERRED_REASONS | frozenset({
    "plex_discover_manual_review", "plex_discover_account_token_required", "plex_discover_endpoint_unavailable",
})
_COMPLETED_TTL = 86400
_HISTORY_PAGE_BUDGET = 100
_HISTORY_BACKOFF = 3600
_PENDING_TIMEOUT = 3600
_HISTORY_QUERY = ("query history($id: ID!, $after: String) { user(id: $id) { "
                  "watchHistory(first: 100, after: $after) { nodes { id date metadataItem { id } } "
                  "pageInfo { hasNextPage endCursor } } } }")


class CloudError(Exception):
    pass


def enabled(adapter: Any) -> bool:
    return common.plex_feature_cfg(adapter, "history").get("discover_fallback") is True


def _signature(item: Mapping[str, Any]) -> str:
    kind = item.get("type")
    ids = item.get("show_ids" if kind == "episode" else "ids") or {}
    identity = sorted((k, str(v)) for k, v in ids.items() if k in {"tmdb", "tvdb", "imdb"} and v)
    episode_ids = sorted((k, str(v)) for k, v in (item.get("ids") or {}).items()
                         if kind == "episode" and k in {"tmdb", "tvdb", "imdb"} and v)
    return sha256(json.dumps([kind, identity, item.get("season"), item.get("episode"), episode_ids]).encode()).hexdigest()


def completed_items(adapter: Any, items: list[Mapping[str, Any]] | None) -> set[str]:
    if not enabled(adapter):
        return set()
    token = str(common.plex_cfg_get(adapter, "account_token", "") or "")
    if not token:
        return set()
    fingerprint = sha256(token.encode()).hexdigest()
    cli = getattr(adapter, "client", None)
    if getattr(cli, "selected_account_id", None) and not getattr(cli, "selected_username", None):
        return set()
    expected = getattr(cli, "selected_username", None) or getattr(cli, "user_username", None)
    signatures = {_signature(item) for item in items} if items is not None else None
    found: set[str] = set()
    for path in common.STATE_DIR.glob("plex_discover_history.*.json"):
        try:
            state = json.loads(path.read_text("utf-8"))
            if state.get("account_fingerprint") != fingerprint:
                continue
            if expected and str(expected).casefold() != str(state.get("username") or "").casefold():
                continue
            pending = state.get("pending") or {}
            found.update(key for key, value in state.get("completed", {}).items() if (signatures is None or key in signatures)
                         and float(value["at"]) > time.time() - _COMPLETED_TTL and value["key"] not in pending)
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            continue
    return found


@contextmanager
def _locked(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK, path.with_suffix(".lock").open("a+b") as handle:
        if os.name == "nt":
            import msvcrt
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            if os.name == "nt":
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _save(path: Path, state: Mapping[str, Any]) -> None:
    state = dict(state)
    if "completed" in state:
        state["completed"] = {key: value for key, value in state["completed"].items()
                              if float(value.get("at") or 0) > time.time() - _COMPLETED_TTL}
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            name = handle.name
            json.dump(state, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


class Writer:
    def __init__(self, adapter: Any):
        self.adapter = adapter
        self.token = str(common.plex_cfg_get(adapter, "account_token", "") or "")
        self.session = requests.Session()
        self.session.headers.update(common.plex_headers(self.token))
        self.session.headers["Accept"] = "application/json"
        self.uuid = ""
        self.username = ""
        self.cache: dict[tuple, Any] = {}
        self.blocked_until = 0.0
        self.clock_offset = 0.0
        self.account_error = ""
        self.history_rows: list[dict[str, Any]] | None = None
        self.history_pages = 0
        self.history_scan_pages = 0
        self.history_since = float("inf")

    def close(self) -> None:
        self.session.close()

    def _request(self, method: str, url: str, *, empty: bool = False, **kwargs: Any) -> dict[str, Any]:
        if self.account_error:
            raise CloudError(self.account_error)
        if time.time() < self.blocked_until:
            raise CloudError("plex_discover_rate_limited")
        response = self.session.request(method, url, timeout=(10, 30), allow_redirects=False, **kwargs)
        if response.headers.get("Date"):
            try:
                self.clock_offset = parsedate_to_datetime(response.headers["Date"]).timestamp() - time.time()
            except (TypeError, ValueError, OverflowError):
                pass
        if response.status_code == 429:
            try:
                delay = max(60, int(response.headers.get("Retry-After", 60)))
            except (ValueError, TypeError):
                delay = 60
            self.blocked_until = time.time() + delay
            raise CloudError("plex_discover_rate_limited")
        if response.status_code in {401, 403}:
            self.account_error = "plex_discover_account_token_required"
            raise CloudError(self.account_error)
        if response.status_code == 404:
            raise CloudError("plex_discover_endpoint_unavailable")
        if not 200 <= response.status_code < 300:
            raise CloudError("plex_discover_request_failed")
        if empty:
            return {}
        data = response.json()
        if not isinstance(data, dict):
            raise CloudError("plex_discover_invalid_response")
        return data

    def _gql(self, query: str, variables: Mapping[str, Any]) -> dict[str, Any]:
        result = self._request("POST", COMMUNITY, json={"query": query, "variables": dict(variables)})
        errors = result.get("errors") or []
        for error in errors:
            ext = error.get("extensions") or {}
            if ext.get("code") == "RATE_LIMITED":
                try:
                    delay = max(60, int(ext.get("retryAfter") or 60))
                except (TypeError, ValueError):
                    delay = 60
                self.blocked_until = time.time() + delay
                raise CloudError("plex_discover_rate_limited")
        if errors or not isinstance(result.get("data"), dict):
            raise CloudError("plex_discover_request_failed")
        return result["data"]

    def _account(self) -> str:
        if self.account_error:
            raise CloudError(self.account_error)
        try:
            return self._load_account()
        except CloudError as exc:
            self.account_error = str(exc)
            raise
        except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
            self.account_error = "plex_discover_request_failed"
            raise CloudError(self.account_error)

    def _load_account(self) -> str:
        if not self.token:
            raise CloudError("plex_discover_account_token_required")
        if not self.uuid:
            row = self._request("GET", "https://plex.tv/api/v2/user")
            cli = getattr(self.adapter, "client", None)
            expected = getattr(cli, "selected_username", None) or getattr(cli, "user_username", None)
            if expected and str(expected).casefold() != str(row.get("username") or "").casefold():
                raise CloudError("plex_discover_account_mismatch")
            if row.get("restricted") or not row.get("uuid"):
                raise CloudError("plex_discover_account_unsupported")
            self.uuid = str(row["uuid"])
            self.username = str(row.get("username") or "")
        return self.uuid

    def _metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return self._request("GET", common.METADATA + "/library/metadata/" + path, **kwargs).get("MediaContainer") or {}

    def _match(self, kind: str, ids: Mapping[str, Any]) -> str | None:
        key = (kind, tuple(sorted((k, str(v)) for k, v in ids.items() if k in {"tvdb", "tmdb", "imdb"} and v)))
        if key in self.cache:
            return self.cache[key]
        found: set[str] = set()
        for provider, ident in key[1]:
            rows = self._metadata("matches", params={"type": 1 if kind == "movie" else 2,
                                                       "guid": f"{provider}://{ident}", "includeGuids": 1}).get("Metadata") or []
            for row in rows:
                if row.get("type") != kind or not row.get("ratingKey"):
                    continue
                external = common.ids_from_discover_row(row)
                if str(external.get(provider) or "") != ident:
                    continue
                if any(external.get(k) and str(external[k]) != value for k, value in key[1]):
                    raise CloudError("plex_discover_ambiguous_match")
                found.add(str(row["ratingKey"]))
        if len(found) > 1:
            raise CloudError("plex_discover_ambiguous_match")
        self.cache[key] = next(iter(found), None)
        return self.cache[key]

    def _child(self, parent: str, kind: str, index: int) -> str | None:
        key = ("children", parent)
        if key not in self.cache:
            rows = []
            for start in range(0, 100_000, 200):
                page = self._metadata(quote(parent, safe="") + "/children",
                                      params={"includeGuids": 1},
                                      headers={"X-Plex-Container-Start": str(start), "X-Plex-Container-Size": "200"})
                batch = page.get("Metadata") or []
                rows.extend(batch)
                total = common._safe_int(page.get("totalSize"))
                if not batch or (total is not None and start + len(batch) >= total) or (total is None and len(batch) < 200):
                    break
            else:
                raise CloudError("plex_discover_catalog_incomplete")
            self.cache[key] = rows
        matches = {str(r["ratingKey"]) for r in self.cache[key] if r.get("type") == kind and r.get("ratingKey")
                   and common._safe_int(r.get("index")) == index}
        if len(matches) > 1:
            raise CloudError("plex_discover_ambiguous_match")
        return next(iter(matches), None)

    def _resolve(self, item: Mapping[str, Any]) -> str | None:
        kind = item.get("type")
        if kind == "movie":
            return self._match("movie", item.get("ids") or {})
        if kind != "episode":
            return None
        season, episode = common._safe_int(item.get("season")), common._safe_int(item.get("episode"))
        if season is None or season < 0 or episode is None or episode < 1:
            return None
        show = self._match("show", item.get("show_ids") or {})
        parent = self._child(show, "season", season) if show else None
        key = self._child(parent, "episode", episode) if parent else None
        if key:
            ids = item.get("ids") or {}
            for row in self.cache.get(("children", parent), []):
                if str(row.get("ratingKey") or "") != key:
                    continue
                found = common.ids_from_discover_row(row)
                for provider in ("tmdb", "tvdb", "imdb"):
                    if ids.get(provider) and found.get(provider) and str(ids[provider]) != str(found[provider]):
                        raise CloudError("plex_discover_ambiguous_match")
        return key

    def _watched(self, key: str) -> bool:
        data = self._request("GET", common.DISCOVER + "/library/metadata/" + quote(key, safe="") + "/userState")
        states = (data.get("MediaContainer") or {}).get("UserState")
        if not isinstance(states, list) or not states:
            raise CloudError("plex_discover_watch_state_unavailable")
        return any((common._safe_int(s.get("viewCount")) or 0) > 0 for s in states)

    def _entries(self, key: str, since: float) -> list[dict[str, Any]]:
        if self.history_rows is not None and since >= self.history_since:
            return [n for n in self.history_rows if str((n.get("metadataItem") or {}).get("id") or "") == key]
        after = None
        scan_start = self.history_pages
        previous_date = float("inf")
        rows: list[dict[str, Any]] = []
        while self.history_pages < _HISTORY_PAGE_BUDGET:
            self.history_pages += 1
            result = self._gql(_HISTORY_QUERY, {"id": self.uuid, "after": after})
            history = (result.get("user") or {}).get("watchHistory")
            if not isinstance(history, dict) or not isinstance(history.get("nodes"), list):
                raise CloudError("plex_discover_history_unavailable")
            nodes = history["nodes"]
            for node in nodes:
                date = common.as_epoch(node.get("date"))
                if date is None or date > previous_date:
                    self.blocked_until = time.time() + _HISTORY_BACKOFF
                    raise CloudError("plex_discover_history_order_unavailable")
                previous_date = date
            rows.extend(nodes)
            info = history.get("pageInfo") or {}
            if not info.get("hasNextPage") or (nodes and previous_date < since):
                self.history_rows = rows
                self.history_since = since if info.get("hasNextPage") else float("-inf")
                self.history_scan_pages = self.history_pages - scan_start
                return [n for n in rows if str((n.get("metadataItem") or {}).get("id") or "") == key]
            cursor = info.get("endCursor")
            if not cursor or cursor == after:
                break
            after = cursor
        self.blocked_until = time.time() + _HISTORY_BACKOFF
        raise CloudError("plex_discover_history_incomplete")

    def _write(self, path: Path, state: dict[str, Any], key: str, epoch: int, signature: str = "") -> str:
        pending = state.setdefault("pending", {})
        entry = pending.get(key)
        if entry and entry.get("terminal_reason"):
            raise CloudError(entry["terminal_reason"])
        if entry is None:
            if self._watched(key):
                return "present"
            edits = [t for t in state.get("edits", []) if t > time.time() - 61]
            if len(edits) >= 6:
                self.blocked_until = min(edits) + 61
                raise CloudError("plex_discover_rate_limited")
            started = time.time() + self.clock_offset
            before = [str(n["id"]) for n in self._entries(key, started - 120) if n.get("id")]
            if _HISTORY_PAGE_BUDGET - self.history_pages < self.history_scan_pages + 1:
                self.blocked_until = time.time() + _HISTORY_BACKOFF
                raise CloudError("plex_discover_history_incomplete")
            started = time.time() + self.clock_offset
            entry = {"epoch": epoch, "started": started, "created_at": time.time(), "before": before, "signature": signature}
            pending[key] = entry
            _save(path, state)
            try:
                self._request("GET", common.DISCOVER + "/actions/scrobble", empty=True,
                              params={"identifier": "tv.plex.provider.discover", "key": key})
            finally:
                self.history_rows = None
        if not entry.get("activity"):
            rows = self._entries(key, float(entry["started"]) - 120)
            if "review_started_at" not in entry:
                entry["review_started_at"] = time.time()
                _save(path, state)
            candidates = [n for n in rows if n.get("id") and str(n["id"]) not in entry["before"]
                          and float(entry["started"]) - 5 <= (common.as_epoch(n.get("date")) or 0) <= float(entry["started"]) + 120]
            if len(candidates) != 1:
                if time.time() - float(entry["review_started_at"]) >= _PENDING_TIMEOUT:
                    entry["terminal_reason"] = "plex_discover_manual_review"
                    _save(path, state)
                    raise CloudError(entry["terminal_reason"])
                raise CloudError("plex_discover_date_pending")
            entry["activity"] = str(candidates[0]["id"])
            _save(path, state)
        edits = [t for t in state.get("edits", []) if t > time.time() - 61]
        if len(edits) >= 6:
            self.blocked_until = min(edits) + 61
            raise CloudError("plex_discover_rate_limited")
        state["edits"] = edits + [time.time()]
        _save(path, state)
        data = self._gql("mutation updateActivityDate($id: ID!, $input: UpdateActivityInput!) { "
                         "updateActivity(id: $id, input: $input) { id } }",
                         {"id": entry["activity"], "input": {"date": common.iso_from_epoch(entry["epoch"])}})
        if str((data.get("updateActivity") or {}).get("id") or "") != entry["activity"]:
            raise CloudError("plex_discover_date_pending")
        del pending[key]
        if entry.get("signature"):
            state.setdefault("completed", {})[entry["signature"]] = {"at": time.time(), "key": key}
        _save(path, state)
        return "dated" if epoch == entry["epoch"] else "present"

    def repair_pending(self) -> dict[str, str]:
        if not enabled(self.adapter) or common._is_capture_mode():
            return {}
        if not any(common.STATE_DIR.glob("plex_discover_history.*.json")):
            return {}
        results: dict[str, str] = {}
        try:
            uuid = self._account()
            path = common.STATE_DIR / ("plex_discover_history." + sha256(uuid.encode()).hexdigest() + ".json")
            with _locked(path):
                state = json.loads(path.read_text("utf-8")) if path.exists() else {}
                if state.pop("history_limit", None) is not None:
                    state.pop("retry_at", None)
                    _save(path, state)
                if time.time() < float(state.get("retry_at") or 0):
                    return {key: "plex_discover_rate_limited" for key in state.get("pending", {})}
                for key, entry in list(state.get("pending", {}).items()):
                    try:
                        results[key] = self._write(path, state, key, int(entry["epoch"]))
                    except CloudError as exc:
                        results[key] = str(exc)
                    except (requests.RequestException, OSError, ValueError, TypeError, KeyError):
                        results[key] = "plex_discover_request_failed"
                    if self.blocked_until:
                        state["retry_at"] = self.blocked_until
                        _save(path, state)
                        break
        except CloudError as exc:
            return {"journal": str(exc)}
        except (requests.RequestException, OSError, ValueError, TypeError, KeyError, AttributeError):
            return {"journal": "plex_discover_request_failed"}
        return results

    def add(self, item: Mapping[str, Any], epoch: int) -> str:
        if not enabled(self.adapter) or common._is_capture_mode():
            return "plex_discover_disabled"
        try:
            uuid = self._account()
            path = common.STATE_DIR / ("plex_discover_history." + sha256(uuid.encode()).hexdigest() + ".json")
            with _locked(path):
                state = json.loads(path.read_text("utf-8")) if path.exists() else {}
                if state.pop("history_limit", None) is not None:
                    state.pop("retry_at", None)
                    _save(path, state)
                signature = _signature(item)
                for entry in state.get("pending", {}).values():
                    if entry.get("signature") == signature and entry.get("terminal_reason"):
                        return str(entry["terminal_reason"])
                completed = state.get("completed", {}).get(signature) or {}
                if (float(completed.get("at") or 0) > time.time() - _COMPLETED_TTL
                        and completed.get("key") not in state.get("pending", {})):
                    return "cached"
                state["account_fingerprint"] = sha256(self.token.encode()).hexdigest()
                state["username"] = self.username
                if time.time() < float(state.get("retry_at") or 0):
                    return "plex_discover_rate_limited"
                try:
                    key = self._resolve(item)
                    if not key:
                        return "plex_discover_not_found"
                    outcome = self._write(path, state, key, epoch, signature)
                    if outcome in {"dated", "present"}:
                        state.setdefault("completed", {})[signature] = {"at": time.time(), "key": key}
                        _save(path, state)
                    return outcome
                finally:
                    if self.blocked_until:
                        state["retry_at"] = self.blocked_until
                        _save(path, state)
        except CloudError as exc:
            return str(exc)
        except (requests.RequestException, OSError, ValueError, TypeError, KeyError, AttributeError):
            return "plex_discover_request_failed"
