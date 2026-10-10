# providers/scrobble/episode_groups.py
# CrossWatch - Completed scrobbles for explicit split and combined episode groups
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import math
import threading
from dataclasses import replace

from cw_platform.account_match import normalize_media_account_name
from cw_platform.config_base import CONFIG_BASE
from cw_platform.episode_groups import check_corrections, endpoint, matches, tokens, validate_groups
from cw_platform.local_db import episode_groups as storage
from cw_platform.local_db.manual_policy import load_policy
from cw_platform.mapping_policy import effective_policy, feature_node
from cw_platform.pair_scope import pair_feature_scope
from cw_platform.provider_instances import normalize_instance_id
from cw_platform.scrobble_ack import delivery_receipt
from providers.scrobble.anime_mapping import _event_item

_LOCKS = [threading.RLock() for _ in range(64)]


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _skip(reason):
    return {"ok": True, "skipped": True, "reason": reason}


def _selection(ev, cfg):
    item = _event_item(ev)
    if not item or item.get("type") != "episode":
        return None
    watch = (cfg.get("scrobble") or {}).get("watch") or {}
    source = (str(watch.get("route_provider") or "").upper(), normalize_instance_id(watch.get("route_provider_instance")))
    target = (str(watch.get("route_sink") or "").upper(), normalize_instance_id(watch.get("route_sink_instance")))
    if not source[0] or not target[0]:
        return None
    base = CONFIG_BASE()
    policy = None
    found = []
    for pair in cfg.get("pairs") or []:
        history = (pair.get("features") or {}).get("history")
        if not pair.get("enabled", True) or not isinstance(history, dict) or not history.get("enable", True):
            continue
        if str(pair.get("profile_id") or "") != str(watch.get("route_effective_profile_id") or ""):
            continue
        sides = {side: (str(pair.get(side) or "").upper(), normalize_instance_id(pair.get(f"{side}_instance")))
                 for side in ("source", "target")}
        order = ("source", "target") if (source, target) == (sides["source"], sides["target"]) else ("target", "source")
        if (source, target) != (sides[order[0]], sides[order[1]]):
            continue
        if order[0] == "target" and pair.get("mode") != "two-way":
            continue
        if policy is None:
            policy = load_policy(base)
        groups = validate_groups(((policy.get("pairs") or {}).get(str(pair.get("id"))) or {}).get("episode_groups") or [])
        for group in groups:
            if not group["scrobble"] or any(endpoint(group[side]) != sides[side] for side in sides):
                continue
            if any(matches(item, member) for member in group[order[0]]["episodes"]):
                found.append((base, policy, pair, group, order, item))
    if len(found) > 1:
        raise ValueError("Multiple episode groups match this scrobble route; enable only one matching pair's group")
    return found[0] if found else None


def _completed(ev, cfg):
    scrobble = cfg.get("scrobble") or {}
    watch = scrobble.get("watch") or {}
    sink = str(watch.get("route_sink") or "").lower()
    shared_threshold = sink in {"myanimelist", "kitsu", "wetrakr", "plex", "jellyfin", "emby", "kodi"}
    threshold = None if shared_threshold else (scrobble.get(sink) or {}).get("watched_at")
    if threshold is None:
        threshold = (scrobble.get("trakt") or {}).get("watched_at", 90)
    if sink in {"plex", "jellyfin", "emby", "kodi"}:
        threshold = ((watch.get("route_options") or {}).get("scrobble") or {}).get("watched_at", threshold)
    progress, threshold = float(ev.progress), float(threshold)
    minimum = 0
    if sink == "wetrakr":
        minimum = 80
    elif sink == "simkl":
        from providers.scrobble.simkl import rewatches
        if rewatches.enabled(cfg):
            minimum = 80
    return ev.action == "stop" and math.isfinite(progress) and math.isfinite(threshold) and progress >= max(minimum, min(100, threshold))


def _mapped_event(ev, episode, group, delivery_scope, number):
    raw = {key: value for key, value in (ev.raw or {}).items()
           if key in {"_cw_activity_method", "_cw_source_provider", "_cw_archive_item"}}
    raw["_cw_episode_group"] = group["id"]
    raw["_cw_episode_group_delivery"] = f"{delivery_scope}:{number}"
    return replace(ev, action="stop", ids={f"{key}_show": value for key, value in episode["show_ids"].items()},
                   season=episode["season"], number=episode["episode"], progress=100,
                   session_key=f"episode-group:{delivery_scope}:{number}", raw=raw, position_ms=None, duration_ms=None)


def send_grouped(ev, cfg, send):
    selected = _selection(ev, cfg)
    if selected is None:
        return send(ev)
    base, policy, pair, group, order, item = selected
    watch = (cfg.get("scrobble") or {}).get("watch") or {}
    if not str(ev.account or "").strip() and watch.get("route_provider") != "stremio":
        return _skip("episode_group_account_required")
    check_corrections(policy, str(pair["id"]), [group])
    effective = effective_policy(policy, str(pair["id"]))
    for side in ("source", "target"):
        node = feature_node(effective, *endpoint(group[side]), "history")
        blocks = {str(key).lower() for key in node.get("blocks") or []}
        if any(tokens(member) & blocks for member in group[side]["episodes"]):
            return _skip("episode_group_blocked")
    if not _completed(ev, cfg):
        return _skip("episode_group_completion_only")
    scope = pair_feature_scope(cfg, pair, "history")
    observed = storage.load(base, scope).get(group["id"]) or {}
    signature = _hash({side: dict(endpoint=endpoint(group[side]), episodes=sorted(
        sorted(tokens(episode)) for episode in group[side]["episodes"])) for side in ("source", "target")})
    if observed.get("mapping") == signature and observed.get("held"):
        return _skip("episode_group_unwatched_hold")
    delivery_scope = _hash({"id": group["id"], "mapping": signature, "direction": order,
                           "account": normalize_media_account_name(ev.account), "server": ev.server_uuid,
                           "profile": str(watch.get("route_effective_profile_id") or "")})
    with _LOCKS[int(delivery_scope[:8], 16) % len(_LOCKS)]:
        state = storage.load_scrobble(base, scope, delivery_scope)
        completed = set(state["completed"])
        completed.update(i for i, member in enumerate(group[order[0]]["episodes"]) if matches(item, member))
        state["completed"] = sorted(completed)
        storage.save_scrobble(base, scope, delivery_scope, state)
        if len(completed) != len(group[order[0]]["episodes"]):
            return _skip("episode_group_waiting_for_parts")
        sent = set(state["sent"])
        delivered = False
        for i, member in enumerate(group[order[1]]["episodes"]):
            if i in sent:
                continue
            mapped = _mapped_event(ev, member, group, delivery_scope, i)
            with delivery_receipt(mapped.raw["_cw_episode_group_delivery"], watch.get("route_sink"), watch.get("route_sink_instance")) as receipt:
                try:
                    result = send(mapped)
                except Exception:
                    if not receipt["confirmed"]:
                        raise
                    result = {"ok": True}
            if receipt["confirmed"]:
                result = {"ok": True}
            if not isinstance(result, dict):
                return {"ok": False, "error": "episode_group_delivery_unconfirmed", "retryable": True}
            if result.get("ok") is False:
                return result
            skipped = result.get("skipped") or result.get("log_status") == "skipped"
            accepted = {"duplicate", "already_completed", "already_watched", "destination_already_watched", "session_completed"}
            if str(watch.get("route_sink") or "").lower() == "anilist":
                accepted.add("not_newer")
            if skipped and result.get("reason") not in accepted:
                return {"ok": False, "error": result.get("reason") or "episode_group_delivery_skipped", "retryable": True}
            if result.get("ok") is not True:
                return {"ok": False, "error": "episode_group_delivery_unconfirmed", "retryable": True}
            sent.add(i)
            state["sent"] = sorted(sent)
            storage.save_scrobble(base, scope, delivery_scope, state)
            delivered = True
        return {"ok": True} if delivered else _skip("episode_group_already_completed")
