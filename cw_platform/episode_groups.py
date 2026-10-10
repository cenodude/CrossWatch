# cw_platform/episode_groups.py
# CrossWatch - Explicit History episode group identities and watched-state planning
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .id_map import ID_KEYS, coalesce_ids
from .history_events import history_epoch_from_item
from .provider_instances import normalize_instance_id


class GroupEpisode(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["episode"] = "episode"
    title: str = Field(default="", max_length=300)
    show_ids: dict[str, str] = Field(min_length=1, max_length=16)
    season: int = Field(ge=0, le=999, strict=True)
    episode: int = Field(ge=1, le=99999, strict=True)

    @model_validator(mode="after")
    def valid_ids(self):
        if any(key not in ID_KEYS or not value.strip() or len(value) > 128
               or any(ord(c) < 32 for c in value) for key, value in self.show_ids.items()):
            raise ValueError("Use valid show identifiers")
        ids = coalesce_ids(self.show_ids)
        if len(ids) != len(self.show_ids):
            raise ValueError("Use valid show identifiers")
        self.show_ids = ids
        return self


class GroupSide(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: str = Field(min_length=1, max_length=128)
    instance: str = Field(default="default", max_length=128)
    episodes: list[GroupEpisode] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def distinct_episodes(self):
        self.provider = self.provider.strip().upper()
        self.instance = normalize_instance_id(self.instance)
        if not self.provider:
            raise ValueError("Provider is required")
        rows = [row.model_dump() for row in self.episodes]
        if any(row["show_ids"] != rows[0]["show_ids"] for row in rows):
            raise ValueError("Use the same show identifiers for every episode on one side")
        if any(matches(a, b) for i, a in enumerate(rows) for b in rows[i + 1:]):
            raise ValueError("An episode may appear only once on each side")
        self.episodes.sort(key=lambda row: (row.season, row.episode))
        return self


class EpisodeGroup(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    name: str = Field(min_length=1, max_length=200)
    scrobble: bool = Field(default=False, strict=True)
    source: GroupSide
    target: GroupSide

    @model_validator(mode="after")
    def valid_shape(self):
        counts = (len(self.source.episodes), len(self.target.episodes))
        if min(counts) != 1 or max(counts) < 2:
            raise ValueError("Map one combined episode to two or more separate episodes")
        if endpoint(self.source.model_dump()) == endpoint(self.target.model_dump()):
            raise ValueError("Choose different provider instances")
        return self


def endpoint(side):
    return str(side["provider"]).upper(), normalize_instance_id(side.get("instance"))


def tokens(item):
    if item.get("type") != "episode":
        return set()
    try:
        season, episode = int(item["season"]), int(item["episode"])
    except (KeyError, TypeError, ValueError):
        return set()
    ids = item.get("show_ids") or item.get("ids") or {}
    return {f"{key}:{str(value).lower()}#s{season:02d}e{episode:02d}" for key, value in ids.items() if value}


def matches(a, b):
    return bool(tokens(a) & tokens(b))


def validate_groups(groups):
    result = [EpisodeGroup.model_validate(raw).model_dump() for raw in groups]
    if len({g["id"] for g in result}) != len(result):
        raise ValueError("Episode group IDs must be unique")
    used = {}
    for group in result:
        for side in ("source", "target"):
            members = {token for episode in group[side]["episodes"] for token in tokens(episode)}
            existing = used.setdefault(endpoint(group[side]), set())
            if existing & members:
                raise ValueError("An episode already belongs to another group in this pair")
            existing.update(members)
    return result


def check_corrections(policy, pair_id, groups):
    if not groups:
        return
    from .local_db.manual_policy import _feature_blocks
    from .mapping_policy import effective_policy

    for provider, instance, feature, block in _feature_blocks(effective_policy(policy, pair_id)):
        if feature != "history":
            continue
        members = [episode for group in groups for side in ("source", "target")
                   if endpoint(group[side]) == (provider.upper(), instance) for episode in group[side]["episodes"]]
        targets = [episode for group in groups for side in ("source", "target")
                   if endpoint(group[side]) != (provider.upper(), instance) for episode in group[side]["episodes"]]
        records = block.get("mappings") or {}
        for key, item in ((block.get("adds") or {}).get("items") or {}).items():
            record = records.get(key) or {}
            if any(matches(item, member) for member in targets if members) or (
                    not record.get("original_key") and any(matches(item, member) for member in members)):
                raise ValueError("Remove ordinary History corrections for the grouped episodes first")
        originals = {str(record.get("original_key") or "").lower() for record in records.values()}
        if any(tokens(member) & originals for member in members):
            raise ValueError("Remove ordinary History corrections for the grouped episodes first")


def watched(item):
    return item.get("watched") is not False and bool(
        item.get("watched") or item.get("watched_at") or item.get("last_watched_at") or item.get("_cw_watched_state")
    )


def matched_items(index, episode):
    return [item for item in index.values() if matches(item, episode) and watched(item)]


def coverage(index, side):
    return [bool(matched_items(index, episode)) for episode in side["episodes"]]


def completion_item(episode, seen, group):
    result = deepcopy(episode)
    dates = [ts for item in seen if (ts := history_epoch_from_item(item)) is not None]
    when = datetime.fromtimestamp(max(dates), timezone.utc).isoformat().replace("+00:00", "Z") if dates else "unknown"
    result.update(watched=True, watched_at=when,
                  _cw_episode_group=group["name"])
    return result


def plan_group(group, indexes, previous, *, reverse=True, blocked=""):
    states = {side: coverage(indexes[side], group[side]) for side in ("source", "target")}
    old = previous.get("coverage") or {}
    removed = any(any(was and not now for was, now in zip(old.get(side, []), states[side]))
                  for side in states)
    aligned = all(all(values) for values in states.values()) or not any(any(values) for values in states.values())
    held = not aligned and bool(previous.get("held") or removed)
    reason = blocked or ("A grouped episode was marked unwatched. Resolve the group's watched states on the providers, then refresh. No grouped additions or removals were sent." if held else "")
    adds = {"source": [], "target": []}
    if not reason:
        for src, dst in (("source", "target"), ("target", "source")):
            if dst == "source" and not reverse:
                continue
            if not all(states[src]):
                continue
            seen = [item for episode in group[src]["episodes"] for item in matched_items(indexes[src], episode)]
            adds[dst] = [completion_item(episode, seen, group) for episode, present in zip(group[dst]["episodes"], states[dst]) if not present]
    return adds, {"coverage": states, "held": held}, reason
