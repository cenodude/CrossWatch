# cw_platform/provider_instances.py
# Provider instance helpers (multi-profile / multi-account).
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import copy
import re
import uuid

_DEFAULT_INSTANCE = "default"
_INSTANCES_KEY = "instances"
_CROSSWATCH_PROFILE_ROOT = "profiles"
_USER_PROFILES_KEY = "user_profiles"
_PROVIDER_INSTANCE_IDS_KEY = "provider_instance_ids"
_PROFILE_INSTANCE_UIDS_KEY = "instance_uids"
_PROFILE_MANUAL_INSTANCE_UIDS_KEY = "manual_instance_uids"


def _deep_merge(base: dict[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(base or {})
    for k, v in (overlay or {}).items():
        key = str(k)
        cur = out.get(key)
        if isinstance(cur, dict) and isinstance(v, Mapping):
            out[key] = _deep_merge(cur, dict(v))
        else:
            out[key] = copy.deepcopy(v)
    return out


def normalize_instance_id(v: Any) -> str:
    s = str(v or "").strip()
    if not s or s.lower() == _DEFAULT_INSTANCE:
        return _DEFAULT_INSTANCE

    m = re.match(r"^(?P<id>.+?):(?P<n>\d+)$", s)
    if m:
        s = m.group("id").strip()
    return _DEFAULT_INSTANCE if not s or s.lower() == _DEFAULT_INSTANCE else s


def sanitize_instance_label(v: Any, *, max_len: int = 12) -> str:
    s = str(v or "").strip()
    s = " ".join(s.split())
    return s[:max(0, int(max_len or 0))]


def sanitize_user_profile_label(v: Any, *, max_len: int = 40) -> str:
    s = str(v or "").strip()
    s = " ".join(s.split())
    return s[:max(0, int(max_len or 0))]


def normalize_user_profile_id(v: Any) -> str:
    raw = str(v or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", raw):
        return ""
    return raw


def normalize_provider_instance_uid(v: Any) -> str:
    raw = str(v or "").strip().lower()
    compact = raw.replace("-", "")
    if not re.fullmatch(r"[a-f0-9]{32}", compact):
        return ""
    if raw != compact and not re.fullmatch(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}", raw):
        return ""
    return compact


def generate_user_profile_id(cfg: Mapping[str, Any] | dict[str, Any]) -> str:
    root = user_profiles_root(cfg)
    for _ in range(32):
        pid = uuid.uuid4().hex
        if pid not in root:
            return pid
    raise RuntimeError("user_profile_id_generation_failed")


def generate_provider_instance_uid(cfg: Mapping[str, Any] | dict[str, Any]) -> str:
    root = provider_instance_ids_root(cfg)
    for _ in range(32):
        uid = uuid.uuid4().hex
        if uid not in root:
            return uid
    raise RuntimeError("provider_instance_uid_generation_failed")


def provider_key(provider_name: str) -> str:
    raw = str(provider_name or "").strip().lower()
    return "crosswatch" if raw in {"cw", "crosswatch"} else raw


def provider_display_key(provider_name: Any) -> str:
    key = provider_key(str(provider_name or ""))
    return "TMDB" if key == "tmdb" else key.upper()


def provider_instance_ids_root(cfg: Mapping[str, Any] | dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    root = cfg.get(_PROVIDER_INSTANCE_IDS_KEY) if isinstance(cfg, Mapping) else None
    if isinstance(root, dict):
        return root
    if create and isinstance(cfg, dict):
        cfg[_PROVIDER_INSTANCE_IDS_KEY] = {}
        return cfg[_PROVIDER_INSTANCE_IDS_KEY]
    return {}


def provider_instance_uid_for(
    cfg: Mapping[str, Any] | dict[str, Any],
    provider_name: Any,
    instance_id: Any = None,
    *,
    create: bool = False,
) -> str:
    prov = provider_display_key(provider_name)
    inst = normalize_instance_id(instance_id)
    root = provider_instance_ids_root(cfg, create=create)
    for raw_uid, raw in list(root.items()):
        uid = normalize_provider_instance_uid(raw_uid)
        if not uid or not isinstance(raw, Mapping):
            continue
        if provider_display_key(raw.get("provider")) == prov and normalize_instance_id(raw.get("instance")) == inst:
            if uid != raw_uid and isinstance(cfg, dict):
                root.pop(raw_uid, None)
                root[uid] = dict(raw)
            return uid
    if not create or not isinstance(cfg, dict):
        return ""
    uid = generate_provider_instance_uid(cfg)
    root = provider_instance_ids_root(cfg, create=True)
    root[uid] = {"provider": prov, "instance": inst}
    return uid


def provider_instance_ref(
    cfg: Mapping[str, Any] | dict[str, Any],
    provider_name: Any,
    ref: Any,
) -> tuple[str, str]:
    uid = normalize_provider_instance_uid(ref)
    if uid:
        raw = provider_instance_ids_root(cfg).get(uid)
        if isinstance(raw, Mapping):
            return provider_display_key(raw.get("provider")), normalize_instance_id(raw.get("instance"))
    return provider_display_key(provider_name), normalize_instance_id(ref)


def remove_provider_instance_uid(cfg: dict[str, Any], provider_name: Any, instance_id: Any = None) -> None:
    prov = provider_display_key(provider_name)
    inst = normalize_instance_id(instance_id)
    root = provider_instance_ids_root(cfg)
    for uid, raw in list(root.items()):
        if not isinstance(raw, Mapping):
            continue
        if provider_display_key(raw.get("provider")) == prov and normalize_instance_id(raw.get("instance")) == inst:
            root.pop(uid, None)


def provider_instance_uids_for_ref(cfg: Mapping[str, Any], provider_name: Any, instance_id: Any = None) -> list[str]:
    prov = provider_display_key(provider_name)
    inst = normalize_instance_id(instance_id)
    out: list[str] = []
    for raw_uid, raw in provider_instance_ids_root(cfg).items():
        uid = normalize_provider_instance_uid(raw_uid)
        if not uid or not isinstance(raw, Mapping):
            continue
        if provider_display_key(raw.get("provider")) == prov and normalize_instance_id(raw.get("instance")) == inst:
            out.append(uid)
    return out


def _provider_ref_exists(cfg: Mapping[str, Any], provider_name: Any, instance_id: Any = None) -> bool:
    prov = provider_display_key(provider_name)
    inst = normalize_instance_id(instance_id)
    key = _config_key_for(cfg, prov)
    block = cfg.get(key) if isinstance(cfg, Mapping) else None
    if not isinstance(block, Mapping):
        return False
    if inst == _DEFAULT_INSTANCE:
        return True
    insts = block.get(_INSTANCES_KEY)
    return isinstance(insts, Mapping) and inst in insts and isinstance(insts.get(inst), Mapping)


def _collect_provider_instance_refs(cfg: Mapping[str, Any]) -> set[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()
    for provider, raw in (cfg or {}).items():
        if not isinstance(raw, Mapping):
            continue
        insts = raw.get(_INSTANCES_KEY)
        if not isinstance(insts, Mapping):
            continue
        prov = provider_display_key(provider)
        for instance_id in insts.keys():
            inst = normalize_instance_id(instance_id)
            if inst != _DEFAULT_INSTANCE:
                refs.add((prov, inst))
    profiles = user_profiles_root(cfg)
    for raw_profile in profiles.values():
        if not isinstance(raw_profile, Mapping):
            continue
        insts = raw_profile.get("instances")
        if isinstance(insts, Mapping):
            for provider, values in insts.items():
                for instance_id in _profile_instance_values(values):
                    refs.add((provider_display_key(provider), instance_id))
    pairs = cfg.get("pairs")
    if isinstance(pairs, list):
        for pair in pairs:
            if not isinstance(pair, Mapping):
                continue
            for provider_field, instance_fields in (
                ("source", ("source_instance", "src_instance")),
                ("target", ("target_instance", "dst_instance")),
            ):
                prov = provider_display_key(pair.get(provider_field))
                raw_inst = next((pair.get(field) for field in instance_fields if field in pair), None)
                refs.add((prov, normalize_instance_id(raw_inst)))
    scrobble = cfg.get("scrobble")
    watch = scrobble.get("watch") if isinstance(scrobble, Mapping) else None
    routes = watch.get("routes") if isinstance(watch, Mapping) else None
    if isinstance(routes, list):
        for route in routes:
            if not isinstance(route, Mapping):
                continue
            for provider_field, instance_field in (
                ("provider", "provider_instance"),
                ("sink", "sink_instance"),
            ):
                refs.add((provider_display_key(route.get(provider_field)), normalize_instance_id(route.get(instance_field))))
    webhook = scrobble.get("webhook") if isinstance(scrobble, Mapping) else None
    profiles = webhook.get("profiles") if isinstance(webhook, Mapping) else None
    if isinstance(profiles, Mapping):
        for provider, instances in profiles.items():
            if not isinstance(instances, Mapping):
                continue
            for instance_id in instances.keys():
                refs.add((provider_display_key(provider), normalize_instance_id(instance_id)))
    return {ref for ref in refs if ref[0] and _provider_ref_exists(cfg, ref[0], ref[1])}


def ensure_provider_instance_uids(cfg: dict[str, Any]) -> bool:
    if not isinstance(cfg, dict):
        return False
    before = {
        "provider_instance_ids": copy.deepcopy(provider_instance_ids_root(cfg)),
        "user_profiles": copy.deepcopy(user_profiles_root(cfg)),
    }
    root = provider_instance_ids_root(cfg, create=True)
    seen_refs: set[tuple[str, str]] = set()
    for uid, raw in list(root.items()):
        clean_uid = normalize_provider_instance_uid(uid)
        if not clean_uid or not isinstance(raw, Mapping):
            root.pop(uid, None)
            continue
        prov = provider_display_key(raw.get("provider"))
        inst = normalize_instance_id(raw.get("instance"))
        ref = (prov, inst)
        if not _provider_ref_exists(cfg, prov, inst):
            root.pop(uid, None)
            continue
        if ref in seen_refs:
            root.pop(uid, None)
            continue
        seen_refs.add(ref)
        clean = {"provider": prov, "instance": inst}
        if clean_uid != uid:
            root.pop(uid, None)
            root[clean_uid] = clean
        else:
            root[uid] = clean
    for prov, inst in sorted(_collect_provider_instance_refs(cfg)):
        provider_instance_uid_for(cfg, prov, inst, create=True)
    _migrate_user_profiles_to_instance_uids(cfg)
    after = {
        "provider_instance_ids": provider_instance_ids_root(cfg),
        "user_profiles": user_profiles_root(cfg),
    }
    return after != before


def _profile_prefix(provider: str) -> str:
    k = provider_key(provider)
    if k == "crosswatch":
        return "CW"
    return str(provider or "").strip().upper()


def _safe_profile_dir_name(instance_id: Any) -> str:
    raw = normalize_instance_id(instance_id)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("._- ")
    return safe or "default"


def _crosswatch_profile_root_dir(base_block: Mapping[str, Any], instance_id: Any) -> str:
    root = str(base_block.get("root_dir") or "/config/.cw_provider").strip() or "/config/.cw_provider"
    inst = _safe_profile_dir_name(instance_id)
    clean_root = root.rstrip("/").rstrip("\\")
    return f"{clean_root}/{_CROSSWATCH_PROFILE_ROOT}/{inst}"


def _with_crosswatch_profile_defaults(
    provider_name: str,
    base_block: Mapping[str, Any],
    instance_id: Any,
    block: dict[str, Any],
    instance_block: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if provider_key(provider_name) != "crosswatch" or normalize_instance_id(instance_id) == _DEFAULT_INSTANCE:
        return block
    out = dict(block or {})
    inst_root = str((instance_block or {}).get("root_dir") or "").strip()
    if not inst_root:
        out["root_dir"] = _crosswatch_profile_root_dir(base_block, instance_id)
    out["connected"] = (instance_block or {}).get("connected") is True
    return out


def _config_key_for(cfg: Mapping[str, Any], provider_name: str) -> str:
    k = provider_key(provider_name)
    # TMDb sync stores auth under tmdb_sync (metadata uses tmdb).
    if k == "tmdb":
        return "tmdb_sync"
    return k


def get_provider_block(cfg: Mapping[str, Any], provider_name: str, instance_id: Any = None) -> dict[str, Any]:
    key = provider_key(provider_name)
    base = cfg.get(key) if isinstance(cfg, Mapping) else None
    base_block = dict(base or {}) if isinstance(base, Mapping) else {}

    inst = normalize_instance_id(instance_id)
    if inst == _DEFAULT_INSTANCE:
        return base_block

    insts = base_block.get(_INSTANCES_KEY)
    if isinstance(insts, Mapping) and inst in insts and isinstance(insts.get(inst), Mapping):
        inst_block = dict(insts.get(inst) or {})
        merged_base = dict(base_block or {})
        merged_base.pop(_INSTANCES_KEY, None)
        return _with_crosswatch_profile_defaults(
            provider_name,
            base_block,
            inst,
            _deep_merge(merged_base, inst_block),
            inst_block,
        )

    return {}


def list_instance_ids(cfg: Mapping[str, Any], provider_name: str) -> list[str]:
    key = provider_key(provider_name)
    base = cfg.get(key) if isinstance(cfg, Mapping) else None
    insts = (base or {}).get(_INSTANCES_KEY) if isinstance(base, Mapping) else None

    out: list[str] = [_DEFAULT_INSTANCE]
    if isinstance(insts, Mapping):
        extra = [str(k) for k in insts.keys() if str(k).strip()]
        out.extend(sorted(set(extra)))
    return out


def build_config_view(cfg: Mapping[str, Any], selections: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(cfg or {})
    for prov, inst in (selections or {}).items():
        ck = _config_key_for(cfg, str(prov))
        out[ck] = copy.deepcopy(get_provider_block(cfg, ck, inst))
    return out


def build_pair_config_view(
    cfg: Mapping[str, Any],
    src: str,
    src_instance: Any,
    dst: str,
    dst_instance: Any,
) -> dict[str, Any]:
    return build_config_view(cfg, {src: src_instance, dst: dst_instance})


def build_provider_config_view(cfg: Mapping[str, Any], provider_name: str, instance_id: Any) -> dict[str, Any]:
    return build_config_view(cfg, {provider_name: instance_id})


def ensure_provider_block(cfg: dict[str, Any], provider_name: str) -> dict[str, Any]:
    key = provider_key(provider_name)
    blk = cfg.get(key)
    if isinstance(blk, dict):
        return blk
    out: dict[str, Any] = {}
    cfg[key] = out
    return out


def get_instance_block(
    cfg: Mapping[str, Any] | dict[str, Any],
    provider_name: str,
    instance_id: Any = None,
    *,
    create: bool = False,
) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    key = provider_key(provider_name)

    base = cfg.get(key) if isinstance(cfg, Mapping) else None
    if not isinstance(base, dict):
        if not create or not isinstance(cfg, dict):
            return {}
        base = {}
        cfg[key] = base

    if inst == _DEFAULT_INSTANCE:
        return base

    insts = base.get(_INSTANCES_KEY)
    if not isinstance(insts, dict):
        if not create:
            return {}
        insts = {}
        base[_INSTANCES_KEY] = insts

    blk = insts.get(inst)
    if isinstance(blk, dict):
        return blk

    if not create:
        return {}

    out: dict[str, Any] = {}
    insts[inst] = out
    return out


def apply_instance_defaults(cfg: dict[str, Any], provider_name: str, instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    block = get_instance_block(cfg, provider_name, inst, create=True)
    if provider_key(provider_name) == "crosswatch" and inst != _DEFAULT_INSTANCE:
        base = cfg.get(provider_key(provider_name))
        base_block = base if isinstance(base, Mapping) else {}
        block.setdefault("root_dir", _crosswatch_profile_root_dir(base_block, inst))
    if "label" in block:
        block["label"] = sanitize_instance_label(block.get("label"))
    return block


def set_instance_label(cfg: dict[str, Any], provider_name: str, instance_id: Any = None, label: Any = None) -> dict[str, Any]:
    block = get_instance_block(cfg, provider_name, instance_id, create=True)
    clean = sanitize_instance_label(label)
    if clean:
        block["label"] = clean
    else:
        block.pop("label", None)
    return block


def instance_exists(cfg: Mapping[str, Any], provider_name: str, instance_id: Any = None) -> bool:
    inst = normalize_instance_id(instance_id)
    if inst == _DEFAULT_INSTANCE:
        return True
    return bool(get_instance_block(cfg, provider_name, inst, create=False))


def ensure_instance_block(cfg: dict[str, Any], provider_name: str, instance_id: Any = None) -> dict[str, Any]:
    return get_instance_block(cfg, provider_name, instance_id, create=True)


def user_profiles_root(cfg: Mapping[str, Any] | dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    root = cfg.get(_USER_PROFILES_KEY) if isinstance(cfg, Mapping) else None
    if isinstance(root, dict):
        return root
    if create and isinstance(cfg, dict):
        cfg[_USER_PROFILES_KEY] = {}
        return cfg[_USER_PROFILES_KEY]
    return {}


def _profile_uid_values(raw_uids: Any) -> list[str]:
    values = raw_uids if isinstance(raw_uids, list) else [raw_uids]
    clean: list[str] = []
    for value in values:
        uid = normalize_provider_instance_uid(value)
        if uid and uid not in clean:
            clean.append(uid)
    return clean


def _profile_instance_values(raw_instances: Any) -> list[str]:
    values = raw_instances if isinstance(raw_instances, list) else [raw_instances]
    clean: list[str] = []
    for instance in values:
        raw = str(instance or "").strip()
        if not raw:
            continue
        inst = normalize_instance_id(raw)
        if inst and inst not in clean:
            clean.append(inst)
    return clean[:1]


def _uid_ref(cfg: Mapping[str, Any], uid: Any) -> tuple[str, str] | None:
    clean_uid = normalize_provider_instance_uid(uid)
    if not clean_uid:
        return None
    raw = provider_instance_ids_root(cfg).get(clean_uid)
    if not isinstance(raw, Mapping):
        return None
    prov = provider_display_key(raw.get("provider"))
    inst = normalize_instance_id(raw.get("instance"))
    if not prov or not _provider_ref_exists(cfg, prov, inst):
        return None
    return prov, inst


def _profile_uids_for_instances(cfg: dict[str, Any], instances: Mapping[str, Any]) -> list[str]:
    by_provider: dict[str, str] = {}
    for provider, raw_instances in instances.items():
        prov = provider_display_key(provider)
        if not prov:
            continue
        for inst in _profile_instance_values(raw_instances):
            if not _provider_ref_exists(cfg, prov, inst):
                continue
            uid = provider_instance_uid_for(cfg, prov, inst, create=True)
            if uid:
                by_provider[prov] = uid
    return [by_provider[prov] for prov in sorted(by_provider)]


def _resolved_instances_for_uids(cfg: Mapping[str, Any], uids: list[str]) -> dict[str, list[str]]:
    by_provider: dict[str, list[str]] = {}
    for uid in uids:
        ref = _uid_ref(cfg, uid)
        if ref is None:
            continue
        prov, inst = ref
        if prov not in by_provider:
            by_provider[prov] = [inst]
    return dict(sorted(by_provider.items()))


def _profile_uids_from_raw(cfg: Mapping[str, Any], raw: Mapping[str, Any]) -> list[str]:
    by_provider: dict[str, str] = {}
    for uid in _profile_uid_values(raw.get(_PROFILE_INSTANCE_UIDS_KEY)):
        ref = _uid_ref(cfg, uid)
        if ref is None:
            continue
        prov, _inst = ref
        if prov not in by_provider:
            by_provider[prov] = uid
    insts = raw.get("instances")
    if isinstance(cfg, dict) and isinstance(insts, Mapping):
        for uid in _profile_uids_for_instances(cfg, insts):
            ref = _uid_ref(cfg, uid)
            if ref is None:
                continue
            prov, _inst = ref
            by_provider[prov] = uid
    return [by_provider[prov] for prov in sorted(by_provider)]


def _migrate_user_profiles_to_instance_uids(cfg: dict[str, Any]) -> None:
    root = user_profiles_root(cfg)
    for profile_id, raw in list(root.items()):
        if not isinstance(raw, dict):
            continue
        uids = _profile_uids_from_raw(cfg, raw)
        raw[_PROFILE_INSTANCE_UIDS_KEY] = uids
        if _PROFILE_MANUAL_INSTANCE_UIDS_KEY not in raw:
            raw[_PROFILE_MANUAL_INSTANCE_UIDS_KEY] = list(uids)
        raw.pop("instances", None)


def _clean_user_profile(cfg: Mapping[str, Any], profile_id: Any, raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        return None
    pid = normalize_user_profile_id(profile_id)
    if not pid:
        return None
    label = sanitize_user_profile_label(raw.get("label")) or str(profile_id or "").strip() or pid
    uids = _profile_uids_from_raw(cfg, raw)
    manual_uids = _profile_uid_values(raw.get(_PROFILE_MANUAL_INSTANCE_UIDS_KEY))
    manual_uids = [uid for uid in manual_uids if _uid_ref(cfg, uid) is not None]
    if not manual_uids:
        manual_uids = list(uids)
    instances = _resolved_instances_for_uids(cfg, uids)
    manual_instances = _resolved_instances_for_uids(cfg, manual_uids)
    return {"id": pid, "label": label, "instance_uids": uids, "instances": instances, "manual_instance_uids": manual_uids, "manual_instances": manual_instances}


def list_user_profiles(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    root = user_profiles_root(cfg)
    rows: list[dict[str, Any]] = []
    for profile_id, raw in root.items():
        row = _clean_user_profile(cfg, profile_id, raw)
        if row is not None:
            rows.append(row)
    rows.sort(key=lambda row: (str(row.get("label") or "").lower(), str(row.get("id") or "")))
    return rows


def user_profile_id_for_label(cfg: Mapping[str, Any], label: Any, *, exclude_id: Any = None) -> str:
    clean = sanitize_user_profile_label(label)
    if not clean:
        return ""
    wanted = clean.casefold()
    excluded = normalize_user_profile_id(exclude_id)
    for row in list_user_profiles(cfg):
        pid = str(row.get("id") or "")
        if excluded and pid == excluded:
            continue
        if sanitize_user_profile_label(row.get("label")).casefold() == wanted:
            return pid
    return ""


def _ensure_unique_user_profile_label(cfg: Mapping[str, Any], label: Any, *, profile_id: Any = None) -> None:
    clean = sanitize_user_profile_label(label)
    if not clean:
        raise ValueError("invalid_user_profile")
    if user_profile_id_for_label(cfg, clean, exclude_id=profile_id):
        raise ValueError("duplicate_user_profile_label")


def upsert_user_profile(
    cfg: dict[str, Any],
    profile_id: Any,
    *,
    label: Any = None,
    instances: Mapping[str, Any] | None = None,
    manual_instances: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    clean_label = sanitize_user_profile_label(label)
    pid = normalize_user_profile_id(profile_id)
    if not pid:
        raise ValueError("invalid_user_profile")
    root = user_profiles_root(cfg, create=True)
    current = root.get(pid)
    block = dict(current or {}) if isinstance(current, Mapping) else {}
    next_label = clean_label or sanitize_user_profile_label(block.get("label"))
    _ensure_unique_user_profile_label(cfg, next_label, profile_id=pid)
    block["label"] = next_label
    if instances is not None:
        uids = _profile_uids_for_instances(cfg, instances)
        manual_uids = _profile_uids_for_instances(cfg, manual_instances) if isinstance(manual_instances, Mapping) else list(uids)
        block[_PROFILE_INSTANCE_UIDS_KEY] = uids
        block[_PROFILE_MANUAL_INSTANCE_UIDS_KEY] = manual_uids
        block.pop("instances", None)
    else:
        block[_PROFILE_INSTANCE_UIDS_KEY] = _profile_uids_from_raw(cfg, block)
        block[_PROFILE_MANUAL_INSTANCE_UIDS_KEY] = _profile_uid_values(block.get(_PROFILE_MANUAL_INSTANCE_UIDS_KEY)) or list(block[_PROFILE_INSTANCE_UIDS_KEY])
        block.pop("instances", None)
    root[pid] = block
    return _clean_user_profile(cfg, pid, block) or {"id": pid, "label": block["label"], "instance_uids": [], "instances": {}}


def delete_user_profile(cfg: dict[str, Any], profile_id: Any) -> bool:
    pid = normalize_user_profile_id(profile_id)
    root = user_profiles_root(cfg)
    if not pid or pid not in root:
        return False
    root.pop(pid, None)
    return True


def remove_instance_from_user_profiles(cfg: dict[str, Any], provider_name: Any, instance_id: Any) -> None:
    prov = provider_display_key(provider_name)
    inst = normalize_instance_id(instance_id)
    remove_uids = set(provider_instance_uids_for_ref(cfg, prov, inst))
    root = user_profiles_root(cfg)
    for pid in list(root.keys()):
        block = root.get(pid)
        if not isinstance(block, dict):
            continue
        uids = [uid for uid in _profile_uids_from_raw(cfg, block) if uid not in remove_uids]
        legacy_insts = block.get("instances")
        if isinstance(legacy_insts, dict):
            keep = [value for value in _profile_instance_values(legacy_insts.get(prov)) if value != inst]
            if keep:
                legacy_insts[prov] = keep
            else:
                legacy_insts.pop(prov, None)
        block[_PROFILE_INSTANCE_UIDS_KEY] = uids
        block[_PROFILE_MANUAL_INSTANCE_UIDS_KEY] = [uid for uid in _profile_uid_values(block.get(_PROFILE_MANUAL_INSTANCE_UIDS_KEY)) if uid not in remove_uids]
        block.pop("instances", None)


def assign_instance_to_user_profile(
    cfg: dict[str, Any],
    user_label: Any,
    provider_name: Any,
    instance_id: Any,
) -> dict[str, Any] | None:
    clean_label = sanitize_user_profile_label(user_label)
    prov = provider_display_key(provider_name)
    inst = normalize_instance_id(instance_id)
    if not clean_label:
        remove_instance_from_user_profiles(cfg, prov, inst)
        return None
    uid = provider_instance_uid_for(cfg, prov, inst, create=True)
    if not uid:
        return None
    pid = user_profile_id_for_label(cfg, clean_label) or generate_user_profile_id(cfg)
    profile = upsert_user_profile(cfg, pid, label=clean_label)
    root = user_profiles_root(cfg, create=True)
    block = root.get(profile["id"])
    if not isinstance(block, dict):
        block = {"label": clean_label, "instances": {}}
        root[profile["id"]] = block
    existing = [value for value in _profile_uids_from_raw(cfg, block) if value != uid]
    filtered: list[str] = []
    for value in existing:
        ref = _uid_ref(cfg, value)
        if ref is None or ref[0] == prov:
            continue
        filtered.append(value)
    block[_PROFILE_INSTANCE_UIDS_KEY] = sorted([*filtered, uid])
    block[_PROFILE_MANUAL_INSTANCE_UIDS_KEY] = list(block[_PROFILE_INSTANCE_UIDS_KEY])
    block.pop("instances", None)
    return _clean_user_profile(cfg, profile["id"], block)


def assign_instance_to_user_profile_id(
    cfg: dict[str, Any],
    profile_id: Any,
    provider_name: Any,
    instance_id: Any,
) -> dict[str, Any] | None:
    pid = normalize_user_profile_id(profile_id)
    prov = provider_display_key(provider_name)
    inst = normalize_instance_id(instance_id)
    if not pid:
        remove_instance_from_user_profiles(cfg, prov, inst)
        return None
    uid = provider_instance_uid_for(cfg, prov, inst, create=True)
    if not uid:
        return None
    root = user_profiles_root(cfg, create=True)
    current = root.get(pid)
    if not isinstance(current, Mapping):
        remove_instance_from_user_profiles(cfg, prov, inst)
        return None
    label = sanitize_user_profile_label(current.get("label")) or pid
    root = user_profiles_root(cfg, create=True)
    profile = upsert_user_profile(cfg, pid, label=label)
    block = root.get(profile["id"])
    if not isinstance(block, dict):
        block = {"label": profile["label"], "instances": {}}
        root[profile["id"]] = block
    existing = [value for value in _profile_uids_from_raw(cfg, block) if value != uid]
    filtered: list[str] = []
    for value in existing:
        ref = _uid_ref(cfg, value)
        if ref is None or ref[0] == prov:
            continue
        filtered.append(value)
    block[_PROFILE_INSTANCE_UIDS_KEY] = sorted([*filtered, uid])
    block[_PROFILE_MANUAL_INSTANCE_UIDS_KEY] = list(block[_PROFILE_INSTANCE_UIDS_KEY])
    block.pop("instances", None)
    return _clean_user_profile(cfg, profile["id"], block)


def user_profiles_for_instance(cfg: Mapping[str, Any], provider_name: Any, instance_id: Any) -> list[dict[str, Any]]:
    prov = provider_display_key(provider_name)
    inst = normalize_instance_id(instance_id)
    wanted = set(provider_instance_uids_for_ref(cfg, prov, inst))
    if not wanted:
        return []
    return [row for row in list_user_profiles(cfg) if set(_profile_uid_values(row.get("instance_uids"))) & wanted]


def user_profile_for_instance(cfg: Mapping[str, Any], provider_name: Any, instance_id: Any) -> dict[str, Any] | None:
    rows = user_profiles_for_instance(cfg, provider_name, instance_id)
    return rows[0] if rows else None


def instances_for_user_profile(cfg: Mapping[str, Any], profile_id: Any) -> dict[str, list[str]]:
    pid = normalize_user_profile_id(profile_id)
    if not pid:
        return {}
    for row in list_user_profiles(cfg):
        if row.get("id") == pid:
            return dict(row.get("instances") or {})
    return {}
