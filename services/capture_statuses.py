# services/capture_statuses.py
# CrossWatch - Native title status capture and guarded restore.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from dataclasses import asdict
from types import SimpleNamespace

from cw_platform.interactive_reads import use_review_reads
from cw_platform.provider_instances import build_provider_config_view, normalize_instance_id
from cw_platform.run_control import raise_if_cancelled
from cw_platform.status_mapping import Endpoint, ReadState, Status
from cw_platform.status_mapping_store import StatusStore, decode_observation, encode_observation
from providers.sync._status_mapping import NativeStatusAdapter, Record, adapter_class, digest


def capture_statuses(cfg, provider, instance):
    cls = adapter_class(provider)
    if cls is None:
        return None
    view = build_provider_config_view(cfg, provider, instance)
    view['_cw_provider_instance'] = normalize_instance_id(instance)
    view['_cw_readonly'] = True
    instances = (cfg.get(provider.lower()) or {}).get('instances')
    if instances:
        view[provider.lower()] = {**view[provider.lower()], 'instances': instances}
    raise_if_cancelled()
    with use_review_reads(None):
        records = cls(view).read()
    raise_if_cancelled()
    incomplete = sum(not record.observation.complete for record in records.values())
    if incomplete:
        raise ValueError(f'Cannot capture title statuses: {incomplete} incomplete observations. No title statuses were saved.')
    rows = []
    for record in records.values():
        if record.observation.state == ReadState.ABSENT:
            continue
        rows.append({
            'native_id': record.native_id, 'media': record.media,
            'item': {key: deepcopy(record.item[key]) for key in ('ids', 'title', 'type', 'year') if key in record.item},
            'observation': encode_observation(record.observation),
            'native_ids': deepcopy(record.raw.get('native_ids', record.item.get('ids', {}))),
        })
    return {'version': 1, 'items': rows}


def status_records(payload):
    if not isinstance(payload, dict) or type(payload.get('version')) is not int or payload['version'] != 1 or not isinstance(payload.get('items'), list):
        raise ValueError('Invalid captured title statuses')
    records = {}
    for row in payload['items']:
        if not isinstance(row, dict) or not isinstance(row.get('native_id'), str) or not row['native_id'] or row.get('media') not in {'movies', 'shows'}:
            raise ValueError('Invalid captured status identity')
        item, native_ids = row.get('item'), row.get('native_ids')
        if not isinstance(item, dict) or not isinstance(item.get('ids'), dict) or not isinstance(native_ids, dict) or row['native_id'] in records:
            raise ValueError('Invalid captured status title')
        observation = decode_observation(row['observation'])
        records[row['native_id']] = Record(row['native_id'], row['media'], deepcopy(item), observation, {'native_ids': deepcopy(native_ids)})
    return records


def comparison_statuses(payload):
    return {'title-status:' + record.media + ':' + key: {
                **record.item, 'record_type': 'title_status', 'status': record.observation.status,
                'native_status': record.observation.native_status, 'read_state': record.observation.state,
                'rewatching': record.observation.rewatching, 'watchlisted': record.observation.watchlisted}
            for key, record in status_records(payload).items()}


@contextmanager
def guard_feature_restore(provider, instance):
    endpoint = Endpoint(provider.lower(), normalize_instance_id(instance))
    store = StatusStore()
    with store.exclusive_instances([endpoint]):
        if any(target == endpoint for target, _, _ in store.pending_targets()):
            raise ValueError('A previous status write for this provider instance is still pending. Resolve it before restoring feature data.')
        raise_if_cancelled()
        yield


class CapturedStatuses(NativeStatusAdapter):
    def __init__(self, records):
        super().__init__({})
        self.records = deepcopy(records)

    def read(self):
        return deepcopy(self.records)


def restore_statuses(cfg, provider, instance, payload, *, store=None):
    from cw_platform.orchestrator._status_mapping import run_status_mappings

    records = status_records(payload)
    cls = adapter_class(provider)
    if cls is None:
        return {'ok': False, 'updated': 0, 'errors': ['This provider no longer supports title status restore.']}
    capture_id = 'capture-' + digest(payload)
    source = Endpoint(provider.lower(), capture_id)
    destination = Endpoint(provider.lower(), normalize_instance_id(instance))
    rules = {media: {status.value: {'include': True, 'target': status.value}
                     for status in caps.readable & caps.writable if status != Status.PLANNED}
             for media, caps in cls.capabilities.items()}
    eligible = {key: record for key, record in records.items()
                if not record.observation.complete or record.observation.status in rules.get(record.media, {})}
    pair = {'id': capture_id, 'source': provider.upper(), 'source_instance': source.instance,
            'target': provider.upper(), 'target_instance': destination.instance, 'mode': 'one-way',
            'features': {'history': {'enable': True, 'use_source_status': True}},
            'status_mapping': {'version': 1, 'a': asdict(source), 'b': asdict(destination),
                               'conflict': 'prefer_a', 'directions': {'a_to_b': rules}}}
    events = []
    ctx = SimpleNamespace(dry_run=False, interactive=None, emit=lambda event, **data: events.append((event, data)))
    with use_review_reads(None):
        restore_cfg = {**cfg, 'sync': {**(cfg.get('sync') or {}), 'dry_run': False, 'enable_add': True}}
        totals = run_status_mappings(ctx, [pair], restore_cfg, {}, store=store, adapter_overrides={source: CapturedStatuses(eligible)})
    summary = next((data for event, data in reversed(events) if event == 'status_mapping:done'), {})
    errors = [data['reason'] + (': ' + data['title'] if data.get('title') else '')
              for event, data in events if event == 'status_mapping:review']
    return {**totals, 'ok': not errors, 'errors': errors, 'captured': len(records),
            'unchanged': summary.get('unchanged', 0), 'excluded': summary.get('excluded', 0) + len(records) - len(eligible),
            'recovered': summary.get('recovered', 0), 'dry_run': summary.get('dry_run', False)}
